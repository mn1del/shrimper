import pathlib
import sys
import json

import pytest

sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))
from app import create_app


@pytest.fixture()
def client(memory_store):
    # Fleet with one competitor, starting=100
    memory_store["fleet"] = {
        "competitors": [
            {
                "competitor_id": 1,
                "sailor_name": "Pelican Skipper",
                "boat_name": "Pelican",
                "sail_no": "102",
                "starting_handicap_s_per_hr": 100,
                "current_handicap_s_per_hr": 100,
            }
        ]
    }

    # Season with two races
    memory_store["seasons"] = [
        {
            "year": 2025,
            "series": [
                {
                    "series_id": "SER_2025_Test",
                    "name": "Test",
                    "season": 2025,
                    "races": [
                        {
                            # Prior race with manual override only (no finishers)
                            "race_id": "RACE_2025-06-22_Test_1",
                            "series_id": "SER_2025_Test",
                            "name": "SER_2025_Test_1",
                            "date": "2025-06-22",
                            "start_time": "00:00:00",
                            "competitors": [
                                {"competitor_id": 1, "handicap_override": -8}
                            ],
                            "race_no": 1,
                        },
                        {
                            # Subsequent race initially without entrant
                            "race_id": "RACE_2025-06-27_Test_2",
                            "series_id": "SER_2025_Test",
                            "name": "SER_2025_Test_2",
                            "date": "2025-06-27",
                            "start_time": "00:00:00",
                            "competitors": [],
                            "race_no": 2,
                        },
                    ],
                }
            ],
        }
    ]

    app = create_app()
    app.config.update({"TESTING": True})
    with app.test_client() as c:
        yield c


def _find_race(memory_store, rid):
    for season in memory_store.get("seasons", []):
        for series in season.get("series", []) or []:
            for race in series.get("races", []) or []:
                if race.get("race_id") == rid:
                    return race
    return None


def test_save_adds_new_entrant_seeded_from_snapshot(client, memory_store):
    # Save: add finish for competitor 1 in the second race
    res = client.post(
        "/api/races/RACE_2025-06-27_Test_2",
        data=json.dumps(
            {
                "series_id": "SER_2025_Test",
                "new_series_name": "",
                "date": "2025-06-27",
                "start_time": "00:00:00",
                "finish_times": [{"competitor_id": 1, "finish_time": "00:10:00"}],
                "handicap_overrides": [],
            }
        ),
        content_type="application/json",
    )
    assert res.status_code == 200
    race2 = _find_race(memory_store, "RACE_2025-06-27_Test_2")
    assert race2 is not None
    ent = next(e for e in race2.get("competitors", []) if e.get("competitor_id") == 1)
    # initial_handicap should be seeded from snapshot (-8), not fleet starting (100)
    assert ent.get("initial_handicap") == -8


def test_save_clearing_override_resets_seed_from_snapshot(client, memory_store):
    # Set up entrant in second race with a manual override
    race2 = _find_race(memory_store, "RACE_2025-06-27_Test_2")
    race2.setdefault("competitors", []).append({"competitor_id": 1, "handicap_override": -12})

    # Save: clear the override
    res = client.post(
        "/api/races/RACE_2025-06-27_Test_2",
        data=json.dumps(
            {
                "series_id": "SER_2025_Test",
                "new_series_name": "",
                "date": "2025-06-27",
                "start_time": "00:00:00",
                "finish_times": [],
                "handicap_overrides": [{"competitor_id": 1, "handicap": ""}],
            }
        ),
        content_type="application/json",
    )
    assert res.status_code == 200
    race2 = _find_race(memory_store, "RACE_2025-06-27_Test_2")
    ent = next(e for e in race2.get("competitors", []) if e.get("competitor_id") == 1)
    # Override should be removed, and initial_handicap reset to snapshot (-8)
    assert ent.get("handicap_override") is None or ("handicap_override" not in ent)
    assert ent.get("initial_handicap") == -8

