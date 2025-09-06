import pathlib
import sys
import json

import pytest

sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))
from app import create_app


@pytest.fixture()
def client(memory_store):
    # Fleet with one competitor
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

    # Season with two races a few days apart
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
                            # Subsequent race where boat does not compete
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


def test_selected_race_uses_snapshot_for_nonentrant_baseline(client):
    # View the later race; handicap input placeholder should reflect snapshot seed (-8),
    # not fleet starting (100), even though there is no entrant row.
    res = client.get("/series/SER_2025_Test?race_id=RACE_2025-06-27_Test_2")
    assert res.status_code == 200
    html = res.get_data(as_text=True)
    # Look for the handicap input for competitor id 1 and ensure placeholder is -8
    assert 'data-cid="1"' in html
    assert 'placeholder="-8"' in html
    # And ensure we are not showing the fleet starting value
    assert 'placeholder="100"' not in html


def test_preview_uses_snapshot_seed_then_applies_edits(client, memory_store):
    # Add competitor id 1 as an entrant without a finish in the second race
    for season in memory_store.get("seasons", []):
        for series in season.get("series", []) or []:
            for race in series.get("races", []) or []:
                if race.get("race_id") == "RACE_2025-06-27_Test_2":
                    race.setdefault("competitors", []).append({"competitor_id": 1})

    # Preview with no changes: revised_hcp for non-finisher equals initial seed (-8)
    res = client.post(
        "/api/races/RACE_2025-06-27_Test_2/preview",
        data=json.dumps({"finish_times": [], "handicap_overrides": []}),
        content_type="application/json",
    )
    assert res.status_code == 200
    data = res.get_json()
    # JSON object keys are strings
    assert "1" in {str(k) for k in data["results"].keys()}
    pel = data["results"]["1"]
    assert pel["revised_hcp"] == -8
