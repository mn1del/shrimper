import pathlib
import sys
import json

import pytest

sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))
from app import create_app


@pytest.fixture()
def client(memory_store):
    # Minimal dataset with one series and one race having a non-zero start time
    memory_store["fleet"] = {"competitors": []}
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
                            "race_id": "RACE_2025-07-01_Test_1",
                            "series_id": "SER_2025_Test",
                            "name": "SER_2025_Test_1",
                            "date": "2025-07-01",
                            "start_time": "12:34:56",
                            "competitors": [],
                            "race_no": 1,
                        }
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


def test_clear_start_time_persists_as_zero(client, memory_store):
    rid = "RACE_2025-07-01_Test_1"
    # Save with explicit clear of start_time (null)
    res = client.post(
        f"/api/races/{rid}",
        data=json.dumps(
            {
                "series_id": "SER_2025_Test",
                "new_series_name": "",
                "date": "2025-07-01",
                "start_time": None,
                "finish_times": [],
                "handicap_overrides": [],
            }
        ),
        content_type="application/json",
    )
    assert res.status_code == 200
    race = _find_race(memory_store, rid)
    assert race is not None
    assert race.get("start_time") == "00:00:00"

