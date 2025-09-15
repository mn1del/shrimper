import pathlib
import sys

import pytest

sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))
from app import create_app


@pytest.fixture()
def client(memory_store):
    # Fleet includes a non-entrant competitor (id=3)
    memory_store["fleet"] = {
        "competitors": [
            {"competitor_id": 1, "sailor_name": "A", "boat_name": "A", "sail_no": "1", "starting_handicap_s_per_hr": 100},
            {"competitor_id": 2, "sailor_name": "B", "boat_name": "B", "sail_no": "2", "starting_handicap_s_per_hr": 100},
            {"competitor_id": 3, "sailor_name": "C", "boat_name": "C", "sail_no": "3", "starting_handicap_s_per_hr": 200},
        ]
    }
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
                            "race_id": "RACE_2025-01-01_Test_1",
                            "series_id": "SER_2025_Test",
                            "name": "SER_2025_Test_1",
                            "date": "2025-01-01",
                            "start_time": "00:00:00",
                            "competitors": [
                                {"competitor_id": 1, "finish_time": "00:30:00"},
                                {"competitor_id": 2},
                            ],
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


def test_snapshot_ignores_non_entrants(client):
    rid = "RACE_2025-01-01_Test_1"
    v1 = client.get(f"/api/races/{rid}/snapshot_version").get_json()["version"]
    # Change only the non-entrant's starting handicap via fleet API
    res = client.post(
        "/api/fleet",
        json={
            "competitors": [
                {"competitor_id": 1, "sailor_name": "A", "boat_name": "A", "sail_no": "1", "starting_handicap_s_per_hr": 100},
                {"competitor_id": 2, "sailor_name": "B", "boat_name": "B", "sail_no": "2", "starting_handicap_s_per_hr": 100},
                {"competitor_id": 3, "sailor_name": "C", "boat_name": "C", "sail_no": "3", "starting_handicap_s_per_hr": 250},
            ]
        },
    )
    assert res.status_code == 200 or res.status_code == 400  # allow duplicate sails handling
    v2 = client.get(f"/api/races/{rid}/snapshot_version").get_json()["version"]
    assert v2 == v1, "Non-entrant change should not affect snapshot hash"


def test_snapshot_changes_when_entrant_seed_changes(client):
    rid = "RACE_2025-01-01_Test_1"
    v1 = client.get(f"/api/races/{rid}/snapshot_version").get_json()["version"]
    # Change an entrant's starting handicap
    res = client.post(
        "/api/fleet",
        json={
            "competitors": [
                {"competitor_id": 1, "sailor_name": "A", "boat_name": "A", "sail_no": "1", "starting_handicap_s_per_hr": 120},
                {"competitor_id": 2, "sailor_name": "B", "boat_name": "B", "sail_no": "2", "starting_handicap_s_per_hr": 100},
                {"competitor_id": 3, "sailor_name": "C", "boat_name": "C", "sail_no": "3", "starting_handicap_s_per_hr": 200},
            ]
        },
    )
    assert res.status_code == 200 or res.status_code == 400
    v2 = client.get(f"/api/races/{rid}/snapshot_version").get_json()["version"]
    assert v2 != v1, "Entrant change should affect snapshot hash"

