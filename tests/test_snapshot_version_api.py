import pathlib
import sys

import pytest

sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))
from app import create_app


@pytest.fixture()
def client(memory_store):
    # Seed a dataset with two races in a series
    memory_store["fleet"] = {
        "competitors": [
            {"competitor_id": 1, "sailor_name": "Alice", "boat_name": "Boaty", "sail_no": "1", "starting_handicap_s_per_hr": 100},
            {"competitor_id": 2, "sailor_name": "Bob", "boat_name": "Crafty", "sail_no": "2", "starting_handicap_s_per_hr": 100},
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
                                {"competitor_id": 2, "finish_time": "00:31:00"},
                            ],
                            "race_no": 1,
                        },
                        {
                            "race_id": "RACE_2025-01-08_Test_2",
                            "series_id": "SER_2025_Test",
                            "name": "SER_2025_Test_2",
                            "date": "2025-01-08",
                            "start_time": "00:00:00",
                            "competitors": [
                                {"competitor_id": 1},
                                {"competitor_id": 2},
                            ],
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


def test_snapshot_version_changes_with_settings(client):
    # Use a race with 2 finishers so filtered scoring includes rank 1 and factor for 2
    rid = "RACE_2025-01-01_Test_1"
    res1 = client.get(f"/api/races/{rid}/snapshot_version")
    assert res1.status_code == 200
    v1 = res1.get_json().get('version')
    assert isinstance(v1, str) and len(v1) > 0

    # Change settings via API; version should bump and snapshot hash change
    res_set = client.post(
        "/api/settings",
        json={
            "handicap_delta_by_rank": [{"rank": 1, "delta_s_per_hr": -5}],
            "league_points_by_rank": [{"rank": 1, "points": 9}],
            "fleet_size_factor": [{"finishers": 2, "factor": 0.9}],
        },
    )
    assert res_set.status_code == 200

    res2 = client.get(f"/api/races/{rid}/snapshot_version")
    assert res2.status_code == 200
    v2 = res2.get_json().get('version')
    assert v1 != v2


def test_series_detail_embeds_scoring_and_seeds(client):
    rid = "RACE_2025-01-01_Test_1"
    res = client.get(f"/series/SER_2025_Test?race_id={rid}")
    assert res.status_code == 200
    html = res.get_data(as_text=True)
    # Ensure script injections exist (snapshot version is no longer embedded)
    assert "window.SCORING_SETTINGS" in html
    assert "window.SCORING_VERSION" in html
    assert "window.PRE_RACE_SEEDS" in html
    assert "window.PRE_SNAPSHOT_VERSION" not in html
