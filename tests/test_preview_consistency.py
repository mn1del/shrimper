import pathlib
import sys

import pytest

sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))
from app import create_app


@pytest.fixture()
def client(memory_store):
    # Two competitors, one empty race to edit
    memory_store["fleet"] = {
        "competitors": [
            {"competitor_id": 1, "sailor_name": "A", "boat_name": "A", "sail_no": "1", "starting_handicap_s_per_hr": 100},
            {"competitor_id": 2, "sailor_name": "B", "boat_name": "B", "sail_no": "2", "starting_handicap_s_per_hr": 100},
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
                            "start_time": "",
                            "competitors": [
                                {"competitor_id": 1},
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


def test_preview_matches_saved_results(client):
    rid = "RACE_2025-01-01_Test_1"
    # Build an edit payload with explicit start and two finishers
    payload = {
        "start_time": "01:00:00",
        "finish_times": [
            {"competitor_id": 1, "finish_time": "01:30:00"},
            {"competitor_id": 2, "finish_time": "01:31:00"},
        ],
        "handicap_overrides": [],
    }
    # Preview
    p = client.post(f"/api/races/{rid}/preview", json=payload)
    assert p.status_code == 200
    preview = p.get_json()
    assert isinstance(preview, dict) and "results" in preview

    # Save
    s = client.post(f"/api/races/{rid}", json=payload)
    assert s.status_code == 200

    # Preview again with empty diff (should reflect saved state)
    p2 = client.post(f"/api/races/{rid}/preview", json={})
    assert p2.status_code == 200
    after = p2.get_json()

    # Compare a subset of stable numeric/text fields per competitor
    for cid in (1, 2):
        cid_key = str(cid)
        a = preview["results"][cid_key]
        b = after["results"][cid_key]
        keys = [
            "finish_time",
            "on_course_secs",
            "abs_pos",
            "allowance",
            "adj_time_secs",
            "hcp_pos",
            "race_pts",
            "league_pts",
            "full_delta",
            "actual_delta",
            "revised_hcp",
        ]
        for k in keys:
            assert a.get(k) == b.get(k)
