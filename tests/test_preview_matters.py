import pathlib
import sys

import pytest

sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))
from app import create_app


@pytest.fixture()
def client(memory_store):
    # Simple two-race season; second race is the target for preview
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


def test_preview_changes_when_prior_race_changes(client):
    target = "RACE_2025-01-08_Test_2"
    # Make fleet factor non-zero for small fleets so prior race changes matter
    res_settings = client.post(
        "/api/settings",
        json={
            "handicap_delta_by_rank": [{"rank": 1, "delta_s_per_hr": -5}, {"rank": 2, "delta_s_per_hr": 2}, {"rank": "default_or_higher", "delta_s_per_hr": 0}],
            "league_points_by_rank": [{"rank": 1, "points": 9}, {"rank": 2, "points": 6}, {"rank": "default_or_higher", "points": 3}],
            "fleet_size_factor": [{"finishers": 2, "factor": 1.0}, {"finishers": "default_or_higher", "factor": 1.0}],
        },
    )
    assert res_settings.status_code == 200

    payload = {
        "start_time": "01:00:00",
        "finish_times": [
            {"competitor_id": 1, "finish_time": "01:30:00"},
            {"competitor_id": 2, "finish_time": "01:31:00"},
        ],
        "handicap_overrides": [],
    }
    p1 = client.post(f"/api/races/{target}/preview", json=payload)
    assert p1.status_code == 200
    r1 = p1.get_json()["results"]

    # Change prior race to swap positions, which should change seeds
    res_edit = client.post(
        "/api/races/RACE_2025-01-01_Test_1",
        json={
            "finish_times": [
                {"competitor_id": 1, "finish_time": "00:32:00"},
                {"competitor_id": 2, "finish_time": "00:30:00"},
            ]
        },
    )
    assert res_edit.status_code == 200
    # Ensure fleet still present for preview (update_race persists only seasons)
    client.post(
        "/api/fleet",
        json={
            "competitors": [
                {"competitor_id": 1, "sailor_name": "A", "boat_name": "A", "sail_no": "1", "starting_handicap_s_per_hr": 100},
                {"competitor_id": 2, "sailor_name": "B", "boat_name": "B", "sail_no": "2", "starting_handicap_s_per_hr": 100},
            ]
        },
    )

    p2 = client.post(f"/api/races/{target}/preview", json=payload)
    if p2.status_code != 200:
        print('Preview after edit failed:', p2.status_code, p2.get_data(as_text=True))
    assert p2.status_code == 200
    r2 = p2.get_json()["results"]

    # At least one of the compared fields should differ for an entrant
    changed = False
    for cid in ("1", "2"):
        for k in ("abs_pos", "hcp_pos", "actual_delta", "race_pts", "league_pts", "revised_hcp"):
            if r1[cid].get(k) != r2[cid].get(k):
                changed = True
                break
        if changed:
            break
    assert changed, "Expected preview results to change when prior race changes"
