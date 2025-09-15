import pathlib
import sys

import pytest

sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))
from app import create_app


@pytest.fixture()
def client(memory_store):
    memory_store["fleet"] = {
        "competitors": [
            {"competitor_id": 1, "sailor_name": "A", "boat_name": "A", "sail_no": "1", "starting_handicap_s_per_hr": 100},
            {"competitor_id": 2, "sailor_name": "B", "boat_name": "B", "sail_no": "2", "starting_handicap_s_per_hr": 100},
            {"competitor_id": 3, "sailor_name": "C", "boat_name": "C", "sail_no": "3", "starting_handicap_s_per_hr": 100},
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


def _snap(client, rid, finishers=None):
    url = f"/api/races/{rid}/snapshot_version"
    if finishers is not None:
        url += f"?finishers={finishers}"
    return client.get(url).get_json()["version"]


def test_irrelevant_rank_changes_do_not_flip_hash(client):
    rid = "RACE_2025-01-01_Test_1"  # 2 finishers
    res_settings = client.post(
        "/api/settings",
        json={
            "handicap_delta_by_rank": [
                {"rank": 1, "delta_s_per_hr": -5},
                {"rank": 2, "delta_s_per_hr": -3},
                {"rank": 3, "delta_s_per_hr": -1},
                {"rank": "default_or_higher", "delta_s_per_hr": 0},
            ],
            "league_points_by_rank": [
                {"rank": 1, "points": 9},
                {"rank": 2, "points": 6},
                {"rank": 3, "points": 3},
                {"rank": "default_or_higher", "points": 1},
            ],
            "fleet_size_factor": [
                {"finishers": 2, "factor": 1.0},
                {"finishers": 3, "factor": 0.8},
                {"finishers": "default_or_higher", "factor": 1.0},
            ],
        },
    )
    assert res_settings.status_code == 200
    v1 = _snap(client, rid)
    # Change rank 3 values only (finishers=2 → ranks>2 irrelevant)
    res_settings2 = client.post(
        "/api/settings",
        json={
            "handicap_delta_by_rank": [
                {"rank": 1, "delta_s_per_hr": -5},
                {"rank": 2, "delta_s_per_hr": -3},
                {"rank": 3, "delta_s_per_hr": 99},
                {"rank": "default_or_higher", "delta_s_per_hr": 0},
            ],
            "league_points_by_rank": [
                {"rank": 1, "points": 9},
                {"rank": 2, "points": 6},
                {"rank": 3, "points": 99},
                {"rank": "default_or_higher", "points": 1},
            ],
            "fleet_size_factor": [
                {"finishers": 2, "factor": 1.0},
                {"finishers": 3, "factor": 0.7},
                {"finishers": "default_or_higher", "factor": 1.0},
            ],
        },
    )
    assert res_settings2.status_code == 200
    v2 = _snap(client, rid)
    assert v2 == v1, "Irrelevant rank changes should not flip entrant-filtered scoring hash"


def test_relevant_changes_flip_hash(client):
    rid = "RACE_2025-01-01_Test_1"  # 2 finishers
    v1 = _snap(client, rid)
    # Now change rank 2 delta and fleet factor for finishers=2
    res_settings = client.post(
        "/api/settings",
        json={
            "handicap_delta_by_rank": [
                {"rank": 1, "delta_s_per_hr": -5},
                {"rank": 2, "delta_s_per_hr": -10},
                {"rank": 3, "delta_s_per_hr": -1},
                {"rank": "default_or_higher", "delta_s_per_hr": 0},
            ],
            "league_points_by_rank": [
                {"rank": 1, "points": 9},
                {"rank": 2, "points": 6},
                {"rank": 3, "points": 3},
                {"rank": "default_or_higher", "points": 1},
            ],
            "fleet_size_factor": [
                {"finishers": 2, "factor": 0.5},
                {"finishers": 3, "factor": 0.8},
                {"finishers": "default_or_higher", "factor": 1.0},
            ],
        },
    )
    assert res_settings.status_code == 200
    v2 = _snap(client, rid)
    assert v2 != v1, "Relevant changes should flip snapshot hash"


def test_param_finishers_controls_hash(client):
    rid = "RACE_2025-01-01_Test_1"  # actual finishers=2
    # Establish a baseline settings payload
    base_settings = {
        'handicap_delta_by_rank': [
            {'rank': 1, 'delta_s_per_hr': -5},
            {'rank': 2, 'delta_s_per_hr': -3},
            {'rank': 3, 'delta_s_per_hr': -1},
            {'rank': 'default_or_higher', 'delta_s_per_hr': 0},
        ],
        'league_points_by_rank': [
            {'rank': 1, 'points': 9},
            {'rank': 2, 'points': 6},
            {'rank': 3, 'points': 3},
            {'rank': 'default_or_higher', 'points': 1},
        ],
        'fleet_size_factor': [
            {'finishers': 2, 'factor': 1.0},
            {'finishers': 3, 'factor': 0.9},
            {'finishers': 'default_or_higher', 'factor': 1.0},
        ],
    }
    res_base = client.post('/api/settings', json=base_settings)
    assert res_base.status_code == 200
    v_for2 = _snap(client, rid, finishers=2)
    v_for3_before = _snap(client, rid, finishers=3)

    # Change factor for finishers=3 only
    res_settings = client.post(
        "/api/settings",
        json={
            **base_settings,
            "fleet_size_factor": [
                {"finishers": 2, "factor": 1.0},
                {"finishers": 3, "factor": 0.123},
                {"finishers": "default_or_higher", "factor": 1.0},
            ],
        },
    )
    assert res_settings.status_code == 200
    v_for2_after = _snap(client, rid, finishers=2)
    v_for3_after = _snap(client, rid, finishers=3)
    assert v_for2_after == v_for2, "Finishers=2 hash should remain same"
    assert v_for3_after != v_for3_before, "Finishers=3 hash should change when its factor changes"
