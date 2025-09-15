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


def _get_snapshot(client, rid):
    return client.get(f"/api/races/{rid}/snapshot_version").get_json()["version"]


def _get_compact_settings(client):
    return client.get('/api/settings/scoring').get_json()


def test_snapshot_unchanged_on_settings_noop(client):
    rid = "RACE_2025-01-01_Test_1"
    v1 = _get_snapshot(client, rid)
    compact = _get_compact_settings(client)
    # Re-save exactly the same compact settings
    payload = {
        'handicap_delta_by_rank': compact['handicap_delta_by_rank'],
        'league_points_by_rank': compact['league_points_by_rank'],
        'fleet_size_factor': compact['fleet_size_factor'],
    }
    res = client.post('/api/settings', json=payload)
    assert res.status_code == 200
    v2 = _get_snapshot(client, rid)
    assert v2 == v1, 'No-op settings save must not change snapshot hash'


def test_snapshot_unchanged_when_settings_reordered(client):
    rid = "RACE_2025-01-01_Test_1"
    v1 = _get_snapshot(client, rid)
    compact = _get_compact_settings(client)
    # Reorder lists but keep content identical
    payload = {
        'handicap_delta_by_rank': list(reversed(compact['handicap_delta_by_rank'])),
        'league_points_by_rank': list(reversed(compact['league_points_by_rank'])),
        'fleet_size_factor': list(reversed(compact['fleet_size_factor'])),
    }
    res = client.post('/api/settings', json=payload)
    assert res.status_code == 200
    v2 = _get_snapshot(client, rid)
    assert v2 == v1, 'Reordering settings must not change snapshot hash'


def test_snapshot_changes_when_compact_settings_change(client):
    rid = "RACE_2025-01-01_Test_1"
    v1 = _get_snapshot(client, rid)
    compact = _get_compact_settings(client)
    # Change an actual value (e.g., delta rank 1)
    payload = {
        'handicap_delta_by_rank': [{'rank': 1, 'delta_s_per_hr': -999}] + [
            x for x in compact['handicap_delta_by_rank'] if x.get('rank') != 1
        ],
        'league_points_by_rank': compact['league_points_by_rank'],
        'fleet_size_factor': compact['fleet_size_factor'],
    }
    res = client.post('/api/settings', json=payload)
    assert res.status_code == 200
    v2 = _get_snapshot(client, rid)
    assert v2 != v1, 'Changing scoring content must change snapshot hash'

