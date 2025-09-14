import pathlib
import sys

import pytest

sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))
from app import create_app


@pytest.fixture()
def client(memory_store):
    app = create_app()
    app.config.update({"TESTING": True})
    with app.test_client() as c:
        yield c


def test_scoring_settings_compact(client):
    # Baseline version via lightweight endpoint
    vres = client.get('/api/settings/scoring?only=version')
    assert vres.status_code == 200
    base_version = vres.get_json().get('version')

    res = client.get('/api/settings/scoring')
    assert res.status_code == 200
    data = res.get_json()
    assert isinstance(data, dict)
    # Contains version and three compact lists
    assert data['version'] == base_version
    assert isinstance(data.get('handicap_delta_by_rank'), list)
    assert isinstance(data.get('league_points_by_rank'), list)
    assert isinstance(data.get('fleet_size_factor'), list)


def test_scoring_settings_only_version(client):
    res = client.get('/api/settings/scoring?only=version')
    assert res.status_code == 200
    data = res.get_json()
    assert list(data.keys()) == ['version']
    assert isinstance(data['version'], int)


def test_settings_post_returns_version_and_increments(client):
    # Save a minimal settings payload (lists may be empty for test)
    payload = {
        "handicap_delta_by_rank": [{"rank": 1, "delta_s_per_hr": -5}],
        "league_points_by_rank": [{"rank": 1, "points": 9}],
        "fleet_size_factor": [{"finishers": 1, "factor": 0.9}],
    }
    res = client.post('/api/settings', json=payload)
    assert res.status_code == 200
    data = res.get_json()
    assert 'version' in data
    # Second save should bump version again
    res2 = client.post('/api/settings', json=payload)
    assert res2.status_code == 200
    data2 = res2.get_json()
    assert data2['version'] == data['version'] + 1
