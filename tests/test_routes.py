import pytest
import pathlib
import sys
import json

sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))
from app import create_app


@pytest.fixture()
def client():
    app = create_app()
    app.config.update({'TESTING': True})
    with app.test_client() as client:
        yield client


def test_race_page_uses_race_json_data(client):
    res = client.get('/series/SER_2025_MYHF?race_id=RACE_2025-07-11_MYHF_1')
    html = res.get_data(as_text=True)
    assert 'value="18:25:00"' in html
    assert 'value="19:52:41"' in html
    assert 'Number of Finishers: 8' in html


def test_race_sheet_redirects(client):
    res = client.get('/races/RACE_2025-07-11_MYHF_1', follow_redirects=False)
    assert res.status_code == 302
    assert '/series/SER_2025_MYHF?race_id=RACE_2025-07-11_MYHF_1' in res.headers['Location']


def test_race_page_calculates_results(client):
    res = client.get('/series/SER_2025_MYHF?race_id=RACE_2025-07-11_MYHF_1')
    html = res.get_data(as_text=True)
    # On course time and adjusted time are calculated
    assert '5261' in html  # on course seconds for first finisher
    assert '01:24:53' in html  # adjusted time hh:mm:ss


def test_series_detail_case_insensitive(client):
    """Series routes should be accessible regardless of ID casing."""
    res = client.get('/series/ser_2025_myhf?race_id=RACE_2025-07-11_MYHF_1')
    assert res.status_code == 200


def test_races_page_lists_races(client):
    res = client.get('/races')
    html = res.get_data(as_text=True)
    # most recent race date should appear before earlier ones
    assert html.index('2025-05-16') < html.index('2025-04-26')
    # rows link to individual race pages
    assert '/races/RACE_2025-05-23_CastF_2' in html


def test_races_page_has_create_button(client):
    res = client.get('/races')
    html = res.get_data(as_text=True)
    assert 'Create New Race' in html
    assert 'href="/races/new"' in html


def test_race_sheet_redirects_to_canonical_series_id(client):
    res = client.get('/races/RACE_2025-05-23_CastF_2', follow_redirects=False)
    assert res.status_code == 302
    assert '/series/SER_2025_CASTF?race_id=RACE_2025-05-23_CastF_2' in res.headers['Location']


def test_create_new_race_creates_files(client, tmp_path, monkeypatch):
    from app import routes
    monkeypatch.setattr(routes, 'DATA_DIR', tmp_path)
    res = client.post('/api/races/__new__', json={
        'series_id': '__new__',
        'new_series_name': 'Test',
        'date': '2030-01-01',
        'start_time': '12:30:45',
        'finish_times': [],
    })
    assert res.status_code == 200
    data = res.get_json()
    assert data['redirect']
    series_meta = tmp_path / '2030' / 'Test' / 'series_metadata.json'
    assert series_meta.exists()
    race_files = list((tmp_path / '2030' / 'Test' / 'races').glob('*.json'))
    assert len(race_files) == 1
    with series_meta.open() as f:
        meta = json.load(f)
    assert meta['season'] == 2030
    with race_files[0].open() as f:
        race_data = json.load(f)
    assert race_data['date'] == '2030-01-01'
    assert race_data['start_time'] == '12:30:45'
    assert race_data['race_id'].startswith('RACE_2030-01-01_Test_')


def test_races_page_can_filter_by_season(client, tmp_path, monkeypatch):
    from app import routes
    monkeypatch.setattr(routes, 'DATA_DIR', tmp_path)

    def create_season(year: int):
        sdir = tmp_path / str(year) / f'S{year}'
        (sdir / 'races').mkdir(parents=True)
        (sdir / 'series_metadata.json').write_text(json.dumps({
            'series_id': f'SER_{year}_S{year}',
            'name': f'S{year}',
            'season': year,
        }))
        (sdir / 'races' / f'RACE_{year}-01-01_S{year}_1.json').write_text(json.dumps({
            'race_id': f'RACE_{year}-01-01_S{year}_1',
            'series_id': f'SER_{year}_S{year}',
            'name': 'Race',
            'date': f'{year}-01-01',
            'start_time': '10:00:00',
            'entrants': [],
        }))

    create_season(2024)
    create_season(2025)

    res = client.get('/races')
    html = res.get_data(as_text=True)
    assert '<option value="2024"' in html
    assert '<option value="2025"' in html

    res = client.get('/races?season=2024')
    html = res.get_data(as_text=True)
    assert '2024-01-01' in html
    assert '2025-01-01' not in html
