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
    assert '01:24:53' in html  # adjusted time hh:mm:ss based on prior handicap


def test_race_page_shows_fleet_adjustment(client):
    res = client.get('/series/SER_2025_MYHF?race_id=RACE_2025-07-11_MYHF_1')
    html = res.get_data(as_text=True)
    assert 'Fleet Adjustment (%)' in html
    assert '<td>100</td>' in html


def test_race_page_has_dropdown_navigation(client):
    res = client.get('/series/SER_2025_MYHF?race_id=RACE_2025-07-11_MYHF_1')
    html = res.get_data(as_text=True)
    # Breadcrumbs should be absent and replaced with a link and dropdown
    assert '<ol class="breadcrumb">' not in html
    assert '<a href="/races">Races</a>' in html
    assert 'onchange="window.location=this.value"' in html
    # Dropdown should list races in descending order by date/time
    assert '2025-09-21 00:00:00 (CastS)' in html
    assert '2025-09-14 00:00:00 (CastS)' in html
    assert html.index('2025-09-21 00:00:00 (CastS)') < html.index('2025-09-14 00:00:00 (CastS)')
    # Currently viewed race should be selected
    assert 'selected>2025-07-11 18:25:00 (MYHF)</option>' in html


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
    #<getdata>
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
    assert race_data['race_no'] == 1
    #</getdata>


def test_delete_race_removes_file(client, tmp_path, monkeypatch):
    from app import routes
    monkeypatch.setattr(routes, 'DATA_DIR', tmp_path)
    #<getdata>
    sdir = tmp_path / '2030' / 'Test'
    (sdir / 'races').mkdir(parents=True)
    (sdir / 'series_metadata.json').write_text(json.dumps({
        'series_id': 'SER_2030_Test',
        'name': 'Test',
        'season': 2030,
    }))
    race_id = 'RACE_2030-01-01_Test_1'
    race_path = sdir / 'races' / f'{race_id}.json'
    race_path.write_text(json.dumps({
        'race_id': race_id,
        'series_id': 'SER_2030_Test',
        'name': 'Race',
        'date': '2030-01-01',
        'start_time': '10:00:00',
        'entrants': [],
        'race_no': 1,
    }))
    #</getdata>
    res = client.delete(f'/api/races/{race_id}')
    assert res.status_code == 200
    assert not race_path.exists()
    data = res.get_json()
    assert data['redirect']


def test_races_page_can_filter_by_season(client, tmp_path, monkeypatch):
    from app import routes
    monkeypatch.setattr(routes, 'DATA_DIR', tmp_path)

    def create_season(year: int):
        #<getdata>
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
            'race_no': 1,
        }))
        #</getdata>

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


def test_race_page_shows_defaults_for_non_finishers(client, tmp_path, monkeypatch):
    from app import routes
    import json, shutil
    from pathlib import Path

    # Redirect data directory to a temporary location
    monkeypatch.setattr(routes, 'DATA_DIR', tmp_path)

    # Copy settings required for scoring
    #<getdata>
    shutil.copy(Path('data/settings.json'), tmp_path / 'settings.json')
    #</getdata>

    # Create a minimal fleet with a single competitor
    fleet = {
        'competitors': [
            {
                'competitor_id': 'C1',
                'sailor_name': 'Test Sailor',
                'boat_name': 'Test Boat',
                'sail_no': '1',
                'starting_handicap_s_per_hr': 100,
                'current_handicap_s_per_hr': 100,
                'active': True,
                'notes': '',
            }
        ]
    }
    #<getdata>
    (tmp_path / 'fleet.json').write_text(json.dumps(fleet))
    #</getdata>

    # Set up a series and race where the entrant has no finish time
    #<getdata>
    series_dir = tmp_path / '2025' / 'Test'
    race_dir = series_dir / 'races'
    race_dir.mkdir(parents=True)
    (series_dir / 'series_metadata.json').write_text(
        json.dumps({'series_id': 'SER_2025_Test', 'name': 'Test', 'season': 2025})
    )
    race_id = 'RACE_2025-01-01_Test_1'
    race_data = {
        'race_id': race_id,
        'series_id': 'SER_2025_Test',
        'name': 'SER_2025_Test_1',
        'date': '2025-01-01',
        'start_time': '00:00:00',
        'entrants': [
            {'competitor_id': 'C1', 'initial_handicap': 100}
        ],
        'race_no': 1,
    }
    (race_dir / f'{race_id}.json').write_text(json.dumps(race_data))
    #</getdata>

    res = client.get(f'/series/SER_2025_Test?race_id={race_id}')
    html = res.get_data(as_text=True)

    # Non-finisher row should show default handicap changes and points
    assert '>0</td>' in html  # Full/adjusted handicap change or league pts
    assert '>100</td>' in html  # Revised handicap equals initial handicap
    assert '>1</td>' in html  # Traditional points = finishers + 1 (0 + 1)


def test_race_page_shows_defaults_for_absent_competitor(client, tmp_path, monkeypatch):
    """Competitors missing from entrants should still show zeroed fields."""
    from app import routes
    import json, shutil, re
    from pathlib import Path

    monkeypatch.setattr(routes, 'DATA_DIR', tmp_path)
    #<getdata>
    shutil.copy(Path('data/settings.json'), tmp_path / 'settings.json')
    #</getdata>

    fleet = {
        'competitors': [
            {
                'competitor_id': 'C1',
                'sailor_name': 'Test Sailor',
                'boat_name': 'Test Boat',
                'sail_no': '1',
                'starting_handicap_s_per_hr': 100,
                'current_handicap_s_per_hr': 100,
                'active': True,
                'notes': '',
            }
        ]
    }
    #<getdata>
    (tmp_path / 'fleet.json').write_text(json.dumps(fleet))
    #</getdata>

    #<getdata>
    series_dir = tmp_path / '2025' / 'Test'
    race_dir = series_dir / 'races'
    race_dir.mkdir(parents=True)
    (series_dir / 'series_metadata.json').write_text(
        json.dumps({'series_id': 'SER_2025_Test', 'name': 'Test', 'season': 2025})
    )
    race_id = 'RACE_2025-01-02_Test_1'
    race_data = {
        'race_id': race_id,
        'series_id': 'SER_2025_Test',
        'name': 'SER_2025_Test_1',
        'date': '2025-01-02',
        'start_time': '00:00:00',
        'entrants': [],
        'race_no': 1,
    }
    (race_dir / f'{race_id}.json').write_text(json.dumps(race_data))
    #</getdata>

    res = client.get(f'/series/SER_2025_Test?race_id={race_id}')
    html = res.get_data(as_text=True)

    row = re.search(r'Test Sailor.*?</tr>', html, re.DOTALL)
    assert row is not None
    row_html = row.group()
    assert 'value=""' in row_html  # No finish time provided
    assert '00:00:00' in row_html  # Adjusted time zeroed
    assert row_html.count('>0</td>') >= 4  # timing/handicap/points zeroed
    assert '>100</td>' in row_html  # Revised handicap equals initial
    assert '>1</td>' in row_html  # Traditional points = finishers + 1 (0 + 1)


def test_settings_save_updates_json_and_page(client, tmp_path, monkeypatch):
    from app import routes

    monkeypatch.setattr(routes, 'DATA_DIR', tmp_path)
    #<getdata>
    settings_path = tmp_path / 'settings.json'
    original = {
        'version': 1,
        'updated_at': '2025-01-01T00:00:00Z',
        'handicap_delta_by_rank': [{'rank': 1, 'delta_s_per_hr': -10}],
        'league_points_by_rank': [{'rank': 1, 'points': 5}],
        'fleet_size_factor': [{'finishers': 1, 'factor': 0.5}],
    }
    settings_path.write_text(json.dumps(original))
    #</getdata>

    res = client.get('/settings')
    html = res.get_data(as_text=True)
    assert 'value="-10"' in html

    new_data = {
        'handicap_delta_by_rank': [{'rank': 1, 'delta_s_per_hr': -5}],
        'league_points_by_rank': [{'rank': 1, 'points': 9}],
        'fleet_size_factor': [{'finishers': 1, 'factor': 0.9}],
    }
    res = client.post('/api/settings', json=new_data)
    assert res.status_code == 200

    #<getdata>
    with settings_path.open() as f:
        saved = json.load(f)
    #</getdata>
    assert saved['handicap_delta_by_rank'][0]['delta_s_per_hr'] == -5
    assert saved['league_points_by_rank'][0]['points'] == 9
    assert saved['fleet_size_factor'][0]['factor'] == 0.9

    res = client.get('/settings')
    html = res.get_data(as_text=True)
    assert 'value="-5"' in html
    assert 'value="9"' in html
    assert 'value="0.9"' in html


def test_fleet_update_propagates(client, tmp_path, monkeypatch):
    from app import routes

    monkeypatch.setattr(routes, 'DATA_DIR', tmp_path)

    fleet = {
        'competitors': [{
            'competitor_id': 'C1',
            'sailor_name': 'Old',
            'boat_name': 'Boat',
            'sail_no': '1',
            'starting_handicap_s_per_hr': 100,
            'current_handicap_s_per_hr': 100,
            'active': True,
            'notes': ''
        }]
    }
    #<getdata>
    (tmp_path / 'fleet.json').write_text(json.dumps(fleet))
    #</getdata>

    #<getdata>
    series_dir = tmp_path / '2025' / 'Test'
    race_dir = series_dir / 'races'
    race_dir.mkdir(parents=True)
    (series_dir / 'series_metadata.json').write_text(json.dumps({
        'series_id': 'SER_2025_Test', 'name': 'Test', 'season': 2025
    }))
    race_id = 'RACE_2025-01-01_Test_1'
    race_data = {
        'race_id': race_id,
        'series_id': 'SER_2025_Test',
        'name': 'SER_2025_Test_1',
        'date': '2025-01-01',
        'start_time': '00:00:00',
        'entrants': [
            {'competitor_id': 'C1', 'initial_handicap': 100, 'finish_time': '00:30:00'}
        ],
        'results': {},
        'race_no': 1,
    }
    (race_dir / f'{race_id}.json').write_text(json.dumps(race_data))
    #</getdata>

    payload = {
        'competitors': [{
            'competitor_id': 'C1',
            'sailor_name': 'New',
            'boat_name': 'New Boat',
            'sail_no': '99',
            'starting_handicap_s_per_hr': 150
        }]
    }
    res = client.post('/api/fleet', json=payload)
    assert res.status_code == 200

    #<getdata>
    with (tmp_path / 'fleet.json').open() as f:
        saved = json.load(f)
    assert saved['competitors'][0]['sailor_name'] == 'New'
    assert saved['competitors'][0]['starting_handicap_s_per_hr'] == 150

    with (race_dir / f'{race_id}.json').open() as f:
        updated = json.load(f)
    assert updated['entrants'][0]['initial_handicap'] == 150
    #</getdata>


def test_fleet_update_rejects_duplicate_sail_numbers(client, tmp_path, monkeypatch):
    from app import routes

    monkeypatch.setattr(routes, 'DATA_DIR', tmp_path)

    fleet = {
        'competitors': [
            {
                'competitor_id': 'C1',
                'sailor_name': 'A',
                'boat_name': 'BoatA',
                'sail_no': '1',
                'starting_handicap_s_per_hr': 100,
                'current_handicap_s_per_hr': 100,
                'active': True,
                'notes': ''
            },
            {
                'competitor_id': 'C2',
                'sailor_name': 'B',
                'boat_name': 'BoatB',
                'sail_no': '2',
                'starting_handicap_s_per_hr': 100,
                'current_handicap_s_per_hr': 100,
                'active': True,
                'notes': ''
            },
        ]
    }
    #<getdata>
    (tmp_path / 'fleet.json').write_text(json.dumps(fleet))
    #</getdata>

    payload = {
        'competitors': [
            {
                'competitor_id': 'C2',
                'sailor_name': 'B',
                'boat_name': 'BoatB',
                'sail_no': '1',
                'starting_handicap_s_per_hr': 100
            }
        ]
    }
    res = client.post('/api/fleet', json=payload)
    assert res.status_code == 400
    assert 'Duplicate sail numbers' in res.get_json()['error']


def test_add_race_renumbers(client, tmp_path, monkeypatch):
    from app import routes
    monkeypatch.setattr(routes, 'DATA_DIR', tmp_path)
    #<getdata>
    series_dir = tmp_path / '2025' / 'Test'
    races_dir = series_dir / 'races'
    races_dir.mkdir(parents=True)
    (series_dir / 'series_metadata.json').write_text(json.dumps({
        'series_id': 'SER_2025_Test', 'name': 'Test', 'season': 2025
    }))
    r1_id = 'RACE_2025-01-08_Test_1'
    r2_id = 'RACE_2025-01-15_Test_2'
    (races_dir / f'{r1_id}.json').write_text(json.dumps({
        'race_id': r1_id,
        'series_id': 'SER_2025_Test',
        'name': 'SER_2025_Test_1',
        'date': '2025-01-08',
        'start_time': '00:00:00',
        'entrants': [],
        'race_no': 1,
    }))
    (races_dir / f'{r2_id}.json').write_text(json.dumps({
        'race_id': r2_id,
        'series_id': 'SER_2025_Test',
        'name': 'SER_2025_Test_2',
        'date': '2025-01-15',
        'start_time': '00:00:00',
        'entrants': [],
        'race_no': 2,
    }))

    res = client.post('/api/races/__new__', json={
        'series_id': 'SER_2025_Test',
        'date': '2025-01-01',
        'start_time': '00:00:00',
        'finish_times': [],
    })
    assert res.status_code == 200

    files = sorted(races_dir.glob('RACE_*.json'))
    data = [json.loads(p.read_text()) for p in files]
    data.sort(key=lambda d: d['race_no'])
    assert [d['race_id'] for d in data] == [
        'RACE_2025-01-01_Test_1',
        'RACE_2025-01-08_Test_2',
        'RACE_2025-01-15_Test_3',
    ]
    #</getdata>


def test_edit_race_renumbers(client, tmp_path, monkeypatch):
    from app import routes
    monkeypatch.setattr(routes, 'DATA_DIR', tmp_path)
    #<getdata>
    series_dir = tmp_path / '2025' / 'Test'
    races_dir = series_dir / 'races'
    races_dir.mkdir(parents=True)
    (series_dir / 'series_metadata.json').write_text(json.dumps({
        'series_id': 'SER_2025_Test', 'name': 'Test', 'season': 2025
    }))
    r1_id = 'RACE_2025-01-01_Test_1'
    r2_id = 'RACE_2025-01-08_Test_2'
    (races_dir / f'{r1_id}.json').write_text(json.dumps({
        'race_id': r1_id,
        'series_id': 'SER_2025_Test',
        'name': 'SER_2025_Test_1',
        'date': '2025-01-01',
        'start_time': '00:00:00',
        'entrants': [],
        'race_no': 1,
    }))
    (races_dir / f'{r2_id}.json').write_text(json.dumps({
        'race_id': r2_id,
        'series_id': 'SER_2025_Test',
        'name': 'SER_2025_Test_2',
        'date': '2025-01-08',
        'start_time': '00:00:00',
        'entrants': [],
        'race_no': 2,
    }))

    res = client.post(f'/api/races/{r1_id}', json={'date': '2025-01-10'})
    assert res.status_code == 200

    files = sorted(races_dir.glob('RACE_*.json'))
    data = [json.loads(p.read_text()) for p in files]
    data.sort(key=lambda d: d['race_no'])
    assert [d['race_id'] for d in data] == [
        'RACE_2025-01-08_Test_1',
        'RACE_2025-01-10_Test_2',
    ]
    #</getdata>
