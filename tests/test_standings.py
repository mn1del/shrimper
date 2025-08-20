import json
import shutil
from pathlib import Path

import pytest

from app import routes, create_app


def test_traditional_standings_include_non_finishers(tmp_path, monkeypatch):
    # Copy settings required for scoring
    shutil.copy(Path('data/settings.json'), tmp_path / 'settings.json')

    # Minimal fleet with one finisher and one non-finisher
    fleet = {
        'competitors': [
            {
                'competitor_id': 'C1',
                'sailor_name': 'Finisher',
                'boat_name': 'Boat 1',
                'sail_no': '1',
                'starting_handicap_s_per_hr': 0,
            },
            {
                'competitor_id': 'C2',
                'sailor_name': 'NoFinish',
                'boat_name': 'Boat 2',
                'sail_no': '2',
                'starting_handicap_s_per_hr': 0,
            },
        ]
    }
    (tmp_path / 'fleet.json').write_text(json.dumps(fleet))

    # Series and race with a single DNF
    series_dir = tmp_path / '2025' / 'Test'
    (series_dir / 'races').mkdir(parents=True)
    (series_dir / 'series_metadata.json').write_text(
        json.dumps({'series_id': 'SER_2025_TEST', 'name': 'Test', 'season': 2025})
    )
    race = {
        'race_id': 'RACE_2025-01-01_TEST_1',
        'series_id': 'SER_2025_TEST',
        'date': '2025-01-01',
        'start_time': '10:00:00',
        'entrants': [
            {'competitor_id': 'C1', 'initial_handicap': 0, 'finish_time': '10:30:00'},
            {'competitor_id': 'C2', 'initial_handicap': 0, 'status': 'DNF'},
        ],
    }
    (series_dir / 'races' / 'RACE_2025-01-01_TEST_1.json').write_text(json.dumps(race))

    monkeypatch.setattr(routes, 'DATA_DIR', tmp_path)

    standings, _ = routes._season_standings(2025, 'traditional')
    names = [row['sailor'] for row in standings]
    assert 'NoFinish' in names
    nonfin = next(row for row in standings if row['sailor'] == 'NoFinish')
    assert nonfin['total_points'] == 2
    assert nonfin['race_count'] == 0
    assert nonfin['race_points']['RACE_2025-01-01_TEST_1'] == 2
    assert nonfin['series_counts'][0] == 0


def test_absent_sailors_scored_as_dns(tmp_path, monkeypatch):
    # Copy settings required for scoring
    shutil.copy(Path('data/settings.json'), tmp_path / 'settings.json')

    # Fleet with one finisher and one absent sailor
    fleet = {
        'competitors': [
            {
                'competitor_id': 'C1',
                'sailor_name': 'Finisher',
                'boat_name': 'Boat 1',
                'sail_no': '1',
                'starting_handicap_s_per_hr': 0,
            },
            {
                'competitor_id': 'C2',
                'sailor_name': 'Absent',
                'boat_name': 'Boat 2',
                'sail_no': '2',
                'starting_handicap_s_per_hr': 0,
            },
        ]
    }
    (tmp_path / 'fleet.json').write_text(json.dumps(fleet))

    # Series and race where only C1 is an entrant
    series_dir = tmp_path / '2025' / 'Test'
    (series_dir / 'races').mkdir(parents=True)
    (series_dir / 'series_metadata.json').write_text(
        json.dumps({'series_id': 'SER_2025_TEST', 'name': 'Test', 'season': 2025})
    )
    race = {
        'race_id': 'RACE_2025-01-01_TEST_1',
        'series_id': 'SER_2025_TEST',
        'date': '2025-01-01',
        'start_time': '10:00:00',
        'entrants': [
            {'competitor_id': 'C1', 'initial_handicap': 0, 'finish_time': '10:30:00'},
        ],
    }
    (series_dir / 'races' / 'RACE_2025-01-01_TEST_1.json').write_text(json.dumps(race))

    monkeypatch.setattr(routes, 'DATA_DIR', tmp_path)

    standings, _ = routes._season_standings(2025, 'traditional')
    names = [row['sailor'] for row in standings]
    assert 'Absent' in names
    dns_row = next(row for row in standings if row['sailor'] == 'Absent')
    assert dns_row['total_points'] == 2
    assert dns_row['race_count'] == 0
    assert dns_row['race_points']['RACE_2025-01-01_TEST_1'] == 2
    assert dns_row['series_counts'][0] == 0


def test_league_includes_absent_sailors(tmp_path, monkeypatch):
    shutil.copy(Path('data/settings.json'), tmp_path / 'settings.json')

    fleet = {
        'competitors': [
            {
                'competitor_id': 'C1',
                'sailor_name': 'Finisher',
                'boat_name': 'Boat 1',
                'sail_no': '1',
                'starting_handicap_s_per_hr': 0,
            },
            {
                'competitor_id': 'C2',
                'sailor_name': 'Absent',
                'boat_name': 'Boat 2',
                'sail_no': '2',
                'starting_handicap_s_per_hr': 0,
            },
        ]
    }
    (tmp_path / 'fleet.json').write_text(json.dumps(fleet))

    series_dir = tmp_path / '2025' / 'Test'
    (series_dir / 'races').mkdir(parents=True)
    (series_dir / 'series_metadata.json').write_text(
        json.dumps({'series_id': 'SER_2025_TEST', 'name': 'Test', 'season': 2025})
    )
    race = {
        'race_id': 'RACE_2025-01-01_TEST_1',
        'series_id': 'SER_2025_TEST',
        'date': '2025-01-01',
        'start_time': '10:00:00',
        'entrants': [
            {'competitor_id': 'C1', 'initial_handicap': 0, 'finish_time': '10:30:00'},
        ],
    }
    (series_dir / 'races' / 'RACE_2025-01-01_TEST_1.json').write_text(json.dumps(race))

    monkeypatch.setattr(routes, 'DATA_DIR', tmp_path)

    standings, _ = routes._season_standings(2025, 'league')
    names = [row['sailor'] for row in standings]
    assert 'Absent' in names
    row = next(r for r in standings if r['sailor'] == 'Absent')
    assert row['total_points'] == 0
    assert row['race_count'] == 0
    assert row['race_points']['RACE_2025-01-01_TEST_1'] == 0
    assert row['race_finished']['RACE_2025-01-01_TEST_1'] is False


def test_traditional_series_drops_high_scores(tmp_path, monkeypatch):
    shutil.copy(Path('data/settings.json'), tmp_path / 'settings.json')

    fleet = {
        'competitors': [
            {
                'competitor_id': f'C{i}',
                'sailor_name': f'S{i}',
                'boat_name': f'B{i}',
                'sail_no': str(i),
                'starting_handicap_s_per_hr': 0,
            }
            for i in range(1, 6)
        ]
    }
    (tmp_path / 'fleet.json').write_text(json.dumps(fleet))

    series_dir = tmp_path / '2024' / 'Test'
    (series_dir / 'races').mkdir(parents=True)
    (series_dir / 'series_metadata.json').write_text(
        json.dumps({'series_id': 'SER_2024_TEST', 'name': 'Test', 'season': 2024})
    )

    positions = [1, 2, 3, 4, 5]
    for i, pos in enumerate(positions, start=1):
        order = list(range(1, 6))
        order.remove(1)
        order.insert(pos - 1, 1)
        entrants = []
        for idx, comp in enumerate(order, start=1):
            entrants.append(
                {
                    'competitor_id': f'C{comp}',
                    'initial_handicap': 0,
                    'finish_time': f"10:{30 + idx:02d}:00",
                }
            )
        race = {
            'race_id': f'RACE_{i}',
            'series_id': 'SER_2024_TEST',
            'date': f'2024-01-0{i}',
            'start_time': '10:00:00',
            'entrants': entrants,
        }
        (series_dir / 'races' / f'RACE_{i}.json').write_text(json.dumps(race))

    monkeypatch.setattr(routes, 'DATA_DIR', tmp_path)

    standings, _ = routes._season_standings(2024, 'traditional')
    row = next(r for r in standings if r['sailor'] == 'S1')
    assert row['series_totals'][0] == pytest.approx(6)
    assert row['total_points'] == pytest.approx(6)
    assert {'RACE_4', 'RACE_5'} <= row['dropped_races']
    assert row['series_counts'][0] == 5


def test_invalid_race_zero_points(tmp_path, monkeypatch):
    shutil.copy(Path('data/settings.json'), tmp_path / 'settings.json')

    fleet = {
        'competitors': [
            {
                'competitor_id': 'C1',
                'sailor_name': 'Solo',
                'boat_name': 'Boat',
                'sail_no': '1',
                'starting_handicap_s_per_hr': 0,
            }
        ]
    }
    (tmp_path / 'fleet.json').write_text(json.dumps(fleet))

    series_dir = tmp_path / '2025' / 'Test'
    (series_dir / 'races').mkdir(parents=True)
    (series_dir / 'series_metadata.json').write_text(
        json.dumps({'series_id': 'SER_2025_TEST', 'name': 'Test', 'season': 2025})
    )

    race1 = {
        'race_id': 'R1',
        'series_id': 'SER_2025_TEST',
        'date': '2025-01-01',
        # Missing start_time
        'entrants': [
            {'competitor_id': 'C1', 'initial_handicap': 0, 'finish_time': '00:30:00'}
        ],
    }
    race2 = {
        'race_id': 'R2',
        'series_id': 'SER_2025_TEST',
        'date': '2025-01-02',
        'start_time': '10:00:00',
        'entrants': [
            {'competitor_id': 'C1', 'initial_handicap': 0, 'status': 'DNF'}
        ],
    }
    (series_dir / 'races' / 'R1.json').write_text(json.dumps(race1))
    (series_dir / 'races' / 'R2.json').write_text(json.dumps(race2))

    monkeypatch.setattr(routes, 'DATA_DIR', tmp_path)

    standings, _ = routes._season_standings(2025, 'traditional')
    row = standings[0]
    assert row['total_points'] == 0
    assert row['race_points']['R1'] == 0
    assert row['race_points']['R2'] == 0


def test_race_with_no_entrants_included(tmp_path, monkeypatch):
    shutil.copy(Path('data/settings.json'), tmp_path / 'settings.json')

    fleet = {
        'competitors': [
            {
                'competitor_id': 'C1',
                'sailor_name': 'Solo',
                'boat_name': 'Boat',
                'sail_no': '1',
                'starting_handicap_s_per_hr': 0,
            },
        ]
    }
    (tmp_path / 'fleet.json').write_text(json.dumps(fleet))

    series_dir = tmp_path / '2025' / 'Test'
    (series_dir / 'races').mkdir(parents=True)
    (series_dir / 'series_metadata.json').write_text(
        json.dumps({'series_id': 'SER_2025_TEST', 'name': 'Test', 'season': 2025})
    )
    race = {
        'race_id': 'R1',
        'series_id': 'SER_2025_TEST',
        'date': '2025-01-01',
        'start_time': '10:00:00',
        'entrants': [],
    }
    (series_dir / 'races' / 'R1.json').write_text(json.dumps(race))

    monkeypatch.setattr(routes, 'DATA_DIR', tmp_path)

    standings, race_groups = routes._season_standings(2025, 'league')
    assert race_groups[0]['races'][0]['race_id'] == 'R1'
    row = standings[0]
    assert row['race_points']['R1'] == 0
    assert row['race_finished']['R1'] is False


def test_standings_cells_link_to_race(tmp_path, monkeypatch):
    shutil.copy(Path('data/settings.json'), tmp_path / 'settings.json')

    fleet = {
        'competitors': [
            {
                'competitor_id': 'C1',
                'sailor_name': 'Solo',
                'boat_name': 'Boat',
                'sail_no': '1',
                'starting_handicap_s_per_hr': 0,
            }
        ]
    }
    (tmp_path / 'fleet.json').write_text(json.dumps(fleet))

    series_dir = tmp_path / '2025' / 'Test'
    (series_dir / 'races').mkdir(parents=True)
    (series_dir / 'series_metadata.json').write_text(
        json.dumps({'series_id': 'SER_2025_TEST', 'name': 'Test', 'season': 2025})
    )
    race = {
        'race_id': 'RACE_2025-01-01_TEST_1',
        'series_id': 'SER_2025_TEST',
        'date': '2025-01-01',
        'start_time': '10:00:00',
        'entrants': [
            {'competitor_id': 'C1', 'initial_handicap': 0, 'finish_time': '10:30:00'}
        ],
    }
    race_id = race['race_id']
    (series_dir / 'races' / f'{race_id}.json').write_text(json.dumps(race))

    monkeypatch.setattr(routes, 'DATA_DIR', tmp_path)
    app = create_app()
    client = app.test_client()
    resp = client.get('/standings?season=2025&format=league')
    assert resp.status_code == 200
    assert f'href="/series/SER_2025_TEST?race_id={race_id}"' in resp.get_data(as_text=True)
