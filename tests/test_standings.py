import json
import shutil
from pathlib import Path

from app import routes


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
