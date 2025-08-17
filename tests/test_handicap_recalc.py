import json
import shutil
from pathlib import Path
import pathlib
import sys

import pytest

sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))
from app import routes
from app.scoring import calculate_race_results


def test_recalculate_handicaps_uses_revised(tmp_path, monkeypatch):
    monkeypatch.setattr(routes, 'DATA_DIR', tmp_path)
    shutil.copy(Path('data/settings.json'), tmp_path / 'settings.json')

    fleet = {
        'competitors': [
            {
                'competitor_id': f'C{i}',
                'sailor_name': f'S{i}',
                'boat_name': '',
                'sail_no': str(i),
                'starting_handicap_s_per_hr': 100,
                'current_handicap_s_per_hr': 100,
                'active': True,
                'notes': '',
            }
            for i in range(1, 5)
        ]
    }
    (tmp_path / 'fleet.json').write_text(json.dumps(fleet))

    series_dir = tmp_path / '2025' / 'Test'
    race_dir = series_dir / 'races'
    race_dir.mkdir(parents=True)
    (series_dir / 'series_metadata.json').write_text(
        json.dumps({'series_id': 'SER_2025_Test', 'name': 'Test', 'season': 2025})
    )

    race1_id = 'RACE_2025-01-01_Test_1'
    race2_id = 'RACE_2025-01-08_Test_2'

    race1 = {
        'race_id': race1_id,
        'series_id': 'SER_2025_Test',
        'name': 'R1',
        'date': '2025-01-01',
        'start_time': '00:00:00',
        'entrants': [
            {'competitor_id': 'C1', 'initial_handicap': 0, 'finish_time': '00:30:00'},
            {'competitor_id': 'C2', 'initial_handicap': 0, 'finish_time': '00:31:00'},
            {'competitor_id': 'C3', 'initial_handicap': 0, 'finish_time': '00:32:00'},
            {'competitor_id': 'C4', 'initial_handicap': 0, 'finish_time': '00:33:00'},
        ],
    }
    race2 = {
        'race_id': race2_id,
        'series_id': 'SER_2025_Test',
        'name': 'R2',
        'date': '2025-01-08',
        'start_time': '00:00:00',
        'entrants': [
            {'competitor_id': 'C1', 'initial_handicap': 0},
            {'competitor_id': 'C2', 'initial_handicap': 0},
            {'competitor_id': 'C3', 'initial_handicap': 0},
            {'competitor_id': 'C4', 'initial_handicap': 0},
        ],
    }
    (race_dir / f'{race1_id}.json').write_text(json.dumps(race1))
    (race_dir / f'{race2_id}.json').write_text(json.dumps(race2))

    routes.recalculate_handicaps()

    r1 = json.loads((race_dir / f'{race1_id}.json').read_text())
    r2 = json.loads((race_dir / f'{race2_id}.json').read_text())

    start_sec = routes._parse_hms(r1['start_time']) or 0
    entries = []
    for ent in r1['entrants']:
        entries.append(
            {
                'competitor_id': ent['competitor_id'],
                'start': start_sec,
                'finish': routes._parse_hms(ent['finish_time']),
                'initial_handicap': 100,
            }
        )
    expected = calculate_race_results(entries)
    expected_map = {res['competitor_id']: res['revised_handicap'] for res in expected}

    for ent in r1['entrants']:
        assert ent['initial_handicap'] == 100

    for ent in r2['entrants']:
        cid = ent['competitor_id']
        assert ent['initial_handicap'] == expected_map[cid]

    fleet_after = json.loads((tmp_path / 'fleet.json').read_text())
    cur_map = {c['competitor_id']: c['current_handicap_s_per_hr'] for c in fleet_after['competitors']}
    for cid, hcp in expected_map.items():
        assert cur_map[cid] == hcp
