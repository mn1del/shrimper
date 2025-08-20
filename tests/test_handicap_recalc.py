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


def test_handicap_override(tmp_path, monkeypatch):
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

    # Three races, override applied in race2 for C1
    race_ids = [
        'RACE_2025-01-01_Test_1',
        'RACE_2025-01-08_Test_2',
        'RACE_2025-01-15_Test_3',
    ]

    finish_order = ['00:30:00', '00:31:00', '00:32:00', '00:33:00']
    race1 = {
        'race_id': race_ids[0],
        'series_id': 'SER_2025_Test',
        'name': 'R1',
        'date': '2025-01-01',
        'start_time': '00:00:00',
        'entrants': [
            {'competitor_id': f'C{i}', 'finish_time': finish_order[i-1]} for i in range(1,5)
        ],
    }
    race2 = {
        'race_id': race_ids[1],
        'series_id': 'SER_2025_Test',
        'name': 'R2',
        'date': '2025-01-08',
        'start_time': '00:00:00',
        'entrants': [
            {'competitor_id': 'C1', 'finish_time': finish_order[0], 'handicap_override': 200},
            {'competitor_id': 'C2', 'finish_time': finish_order[1]},
            {'competitor_id': 'C3', 'finish_time': finish_order[2]},
            {'competitor_id': 'C4', 'finish_time': finish_order[3]},
        ],
    }
    race3 = {
        'race_id': race_ids[2],
        'series_id': 'SER_2025_Test',
        'name': 'R3',
        'date': '2025-01-15',
        'start_time': '00:00:00',
        'entrants': [
            {'competitor_id': f'C{i}'} for i in range(1,5)
        ],
    }

    for race in (race1, race2, race3):
        (race_dir / f"{race['race_id']}.json").write_text(json.dumps(race))

    # Expected handicaps after each race when override applied
    start_sec = routes._parse_hms('00:00:00') or 0
    entries1 = [
        {
            'competitor_id': f'C{i}',
            'start': start_sec,
            'finish': routes._parse_hms(finish_order[i-1]),
            'initial_handicap': 100,
        }
        for i in range(1,5)
    ]
    res1 = calculate_race_results(entries1)
    after_r1 = {r['competitor_id']: r['revised_handicap'] for r in res1}

    entries2 = []
    for i in range(1,5):
        cid = f'C{i}'
        init = 200 if cid == 'C1' else after_r1[cid]
        entries2.append(
            {
                'competitor_id': cid,
                'start': start_sec,
                'finish': routes._parse_hms(finish_order[i-1]),
                'initial_handicap': init,
            }
        )
    res2 = calculate_race_results(entries2)
    after_r2 = {r['competitor_id']: r['revised_handicap'] for r in res2}

    routes.recalculate_handicaps()

    r2 = json.loads((race_dir / f"{race_ids[1]}.json").read_text())
    r3 = json.loads((race_dir / f"{race_ids[2]}.json").read_text())

    # Race2 should use override for initial handicap
    r2_map = {e['competitor_id']: e for e in r2['entrants']}
    assert r2_map['C1']['initial_handicap'] == 200
    assert r2_map['C1']['handicap_override'] == 200
    for cid in ['C2', 'C3', 'C4']:
        assert r2_map[cid]['initial_handicap'] == after_r1[cid]

    # Race3 should seed from revised handicaps of race2
    r3_map = {e['competitor_id']: e for e in r3['entrants']}
    for cid, hcp in after_r2.items():
        assert r3_map[cid]['initial_handicap'] == hcp
