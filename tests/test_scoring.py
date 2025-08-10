import math

from app.scoring import adjusted_time, calculate_race_results, compute_league_standings


def test_adjusted_time_and_rank():
    entries = [
        {"sailor": "A", "boat": "B", "sail_number": 1, "start": 0, "finish": 3600, "initial_handicap": 360},
        {"sailor": "C", "boat": "D", "sail_number": 2, "start": 0, "finish": 3720, "initial_handicap": 300},
    ]
    results = calculate_race_results(entries)
    assert results[0]["sailor"] == "A"
    assert math.isclose(results[0]["allowance_seconds"], 360.0)
    assert math.isclose(results[0]["adjusted_time_seconds"], 3240.0)
    assert results[0]["handicap_position"] == 1
    assert results[0]["actual_delta"] == 0
    assert results[0]["revised_handicap"] == 360


def test_handicap_adjustment_with_fleet_scaling():
    entries = []
    for i in range(5):
        entries.append({
            "sailor": f"S{i+1}",
            "boat": "",
            "sail_number": i + 1,
            "start": 0,
            "finish": 3600 + i * 40,
            "initial_handicap": 300,
        })
    results = calculate_race_results(entries)
    assert len(results) == 5
    assert results[0]["actual_delta"] == -18
    assert results[1]["actual_delta"] == -12
    assert results[2]["actual_delta"] == -6
    assert results[3]["actual_delta"] == 0
    assert results[4]["actual_delta"] == 6


def test_high_positions_capped_and_full_scaling():
    entries = []
    for i in range(12):
        entries.append(
            {
                "sailor": f"H{i+1}",
                "boat": "",
                "sail_number": i + 1,
                "start": 0,
                "finish": 3600 + i * 10,
                "initial_handicap": 300,
            }
        )
    results = calculate_race_results(entries)
    # 7th place should use full scaling factor of 1.0
    assert results[6]["handicap_position"] == 7
    assert results[6]["actual_delta"] == 30
    # Positions beyond defined deltas should cap at the final value (60)
    assert results[10]["handicap_position"] == 11
    assert results[10]["full_delta"] == 60
    assert results[10]["actual_delta"] == 60


def test_league_standings():
    race1_entries = [
        {"sailor": "A", "boat": "", "sail_number": 1, "start": 0, "finish": 3600, "initial_handicap": 300},
        {"sailor": "B", "boat": "", "sail_number": 2, "start": 0, "finish": 3660, "initial_handicap": 300},
        {"sailor": "C", "boat": "", "sail_number": 3, "start": 0, "finish": 3720, "initial_handicap": 300},
    ]
    race2_entries = [
        {"sailor": "A", "boat": "", "sail_number": 1, "start": 0, "finish": 3700, "initial_handicap": 300},
        {"sailor": "B", "boat": "", "sail_number": 2, "start": 0, "finish": 3600, "initial_handicap": 300},
        {"sailor": "C", "boat": "", "sail_number": 3, "start": 0, "finish": 3750, "initial_handicap": 300},
    ]
    race1 = calculate_race_results(race1_entries)
    race2 = calculate_race_results(race2_entries)
    standings = compute_league_standings([race1, race2])
    totals = {s["sailor"]: s["total_points"] for s in standings}
    assert totals["C"] == 6
    assert totals["A"] == 3
    assert totals["B"] == 3
    assert [s["sailor"] for s in standings] == ["A", "B", "C"]
