import pathlib
import sys

import pytest

sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))
from app import routes
from app.scoring import calculate_race_results


@pytest.mark.usefixtures("patch_datastore")
def test_recalculate_handicaps_uses_revised(memory_store):
    # Seed fleet and races in memory
    memory_store["fleet"] = {"competitors": [
        {"competitor_id": i, "sail_no": str(i), "sailor_name": f"S{i}", "boat_name": "", "starting_handicap_s_per_hr": 100, "current_handicap_s_per_hr": 100}
        for i in range(1, 5)
    ]}
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
                            "name": "R1",
                            "date": "2025-01-01",
                            "start_time": "00:00:00",
                            "competitors": [
                                {"competitor_id": 1, "finish_time": "00:30:00"},
                                {"competitor_id": 2, "finish_time": "00:31:00"},
                                {"competitor_id": 3, "finish_time": "00:32:00"},
                                {"competitor_id": 4, "finish_time": "00:33:00"},
                            ],
                            "race_no": 1,
                        },
                        {
                            "race_id": "RACE_2025-01-08_Test_2",
                            "series_id": "SER_2025_Test",
                            "name": "R2",
                            "date": "2025-01-08",
                            "start_time": "00:00:00",
                            "competitors": [
                                {"competitor_id": 1},
                                {"competitor_id": 2},
                                {"competitor_id": 3},
                                {"competitor_id": 4},
                            ],
                            "race_no": 2,
                        },
                    ],
                }
            ],
        }
    ]

    routes.recalculate_handicaps()

    # Compute expected from first race
    r1 = memory_store["seasons"][0]["series"][0]["races"][0]
    start_sec = routes._parse_hms(r1["start_time"]) or 0
    entries = [
        {
            "competitor_id": ent["competitor_id"],
            "start": start_sec,
            "finish": routes._parse_hms(ent["finish_time"]),
            "initial_handicap": 100,
        }
        for ent in r1["competitors"]
    ]
    expected = calculate_race_results(entries)
    expected_map = {res["competitor_id"]: res["revised_handicap"] for res in expected}

    # Check r1 assigned initial=100 and r2 seeded from expected
    assert all(e.get("initial_handicap") == 100 for e in r1["competitors"])
    r2 = memory_store["seasons"][0]["series"][0]["races"][1]
    r2_map = {e["competitor_id"]: e.get("initial_handicap") for e in r2["competitors"]}
    for cid, h in expected_map.items():
        assert r2_map[cid] == h

    # Fleet current handicaps updated
    cur_map = {c["competitor_id"]: c.get("current_handicap_s_per_hr") for c in memory_store["fleet"]["competitors"]}
    for cid, h in expected_map.items():
        assert cur_map[cid] == h


@pytest.mark.usefixtures("patch_datastore")
def test_handicap_override(memory_store):
    memory_store["fleet"] = {"competitors": [
        {"competitor_id": i, "sail_no": str(i), "sailor_name": f"S{i}", "boat_name": "", "starting_handicap_s_per_hr": 100, "current_handicap_s_per_hr": 100}
        for i in range(1, 5)
    ]}
    finish_order = ["00:30:00", "00:31:00", "00:32:00", "00:33:00"]
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
                            "name": "R1",
                            "date": "2025-01-01",
                            "start_time": "00:00:00",
                            "competitors": [
                                {"competitor_id": i, "finish_time": finish_order[i - 1]} for i in range(1, 5)
                            ],
                            "race_no": 1,
                        },
                        {
                            "race_id": "RACE_2025-01-08_Test_2",
                            "series_id": "SER_2025_Test",
                            "name": "R2",
                            "date": "2025-01-08",
                            "start_time": "00:00:00",
                            "competitors": [
                                {"competitor_id": 1, "finish_time": finish_order[0], "handicap_override": 200},
                                {"competitor_id": 2, "finish_time": finish_order[1]},
                                {"competitor_id": 3, "finish_time": finish_order[2]},
                                {"competitor_id": 4, "finish_time": finish_order[3]},
                            ],
                            "race_no": 2,
                        },
                        {
                            "race_id": "RACE_2025-01-15_Test_3",
                            "series_id": "SER_2025_Test",
                            "name": "R3",
                            "date": "2025-01-15",
                            "start_time": "00:00:00",
                            "competitors": [
                                {"competitor_id": i} for i in range(1, 5)
                            ],
                            "race_no": 3,
                        },
                    ],
                }
            ],
        }
    ]

    # Expected maps
    start_sec = routes._parse_hms("00:00:00") or 0
    entries1 = [
        {"competitor_id": i, "start": start_sec, "finish": routes._parse_hms(finish_order[i - 1]), "initial_handicap": 100}
        for i in range(1, 5)
    ]
    res1 = calculate_race_results(entries1)
    after_r1 = {r["competitor_id"]: r["revised_handicap"] for r in res1}

    entries2 = []
    for i in range(1, 5):
        cid = i
        init = 200 if cid == 1 else after_r1[cid]
        entries2.append({"competitor_id": cid, "start": start_sec, "finish": routes._parse_hms(finish_order[i - 1]), "initial_handicap": init})
    res2 = calculate_race_results(entries2)
    after_r2 = {r["competitor_id"]: r["revised_handicap"] for r in res2}

    routes.recalculate_handicaps()

    r2 = memory_store["seasons"][0]["series"][0]["races"][1]
    r3 = memory_store["seasons"][0]["series"][0]["races"][2]

    r2_map = {e["competitor_id"]: e for e in r2["competitors"]}
    assert r2_map[1]["initial_handicap"] == 200
    assert r2_map[1]["handicap_override"] == 200
    for cid in [2, 3, 4]:
        assert r2_map[cid]["initial_handicap"] == after_r1[cid]

    r3_map = {e["competitor_id"]: e for e in r3["competitors"]}
    for cid, hcp in after_r2.items():
        assert r3_map[cid]["initial_handicap"] == hcp
