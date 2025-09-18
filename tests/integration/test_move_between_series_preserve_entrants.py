import copy
import pathlib
import sys

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2]))

from app import create_app
from app import datastore as ds


def test_move_between_series_preserve_entrants(monkeypatch, memory_store):
    # Series A and B each have two races; move A2 into B at a mid date causing renumber in B
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
                    "series_id": "SER_2025_A",
                    "name": "A",
                    "season": 2025,
                    "races": [
                        {
                            "race_id": "RACE_2025-01-01_A_1",
                            "series_id": "SER_2025_A",
                            "name": "SER_2025_A_1",
                            "date": "2025-01-01",
                            "start_time": "00:00:00",
                            "competitors": [
                                {"competitor_id": 1, "finish_time": "00:30:00"},
                            ],
                            "race_no": 1,
                        },
                        {
                            "race_id": "RACE_2025-01-15_A_2",
                            "series_id": "SER_2025_A",
                            "name": "SER_2025_A_2",
                            "date": "2025-01-15",
                            "start_time": "00:00:00",
                            "competitors": [
                                {"competitor_id": 2, "finish_time": "00:35:00"},
                            ],
                            "race_no": 2,
                        },
                    ],
                },
                {
                    "series_id": "SER_2025_B",
                    "name": "B",
                    "season": 2025,
                    "races": [
                        {
                            "race_id": "RACE_2025-01-05_B_1",
                            "series_id": "SER_2025_B",
                            "name": "SER_2025_B_1",
                            "date": "2025-01-05",
                            "start_time": "00:00:00",
                            "competitors": [
                                {"competitor_id": 1, "finish_time": "00:31:00"},
                            ],
                            "race_no": 1,
                        },
                        {
                            "race_id": "RACE_2025-01-20_B_2",
                            "series_id": "SER_2025_B",
                            "name": "SER_2025_B_2",
                            "date": "2025-01-20",
                            "start_time": "00:00:00",
                            "competitors": [
                                {"competitor_id": 2, "finish_time": "00:40:00"},
                            ],
                            "race_no": 2,
                        },
                    ],
                },
            ],
        }
    ]

    # Simulate moving A2 to series B at date 2025-01-10; compute expected renumber results
    sim = copy.deepcopy(memory_store)
    _season, series_a = ds.find_series("SER_2025_A", data=sim)
    _season, series_b = ds.find_series("SER_2025_B", data=sim)
    moved = None
    for i, r in enumerate(list(series_a.get("races", []))):
        if r.get("race_id") == "RACE_2025-01-15_A_2":
            moved = series_a["races"].pop(i)
            break
    assert moved is not None
    moved["series_id"] = "SER_2025_B"
    moved["name"] = "SER_2025_B_?"  # placeholder; renumber will set
    moved["date"] = "2025-01-10"
    series_b["races"].append(moved)

    mapping_b = ds.renumber_races(series_b)
    mapping_a = ds.renumber_races(series_a)
    expected_new_ids = {str(v) for (k, v) in mapping_b.items() if v and v != k} | {
        str(v) for (k, v) in mapping_a.items() if v and v != k
    }
    assert len(expected_new_ids) >= 2

    from app import routes as routes_mod
    captured = []
    orig_save = routes_mod.save_data

    def wrap_save(data):
        captured.append(copy.deepcopy(data))
        return orig_save(data)

    monkeypatch.setattr(routes_mod, "save_data", wrap_save)

    app = create_app()
    app.config.update({"TESTING": True})
    with app.test_client() as client:
        rid = "RACE_2025-01-15_A_2"
        res = client.post(
            f"/api/races/{rid}",
            json={"series_id": "SER_2025_B", "date": "2025-01-10"},
        )
        assert res.status_code == 200

    assert captured
    payload = captured[-1]
    seen_with_competitors = set()
    for season in payload.get("seasons", []):
        for series in season.get("series", []) or []:
            for race in series.get("races", []) or []:
                if "competitors" in race:
                    seen_with_competitors.add(race.get("race_id"))

    assert expected_new_ids.issubset(seen_with_competitors), (
        f"Expected entrants included for renumber/move races: {expected_new_ids}, got {seen_with_competitors}"
    )

