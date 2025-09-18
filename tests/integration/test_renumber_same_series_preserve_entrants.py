import copy
import pathlib
import sys

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2]))

from app import create_app
from app import datastore as ds


def test_renumber_same_series_preserve_entrants(monkeypatch, memory_store):
    # Two races in one series; changing R2's date earlier causes both race ids to change
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
                    "series_id": "SER_2025_Test",
                    "name": "Test",
                    "season": 2025,
                    "races": [
                        {
                            "race_id": "RACE_2025-01-01_Test_1",
                            "series_id": "SER_2025_Test",
                            "name": "SER_2025_Test_1",
                            "date": "2025-01-01",
                            "start_time": "00:00:00",
                            "competitors": [
                                {"competitor_id": 1, "finish_time": "00:30:00"},
                                {"competitor_id": 2, "finish_time": "00:31:00"},
                            ],
                            "race_no": 1,
                        },
                        {
                            "race_id": "RACE_2025-01-08_Test_2",
                            "series_id": "SER_2025_Test",
                            "name": "SER_2025_Test_2",
                            "date": "2025-01-08",
                            "start_time": "00:00:00",
                            "competitors": [
                                {"competitor_id": 1},
                                {"competitor_id": 2, "finish_time": "00:32:00"},
                            ],
                            "race_no": 2,
                        },
                    ],
                }
            ],
        }
    ]

    # Compute expected new ids after date change by simulating renumber
    sim = copy.deepcopy(memory_store)
    _season, series = ds.find_series("SER_2025_Test", data=sim)
    # Change R2 date to be earlier than R1
    for r in series.get("races", []):
        if r.get("race_id") == "RACE_2025-01-08_Test_2":
            r["date"] = "2024-12-31"
    mapping_expected = ds.renumber_races(series)
    expected_new_ids = {str(v) for (k, v) in mapping_expected.items() if v and v != k}
    assert len(expected_new_ids) == 2  # both races ids change

    captured = []
    from app import routes as routes_mod

    orig_save = routes_mod.save_data

    def wrap_save(data):
        captured.append(copy.deepcopy(data))
        return orig_save(data)

    monkeypatch.setattr(routes_mod, "save_data", wrap_save)

    app = create_app()
    app.config.update({"TESTING": True})
    with app.test_client() as client:
        rid = "RACE_2025-01-08_Test_2"
        res = client.post(
            f"/api/races/{rid}",
            json={"date": "2024-12-31"},
        )
        assert res.status_code == 200

    assert captured, "Expected save_data to be called"
    payload = captured[-1]
    seen_with_competitors = set()
    for season in payload.get("seasons", []):
        for series in season.get("series", []) or []:
            for race in series.get("races", []) or []:
                if "competitors" in race:
                    seen_with_competitors.add(race.get("race_id"))

    assert expected_new_ids.issubset(seen_with_competitors), (
        f"Expected entrants included for renumbered races: {expected_new_ids}, got {seen_with_competitors}"
    )

