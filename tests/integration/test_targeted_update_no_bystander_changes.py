import pathlib
import sys

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2]))

from app import create_app
from app import datastore as ds


def test_targeted_update_no_bystander_changes(monkeypatch, memory_store):
    memory_store["fleet"] = {"competitors": [
        {"competitor_id": 1, "sailor_name": "A", "boat_name": "A", "sail_no": "1", "starting_handicap_s_per_hr": 100},
        {"competitor_id": 2, "sailor_name": "B", "boat_name": "B", "sail_no": "2", "starting_handicap_s_per_hr": 100},
    ]}
    memory_store["seasons"] = [{
        "year": 2025,
        "series": [{
            "series_id": "SER_2025_Test",
            "name": "Test",
            "season": 2025,
            "races": [
                {"race_id": "RACE_2025-01-01_Test_1", "series_id": "SER_2025_Test", "name": "SER_2025_Test_1", "date": "2025-01-01", "start_time": "00:00:00", "competitors": [], "race_no": 1},
                {"race_id": "RACE_2025-01-08_Test_2", "series_id": "SER_2025_Test", "name": "SER_2025_Test_2", "date": "2025-01-08", "start_time": "00:00:00", "competitors": [], "race_no": 2},
            ],
        }],
    }]

    monkeypatch.setenv("USE_TARGETED_SAVE", "1")

    from app import routes as routes_mod
    # Targeted functions update in-memory store
    def fake_update(rid, fields):
        _s, _se, r = ds.find_race(rid, data=memory_store)
        if not r:
            return
        for k in ("date", "start_time"):
            if k in fields:
                r[k] = fields[k]

    def fake_replace(rid, entrants):
        _s, _se, r = ds.find_race(rid, data=memory_store)
        if not r:
            return
        r["competitors"] = list(entrants or [])

    # Validate save_data is not called
    def fail_save(_):
        raise AssertionError("save_data should not be called in targeted path")

    monkeypatch.setattr(routes_mod, "ds_update_race_row", fake_update)
    monkeypatch.setattr(routes_mod, "ds_replace_race_results", fake_replace)
    monkeypatch.setattr(routes_mod, "save_data", fail_save)

    # Avoid startup recalc to keep test focused on targeted path
    monkeypatch.setenv("RECALC_ON_STARTUP", "0")
    app = create_app()
    app.config.update({"TESTING": True, "USE_TARGETED_SAVE": True})
    with app.test_client() as client:
        # Change only start_time and one finish in race 2
        rid = "RACE_2025-01-08_Test_2"
        res = client.post(
            f"/api/races/{rid}",
            json={"start_time": "00:05:00", "finish_times": [{"competitor_id": 1, "finish_time": "00:33:00"}]},
        )
        assert res.status_code == 200

    _s, _se, r1 = ds.find_race("RACE_2025-01-01_Test_1", data=memory_store)
    _s, _se, r2 = ds.find_race("RACE_2025-01-08_Test_2", data=memory_store)
    assert r1 and r2
    # Bystander race untouched
    assert r1.get("start_time") == "00:00:00"
    assert r1.get("competitors", []) == []
    # Edited race updated
    assert r2.get("start_time") == "00:05:00"
    assert any(e.get("competitor_id") == 1 for e in r2.get("competitors", []))
