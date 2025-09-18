import copy
import pathlib
import sys

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2]))
from app import create_app


def test_update_race_prunes_unaffected(monkeypatch, memory_store):
    # Seed a season with two races, each having competitors
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

    # Capture payload that update_race sends to save_data, while merging to avoid
    # clobbering memory_store entrants for unaffected races in this test env.
    from app import routes as routes_mod

    captured = []
    orig_save = routes_mod.save_data

    def merge_and_save(data):
        # Capture the outgoing payload for assertions
        captured.append(copy.deepcopy(data))
        # Merge seasons into the in-memory store to preserve omitted competitors
        merged = copy.deepcopy(memory_store)
        if data and "seasons" in data:
            # Build map for quick lookup
            existing = {}
            for season in merged.get("seasons", []) or []:
                for series in season.get("series", []) or []:
                    for race in series.get("races", []) or []:
                        existing[race.get("race_id")] = race
            new_seasons = []
            for season in (data.get("seasons") or []):
                s_copy = {k: v for k, v in season.items() if k != "series"}
                ser_out = []
                for series in (season.get("series") or []):
                    se_copy = {k: v for k, v in series.items() if k != "races"}
                    races_out = []
                    for race in (series.get("races") or []):
                        r_copy = dict(race)
                        rid = r_copy.get("race_id")
                        if "competitors" not in r_copy and rid in existing:
                            # Preserve existing entrants when omitted in payload
                            r_copy["competitors"] = copy.deepcopy(existing[rid].get("competitors", []))
                        races_out.append(r_copy)
                    se_copy["races"] = races_out
                    ser_out.append(se_copy)
                s_copy["series"] = ser_out
                new_seasons.append(s_copy)
            merged["seasons"] = new_seasons
        return orig_save(merged)

    monkeypatch.setattr(routes_mod, "save_data", merge_and_save)

    app = create_app()
    app.config.update({"TESTING": True})
    with app.test_client() as client:
        rid = "RACE_2025-01-08_Test_2"
        res = client.post(
            f"/api/races/{rid}",
            json={
                "finish_times": [
                    {"competitor_id": 1, "finish_time": "00:33:00"}
                ]
            },
        )
        assert res.status_code == 200

    # Validate payload pruning: only the edited race should include 'competitors'
    assert captured, "Expected save_data to be called"
    payload = captured[-1]
    assert "seasons" in payload
    seen_with_competitors = []
    seen_without_competitors = []
    for season in payload.get("seasons", []):
        for series in season.get("series", []) or []:
            for race in series.get("races", []) or []:
                if "competitors" in race:
                    seen_with_competitors.append(race.get("race_id"))
                else:
                    seen_without_competitors.append(race.get("race_id"))

    assert seen_with_competitors == ["RACE_2025-01-08_Test_2"], (
        "Only the edited race should include entrants in payload"
    )
    assert "RACE_2025-01-01_Test_1" in seen_without_competitors
