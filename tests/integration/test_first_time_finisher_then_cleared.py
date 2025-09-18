import pathlib
import sys

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2]))

from app import create_app
from app import datastore as ds


def test_first_time_finisher_then_cleared(monkeypatch, memory_store):
    # Fleet with two competitors; race initially has only C1 as finisher
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
                                {"competitor_id": 1, "finish_time": "00:30:00"}
                            ],
                            "race_no": 1,
                        }
                    ],
                }
            ],
        }
    ]

    app = create_app()
    app.config.update({"TESTING": True})
    with app.test_client() as client:
        rid = "RACE_2025-01-01_Test_1"
        # Add first-time finisher C2
        res1 = client.post(
            f"/api/races/{rid}",
            json={"finish_times": [{"competitor_id": 2, "finish_time": "00:35:00"}]},
        )
        assert res1.status_code == 200
        # Persist fleet back because update_race only saves seasons in tests
        client.post(
            "/api/fleet",
            json={
                "competitors": [
                    {"competitor_id": 1, "sailor_name": "A", "boat_name": "A", "sail_no": "1", "starting_handicap_s_per_hr": 100},
                    {"competitor_id": 2, "sailor_name": "B", "boat_name": "B", "sail_no": "2", "starting_handicap_s_per_hr": 100},
                ]
            },
        )
        # After add, page should show 2 finishers
        res_page = client.get(f"/series/SER_2025_Test?race_id={rid}")
        html = res_page.get_data(as_text=True)
        assert "Number of Finishers: 2" in html
        # Verify entrant persisted
        _season, _series, race = ds.find_race(rid)
        ids = [e.get("competitor_id") for e in race.get("competitors", [])]
        assert 2 in ids

        # Now clear finish for C2
        res2 = client.post(
            f"/api/races/{rid}",
            json={"finish_times": [{"competitor_id": 2, "finish_time": ""}]},
        )
        assert res2.status_code == 200
        # Persist fleet again to ensure snapshot seed path remains available
        client.post(
            "/api/fleet",
            json={
                "competitors": [
                    {"competitor_id": 1, "sailor_name": "A", "boat_name": "A", "sail_no": "1", "starting_handicap_s_per_hr": 100},
                    {"competitor_id": 2, "sailor_name": "B", "boat_name": "B", "sail_no": "2", "starting_handicap_s_per_hr": 100},
                ]
            },
        )
        # After clear, page should show 1 finisher
        res_page2 = client.get(f"/series/SER_2025_Test?race_id={rid}")
        html2 = res_page2.get_data(as_text=True)
        assert "Number of Finishers: 1" in html2
        # Entrant should be pruned (no row for C2)
        _season2, _series2, race2 = ds.find_race(rid)
        ids2 = [e.get("competitor_id") for e in race2.get("competitors", [])]
        assert 2 not in ids2
