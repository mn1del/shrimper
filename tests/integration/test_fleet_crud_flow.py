import pathlib
import sys

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2]))

from app import create_app
from app.datastore import get_fleet as ds_get_fleet
from app.datastore import find_race as ds_find_race


def test_add_competitor_end_to_end(memory_store):
    # Seed initial state with two competitors
    memory_store["fleet"] = {
        "competitors": [
            {
                "competitor_id": 1,
                "sailor_name": "Alice",
                "boat_name": "Boaty",
                "sail_no": "1",
                "starting_handicap_s_per_hr": 100,
                "current_handicap_s_per_hr": 100,
            },
            {
                "competitor_id": 2,
                "sailor_name": "Bob",
                "boat_name": "Crafty",
                "sail_no": "2",
                "starting_handicap_s_per_hr": 110,
                "current_handicap_s_per_hr": 110,
            },
        ]
    }

    app = create_app()
    app.config.update({"TESTING": True})

    with app.test_client() as client:
        payload = {
            "competitors": [
                {
                    "competitor_id": 1,
                    "sailor_name": "Alice",
                    "boat_name": "Boaty",
                    "sail_no": "1",
                    "starting_handicap_s_per_hr": 100,
                },
                {
                    "competitor_id": 2,
                    "sailor_name": "Bob",
                    "boat_name": "Crafty",
                    "sail_no": "2",
                    "starting_handicap_s_per_hr": 110,
                },
                {
                    "competitor_id": None,
                    "sailor_name": "Charlie",
                    "boat_name": "Clipper",
                    "sail_no": "3",
                    "starting_handicap_s_per_hr": 95,
                },
            ]
        }

        res = client.post("/api/fleet", json=payload)
        assert res.status_code == 200
        body = res.get_json()
        assert body["added"] == 1
        assert any(comp["sailor_name"] == "Charlie" for comp in memory_store["fleet"]["competitors"])

        # Remove Bob leaving only Alice
        delete_payload = {
            "competitors": [
                {
                    "competitor_id": 1,
                    "sailor_name": "Alice",
                    "boat_name": "Boaty",
                    "sail_no": "1",
                    "starting_handicap_s_per_hr": 100,
                }
            ]
        }

        res_delete = client.post("/api/fleet", json=delete_payload)
        assert res_delete.status_code == 200
        assert len(memory_store["fleet"]["competitors"]) == 1
        assert memory_store["fleet"]["competitors"][0]["competitor_id"] == 1


def test_new_competitor_visible_in_race_sheet(memory_store):
    # Fleet initially has Alice only; Bob appears in race results but should be absent until added
    memory_store["fleet"] = {
        "competitors": [
            {
                "competitor_id": 1,
                "sailor_name": "Alice",
                "boat_name": "Boaty",
                "sail_no": "A1",
                "starting_handicap_s_per_hr": 100,
                "current_handicap_s_per_hr": 100,
            }
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
                        }
                    ],
                }
            ],
        }
    ]

    app = create_app()
    app.config.update({"TESTING": True})

    with app.test_client() as client:
        # Initially Bob (id=2) has no fleet entry so race sheet should render him as unknown
        res_initial = client.get("/series/SER_2025_Test?race_id=RACE_2025-01-01_Test_1")
        html_initial = res_initial.get_data(as_text=True)
        assert "Bob" not in html_initial

        # Add Bob via fleet API
        payload = {
            "competitors": [
                {
                    "competitor_id": 1,
                    "sailor_name": "Alice",
                    "boat_name": "Boaty",
                    "sail_no": "A1",
                    "starting_handicap_s_per_hr": 100,
                },
                {
                    "competitor_id": 2,
                    "sailor_name": "Bob",
                    "boat_name": "Breeze",
                    "sail_no": "B2",
                    "starting_handicap_s_per_hr": 105,
                },
            ]
        }
        res_add = client.post("/api/fleet", json=payload)
        assert res_add.status_code == 200
        body = res_add.get_json()
        assert body["added"] == 0
        assert body["updated"] == 1

        # Bob should now appear in fleet datastore
        fleet_after = ds_get_fleet(memory_store)
        assert any(c["sailor_name"] == "Bob" for c in fleet_after["competitors"])

        # Race sheet should now display Bob with his name
        res_after = client.get("/series/SER_2025_Test?race_id=RACE_2025-01-01_Test_1")
        html_after = res_after.get_data(as_text=True)
        assert "Bob" in html_after
