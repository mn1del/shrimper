import pathlib
import sys

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2]))

from app import create_app


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
