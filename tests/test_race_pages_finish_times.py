import pathlib
import sys

import pytest

sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))
from app import create_app


@pytest.fixture()
def client(memory_store):
    # Seed a dataset with two races, some finish times recorded
    memory_store["fleet"] = {
        "competitors": [
            {
                "competitor_id": "C1",
                "sailor_name": "Alice",
                "boat_name": "Boaty",
                "sail_no": "1",
                "starting_handicap_s_per_hr": 100,
                "current_handicap_s_per_hr": 100,
            },
            {
                "competitor_id": "C2",
                "sailor_name": "Bob",
                "boat_name": "Crafty",
                "sail_no": "2",
                "starting_handicap_s_per_hr": 100,
                "current_handicap_s_per_hr": 100,
            },
            {
                "competitor_id": "C3",
                "sailor_name": "Cara",
                "boat_name": "Dinghy",
                "sail_no": "3",
                "starting_handicap_s_per_hr": 100,
                "current_handicap_s_per_hr": 100,
            },
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
                                {"competitor_id": "C1", "finish_time": "00:30:00"},
                                {"competitor_id": "C2", "finish_time": "00:31:00"},
                                {"competitor_id": "C3"},
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
                                {"competitor_id": "C1"},
                                {"competitor_id": "C2", "finish_time": "00:32:00"},
                            ],
                            "race_no": 2,
                        },
                    ],
                }
            ],
        }
    ]

    app = create_app()
    app.config.update({"TESTING": True})
    with app.test_client() as c:
        yield c


def test_each_race_page_displays_finish_times(client):
    races = [
        ("RACE_2025-01-01_Test_1", {"00:30:00", "00:31:00"}),
        ("RACE_2025-01-08_Test_2", {"00:32:00"}),
    ]
    for rid, expected_times in races:
        res = client.get(f"/series/SER_2025_Test?race_id={rid}")
        assert res.status_code == 200
        html = res.get_data(as_text=True)
        # Finishers count equals number of expected times
        assert f"Number of Finishers: {len(expected_times)}" in html
        # Each recorded finish time should appear in the race page
        for t in expected_times:
            assert f'value="{t}"' in html

