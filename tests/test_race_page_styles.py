import pathlib
import sys

import pytest

sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))
from app import create_app


@pytest.fixture()
def client(memory_store):
    # Minimal dataset with a single race having finishers
    memory_store["fleet"] = {
        "competitors": [
            {"competitor_id": 1, "sailor_name": "A", "boat_name": "X", "sail_no": "1", "starting_handicap_s_per_hr": 100, "current_handicap_s_per_hr": 100},
            {"competitor_id": 2, "sailor_name": "B", "boat_name": "Y", "sail_no": "2", "starting_handicap_s_per_hr": 100, "current_handicap_s_per_hr": 100},
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
    with app.test_client() as c:
        yield c


def test_style_hooks_present(client):
    rid = "RACE_2025-01-01_Test_1"
    res = client.get(f"/series/SER_2025_Test?race_id={rid}")
    assert res.status_code == 200
    html = res.get_data(as_text=True)
    # Finish Time cells are present
    assert 'class="finish-time-cell"' in html
    # Post-finish columns use the new class
    assert 'post-finish-col' in html

