import pathlib
import sys

import pytest

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2]))

from app import create_app


@pytest.mark.usefixtures("memory_store")
def test_logging_new_race_counts_emitted(monkeypatch, caplog, memory_store):
    # Seed minimal dataset with one season/series
    memory_store["fleet"] = {"competitors": []}
    memory_store["seasons"] = [
        {
            "year": 2025,
            "series": [
                {"series_id": "SER_2025_Test", "name": "Test", "season": 2025, "races": []}
            ],
        }
    ]
    monkeypatch.setenv("RECALC_ON_STARTUP", "0")
    app = create_app()
    app.config.update({"TESTING": True})
    with app.test_client() as client:
        caplog.set_level("DEBUG")
        res = client.post(
            "/api/races/__new__",
            json={
                "series_id": "SER_2025_Test",
                "new_series_name": "",
                "date": "2030-01-02",
                "start_time": "00:00:00",
                "finish_times": [],
            },
        )
        assert res.status_code == 200
    messages = [r.getMessage() for r in caplog.records]
    assert any(m.startswith("save_payload_counts new_race pre=") for m in messages)


@pytest.mark.usefixtures("memory_store")
def test_logging_edit_race_counts_emitted(monkeypatch, caplog, memory_store):
    # Seed dataset with one series and two races (no renumber on simple edit)
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
                            "competitors": [],
                            "race_no": 1,
                        },
                        {
                            "race_id": "RACE_2025-01-08_Test_2",
                            "series_id": "SER_2025_Test",
                            "name": "SER_2025_Test_2",
                            "date": "2025-01-08",
                            "start_time": "00:00:00",
                            "competitors": [],
                            "race_no": 2,
                        },
                    ],
                }
            ],
        }
    ]
    monkeypatch.setenv("RECALC_ON_STARTUP", "0")
    app = create_app()
    app.config.update({"TESTING": True})
    with app.test_client() as client:
        caplog.set_level("DEBUG")
        rid = "RACE_2025-01-08_Test_2"
        res = client.post(
            f"/api/races/{rid}",
            json={"finish_times": [{"competitor_id": 1, "finish_time": "00:33:00"}]},
        )
        assert res.status_code == 200
    messages = [r.getMessage() for r in caplog.records]
    # Expect the edit_race log with renum counts
    assert any(m.startswith("save_payload_counts edit_race pre=") and "renum_target=" in m and "renum_source=" in m for m in messages)

