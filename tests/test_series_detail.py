import json
import os
import sys
from pathlib import Path

from flask import Flask

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from app import routes


def test_finish_time_loaded(monkeypatch, tmp_path):
    # Prepare temporary fleet data
    fleet_data = {
        "competitors": [
            {
                "competitor_id": "C_45",
                "sailor_name": "NH",
                "boat_name": "Braken Two",
                "sail_no": "45",
                "current_handicap_s_per_hr": 520,
            }
        ]
    }
    (tmp_path / "fleet.json").write_text(json.dumps(fleet_data))

    # Patch DATA_DIR to temporary path containing fleet
    monkeypatch.setattr(routes, "DATA_DIR", tmp_path)

    # Fake series and race data
    def fake_find_series(series_id):
        return {"series_id": series_id, "name": "Test Series"}, [
            {"race_id": "R1", "name": "Race 1", "date": "2025-01-01"}
        ]

    def fake_find_race(race_id):
        return {
            "race_id": race_id,
            "results": [
                {"competitor_id": "C_45", "finish_time": "12:34"}
            ],
        }

    monkeypatch.setattr(routes, "_find_series", fake_find_series)
    monkeypatch.setattr(routes, "_find_race", fake_find_race)

    templates = Path(__file__).resolve().parent.parent / "app" / "templates"
    app = Flask(__name__, template_folder=str(templates))
    app.register_blueprint(routes.bp)

    with app.test_client() as client:
        resp = client.get("/series/TEST?race_id=R1")
        assert resp.status_code == 200
        html = resp.get_data(as_text=True)
        assert 'value="12:34"' in html
