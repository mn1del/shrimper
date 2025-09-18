import copy
import pathlib
import sys

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2]))

from app import create_app


def _race_ids(tree):
    ids = []
    for s in tree.get("seasons", []):
        for se in s.get("series", []) or []:
            for r in se.get("races", []) or []:
                ids.append(r.get("race_id"))
    return [i for i in ids if i]


def test_no_unintended_deletions_on_finish_edit(monkeypatch, memory_store):
    # Build season with three races, editing only finish_time should not drop any race
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
                        {"race_id": "RACE_2025-01-01_Test_1", "series_id": "SER_2025_Test", "name": "SER_2025_Test_1", "date": "2025-01-01", "start_time": "00:00:00", "competitors": [], "race_no": 1},
                        {"race_id": "RACE_2025-01-08_Test_2", "series_id": "SER_2025_Test", "name": "SER_2025_Test_2", "date": "2025-01-08", "start_time": "00:00:00", "competitors": [], "race_no": 2},
                        {"race_id": "RACE_2025-01-15_Test_3", "series_id": "SER_2025_Test", "name": "SER_2025_Test_3", "date": "2025-01-15", "start_time": "00:00:00", "competitors": [], "race_no": 3},
                    ],
                }
            ],
        }
    ]

    original_ids = _race_ids(memory_store)
    app = create_app()
    app.config.update({"TESTING": True})
    with app.test_client() as client:
        # Edit one race: add a finisher (does not change date or series => no renumber)
        rid = "RACE_2025-01-08_Test_2"
        res = client.post(
            f"/api/races/{rid}",
            json={"finish_times": [{"competitor_id": 1, "finish_time": "00:33:00"}]},
        )
        assert res.status_code == 200

    # Ensure after save the memory_store still has all original ids
    after_ids = _race_ids(memory_store)
    assert set(after_ids) == set(original_ids)

