import importlib
import pytest
import pathlib
import sys

sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))
from app import create_app


@pytest.fixture()
def client(memory_store):
    # Seed a basic dataset in memory
    memory_store["fleet"] = {
        "competitors": [
            {
                "competitor_id": 1,
                "sailor_name": "Alice",
                "boat_name": "Boaty",
                "sail_no": "1",
                "starting_handicap_s_per_hr": 100,
                "current_handicap_s_per_hr": 100,
                "active": True,
                "notes": "",
            },
            {
                "competitor_id": 2,
                "sailor_name": "Bob",
                "boat_name": "Crafty",
                "sail_no": "2",
                "starting_handicap_s_per_hr": 100,
                "current_handicap_s_per_hr": 100,
                "active": True,
                "notes": "",
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

    app = create_app()
    app.config.update({"TESTING": True})
    with app.test_client() as c:
        yield c


def test_race_page_and_redirect(client):
    rid = "RACE_2025-01-01_Test_1"
    res = client.get(f"/series/SER_2025_Test?race_id={rid}")
    html = res.get_data(as_text=True)
    assert "Number of Finishers: 2" in html
    assert 'value="00:30:00"' in html
    assert ">01-01-2025 00:00:00 (Test)<" in html

    res = client.get(f"/races/{rid}", follow_redirects=False)
    assert res.status_code == 302
    assert f"/series/SER_2025_Test?race_id={rid}" in res.headers["Location"]


def test_races_page_lists_and_filters(client):
    res = client.get("/races")
    html = res.get_data(as_text=True)
    assert "08-01-2025" in html and "01-01-2025" in html

    res = client.get("/races?season=2025")
    html = res.get_data(as_text=True)
    assert "01-01-2025" in html


def test_create_edit_delete_race(client):
    # Create in new series
    res = client.post(
        "/api/races/__new__",
        json={
            "series_id": "__new__",
            "new_series_name": "New",
            "date": "2030-01-01",
            "start_time": "12:30:45",
            "finish_times": [],
        },
    )
    assert res.status_code == 200
    rid = res.get_json()["redirect"].split("race_id=")[-1]

    # Edit date (capture new id via redirect)
    res2 = client.post(f"/api/races/{rid}", json={"date": "2030-01-02"})
    assert res2.status_code == 200
    rid = res2.get_json()["redirect"].split("race_id=")[-1]

    # Delete
    res3 = client.delete(f"/api/races/{rid}")
    assert res3.status_code == 200


def test_fleet_update_and_duplicates(client):
    # Duplicate sail number
    payload = {
        "competitors": [
            {"competitor_id": 1, "sailor_name": "A", "boat_name": "A", "sail_no": "1", "starting_handicap_s_per_hr": 100},
            {"competitor_id": 2, "sailor_name": "B", "boat_name": "B", "sail_no": "1", "starting_handicap_s_per_hr": 100},
        ]
    }
    res = client.post("/api/fleet", json=payload)
    assert res.status_code == 400

    # Valid update propagates (send full fleet with unique sails)
    payload2 = {
        "competitors": [
            {"competitor_id": 1, "sailor_name": "New", "boat_name": "Boaty", "sail_no": "1", "starting_handicap_s_per_hr": 150},
            {"competitor_id": 2, "sailor_name": "Bob", "boat_name": "Crafty", "sail_no": "2", "starting_handicap_s_per_hr": 100},
        ]
    }
    res2 = client.post("/api/fleet", json=payload2)
    assert res2.status_code == 200


def test_update_fleet_add_edit_delete(client, memory_store):
    payload = {
        "competitors": [
            {
                "competitor_id": 1,
                "sailor_name": "Alice Updated",
                "boat_name": "Boaty",
                "sail_no": "1",
                "starting_handicap_s_per_hr": 120,
            },
            {
                "competitor_id": 2,
                "sailor_name": "Bob",
                "boat_name": "Crafty",
                "sail_no": "2",
                "starting_handicap_s_per_hr": 100,
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
    assert body["updated"] == 1
    assert body["removed"] == 0
    assert any(c["sailor_name"] == "Charlie" for c in memory_store["fleet"]["competitors"])
    assert any(c["sailor_name"] == "Alice Updated" for c in memory_store["fleet"]["competitors"])

    # Delete Alice by removing her entry
    payload_delete = {
        "competitors": [
            {
                "competitor_id": 2,
                "sailor_name": "Bob",
                "boat_name": "Crafty",
                "sail_no": "2",
                "starting_handicap_s_per_hr": 100,
            }
        ]
    }

    res2 = client.post("/api/fleet", json=payload_delete)
    assert res2.status_code == 200
    body2 = res2.get_json()
    assert body2["removed"] == 1
    assert len(memory_store["fleet"]["competitors"]) == 1
    assert memory_store["fleet"]["competitors"][0]["competitor_id"] == 2


def test_update_fleet_rejects_invalid_handicap(client):
    payload = {
        "competitors": [
            {
                "competitor_id": 1,
                "sailor_name": "Alice",
                "boat_name": "Boaty",
                "sail_no": "1",
                "starting_handicap_s_per_hr": "fast",
            },
            {
                "competitor_id": 2,
                "sailor_name": "Bob",
                "boat_name": "Crafty",
                "sail_no": "2",
                "starting_handicap_s_per_hr": 100,
            },
        ]
    }

    res = client.post("/api/fleet", json=payload)
    assert res.status_code == 400
    body = res.get_json()
    assert "handicap" in body["error"].lower()


def test_api_fleet_creates_competitor_with_generated_id(monkeypatch):
    import app.datastore_pg as pg

    pg = importlib.reload(pg)
    monkeypatch.setattr(pg, "init_pool", lambda *args, **kwargs: None)

    recorded = []

    class FakeCursor:
        def __init__(self, rec):
            self.rec = rec
            self._next_fetchall = []
            self._next_fetchone = None

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, sql, params=None):
            self.rec.append((sql, params))
            normalized = " ".join(sql.split())
            if normalized.startswith("SELECT id, competitor_id FROM competitors"):
                self._next_fetchall = []
                self._next_fetchone = None
            elif "INSERT INTO competitors" in normalized and "RETURNING" in normalized:
                comp_code = params[0]
                self._next_fetchall = []
                self._next_fetchone = {"id": 881, "competitor_id": comp_code}
            else:
                self._next_fetchall = []
                self._next_fetchone = None

        def fetchall(self):
            return list(self._next_fetchall)

        def fetchone(self):
            return self._next_fetchone

    class FakeConn:
        def __init__(self, rec):
            self.rec = rec

        def cursor(self, cursor_factory=None):
            return FakeCursor(self.rec)

        def commit(self):
            pass

    class _Ctx:
        def __init__(self, conn):
            self.conn = conn

        def __enter__(self):
            return self.conn

        def __exit__(self, exc_type, exc, tb):
            return False

    def fake_get_conn():
        return _Ctx(FakeConn(recorded))

    monkeypatch.setattr(pg, "_get_conn", fake_get_conn)

    import app.datastore as datastore
    datastore = importlib.reload(datastore)

    import app.routes as routes
    routes = importlib.reload(routes)

    monkeypatch.setattr(routes, "ds_get_fleet", lambda: {"competitors": []})
    monkeypatch.setattr(routes, "recalculate_handicaps", lambda: None)
    monkeypatch.setattr(routes, "_cache_clear_all", lambda: None)

    import app as app_pkg
    app_pkg = importlib.reload(app_pkg)

    monkeypatch.setenv("RECALC_ON_STARTUP", "0")

    app = app_pkg.create_app()
    app.config.update({"TESTING": True})

    with app.test_client() as client_http:
        payload = {
            "competitors": [
                {
                    "competitor_id": None,
                    "sailor_name": "W",
                    "boat_name": "Pelican",
                    "sail_no": "102",
                    "starting_handicap_s_per_hr": -118,
                    "current_handicap_s_per_hr": -28,
                }
            ]
        }
        res = client_http.post("/api/fleet", json=payload)
        assert res.status_code == 200
        body = res.get_json()
        assert body["new_competitor_ids"] == [881]
        assert body["competitors"][0]["competitor_id"] == 881

    insert_statements = [
        (sql, params)
        for (sql, params) in recorded
        if isinstance(sql, str) and sql.strip().startswith("INSERT INTO competitors")
    ]
    assert insert_statements, "Expected INSERT into competitors to occur"
    _, params = insert_statements[0]
    assert params is not None
    assert params[0] is not None and str(params[0]).startswith("C_"), "competitor_id should be generated"


def test_get_fleet_page_contains_controls(client):
    res = client.get("/fleet")
    html = res.get_data(as_text=True)
    assert 'id="addCompetitor"' in html
    assert '<th scope="col" class="text-end">Actions</th>' in html
    assert 'class="btn btn-outline-danger btn-sm delete-row"' in html


def test_settings_page_and_save(client):
    res = client.get("/settings")
    html = res.get_data(as_text=True)
    assert "handicap_delta_by_rank" in html

    res2 = client.post(
        "/api/settings",
        json={
            "handicap_delta_by_rank": [{"rank": 1, "delta_s_per_hr": -5}],
            "league_points_by_rank": [{"rank": 1, "points": 9}],
            "fleet_size_factor": [{"finishers": 1, "factor": 0.9}],
        },
    )
    assert res2.status_code == 200
