import importlib
import pathlib
import sys


ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def test_set_fleet_generates_competitor_code(monkeypatch):
    import app.datastore_pg as pg

    pg = importlib.reload(pg)

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
            if "SELECT id, competitor_id FROM competitors" in normalized or "SELECT id FROM competitors" in normalized:
                self._next_fetchall = []
            elif normalized.startswith("INSERT INTO competitors") and "RETURNING" in normalized:
                comp_code = params[0]
                self._next_fetchone = {"id": 101, "competitor_id": comp_code}
            else:
                self._next_fetchone = None

        def fetchall(self):
            return self._next_fetchall

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

    payload = {
        "competitors": [
            {
                "competitor_id": None,
                "sailor_name": "Charlie",
                "boat_name": "Clipper",
                "sail_no": "H34",
                "starting_handicap_s_per_hr": 95,
                "current_handicap_s_per_hr": 95,
            }
        ]
    }

    result = pg.set_fleet(payload)

    assert result["competitors"], "Expected competitor to be returned from datastore"
    assert result["competitors"][0]["competitor_id"] == 101

    inserts = [
        (sql, params)
        for (sql, params) in recorded
        if isinstance(sql, str) and sql.strip().startswith("INSERT INTO competitors")
    ]
    assert inserts, "Expected INSERT into competitors to be executed"

    insert_sql, insert_params = inserts[0]
    assert "competitor_id" in insert_sql, "competitor_id column should be part of INSERT"

    code_param = insert_params[0]
    assert code_param is not None and code_param.startswith("C_"), "Generated code must be non-null with C_ prefix"
    assert len(code_param) <= 20, "Generated competitor_id should fit column constraint"
