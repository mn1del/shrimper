import importlib
import pathlib
import sys
from typing import List, Optional


ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _run_set_fleet(monkeypatch, payload, select_rows: Optional[List[dict]] = None, returning_id: Optional[int] = None):
    import app.datastore_pg as pg

    pg = importlib.reload(pg)

    recorded: List[tuple] = []
    select_rows = select_rows or []

    class FakeCursor:
        def __init__(self, rec):
            self.rec = rec
            self._next_fetchall: List[dict] = []
            self._next_fetchone: Optional[dict] = None

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, sql, params=None):
            self.rec.append((sql, params))
            normalized = " ".join(sql.split())
            if "SELECT id, competitor_id FROM competitors" in normalized:
                self._next_fetchall = list(select_rows)
                self._next_fetchone = None
            elif normalized.startswith("INSERT INTO competitors") and "RETURNING" in normalized:
                comp_code = params[0]
                self._next_fetchall = []
                if returning_id is not None:
                    self._next_fetchone = {"id": returning_id, "competitor_id": comp_code}
                else:
                    self._next_fetchone = None
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

    result = pg.set_fleet(payload)
    return result, recorded


def test_set_fleet_generates_competitor_code(monkeypatch):
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

    result, recorded = _run_set_fleet(monkeypatch, payload, select_rows=[], returning_id=101)

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


def test_set_fleet_upserts_existing_competitor_with_code(monkeypatch):
    payload = {
        "competitors": [
            {
                "competitor_id": 7,
                "sailor_name": "Alice",
                "boat_name": "Clipper",
                "sail_no": "H34",
                "starting_handicap_s_per_hr": 95,
                "current_handicap_s_per_hr": 95,
            }
        ]
    }

    select_rows = [{"id": 7, "competitor_id": "C_H34"}]

    result, recorded = _run_set_fleet(monkeypatch, payload, select_rows=select_rows)

    assert result["competitors"], "Expected competitor returned"
    assert result["competitors"][0]["competitor_id"] == 7

    inserts = [
        (sql, params)
        for (sql, params) in recorded
        if isinstance(sql, str) and sql.strip().startswith("INSERT INTO competitors")
    ]
    assert inserts, "Expected UPSERT INSERT to run"

    insert_sql, insert_params = inserts[0]
    assert "competitor_id" in insert_sql, "Existing competitor UPSERT should include competitor_id column"
    assert insert_params[1] == "C_H34", "Existing competitor should reuse stored competitor_id"
