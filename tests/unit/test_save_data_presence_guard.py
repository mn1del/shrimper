import importlib


def test_save_data_rewrites_only_when_competitors_present(monkeypatch):
    import app.datastore_pg as pg
    # Reload module to restore original save_data (autouse fixture patches it)
    pg = importlib.reload(pg)

    recorded = []

    class FakeCursor:
        def __init__(self, rec):
            self.rec = rec

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        # Record all SQL statements with parameters
        def execute(self, sql, params=None):
            self.rec.append((sql, params))

        def fetchone(self):
            return None

        def fetchall(self):
            return []

    class FakeConn:
        def __init__(self, rec):
            self.rec = rec

        def cursor(self, cursor_factory=None):
            return FakeCursor(self.rec)

        def commit(self):
            pass

    # Context manager yielding our fake connection
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

    # Build payload with two races in one series: only one provides 'competitors'
    race_a = {
        "race_id": "RACE_A",
        "series_id": "SER_2025_S",
        "name": "SER_2025_S_1",
        "date": "2025-06-01",
        "start_time": "00:00:00",
        "race_no": 1,
        "competitors": [
            {
                "competitor_id": 1,
                "initial_handicap": 100,
                "finish_time": "01:00:00",
                "handicap_override": None,
            }
        ],
    }
    race_b = {
        "race_id": "RACE_B",
        "series_id": "SER_2025_S",
        "name": "SER_2025_S_2",
        "date": "2025-06-02",
        "start_time": "00:00:00",
        "race_no": 2,
        # NOTE: intentionally no 'competitors' key
    }
    data = {
        "seasons": [
            {
                "year": 2025,
                "series": [
                    {
                        "series_id": "SER_2025_S",
                        "name": "S",
                        "races": [race_a, race_b],
                    }
                ],
            }
        ]
    }

    # Invoke real save_data against fake connection
    pg.save_data(data)

    # Extract race_results per-race deletions and inserts from recorded SQL
    deletes = [
        params[0]
        for (sql, params) in recorded
        if isinstance(sql, str)
        and "DELETE FROM race_results WHERE race_id = %s" in sql
        and params is not None
    ]
    inserts = [
        params[0]
        for (sql, params) in recorded
        if isinstance(sql, str)
        and sql.strip().startswith("INSERT INTO race_results")
        and params is not None
    ]

    # Expect entrants DML only for RACE_A (which had 'competitors')
    assert "RACE_A" in deletes, "Expected entrants delete for RACE_A"
    assert "RACE_A" in inserts, "Expected entrants insert for RACE_A"

    assert "RACE_B" not in deletes, "Did not expect entrants delete for RACE_B"
    assert "RACE_B" not in inserts, "Did not expect entrants insert for RACE_B"

