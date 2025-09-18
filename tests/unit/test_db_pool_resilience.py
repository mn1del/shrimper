import importlib


def test_pool_checkout_retries_on_stale_connection(monkeypatch):
    import app.datastore_pg as pg
    pg = importlib.reload(pg)

    # Fake cursor/connection/pool to simulate first checkout failure then success
    class BadCursor:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, sql, params=None):  # pragma: no cover - exercised via _get_conn
            from psycopg2 import OperationalError

            raise OperationalError("SSL connection has been closed unexpectedly")

    class GoodCursor:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, sql, params=None):
            return None

    class BadConn:
        autocommit = False
        closed = 0
        status = 0

        def cursor(self, cursor_factory=None):
            return BadCursor()

        def rollback(self):
            pass

        def close(self):
            self.closed = 1

    class GoodConn:
        autocommit = False
        closed = 0
        status = 0

        def cursor(self, cursor_factory=None):
            return GoodCursor()

        def rollback(self):
            pass

        def close(self):
            self.closed = 1

    class FakePool:
        def __init__(self):
            self.calls_get = 0
            self.calls_put = []

        def getconn(self):
            self.calls_get += 1
            if self.calls_get == 1:
                return BadConn()
            return GoodConn()

        def putconn(self, conn, close=False):
            self.calls_put.append((conn, close))
            if close:
                try:
                    conn.close()
                except Exception:
                    pass

    pool = FakePool()
    monkeypatch.setattr(pg, "_POOL", pool)

    with pg._get_conn() as conn:
        # Should have replaced the bad connection and yielded a healthy one
        assert isinstance(conn, GoodConn)

    # Expect two getconn() calls: 1 bad (discarded), 1 good
    assert pool.calls_get >= 2
    # Expect the bad connection to be returned with close=True at least once
    assert any(close for (_c, close) in pool.calls_put)

