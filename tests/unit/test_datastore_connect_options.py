import importlib


def test_init_pool_passes_keepalive_kwargs(monkeypatch):
    import app.datastore_pg as pg
    pg = importlib.reload(pg)

    # Ensure DATABASE_URL present
    monkeypatch.setenv("DATABASE_URL", "postgresql://user:pass@localhost/db")
    # Keepalive + timeout envs
    monkeypatch.setenv("DB_CONNECT_TIMEOUT", "7")
    monkeypatch.setenv("DB_KEEPALIVES", "1")
    monkeypatch.setenv("DB_KEEPALIVES_IDLE", "30")
    monkeypatch.setenv("DB_KEEPALIVES_INTERVAL", "10")
    monkeypatch.setenv("DB_KEEPALIVES_COUNT", "3")

    captured = {}

    class FakePool:
        def __init__(self, minconn, maxconn, dsn=None, **kwargs):  # type: ignore[no-redef]
            captured["minconn"] = minconn
            captured["maxconn"] = maxconn
            captured["dsn"] = dsn
            captured["kwargs"] = kwargs

    # Reset and patch pool factory
    monkeypatch.setattr(pg, "_POOL", None)
    monkeypatch.setattr(pg.pg_pool, "ThreadedConnectionPool", FakePool)

    # Call
    pg.init_pool(minconn=2, maxconn=5)

    # Assert kwargs propagated
    assert captured["dsn"].startswith("postgresql://"), "Expected DSN passed to pool"
    kw = captured["kwargs"]
    assert kw.get("connect_timeout") == 7
    assert kw.get("keepalives") == 1
    assert kw.get("keepalives_idle") == 30
    assert kw.get("keepalives_interval") == 10
    assert kw.get("keepalives_count") == 3


def test_direct_connect_uses_keepalive_kwargs(monkeypatch):
    import app.datastore_pg as pg
    pg = importlib.reload(pg)

    # Env setup
    monkeypatch.setenv("DATABASE_URL", "postgresql://user:pass@localhost/db")
    # Use different values to prove they are read
    monkeypatch.setenv("DB_CONNECT_TIMEOUT", "12")
    monkeypatch.setenv("DB_KEEPALIVES", "1")
    monkeypatch.setenv("DB_KEEPALIVES_IDLE", "111")
    monkeypatch.setenv("DB_KEEPALIVES_INTERVAL", "22")
    monkeypatch.setenv("DB_KEEPALIVES_COUNT", "5")

    captured = {}

    class FakeConn:
        autocommit = False

        def close(self):
            pass

    def fake_connect(dsn=None, **kwargs):
        captured["dsn"] = dsn
        captured["kwargs"] = kwargs
        return FakeConn()

    # Ensure no pool path
    monkeypatch.setattr(pg, "_POOL", None)
    monkeypatch.setattr(pg.psycopg2, "connect", fake_connect)

    # Open and close a connection context
    with pg._get_conn() as _conn:
        pass

    # Assert kwargs propagated to direct connect
    assert captured["dsn"].startswith("postgresql://")
    kw = captured["kwargs"]
    assert kw.get("connect_timeout") == 12
    assert kw.get("keepalives") == 1
    assert kw.get("keepalives_idle") == 111
    assert kw.get("keepalives_interval") == 22
    assert kw.get("keepalives_count") == 5

