from contextlib import contextmanager
from typing import Any, Dict, List, Optional
import pathlib
import sys

import pytest
from psycopg2 import OperationalError
from psycopg2 import errors as pg_errors

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2]))
import app.datastore_pg as admin_store  # noqa: E402


class _StubCursor:
    def __init__(self, row: Optional[Dict[str, Any]] = None, rows: Optional[List[Dict[str, Any]]] = None, error: Optional[Exception] = None):
        self._row = row
        self._rows = rows or []
        self._error = error
        self.statements: List[tuple[str, tuple[Any, ...]]] = []

    def execute(self, sql: str, params: tuple[Any, ...]):
        self.statements.append((sql.strip(), params))
        if self._error:
            raise self._error

    def fetchone(self):
        return self._row

    def fetchall(self):
        return self._rows

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class _StubConn:
    def __init__(self, cursor: _StubCursor):
        self.cursor_instance = cursor
        self.committed = False
        self.rollbacked = False

    def cursor(self, cursor_factory=None):
        return self.cursor_instance

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rollbacked = True


def _patch_conn(monkeypatch, cursor: _StubCursor):
    created: List[_StubConn] = []

    @contextmanager
    def _fake_conn():
        conn = _StubConn(cursor)
        created.append(conn)
        yield conn

    monkeypatch.setattr(admin_store, "_get_conn", _fake_conn)
    return created


def test_create_admin_hashes_password_and_commits(monkeypatch):
    row = {
        "id": 1,
        "email": "admin@example.com",
        "is_superadmin": True,
        "is_active": True,
        "created_at": "2024-01-01T00:00:00Z",
        "updated_at": "2024-01-01T00:00:00Z",
        "last_login_at": None,
    }
    cursor = _StubCursor(row=row)
    conns = _patch_conn(monkeypatch, cursor)

    monkeypatch.setattr(admin_store, "generate_password_hash", lambda password: f"HASH::{password}")

    admin = admin_store.create_admin("Admin@Example.com", "secret", is_superadmin=True)

    assert admin["email"] == "admin@example.com"
    assert admin["is_superadmin"] is True
    assert "password_hash" not in admin

    assert conns[0].committed is True
    executed_sql, params = cursor.statements[0]
    assert "INSERT INTO admins" in executed_sql
    assert params[0] == "admin@example.com"
    assert params[1] == "HASH::secret"


def test_create_admin_duplicate_email_rolls_back(monkeypatch):
    error = pg_errors.UniqueViolation()
    cursor = _StubCursor(error=error)
    conns = _patch_conn(monkeypatch, cursor)
    monkeypatch.setattr(admin_store, "generate_password_hash", lambda password: f"H::{password}")

    with pytest.raises(ValueError):
        admin_store.create_admin("dup@example.com", "secret")

    assert conns[0].rollbacked is True


def test_get_admin_by_email_returns_public_dict(monkeypatch):
    row = {
        "id": 42,
        "email": "hello@example.com",
        "is_superadmin": False,
        "is_active": True,
        "created_at": "2024-01-01T00:00:00Z",
        "updated_at": "2024-01-02T00:00:00Z",
        "last_login_at": None,
    }
    cursor = _StubCursor(row=row)
    _patch_conn(monkeypatch, cursor)

    admin = admin_store.get_admin_by_email("Hello@Example.com")

    assert admin == row
    executed_sql, params = cursor.statements[0]
    assert "WHERE email = %s" in executed_sql
    assert params[0] == "hello@example.com"


def test_verify_admin_credentials(monkeypatch):
    record = {
        "id": 7,
        "email": "admin@example.com",
        "is_superadmin": False,
        "is_active": True,
        "created_at": None,
        "updated_at": None,
        "last_login_at": None,
        "password_hash": "HASHED",
    }

    monkeypatch.setattr(admin_store, "get_admin_by_email", lambda email, include_password_hash=False: dict(record))
    monkeypatch.setattr(admin_store, "check_password_hash", lambda stored, supplied: stored == "HASHED" and supplied == "secret")

    result = admin_store.verify_admin_credentials("admin@example.com", "secret")
    assert result["email"] == "admin@example.com"
    assert "password_hash" not in result

    # Wrong password -> None
    assert admin_store.verify_admin_credentials("admin@example.com", "badpass") is None


def test_create_admin_db_failure(monkeypatch):
    @contextmanager
    def _failing_conn():
        raise OperationalError("boom")

    monkeypatch.setattr(admin_store, "_get_conn", _failing_conn)

    with pytest.raises(RuntimeError):
        admin_store.create_admin("admin@example.com", "secret")
