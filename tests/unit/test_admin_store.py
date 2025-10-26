from contextlib import contextmanager
import pathlib
import sys
from unittest.mock import MagicMock
import importlib

import pytest
from psycopg2 import OperationalError
from psycopg2 import errors as pg_errors

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2]))
import app.datastore_pg as admin_store  # noqa: E402


def test_create_admin_hashes_password_and_commits(monkeypatch):
    global admin_store
    admin_store = importlib.reload(admin_store)
    row = {
        "id": 1,
        "email": "admin@example.com",
        "is_superadmin": True,
        "is_active": True,
        "created_at": "2024-01-01T00:00:00Z",
        "updated_at": "2024-01-01T00:00:00Z",
        "last_login_at": None,
    }
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.__enter__.return_value = mock_cursor
    mock_cursor.fetchone.return_value = row
    mock_conn.cursor.return_value = mock_cursor

    @contextmanager
    def _fake_conn():
        yield mock_conn

    monkeypatch.setattr(admin_store, "_get_conn", _fake_conn)
    assert admin_store._get_conn is _fake_conn
    monkeypatch.setattr(admin_store, "generate_password_hash", lambda password: f"HASH::{password}")

    admin = admin_store.create_admin("Admin@Example.com", "secret", is_superadmin=True)

    assert admin["email"] == "admin@example.com"
    assert admin["is_superadmin"] is True
    assert "password_hash" not in admin
    assert mock_conn.cursor.called
    mock_cursor.execute.assert_called_once()
    executed_sql, params = mock_cursor.execute.call_args[0]
    assert "INSERT INTO admins" in executed_sql
    assert params[0] == "admin@example.com"
    assert params[1] == "HASH::secret"
    mock_conn.commit.assert_called_once()


def test_create_admin_duplicate_email_rolls_back(monkeypatch):
    global admin_store
    admin_store = importlib.reload(admin_store)
    class FakeUniqueViolation(Exception):
        pass

    monkeypatch.setattr(admin_store.pg_errors, "UniqueViolation", FakeUniqueViolation, raising=False)

    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.__enter__.return_value = mock_cursor
    mock_cursor.execute.side_effect = FakeUniqueViolation()
    mock_conn.cursor.return_value = mock_cursor

    @contextmanager
    def _fake_conn():
        yield mock_conn

    monkeypatch.setattr(admin_store, "_get_conn", _fake_conn)
    monkeypatch.setattr(admin_store, "generate_password_hash", lambda password: f"H::{password}")

    with pytest.raises(ValueError):
        admin_store.create_admin("dup@example.com", "secret")

    mock_conn.rollback.assert_called_once()


def test_get_admin_by_email_returns_public_dict(monkeypatch):
    global admin_store
    admin_store = importlib.reload(admin_store)
    row = {
        "id": 42,
        "email": "hello@example.com",
        "is_superadmin": False,
        "is_active": True,
        "created_at": "2024-01-01T00:00:00Z",
        "updated_at": "2024-01-02T00:00:00Z",
        "last_login_at": None,
    }
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.__enter__.return_value = mock_cursor
    mock_cursor.fetchone.return_value = row
    mock_conn.cursor.return_value = mock_cursor

    @contextmanager
    def _fake_conn():
        yield mock_conn

    monkeypatch.setattr(admin_store, "_get_conn", _fake_conn)

    admin = admin_store.get_admin_by_email("Hello@Example.com")

    assert admin == row
    executed_sql, params = mock_cursor.execute.call_args[0]
    assert "WHERE email = %s" in executed_sql
    assert params[0] == "hello@example.com"


def test_verify_admin_credentials(monkeypatch):
    global admin_store
    admin_store = importlib.reload(admin_store)
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
    global admin_store
    admin_store = importlib.reload(admin_store)
    class _FailConn:
        def __enter__(self):
            raise OperationalError("boom")

        def __exit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(admin_store, "_get_conn", lambda: _FailConn())

    with pytest.raises(RuntimeError):
        admin_store.create_admin("admin@example.com", "secret")
