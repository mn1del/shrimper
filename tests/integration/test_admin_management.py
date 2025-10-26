import pathlib
import re
import sys

import pytest

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2]))

from app import create_app, datastore, email as email_module


@pytest.fixture()
def super_admin_client(monkeypatch):
    app = create_app()
    app.config.update(TESTING=True)
    sent_messages = []

    def _capture(message):
        sent_messages.append(message)
        return None

    monkeypatch.setattr(email_module.mail, "send", _capture)

    datastore.create_admin("root@example.com", "secret123", is_superadmin=True)

    with app.test_client() as client:
        client.post("/login", data={"email": "root@example.com", "password": "secret123"})
        yield client, sent_messages


def test_admin_page_requires_superadmin(super_admin_client):
    client, _ = super_admin_client
    response = client.get("/admins")
    assert response.status_code == 200
    assert "Admin Users" in response.get_data(as_text=True)

    app = create_app()
    app.config.update(TESTING=True)
    try:
        datastore.create_admin("basic@example.com", "secret456", is_superadmin=False)
    except ValueError:
        pass
    with app.test_client() as basic_client:
        basic_client.post("/login", data={"email": "basic@example.com", "password": "secret456"})
        forbidden = basic_client.get("/admins")
        assert forbidden.status_code == 403


def test_create_admin_via_api(super_admin_client, memory_store):
    client, _ = super_admin_client
    payload = {
        "email": "new@example.com",
        "password": "tempPass1",
        "is_superadmin": False,
    }
    response = client.post("/api/admins", json=payload)
    assert response.status_code == 201
    body = response.get_json()
    assert body["admin"]["email"] == "new@example.com"
    record = datastore.get_admin_by_email("new@example.com")
    assert record is not None


def test_toggle_admin_active(super_admin_client, memory_store):
    client, _ = super_admin_client
    created = datastore.create_admin("toggle@example.com", "changeme1", is_superadmin=False)
    response = client.patch(f"/api/admins/{created['id']}", json={"is_active": False})
    assert response.status_code == 200
    body = response.get_json()
    assert body["admin"]["is_active"] is False
    stored = datastore.get_admin_by_email("toggle@example.com")
    assert stored and stored["is_active"] is False


def test_send_reset_from_admin_panel(super_admin_client, memory_store):
    client, sent_messages = super_admin_client
    created = datastore.create_admin("resetme@example.com", "changeme2", is_superadmin=False)
    response = client.post(f"/api/admins/{created['id']}/reset")
    assert response.status_code == 200
    data = response.get_json()
    assert data["sent"] is True
    assert sent_messages, "Expected an email to be captured"
    assert re.search(r"/password/reset/", sent_messages[0].body)
