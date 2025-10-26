import pathlib
import re
import sys

import pytest

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2]))

from app import create_app, datastore, email as email_module


@pytest.fixture()
def client(monkeypatch):
    app = create_app()
    app.config.update(TESTING=True)
    sent_messages = []

    def _capture(message):
        sent_messages.append(message)
        return None

    monkeypatch.setattr(email_module.mail, "send", _capture)

    with app.test_client() as client:
        yield client, sent_messages


def test_password_reset_flow(client, memory_store):
    http_client, sent_messages = client
    datastore.create_admin("admin@example.com", "oldpassword", is_superadmin=False)

    response = http_client.post("/password/forgot", data={"email": "admin@example.com"}, follow_redirects=False)
    assert response.status_code == 302
    assert sent_messages, "reset email should be queued"

    body = sent_messages[0].body
    match = re.search(r"/password/reset/([A-Za-z0-9_\-]+)", body)
    assert match, "reset email should contain token"
    token = match.group(1)

    form_page = http_client.get(f"/password/reset/{token}")
    assert form_page.status_code == 200
    assert "Choose a new password" in form_page.get_data(as_text=True)

    submit = http_client.post(
        f"/password/reset/{token}",
        data={"password": "newpassword", "confirm": "newpassword"},
        follow_redirects=False,
    )
    assert submit.status_code == 302
    location = submit.headers.get("Location", "")
    assert location.startswith("/login")

    # New password works, old one does not
    assert datastore.verify_admin_credentials("admin@example.com", "newpassword") is not None
    assert datastore.verify_admin_credentials("admin@example.com", "oldpassword") is None

    # Token cannot be reused
    reused = http_client.get(f"/password/reset/{token}")
    assert reused.status_code == 400


def test_password_reset_unknown_email(client, memory_store):
    http_client, sent_messages = client
    before_tokens = list(memory_store.get("password_reset_tokens", []))
    response = http_client.post("/password/forgot", data={"email": "missing@example.com"}, follow_redirects=False)
    assert response.status_code == 302
    assert not sent_messages
    after_tokens = list(memory_store.get("password_reset_tokens", []))
    assert after_tokens == before_tokens

    invalid = http_client.get("/password/reset/not-a-real-token")
    assert invalid.status_code == 400
