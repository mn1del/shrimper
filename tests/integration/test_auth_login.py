import pathlib
import sys

import pytest

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2]))

from app import create_app
from app import datastore


@pytest.fixture()
def client():
    app = create_app()
    app.config.update(TESTING=True)
    with app.test_client() as client:
        yield client


def test_admin_route_redirects_to_login(client):
    response = client.get('/fleet', follow_redirects=False)
    assert response.status_code == 302
    assert '/login' in response.headers['Location']


def test_api_requires_login(client):
    response = client.post('/api/fleet', json={'competitors': []})
    assert response.status_code == 401
    payload = response.get_json()
    assert payload and payload.get('error') == 'authentication_required'


def test_successful_login_grants_access(client):
    datastore.create_admin('admin@example.com', 'secret', is_superadmin=True)

    response = client.post('/login', data={'email': 'admin@example.com', 'password': 'secret'}, follow_redirects=False)
    assert response.status_code == 302
    assert response.headers['Location'].endswith('/races')

    fleet_page = client.get('/fleet')
    assert fleet_page.status_code == 200

    logout_response = client.get('/logout', follow_redirects=False)
    assert logout_response.status_code == 302
    assert logout_response.headers['Location'].endswith('/races')

    # After logout the page should again redirect to login
    relocked = client.get('/fleet', follow_redirects=False)
    assert relocked.status_code == 302
    assert '/login' in relocked.headers['Location']
