import pytest
import pathlib
import sys

sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))
from app import create_app


@pytest.fixture()
def client():
    app = create_app()
    app.config.update({'TESTING': True})
    with app.test_client() as client:
        yield client


def test_race_page_uses_race_json_data(client):
    res = client.get('/series/SER_2025_MYHF?race_id=RACE_2025-07-11_MYHF_1')
    html = res.get_data(as_text=True)
    assert 'value="18:25:00"' in html
    assert 'value="19:52:41"' in html
    assert 'value="8"' in html


def test_race_sheet_redirects(client):
    res = client.get('/races/RACE_2025-07-11_MYHF_1', follow_redirects=False)
    assert res.status_code == 302
    assert '/series/SER_2025_MYHF?race_id=RACE_2025-07-11_MYHF_1' in res.headers['Location']


def test_race_page_calculates_results(client):
    res = client.get('/series/SER_2025_MYHF?race_id=RACE_2025-07-11_MYHF_1')
    html = res.get_data(as_text=True)
    # On course time and adjusted time are calculated
    assert '5261' in html  # on course seconds for first finisher
    assert '01:24:53' in html  # adjusted time hh:mm:ss


def test_series_detail_case_insensitive(client):
    """Series routes should be accessible regardless of ID casing."""
    res = client.get('/series/ser_2025_myhf?race_id=RACE_2025-07-11_MYHF_1')
    assert res.status_code == 200


def test_races_page_lists_races(client):
    res = client.get('/races')
    html = res.get_data(as_text=True)
    # Table should be sorted by race date, then start time
    import re
    tbody = re.search(r'<tbody>(.*?)</tbody>', html, re.S).group(1)
    rows = re.findall(r'<tr.*?>\s*(.*?)\s*</tr>', tbody, re.S)
    pairs = []
    for row_html in rows:
        cells = re.findall(r'<td>(.*?)</td>', row_html)
        if cells:
            pairs.append((cells[1], cells[2]))
    assert pairs == sorted(pairs)
    # rows link to individual race pages
    assert '/races/RACE_2025-05-23_CastF_2' in html


def test_races_page_has_create_button(client):
    res = client.get('/races')
    html = res.get_data(as_text=True)
    assert 'Create New Race' in html
    assert 'href="/race-series/new"' in html


def test_race_sheet_redirects_to_canonical_series_id(client):
    res = client.get('/races/RACE_2025-05-23_CastF_2', follow_redirects=False)
    assert res.status_code == 302
    assert '/series/SER_2025_CASTF?race_id=RACE_2025-05-23_CastF_2' in res.headers['Location']
