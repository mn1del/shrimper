# AGENTS

This repository is a Flask + Bootstrap skeleton for tracking sailing league results.

## Structure
- `app/` – Flask application package
  - `__init__.py` – application factory registering routes and recalculating handicaps
  - `routes.py` – blueprint and data-loading logic for views
  - `scoring.py` – race scoring and handicap utilities
- `templates/` – Jinja page templates
  - `static/` – front-end assets
- `data.json` – single JSON file containing seasons, series, races, fleet and settings
- `tests/` – pytest suite for routes, scoring and handicap logic
- `requirements.txt` – project dependencies

### Front-end notes
- Standings table: click a series header to toggle its race columns. Race columns are hidden by default and header dates rotate 90° when visible for a compact layout.

## App Workflow
- Data lives in a single `data.json` at the project root containing seasons/series/races and the fleet and settings.
- When the Flask app starts, routes are registered and handicaps recalculated from race data in `data.json`.
- Each request loads the relevant section from `data.json` and uses `scoring` utilities to compute race results and handicaps on the fly.
- New races or edits submitted via API endpoints update `data.json` and trigger a handicap recalculation.
- The fleet page lets you edit sailor, boat, sail number and starting handicap; saves post to `/api/fleet`, updating the `fleet` section of `data.json` and recalculating handicaps.

## Dev Workflow
1. Install dependencies: `pip install -r requirements.txt`
2. Run the dev server: `flask --app app run --debug`
3. Run tests: `pytest`
4. Keep documentation, especially this file, accurate when structure or workflow changes
