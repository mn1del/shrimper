# AGENTS

This repository is a Flask + Bootstrap skeleton for tracking sailing league results.

## Structure
- `app/` – Flask application package
  - `__init__.py` – application factory registering routes and recalculating handicaps
  - `routes.py` – blueprint and data-loading logic for views
  - `scoring.py` – race scoring and handicap utilities
- `templates/` – Jinja page templates
  - `static/` – front-end assets
- PostgreSQL – primary datastore (configured via `DATABASE_URL`)
- `tests/` – pytest suite for routes, scoring and handicap logic
- `requirements.txt` – project dependencies

### Front-end notes
- Standings table: click a series header to toggle its race columns. Race columns are hidden by default and header dates rotate 90° when visible for a compact layout.

## App Workflow
- Data lives in PostgreSQL (configured via `DATABASE_URL`) containing seasons/series/races, the fleet, and settings.
- When the Flask app starts, routes are registered and handicaps recalculated from race data in the database.
- Each request loads the relevant section from the DB and uses `scoring` utilities to compute race results and handicaps on the fly.
- New races or edits submitted via API endpoints persist to the DB and trigger a handicap recalculation.
- The fleet page lets you edit sailor, boat, sail number and starting handicap; saves post to `/api/fleet`, updating the `competitors` table and recalculating handicaps.

## Dev Workflow
1. Install dependencies: `pip install -r requirements.txt`
2. Set `DATABASE_URL` to your PostgreSQL connection string
3. Run the dev server: `flask --app app run --debug`
4. Run tests: `pytest`
5. Keep documentation, especially this file, accurate when structure or workflow changes
6. In VS Code Codex has DB connectivity from the shell using psycopg2-binary
