# AGENTS

This repository is a Flask + Bootstrap skeleton for tracking sailing league results.

## Structure
- `app/` – Flask application package
  - `__init__.py` – application factory registering routes and recalculating handicaps
  - `routes.py` – blueprint and data-loading logic for views
  - `scoring.py` – race scoring and handicap utilities
  - `templates/` – Jinja page templates
  - `static/` – front-end assets
- `data/` – sample JSON data for seasons, races and fleet
- `tests/` – pytest suite for routes, scoring and handicap logic
- `requirements.txt` – project dependencies

## Workflow
1. Install dependencies: `pip install -r requirements.txt`
2. Run the dev server: `flask --app app run --debug`
3. Run tests: `pytest`
4. Keep documentation, especially this file, accurate when structure or workflow changes

***IMPORTANT: ALWAYS BEFORE ISSUING A PULL REQUEST ALWAYS UPDATE THE AGENTS.MD FILE AND ENSURE IT IS UP-TO-DATE, ACCURATE AND CONCISE***
