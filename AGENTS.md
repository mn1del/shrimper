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

## Dev Notes
1. Install dependencies: `pip install -r requirements.txt`
2. `DATABASE_URL` can be found in .env
3. Run the dev server: `flask --app app run --debug`
4. Run tests: `pytest`
5. Keep documentation, especially this file, accurate when structure or workflow changes
6. In VS Code Codex has DB connectivity from the shell using psycopg2-binary
7. Do not make changes on the main branch. Create a new branch, name it appropriately, and work on that.

## Instructions

You are a senior engineer. When on the main branch and you are given a new task to work on: 

1) create a new branch (prefix: "codex/") and draft a branch spec for the task. Output only valid JSON matching this schema:

<branch_spec_schema>
{ "branch": "string-kebab",
  "objective": "one sentence",
  "non_goals": ["..."],
  "invariants": ["APIs or behaviors that must not change"],
  "files_in_scope": ["paths/glob"],
  "external_effects": ["db/mq/http etc"],
  "acceptance_tests": [{"name": "...","given":"...","when":"...","then":"..."}],
  "risks": ["..."],
  "plan": [
    {"step": 1, "title":"...", "rationale":"...", "artifacts":["files"], "tests_to_add_or_update":["..."], "done_criteria":["..."]}
  ]
}
</branch_spec_schema>

2) Ask the user if they are happy with the branch spec. If so, freeze this spec. Treat it as ground truth. Do not expand scope unless the user explicitly says ‘AMEND SPEC’.

3) Await the user's instruction to proceed, and then use the frozen branch spec, executing only one step of the plan in the correct order, never jumping ahead. Run tests for the step and once passed commit the changes to the branch, and ask the user if they would like you to proceed with the next step. If the step is ambiguous, ask a single clarifying question.