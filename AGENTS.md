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
- Race edit time inputs: on first focus/click the hours segment is selected for both Start Time and each Finish Time; typing is numeric-only with segment-aware caret movement; values normalize to `HH:MM:SS` on change/blur.

## App Workflow
- Data lives in PostgreSQL (configured via `DATABASE_URL`) containing seasons/series/races, the fleet, and settings.
- When the Flask app starts, routes are registered and handicaps recalculated from race data in the database.
- Each request loads the relevant section from the DB and uses `scoring` utilities to compute race results and handicaps on the fly.
- New races or edits submitted via API endpoints persist to the DB and trigger a handicap recalculation.
- The fleet page lets you edit sailor, boat, sail number and starting handicap; saves post to `/api/fleet`, updating the `competitors` table and recalculating handicaps.

## Dev Notes
1. Create the `.venv` virtual environment if it does not exist (`python -m venv .venv`) and activate it for your shell session.
2. Install dependencies inside `.venv`: `python -m pip install -r requirements.txt`
3. `DATABASE_URL` can be found in .env
4. Run the dev server: `flask --app app run --debug`
5. Run tests from the activated environment: `pytest`
6. Keep documentation, especially this file, accurate when structure or workflow changes
7. In VS Code Codex has DB connectivity from the shell using psycopg2-binary
8. Do not make changes on the main branch. Create a new branch, name it appropriately, and work on that.


# How to Work

You are a senior engineer. When on the main branch and you are given a new task to work on: 

 Draft a branch spec for the task. Output only valid JSON matching this schema:
<branch_spec_schema> { "branch": "string-kebab", "objective": "one sentence", "non_goals": ["..."], "invariants": ["APIs or behaviors that must not change"], "files_in_scope": ["paths/glob"], "external_effects": ["db/mq/http etc"], "acceptance_tests": [{"name": "...","given":"...","when":"...","then":"..."}], "risks": ["..."], "plan": [ {"step": 1, "title":"...", "rationale":"...", "artifacts":["files"], "tests_to_add_or_update":["..."], "done_criteria":["..."]} ] } </branch_spec_schema>

Print the branch spec in a human readable form, and underneath summarise the spec in words. Ask if they are happy with it. If they are, freeze this spec. Treat it as ground truth. Do not expand scope unless the user explicitly says ‘AMEND SPEC’.

Await the user's instruction to proceed, and then create a new branch (prefix: "codex/"). On that branch use the frozen branch spec, executing only one step of the plan in the correct order, never jumping ahead. 

For each step: Write and run tests for the step and once passed check for consistency with the design documentation contained in docs/. Once satisfied, commit the changes to the branch, and ask the user if they would like you to proceed with the next step. If the step is ambiguous, ask a single clarifying question. If the user explicitly states that you can complete multiple steps, then go ahead, but ensure you proceed in order without jumping ahead, and maintain the same rigour with testing and alignment with the design documentation between steps.

Once finished state how to run the tests locally, and ask whether to proceed with the next step (including a very short description of what that step entails).

# Final response

When all steps are complete, tests run, and documents updated you can ask the user if they would like to merge changes into the main branch and delete this branch