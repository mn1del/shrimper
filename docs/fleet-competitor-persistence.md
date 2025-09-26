# Fleet Competitor Persistence Audit

## Context
Saving a brand-new competitor via `/api/fleet` currently fails with `psycopg2.errors.NotNullViolation` complaining about `competitors.competitor_id`.

## Observations
- `app/routes.py` normalises incoming fleet data and forwards `competitor_id` values exactly as supplied by the client. Brand-new rows have `competitor_id = None`.
- `app/datastore_pg.py:set_fleet` handles new competitors by inserting into `competitors` with the statement:
  ```sql
  INSERT INTO competitors (sailor_name, boat_name, sail_no, starting_handicap_s_per_hr, current_handicap_s_per_hr)
  VALUES (...)
  ```
  The insert omits the `competitor_id` column entirely.
- Our canonical schema (see README) defines `competitors` with both:
  ```sql
  id SERIAL PRIMARY KEY,
  competitor_id VARCHAR(20) UNIQUE NOT NULL
  ```
  and `race_results.competitor_id` has a foreign-key reference to that column.
- Because the column list excludes `competitor_id`, PostgreSQL attempts to use a `NULL` default, violating the `NOT NULL` constraint.
- Existing competitors avoid the problem because their payload includes a concrete `competitor_id`; the UPSERT path supplies that value explicitly.

## Root Cause
The new-competitor branch in `set_fleet` never populates `competitors.competitor_id`, leaving the DB to default it to `NULL`. The schema demands a non-null, unique identifier, so inserts fail before the primary key/foreign key relationships can be established.

## Next Steps (tracked in plan)
- Generate a deterministic `competitor_id` server-side for inserts, ensuring uniqueness and compatibility with existing references.
- Extend tests to cover fleet inserts without client-supplied IDs.
