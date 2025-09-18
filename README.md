# Shrimper Sailing League Tracker

This project is a Flask + Bootstrap skeleton for managing a sailing league. It provides the navigation and page structure described in the product spec. All pages are placeholders intended to be fleshed out with real forms and business logic.

## Development

Install dependencies and run the dev server (PostgreSQL required):

```bash
pip install -r requirements.txt
export DATABASE_URL=postgresql://user:pass@host:port/dbname
flask --app app run --debug
```

Routes are defined in `app/routes.py` and templates live in `app/templates`.

## Database Schema

This branch is PostgreSQL-only (the prior `data.json` backend is retired). The following schema is used:

### Tables

#### `seasons`
Stores sailing seasons by year.
```sql
CREATE TABLE seasons (
    id SERIAL PRIMARY KEY,
    year INTEGER UNIQUE NOT NULL
);
```

#### `series`
Stores race series within seasons.
```sql
CREATE TABLE series (
    id SERIAL PRIMARY KEY,
    series_id VARCHAR(100) UNIQUE NOT NULL,
    name VARCHAR(100) NOT NULL,
    season_id INTEGER REFERENCES seasons(id) ON DELETE CASCADE,
    year INTEGER NOT NULL
);
```

#### `races`
Stores individual races within series.
```sql
CREATE TABLE races (
    id SERIAL PRIMARY KEY,
    race_id VARCHAR(200) UNIQUE NOT NULL,
    series_id VARCHAR(100) REFERENCES series(series_id) ON DELETE CASCADE,
    name VARCHAR(200),
    date DATE,
    start_time TIME,
    race_no INTEGER
);
```

#### `competitors`
Stores fleet information (sailors, boats, handicaps).
```sql
CREATE TABLE competitors (
    id SERIAL PRIMARY KEY,
    competitor_id VARCHAR(20) UNIQUE NOT NULL,
    sailor_name VARCHAR(100),
    boat_name VARCHAR(100),
    sail_no VARCHAR(20),
    starting_handicap_s_per_hr INTEGER,
    current_handicap_s_per_hr INTEGER
);
```

#### `race_results`
Stores race participation and finish times.
```sql
CREATE TABLE race_results (
    id SERIAL PRIMARY KEY,
    race_id VARCHAR(200) REFERENCES races(race_id) ON DELETE CASCADE,
    competitor_id VARCHAR(20) REFERENCES competitors(competitor_id) ON DELETE CASCADE,
    initial_handicap INTEGER,
    finish_time TIME,
    handicap_override INTEGER,
    UNIQUE(race_id, competitor_id)
);
```

#### `settings`
Stores application configuration and scoring parameters.
```sql
CREATE TABLE settings (
    id SERIAL PRIMARY KEY,
    version INTEGER,
    updated_at TIMESTAMP,
    handicap_delta_by_rank JSONB,
    league_points_by_rank JSONB,
    fleet_size_factor JSONB,
    config JSONB
);
```

### Key Relationships

- **seasons** → **series** (1:many via `season_id`)
- **series** → **races** (1:many via `series_id`)
- **races** → **race_results** (1:many via `race_id`)
- **competitors** → **race_results** (1:many via `competitor_id`)

### Data Types & Constraints

- **competitor_id**: Generated as `C_{sail_no}` format
- **series_id**: Generated as `SER_{year}_{name}` format  
- **race_id**: Generated as `RACE_{date}_{series_name}_{race_no}` format
- **handicap values**: Stored as seconds per hour (integer)
- **times**: Stored as PostgreSQL TIME type, converted to HH:MM:SS strings in application
- **dates**: Stored as PostgreSQL DATE type, converted to ISO format strings
- **JSONB fields**: Store arrays of scoring configuration objects

### Database Indexes

For optimal query performance, the following indexes are recommended:

```sql
-- Index for finding series within a season
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_series_season ON series(season_id);

-- Index for finding races within a series
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_races_series ON races(series_id);

-- Index for ordering races by date and time
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_races_date_time ON races(date, start_time);

-- Composite index for finding races within a series ordered by date/time
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_races_series_date_time ON races(series_id, date, start_time);

-- Index for finding all races a competitor participated in
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_results_competitor ON race_results(competitor_id);

-- Index for competitors lookup by sail number
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_competitors_sail_no ON competitors(sail_no);
```

To apply these indexes:
```bash
psql $DATABASE_URL -f add_indexes.sql
```

### Migration

If migrating from an older deployment that used `data.json`, populate PostgreSQL once using:
```bash
export DATABASE_URL=postgresql://user:pass@host:port/dbname
python migrate_to_postgres.py
```

If upgrading from a schema without `race_results.handicap_override`, either rerun the migration or POST to `/admin/schema/upgrade` to add the column in place. Overrides persist and seed subsequent races once this column exists.

## Dev Workflow
1. Install dependencies: `pip install -r requirements.txt`
2. Export `DATABASE_URL` for your Postgres instance
3. Run the dev server: `flask --app app run --debug`
4. Run tests: `pytest`
5. Keep documentation, especially this file, accurate when structure or workflow changes

Note: If you change or upgrade the Bootstrap CDN version/URL, update the SRI hashes in `app/templates/base.html`.

## Performance

- Forward-only handicap recalculation runs from the edited race forward rather than over the full history. It bulk-loads the affected races and applies updates in batches for speed.
- Recommended indexes can be inspected at `/health/indexes` and applied via `POST /admin/indexes/apply` (uses `CREATE INDEX CONCURRENTLY`).
- To skip the full recalculation during app startup (useful on large datasets), set `RECALC_ON_STARTUP=0` in the environment.

## Database Connections & Resilience

The app uses a psycopg2 `ThreadedConnectionPool` with a lightweight liveness check and TCP keepalives to avoid stale-idle disconnects that surface as:

"psycopg2.OperationalError: SSL connection has been closed unexpectedly"

- Defaults: connections are created with `connect_timeout=10` and TCP keepalives enabled.
- On checkout: a fast `SELECT 1` ping runs; if it fails, the connection is discarded and reacquired once transparently.
- Direct connects (health/admin routes) use the same options as the pool.

Environment variables to tune behavior:

- `DB_CONNECT_TIMEOUT`: seconds for initial connect (default 10)
- `DB_KEEPALIVES`: set to `0` to disable (default enabled)
- `DB_KEEPALIVES_IDLE`: seconds of idle before sending keepalive probes
- `DB_KEEPALIVES_INTERVAL`: seconds between keepalive probes
- `DB_KEEPALIVES_COUNT`: number of failed probes before the OS deems the connection dead

Recommended starting point for providers that drop idle connections aggressively (e.g., managed Postgres, PgBouncer):

```bash
export DB_CONNECT_TIMEOUT=10
export DB_KEEPALIVES=1
export DB_KEEPALIVES_IDLE=30
export DB_KEEPALIVES_INTERVAL=10
export DB_KEEPALIVES_COUNT=3
```

Troubleshooting tips:

- Use `/health/db` to verify connectivity and server version.
- Use `/health/indexes` to check for recommended indexes; POST to `/admin/indexes/apply` to create missing ones concurrently.
- Use `/health/schema` to confirm `race_results.handicap_override` exists and `finish_time` is TIME; POST to `/admin/schema/upgrade` to fix.
- If errors persist, lower `DB_KEEPALIVES_IDLE` and `DB_KEEPALIVES_INTERVAL` values to match your platform’s idle timeouts.

## Optional To‑Do (Future Cleanup)

The app now uses integer competitor IDs end‑to‑end (DB FK to `competitors.id`) with no sail‑number fallbacks. These items can further simplify and harden the codebase:

- Finalize column rename: switch `race_results.competitor_ref` (INT) to `race_results.competitor_id` (INT), drop the legacy varchar column and related indexes; update datastore SQL to select/insert `competitor_id` directly instead of aliasing. Files: `app/datastore_pg.py` (queries: SELECT, INSERT/UPSERT, UPDATE).
- Remove deprecated helpers: delete `_next_competitor_id` in `app/routes.py` and the no‑op `normalize_competitor_ids()` in `app/datastore_pg.py`. Both are unnecessary in the integer‑ID model.
- Drop legacy schema fallbacks: remove try/except code that tolerates missing columns (e.g., `handicap_override` or split settings columns) now that PostgreSQL schema is authoritative. Files: `app/datastore_pg.py` (settings load/save, race_results selects), `app/routes.py` (schema/health helpers).
- Tests parity (then simplify): migrate tests to use integer competitor IDs directly (update fixtures and expected maps). After that, remove the conversion layer added in `tests/conftest.py` that translates between string IDs (e.g., "C1") and ints.
- Health/index endpoints: once the column rename is complete, remove compatibility logic that recognizes both `competitor_ref` and legacy `competitor_id` in the index checks. Files: `app/routes.py` (`/health/indexes`, `/admin/indexes/apply`).
- Documentation refresh: update this README (and AGENTS.md if present) to describe competitor IDs as integers (`competitors.id`) throughout; remove any references to `C_<sail>`/`C_UNK_*` placeholder IDs. Ensure schema examples and index recommendations match the final column names.
