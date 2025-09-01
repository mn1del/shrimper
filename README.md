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

## Dev Workflow
1. Install dependencies: `pip install -r requirements.txt`
2. Export `DATABASE_URL` for your Postgres instance
3. Run the dev server: `flask --app app run --debug`
4. Run tests: `pytest`
5. Keep documentation, especially this file, accurate when structure or workflow changes
