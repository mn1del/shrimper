from flask import Blueprint, redirect, render_template, url_for, abort, request
import json
import importlib
from datetime import datetime
import os
import time

from .scoring import calculate_race_results, _scaling_factor
from . import scoring as scoring_module
from .datastore import (
    load_data,
    save_data,
    list_all_races as ds_list_all_races,
    list_seasons as ds_list_seasons,
    list_series as ds_list_series,
    list_season_races_with_results as ds_list_season_races_with_results,
    find_series as ds_find_series,
    find_race as ds_find_race,
    ensure_series as ds_ensure_series,
    renumber_races as ds_renumber_races,
    get_fleet as ds_get_fleet,
    set_fleet as ds_set_fleet,
    get_settings as ds_get_settings,
    set_settings as ds_set_settings,
)
from .datastore import get_races as ds_get_races


bp = Blueprint('main', __name__)

# Simple in-process caches for expensive computations
_STANDINGS_CACHE: dict[tuple[int, str], tuple[float, tuple[list[dict], list[dict]]]] = {}
_RACE_CACHE: dict[str, tuple[float, dict, int]] = {}
_STANDINGS_TTL = int(os.environ.get('CACHE_TTL_STANDINGS', '180'))  # seconds
_RACE_TTL = int(os.environ.get('CACHE_TTL_RACE', '120'))  # seconds


def _cache_get_standings(season: int, scoring: str) -> tuple[list[dict], list[dict]] | None:
    key = (int(season), scoring)
    entry = _STANDINGS_CACHE.get(key)
    if not entry:
        return None
    exp, value = entry
    if exp < time.time():
        _STANDINGS_CACHE.pop(key, None)
        return None
    return value


def _cache_set_standings(season: int, scoring: str, table: list[dict], groups: list[dict]) -> None:
    _STANDINGS_CACHE[(int(season), scoring)] = (time.time() + _STANDINGS_TTL, (table, groups))


def _cache_get_race(race_id: str) -> tuple[dict, int] | None:
    entry = _RACE_CACHE.get(race_id or '')
    if not entry:
        return None
    exp, results, fleet_adj = entry
    if exp < time.time():
        _RACE_CACHE.pop(race_id, None)
        return None
    return results, fleet_adj


def _cache_set_race(race_id: str, results: dict, fleet_adjustment: int) -> None:
    if not race_id:
        return
    _RACE_CACHE[race_id] = (time.time() + _RACE_TTL, results, int(fleet_adjustment or 0))


def _cache_clear_all() -> None:
    _STANDINGS_CACHE.clear()
    _RACE_CACHE.clear()


def _cache_delete_race(race_id: str) -> None:
    _RACE_CACHE.pop(race_id or '', None)

@bp.route('/health/db')
def health_db():
    """Database connectivity health check.

    Attempts to connect using the ``DATABASE_URL`` environment variable and
    returns basic server/user info. Always returns HTTP 200 with a JSON body
    describing connection status.
    """
    import os
    url = os.environ.get('DATABASE_URL')
    if not url:
        return {
            'connected': False,
            'status': 'no_database_url',
            'message': 'DATABASE_URL is not set; JSON backend likely in use.'
        }
    try:
        import psycopg2  # type: ignore
        with psycopg2.connect(url, connect_timeout=5) as conn:
            # CREATE INDEX CONCURRENTLY requires autocommit
            try:
                conn.autocommit = True
            except Exception:
                pass
            with conn.cursor() as cur:
                cur.execute('SELECT current_user, current_database(), version()')
                user, db, ver = cur.fetchone()
            return {
                'connected': True,
                'status': 'ok',
                'user': user,
                'database': db,
                'server_version': (ver or '').split('\n')[0],
            }
    except ImportError:
        return {
            'connected': False,
            'status': 'client_missing',
            'error': 'psycopg2 is not installed in this environment.'
        }
    except Exception as e:  # pragma: no cover - best-effort health output
        return {
            'connected': False,
            'status': 'error',
            'error': str(e),
        }

@bp.route('/health/indexes')
def health_indexes():
    """Report presence of recommended indexes for performance.

    Checks for common lookup indexes on foreign keys and date ordering.
    """
    import os
    url = os.environ.get('DATABASE_URL')
    if not url:
        return {
            'connected': False,
            'status': 'no_database_url',
            'message': 'DATABASE_URL is not set; cannot inspect PostgreSQL indexes.'
        }
    try:
        import psycopg2  # type: ignore
        with psycopg2.connect(url, connect_timeout=5) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT tablename, indexname, indexdef
                    FROM pg_indexes
                    WHERE schemaname = 'public'
                      AND tablename IN ('seasons','series','races','race_results','competitors','settings')
                    ORDER BY tablename, indexname
                    """
                )
                idx = cur.fetchall()
        # Helper to find an index by exact leading column list
        def has_index(table: str, cols: str) -> bool:
            cols_norm = cols.replace(' ', '')
            for t, _name, defn in idx:
                if t != table:
                    continue
                # Normalize definition
                d = (defn or '').lower().replace(' ', '')
                # Match "(col1,col2,...)" anywhere in def
                if f'({cols_norm})' in d:
                    return True
            return False

        checks = {
            'series(season_id)': has_index('series', 'season_id'),
            'races(series_id)': has_index('races', 'series_id'),
            'races(date,start_time)': has_index('races', 'date, start_time'),
            'races(series_id,date,start_time)': has_index('races', 'series_id, date, start_time'),
            # For race_results, accept either competitor_ref (int FK) or legacy competitor_id
            'race_results(race_id)': (
                has_index('race_results', 'race_id')
                or has_index('race_results', 'race_id, competitor_ref')
                or has_index('race_results', 'race_id, competitor_id')
            ),
            'race_results(competitor)': (
                has_index('race_results', 'competitor_ref')
                or has_index('race_results', 'competitor_ref, race_id')
                or has_index('race_results', 'competitor_id')
                or has_index('race_results', 'competitor_id, race_id')
            ),
        }
        missing = [k for k, v in checks.items() if not v]
        suggestions = []
        if 'series(season_id)' in missing:
            suggestions.append('CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_series_season ON public.series(season_id);')
        if 'races(series_id)' in missing:
            suggestions.append('CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_races_series ON public.races(series_id);')
        if 'races(date,start_time)' in missing:
            suggestions.append('CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_races_date_time ON public.races(date, start_time);')
        if 'races(series_id,date,start_time)' in missing:
            suggestions.append('CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_races_series_date_time ON public.races(series_id, date, start_time);')
        if 'race_results(race_id)' in missing:
            suggestions.append('CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_results_race ON public.race_results(race_id);')
        if 'race_results(competitor)' in missing:
            suggestions.append('CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_results_competitor_ref ON public.race_results(competitor_ref);')

        return {
            'connected': True,
            'status': 'ok',
            'indexes_present': checks,
            'missing': missing,
            'suggestions': suggestions,
        }
    except ImportError:
        return {
            'connected': False,
            'status': 'client_missing',
            'error': 'psycopg2 is not installed in this environment.'
        }
    except Exception as e:  # pragma: no cover
        return {
            'connected': False,
            'status': 'error',
            'error': str(e),
        }

@bp.route('/health/schema')
def health_schema():
    """Report presence of required schema elements and types.

    - Confirms race_results.handicap_override column exists
    - Reports the data type of race_results.finish_time (expects TIME)
    """
    import os
    url = os.environ.get('DATABASE_URL')
    if not url:
        return {
            'connected': False,
            'status': 'no_database_url',
            'message': 'DATABASE_URL is not set; cannot inspect PostgreSQL schema.'
        }
    try:
        import psycopg2  # type: ignore
        with psycopg2.connect(url, connect_timeout=5) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT 1 FROM information_schema.columns
                    WHERE table_schema='public' AND table_name='race_results' AND column_name='handicap_override'
                    """
                )
                has_override = cur.fetchone() is not None
                # Finish time type
                cur.execute(
                    """
                    SELECT data_type
                    FROM information_schema.columns
                    WHERE table_schema='public' AND table_name='race_results' AND column_name='finish_time'
                    """
                )
                row = cur.fetchone()
                finish_type = row[0] if row else None
        return {
            'connected': True,
            'status': 'ok',
            'race_results.handicap_override': has_override,
            'race_results.finish_time_type': finish_type,
        }
    except Exception as e:  # pragma: no cover
        return {'connected': False, 'status': 'error', 'error': str(e)}

@bp.route('/health/handicaps')
def health_handicaps():
    """Validate chronological handicap seeding across all races.

    Asserts that for every (race, competitor):
      - If a per-race handicap_override is present, the stored initial_handicap equals it
      - Else initial_handicap equals the competitor's current handicap prior to that race

    Returns a JSON report with counts and up to 50 examples.
    """
    try:
        data = load_data()
        # Seed handicap map from fleet starting handicaps
        fleet_data = data.get('fleet', {}) or {}
        competitors = fleet_data.get('competitors', []) or []
        start_map: dict[str, int] = {
            c.get('competitor_id'): int(c.get('starting_handicap_s_per_hr') or 0)
            for c in competitors if c.get('competitor_id')
        }
        handicap_map: dict[str, int] = dict(start_map)

        # Flatten races and order chronologically (global)
        race_list: list[dict] = []
        for season in data.get('seasons', []) or []:
            for series in season.get('series', []) or []:
                for race in series.get('races', []) or []:
                    race_list.append(race)
        order = _race_order_map()
        if order:
            race_list.sort(key=lambda r: order.get(r.get('race_id'), 10**9))
        else:
            race_list.sort(key=lambda r: (r.get('date'), r.get('start_time')))

        mismatches: list[dict] = []
        # Helper to parse times
        def _p(t: str | None) -> int | None:
            if not t:
                return None
            try:
                h, m, s = map(int, t.split(':'))
                return h * 3600 + m * 60 + s
            except Exception:
                return None

        for race in race_list:
            rid = race.get('race_id')
            start_seconds = _p(race.get('start_time')) or 0
            entrants = race.get('competitors', []) or []
            # Check seeds and prepare calc entries using expected initial
            calc_entries: list[dict] = []
            for ent in entrants:
                cid = ent.get('competitor_id')
                if not cid:
                    continue
                ov = ent.get('handicap_override')
                if ov is not None:
                    try:
                        expected = int(ov)
                    except Exception:
                        expected = handicap_map.get(cid, 0)
                else:
                    expected = handicap_map.get(cid, 0)
                stored = ent.get('initial_handicap')
                if stored is None or int(stored) != int(expected):
                    mismatches.append({
                        'race_id': rid,
                        'competitor_id': cid,
                        'stored_initial': stored,
                        'expected_initial': expected,
                        'override': ov,
                    })
                entry = {
                    'competitor_id': cid,
                    'start': start_seconds,
                    'initial_handicap': int(expected),
                }
                ft = _p(ent.get('finish_time'))
                if ft is not None:
                    entry['finish'] = ft
                status = ent.get('status')
                if status:
                    entry['status'] = status
                calc_entries.append(entry)
            # Feed forward revised handicaps for chronology
            if calc_entries:
                try:
                    results = calculate_race_results(calc_entries)
                    for res in results:
                        cid2 = res.get('competitor_id')
                        rev = res.get('revised_handicap')
                        if cid2 is not None and rev is not None:
                            handicap_map[cid2] = int(rev)
                except Exception:
                    # If calculation fails, continue best-effort
                    pass

        return {
            'status': 'ok' if not mismatches else 'mismatch',
            'mismatch_count': len(mismatches),
            'examples': mismatches[:50],
        }
    except Exception as e:  # pragma: no cover
        return {'status': 'error', 'error': str(e)}, 500

@bp.route('/admin/schema/upgrade', methods=['POST'])
def schema_upgrade():
    """Ensure required schema is present and correct.

    - Adds race_results.handicap_override if missing
    - Coerces race_results.finish_time to TIME when not already TIME
    """
    import os
    url = os.environ.get('DATABASE_URL')
    if not url:
        return {'ok': False, 'status': 'no_database_url'}
    try:
        import psycopg2  # type: ignore
        with psycopg2.connect(url, connect_timeout=5) as conn:
            with conn.cursor() as cur:
                # Add column if missing
                cur.execute(
                    """
                    DO $$
                    BEGIN
                        IF NOT EXISTS (
                            SELECT 1 FROM information_schema.columns
                            WHERE table_schema='public' AND table_name='race_results' AND column_name='handicap_override'
                        ) THEN
                            ALTER TABLE public.race_results ADD COLUMN handicap_override INTEGER;
                        END IF;
                    END$$;
                    """
                )
                # Ensure finish_time column is TIME type
                try:
                    cur.execute(
                        """
                        SELECT data_type
                        FROM information_schema.columns
                        WHERE table_schema='public' AND table_name='race_results' AND column_name='finish_time'
                        """
                    )
                    row = cur.fetchone()
                    col_type = row[0] if row else None
                    if col_type and col_type.lower() != 'time without time zone':
                        # Convert from interval/text to time safely
                        cur.execute(
                            """
                            ALTER TABLE public.race_results
                            ALTER COLUMN finish_time TYPE time
                            USING CASE
                                WHEN finish_time IS NULL THEN NULL
                                WHEN pg_typeof(finish_time)::text = 'interval' THEN time '00:00' + finish_time
                                ELSE finish_time::time
                            END
                            """
                        )
                except Exception:
                    # Be tolerant if race_results is missing entirely
                    pass
                conn.commit()
        return {'ok': True}
    except Exception as e:  # pragma: no cover
        return {'ok': False, 'status': 'error', 'error': str(e)}

@bp.route('/admin/indexes/apply', methods=['POST'])
def apply_missing_indexes():
    """Create recommended indexes if missing.

    Runs CREATE INDEX CONCURRENTLY IF NOT EXISTS statements for each missing
    index detected by the same logic as /health/indexes.
    """
    import os
    url = os.environ.get('DATABASE_URL')
    if not url:
        return {'ok': False, 'status': 'no_database_url'}
    try:
        import psycopg2  # type: ignore
        with psycopg2.connect(url, connect_timeout=5) as conn:
            with conn.cursor() as cur:
                # Inspect existing indexes
                cur.execute(
                    """
                    SELECT tablename, indexname, indexdef
                    FROM pg_indexes
                    WHERE schemaname = 'public'
                      AND tablename IN ('seasons','series','races','race_results','competitors','settings')
                    ORDER BY tablename, indexname
                    """
                )
                idx = cur.fetchall()

                def has_index(table: str, cols: str) -> bool:
                    cols_norm = cols.replace(' ', '')
                    for t, _name, defn in idx:
                        if t != table:
                            continue
                        d = (defn or '').lower().replace(' ', '')
                        if f'({cols_norm})' in d:
                            return True
                    return False

                statements: list[str] = []
                if not has_index('series', 'season_id'):
                    statements.append('CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_series_season ON public.series(season_id);')
                # If series_id,date,start_time composite exists, plain series_id is optional
                if not has_index('races', 'series_id, date, start_time'):
                    if not has_index('races', 'series_id'):
                        statements.append('CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_races_series ON public.races(series_id);')
                if not has_index('races', 'date, start_time'):
                    statements.append('CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_races_date_time ON public.races(date, start_time);')
                if not has_index('races', 'series_id, date, start_time'):
                    statements.append('CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_races_series_date_time ON public.races(series_id, date, start_time);')
                # Prefer competitor_ref; accept legacy competitor_id for compatibility
                has_comp_idx = (
                    has_index('race_results', 'competitor_ref')
                    or has_index('race_results', 'competitor_ref, race_id')
                    or has_index('race_results', 'competitor_id')
                    or has_index('race_results', 'competitor_id, race_id')
                )
                if not has_comp_idx:
                    statements.append('CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_results_competitor_ref ON public.race_results(competitor_ref);')

                applied: list[str] = []
                for sql in statements:
                    cur.execute(sql)
                    applied.append(sql)
        return {'ok': True, 'applied': applied}
    except ImportError:
        return {'ok': False, 'status': 'client_missing'}
    except Exception as e:  # pragma: no cover
        return {'ok': False, 'status': 'error', 'error': str(e)}


## Admin cleanup route removed: ID normalization no longer required


#<getdata>
def _load_series_entries():
    """Return list of series (metadata + races) from data.json."""
    # Use datastore helper to avoid materializing the full dataset
    series_list = ds_list_series()
    entries = []
    for s in series_list:
        meta = {"series_id": s.get("series_id"), "name": s.get("name"), "season": s.get("season")}
        # For callers that only need metadata (e.g. race_new), keep races empty
        entries.append({"series": meta, "races": []})
    return entries
#</getdata>


#<getdata>
def _load_all_races():
    """Return a flat list of all races with series info from data.json."""
    return ds_list_all_races()
#</getdata>


#<getdata>
def _find_series(series_id: str):
    """Return (series_meta, races) for the given series id or (None, None)."""
    _season, series = ds_find_series(series_id)
    if not series:
        return None, None
    meta = {"series_id": series.get("series_id"), "name": series.get("name"), "season": series.get("season")}
    return meta, list(series.get("races", []))
#</getdata>


#<getdata>
def _find_race(race_id: str):
    """Return race data for the given race id or None if not found."""
    _season, _series, race = ds_find_race(race_id)
    return race
#</getdata>


def _parse_hms(t: str | None) -> int | None:
    """Return seconds for an ``HH:MM:SS`` timestamp or ``None``."""
    if not t:
        return None
    h, m, s = map(int, t.split(":"))
    return h * 3600 + m * 60 + s


def _race_order_map() -> dict[str, int]:
    """Return mapping race_id -> chronological index (0=earliest).

    Uses datastore.get_races(); returns empty dict on failure.
    """
    try:
        ids = ds_get_races() or []
        return {rid: idx for idx, rid in enumerate(ids) if rid}
    except Exception:
        return {}


#<getdata>
def _fleet_lookup() -> dict:
    """Return mapping of competitor_id -> fleet details (no fallbacks).

    Keys are the competitor_id values as provided by the datastore. In the
    production Postgres path these are integers. In tests (patched datastore),
    they may be strings. No sail-number derived fallbacks are produced.
    """
    fleet = ds_get_fleet() or {"competitors": []}
    competitors = fleet.get("competitors", []) or []
    mapping = {}
    for c in competitors:
        cid = c.get("competitor_id")
        if cid is not None:
            mapping[cid] = c
    return mapping
#</getdata>


# Deprecated in integer-ID model; ids are DB-assigned


#<getdata>
def recalculate_handicaps() -> None:
    """Recompute starting handicaps for all races from revised results.

    The fleet register provides the baseline starting handicaps. Each race is
    processed in chronological order and the entrants' ``initial_handicap``
    values are replaced with the current handicap prior to that race. Revised
    handicaps produced from the race are then fed forward to subsequent races
    and ultimately written back to the fleet register.
    """
    data = load_data()
    fleet_data = data.get("fleet", {"competitors": []})
    competitors = fleet_data.get("competitors", [])
    handicap_map = {
        c.get("competitor_id"): c.get("starting_handicap_s_per_hr", 0)
        for c in competitors
        if c.get("competitor_id")
    }

    # Build list of (race_obj) across all seasons/series
    race_list: list[dict] = []
    for season in data.get("seasons", []):
        for series in season.get("series", []):
            for race in series.get("races", []):
                race_list.append(race)

    order = _race_order_map()
    if order:
        race_list.sort(key=lambda r: order.get(r.get("race_id"), 10**9))
    else:
        race_list.sort(key=lambda r: (r.get("date"), r.get("start_time")))

    # Collect per-race pre-seeded initial handicaps for robust persistence
    pre_by_race: dict[str, dict[str, int]] = {}

    for race in race_list:
        start_seconds = _parse_hms(race.get("start_time")) or 0
        calc_entries: list[dict] = []
        for ent in race.get("competitors", []):
            cid = ent.get("competitor_id")
            if not cid:
                continue
            override = ent.get("handicap_override")
            if override is not None:
                initial = int(override)
                handicap_map[cid] = initial
            else:
                initial = handicap_map.get(cid, 0)
            ent["initial_handicap"] = initial
            # Track the computed pre-race seed for this entrant
            rid = str(race.get("race_id") or "")
            if rid:
                pre_by_race.setdefault(rid, {})[cid] = int(initial)
            entry = {
                "competitor_id": cid,
                "start": start_seconds,
                "initial_handicap": initial,
            }
            ft = ent.get("finish_time")
            if ft:
                parsed = _parse_hms(ft)
                if parsed is not None:
                    entry["finish"] = parsed
            status = ent.get("status")
            if status:
                entry["status"] = status
            calc_entries.append(entry)

        if calc_entries:
            results = calculate_race_results(calc_entries)
            for res in results:
                cid = res.get("competitor_id")
                revised = res.get("revised_handicap")
                if cid and revised is not None:
                    handicap_map[cid] = revised

    for comp in competitors:
        cid = comp.get("competitor_id")
        if cid:
            comp["current_handicap_s_per_hr"] = handicap_map.get(
                cid, comp.get("current_handicap_s_per_hr", 0)
            )

    data["fleet"] = fleet_data
    # Persist via JSON-like path for in-memory/testing backends.
    # Write only the sections we actually changed: seasons (race seeds) and fleet.
    try:
        save_data({
            'seasons': data.get('seasons', []),
            'fleet': data.get('fleet', {}),
        })
    except Exception:
        # Best-effort: do not fail if JSON-like save is unavailable
        pass

    # Additionally, apply targeted SQL updates to ensure PostgreSQL rows are in sync
    try:
        from . import datastore_pg as _pg
        # Build final current handicap map from fleet_data after recalc
        fleet_current: dict[str, int] = {}
        for c in fleet_data.get("competitors", []) or []:
            cid = c.get("competitor_id")
            if cid:
                try:
                    fleet_current[cid] = int(c.get("current_handicap_s_per_hr") or 0)
                except Exception:
                    pass
        if pre_by_race:
            _pg.apply_recalculated_handicaps(pre_by_race, fleet_current)
    except Exception:
        # If Postgres helpers are unavailable (e.g., during tests), ignore
        pass
#</getdata>


#<getdata>
def recalculate_handicaps_from(start_race_id: str) -> None:
    """Forward-only recalculation starting from a specific race.

    - Uses persisted initial_handicap values for the start race as the
      pre-race seeds, honoring any handicap_override values
    - Propagates revised handicaps forward to subsequent races only
    - Writes updated pre-race seeds for affected races and updates fleet
      current handicaps for competitors impacted in the forward pass
    """
    try:
        order_map = _race_order_map() or {}
        all_ids = ds_get_races() or []
    except Exception:
        order_map = {}
        all_ids = []
    if not all_ids:
        # Fallback: materialize via list_all_races if ordering unavailable
        all_races = _load_all_races() or []
        order_map = {r.get('race_id'): idx for idx, r in enumerate(all_races)}
        all_ids = [r.get('race_id') for r in all_races if r.get('race_id')]
    try:
        start_idx = all_ids.index(start_race_id)
    except ValueError:
        # If race id not found, fall back to full recalculation
        # Only recalc forward from the newly created race
        recalculate_handicaps_from(new_race_id)
        return

    forward_ids = all_ids[start_idx:]
    if not forward_ids:
        return

    pre_by_race: dict[str, dict[int, int]] = {}
    revised_latest: dict[int, int] = {}

    for rid in forward_ids:
        # Fetch entrants for this race directly from datastore/DB
        _season, _series, race = ds_find_race(rid)
        if not race:
            continue
        start_seconds = _parse_hms(race.get('start_time')) or 0
        entrants = race.get('competitors', []) or []
        calc_entries: list[dict] = []
        seeds_for_race: dict[int, int] = {}

        for ent in entrants:
            cid = ent.get('competitor_id')
            if cid is None:
                continue
            initial = None
            ov = ent.get('handicap_override')
            if ov is not None:
                try:
                    initial = int(ov)
                except Exception:
                    initial = None
            if initial is None:
                # If we have a revised handicap from a prior race in this forward pass, prefer it
                if int(cid) in revised_latest:
                    initial = int(revised_latest[int(cid)])
                else:
                    ih = ent.get('initial_handicap')
                    try:
                        initial = int(ih) if ih is not None else None
                    except Exception:
                        initial = None
            entry = {
                'competitor_id': int(cid),
                'start': start_seconds,
                'initial_handicap': initial,
            }
            ft_raw = ent.get('finish_time')
            try:
                ft = _parse_hms(ft_raw)
            except Exception:
                ft = None
            if ft is not None:
                entry['finish'] = ft
            status = ent.get('status')
            if status:
                entry['status'] = status

            calc_entries.append(entry)
            if initial is not None:
                seeds_for_race[int(cid)] = int(initial)

        if calc_entries:
            results = calculate_race_results(calc_entries)
            for res in results:
                cid = res.get('competitor_id')
                revised = res.get('revised_handicap')
                if cid is not None and revised is not None:
                    revised_latest[int(cid)] = int(revised)

        if seeds_for_race:
            pre_by_race[str(rid)] = seeds_for_race

    # Build a partial fleet current map for touched competitors
    fleet_current: dict[int, int] = {cid: h for cid, h in revised_latest.items()}

    try:
        from . import datastore_pg as _pg
        if pre_by_race:
            _pg.apply_recalculated_handicaps(pre_by_race, fleet_current)
    except Exception:
        # If Postgres helpers are unavailable (e.g., during tests), ignore
        pass
#</getdata>
#<getdata>
def _season_standings(season: int, scoring: str) -> tuple[list[dict], list[dict]]:
    """Compute standings and per-race metadata for a season."""
    fleet = _fleet_lookup()
    race_groups: list[dict] = []

    season_obj = ds_list_season_races_with_results(int(season)) or {"series": []}
    for series in season_obj.get("series", []):
            group = {
                "series_name": series.get("name"),
                "series_id": series.get("series_id"),
                "races": [],
            }
            for race in series.get("races", []):
                start_seconds = _parse_hms(race.get("start_time")) or 0
                entrants_map = {
                ent.get("competitor_id"): ent
                    for ent in race.get("competitors", [])
                    if ent.get("competitor_id")
                }
                entries: list[dict] = []
                for cid, info in fleet.items():
                    entry = {
                        "competitor_id": cid,
                        "start": start_seconds,
                        "initial_handicap": entrants_map.get(cid, {}).get(
                            "initial_handicap",
                            info.get("starting_handicap_s_per_hr")
                            or info.get("current_handicap_s_per_hr")
                            or 0,
                        ),
                        "sailor": info.get("sailor_name"),
                        "boat": info.get("boat_name"),
                        "sail_number": info.get("sail_no"),
                    }
                    ent = entrants_map.get(cid)
                    if ent:
                        ft = ent.get("finish_time")
                        if ft:
                            entry["finish"] = _parse_hms(ft)
                        status = ent.get("status")
                        if status:
                            entry["status"] = status
                    entries.append(entry)
                results = calculate_race_results(entries)
                group["races"].append(
                    {
                        "race_id": race.get("race_id"),
                        "date": race.get("date"),
                        "start_time": race.get("start_time"),
                        "results": results,
                    }
                )
            if group["races"]:
                order = _race_order_map()
                if order:
                    group["races"].sort(key=lambda r: order.get(r.get("race_id"), 10**9))
                else:
                    group["races"].sort(key=lambda r: (r["date"] or "", r["start_time"] or ""))
                race_groups.append(group)

    race_groups.sort(key=lambda g: g["series_name"] or "")

    aggregates: dict[str, dict] = {}
    for idx, group in enumerate(race_groups):
        for race in group["races"]:
            finisher_count = sum(1 for r in race["results"] if r.get("finish") is not None)
            for res in race["results"]:
                cid = res.get("competitor_id")
                agg = aggregates.setdefault(
                    cid,
                    {
                        "sailor": res.get("sailor"),
                        "boat": res.get("boat"),
                        "sail_number": res.get("sail_number"),
                        "race_count": 0,
                        "league_points": 0.0,
                        "traditional_points": 0.0,
                        "race_points": {},
                        "series_totals": {},
                        "series_results": {},
                        "dropped_races": set(),
                        "race_finished": {},
                    },
                )
                finished = res.get("finish") is not None
                if finished:
                    agg["race_count"] += 1
                league_pts = res.get("points", 0.0)
                trad_pts = res.get("traditional_points")
                if trad_pts is None and res.get("finish") is None:
                    trad_pts = finisher_count + 1
                elif trad_pts is None:
                    trad_pts = 0.0
                agg["league_points"] += league_pts
                agg["traditional_points"] += trad_pts
                if scoring == "traditional":
                    agg["race_points"][race["race_id"]] = trad_pts
                    series_list = agg["series_results"].setdefault(idx, [])
                    series_list.append(
                        {
                            "race_id": race["race_id"],
                            "points": trad_pts,
                            "finished": finished,
                        }
                    )
                else:
                    agg["race_points"][race["race_id"]] = league_pts
                    agg["series_totals"][idx] = agg["series_totals"].get(idx, 0.0) + league_pts
                agg["race_finished"][race["race_id"]] = finished

    standings: list[dict] = []
    for agg in aggregates.values():
        if scoring == "traditional":
            series_totals: dict[int, float] = {}
            series_counts: dict[int, int] = {}
            dropped: set[str] = set()
            for sidx, results in agg["series_results"].items():
                raw_total = sum(r["points"] for r in results)
                finish_count = sum(1 for r in results if r["finished"])
                series_counts[sidx] = finish_count
                if finish_count > 4:
                    drop_n = 2
                elif finish_count == 4:
                    drop_n = 1
                else:
                    drop_n = 0
                drop_points = 0.0
                if drop_n:
                    sorted_res = sorted(results, key=lambda r: r["points"], reverse=True)
                    to_drop = sorted_res[:drop_n]
                    drop_points = sum(r["points"] for r in to_drop)
                    dropped.update(r["race_id"] for r in to_drop)
                series_totals[sidx] = raw_total - drop_points
            total = sum(series_totals.values())
            standings.append(
                {
                    "sailor": agg["sailor"],
                    "boat": agg["boat"],
                    "sail_number": agg["sail_number"],
                    "race_count": agg["race_count"],
                    "total_points": total,
                    "race_points": agg["race_points"],
                    "series_totals": series_totals,
                    "series_counts": series_counts,
                    "dropped_races": dropped,
                    "race_finished": agg["race_finished"],
                }
            )
        else:
            total = agg["league_points"]
            standings.append(
                {
                    "sailor": agg["sailor"],
                    "boat": agg["boat"],
                    "sail_number": agg["sail_number"],
                    "race_count": agg["race_count"],
                    "total_points": total,
                    "race_points": agg["race_points"],
                    "series_totals": agg["series_totals"],
                    "series_counts": {},
                    "dropped_races": set(),
                    "race_finished": agg["race_finished"],
                }
            )

    if scoring == "traditional":
        standings.sort(key=lambda r: (r["total_points"], -r["race_count"], r["sailor"]))
    else:
        standings.sort(key=lambda r: (-r["total_points"], -r["race_count"], r["sailor"]))

    prev_points: float | None = None
    prev_races: int | None = None
    prev_place = 0
    for idx, row in enumerate(standings, start=1):
        if prev_points is not None and row["total_points"] == prev_points and row["race_count"] == prev_races:
            row["position"] = f"={prev_place}"
        else:
            row["position"] = str(idx)
            prev_place = idx
            prev_points = row["total_points"]
            prev_races = row["race_count"]

    return standings, race_groups
#</getdata>


@bp.route('/')
def index():
    return redirect(url_for('main.races'))


@bp.route('/races')
def races():
    season = request.args.get('season') or None
    #<getdata>
    all_races = _load_all_races()
    #</getdata>
    seasons = sorted({r.get('season') for r in all_races if r.get('season')}, reverse=True)
    if season:
        race_list = [r for r in all_races if str(r.get('season')) == str(season)]
    else:
        race_list = all_races
    breadcrumbs = [('Races', None)]
    return render_template(
        'races.html',
        title='Races',
        breadcrumbs=breadcrumbs,
        races=race_list,
        seasons=seasons,
        selected_season=season,
    )


@bp.route('/db')
def db_browser():
    """Read-only browser for database contents.

    Provides clickable lists of seasons, series, races, competitors and shows
    current settings. Filters by optional season/series query params.
    """
    try:
        season_param = request.args.get('season')
        series_param = request.args.get('series')
        seasons_meta = ds_list_seasons() or []
        seasons = sorted({int(s.get('year')) for s in seasons_meta if s.get('year') is not None})
        selected_season = int(season_param) if season_param else None
    except Exception:
        seasons = []
        selected_season = None

    all_series = ds_list_series() or []
    if selected_season is not None:
        series_list = [s for s in all_series if int(s.get('season') or 0) == selected_season]
    else:
        series_list = all_series

    selected_series = (series_param or '')
    all_races = ds_list_all_races() or []
    if selected_series:
        races_list = [r for r in all_races if (r.get('series_id') or '').lower() == selected_series.lower()]
    else:
        races_list = all_races

    fleet = ds_get_fleet().get('competitors', [])
    settings = ds_get_settings() or {}

    breadcrumbs = [('DB', None)]
    return render_template(
        'db.html',
        title='Database Browser',
        breadcrumbs=breadcrumbs,
        seasons=seasons,
        selected_season=selected_season,
        series_list=series_list,
        selected_series=selected_series,
        races=races_list,
        fleet=fleet,
        settings=settings,
    )


#<getdata>
def _load_series_meta(series_id: str):
    """Return (path, data) for the given series id or (None, None).

    Comparison is case-insensitive to tolerate differing user input.
    """
    target = series_id.lower()
    for meta_path in _series_meta_paths():
        with meta_path.open() as f:
            data = json.load(f)
        sid = data.get("series_id")
        if sid and sid.lower() == target:
            return meta_path, data
    return None, None
#</getdata>


## File-based renumber helper removed (JSON backend retired)


#<getdata>
@bp.route('/races/new')
def race_new():
    series_list = [entry['series'] for entry in _load_series_entries()]
    fleet = ds_get_fleet().get('competitors', [])
    blank_race = {
        'race_id': '__new__',
        'series_id': '',
        'date': '',
        'start_time': '',
        'competitors': [],
        'results': {},
    }
    breadcrumbs = [('Races', url_for('main.races')), ('Create New Race', None)]
    return render_template(
        'series_detail.html',
        title='Create New Race',
        breadcrumbs=breadcrumbs,
        series={},
        races=[],
        selected_race=blank_race,
        finisher_display='Number of Finishers: 0',
        fleet=fleet,
        series_list=series_list,
        unlocked=True,
        fleet_adjustment=0,
    )
#</getdata>


@bp.route('/series/<series_id>')
def series_detail(series_id):
    series, races = _find_series(series_id)
    if series is None:
        abort(404)

    race_id = request.args.get('race_id')
    if race_id == '__new__':
        return redirect(url_for('main.race_new'))

    selected_race = None
    finisher_count = 0
    fleet = []
    fleet_adjustment = 0

    def _parse_hms(t: str | None) -> int | None:
        if not t:
            return None
        h, m, s = map(int, t.split(":"))
        return h * 3600 + m * 60 + s

    def _format_hms(seconds: float | None) -> str | None:
        if seconds is None:
            return None
        total = int(round(seconds))
        h = total // 3600
        m = (total % 3600) // 60
        s = total % 60
        return f"{h:02d}:{m:02d}:{s:02d}"

    if race_id:
        #<getdata>
        # Load baseline handicaps from fleet register
        fleet = ds_get_fleet().get('competitors', [])
        handicap_map = {
            comp.get('competitor_id'): comp.get('starting_handicap_s_per_hr', 0)
            for comp in fleet
            if comp.get('competitor_id')
        }
        errors: list[str] = []

        # Fast path: compute only the selected race without scanning all races
        skip_heavy = False
        race = _find_race(race_id)
        if race is not None:
            # Validate start time format when finishers present
            start_raw = race.get('start_time')
            try:
                start_seconds = _parse_hms(start_raw)
            except Exception:
                start_seconds = None
                if any((e.get('finish_time') or '').strip() for e in (race.get('competitors') or [])):
                    errors.append(f"Unable to parse start time '{start_raw}' for this race. Expected HH:MM:SS.")
            if start_seconds is None:
                start_seconds = 0
            entrants = race.get('competitors', []) or []
            entrants_map: dict[int, dict] = {
                int(e.get('competitor_id')): e for e in entrants if e.get('competitor_id') is not None
            }

            ordered_ids: list[int] = []
            for comp in fleet:
                cid = comp.get('competitor_id')
                if cid is not None:
                    ordered_ids.append(int(cid))
            for cid in entrants_map.keys():
                if cid not in ordered_ids:
                    ordered_ids.append(cid)

            # Precompute pre-race handicaps (used for display and as calc inputs)
            pre_race_handicaps: dict[int, int] = {}
            fleet_map: dict[int, dict] = {int(c.get('competitor_id')): c for c in fleet if c.get('competitor_id') is not None}
            for cid in ordered_ids:
                ent = entrants_map.get(cid, {})
                comp = fleet_map.get(cid)
                initial = None
                if ent.get('handicap_override') is not None:
                    try:
                        initial = int(ent['handicap_override'])
                    except (ValueError, TypeError):
                        initial = None
                if initial is None:
                    ih = ent.get('initial_handicap')
                    if ih is not None:
                        try:
                            initial = int(ih)
                        except (ValueError, TypeError):
                            initial = None
                if initial is None and comp is not None:
                    # Default baseline should be starting handicap; use current only if starting is missing
                    initial = comp.get('starting_handicap_s_per_hr')
                if initial is None:
                    # Missing a usable starting handicap and no override/initial on entrant
                    name = (comp.get('sailor_name') if comp else '') or str(cid)
                    errors.append(f"No starting handicap available from the Fleet for competitor {name}.")
                    # Do not select a fallback value here; leave pre_race_handicaps without an entry
                    continue
                pre_race_handicaps[cid] = int(initial)

            cached = _cache_get_race(race_id)
            if cached is not None and not errors:
                results, fleet_adjustment = cached
                # Ensure finisher count reflects cached results for display
                try:
                    finisher_count = sum(
                        1 for r in (results or {}).values() if r.get('finish_time')
                    )
                except Exception:
                    finisher_count = 0
            else:
                if errors:
                    # Skip heavy calculation; surface basic results with errors
                    results = {}
                    basic: dict[str, dict] = {}
                    for ent in entrants:
                        cid = ent.get('competitor_id')
                        if not cid:
                            continue
                        basic[cid] = {
                            'finish_time': ent.get('finish_time')
                        }
                    selected_race = race
                    selected_race['results'] = basic
                    return render_template(
                        'series_detail.html',
                        title=series.get('name') or 'Series',
                        breadcrumbs=[('Races', url_for('main.races')), (series.get('name'), None)],
                        series=series,
                        races=races,
                        selected_race=selected_race,
                        finisher_display=f"Number of Finishers: {sum(1 for v in basic.values() if v.get('finish_time'))}",
                        fleet=fleet,
                        series_list=[entry['series'] for entry in _load_series_entries()],
                        unlocked=False,
                        fleet_adjustment=0,
                        errors=errors,
                    )
                calc_entries: list[dict] = []
                for cid in ordered_ids:
                    ent = entrants_map.get(cid, {})
                    entry = {
                        'competitor_id': cid,
                        'start': start_seconds,
                        'initial_handicap': pre_race_handicaps.get(cid),
                    }
                    try:
                        ft = _parse_hms(ent.get('finish_time')) if ent else None
                    except Exception:
                        ft = None
                        if (ent.get('finish_time') or '').strip():
                            errors.append(f"Invalid finish time '{ent.get('finish_time')}' for competitor {cid}. Expected HH:MM:SS.")
                    if ft is not None:
                        entry['finish'] = ft
                    status = ent.get('status') if ent else None
                    if status:
                        entry['status'] = status
                    # Only include entries with a valid initial handicap
                    if entry['initial_handicap'] is not None:
                        calc_entries.append(entry)

                if errors:
                    # Surface errors without calculating potentially incorrect results
                    results_list = []
                else:
                    results_list = calculate_race_results(calc_entries)
                finisher_count = sum(1 for r in results_list if r.get('finish') is not None)
                fleet_adjustment = int(round(_scaling_factor(finisher_count) * 100)) if finisher_count else 0

                results: dict[int, dict] = {}
                for res in results_list:
                    cid = res.get('competitor_id')
                    entrant = entrants_map.get(cid, {})
                    finish_str = entrant.get('finish_time')
                    is_non_finisher = res.get('finish') is None
                    results[cid] = {
                        'finish_time': finish_str,
                        'on_course_secs': res.get('elapsed_seconds'),
                        'abs_pos': res.get('absolute_position'),
                        'allowance': res.get('allowance_seconds'),
                        'adj_time_secs': res.get('adjusted_time_seconds'),
                        'adj_time': _format_hms(res.get('adjusted_time_seconds')),
                        'hcp_pos': res.get('handicap_position'),
                        'race_pts': res.get('traditional_points')
                        if res.get('traditional_points') is not None
                        else (finisher_count + 1 if is_non_finisher else None),
                        'league_pts': res.get('points')
                        if res.get('points') is not None
                        else (0.0 if is_non_finisher else None),
                        'full_delta': res.get('full_delta')
                        if res.get('full_delta') is not None
                        else (0 if is_non_finisher else None),
                        'scaled_delta': res.get('scaled_delta')
                        if res.get('scaled_delta') is not None
                        else (0 if is_non_finisher else None),
                        'actual_delta': res.get('actual_delta')
                        if res.get('actual_delta') is not None
                        else (0 if is_non_finisher else None),
                        'revised_hcp': res.get('revised_handicap')
                        if res.get('revised_handicap') is not None
                        else (
                            res.get('initial_handicap') if is_non_finisher else None
                        ),
                        'place': res.get('status'),
                        'handicap_override': entrant.get('handicap_override'),
                    }
                # Keys are canonical integer ids; no normalization required

                _cache_set_race(race_id, results, fleet_adjustment)

            selected_race = race
            skip_heavy = True

        if not skip_heavy:
            # Load all races from data.json and process them chronologically until target race
            data = load_data()
            race_objs: list[dict] = []
            for season in data.get('seasons', []):
                for s in season.get('series', []):
                    for r in s.get('races', []):
                        race_objs.append(r)
            # Use global chronological order when available
            order = _race_order_map()
            if order:
                race_objs.sort(key=lambda r: order.get(r.get('race_id'), 10**9))
            else:
                race_objs.sort(key=lambda r: (r.get('date'), r.get('start_time')))

            pre_race_handicaps = handicap_map
            results: dict[int, dict] = {}

            for race in race_objs:
                start_seconds = _parse_hms(race.get('start_time'))
                entrants = race.get('competitors', [])
                entrants_map: dict[int, dict] = {
                    int(e.get('competitor_id')): e for e in entrants if e.get('competitor_id') is not None
                }
                snapshot = handicap_map.copy()
            
                if race.get('race_id') == race_id:
                    # Build entries for the full fleet order, enriching with any
                    # entrants. This guarantees all fleet members appear and
                    # finishers line up with their result rows.
                    calc_entries: list[dict] = []

                    # Helper maps by sail number for fleets without competitor_id
                    # Build ordered ids using fleet order (canonical integer ids)
                    ordered_ids: list[int] = []
                    for comp in fleet:
                        cid = comp.get('competitor_id')
                        if cid is not None:
                            ordered_ids.append(int(cid))

                    # Append any remaining entrants not represented in fleet
                    for cid in entrants_map.keys():
                        if cid not in ordered_ids:
                            ordered_ids.append(cid)

                    # Build calc entries
                    fleet_map: dict[int, dict] = {int(c.get('competitor_id')): c for c in fleet if c.get('competitor_id') is not None}
                    for cid in ordered_ids:
                        ent = entrants_map.get(cid, {})
                        # Find fleet record for this id if possible
                        comp = fleet_map.get(cid)
                        # initial handicap preference: per-race override -> snapshot ->
                        # entrant initial -> fleet current/starting -> 0
                        initial = snapshot.get(cid)
                        if ent.get('handicap_override') is not None:
                            try:
                                initial = int(ent['handicap_override'])
                                snapshot[cid] = initial
                            except (ValueError, TypeError):
                                pass
                        if initial is None:
                            initial = ent.get('initial_handicap')
                        if initial is None and comp is not None:
                            initial = (
                                comp.get('starting_handicap_s_per_hr')
                                or comp.get('current_handicap_s_per_hr')
                                or 0
                            )
                        if initial is None:
                            initial = 0

                        entry = {
                            'competitor_id': cid,
                            'start': start_seconds or 0,
                            'initial_handicap': int(initial),
                        }
                        ft = _parse_hms(ent.get('finish_time')) if ent else None
                        if ft is not None:
                            entry['finish'] = ft
                        status = ent.get('status') if ent else None
                        if status:
                            entry['status'] = status
                        calc_entries.append(entry)

                    results_list = calculate_race_results(calc_entries)
                    finisher_count = sum(
                        1 for r in results_list if r.get('finish') is not None
                    )
                    if finisher_count:
                        fleet_adjustment = int(
                            round(_scaling_factor(finisher_count) * 100)
                        )
                    for res in results_list:
                        cid = res.get('competitor_id')
                        entrant = entrants_map.get(cid, {})
                        finish_str = entrant.get('finish_time')
                        is_non_finisher = res.get('finish') is None
                        results[cid] = {
                            'finish_time': finish_str,
                            'on_course_secs': res.get('elapsed_seconds'),
                            'abs_pos': res.get('absolute_position'),
                            'allowance': res.get('allowance_seconds'),
                            'adj_time_secs': res.get('adjusted_time_seconds'),
                            'adj_time': _format_hms(res.get('adjusted_time_seconds')),
                            'hcp_pos': res.get('handicap_position'),
                            'race_pts': res.get('traditional_points')
                            if res.get('traditional_points') is not None
                            else (finisher_count + 1 if is_non_finisher else None),
                            'league_pts': res.get('points')
                            if res.get('points') is not None
                            else (0.0 if is_non_finisher else None),
                            'full_delta': res.get('full_delta')
                            if res.get('full_delta') is not None
                            else (0 if is_non_finisher else None),
                            'scaled_delta': res.get('scaled_delta')
                            if res.get('scaled_delta') is not None
                            else (0 if is_non_finisher else None),
                            'actual_delta': res.get('actual_delta')
                            if res.get('actual_delta') is not None
                            else (0 if is_non_finisher else None),
                            'revised_hcp': res.get('revised_handicap')
                            if res.get('revised_handicap') is not None
                            else (
                                res.get('initial_handicap') if is_non_finisher else None
                            ),
                            'place': res.get('status'),
                            'handicap_override': entrant.get('handicap_override'),
                        }

                    # Keys are canonical integer ids; no normalization required

                    selected_race = race
                    pre_race_handicaps = snapshot

                    # Update map for completeness then stop processing
                    for res in results_list:
                        cid = res.get('competitor_id')
                        revised = res.get('revised_handicap')
                        if revised is not None:
                            handicap_map[cid] = revised
                    break

            # Process prior races to update handicap map
            calc_entries: list[dict] = []
            for cid, entrant in entrants_map.items():
                initial = snapshot.get(cid, 0)
                if entrant.get('handicap_override') is not None:
                    try:
                        initial = int(entrant['handicap_override'])
                        snapshot[cid] = initial
                    except (ValueError, TypeError):
                        pass
                entry = {
                    'competitor_id': cid,
                    'start': start_seconds or 0,
                    'initial_handicap': initial,
                }
                ft = _parse_hms(entrant.get('finish_time'))
                if ft is not None:
                    entry['finish'] = ft
                status = entrant.get('status')
                if status:
                    entry['status'] = status
                calc_entries.append(entry)

            prior_results = calculate_race_results(calc_entries)
            for res in prior_results:
                cid = res.get('competitor_id')
                revised = res.get('revised_handicap')
                if revised is not None:
                    handicap_map[cid] = revised

        # Build a display list for the race table that includes the entire
        # fleet, plus any entrants not present in the fleet register. This
        # ensures the race page serves as the full data entry point and shows
        # all boats even without a recorded finish time.
        display_list: list[dict] = []
        if selected_race:
            # Build map of this race's entrants
            local_entrants_map = {
                e.get('competitor_id'): e
                for e in (selected_race.get('competitors', []) or [])
                if e.get('competitor_id')
            }

            seen: set[int] = set()

            # 1) Add every fleet record using canonical integer competitor ids
            for f in fleet:
                cid = f.get('competitor_id')
                if cid is None:
                    continue
                cid = int(cid)
                seen.add(cid)
                ent = local_entrants_map.get(cid, {})
                display_list.append({
                    'competitor_id': cid,
                    'sailor_name': f.get('sailor_name', ''),
                    'boat_name': f.get('boat_name', ''),
                    'sail_no': f.get('sail_no', ''),
                    'current_handicap_s_per_hr': (
                        pre_race_handicaps.get(cid)
                        if cid in pre_race_handicaps else (
                            ent.get('initial_handicap')
                            if ent.get('initial_handicap') is not None
                            else f.get('starting_handicap_s_per_hr') or f.get('current_handicap_s_per_hr') or 0
                        )
                    ),
                })

            # 2) Include any entrants not present in the fleet list
            for cid, ent in local_entrants_map.items():
                if cid in seen:
                    continue
                display_list.append({
                    'competitor_id': cid,
                    'sailor_name': '',
                    'boat_name': '',
                    'sail_no': '',
                    'current_handicap_s_per_hr': pre_race_handicaps.get(
                        cid,
                        ent.get('initial_handicap', 0),
                    ),
                })

            # Replace the fleet list used by the template with the full display list
            fleet = display_list

        if selected_race:
            # Primary path: full results computed above
            selected_race['results'] = results
            # Fallback: if for any reason results is empty, at least surface finish times
            if not selected_race['results']:
                basic: dict[str, dict] = {}
                for ent in selected_race.get('competitors', []) or []:
                    cid = ent.get('competitor_id')
                    if not cid:
                        continue
                    basic[cid] = {
                        'finish_time': ent.get('finish_time')
                    }
                selected_race['results'] = basic
        #</getdata>

    finisher_display = f"Number of Finishers: {finisher_count}"

    # When viewing an individual race, suppress breadcrumbs and provide a list
    # of all races for navigation. Otherwise show the standard breadcrumb trail.
    if selected_race:
        breadcrumbs = None
        all_races = _load_all_races()
        order = _race_order_map()
        if order:
            all_races.sort(key=lambda r: order.get(r.get('race_id'), 10**9))
    else:
        breadcrumbs = [('Races', url_for('main.races')), (series.get('name', series_id), None)]
        all_races = []

    series_list = [entry['series'] for entry in _load_series_entries()]
    # Ensure errors variable exists for template
    try:
        errors
    except NameError:
        errors = []
    return render_template(
        'series_detail.html',
        title=series.get('name', series_id),
        breadcrumbs=breadcrumbs,
        series=series,
        races=races,
        selected_race=selected_race,
        finisher_display=finisher_display,
        fleet=fleet,
        series_list=series_list,
        fleet_adjustment=fleet_adjustment,
        all_races=all_races,
        errors=errors,
    )


@bp.route('/races/<race_id>')
def race_sheet(race_id):
    race = _find_race(race_id)
    if race is None:
        abort(404)
    series_id = race.get('series_id')
    if not series_id:
        abort(404)
    series, _ = _find_series(series_id)
    if not series:
        abort(404)
    canonical_id = series.get('series_id')
    return redirect(url_for('main.series_detail', series_id=canonical_id, race_id=race_id))


#<getdata>
@bp.route('/standings')
def standings():
    scoring = request.args.get('format', 'league').lower()
    season_param = request.args.get('season')
    # Avoid full-tree load: get just the season years
    seasons_meta = ds_list_seasons()
    seasons = sorted({int(s.get('year')) for s in (seasons_meta or []) if s.get('year') is not None}, reverse=True)
    if not seasons:
        season_val = None
        table = []
        race_groups = []
    else:
        try:
            season_int = int(season_param) if season_param is not None else None
        except ValueError:
            season_int = None
        if season_int is None or season_int not in seasons:
            season_val = seasons[0]
        else:
            season_val = season_int
        cached = _cache_get_standings(season_val, scoring)
        if cached is not None:
            table, race_groups = cached
        else:
            table, race_groups = _season_standings(season_val, scoring)
            _cache_set_standings(season_val, scoring, table, race_groups)
    breadcrumbs = [('Standings', None)]
    return render_template(
        'standings.html',
        title='Standings',
        breadcrumbs=breadcrumbs,
        seasons=seasons,
        selected_season=season_val,
        scoring_format=scoring,
        standings=table,
        race_groups=race_groups,
    )
#</getdata>


#<getdata>
@bp.route('/fleet')
def fleet():
    breadcrumbs = [('Fleet', None)]
    competitors = ds_get_fleet().get('competitors', [])
    return render_template('fleet.html', title='Fleet', breadcrumbs=breadcrumbs, fleet=competitors)
#</getdata>


#<getdata>
@bp.route('/api/fleet', methods=['POST'])
def update_fleet():
    """Persist fleet edits and refresh handicaps."""
    payload = request.get_json() or {}
    comps = payload.get('competitors', [])
    # Load existing fleet only (avoid full dataset materialization)
    fleet_data = ds_get_fleet()
    if not fleet_data:
        fleet_data = {'competitors': []}
    # Normalize incoming list; preserve id when provided; let datastore assign when missing
    normalized: list[dict] = []
    for comp in comps:
        cid = comp.get('competitor_id')
        try:
            cid = int(cid) if cid is not None else None
        except Exception:
            cid = None
        entry = {
            'competitor_id': cid,
            'sailor_name': comp.get('sailor_name', ''),
            'boat_name': comp.get('boat_name', ''),
            'sail_no': comp.get('sail_no', ''),
            'starting_handicap_s_per_hr': comp.get('starting_handicap_s_per_hr', 0),
            'current_handicap_s_per_hr': comp.get('current_handicap_s_per_hr', comp.get('starting_handicap_s_per_hr', 0)),
        }
        normalized.append(entry)

    # Enforce unique sail numbers (non-empty)
    sail_counts: dict[str, int] = {}
    for c in normalized:
        sail = (c.get('sail_no') or '').strip()
        if not sail:
            continue
        sail_counts[sail] = sail_counts.get(sail, 0) + 1
    dups = [sn for sn, cnt in sail_counts.items() if cnt > 1]
    if dups:
        return {'error': f"Duplicate sail numbers: {', '.join(sorted(dups))}"}, 400

    fleet_data['competitors'] = normalized
    fleet_data['updated_at'] = datetime.utcnow().isoformat() + 'Z'
    # Persist only fleet changes via datastore helper
    ds_set_fleet(fleet_data)
    # Fleet changes affect baselines for all races: do a full recalc
    recalculate_handicaps()
    # Bust caches after fleet update
    _cache_clear_all()
    return {'status': 'ok'}
#</getdata>


@bp.route('/rules')
def rules():
    breadcrumbs = [('Rules', None)]
    return render_template('rules.html', title='Rules', breadcrumbs=breadcrumbs)


#<getdata>
@bp.route('/settings')
def settings():
    breadcrumbs = [('Settings', None)]
    settings_data = ds_get_settings()
    return render_template('settings.html', title='Settings', breadcrumbs=breadcrumbs, settings=settings_data)
#</getdata>


#<getdata>
@bp.route('/api/settings', methods=['POST'])
def save_settings():
    """Persist updated settings to the JSON configuration file."""
    payload = request.get_json() or {}
    # Preserve versioning information and update timestamp
    existing = ds_get_settings() or {"version": 0}

    payload["version"] = int(existing.get("version", 0)) + 1
    payload["updated_at"] = datetime.utcnow().isoformat() + "Z"

    # Persist only settings via datastore helper
    ds_set_settings(payload)

    # Reload scoring settings so future calculations use the new values
    importlib.reload(scoring_module)

    # Bust caches after settings change
    _cache_clear_all()

    return {"status": "ok"}
#</getdata>


#<getdata>
@bp.route('/competitor')
def competitor():
    """Competitor page with searchable selectors and an empty results table.

    - Top: three searchable inputs (Sailor, Boat, Sail No) populated from fleet
    - Under: empty table scaffold for per-race results; races are provided in
      chronological order so row order can adapt to future changes without
      hard-coding
    """
    breadcrumbs = [('Competitor', None)]
    fleet = ds_get_fleet().get('competitors', [])
    sailors = sorted({(c.get('sailor_name') or '').strip() for c in fleet if (c.get('sailor_name') or '').strip()})
    boats = sorted({(c.get('boat_name') or '').strip() for c in fleet if (c.get('boat_name') or '').strip()})
    sail_nos = sorted({str((c.get('sail_no') or '')).strip() for c in fleet if str((c.get('sail_no') or '')).strip()})

    # Load all races and sort via get_races() chronological order
    races = _load_all_races() or []
    order = _race_order_map()
    if order:
        races_sorted = sorted(races, key=lambda r: order.get(r.get('race_id'), 10**9))
    else:
        def _key(r):
            d = r.get('date') or ''
            t = r.get('start_time') or ''
            return (d, t)
        races_sorted = sorted(races, key=_key)

    return render_template(
        'competitor.html',
        title='Competitor',
        breadcrumbs=breadcrumbs,
        sailors=sailors,
        boats=boats,
        sail_nos=sail_nos,
        races=races_sorted,
    )
#</getdata>


#<getdata>
@bp.route('/api/races/<race_id>', methods=['POST'])
def update_race(race_id):
    data = request.get_json() or {}
    series_choice = data.get('series_id')
    new_series_name = data.get('new_series_name')
    race_date = data.get('date')
    start_time = data.get('start_time')
    finish_times = data.get('finish_times', [])
    handicap_overrides = data.get('handicap_overrides', [])

    # Validate start_time format if provided
    if isinstance(start_time, str) and start_time.strip():
        try:
            _ = _parse_hms(start_time)
        except Exception:
            abort(400, description=f"Invalid start time '{start_time}'. Expected HH:MM:SS.")

    # Validate finish time formats in payload
    for ft in (finish_times or []):
        val = (ft or {}).get('finish_time')
        if isinstance(val, str) and val.strip():
            try:
                _ = _parse_hms(val)
            except Exception:
                who = (ft or {}).get('competitor_id') or 'unknown competitor'
                abort(400, description=f"Invalid finish time '{val}' for {who}. Expected HH:MM:SS.")

    store = load_data()

    # Validate incoming payload competitor IDs as integers existing in fleet
    fleet = ds_get_fleet().get('competitors', [])
    valid_ids = {int(c.get('competitor_id')) for c in (fleet or []) if c.get('competitor_id') is not None}

    def _parse_cid(val) -> int:
        try:
            cid = int(val)
        except Exception:
            abort(400, description=f"Invalid competitor id '{val}'. Must be an integer id from the Fleet.")
        if cid not in valid_ids:
            abort(400, description=f"Unknown competitor id: {cid}. Add to Fleet first.")
        return cid

    # Normalize finish_times and overrides to use integer ids
    finish_times = [
        {'competitor_id': _parse_cid(ft.get('competitor_id')), 'finish_time': ft.get('finish_time')}
        for ft in (finish_times or [])
    ]
    handicap_overrides = [
        {'competitor_id': _parse_cid(o.get('competitor_id')), 'handicap': o.get('handicap')}
        for o in (handicap_overrides or [])
    ]

    def _apply_overrides(entrants_list: list[dict]):
        if not handicap_overrides:
            return entrants_list
        ov_map = {o['competitor_id']: o.get('handicap') for o in handicap_overrides}
        for ent in entrants_list:
            cid = ent.get('competitor_id')
            if cid in ov_map:
                val = ov_map[cid]
                if val in (None, ''):
                    ent.pop('handicap_override', None)
                else:
                    try:
                        ent['handicap_override'] = int(val)
                    except (ValueError, TypeError):
                        ent.pop('handicap_override', None)
        return entrants_list

    if race_id == '__new__':
        if series_choice is None or not race_date:
            abort(400)
        start_time = start_time or ''
        timestamp = datetime.utcnow().isoformat() + 'Z'
        try:
            season_year = int(datetime.strptime(race_date, '%Y-%m-%d').year)
        except ValueError:
            abort(400)
        if series_choice == '__new__':
            if not new_series_name:
                abort(400)
            store, season_obj, series_obj = ds_ensure_series(season_year, new_series_name, data=store)
        else:
            _season_obj, series_obj = ds_find_series(series_choice, data=store)
            if not series_obj:
                abort(400)
        series_id_val = series_obj.get('series_id')
        # Build competitors from validated integer finish_times
        competitors: list[dict] = []
        for ft in finish_times:
            ent = {'competitor_id': ft['competitor_id'], 'finish_time': ft.get('finish_time')}
            competitors.append(ent)
        competitors = _apply_overrides(competitors)
        # competitor_ids are already canonical integers
        # Append new race, then renumber to assign id and sequence
        series_obj.setdefault('races', []).append({
            'race_id': '',
            'series_id': series_id_val,
            'name': '',
            'date': race_date,
            'start_time': start_time,
            'status': 'draft',
            'created_at': timestamp,
            'updated_at': timestamp,
            'competitors': competitors,
            'results': {},
            'race_no': 0,
        })
        mapping = ds_renumber_races(series_obj)
        # The last race is the one we added
        new_race = series_obj['races'][-1]
        new_race_id = new_race.get('race_id')
        # Persist
        # Persist only races/series/seasons to avoid touching settings unnecessarily
        save_data({'seasons': store.get('seasons', [])})
        recalculate_handicaps()
        finisher_count = sum(1 for ft in finish_times if ft.get('finish_time'))
        redirect_url = url_for('main.series_detail', series_id=series_id_val, race_id=new_race_id)
        return {'finisher_count': finisher_count, 'redirect': redirect_url}

    # Editing an existing race
    season_obj, series_obj, race_obj = ds_find_race(race_id, data=store)
    if not race_obj or not series_obj:
        abort(404)

    current_series_id = series_obj.get('series_id')
    redirect_url = None
    target_series = series_obj

    if series_choice:
        if series_choice == '__new__':
            if not new_series_name:
                abort(400)
            date_str = race_date or race_obj.get('date')
            if not date_str:
                abort(400)
            try:
                season_year = int(datetime.strptime(date_str, '%Y-%m-%d').year)
            except ValueError:
                abort(400)
            store, _season_new, target_series = ds_ensure_series(season_year, new_series_name, data=store)
        else:
            _s, ts = ds_find_series(series_choice, data=store)
            if not ts:
                abort(400)
            target_series = ts

        if target_series.get('series_id') != current_series_id:
            # Move race to target series
            series_obj['races'].remove(race_obj)
            race_obj['series_id'] = target_series.get('series_id')
            target_series.setdefault('races', []).append(race_obj)

    # Apply field edits
    if race_date is not None:
        race_obj['date'] = race_date
    if start_time is not None:
        race_obj['start_time'] = start_time
    if finish_times or handicap_overrides:
        ft_map = {ft['competitor_id']: ft.get('finish_time') for ft in (finish_times or [])}
        ov_map = {o['competitor_id']: o.get('handicap') for o in (handicap_overrides or [])}

        entrants_list = race_obj.setdefault('competitors', [])
        existing = {int(e.get('competitor_id')): e for e in entrants_list if e.get('competitor_id') is not None}

        # Helper: lookup baseline handicap directly from fleet by integer id
        fleet_map = {int(c.get('competitor_id')): c for c in fleet if c.get('competitor_id') is not None}
        def _baseline_for_cid(cid: int) -> int | None:
            comp = fleet_map.get(int(cid))
            if comp:
                return comp.get('starting_handicap_s_per_hr')
            return None

        # Update existing entrants with new finish times
        for cid, entrant in list(existing.items()):
            if cid in ft_map:
                entrant['finish_time'] = ft_map[cid]

        # Add new entrants that now have a finish time or an override
        for cid, finish in ft_map.items():
            if cid and cid not in existing and (finish not in (None, '')):
                ent = {'competitor_id': cid, 'finish_time': finish}
                base = _baseline_for_cid(cid)
                # If no override is provided for this competitor, ensure a starting handicap exists
                if (cid not in ov_map or ov_map.get(cid) in (None, '')) and base is None:
                    abort(400, description=f"No starting handicap available from the Fleet for competitor {cid}. Add a starting handicap or provide a race override.")
                if base is not None:
                    ent['initial_handicap'] = int(base)
                entrants_list.append(ent)
                existing[cid] = ent

        # Also add entrants that only have a handicap override
        for cid, handicap in ov_map.items():
            if cid and cid not in existing and (handicap not in (None, '')):
                ent = {'competitor_id': cid}
                base = _baseline_for_cid(cid)
                # When an override is provided, it's acceptable for baseline to be missing;
                # initial_handicap can be omitted or set from override for clarity.
                try:
                    ent['initial_handicap'] = int(handicap)
                except Exception:
                    if base is not None:
                        ent['initial_handicap'] = int(base)
                entrants_list.append(ent)
                existing[cid] = ent

        # Apply overrides across the (possibly expanded) entrant list
        _apply_overrides(entrants_list)

        # Ensure competitor ids are integers in entrants_list
        normalized_existing: list[dict] = []
        for ent in entrants_list:
            try:
                ent['competitor_id'] = int(ent.get('competitor_id'))
                normalized_existing.append(ent)
            except Exception:
                # Skip invalid entries
                continue
        race_obj['competitors'] = normalized_existing

    race_obj['updated_at'] = datetime.utcnow().isoformat() + 'Z'

    # Renumber races in affected series (and original if moved)
    mapping_target = ds_renumber_races(target_series)
    if target_series is not series_obj:
        ds_renumber_races(series_obj)

    # Persist and recalc
    # Persist only races/series/seasons to avoid unintended settings writes
    save_data({'seasons': store.get('seasons', [])})
    recalculate_handicaps()

    # Bust caches after race update
    _cache_clear_all()

    # Determine final race id after any renumber
    final_race_id = mapping_target.get(race_id, race_obj.get('race_id'))
    redirect_series_id = target_series.get('series_id')
    redirect_url = url_for('main.series_detail', series_id=redirect_series_id, race_id=final_race_id)
    finisher_count = sum(1 for e in race_obj.get('competitors', []) if e.get('finish_time'))
    return {'finisher_count': finisher_count, 'redirect': redirect_url}
#</getdata>


#<getdata>
@bp.route('/api/races/<race_id>', methods=['DELETE'])
def delete_race(race_id):
    store = load_data()
    season_obj, series_obj, race_obj = ds_find_race(race_id, data=store)
    if not race_obj or not series_obj:
        abort(404)
    series_id = series_obj.get('series_id')
    series_obj['races'].remove(race_obj)
    ds_renumber_races(series_obj)
    # Persist only races/series/seasons for deletion
    save_data({'seasons': store.get('seasons', [])})
    redirect_url = url_for('main.series_detail', series_id=series_id)
    # Bust caches after race deletion
    _cache_clear_all()
    return {'redirect': redirect_url}
#</getdata>
