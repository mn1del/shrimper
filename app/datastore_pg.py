import os
import json
import re
import uuid
from typing import Any, Dict, List, Optional, Tuple

import psycopg2
from psycopg2 import pool as pg_pool
from psycopg2.extras import RealDictCursor, execute_values
from psycopg2 import errors as pg_errors
from contextlib import contextmanager


_POOL: Optional[pg_pool.AbstractConnectionPool] = None

_COMPETITOR_CODE_MAXLEN = 20
_COMPETITOR_CODE_PREFIX = "C_"


def _generate_competitor_code(sail_no: Optional[str], existing_codes: set[str]) -> str:
    """Derive a unique VARCHAR identifier for competitors.competitor_id."""

    cleaned = ""
    if sail_no:
        cleaned = re.sub(r"[^A-Z0-9]", "", str(sail_no).upper())
    allowed_payload_len = max(_COMPETITOR_CODE_MAXLEN - len(_COMPETITOR_CODE_PREFIX), 0)
    if allowed_payload_len and cleaned:
        cleaned = cleaned[:allowed_payload_len]
    else:
        cleaned = cleaned[:allowed_payload_len] if cleaned else ""

    if cleaned:
        base = f"{_COMPETITOR_CODE_PREFIX}{cleaned}"
        if base not in existing_codes:
            existing_codes.add(base)
            return base
        suffix = 1
        while suffix < 10000:
            suffix_token = f"_{suffix}"
            max_base_len = _COMPETITOR_CODE_MAXLEN - len(suffix_token)
            candidate = f"{base[:max_base_len]}{suffix_token}"
            if candidate and candidate not in existing_codes:
                existing_codes.add(candidate)
                return candidate
            suffix += 1

    while True:
        token = uuid.uuid4().hex.upper()
        candidate = f"{_COMPETITOR_CODE_PREFIX}{token}"[:_COMPETITOR_CODE_MAXLEN]
        if candidate and candidate not in existing_codes:
            existing_codes.add(candidate)
            return candidate


def _env_int(name: str, default: Optional[int] = None) -> Optional[int]:
    val = os.environ.get(name)
    if val is None:
        return default
    try:
        return int(val)
    except Exception:
        return default


def _connect_kwargs() -> Dict[str, Any]:
    """Common connection kwargs: connect_timeout + TCP keepalives.

    Defaults:
      - connect_timeout: 10 seconds (overridable via DB_CONNECT_TIMEOUT)
      - keepalives: enabled by default; can be disabled by DB_KEEPALIVES=0
      - keepalive tunables applied if provided (IDLE/INTERVAL/COUNT)
    """
    kwargs: Dict[str, Any] = {}
    # Reasonable default timeout unless explicitly set via env
    ct_env = _env_int("DB_CONNECT_TIMEOUT")
    kwargs["connect_timeout"] = ct_env if ct_env is not None else 10

    # Keepalives: enable by default; allow explicit disable
    ka_env = os.environ.get("DB_KEEPALIVES")
    if ka_env is None:
        kwargs["keepalives"] = 1
    else:
        kwargs["keepalives"] = 0 if str(ka_env).lower() in ("0", "false") else 1

    idle = _env_int("DB_KEEPALIVES_IDLE")
    if idle is not None:
        kwargs["keepalives_idle"] = idle
    interval = _env_int("DB_KEEPALIVES_INTERVAL")
    if interval is not None:
        kwargs["keepalives_interval"] = interval
    count = _env_int("DB_KEEPALIVES_COUNT")
    if count is not None:
        kwargs["keepalives_count"] = count
    return kwargs


def init_pool(minconn: int = 1, maxconn: int = 10) -> None:
    """Initialize a global connection pool using DATABASE_URL.

    Safe to call multiple times; subsequent calls are ignored once a pool exists.
    """
    global _POOL
    if _POOL is not None:
        return
    url = os.environ.get("DATABASE_URL")
    if not url:
        # Leave _POOL as None; callers will fall back to direct connections
        return
    _POOL = pg_pool.ThreadedConnectionPool(minconn, maxconn, dsn=url, **_connect_kwargs())


@contextmanager
def _get_conn():
    """Yield a database connection from the pool if available, else direct.

    Returned object behaves like a psycopg2 connection within a context manager
    and may be used with nested "with conn.cursor() as cur:" blocks.
    """
    url = os.environ.get("DATABASE_URL")
    if not url:
        raise RuntimeError("DATABASE_URL not set; configure a PostgreSQL connection string")
    if _POOL is not None:
        retried = False
        while True:
            conn = _POOL.getconn()
            # Lightweight liveness check: SELECT 1
            healthy = True
            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
                # Clear implicit transaction started by SELECT when autocommit is off
                try:
                    if not getattr(conn, "autocommit", False):
                        conn.rollback()
                except Exception:
                    pass
            except (psycopg2.OperationalError, psycopg2.InterfaceError):
                healthy = False
            except Exception:
                # Treat unexpected ping errors as unhealthy to be safe
                healthy = False

            if not healthy:
                # Discard the broken connection and retry once
                try:
                    _POOL.putconn(conn, close=True)
                except Exception:
                    pass
                if retried:
                    # Second failure: surface error
                    raise psycopg2.OperationalError("Failed to acquire healthy DB connection after retry")
                retried = True
                continue

            # Healthy: yield and ensure cleanup + return to pool afterwards
            try:
                try:
                    yield conn
                except Exception:
                    try:
                        conn.rollback()
                    except Exception:
                        pass
                    raise
            finally:
                # Ensure connection not left in a transaction
                try:
                    if getattr(conn, "closed", 0) == 0 and not getattr(conn, "autocommit", False):
                        # status 0 = idle, 1 = active, 2 = intrans, 3 = inerror
                        if getattr(conn, "status", 0) in (1, 2, 3):
                            try:
                                conn.rollback()
                            except Exception:
                                pass
                finally:
                    _POOL.putconn(conn)
            break
    else:
        conn = psycopg2.connect(url, **_connect_kwargs())
        try:
            try:
                yield conn
            except Exception:
                try:
                    conn.rollback()
                except Exception:
                    pass
                raise
        finally:
            try:
                conn.close()
            except Exception:
                pass


def _time_to_str(val) -> Optional[str]:
    if val is None:
        return None
    # psycopg2 returns datetime.time
    try:
        return val.strftime("%H:%M:%S")
    except Exception:
        return str(val)


def load_data() -> Dict[str, Any]:
    """Materialize the full JSON structure from PostgreSQL.

    Returns a dict compatible with the JSON datastore structure:
    {"fleet": {"competitors": [...]}, "seasons": [...], "settings": {...}}
    """
    out: Dict[str, Any] = {"fleet": {"competitors": []}, "seasons": [], "settings": {}}

    with _get_conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        # Settings (prefer stored JSON config if available); be tolerant of older schemas
        try:
            cur.execute("SELECT config FROM settings ORDER BY id DESC LIMIT 1")
            row = cur.fetchone()
            if row and row.get("config"):
                out["settings"] = row["config"]
            else:
                # Try to assemble minimal settings from split columns if they exist
                try:
                    cur.execute(
                        "SELECT handicap_delta_by_rank, league_points_by_rank, fleet_size_factor FROM settings ORDER BY id DESC LIMIT 1"
                    )
                    r2 = cur.fetchone() or {}
                    out["settings"] = {
                        "handicap_delta_by_rank": r2.get("handicap_delta_by_rank") or [],
                        "league_points_by_rank": r2.get("league_points_by_rank") or [],
                        "fleet_size_factor": r2.get("fleet_size_factor") or [],
                    }
                except Exception as e2:
                    # Columns may not exist yet; leave settings empty
                    if not isinstance(e2, getattr(pg_errors, "UndefinedColumn", tuple())):
                        raise
        except Exception as e:  # settings table may not exist yet
            if not isinstance(e, getattr(pg_errors, "UndefinedTable", tuple())):
                raise

        # Fleet: emit canonical integer IDs from competitors.id
        try:
            cur.execute(
                """
                SELECT id AS competitor_id, sailor_name, boat_name, sail_no,
                       starting_handicap_s_per_hr, current_handicap_s_per_hr
                FROM competitors
                ORDER BY sail_no NULLS LAST, id
                """
            )
            comps = []
            for r in cur.fetchall():
                comps.append(
                    {
                        "competitor_id": r.get("competitor_id"),  # int
                        "sailor_name": r.get("sailor_name"),
                        "boat_name": r.get("boat_name"),
                        "sail_no": r.get("sail_no"),
                        "starting_handicap_s_per_hr": r.get("starting_handicap_s_per_hr") or 0,
                        "current_handicap_s_per_hr": r.get("current_handicap_s_per_hr") or r.get("starting_handicap_s_per_hr") or 0,
                    }
                )
            out["fleet"]["competitors"] = comps
        except Exception as e:
            if not isinstance(e, getattr(pg_errors, "UndefinedTable", tuple())):
                raise

        # Seasons + Series + Races -> Entrants (optimized in 2 round-trips)
        joined_rows: List[Dict[str, Any]] = []
        try:
            cur.execute(
                """
                SELECT s.year AS season_year,
                       se.series_id AS series_id,
                       se.name AS series_name,
                       COALESCE(se.year, s.year) AS series_year,
                       r.race_id AS race_id,
                       r.name AS race_name,
                       r.date AS race_date,
                       r.start_time AS start_time,
                       r.race_no AS race_no
                FROM seasons s
                LEFT JOIN series se ON se.season_id = s.id
                LEFT JOIN races r ON r.series_id = se.series_id
                ORDER BY s.year, se.name, r.date NULLS LAST, r.start_time NULLS LAST, r.race_id
                """
            )
            joined_rows = cur.fetchall() or []
        except Exception as e:
            if not isinstance(e, getattr(pg_errors, "UndefinedTable", tuple())):
                raise

        race_ids = [row.get("race_id") for row in joined_rows if row.get("race_id")]
        results_by_race: Dict[str, List[Dict[str, Any]]] = {}
        results_by_race_maps: Dict[str, Dict[int, Dict[str, Any]]] = {}
        if race_ids:
            try:
                cur.execute(
                    """
                    SELECT race_id, competitor_ref AS competitor_id, initial_handicap, finish_time, handicap_override
                    FROM race_results
                    WHERE race_id = ANY(%s)
                    ORDER BY race_id, competitor_ref
                    """,
                    (race_ids,),
                )
                for ent in cur.fetchall() or []:
                    rid = ent.get("race_id")
                    if not rid:
                        continue
                    cid = ent.get("competitor_id")  # int
                    entry = {
                        "competitor_id": cid,
                        "initial_handicap": ent.get("initial_handicap"),
                        "finish_time": _time_to_str(ent.get("finish_time")),
                        "handicap_override": ent.get("handicap_override"),
                    }
                    m = results_by_race_maps.setdefault(rid, {})
                    prev = m.get(int(cid) if cid is not None else None)
                    if prev is None:
                        if cid is not None:
                            m[int(cid)] = entry
                    else:
                        # Prefer row with a finish_time or an override
                        if (prev.get("finish_time") is None and entry.get("finish_time") is not None) or (
                            prev.get("handicap_override") is None and entry.get("handicap_override") is not None
                        ):
                            m[int(cid)] = entry
            except Exception as e:
                if not isinstance(e, getattr(pg_errors, "UndefinedTable", tuple())):
                    raise
        # Convert maps to lists
        for rid, cmap in results_by_race_maps.items():
            results_by_race[rid] = list(cmap.values())

        seasons_map: Dict[int, Dict[str, Any]] = {}
        series_map: Dict[tuple[int, str], Dict[str, Any]] = {}
        for row in joined_rows:
            y = row.get("season_year")
            if y is None:
                continue
            year = int(y)
            season_obj = seasons_map.setdefault(year, {"year": year, "series": []})
            sid = row.get("series_id")
            if not sid:
                continue  # season with no series/races
            key = (year, sid)
            series_obj = series_map.get(key)
            if series_obj is None:
                series_obj = {
                    "series_id": sid,
                    "name": row.get("series_name"),
                    "season": int(row.get("series_year") or year),
                    "races": [],
                }
                series_map[key] = series_obj
                season_obj["series"].append(series_obj)
            rid = row.get("race_id")
            if not rid:
                continue
            race_obj = {
                "race_id": rid,
                "series_id": sid,
                "name": row.get("race_name"),
                "date": (row.get("race_date").isoformat() if row.get("race_date") else None),
                "start_time": _time_to_str(row.get("start_time")),
                "race_no": row.get("race_no"),
                "competitors": results_by_race.get(rid, []),
            }
            series_obj["races"].append(race_obj)
        out["seasons"] = [seasons_map[k] for k in sorted(seasons_map.keys())]

    return out


def save_data(data: Dict[str, Any]) -> None:
    """Persist the provided JSON-like structure into PostgreSQL.

    Optimized to upsert only the provided sections without wholesale deletes.
    For races, deletes are targeted by comparing race_id sets.
    """
    with _get_conn() as conn:
        with conn.cursor() as cur:
            # Settings
            if "settings" in data and data["settings"] is not None:
                settings = data["settings"]
                try:
                    cur.execute("DELETE FROM settings")
                except Exception as e:
                    if not isinstance(e, getattr(pg_errors, "UndefinedTable", tuple())):
                        raise
                try:
                    cur.execute(
                        """
                        INSERT INTO settings (version, updated_at, handicap_delta_by_rank, league_points_by_rank, fleet_size_factor, config)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        """,
                        (
                            settings.get("version"),
                            settings.get("updated_at"),
                            json.dumps(settings.get("handicap_delta_by_rank", [])),
                            json.dumps(settings.get("league_points_by_rank", [])),
                            json.dumps(settings.get("fleet_size_factor", [])),
                            json.dumps(settings),
                        ),
                    )
                except Exception as e:
                    if isinstance(e, getattr(pg_errors, "UndefinedColumn", tuple())) or isinstance(e, getattr(pg_errors, "UndefinedTable", tuple())):
                        # The first INSERT failed due to schema shape; rollback the
                        # failed statement so we can run a simplified fallback.
                        try:
                            conn.rollback()
                        except Exception:
                            pass
                        with conn.cursor() as cur2:
                            cur2.execute(
                                "INSERT INTO settings (config) VALUES (%s)",
                                (json.dumps(settings),),
                            )
                    else:
                        raise

            # Fleet
            if "fleet" in data and data["fleet"] is not None:
                fleet = data["fleet"] or {"competitors": []}
                competitors = fleet.get("competitors", []) or []
                # Upsert by integer id; do not delete existing rows to preserve history
                for comp in competitors:
                    cid = comp.get("competitor_id")
                    sailor = comp.get("sailor_name")
                    boat = comp.get("boat_name")
                    sail_no = comp.get("sail_no")
                    start_h = int(comp.get("starting_handicap_s_per_hr") or 0)
                    curr_h = int(comp.get("current_handicap_s_per_hr") or start_h)
                    if cid is None:
                        cur.execute(
                            """
                            INSERT INTO competitors (sailor_name, boat_name, sail_no, starting_handicap_s_per_hr, current_handicap_s_per_hr)
                            VALUES (%s, %s, %s, %s, %s)
                            """,
                            (sailor, boat, sail_no, start_h, curr_h),
                        )
                    else:
                        cur.execute(
                            """
                            INSERT INTO competitors (id, sailor_name, boat_name, sail_no, starting_handicap_s_per_hr, current_handicap_s_per_hr)
                            VALUES (%s, %s, %s, %s, %s, %s)
                            ON CONFLICT (id) DO UPDATE SET
                                sailor_name = EXCLUDED.sailor_name,
                                boat_name = EXCLUDED.boat_name,
                                sail_no = EXCLUDED.sail_no,
                                starting_handicap_s_per_hr = EXCLUDED.starting_handicap_s_per_hr,
                                current_handicap_s_per_hr = EXCLUDED.current_handicap_s_per_hr
                            """,
                            (int(cid), sailor, boat, sail_no, start_h, curr_h),
                        )

            # Seasons / Series / Races / Results
            if "seasons" in data and data["seasons"] is not None:
                seasons = data.get("seasons", [])

                # Build target sets
                target_race_ids = set()
                target_series = []
                for season in seasons:
                    y = season.get("year")
                    if y is None:
                        continue
                    try:
                        cur.execute(
                            "INSERT INTO seasons (year) VALUES (%s) ON CONFLICT (year) DO NOTHING",
                            (int(y),),
                        )
                    except Exception:
                        pass
                    # get season id
                    cur.execute("SELECT id FROM seasons WHERE year = %s", (int(y),))
                    row = cur.fetchone()
                    season_id = row[0] if row else None
                    for series in season.get("series", []) or []:
                        sid = series.get("series_id")
                        name = series.get("name")
                        target_series.append((sid, name, season_id, int(y)))
                        cur.execute(
                            """
                            INSERT INTO series (series_id, name, season_id, year)
                            VALUES (%s, %s, %s, %s)
                            ON CONFLICT (series_id) DO UPDATE SET name = EXCLUDED.name, season_id = EXCLUDED.season_id, year = EXCLUDED.year
                            """,
                            (sid, name, season_id, int(y)),
                        )
                        for race in series.get("races", []) or []:
                            rid = race.get("race_id")
                            target_race_ids.add(rid)
                            cur.execute(
                                """
                                INSERT INTO races (race_id, series_id, name, date, start_time, race_no)
                                VALUES (%s, %s, %s, %s, %s, %s)
                                ON CONFLICT (race_id) DO UPDATE SET
                                    series_id = EXCLUDED.series_id,
                                    name = EXCLUDED.name,
                                    date = EXCLUDED.date,
                                    start_time = EXCLUDED.start_time,
                                    race_no = EXCLUDED.race_no
                                """,
                                (
                                    rid,
                                    series.get("series_id"),
                                    race.get("name"),
                                    race.get("date"),
                                    race.get("start_time"),
                                    race.get("race_no"),
                                ),
                            )
                            # Replace entrants for this race only when explicitly provided
                            if "competitors" in race:
                                cur.execute("DELETE FROM race_results WHERE race_id = %s", (rid,))
                                for ent in (race.get("competitors") or []):
                                    # Normalize finish_time: empty/whitespace -> NULL for TIME columns
                                    _ft = ent.get("finish_time")
                                    finish_val = None
                                    if _ft is not None:
                                        s = str(_ft).strip()
                                        finish_val = None if s == "" else s
                                    cid = ent.get("competitor_id")
                                    cid_int = int(cid) if cid is not None else None
                                    cur.execute(
                                        """
                                        INSERT INTO race_results (race_id, competitor_ref, initial_handicap, finish_time, handicap_override)
                                        VALUES (%s, %s, %s, %s, %s)
                                        ON CONFLICT (race_id, competitor_ref) DO UPDATE SET
                                            initial_handicap = EXCLUDED.initial_handicap,
                                            finish_time = EXCLUDED.finish_time,
                                            handicap_override = EXCLUDED.handicap_override
                                        """,
                                        (
                                            rid,
                                            cid_int,
                                            ent.get("initial_handicap"),
                                            finish_val,
                                            ent.get("handicap_override"),
                                        ),
                                    )

                # Delete races no longer present (handles race deletions/renames)
                try:
                    cur.execute("SELECT race_id FROM races")
                    existing_rids = {row[0] for row in cur.fetchall()}
                except Exception:
                    existing_rids = set()
                to_delete = list(existing_rids - target_race_ids)
                if to_delete:
                    cur.execute(
                        "DELETE FROM race_results WHERE race_id = ANY(%s)",
                        (to_delete,),
                    )
                    cur.execute(
                        "DELETE FROM races WHERE race_id = ANY(%s)",
                        (to_delete,),
                    )

        conn.commit()


def list_seasons(data: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    # Return seasons with their series metadata (without expanding races)
    seasons: List[Dict[str, Any]] = []
    with _get_conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        try:
            cur.execute("SELECT id, year FROM seasons ORDER BY year")
        except Exception as e:
            if isinstance(e, getattr(pg_errors, "UndefinedTable", tuple())):
                return []
            raise
        for s in cur.fetchall():
            seasons.append({"year": int(s["year"]), "series": []})
    return seasons


def list_series(data: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    # Flattened list of series objects
    out: List[Dict[str, Any]] = []
    with _get_conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        try:
            cur.execute("SELECT series_id, name, year FROM series ORDER BY year, name")
        except Exception as e:
            if isinstance(e, getattr(pg_errors, "UndefinedTable", tuple())):
                return []
            raise
        for r in cur.fetchall():
            out.append({"series_id": r["series_id"], "name": r["name"], "season": int(r["year"])})
    return out


def find_series(series_id: str, data: Optional[Dict[str, Any]] = None) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    with _get_conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT s.id AS season_db_id, s.year AS season_year, se.series_id, se.name
            FROM series se JOIN seasons s ON s.id = se.season_id
            WHERE LOWER(se.series_id) = LOWER(%s)
            """,
            (series_id,),
        )
        row = cur.fetchone()
        if not row:
            return None, None
        season = {"year": int(row["season_year"]), "series": []}
        series = {"series_id": row["series_id"], "name": row["name"], "season": int(row["season_year"]), "races": []}
        # Fetch races for completeness
        cur.execute(
            "SELECT race_id, name, date, start_time, race_no FROM races WHERE series_id = %s ORDER BY date, start_time",
            (row["series_id"],),
        )
        for r in cur.fetchall():
            series["races"].append(
                {
                    "race_id": r["race_id"],
                    "series_id": row["series_id"],
                    "name": r.get("name"),
                    "date": r.get("date").isoformat() if r.get("date") else None,
                    "start_time": _time_to_str(r.get("start_time")),
                    "race_no": r.get("race_no"),
                }
            )
        return season, series


def find_race(race_id: str, data: Optional[Dict[str, Any]] = None) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    with _get_conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT r.race_id, r.series_id, r.name, r.date, r.start_time, r.race_no,
                   se.name AS series_name, se.year AS season_year
            FROM races r JOIN series se ON se.series_id = r.series_id
            WHERE r.race_id = %s
            """,
            (race_id,),
        )
        rr = cur.fetchone()
        if not rr:
            return None, None, None
        season = {"year": int(rr["season_year"]), "series": []}
        series = {"series_id": rr["series_id"], "name": rr["series_name"], "season": int(rr["season_year"]), "races": []}
        race = {
            "race_id": rr["race_id"],
            "series_id": rr["series_id"],
            "name": rr.get("name"),
            "date": rr.get("date").isoformat() if rr.get("date") else None,
            "start_time": _time_to_str(rr.get("start_time")),
            "race_no": rr.get("race_no"),
            "competitors": [],
        }
        # Read entrants with canonical integer ids
        cur.execute(
            "SELECT competitor_ref AS competitor_id, initial_handicap, finish_time, handicap_override FROM race_results WHERE race_id = %s ORDER BY competitor_ref",
            (race_id,),
        )
        for ent in cur.fetchall() or []:
            race["competitors"].append(
                {
                    "competitor_id": ent.get("competitor_id"),  # int
                    "initial_handicap": ent.get("initial_handicap"),
                    "finish_time": _time_to_str(ent.get("finish_time")),
                    "handicap_override": ent.get("handicap_override"),
                }
            )
        return season, series, race


def list_all_races(data: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    with _get_conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        try:
            cur.execute(
                """
                SELECT r.race_id,
                       r.date,
                       r.start_time,
                       s.name AS series_name,
                       r.series_id,
                       s.year AS season,
                       COUNT(rr.finish_time) FILTER (WHERE rr.finish_time IS NOT NULL) AS finishers
                FROM races r
                JOIN series s ON s.series_id = r.series_id
                LEFT JOIN race_results rr ON rr.race_id = r.race_id
                GROUP BY r.race_id, r.date, r.start_time, s.name, r.series_id, s.year
                ORDER BY r.date DESC NULLS LAST, r.start_time DESC NULLS LAST
                """
            )
        except Exception as e:
            if isinstance(e, getattr(pg_errors, "UndefinedTable", tuple())):
                return []
            raise
        for r in cur.fetchall():
            out.append(
                {
                    "race_id": r["race_id"],
                    "date": r.get("date").isoformat() if r.get("date") else None,
                    "start_time": _time_to_str(r.get("start_time")),
                    "series_name": r.get("series_name"),
                    "series_id": r.get("series_id"),
                    "finishers": int(r.get("finishers") or 0),
                    "season": int(r.get("season")) if r.get("season") is not None else None,
                }
            )
    return out


def get_races() -> List[str]:
    """Return race IDs in chronological order by date then start_time.

    - Orders ascending (earliest first)
    - Places NULL dates/times last for deterministic ordering
    - Breaks ties by race_id for stability
    """
    ids: List[str] = []
    with _get_conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        try:
            cur.execute(
                """
                SELECT r.race_id
                FROM races r
                ORDER BY r.date ASC NULLS LAST,
                         r.start_time ASC NULLS LAST,
                         r.race_id ASC
                """
            )
        except Exception as e:
            if isinstance(e, getattr(pg_errors, "UndefinedTable", tuple())):
                return []
            raise
        for r in cur.fetchall() or []:
            rid = r.get("race_id")
            if rid:
                ids.append(rid)
    return ids

def list_season_race_ids(season_year: int) -> List[str]:
    """Return race_ids for a given season ordered chronologically.

    Orders by date ASC NULLS LAST, start_time ASC NULLS LAST, then race_id ASC.
    """
    ids: List[str] = []
    with _get_conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        try:
            cur.execute(
                """
                SELECT r.race_id
                FROM races r
                JOIN series se ON se.series_id = r.series_id
                JOIN seasons s ON s.id = se.season_id
                WHERE s.year = %s
                ORDER BY r.date ASC NULLS LAST,
                         r.start_time ASC NULLS LAST,
                         r.race_id ASC
                """,
                (int(season_year),),
            )
        except Exception as e:
            if isinstance(e, getattr(pg_errors, "UndefinedTable", tuple())):
                return []
            raise
        for r in cur.fetchall() or []:
            rid = r.get("race_id")
            if rid:
                ids.append(rid)
    return ids

def list_season_races_with_results(season_year: int, data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Return a single season object with its series and races (with entrants).

    Queries the DB in two round-trips: one join for seasons/series/races filtered
    by the given year, and one bulk fetch of race_results for those races.
    """
    season_obj: Dict[str, Any] = {"year": int(season_year), "series": []}
    with _get_conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        rows: List[Dict[str, Any]] = []
        try:
            cur.execute(
                """
                SELECT s.year AS season_year,
                       se.series_id AS series_id,
                       se.name AS series_name,
                       COALESCE(se.year, s.year) AS series_year,
                       r.race_id AS race_id,
                       r.name AS race_name,
                       r.date AS race_date,
                       r.start_time AS start_time,
                       r.race_no AS race_no
                FROM seasons s
                LEFT JOIN series se ON se.season_id = s.id
                LEFT JOIN races r ON r.series_id = se.series_id
                WHERE s.year = %s
                ORDER BY se.name, r.date NULLS LAST, r.start_time NULLS LAST, r.race_id
                """,
                (int(season_year),),
            )
            rows = cur.fetchall() or []
        except Exception as e:
            if not isinstance(e, getattr(pg_errors, "UndefinedTable", tuple())):
                raise

        race_ids = [row.get("race_id") for row in rows if row.get("race_id")]
        results_by_race: Dict[str, List[Dict[str, Any]]] = {}
        if race_ids:
            try:
                cur.execute(
                    """
                    SELECT race_id, competitor_ref AS competitor_id, initial_handicap, finish_time, handicap_override
                    FROM race_results
                    WHERE race_id = ANY(%s)
                    ORDER BY race_id, competitor_ref
                    """,
                    (race_ids,),
                )
                rows_rr = cur.fetchall() or []
                rr_maps: Dict[str, Dict[int, Dict[str, Any]]] = {}
                for ent in rows_rr:
                    rid = ent.get("race_id")
                    if not rid:
                        continue
                    cid = ent.get("competitor_id")  # int
                    entry = {
                        "competitor_id": cid,
                        "initial_handicap": ent.get("initial_handicap"),
                        "finish_time": _time_to_str(ent.get("finish_time")),
                        "handicap_override": ent.get("handicap_override"),
                    }
                    m = rr_maps.setdefault(rid, {})
                    prev = m.get(int(cid) if cid is not None else None)
                    if prev is None or (
                        (prev.get("finish_time") is None and entry.get("finish_time") is not None)
                        or (prev.get("handicap_override") is None and entry.get("handicap_override") is not None)
                    ):
                        if cid is not None:
                            m[int(cid)] = entry
                for rid, cmap in rr_maps.items():
                    results_by_race[rid] = list(cmap.values())
            except Exception as e:
                if not isinstance(e, getattr(pg_errors, "UndefinedTable", tuple())):
                    raise

        series_map: Dict[str, Dict[str, Any]] = {}
        for row in rows:
            sid = row.get("series_id")
            if not sid:
                continue
            series_obj = series_map.get(sid)
            if series_obj is None:
                series_obj = {
                    "series_id": sid,
                    "name": row.get("series_name"),
                    "season": int(row.get("series_year") or season_year),
                    "races": [],
                }
                series_map[sid] = series_obj
                season_obj["series"].append(series_obj)
            rid = row.get("race_id")
            if not rid:
                continue
            race_obj = {
                "race_id": rid,
                "series_id": sid,
                "name": row.get("race_name"),
                "date": (row.get("race_date").isoformat() if row.get("race_date") else None),
                "start_time": _time_to_str(row.get("start_time")),
                "race_no": row.get("race_no"),
                "competitors": results_by_race.get(rid, []),
            }
            series_obj["races"].append(race_obj)
    return season_obj


# The following helpers mirror the JSON datastore behavior for in-memory data
def ensure_season(year: int, data: Optional[Dict[str, Any]] = None) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    d = data or load_data()
    seasons = d.setdefault("seasons", [])
    for season in seasons:
        if int(season.get("year")) == int(year):
            return d, season
    season = {"year": int(year), "series": []}
    seasons.append(season)
    return d, season


def ensure_series(year: int, name: str, series_id: Optional[str] = None, data: Optional[Dict[str, Any]] = None) -> Tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any]]:
    d, season = ensure_season(year, data or load_data())
    for s in season.get("series", []):
        if (s.get("name") == name) or (series_id and s.get("series_id") == series_id):
            return d, season, s
    sid = series_id or f"SER_{year}_{name}"
    series = {"series_id": sid, "name": name, "season": int(year), "races": []}
    season["series"].append(series)
    return d, season, series


def renumber_races(series: Dict[str, Any]) -> Dict[str, str]:
    races = series.setdefault("races", [])
    races.sort(key=lambda r: (r.get("date") or "", r.get("start_time") or ""))
    mapping: Dict[str, str] = {}
    name = series.get("name") or ""
    sid = series.get("series_id") or ""
    for idx, race in enumerate(races, start=1):
        old = race.get("race_id")
        date = race.get("date") or ""
        new_id = f"RACE_{date}_{name}_{idx}"
        race["race_id"] = new_id
        race["race_no"] = idx
        if sid:
            race["name"] = f"{sid}_{idx}"
        if old and old != new_id:
            mapping[old] = new_id
    return mapping


def get_fleet(data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    # Targeted SELECT to avoid materializing all data
    if data is not None:
        return data.get("fleet", {"competitors": []})
    with _get_conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        try:
            cur.execute(
                """
                SELECT id AS competitor_id, sailor_name, boat_name, sail_no,
                       starting_handicap_s_per_hr, current_handicap_s_per_hr
                FROM competitors
                ORDER BY sail_no NULLS LAST, id
                """
            )
        except Exception as e:
            if isinstance(e, getattr(pg_errors, "UndefinedTable", tuple())):
                return {"competitors": []}
            raise
        comps: List[Dict[str, Any]] = []
        for r in cur.fetchall():
            comps.append(
                {
                    "competitor_id": r.get("competitor_id"),  # int
                    "sailor_name": r.get("sailor_name"),
                    "boat_name": r.get("boat_name"),
                    "sail_no": r.get("sail_no"),
                    "starting_handicap_s_per_hr": r.get("starting_handicap_s_per_hr") or 0,
                    "current_handicap_s_per_hr": r.get("current_handicap_s_per_hr") or r.get("starting_handicap_s_per_hr") or 0,
                }
            )
        return {"competitors": comps}


def set_fleet(fleet: Dict[str, Any], data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Upsert fleet rows and remove competitors omitted from payload.

    When ``data`` is provided (JSON fixture path), the structure is updated
    in-place. Otherwise, the PostgreSQL backend is used.
    """
    competitors_in = (fleet or {}).get("competitors", []) or []

    # In-memory path used by tests
    if data is not None:
        data["competitors"] = competitors_in
        return {"competitors": competitors_in}

    competitors_out: List[Dict[str, Any]] = []
    deleted_ids: List[int] = []
    with _get_conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        existing_rows: List[Dict[str, Any]] = []
        existing_ids: set[int] = set()
        existing_codes: set[str] = set()
        existing_codes_by_id: Dict[int, str] = {}
        try:
            cur.execute("SELECT id, competitor_id FROM competitors")
            existing_rows = cur.fetchall() or []
        except Exception as exc:
            if isinstance(exc, getattr(pg_errors, "UndefinedColumn", tuple())):
                cur.execute("SELECT id FROM competitors")
                existing_rows = cur.fetchall() or []
            else:
                raise
        for row in existing_rows:
            rid = row.get("id")
            code = row.get("competitor_id")
            parsed_id: Optional[int] = None
            if rid is not None:
                try:
                    parsed_id = int(rid)
                    existing_ids.add(parsed_id)
                except Exception:
                    parsed_id = None
            if code:
                str_code = str(code)
                existing_codes.add(str_code)
                if parsed_id is not None:
                    existing_codes_by_id[parsed_id] = str_code
        retained_ids: set[int] = set()

        for comp in competitors_in:
            cid = comp.get("competitor_id")
            sailor = comp.get("sailor_name")
            boat = comp.get("boat_name")
            sail_no = comp.get("sail_no")
            start_raw = comp.get("starting_handicap_s_per_hr")
            curr_raw = comp.get("current_handicap_s_per_hr")
            try:
                start_h = int(start_raw or 0)
            except Exception as exc:  # pragma: no cover - guarded earlier
                raise ValueError(f"Invalid starting handicap for competitor {sailor or boat or sail_no}: {start_raw}") from exc
            try:
                curr_h = int(curr_raw if curr_raw is not None else start_h)
            except Exception as exc:  # pragma: no cover - guarded earlier
                raise ValueError(f"Invalid current handicap for competitor {sailor or boat or sail_no}: {curr_raw}") from exc

            if cid is None:
                generated_code = _generate_competitor_code(sail_no, existing_codes)
                cur.execute(
                    """
                    INSERT INTO competitors (
                        competitor_id,
                        sailor_name,
                        boat_name,
                        sail_no,
                        starting_handicap_s_per_hr,
                        current_handicap_s_per_hr
                    )
                    VALUES (%s, %s, %s, %s, %s, %s)
                    RETURNING id, competitor_id
                    """,
                    (generated_code, sailor, boat, sail_no, start_h, curr_h),
                )
                row = cur.fetchone()
                if row:
                    cid = row.get("id")
                    code = row.get("competitor_id")
                    if code:
                        str_code = str(code)
                        existing_codes.add(str_code)
                        try:
                            existing_codes_by_id[int(cid)] = str_code
                        except Exception:
                            pass
            else:
                try:
                    cid = int(cid)
                except Exception:
                    # Skip invalid ids so we do not corrupt race references
                    continue
                competitor_code = existing_codes_by_id.get(int(cid))
                if not competitor_code:
                    competitor_code = _generate_competitor_code(sail_no, existing_codes)
                    try:
                        existing_codes_by_id[int(cid)] = competitor_code
                    except Exception:
                        pass
                cur.execute(
                    """
                    INSERT INTO competitors (id, competitor_id, sailor_name, boat_name, sail_no, starting_handicap_s_per_hr, current_handicap_s_per_hr)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (id) DO UPDATE SET
                        competitor_id = EXCLUDED.competitor_id,
                        sailor_name = EXCLUDED.sailor_name,
                        boat_name = EXCLUDED.boat_name,
                        sail_no = EXCLUDED.sail_no,
                        starting_handicap_s_per_hr = EXCLUDED.starting_handicap_s_per_hr,
                        current_handicap_s_per_hr = EXCLUDED.current_handicap_s_per_hr
                    """,
                    (cid, competitor_code, sailor, boat, sail_no, start_h, curr_h),
                )

            if cid is None:
                continue

            retained_ids.add(int(cid))
            competitors_out.append(
                {
                    "competitor_id": int(cid),
                    "sailor_name": sailor,
                    "boat_name": boat,
                    "sail_no": sail_no,
                    "starting_handicap_s_per_hr": start_h,
                    "current_handicap_s_per_hr": curr_h,
                }
            )

        to_delete = sorted(existing_ids - retained_ids)
        if to_delete:
            protected_ids: List[int] = []
            try:
                cur.execute(
                    "SELECT DISTINCT competitor_ref FROM race_results WHERE competitor_ref = ANY(%s)",
                    (to_delete,),
                )
                protected_ids = [int(row["competitor_ref"]) for row in cur.fetchall() if row.get("competitor_ref") is not None]
            except Exception:
                try:
                    cur.execute(
                        "SELECT DISTINCT competitor_id FROM race_results WHERE competitor_id = ANY(%s)",
                        (to_delete,),
                    )
                    protected_ids = [int(row["competitor_id"]) for row in cur.fetchall() if row.get("competitor_id") is not None]
                except Exception:
                    protected_ids = []

            if protected_ids:
                protected_ids.sort()
                raise ValueError(
                    "Cannot delete competitors with recorded race results: "
                    + ", ".join(str(pid) for pid in protected_ids)
                )

            if to_delete:
                cur.execute("DELETE FROM competitors WHERE id = ANY(%s)", (to_delete,))
                deleted_ids = to_delete

        conn.commit()

    result: Dict[str, Any] = {"competitors": competitors_out}
    if deleted_ids:
        result["deleted_ids"] = deleted_ids
    return result


def get_settings(data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    if data is not None:
        return data.get("settings", {})
    with _get_conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        try:
            cur.execute("SELECT config FROM settings ORDER BY id DESC LIMIT 1")
            row = cur.fetchone()
            if row and row.get("config"):
                return row["config"]
        except Exception as e:
            if not isinstance(e, getattr(pg_errors, "UndefinedTable", tuple())):
                raise
        # Fallback on older split columns
        try:
            cur.execute(
                "SELECT handicap_delta_by_rank, league_points_by_rank, fleet_size_factor FROM settings ORDER BY id DESC LIMIT 1"
            )
            r2 = cur.fetchone() or {}
            return {
                "handicap_delta_by_rank": r2.get("handicap_delta_by_rank") or [],
                "league_points_by_rank": r2.get("league_points_by_rank") or [],
                "fleet_size_factor": r2.get("fleet_size_factor") or [],
            }
        except Exception:
            return {}


def set_settings(settings: Dict[str, Any], data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    with _get_conn() as conn, conn.cursor() as cur:
        try:
            cur.execute("DELETE FROM settings")
        except Exception as e:
            if not isinstance(e, getattr(pg_errors, "UndefinedTable", tuple())):
                raise
        try:
            cur.execute(
                """
                INSERT INTO settings (version, updated_at, handicap_delta_by_rank, league_points_by_rank, fleet_size_factor, config)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (
                    settings.get("version"),
                    settings.get("updated_at"),
                    json.dumps(settings.get("handicap_delta_by_rank", [])),
                    json.dumps(settings.get("league_points_by_rank", [])),
                    json.dumps(settings.get("fleet_size_factor", [])),
                    json.dumps(settings),
                ),
            )
        except Exception as e:
            if isinstance(e, getattr(pg_errors, "UndefinedColumn", tuple())) or isinstance(e, getattr(pg_errors, "UndefinedTable", tuple())):
                cur.execute("INSERT INTO settings (config) VALUES (%s)", (json.dumps(settings),))
            else:
                raise
        conn.commit()
    return settings


def normalize_competitor_ids() -> Dict[str, Any]:
    """Deprecated: integer competitor ids enforced; no-op for compatibility."""
    return {"updated": 0, "merged": 0, "deleted": 0, "skipped": 0}


def apply_recalculated_handicaps(
    pre_by_race: Dict[str, Dict[str, int]],
    fleet_current: Optional[Dict[str, int]] = None,
) -> Dict[str, int]:
    """Apply computed pre-race handicaps and fleet currents directly in PostgreSQL.

    - Updates race_results.initial_handicap for each (race_id, competitor_id) where
      there is no manual override (handicap_override IS NULL) and the stored value
      differs from the computed seed. Uses IS DISTINCT FROM to handle NULL safely.
    - Optionally updates competitors.current_handicap_s_per_hr from the provided map.

    Returns a stats dict with counts of updated rows.
    """
    stats = {"race_rows_updated": 0, "competitors_updated": 0}
    if not pre_by_race:
        return stats
    with _get_conn() as conn, conn.cursor() as cur:
        # Update race_results seeds in bulk using a VALUES table
        # Build (race_id, competitor_ref, seed) tuples
        rows: List[Tuple[str, int, int]] = []
        for rid, cmap in pre_by_race.items():
            if not rid or not isinstance(cmap, dict):
                continue
            for cid, seed in cmap.items():
                try:
                    rows.append((str(rid), int(cid), int(seed)))
                except Exception:
                    continue

        if rows:
            # Chunk large updates to keep statements reasonable in size
            chunk_size = 2000
            for i in range(0, len(rows), chunk_size):
                chunk = rows[i : i + chunk_size]
                sql = (
                    """
                    UPDATE race_results AS rr
                    SET initial_handicap = v.seed
                    FROM (VALUES %s) AS v(race_id, competitor_ref, seed)
                    WHERE rr.race_id = v.race_id
                      AND rr.competitor_ref = v.competitor_ref
                      AND (rr.handicap_override IS NULL)
                      AND (rr.initial_handicap IS DISTINCT FROM v.seed)
                    """
                )
                # execute_values will expand the VALUES %s placeholder
                execute_values(cur, sql, chunk)
                # psycopg2 rowcount reflects rows affected by the UPDATE
                stats["race_rows_updated"] += cur.rowcount or 0

        # Update fleet currents if provided
        if fleet_current:
            rows2: List[Tuple[int, int]] = []
            for cid, cur_h in fleet_current.items():
                try:
                    rows2.append((int(cid), int(cur_h)))
                except Exception:
                    continue
            if rows2:
                chunk_size2 = 2000
                for j in range(0, len(rows2), chunk_size2):
                    chunk2 = rows2[j : j + chunk_size2]
                    sql2 = (
                        """
                        UPDATE competitors AS c
                        SET current_handicap_s_per_hr = v.cur_h
                        FROM (VALUES %s) AS v(id, cur_h)
                        WHERE c.id = v.id
                          AND (c.current_handicap_s_per_hr IS DISTINCT FROM v.cur_h)
                        """
                    )
                    execute_values(cur, sql2, chunk2)
                    stats["competitors_updated"] += cur.rowcount or 0
        conn.commit()
    return stats


def get_races_with_entries(race_ids: List[str]) -> Dict[str, Dict[str, Any]]:
    """Return mapping of race_id -> {race_id, date, start_time, competitors[]}.

    The competitors list contains dicts with competitor_id (int),
    initial_handicap, finish_time (HH:MM:SS or None), and handicap_override.
    Uses two queries filtered by the provided race_ids.
    """
    if not race_ids:
        return {}
    meta: Dict[str, Dict[str, Any]] = {}
    # Ensure uniqueness and stable order of input ids where possible
    ids = [str(rid) for rid in race_ids if rid]
    with _get_conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT race_id, date, start_time
            FROM races
            WHERE race_id = ANY(%s)
            """,
            (ids,),
        )
        for r in cur.fetchall() or []:
            rid = r.get("race_id")
            if not rid:
                continue
            meta[str(rid)] = {
                "race_id": str(rid),
                "date": r.get("date").isoformat() if r.get("date") else None,
                "start_time": _time_to_str(r.get("start_time")),
                "competitors": [],
            }
        if not meta:
            return {}
        cur.execute(
            """
            SELECT race_id, competitor_ref AS competitor_id, initial_handicap, finish_time, handicap_override
            FROM race_results
            WHERE race_id = ANY(%s)
            ORDER BY race_id, competitor_ref
            """,
            (list(meta.keys()),),
        )
        for ent in cur.fetchall() or []:
            rid = ent.get("race_id")
            if not rid or str(rid) not in meta:
                continue
            meta[str(rid)]["competitors"].append(
                {
                    "competitor_id": ent.get("competitor_id"),
                    "initial_handicap": ent.get("initial_handicap"),
                    "finish_time": _time_to_str(ent.get("finish_time")),
                    "handicap_override": ent.get("handicap_override"),
                }
            )
    return meta


def update_race_row(race_id: str, fields: Dict[str, Any]) -> None:
    """Update selected columns of a race row.

    Accepted keys: series_id, date, start_time, race_no, name
    """
    allowed = {
        'series_id': 'series_id',
        'date': 'date',
        'start_time': 'start_time',
        'race_no': 'race_no',
        'name': 'name',
    }
    sets: List[str] = []
    params: List[Any] = []
    for k, col in allowed.items():
        if k in fields:
            sets.append(f"{col} = %s")
            params.append(fields.get(k))
    if not sets:
        return
    sql = f"UPDATE races SET {', '.join(sets)} WHERE race_id = %s"
    params.append(race_id)
    with _get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, params)
        conn.commit()


def replace_race_results(race_id: str, entrants: List[Dict[str, Any]]) -> None:
    """Replace entrants (race_results) for a single race ID.

    Entrants fields: competitor_id, initial_handicap, finish_time (HH:MM:SS or None), handicap_override
    """
    with _get_conn() as conn, conn.cursor() as cur:
        cur.execute("DELETE FROM race_results WHERE race_id = %s", (race_id,))
        rows: List[Tuple[str, Optional[int], Optional[str], Optional[int]]] = []
        for ent in (entrants or []):
            cid = ent.get('competitor_id')
            cid_int = int(cid) if cid is not None else None
            # Normalize finish_time: empty/whitespace -> NULL
            _ft = ent.get('finish_time')
            finish_val = None
            if _ft is not None:
                s = str(_ft).strip()
                finish_val = None if s == '' else s
            rows.append((str(race_id), cid_int, finish_val, ent.get('handicap_override')))
        if rows:
            execute_values(
                cur,
                """
                INSERT INTO race_results (race_id, competitor_ref, finish_time, handicap_override)
                VALUES %s
                ON CONFLICT (race_id, competitor_ref) DO UPDATE SET
                    finish_time = EXCLUDED.finish_time,
                    handicap_override = EXCLUDED.handicap_override
                """,
                rows,
            )
        conn.commit()
