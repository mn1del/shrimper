import os
import json
from typing import Any, Dict, List, Optional, Tuple

import psycopg2
from psycopg2 import pool as pg_pool
from psycopg2.extras import RealDictCursor
from psycopg2 import errors as pg_errors
from contextlib import contextmanager


_POOL: Optional[pg_pool.AbstractConnectionPool] = None


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
    _POOL = pg_pool.ThreadedConnectionPool(minconn, maxconn, dsn=url)


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
        conn = _POOL.getconn()
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
                if getattr(conn, "closed", 0) == 0 and not conn.autocommit:
                    # If caller didn't commit/rollback and transaction is open, rollback
                    # status 0 = idle, 1 = active, 2 = intrans, 3 = inerror (psycopg2 docs)
                    if getattr(conn, "status", 0) in (1, 2, 3):
                        try:
                            conn.rollback()
                        except Exception:
                            pass
            finally:
                _POOL.putconn(conn)
    else:
        conn = psycopg2.connect(url)
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

        # Fleet
        try:
            cur.execute(
                """
                SELECT competitor_id, sailor_name, boat_name, sail_no,
                       starting_handicap_s_per_hr, current_handicap_s_per_hr
                FROM competitors
                ORDER BY sail_no NULLS LAST, competitor_id
                """
            )
            comps = []
            for r in cur.fetchall():
                comps.append(
                    {
                        "competitor_id": r.get("competitor_id"),
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
        # Build canonicalization maps for competitor IDs
        canon_ids = {c.get("competitor_id") for c in out["fleet"]["competitors"] if c.get("competitor_id")}
        sail_to_id = {str((c.get("sail_no") or "")).strip(): c.get("competitor_id") for c in out["fleet"]["competitors"] if c.get("competitor_id") and (c.get("sail_no") is not None)}
        import re as _re
        def _canon_cid(cid_raw: str | None) -> str | None:
            if not cid_raw:
                return None
            if cid_raw in canon_ids:
                return cid_raw
            s = cid_raw
            num = None
            if s.startswith("C_UNK_"):
                num = s[6:]
            elif s.startswith("C_"):
                num = s.split("_", 1)[1]
            if not num:
                m = _re.search(r"(\d+)$", s)
                num = m.group(1) if m else None
            if num:
                return sail_to_id.get(str(num)) or cid_raw
            return cid_raw

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
        results_by_race_maps: Dict[str, Dict[str, Dict[str, Any]]] = {}
        if race_ids:
            try:
                # Prefer selecting handicap_override if the column exists; fall back gracefully
                try:
                    cur.execute(
                        """
                        SELECT race_id, competitor_id, initial_handicap, finish_time, handicap_override
                        FROM race_results
                        WHERE race_id = ANY(%s)
                        ORDER BY race_id, competitor_id
                        """,
                        (race_ids,),
                    )
                except Exception as e:
                    if isinstance(e, getattr(pg_errors, "UndefinedColumn", tuple())):
                        try:
                            conn.rollback()
                        except Exception:
                            pass
                        cur.execute(
                            """
                            SELECT race_id, competitor_id, initial_handicap, finish_time
                            FROM race_results
                            WHERE race_id = ANY(%s)
                            ORDER BY race_id, competitor_id
                            """,
                            (race_ids,),
                        )
                    else:
                        raise
                for ent in cur.fetchall() or []:
                    rid = ent.get("race_id")
                    if not rid:
                        continue
                    cid_raw = ent.get("competitor_id")
                    cid = _canon_cid(cid_raw)
                    entry = {
                        "competitor_id": cid,
                        "initial_handicap": ent.get("initial_handicap"),
                        "finish_time": _time_to_str(ent.get("finish_time")),
                        "handicap_override": ent.get("handicap_override"),
                    }
                    m = results_by_race_maps.setdefault(rid, {})
                    prev = m.get(cid or "")
                    if prev is None:
                        m[cid or ""] = entry
                    else:
                        # Prefer row with a finish_time or an override
                        if (prev.get("finish_time") is None and entry.get("finish_time") is not None) or (
                            prev.get("handicap_override") is None and entry.get("handicap_override") is not None
                        ):
                            m[cid or ""] = entry
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
                        cur.execute("INSERT INTO settings (config) VALUES (%s)", (json.dumps(settings),))
                    else:
                        raise

            # Fleet
            if "fleet" in data and data["fleet"] is not None:
                fleet = data["fleet"] or {"competitors": []}
                competitors = fleet.get("competitors", [])
                # Replace competitors set (keeps it simple and bounded)
                try:
                    cur.execute("DELETE FROM competitors")
                except Exception as e:
                    if not isinstance(e, getattr(pg_errors, "UndefinedTable", tuple())):
                        raise
                for comp in competitors:
                    cur.execute(
                        """
                        INSERT INTO competitors (
                            competitor_id, sailor_name, boat_name, sail_no,
                            starting_handicap_s_per_hr, current_handicap_s_per_hr
                        ) VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (competitor_id) DO UPDATE SET
                            sailor_name = EXCLUDED.sailor_name,
                            boat_name = EXCLUDED.boat_name,
                            sail_no = EXCLUDED.sail_no,
                            starting_handicap_s_per_hr = EXCLUDED.starting_handicap_s_per_hr,
                            current_handicap_s_per_hr = EXCLUDED.current_handicap_s_per_hr
                        """,
                        (
                            comp.get("competitor_id"),
                            comp.get("sailor_name"),
                            comp.get("boat_name"),
                            comp.get("sail_no"),
                            comp.get("starting_handicap_s_per_hr") or 0,
                            comp.get("current_handicap_s_per_hr") or comp.get("starting_handicap_s_per_hr") or 0,
                        ),
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
                            # Replace entrants for this race for determinism
                            cur.execute("DELETE FROM race_results WHERE race_id = %s", (rid,))
                            for ent in race.get("competitors", []) or []:
                                # Attempt to upsert including handicap_override; fall back if column missing
                                try:
                                    cur.execute(
                                        """
                                        INSERT INTO race_results (race_id, competitor_id, initial_handicap, finish_time, handicap_override)
                                        VALUES (%s, %s, %s, %s, %s)
                                        ON CONFLICT (race_id, competitor_id) DO UPDATE SET
                                            initial_handicap = EXCLUDED.initial_handicap,
                                            finish_time = EXCLUDED.finish_time,
                                            handicap_override = EXCLUDED.handicap_override
                                        """,
                                        (
                                            rid,
                                            ent.get("competitor_id"),
                                            ent.get("initial_handicap"),
                                            ent.get("finish_time"),
                                            ent.get("handicap_override"),
                                        ),
                                    )
                                except Exception as e:
                                    if isinstance(e, getattr(pg_errors, "UndefinedColumn", tuple())):
                                        cur.execute(
                                            """
                                            INSERT INTO race_results (race_id, competitor_id, initial_handicap, finish_time)
                                            VALUES (%s, %s, %s, %s)
                                            ON CONFLICT (race_id, competitor_id) DO UPDATE SET
                                                initial_handicap = EXCLUDED.initial_handicap,
                                                finish_time = EXCLUDED.finish_time
                                            """,
                                            (
                                                rid,
                                                ent.get("competitor_id"),
                                                ent.get("initial_handicap"),
                                                ent.get("finish_time"),
                                            ),
                                        )
                                    else:
                                        raise

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
        # Read handicap_override if present; fall back when column missing
        try:
            cur.execute(
                "SELECT competitor_id, initial_handicap, finish_time, handicap_override FROM race_results WHERE race_id = %s ORDER BY competitor_id",
                (race_id,),
            )
        except Exception as e:
            if isinstance(e, getattr(pg_errors, "UndefinedColumn", tuple())):
                try:
                    conn.rollback()
                except Exception:
                    pass
                cur.execute(
                    "SELECT competitor_id, initial_handicap, finish_time FROM race_results WHERE race_id = %s ORDER BY competitor_id",
                    (race_id,),
                )
            else:
                raise
        # IMPORTANT: materialize race_results rows before running another query on the same cursor
        race_result_rows = cur.fetchall() or []
        # Canonicalize IDs using fleet table
        # Load fleet for this normalization
        try:
            cur.execute(
                "SELECT competitor_id, sail_no FROM competitors ORDER BY competitor_id"
            )
            fleet_rows = cur.fetchall() or []
        except Exception:
            fleet_rows = []
        canon_ids = {r.get("competitor_id") for r in fleet_rows if r.get("competitor_id")}
        sail_to_id = {str((r.get("sail_no") or "")).strip(): r.get("competitor_id") for r in fleet_rows if r.get("competitor_id") and (r.get("sail_no") is not None)}
        import re as _re
        def _canon_cid(cid_raw: str | None) -> str | None:
            if not cid_raw:
                return None
            if cid_raw in canon_ids:
                return cid_raw
            s = cid_raw
            num = None
            if s.startswith("C_UNK_"):
                num = s[6:]
            elif s.startswith("C_"):
                num = s.split("_", 1)[1]
            if not num:
                m = _re.search(r"(\d+)$", s)
                num = m.group(1) if m else None
            if num:
                return sail_to_id.get(str(num)) or cid_raw
            return cid_raw
        # Deduplicate
        seen: dict[str, dict] = {}
        for ent in race_result_rows:
            cid = _canon_cid(ent.get("competitor_id"))
            entry = {
                "competitor_id": cid,
                "initial_handicap": ent.get("initial_handicap"),
                "finish_time": _time_to_str(ent.get("finish_time")),
                "handicap_override": ent.get("handicap_override"),
            }
            prev = seen.get(cid or "")
            if prev is None or (
                (prev.get("finish_time") is None and entry.get("finish_time") is not None)
                or (prev.get("handicap_override") is None and entry.get("handicap_override") is not None)
            ):
                seen[cid or ""] = entry
        race["competitors"].extend(seen.values())
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
                # Try to include handicap_override where schema supports it
                try:
                    cur.execute(
                        """
                        SELECT race_id, competitor_id, initial_handicap, finish_time, handicap_override
                        FROM race_results
                        WHERE race_id = ANY(%s)
                        ORDER BY race_id, competitor_id
                        """,
                        (race_ids,),
                    )
                except Exception as e:
                    if isinstance(e, getattr(pg_errors, "UndefinedColumn", tuple())):
                        try:
                            conn.rollback()
                        except Exception:
                            pass
                        cur.execute(
                            """
                            SELECT race_id, competitor_id, initial_handicap, finish_time
                            FROM race_results
                            WHERE race_id = ANY(%s)
                            ORDER BY race_id, competitor_id
                            """,
                            (race_ids,),
                        )
                    else:
                        raise
                rows_rr = cur.fetchall() or []
                # Canonicalize using competitors table
                try:
                    cur.execute(
                        "SELECT competitor_id, sail_no FROM competitors ORDER BY competitor_id"
                    )
                    fleet_rows = cur.fetchall() or []
                except Exception:
                    fleet_rows = []
                canon_ids = {r.get("competitor_id") for r in fleet_rows if r.get("competitor_id")}
                sail_to_id = {str((r.get("sail_no") or "")).strip(): r.get("competitor_id") for r in fleet_rows if r.get("competitor_id") and (r.get("sail_no") is not None)}
                import re as _re
                def _canon_cid(cid_raw: str | None) -> str | None:
                    if not cid_raw:
                        return None
                    if cid_raw in canon_ids:
                        return cid_raw
                    s = cid_raw
                    num = None
                    if s.startswith("C_UNK_"):
                        num = s[6:]
                    elif s.startswith("C_"):
                        num = s.split("_", 1)[1]
                    if not num:
                        m = _re.search(r"(\d+)$", s)
                        num = m.group(1) if m else None
                    if num:
                        return sail_to_id.get(str(num)) or cid_raw
                    return cid_raw
                # Build per-race maps with dedupe
                rr_maps: Dict[str, Dict[str, Dict[str, Any]]] = {}
                for ent in rows_rr:
                    rid = ent.get("race_id")
                    if not rid:
                        continue
                    cid = _canon_cid(ent.get("competitor_id"))
                    entry = {
                        "competitor_id": cid,
                        "initial_handicap": ent.get("initial_handicap"),
                        "finish_time": _time_to_str(ent.get("finish_time")),
                        "handicap_override": ent.get("handicap_override"),
                    }
                    m = rr_maps.setdefault(rid, {})
                    prev = m.get(cid or "")
                    if prev is None or (
                        (prev.get("finish_time") is None and entry.get("finish_time") is not None)
                        or (prev.get("handicap_override") is None and entry.get("handicap_override") is not None)
                    ):
                        m[cid or ""] = entry
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
                SELECT competitor_id, sailor_name, boat_name, sail_no,
                       starting_handicap_s_per_hr, current_handicap_s_per_hr
                FROM competitors
                ORDER BY sail_no NULLS LAST, competitor_id
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
                    "competitor_id": r.get("competitor_id"),
                    "sailor_name": r.get("sailor_name"),
                    "boat_name": r.get("boat_name"),
                    "sail_no": r.get("sail_no"),
                    "starting_handicap_s_per_hr": r.get("starting_handicap_s_per_hr") or 0,
                    "current_handicap_s_per_hr": r.get("current_handicap_s_per_hr") or r.get("starting_handicap_s_per_hr") or 0,
                }
            )
        return {"competitors": comps}


def set_fleet(fleet: Dict[str, Any], data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    # Replace competitors table content to match provided fleet
    competitors = (fleet or {}).get("competitors", [])
    with _get_conn() as conn, conn.cursor() as cur:
        try:
            cur.execute("DELETE FROM competitors")
        except Exception as e:
            if not isinstance(e, getattr(pg_errors, "UndefinedTable", tuple())):
                raise
        for comp in competitors:
            cur.execute(
                """
                INSERT INTO competitors (
                    competitor_id, sailor_name, boat_name, sail_no,
                    starting_handicap_s_per_hr, current_handicap_s_per_hr
                ) VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (competitor_id) DO UPDATE SET
                    sailor_name = EXCLUDED.sailor_name,
                    boat_name = EXCLUDED.boat_name,
                    sail_no = EXCLUDED.sail_no,
                    starting_handicap_s_per_hr = EXCLUDED.starting_handicap_s_per_hr,
                    current_handicap_s_per_hr = EXCLUDED.current_handicap_s_per_hr
                """,
                (
                    comp.get("competitor_id"),
                    comp.get("sailor_name"),
                    comp.get("boat_name"),
                    comp.get("sail_no"),
                    comp.get("starting_handicap_s_per_hr") or 0,
                    comp.get("current_handicap_s_per_hr") or comp.get("starting_handicap_s_per_hr") or 0,
                ),
            )
        conn.commit()
    return {"competitors": competitors}


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
    """Normalize race_results.competitor_id values to canonical fleet IDs.

    - If a race_results row uses a fallback like C_<sail> or C_UNK_<sail>, map it
      to the canonical competitors.competitor_id for that sail number.
    - When a canonical row already exists for the same race, merge values by
      coalescing non-null fields, then delete the duplicate old row.

    Returns a dict with counts of updated, merged and deleted rows.
    """
    stats = {"updated": 0, "merged": 0, "deleted": 0, "skipped": 0}
    with _get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Load fleet canon ids and sail numbers
            try:
                cur.execute(
                    "SELECT competitor_id, sail_no FROM competitors ORDER BY competitor_id"
                )
                fleet_rows = cur.fetchall() or []
            except Exception as e:
                if isinstance(e, getattr(pg_errors, "UndefinedTable", tuple())):
                    return stats
                raise

            canon_ids = {r.get("competitor_id") for r in fleet_rows if r.get("competitor_id")}
            sail_to_id = {
                str((r.get("sail_no") or "")).strip(): r.get("competitor_id")
                for r in fleet_rows
                if r.get("competitor_id") and (r.get("sail_no") is not None)
            }

            import re as _re

            def _canon_cid(cid_raw: str | None) -> str | None:
                if not cid_raw:
                    return None
                if cid_raw in canon_ids:
                    return cid_raw
                s = str(cid_raw)
                num = None
                if s.startswith("C_UNK_"):
                    num = s[6:]
                elif s.startswith("C_"):
                    num = s.split("_", 1)[1]
                if not num:
                    m = _re.search(r"(\d+)$", s)
                    num = m.group(1) if m else None
                if num:
                    return sail_to_id.get(str(num)) or cid_raw
                return cid_raw

            # Detect whether handicap_override column exists
            try:
                cur.execute(
                    """
                    SELECT 1 FROM information_schema.columns
                    WHERE table_schema='public' AND table_name='race_results' AND column_name='handicap_override'
                    """
                )
                override_supported = cur.fetchone() is not None
            except Exception:
                override_supported = False

            # Pull all race_results rows
            try:
                if override_supported:
                    cur.execute(
                        """
                        SELECT race_id, competitor_id, initial_handicap, finish_time, handicap_override
                        FROM race_results
                        ORDER BY race_id, competitor_id
                        """
                    )
                else:
                    cur.execute(
                        """
                        SELECT race_id, competitor_id, initial_handicap, finish_time
                        FROM race_results
                        ORDER BY race_id, competitor_id
                        """
                    )
            except Exception as e:
                if isinstance(e, getattr(pg_errors, "UndefinedTable", tuple())):
                    return stats
                raise

            rows = cur.fetchall() or []
            for row in rows:
                rid = row.get("race_id")
                old = row.get("competitor_id")
                new = _canon_cid(old)
                if not new or new == old:
                    stats["skipped"] += 1
                    continue

                # Check if a canonical row already exists for this race
                if override_supported:
                    cur.execute(
                        "SELECT initial_handicap, finish_time, handicap_override FROM race_results WHERE race_id=%s AND competitor_id=%s",
                        (rid, new),
                    )
                else:
                    cur.execute(
                        "SELECT initial_handicap, finish_time FROM race_results WHERE race_id=%s AND competitor_id=%s",
                        (rid, new),
                    )
                existing = cur.fetchone()
                if existing:
                    # Merge values: prefer non-null from either row
                    ih = row.get("initial_handicap") or existing.get("initial_handicap")
                    ft = row.get("finish_time") or existing.get("finish_time")
                    if override_supported:
                        ho = row.get("handicap_override") if row.get("handicap_override") is not None else existing.get("handicap_override")
                        cur.execute(
                            """
                            UPDATE race_results
                            SET initial_handicap=%s, finish_time=%s, handicap_override=%s
                            WHERE race_id=%s AND competitor_id=%s
                            """,
                            (ih, ft, ho, rid, new),
                        )
                    else:
                        cur.execute(
                            """
                            UPDATE race_results
                            SET initial_handicap=%s, finish_time=%s
                            WHERE race_id=%s AND competitor_id=%s
                            """,
                            (ih, ft, rid, new),
                        )
                    cur.execute(
                        "DELETE FROM race_results WHERE race_id=%s AND competitor_id=%s",
                        (rid, old),
                    )
                    stats["merged"] += 1
                    stats["deleted"] += 1
                else:
                    # Simple update to the canonical id
                    cur.execute(
                        "UPDATE race_results SET competitor_id=%s WHERE race_id=%s AND competitor_id=%s",
                        (new, rid, old),
                    )
                    stats["updated"] += 1

        conn.commit()
    return stats


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
        # Update race_results seeds
        for rid, cmap in pre_by_race.items():
            if not rid or not isinstance(cmap, dict):
                continue
            for cid, seed in cmap.items():
                cur.execute(
                    """
                    UPDATE race_results
                    SET initial_handicap = %s
                    WHERE race_id = %s
                      AND competitor_id = %s
                      AND (handicap_override IS NULL)
                      AND (initial_handicap IS DISTINCT FROM %s)
                    """,
                    (int(seed), rid, cid, int(seed)),
                )
                stats["race_rows_updated"] += cur.rowcount or 0

        # Update fleet currents if provided
        if fleet_current:
            for cid, cur_h in fleet_current.items():
                cur.execute(
                    """
                    UPDATE competitors
                    SET current_handicap_s_per_hr = %s
                    WHERE competitor_id = %s
                    """,
                    (int(cur_h), cid),
                )
                stats["competitors_updated"] += cur.rowcount or 0
        conn.commit()
    return stats
