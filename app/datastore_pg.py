import os
import json
from typing import Any, Dict, List, Optional, Tuple

import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2 import errors as pg_errors


# Postgres-backed datastore implementing the same API as app/datastore.py


def _get_conn():
    url = os.environ.get("DATABASE_URL")
    if not url:
        raise RuntimeError("DATABASE_URL not set; configure a PostgreSQL connection string")
    return psycopg2.connect(url)


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

        # Seasons -> Series -> Races -> Entrants
        try:
            cur.execute("SELECT id, year FROM seasons ORDER BY year")
            seasons = cur.fetchall()
        except Exception as e:
            if isinstance(e, getattr(pg_errors, "UndefinedTable", tuple())):
                seasons = []
            else:
                raise
        season_list: List[Dict[str, Any]] = []
        for s in seasons:
            season_obj = {"year": int(s["year"]), "series": []}
            # series for this season
            try:
                cur.execute(
                    "SELECT series_id, name, year FROM series WHERE season_id = %s ORDER BY name",
                    (s["id"],),
                )
                series_rows = cur.fetchall()
            except Exception as e:
                if isinstance(e, getattr(pg_errors, "UndefinedTable", tuple())):
                    series_rows = []
                else:
                    raise
            for ser in series_rows:
                series_obj = {
                    "series_id": ser["series_id"],
                    "name": ser["name"],
                    "season": int(ser.get("year") or s["year"]),
                    "races": [],
                }
                # races
                try:
                    cur.execute(
                        """
                        SELECT race_id, name, date, start_time, race_no
                        FROM races
                        WHERE series_id = %s
                        ORDER BY date NULLS LAST, start_time NULLS LAST, race_id
                        """,
                        (ser["series_id"],),
                    )
                    race_rows = cur.fetchall()
                except Exception as e:
                    if isinstance(e, getattr(pg_errors, "UndefinedTable", tuple())):
                        race_rows = []
                    else:
                        raise
                for r in race_rows:
                    race_obj = {
                        "race_id": r["race_id"],
                        "series_id": ser["series_id"],
                        "name": r.get("name"),
                        "date": r.get("date").isoformat() if r.get("date") else None,
                        "start_time": _time_to_str(r.get("start_time")),
                        "race_no": r.get("race_no"),
                        "competitors": [],
                    }
                    # entrants
                    try:
                        cur.execute(
                            """
                            SELECT competitor_id, initial_handicap, finish_time
                            FROM race_results
                            WHERE race_id = %s
                            ORDER BY competitor_id
                            """,
                            (r["race_id"],),
                        )
                        ent_rows = cur.fetchall()
                    except Exception as e:
                        if isinstance(e, getattr(pg_errors, "UndefinedTable", tuple())):
                            ent_rows = []
                        else:
                            raise
                    entrants = []
                    for ent in ent_rows:
                        entrants.append(
                            {
                                "competitor_id": ent.get("competitor_id"),
                                "initial_handicap": ent.get("initial_handicap"),
                                "finish_time": _time_to_str(ent.get("finish_time")),
                            }
                        )
                    race_obj["competitors"] = entrants
                    series_obj["races"].append(race_obj)
                season_obj["series"].append(series_obj)
            season_list.append(season_obj)
        out["seasons"] = season_list

    return out


def save_data(data: Dict[str, Any]) -> None:
    """Persist the provided JSON-like structure into PostgreSQL.

    For simplicity, this performs full replacement of each section present in
    the input (settings, fleet, seasons/series/races/results).
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
                    # Older schema: only 'config' may exist
                    if isinstance(e, getattr(pg_errors, "UndefinedColumn", tuple())) or isinstance(e, getattr(pg_errors, "UndefinedTable", tuple())):
                        try:
                            cur.execute("INSERT INTO settings (config) VALUES (%s)", (json.dumps(settings),))
                        except Exception:
                            # Give up silently; settings not persisted
                            pass
                    else:
                        raise

            # Fleet
            if "fleet" in data and data["fleet"] is not None:
                fleet = data["fleet"] or {"competitors": []}
                competitors = fleet.get("competitors", [])
                cur.execute("DELETE FROM competitors")
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
                # Replace everything for determinism
                cur.execute("DELETE FROM race_results")
                cur.execute("DELETE FROM races")
                cur.execute("DELETE FROM series")
                cur.execute("DELETE FROM seasons")

                for season in seasons:
                    year = int(season.get("year")) if season.get("year") is not None else None
                    if year is None:
                        continue
                    cur.execute("INSERT INTO seasons (year) VALUES (%s) RETURNING id", (year,))
                    season_id = cur.fetchone()[0]
                    for series in season.get("series", []) or []:
                        series_id = series.get("series_id")
                        name = series.get("name")
                        cur.execute(
                            """
                            INSERT INTO series (series_id, name, season_id, year)
                            VALUES (%s, %s, %s, %s)
                            ON CONFLICT (series_id) DO UPDATE SET name = EXCLUDED.name, season_id = EXCLUDED.season_id, year = EXCLUDED.year
                            RETURNING series_id
                            """,
                            (series_id, name, season_id, year),
                        )
                        # Insert races
                        for race in series.get("races", []) or []:
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
                                    race.get("race_id"),
                                    series_id,
                                    race.get("name"),
                                    race.get("date"),
                                    race.get("start_time"),
                                    race.get("race_no"),
                                ),
                            )
                            # entrants
                            for ent in race.get("competitors", []) or []:
                                cur.execute(
                                    """
                                    INSERT INTO race_results (race_id, competitor_id, initial_handicap, finish_time)
                                    VALUES (%s, %s, %s, %s)
                                    ON CONFLICT (race_id, competitor_id) DO UPDATE SET
                                        initial_handicap = EXCLUDED.initial_handicap,
                                        finish_time = EXCLUDED.finish_time
                                    """,
                                    (
                                        race.get("race_id"),
                                        ent.get("competitor_id"),
                                        ent.get("initial_handicap"),
                                        ent.get("finish_time"),
                                    ),
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
        cur.execute(
            "SELECT competitor_id, initial_handicap, finish_time FROM race_results WHERE race_id = %s ORDER BY competitor_id",
            (race_id,),
        )
        for ent in cur.fetchall():
            race["competitors"].append(
                {
                    "competitor_id": ent.get("competitor_id"),
                    "initial_handicap": ent.get("initial_handicap"),
                    "finish_time": _time_to_str(ent.get("finish_time")),
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
    d = load_data() if data is None else data
    return d.get("fleet", {"competitors": []})


def set_fleet(fleet: Dict[str, Any], data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    d = data or load_data()
    d["fleet"] = fleet
    save_data(d)
    return d


def get_settings(data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    d = load_data() if data is None else data
    return d.get("settings", {})


def set_settings(settings: Dict[str, Any], data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    d = data or load_data()
    d["settings"] = settings
    save_data(d)
    return d
