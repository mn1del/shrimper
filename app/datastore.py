from typing import Any, Dict, List, Tuple, Optional

# PostgreSQL-only datastore proxy
# This module now delegates all operations to datastore_pg so the application
# no longer reads or writes a local data.json file.

from . import datastore_pg as _pg


def _scan_series_in_data(series_id: str, data: Optional[Dict[str, Any]] = None) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    """Return (season, series) from the provided data tree if present.

    This scans the given ``data`` structure and returns references to the
    contained season/series objects so that callers can mutate them and then
    persist via ``save_data``.
    """
    if not data:
        return None, None
    seasons = data.get("seasons", []) or []
    for season in seasons:
        for series in season.get("series", []) or []:
            if series.get("series_id") == series_id:
                return season, series
    return None, None


def _scan_race_in_data(race_id: str, data: Optional[Dict[str, Any]] = None) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    """Return (season, series, race) from provided data if present.

    Returns references to objects inside ``data`` so edits can be persisted
    by saving the same ``data`` tree.
    """
    if not data:
        return None, None, None
    seasons = data.get("seasons", []) or []
    for season in seasons:
        for series in season.get("series", []) or []:
            for race in series.get("races", []) or []:
                if race.get("race_id") == race_id:
                    return season, series, race
    return None, None, None


def load_data() -> Dict[str, Any]:
    return _pg.load_data()


def save_data(data: Dict[str, Any]) -> None:
    _pg.save_data(data)


def list_seasons(data: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    return _pg.list_seasons(data=data)


def list_series(data: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    return _pg.list_series(data=data)


def find_series(series_id: str, data: Optional[Dict[str, Any]] = None) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    # Prefer returning references from provided data so edits persist when saved
    season, series = _scan_series_in_data(series_id, data=data)
    if season is not None and series is not None:
        return season, series
    # Fallback to database fetch
    return _pg.find_series(series_id, data=data)


def find_race(race_id: str, data: Optional[Dict[str, Any]] = None) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    # Prefer returning references from provided data so edits persist when saved
    season, series, race = _scan_race_in_data(race_id, data=data)
    if season is not None and series is not None and race is not None:
        return season, series, race
    # Fallback to database fetch
    return _pg.find_race(race_id, data=data)


def ensure_season(year: int, data: Optional[Dict[str, Any]] = None) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    return _pg.ensure_season(year, data=data)


def ensure_series(year: int, name: str, series_id: Optional[str] = None, data: Optional[Dict[str, Any]] = None) -> Tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any]]:
    return _pg.ensure_series(year, name, series_id=series_id, data=data)


def list_all_races(data: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    return _pg.list_all_races(data=data)


def get_races() -> List[str]:
    """Return race_ids in chronological order.

    Primary source is the PostgreSQL implementation. If unavailable or
    patched out (e.g., in tests), falls back to deriving order from
    ``list_all_races`` sorted by (date, start_time) ascending.
    """
    try:
        fn = getattr(_pg, "get_races", None)
        if callable(fn):
            return fn()  # type: ignore[misc]
    except Exception:
        # Fall through to derive from list_all_races
        pass
    try:
        races = _pg.list_all_races(data=None) or []
    except Exception:
        return []
    def _key(r: Dict[str, Any]):
        d = r.get("date") or ""
        t = r.get("start_time") or ""
        return (d, t)
    races_sorted = sorted(races, key=_key)
    return [str(r.get("race_id")) for r in races_sorted if r.get("race_id")]

def list_season_race_ids(season_year: int) -> List[str]:
    """Return race IDs for a given season in chronological order.

    Tries PostgreSQL helper; falls back to deriving from
    list_season_races_with_results when unavailable (e.g., in tests).
    """
    try:
        fn = getattr(_pg, "list_season_race_ids", None)
        if callable(fn):
            return fn(int(season_year))  # type: ignore[misc]
    except Exception:
        pass
    try:
        season = _pg.list_season_races_with_results(int(season_year)) or {"series": []}
    except Exception:
        return []
    races: List[Dict[str, Any]] = []
    for series in season.get("series", []) or []:
        for race in series.get("races", []) or []:
            races.append(race)
    def _key(r: Dict[str, Any]):
        d = r.get("date") or ""
        t = r.get("start_time") or ""
        return (d, t, r.get("race_id") or "")
    races.sort(key=_key)
    return [str(r.get("race_id")) for r in races if r.get("race_id")]

def list_season_races_with_results(season_year: int, data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    return _pg.list_season_races_with_results(season_year, data=data)


def renumber_races(series: Dict[str, Any]) -> Dict[str, str]:
    return _pg.renumber_races(series)


def get_fleet(data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    return _pg.get_fleet(data=data)


def set_fleet(fleet: Dict[str, Any], data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    return _pg.set_fleet(fleet, data=data)


def get_settings(data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    return _pg.get_settings(data=data)


def set_settings(settings: Dict[str, Any], data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    return _pg.set_settings(settings, data=data)


# Targeted PostgreSQL helpers (no-op in JSON path; tests should monkeypatch)
def update_race_row(race_id: str, fields: Dict[str, Any]) -> None:
    return _pg.update_race_row(race_id, fields)


def replace_race_results(race_id: str, entrants: List[Dict[str, Any]]) -> None:
    return _pg.replace_race_results(race_id, entrants)
