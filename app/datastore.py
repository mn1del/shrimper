import json
from pathlib import Path
from typing import Any, Dict, List, Tuple, Optional


# Path to the single data file used by the application
DATA_FILE = Path(__file__).resolve().parent.parent / "data.json"


def _load_raw() -> Dict[str, Any]:
    if not DATA_FILE.exists():
        # Minimal skeleton
        return {"fleet": {"competitors": []}, "seasons": [], "settings": {}}
    # Use utf-8-sig to tolerate BOM-prefixed files
    with DATA_FILE.open(encoding="utf-8-sig") as f:
        return json.load(f)


def _save_raw(data: Dict[str, Any]) -> None:
    DATA_FILE.write_text(json.dumps(data, indent=2), encoding="utf-8")


def load_data() -> Dict[str, Any]:
    return _load_raw()


def save_data(data: Dict[str, Any]) -> None:
    _save_raw(data)


def list_seasons(data: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    d = data or load_data()
    return list(d.get("seasons", []))


def list_series(data: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    d = data or load_data()
    out: List[Dict[str, Any]] = []
    for season in d.get("seasons", []):
        for series in season.get("series", []):
            out.append(series)
    return out


def find_series(series_id: str, data: Optional[Dict[str, Any]] = None) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    d = data or load_data()
    target = (series_id or "").lower()
    for season in d.get("seasons", []):
        for series in season.get("series", []):
            sid = (series.get("series_id") or "").lower()
            if sid == target:
                return season, series
    return None, None


def find_race(race_id: str, data: Optional[Dict[str, Any]] = None) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    d = data or load_data()
    for season in d.get("seasons", []):
        for series in season.get("series", []):
            for race in series.get("races", []):
                if race.get("race_id") == race_id:
                    return season, series, race
    return None, None, None


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


def list_all_races(data: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    d = data or load_data()
    races: List[Dict[str, Any]] = []
    for season in d.get("seasons", []):
        for series in season.get("series", []):
            for race in series.get("races", []):
                races.append({
                    "race_id": race.get("race_id"),
                    "date": race.get("date"),
                    "start_time": race.get("start_time"),
                    "series_name": series.get("name"),
                    "series_id": series.get("series_id"),
                    "finishers": sum(1 for e in race.get("competitors", []) if e.get("finish_time")),
                    "season": season.get("year"),
                })
    races.sort(key=lambda r: (r.get("date") or "", r.get("start_time") or ""), reverse=True)
    return races


def renumber_races(series: Dict[str, Any]) -> Dict[str, str]:
    """Renumber races in a series and rebuild race_id from date/name.

    Returns mapping of old_id to new_id.
    """
    races = series.setdefault("races", [])
    # Ensure stable sort on date/start_time
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
    d = data or load_data()
    return d.get("fleet", {"competitors": []})


def set_fleet(fleet: Dict[str, Any], data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    d = data or load_data()
    d["fleet"] = fleet
    save_data(d)
    return d


def get_settings(data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    d = data or load_data()
    return d.get("settings", {})


def set_settings(settings: Dict[str, Any], data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    d = data or load_data()
    d["settings"] = settings
    save_data(d)
    return d
