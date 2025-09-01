from typing import Any, Dict, List, Tuple, Optional

# PostgreSQL-only datastore proxy
# This module now delegates all operations to datastore_pg so the application
# no longer reads or writes a local data.json file.

from . import datastore_pg as _pg


def load_data() -> Dict[str, Any]:
    return _pg.load_data()


def save_data(data: Dict[str, Any]) -> None:
    _pg.save_data(data)


def list_seasons(data: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    return _pg.list_seasons(data=data)


def list_series(data: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    return _pg.list_series(data=data)


def find_series(series_id: str, data: Optional[Dict[str, Any]] = None) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    return _pg.find_series(series_id, data=data)


def find_race(race_id: str, data: Optional[Dict[str, Any]] = None) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    return _pg.find_race(race_id, data=data)


def ensure_season(year: int, data: Optional[Dict[str, Any]] = None) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    return _pg.ensure_season(year, data=data)


def ensure_series(year: int, name: str, series_id: Optional[str] = None, data: Optional[Dict[str, Any]] = None) -> Tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any]]:
    return _pg.ensure_series(year, name, series_id=series_id, data=data)


def list_all_races(data: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    return _pg.list_all_races(data=data)


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
