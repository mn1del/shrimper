import os
import copy
import pytest


@pytest.fixture(autouse=True)
def _require_db_env(monkeypatch):
    # Ensure app init doesn't abort on missing DATABASE_URL
    monkeypatch.setenv("DATABASE_URL", "postgresql://user:pass@localhost/db")
    yield


@pytest.fixture()
def memory_store():
    # Minimal in-memory structure compatible with JSON shape
    store = {
        "settings": {
            "version": 1,
            "updated_at": "2025-01-01T00:00:00Z",
            # Keep deltas simple and deterministic for tests
            "handicap_delta_by_rank": [
                {"rank": 1, "delta_s_per_hr": -30},
                {"rank": 2, "delta_s_per_hr": -20},
                {"rank": 3, "delta_s_per_hr": -10},
                {"rank": 4, "delta_s_per_hr": 0},
                {"rank": 5, "delta_s_per_hr": 10},
                {"rank": 6, "delta_s_per_hr": 20},
                {"rank": 7, "delta_s_per_hr": 30},
                {"rank": 8, "delta_s_per_hr": 40},
                {"rank": 9, "delta_s_per_hr": 50},
                {"rank": 10, "delta_s_per_hr": 60},
                {"rank": "default_or_higher", "delta_s_per_hr": 60},
            ],
            "league_points_by_rank": [
                {"rank": 1, "points": 25},
                {"rank": 2, "points": 18},
                {"rank": 3, "points": 12},
                {"rank": 4, "points": 9},
                {"rank": 5, "points": 6},
                {"rank": "default_or_higher", "points": 3},
            ],
            "fleet_size_factor": [
                {"finishers": 1, "factor": 0.0},
                {"finishers": 2, "factor": 0.0},
                {"finishers": 3, "factor": 0.0},
                {"finishers": 4, "factor": 0.3},
                {"finishers": 5, "factor": 0.6},
                {"finishers": 6, "factor": 0.8},
                {"finishers": 7, "factor": 1.0},
                {"finishers": 8, "factor": 1.0},
                {"finishers": 9, "factor": 1.0},
                {"finishers": 10, "factor": 1.0},
                {"finishers": 11, "factor": 1.0},
                {"finishers": 12, "factor": 1.0},
                {"finishers": "default_or_higher", "factor": 1.0},
            ],
        },
        "fleet": {"competitors": []},
        "seasons": [],
    }
    return store


@pytest.fixture(autouse=True)
def patch_datastore(monkeypatch, memory_store):
    # Patch app.datastore_pg with in-memory implementations using integer competitor IDs.
    import app.datastore_pg as pg

    def load_data():
        import copy
        return copy.deepcopy(memory_store)

    def save_data(data):
        import copy
        memory_store.clear()
        memory_store.update(copy.deepcopy(data or {}))

    def list_seasons(data=None):
        d = data or memory_store
        return list(d.get("seasons", []))

    def list_series(data=None):
        d = data or memory_store
        out = []
        for season in d.get("seasons", []):
            out.extend(season.get("series", []) or [])
        return out

    def find_series(series_id, data=None):
        d = data or memory_store
        target = (series_id or "").lower()
        for season in d.get("seasons", []):
            for series in season.get("series", []) or []:
                if (series.get("series_id") or "").lower() == target:
                    return season, series
        return None, None

    def find_race(race_id, data=None):
        d = data or memory_store
        for season in d.get("seasons", []):
            for series in season.get("series", []) or []:
                for race in series.get("races", []) or []:
                    if race.get("race_id") == race_id:
                        return season, series, race
        return None, None, None

    def ensure_season(year, data=None):
        d = data or memory_store
        seasons = d.setdefault("seasons", [])
        for s in seasons:
            if int(s.get("year")) == int(year):
                return d, s
        s = {"year": int(year), "series": []}
        seasons.append(s)
        return d, s

    def ensure_series(year, name, series_id=None, data=None):
        d, s = ensure_season(year, data or memory_store)
        for se in s.get("series", []) or []:
            if (se.get("name") == name) or (series_id and se.get("series_id") == series_id):
                return d, s, se
        sid = series_id or f"SER_{year}_{name}"
        se = {"series_id": sid, "name": name, "season": int(year), "races": []}
        s["series"].append(se)
        return d, s, se

    def renumber_races(series):
        races = series.setdefault("races", [])
        races.sort(key=lambda r: (r.get("date") or "", r.get("start_time") or ""))
        mapping = {}
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

    def list_all_races(data=None):
        d = data or memory_store
        out = []
        for season in d.get("seasons", []):
            for series in season.get("series", []) or []:
                for race in series.get("races", []) or []:
                    out.append({
                        "race_id": race.get("race_id"),
                        "date": race.get("date"),
                        "start_time": race.get("start_time"),
                        "series_name": series.get("name"),
                        "series_id": series.get("series_id"),
                        "finishers": sum(1 for e in race.get("competitors", []) if e.get("finish_time")),
                        "season": season.get("year"),
                    })
        out.sort(key=lambda r: (r.get("date") or "", r.get("start_time") or ""), reverse=True)
        return out

    def list_season_races_with_results(season_year, data=None):
        d = data or memory_store
        for s in d.get("seasons", []):
            if int(s.get("year", 0)) == int(season_year):
                return s
        return {"year": int(season_year), "series": []}

    def get_fleet(data=None):
        d = data or memory_store
        return d.get("fleet", {"competitors": []})

    def set_fleet(fleet, data=None):
        d = data or memory_store
        d["fleet"] = fleet or {"competitors": []}
        return d

    def get_settings(data=None):
        d = data or memory_store
        return d.get("settings", {})

    def set_settings(settings, data=None):
        d = data or memory_store
        d["settings"] = settings or {}
        return d

    # Patch all used functions
    monkeypatch.setattr(pg, "load_data", load_data)
    monkeypatch.setattr(pg, "save_data", save_data)
    monkeypatch.setattr(pg, "list_seasons", list_seasons)
    monkeypatch.setattr(pg, "list_series", list_series)
    monkeypatch.setattr(pg, "find_series", find_series)
    monkeypatch.setattr(pg, "find_race", find_race)
    monkeypatch.setattr(pg, "ensure_season", ensure_season)
    monkeypatch.setattr(pg, "ensure_series", ensure_series)
    monkeypatch.setattr(pg, "renumber_races", renumber_races)
    monkeypatch.setattr(pg, "list_all_races", list_all_races)
    monkeypatch.setattr(pg, "list_season_races_with_results", list_season_races_with_results)
    monkeypatch.setattr(pg, "get_fleet", get_fleet)
    monkeypatch.setattr(pg, "set_fleet", set_fleet)
    monkeypatch.setattr(pg, "get_settings", get_settings)
    monkeypatch.setattr(pg, "set_settings", set_settings)
    # Patch scoring constants and helper functions in-place
    import app.scoring as _scoring
    settings = memory_store["settings"]
    hd, hd_def = _scoring._build_lookup(settings.get("handicap_delta_by_rank", []), "rank", "delta_s_per_hr")
    lp, lp_def = _scoring._build_lookup(settings.get("league_points_by_rank", []), "rank", "points")
    ff, ff_def = _scoring._build_lookup(settings.get("fleet_size_factor", []), "finishers", "factor")
    _scoring._HANDICAP_DELTAS = hd
    _scoring._HANDICAP_DEFAULT = hd_def
    _scoring._LEAGUE_POINTS = lp
    _scoring._POINTS_DEFAULT = lp_def
    _scoring._FLEET_FACTORS = ff
    _scoring._FLEET_DEFAULT = ff_def
    _scoring._full_delta = lambda position: int(hd.get(position, hd_def))  # type: ignore
    _scoring._scaling_factor = lambda size: float(ff.get(size, ff_def))  # type: ignore
    _scoring._base_points = lambda position: float(lp.get(position, lp_def))  # type: ignore
    yield
