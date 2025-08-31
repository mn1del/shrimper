"""
PostgreSQL-backed routes shim.

This module reuses the existing route handlers from app.routes but swaps the
underlying datastore functions to use PostgreSQL via app.datastore_pg.

How to enable:
- Set DATABASE_URL in your environment.
- Update app/__init__.py to import and register this blueprint instead of
  app.routes, e.g.:

    from . import routes_pg as routes
    app.register_blueprint(routes.bp)
    routes.recalculate_handicaps()
"""

from . import routes as _base
from . import datastore_pg as _pg
from . import scoring as _scoring
import importlib as _importlib


# Swap datastore functions used by route handlers
_base.load_data = _pg.load_data
_base.save_data = _pg.save_data
_base.ds_list_all_races = _pg.list_all_races
_base.ds_list_seasons = _pg.list_seasons
_base.ds_list_series = _pg.list_series
_base.ds_list_season_races_with_results = _pg.list_season_races_with_results
_base.ds_find_series = _pg.find_series
_base.ds_find_race = _pg.find_race
_base.ds_ensure_series = _pg.ensure_series
_base.ds_renumber_races = _pg.renumber_races
_base.ds_get_fleet = _pg.get_fleet
_base.ds_set_fleet = _pg.set_fleet
_base.ds_get_settings = _pg.get_settings
_base.ds_set_settings = _pg.set_settings


# Ensure scoring constants reflect PostgreSQL settings
def init_backend():
    """Initialize DB pool and patch scoring settings from PostgreSQL."""
    try:
        # Initialize a modest connection pool; override via env if desired
        import os as _os
        try:
            minconn = int(_os.environ.get("DB_POOL_MIN", "1"))
        except ValueError:
            minconn = 1
        try:
            maxconn = int(_os.environ.get("DB_POOL_MAX", "10"))
        except ValueError:
            maxconn = 10
        _pg.init_pool(minconn=minconn, maxconn=maxconn)

        settings = _pg.get_settings()
        _scoring._SETTINGS = settings  # type: ignore[attr-defined]
        hd, hd_def = _scoring._build_lookup(settings.get("handicap_delta_by_rank", []), "rank", "delta_s_per_hr")  # type: ignore[attr-defined]
        lp, lp_def = _scoring._build_lookup(settings.get("league_points_by_rank", []), "rank", "points")  # type: ignore[attr-defined]
        ff, ff_def = _scoring._build_lookup(settings.get("fleet_size_factor", []), "finishers", "factor")  # type: ignore[attr-defined]
        _scoring._HANDICAP_DELTAS = hd  # type: ignore[attr-defined]
        _scoring._HANDICAP_DEFAULT = hd_def  # type: ignore[attr-defined]
        _scoring._LEAGUE_POINTS = lp  # type: ignore[attr-defined]
        _scoring._POINTS_DEFAULT = lp_def  # type: ignore[attr-defined]
        _scoring._FLEET_FACTORS = ff  # type: ignore[attr-defined]
        _scoring._FLEET_DEFAULT = ff_def  # type: ignore[attr-defined]
    except Exception:
        # Don't break startup if DB is missing or schema incomplete; routes will
        # still function and settings can be updated later.
        pass


def _reload_and_patch(mod):
    res = _importlib.reload(mod)
    if getattr(mod, "__name__", "") == _scoring.__name__:
        settings = _pg.get_settings()
        mod._SETTINGS = settings  # type: ignore[attr-defined]
        hd, hd_def = mod._build_lookup(settings.get("handicap_delta_by_rank", []), "rank", "delta_s_per_hr")  # type: ignore[attr-defined]
        lp, lp_def = mod._build_lookup(settings.get("league_points_by_rank", []), "rank", "points")  # type: ignore[attr-defined]
        ff, ff_def = mod._build_lookup(settings.get("fleet_size_factor", []), "finishers", "factor")  # type: ignore[attr-defined]
        mod._HANDICAP_DELTAS = hd  # type: ignore[attr-defined]
        mod._HANDICAP_DEFAULT = hd_def  # type: ignore[attr-defined]
        mod._LEAGUE_POINTS = lp  # type: ignore[attr-defined]
        mod._POINTS_DEFAULT = lp_def  # type: ignore[attr-defined]
        mod._FLEET_FACTORS = ff  # type: ignore[attr-defined]
        mod._FLEET_DEFAULT = ff_def  # type: ignore[attr-defined]
    return res


# Monkeypatch routes.importlib.reload so settings updates keep PG constants
_base.importlib.reload = _reload_and_patch  # type: ignore[attr-defined]


# Re-export the blueprint and recalculation entry point
bp = _base.bp
recalculate_handicaps = _base.recalculate_handicaps
