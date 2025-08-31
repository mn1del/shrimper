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
from importlib import reload


# Swap datastore functions used by route handlers
_base.load_data = _pg.load_data
_base.save_data = _pg.save_data
_base.ds_list_all_races = _pg.list_all_races
_base.ds_list_seasons = _pg.list_seasons
_base.ds_find_series = _pg.find_series
_base.ds_find_race = _pg.find_race
_base.ds_ensure_series = _pg.ensure_series
_base.ds_renumber_races = _pg.renumber_races
_base.ds_get_fleet = _pg.get_fleet
_base.ds_set_fleet = _pg.set_fleet
_base.ds_get_settings = _pg.get_settings
_base.ds_set_settings = _pg.set_settings


# Ensure scoring module reads settings from PostgreSQL
_scoring.get_settings = _pg.get_settings
reload(_scoring)


# Re-export the blueprint and recalculation entry point
bp = _base.bp
recalculate_handicaps = _base.recalculate_handicaps
