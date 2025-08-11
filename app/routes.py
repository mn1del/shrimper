from flask import Blueprint, redirect, render_template, url_for, abort
import json
from pathlib import Path


bp = Blueprint('main', __name__)

DATA_DIR = Path(__file__).resolve().parent.parent / 'data'


def _series_meta_paths():
    """Yield paths to all series metadata files across seasons."""
    for season_dir in DATA_DIR.iterdir():
        if season_dir.is_dir():
            yield from season_dir.glob("*/series_metadata.json")


def _load_series_entries():
    """Return list of series with their race data."""
    entries = []
    for meta_path in sorted(_series_meta_paths()):
        with meta_path.open() as f:
            series = json.load(f)
        races = []
        races_dir = meta_path.parent / "races"
        for race_id in series.get("race_ids", []):
            race_path = races_dir / f"{race_id}.json"
            if race_path.exists():
                with race_path.open() as rf:
                    races.append(json.load(rf))
        entries.append({"series": series, "races": races})
    return entries


def _find_series(series_id: str):
    """Return (series, races) for the given series id or (None, None)."""
    for entry in _load_series_entries():
        if entry["series"].get("series_id") == series_id:
            return entry["series"], entry["races"]
    return None, None


def _find_race(race_id: str):
    """Return race data for the given race id or None if not found."""
    for meta_path in _series_meta_paths():
        races_dir = meta_path.parent / "races"
        race_path = races_dir / f"{race_id}.json"
        if race_path.exists():
            with race_path.open() as f:
                return json.load(f)
    return None


@bp.app_context_processor
def inject_nav_data():
    """Load series and race data for navigation menus."""
    return {'nav_series': _load_series_entries()}


@bp.route('/')
def index():
    return redirect(url_for('main.series_index'))


@bp.route('/race-sheets')
def series_index():
    series_list = []
    for entry in _load_series_entries():
        series = entry["series"]
        races = entry["races"]
        race_dates = [r.get("date") for r in races if r.get("date")]
        if race_dates:
            dates = f"{min(race_dates)} - {max(race_dates)}" if len(set(race_dates)) > 1 else race_dates[0]
        else:
            dates = ""
        series_list.append({
            "series_id": series.get("series_id"),
            "name": series.get("name"),
            "dates": dates,
            "num_races": len(races),
            "updated_at": series.get("updated_at") or series.get("created_at", ""),
        })
    return render_template("race_sheets.html", title="Series Index", series_list=series_list)


@bp.route('/series/new')
def series_new():
    breadcrumbs = [('Race Sheets', url_for('main.series_index')), ('Create New Series', None)]
    return render_template('series_form.html', title='Create New Series', breadcrumbs=breadcrumbs)


@bp.route('/races/new')
def race_new():
    breadcrumbs = [('Race Sheets', url_for('main.series_index')), ('Create New Race', None)]
    series_list = [entry['series'] for entry in _load_series_entries()]
    fleet_path = DATA_DIR / 'fleet.json'
    with fleet_path.open() as f:
        fleet_data = json.load(f)
    competitors = fleet_data.get('competitors', [])
    return render_template('race_form.html', title='Create New Race', breadcrumbs=breadcrumbs, series_list=series_list, competitors=competitors)


@bp.route('/series/<series_id>')
def series_detail(series_id):
    series, races = _find_series(series_id)
    if series is None:
        abort(404)
    breadcrumbs = [('Race Sheets', url_for('main.series_index')), (series.get('name', series_id), None)]
    return render_template('series_detail.html', title=series.get('name', series_id), breadcrumbs=breadcrumbs, series=series, races=races)


@bp.route('/races/<race_id>')
def race_sheet(race_id):
    race = _find_race(race_id)
    if race is None:
        abort(404)
    breadcrumbs = [('Race Sheets', url_for('main.series_index')), (race.get('name', race_id), None)]
    return render_template('race_sheet.html', title=race.get('name', race_id), breadcrumbs=breadcrumbs, race=race)


@bp.route('/standings/traditional')
def standings_traditional():
    breadcrumbs = [('Standings', None), ('Traditional', None)]
    return render_template('standings_traditional.html', title='Traditional Standings', breadcrumbs=breadcrumbs)


@bp.route('/standings/league')
def standings_league():
    breadcrumbs = [('Standings', None), ('League', None)]
    return render_template('standings_league.html', title='League Standings', breadcrumbs=breadcrumbs)


@bp.route('/fleet')
def fleet():
    breadcrumbs = [('Fleet', None)]
    data_path = Path(__file__).resolve().parent.parent / 'data' / 'fleet.json'
    with data_path.open() as f:
        data = json.load(f)
    competitors = data.get('competitors', [])
    return render_template('fleet.html', title='Fleet', breadcrumbs=breadcrumbs, fleet=competitors)


@bp.route('/rules')
def rules():
    breadcrumbs = [('Rules', None)]
    return render_template('rules.html', title='Rules', breadcrumbs=breadcrumbs)


@bp.route('/settings')
def settings():
    breadcrumbs = [('Settings', None)]
    data_path = DATA_DIR / 'settings.json'
    with data_path.open() as f:
        settings_data = json.load(f)
    return render_template('settings.html', title='Settings', breadcrumbs=breadcrumbs, settings=settings_data)
