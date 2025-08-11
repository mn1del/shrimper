from flask import Blueprint, redirect, render_template, url_for, abort, request
import json
from datetime import datetime
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
        for race_path in sorted(races_dir.glob("*.json")):
            with race_path.open() as rf:
                races.append(json.load(rf))
        entries.append({"series": series, "races": races})
    return entries


def _load_nav_data():
    """Return navigation data grouped by season then series."""
    seasons = {}
    for meta_path in sorted(_series_meta_paths()):
        season = meta_path.parent.parent.name
        with meta_path.open() as f:
            series = json.load(f)
        races = []
        races_dir = meta_path.parent / "races"
        for race_path in sorted(races_dir.glob("*.json")):
            with race_path.open() as rf:
                races.append(json.load(rf))
        seasons.setdefault(season, []).append({"series": series, "races": races})

    nav = []
    for season in sorted(seasons):
        nav.append({"season": season, "series": seasons[season]})
    return nav


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


NAV_RACE_SERIES = _load_nav_data()


@bp.app_context_processor
def inject_nav_data():
    """Expose navigation data for menus."""
    return {'nav_race_series': NAV_RACE_SERIES}


@bp.route('/')
def index():
    return redirect(url_for('main.series_index'))


@bp.route('/race-series')
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
        })
    return render_template("race_series.html", title="Series Index", series_list=series_list)


def _load_series_meta(series_id: str):
    """Return (path, data) for the given series id or (None, None)."""
    for meta_path in _series_meta_paths():
        with meta_path.open() as f:
            data = json.load(f)
        if data.get("series_id") == series_id:
            return meta_path, data
    return None, None


@bp.route('/race-series/new', methods=['GET', 'POST'])
def race_or_series_new():
    if request.method == 'POST':
        existing_series_id = request.form.get('existing_series_id') or None
        race_id = request.form.get('race_id')
        race_name = request.form.get('race_name')
        race_date = request.form.get('race_date')
        timestamp = datetime.utcnow().isoformat() + 'Z'

        if existing_series_id:
            meta_path, series_meta = _load_series_meta(existing_series_id)
            if meta_path is None:
                abort(400)
            series_id = series_meta.get('series_id')
            season_dir = meta_path.parent.parent
            series_dir = meta_path.parent
        else:
            series_id = request.form.get('new_series_id')
            series_name = request.form.get('new_series_name')
            season = request.form.get('new_series_season')
            season_dir = DATA_DIR / str(season)
            series_dir = season_dir / series_name
            (series_dir / 'races').mkdir(parents=True, exist_ok=True)
            series_meta = {
                'series_id': series_id,
                'name': series_name,
                'season': int(season),
            }
            with (series_dir / 'series_metadata.json').open('w') as f:
                json.dump(series_meta, f, indent=2)

        race_data = {
            'race_id': race_id,
            'series_id': series_id,
            'name': race_name,
            'date': race_date,
            'start_time': '',
            'status': 'draft',
            'created_at': timestamp,
            'updated_at': timestamp,
            'entrants': [],
            'results': {},
        }

        races_dir = series_dir / 'races'
        races_dir.mkdir(parents=True, exist_ok=True)
        with (races_dir / f'{race_id}.json').open('w') as f:
            json.dump(race_data, f, indent=2)

        global NAV_RACE_SERIES
        NAV_RACE_SERIES = _load_nav_data()

        return redirect(url_for('main.series_detail', series_id=series_id))

    series_list = [entry['series'] for entry in _load_series_entries()]
    breadcrumbs = [('Race Series', url_for('main.series_index')), ('Create New Race or Series', None)]
    return render_template('race_or_series_form.html', title='Create New Race or Series', breadcrumbs=breadcrumbs, series_list=series_list)


@bp.route('/series/<series_id>')
def series_detail(series_id):
    series, races = _find_series(series_id)
    if series is None:
        abort(404)

    race_id = request.args.get('race_id')
    if race_id == '__new__':
        return redirect(url_for('main.race_or_series_new'))

    selected_race = None
    finisher_count = 0
    fleet = []
    if race_id:
        selected_race = _find_race(race_id)
        if selected_race:
            results = selected_race.get('results', {})
            if isinstance(results, list):
                results = {r.get('competitor_id'): r for r in results if r.get('competitor_id')}
                selected_race['results'] = results
            finisher_count = sum(
                1 for res in results.values() if res.get('finish_time')
            )
            fleet_path = DATA_DIR / 'fleet.json'
            with fleet_path.open() as f:
                fleet = json.load(f).get('competitors', [])

    breadcrumbs = [('Race Series', url_for('main.series_index')), (series.get('name', series_id), None)]
    return render_template(
        'series_detail.html',
        title=series.get('name', series_id),
        breadcrumbs=breadcrumbs,
        series=series,
        races=races,
        selected_race=selected_race,
        finisher_count=finisher_count,
        fleet=fleet,
    )


@bp.route('/races/<race_id>')
def race_sheet(race_id):
    race = _find_race(race_id)
    if race is None:
        abort(404)
    breadcrumbs = [('Race Series', url_for('main.series_index')), (race.get('name', race_id), None)]
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
