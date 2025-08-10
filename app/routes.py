from flask import Blueprint, redirect, render_template, url_for, abort
import json
from pathlib import Path


bp = Blueprint('main', __name__)

DATA_DIR = Path(__file__).resolve().parent.parent / 'data'


@bp.app_context_processor
def inject_nav_data():
    """Load series and race data for navigation menus."""
    series_entries = []
    series_dir = DATA_DIR / 'series'
    races_dir = DATA_DIR / 'races'
    if series_dir.exists():
        for path in sorted(series_dir.glob('*.json')):
            with path.open() as f:
                series = json.load(f)
            races = []
            for race_id in series.get('race_ids', []):
                race_path = races_dir / f"{race_id}.json"
                if race_path.exists():
                    with race_path.open() as rf:
                        races.append(json.load(rf))
            series_entries.append({'series': series, 'races': races})
    return {'nav_series': series_entries}


@bp.route('/')
def index():
    return redirect(url_for('main.series_index'))


@bp.route('/race-sheets')
def series_index():
    series_list = []
    series_dir = DATA_DIR / 'series'
    races_dir = DATA_DIR / 'races'
    if series_dir.exists():
        for path in sorted(series_dir.glob('*.json')):
            with path.open() as f:
                series = json.load(f)
            race_dates = []
            for race_id in series.get('race_ids', []):
                race_path = races_dir / f"{race_id}.json"
                if race_path.exists():
                    with race_path.open() as rf:
                        race = json.load(rf)
                    if race.get('date'):
                        race_dates.append(race['date'])
            if race_dates:
                dates = f"{min(race_dates)} - {max(race_dates)}" if len(set(race_dates)) > 1 else race_dates[0]
            else:
                dates = ''
            series_list.append({
                'series_id': series.get('series_id'),
                'name': series.get('name'),
                'dates': dates,
                'num_races': len(series.get('race_ids', [])),
                'updated_at': series.get('updated_at') or series.get('created_at', ''),
            })
    return render_template('race_sheets.html', title='Series Index', series_list=series_list)


@bp.route('/series/new')
def series_new():
    breadcrumbs = [('Race Sheets', url_for('main.series_index')), ('Create New Series', None)]
    return render_template('series_form.html', title='Create New Series', breadcrumbs=breadcrumbs)


@bp.route('/races/new')
def race_new():
    breadcrumbs = [('Race Sheets', url_for('main.series_index')), ('Create New Race', None)]
    series_dir = DATA_DIR / 'series'
    series_list = []
    if series_dir.exists():
        for path in sorted(series_dir.glob('*.json')):
            with path.open() as f:
                series_list.append(json.load(f))
    fleet_path = DATA_DIR / 'fleet.json'
    with fleet_path.open() as f:
        fleet_data = json.load(f)
    competitors = fleet_data.get('competitors', [])
    return render_template('race_form.html', title='Create New Race', breadcrumbs=breadcrumbs, series_list=series_list, competitors=competitors)


@bp.route('/series/<series_id>')
def series_detail(series_id):
    series_path = DATA_DIR / 'series' / f"{series_id}.json"
    if not series_path.exists():
        abort(404)
    with series_path.open() as f:
        series = json.load(f)
    races = []
    races_dir = DATA_DIR / 'races'
    for race_id in series.get('race_ids', []):
        race_path = races_dir / f"{race_id}.json"
        if race_path.exists():
            with race_path.open() as rf:
                races.append(json.load(rf))
    breadcrumbs = [('Race Sheets', url_for('main.series_index')), (series.get('name', series_id), None)]
    return render_template('series_detail.html', title=series.get('name', series_id), breadcrumbs=breadcrumbs, series=series, races=races)


@bp.route('/races/<race_id>')
def race_sheet(race_id):
    race_path = DATA_DIR / 'races' / f"{race_id}.json"
    if not race_path.exists():
        abort(404)
    with race_path.open() as f:
        race = json.load(f)
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
