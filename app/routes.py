from flask import Blueprint, redirect, render_template, url_for, abort, request
import json
from datetime import datetime
from pathlib import Path

from .scoring import calculate_race_results


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


def _load_all_races():
    """Return a flat list of all races with series info."""
    races = []
    for meta_path in _series_meta_paths():
        with meta_path.open() as f:
            series = json.load(f)
        series_name = series.get("name")
        series_id = series.get("series_id")
        races_dir = meta_path.parent / "races"
        for race_path in races_dir.glob("*.json"):
            with race_path.open() as rf:
                race = json.load(rf)
            finishers = sum(1 for e in race.get("entrants", []) if e.get("finish_time"))
            races.append({
                "race_id": race.get("race_id"),
                "date": race.get("date"),
                "start_time": race.get("start_time"),
                "series_name": series_name,
                "series_id": series_id,
                "finishers": finishers,
            })
    # Sort races by date and start time in descending order so the most recent
    # race appears first in the list. Missing dates or times are treated as
    # empty strings so they sort last.
    races.sort(
        key=lambda r: (r["date"] or "", r["start_time"] or ""),
        reverse=True,
    )
    return races


def _find_series(series_id: str):
    """Return (series, races) for the given series id or (None, None).

    Series identifiers may appear with inconsistent casing across the data.
    To make routing more robust, comparisons are performed case-insensitively.
    """
    target = series_id.lower()
    for entry in _load_series_entries():
        sid = entry["series"].get("series_id")
        if sid and sid.lower() == target:
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


def _race_path(race_id: str):
    """Return the Path to a race JSON file or None if not found."""
    for meta_path in _series_meta_paths():
        race_path = meta_path.parent / "races" / f"{race_id}.json"
        if race_path.exists():
            return race_path
    return None


@bp.route('/')
def index():
    return redirect(url_for('main.races'))


@bp.route('/races')
def races():
    race_list = _load_all_races()
    breadcrumbs = [('Races', None)]
    return render_template('races.html', title='Races', breadcrumbs=breadcrumbs, races=race_list)


def _load_series_meta(series_id: str):
    """Return (path, data) for the given series id or (None, None).

    Comparison is case-insensitive to tolerate differing user input.
    """
    target = series_id.lower()
    for meta_path in _series_meta_paths():
        with meta_path.open() as f:
            data = json.load(f)
        sid = data.get("series_id")
        if sid and sid.lower() == target:
            return meta_path, data
    return None, None


@bp.route('/races/new')
def race_new():
    series_list = [entry['series'] for entry in _load_series_entries()]
    fleet_path = DATA_DIR / 'fleet.json'
    with fleet_path.open() as f:
        fleet = json.load(f).get('competitors', [])
    blank_race = {
        'race_id': '__new__',
        'series_id': '',
        'date': '',
        'start_time': '',
        'entrants': [],
        'results': {},
    }
    breadcrumbs = [('Races', url_for('main.races')), ('Create New Race', None)]
    return render_template(
        'series_detail.html',
        title='Create New Race',
        breadcrumbs=breadcrumbs,
        series={},
        races=[],
        selected_race=blank_race,
        finisher_display='Number of Finishers: 0',
        fleet=fleet,
        series_list=series_list,
        unlocked=True,
    )


@bp.route('/series/<series_id>')
def series_detail(series_id):
    series, races = _find_series(series_id)
    if series is None:
        abort(404)

    race_id = request.args.get('race_id')
    if race_id == '__new__':
        return redirect(url_for('main.race_new'))

    selected_race = None
    finisher_count = 0
    fleet = []

    def _parse_hms(t: str | None) -> int | None:
        if not t:
            return None
        h, m, s = map(int, t.split(":"))
        return h * 3600 + m * 60 + s

    def _format_hms(seconds: float | None) -> str | None:
        if seconds is None:
            return None
        total = int(round(seconds))
        h = total // 3600
        m = (total % 3600) // 60
        s = total % 60
        return f"{h:02d}:{m:02d}:{s:02d}"

    if race_id:
        selected_race = _find_race(race_id)
        if selected_race:
            entrants = selected_race.get('entrants', [])
            start_seconds = _parse_hms(selected_race.get('start_time'))

            calc_entries = []
            for entrant in entrants:
                cid = entrant.get('competitor_id')
                if not cid:
                    continue
                entry = {
                    'competitor_id': cid,
                    'start': start_seconds or 0,
                    'initial_handicap': entrant.get('initial_handicap', 0),
                }
                ft = _parse_hms(entrant.get('finish_time'))
                if ft is not None:
                    entry['finish'] = ft
                status = entrant.get('status')
                if status:
                    entry['status'] = status
                calc_entries.append(entry)

            results_list = calculate_race_results(calc_entries)
            results: dict[str, dict] = {}
            for res in results_list:
                cid = res.get('competitor_id')
                finish_str = next(
                    (e.get('finish_time') for e in entrants if e.get('competitor_id') == cid),
                    None,
                )
                results[cid] = {
                    'finish_time': finish_str,
                    'on_course_secs': res.get('elapsed_seconds'),
                    'abs_pos': res.get('absolute_position'),
                    'allowance': res.get('allowance_seconds'),
                    'adj_time_secs': res.get('adjusted_time_seconds'),
                    'adj_time': _format_hms(res.get('adjusted_time_seconds')),
                    'hcp_pos': res.get('handicap_position'),
                    'race_pts': res.get('traditional_points'),
                    'league_pts': res.get('points'),
                    'full_delta': res.get('full_delta'),
                    'scaled_delta': res.get('scaled_delta'),
                    'actual_delta': res.get('actual_delta'),
                    'revised_hcp': res.get('revised_handicap'),
                    'place': res.get('status'),
                }

            selected_race['results'] = results
            finisher_count = sum(1 for r in results_list if r.get('finish') is not None)

            fleet_path = DATA_DIR / 'fleet.json'
            with fleet_path.open() as f:
                fleet = json.load(f).get('competitors', [])

    finisher_display = f"Number of Finishers: {finisher_count}"

    breadcrumbs = [('Races', url_for('main.races')), (series.get('name', series_id), None)]
    series_list = [entry['series'] for entry in _load_series_entries()]
    return render_template(
        'series_detail.html',
        title=series.get('name', series_id),
        breadcrumbs=breadcrumbs,
        series=series,
        races=races,
        selected_race=selected_race,
        finisher_display=finisher_display,
        fleet=fleet,
        series_list=series_list,
    )


@bp.route('/races/<race_id>')
def race_sheet(race_id):
    race = _find_race(race_id)
    if race is None:
        abort(404)
    series_id = race.get('series_id')
    if not series_id:
        abort(404)
    series, _ = _find_series(series_id)
    if not series:
        abort(404)
    canonical_id = series.get('series_id')
    return redirect(url_for('main.series_detail', series_id=canonical_id, race_id=race_id))


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


@bp.route('/api/races/<race_id>', methods=['POST'])
def update_race(race_id):
    data = request.get_json() or {}
    series_choice = data.get('series_id')
    new_series_name = data.get('new_series_name')
    race_date = data.get('date')
    start_time = data.get('start_time')
    finish_times = data.get('finish_times', [])
    if race_id == '__new__':
        if series_choice is None or not race_date:
            abort(400)
        start_time = start_time or ''
        timestamp = datetime.utcnow().isoformat() + 'Z'
        if series_choice == '__new__':
            if not new_series_name:
                abort(400)
            try:
                season = datetime.strptime(race_date, '%Y-%m-%d').year
            except ValueError:
                abort(400)
            series_id = f"SER_{season}_{new_series_name}"
            season_dir = DATA_DIR / str(season)
            series_dir = season_dir / new_series_name
            (series_dir / 'races').mkdir(parents=True, exist_ok=True)
            series_meta = {
                'series_id': series_id,
                'name': new_series_name,
                'season': int(season),
            }
            with (series_dir / 'series_metadata.json').open('w') as f:
                json.dump(series_meta, f, indent=2)
        else:
            meta_path, series_meta = _load_series_meta(series_choice)
            if not meta_path:
                abort(400)
            series_id = series_meta.get('series_id')
            series_dir = meta_path.parent
        if start_time:
            try:
                datetime.strptime(start_time, '%H:%M:%S')
            except ValueError:
                abort(400)
        races_dir = series_dir / 'races'
        races_dir.mkdir(parents=True, exist_ok=True)
        seq = len(list(races_dir.glob('*.json'))) + 1
        race_id = f"RACE_{race_date}_{series_meta['name']}_{seq}"
        race_name = f"{series_id}_{seq}"
        race_data = {
            'race_id': race_id,
            'series_id': series_id,
            'name': race_name,
            'date': race_date,
            'start_time': start_time,
            'status': 'draft',
            'created_at': timestamp,
            'updated_at': timestamp,
            'entrants': [
                {'competitor_id': ft['competitor_id'], 'finish_time': ft.get('finish_time')}
                for ft in finish_times
            ],
            'results': {},
        }
        with (races_dir / f'{race_id}.json').open('w') as f:
            json.dump(race_data, f, indent=2)
        finisher_count = sum(1 for ft in finish_times if ft.get('finish_time'))
        redirect_url = url_for('main.series_detail', series_id=series_id, race_id=race_id)
        return {'finisher_count': finisher_count, 'redirect': redirect_url}

    race_path = _race_path(race_id)
    if race_path is None:
        abort(404)
    with race_path.open() as f:
        race_data = json.load(f)

    redirect_url = None
    current_series_id = race_data.get('series_id')
    if series_choice:
        if series_choice == '__new__':
            if not new_series_name:
                abort(400)
            date_str = race_date or race_data.get('date')
            if not date_str:
                abort(400)
            try:
                season = datetime.strptime(date_str, '%Y-%m-%d').year
            except ValueError:
                abort(400)
            season_dir = DATA_DIR / str(season)
            series_dir = season_dir / new_series_name
            (series_dir / 'races').mkdir(parents=True, exist_ok=True)
            series_id = f"SER_{season}_{new_series_name}"
            series_meta = {
                'series_id': series_id,
                'name': new_series_name,
                'season': int(season),
            }
            with (series_dir / 'series_metadata.json').open('w') as f:
                json.dump(series_meta, f, indent=2)
        else:
            series_id = series_choice
            meta_path, series_meta = _load_series_meta(series_id)
            if not meta_path:
                abort(400)
            series_dir = meta_path.parent

        if series_id != current_series_id:
            new_races_dir = series_dir / 'races'
            new_races_dir.mkdir(parents=True, exist_ok=True)
            new_path = new_races_dir / f'{race_id}.json'
            race_path.rename(new_path)
            race_path = new_path
            race_data['series_id'] = series_id
            redirect_url = url_for('main.series_detail', series_id=series_id, race_id=race_id)

    if race_date is not None:
        race_data['date'] = race_date
    if start_time is not None:
        race_data['start_time'] = start_time
    if finish_times:
        ft_map = {ft['competitor_id']: ft.get('finish_time') for ft in finish_times}
        for entrant in race_data.get('entrants', []):
            cid = entrant.get('competitor_id')
            if cid in ft_map:
                entrant['finish_time'] = ft_map[cid]

    race_data['updated_at'] = datetime.utcnow().isoformat() + 'Z'
    with race_path.open('w') as f:
        json.dump(race_data, f, indent=2)

    finisher_count = sum(1 for e in race_data.get('entrants', []) if e.get('finish_time'))
    return {'finisher_count': finisher_count, 'redirect': redirect_url}
