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
    races.sort(key=lambda r: (r["date"] or "", r["start_time"] or ""))
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


@bp.route('/races/new', methods=['GET', 'POST'])
def race_new():
    if request.method == 'POST':
        series_choice = request.form.get('series_id')
        race_date = request.form.get('race_date')
        race_time = request.form.get('race_time') or ''
        if series_choice is None or not race_date:
            abort(400)

        timestamp = datetime.utcnow().isoformat() + 'Z'

        if series_choice == '__new__':
            series_name = request.form.get('new_series_name')
            season = request.form.get('new_series_season')
            if not series_name or not season:
                abort(400)
            series_id = f"SER_{season}_{series_name}"
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
        else:
            meta_path, series_meta = _load_series_meta(series_choice)
            if meta_path is None:
                abort(400)
            series_id = series_meta.get('series_id')
            series_dir = meta_path.parent

        if race_time:
            try:
                datetime.strptime(race_time, '%H:%M:%S')
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
            'start_time': race_time,
            'status': 'draft',
            'created_at': timestamp,
            'updated_at': timestamp,
            'entrants': [],
            'results': {},
        }

        with (races_dir / f'{race_id}.json').open('w') as f:
            json.dump(race_data, f, indent=2)

        return redirect(url_for('main.series_detail', series_id=series_id))

    series_list = [entry['series'] for entry in _load_series_entries()]
    breadcrumbs = [('Races', url_for('main.races')), ('Create New Race', None)]
    selected_series = request.args.get('series_id')
    return render_template(
        'race_form.html',
        title='Create New Race',
        breadcrumbs=breadcrumbs,
        series_list=series_list,
        selected_series=selected_series,
    )


@bp.route('/series/<series_id>')
def series_detail(series_id):
    series, races = _find_series(series_id)
    if series is None:
        abort(404)

    race_id = request.args.get('race_id')
    if race_id == '__new__':
        return redirect(url_for('main.race_new', series_id=series.get('series_id')))

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

    breadcrumbs = [('Races', url_for('main.races')), (series.get('name', series_id), None)]
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
