from flask import Blueprint, redirect, render_template, url_for, abort, request
import json
import importlib
from datetime import datetime
from pathlib import Path

from .scoring import calculate_race_results, _scaling_factor
from . import scoring as scoring_module


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
        season = series.get("season")
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
                "season": season,
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


def _parse_hms(t: str | None) -> int | None:
    """Return seconds for an ``HH:MM:SS`` timestamp or ``None``."""
    if not t:
        return None
    h, m, s = map(int, t.split(":"))
    return h * 3600 + m * 60 + s


def _fleet_lookup() -> dict[str, dict]:
    """Return mapping of competitor id to fleet details."""
    path = DATA_DIR / "fleet.json"
    try:
        with path.open() as f:
            competitors = json.load(f).get("competitors", [])
    except FileNotFoundError:
        competitors = []
    return {c.get("competitor_id"): c for c in competitors if c.get("competitor_id")}


def _next_competitor_id(existing: set[str]) -> str:
    """Return a new unique competitor id."""
    max_id = 0
    for cid in existing:
        try:
            num = int(cid.split("_")[1])
            if num > max_id:
                max_id = num
        except (IndexError, ValueError):
            continue
    return f"C_{max_id + 1}"


def recalculate_handicaps() -> None:
    """Recompute starting handicaps for all races from revised results.

    The fleet register provides the baseline starting handicaps. Each race is
    processed in chronological order and the entrants' ``initial_handicap``
    values are replaced with the current handicap prior to that race. Revised
    handicaps produced from the race are then fed forward to subsequent races
    and ultimately written back to the fleet register.
    """

    fleet_path = DATA_DIR / "fleet.json"
    try:
        with fleet_path.open() as f:
            fleet_data = json.load(f)
    except FileNotFoundError:
        return

    competitors = fleet_data.get("competitors", [])
    handicap_map = {
        c.get("competitor_id"): c.get("starting_handicap_s_per_hr", 0)
        for c in competitors
        if c.get("competitor_id")
    }

    race_files: list[tuple[dict, Path]] = []
    for race_path in DATA_DIR.rglob("RACE_*.json"):
        with race_path.open() as rf:
            race_files.append((json.load(rf), race_path))

    race_files.sort(key=lambda r: (r[0].get("date"), r[0].get("start_time")))

    for race, race_path in race_files:
        start_seconds = _parse_hms(race.get("start_time")) or 0
        calc_entries: list[dict] = []
        for ent in race.get("entrants", []):
            cid = ent.get("competitor_id")
            if not cid:
                continue
            initial = handicap_map.get(cid, 0)
            ent["initial_handicap"] = initial
            entry = {
                "competitor_id": cid,
                "start": start_seconds,
                "initial_handicap": initial,
            }
            ft = ent.get("finish_time")
            if ft:
                parsed = _parse_hms(ft)
                if parsed is not None:
                    entry["finish"] = parsed
            status = ent.get("status")
            if status:
                entry["status"] = status
            calc_entries.append(entry)

        if calc_entries:
            results = calculate_race_results(calc_entries)
            for res in results:
                cid = res.get("competitor_id")
                revised = res.get("revised_handicap")
                if cid and revised is not None:
                    handicap_map[cid] = revised

        with race_path.open("w") as rf:
            json.dump(race, rf, indent=2)

    for comp in competitors:
        cid = comp.get("competitor_id")
        if cid:
            comp["current_handicap_s_per_hr"] = handicap_map.get(
                cid, comp.get("current_handicap_s_per_hr", 0)
            )

    with fleet_path.open("w") as f:
        json.dump(fleet_data, f, indent=2)


def _season_standings(season: int, scoring: str) -> tuple[list[dict], list[dict]]:
    """Compute standings and per-race metadata for a season."""
    fleet = _fleet_lookup()
    race_groups: list[dict] = []

    for meta_path in _series_meta_paths():
        with meta_path.open() as f:
            series = json.load(f)
        if int(series.get("season", 0)) != int(season):
            continue

        group = {"series_name": series.get("name"), "races": []}
        races_dir = meta_path.parent / "races"
        for race_path in races_dir.glob("*.json"):
            with race_path.open() as rf:
                race = json.load(rf)
            start_seconds = _parse_hms(race.get("start_time")) or 0
            entrants_map = {
                ent.get("competitor_id"): ent
                for ent in race.get("entrants", [])
                if ent.get("competitor_id")
            }
            if not entrants_map:
                continue
            entries: list[dict] = []
            for cid, info in fleet.items():
                entry = {
                    "competitor_id": cid,
                    "start": start_seconds,
                    "initial_handicap": entrants_map.get(cid, {}).get(
                        "initial_handicap",
                        info.get("current_handicap_s_per_hr")
                        or info.get("starting_handicap_s_per_hr", 0),
                    ),
                    "sailor": info.get("sailor_name"),
                    "boat": info.get("boat_name"),
                    "sail_number": info.get("sail_no"),
                }
                ent = entrants_map.get(cid)
                if ent:
                    ft = ent.get("finish_time")
                    if ft:
                        entry["finish"] = _parse_hms(ft)
                    status = ent.get("status")
                    if status:
                        entry["status"] = status
                entries.append(entry)
            results = calculate_race_results(entries)
            group["races"].append(
                {
                    "race_id": race.get("race_id"),
                    "date": race.get("date"),
                    "start_time": race.get("start_time"),
                    "results": results,
                }
            )
        if group["races"]:
            group["races"].sort(key=lambda r: (r["date"] or "", r["start_time"] or ""))
            race_groups.append(group)

    race_groups.sort(key=lambda g: g["series_name"] or "")

    aggregates: dict[str, dict] = {}
    for idx, group in enumerate(race_groups):
        for race in group["races"]:
            finisher_count = sum(1 for r in race["results"] if r.get("finish") is not None)
            for res in race["results"]:
                cid = res.get("competitor_id")
                agg = aggregates.setdefault(
                    cid,
                    {
                        "sailor": res.get("sailor"),
                        "boat": res.get("boat"),
                        "sail_number": res.get("sail_number"),
                        "race_count": 0,
                        "league_points": 0.0,
                        "traditional_points": 0.0,
                        "race_points": {},
                        "series_totals": {},
                        "series_results": {},
                        "dropped_races": set(),
                    },
                )
                finished = res.get("finish") is not None
                if finished:
                    agg["race_count"] += 1
                league_pts = res.get("points", 0.0)
                trad_pts = res.get("traditional_points")
                if trad_pts is None and res.get("finish") is None:
                    trad_pts = finisher_count + 1
                elif trad_pts is None:
                    trad_pts = 0.0
                agg["league_points"] += league_pts
                agg["traditional_points"] += trad_pts
                if scoring == "traditional":
                    agg["race_points"][race["race_id"]] = trad_pts
                    series_list = agg["series_results"].setdefault(idx, [])
                    series_list.append(
                        {
                            "race_id": race["race_id"],
                            "points": trad_pts,
                            "finished": finished,
                        }
                    )
                else:
                    agg["race_points"][race["race_id"]] = league_pts
                    agg["series_totals"][idx] = agg["series_totals"].get(idx, 0.0) + league_pts

    standings: list[dict] = []
    for agg in aggregates.values():
        if scoring == "league" and agg["race_count"] == 0:
            continue
        if scoring == "traditional":
            series_totals: dict[int, float] = {}
            series_counts: dict[int, int] = {}
            dropped: set[str] = set()
            for sidx, results in agg["series_results"].items():
                raw_total = sum(r["points"] for r in results)
                finish_count = sum(1 for r in results if r["finished"])
                series_counts[sidx] = finish_count
                if finish_count > 4:
                    drop_n = 2
                elif finish_count == 4:
                    drop_n = 1
                else:
                    drop_n = 0
                drop_points = 0.0
                if drop_n:
                    sorted_res = sorted(results, key=lambda r: r["points"], reverse=True)
                    to_drop = sorted_res[:drop_n]
                    drop_points = sum(r["points"] for r in to_drop)
                    dropped.update(r["race_id"] for r in to_drop)
                series_totals[sidx] = raw_total - drop_points
            total = sum(series_totals.values())
            standings.append(
                {
                    "sailor": agg["sailor"],
                    "boat": agg["boat"],
                    "sail_number": agg["sail_number"],
                    "race_count": agg["race_count"],
                    "total_points": total,
                    "race_points": agg["race_points"],
                    "series_totals": series_totals,
                    "series_counts": series_counts,
                    "dropped_races": dropped,
                }
            )
        else:
            total = agg["league_points"]
            standings.append(
                {
                    "sailor": agg["sailor"],
                    "boat": agg["boat"],
                    "sail_number": agg["sail_number"],
                    "race_count": agg["race_count"],
                    "total_points": total,
                    "race_points": agg["race_points"],
                    "series_totals": agg["series_totals"],
                    "series_counts": {},
                    "dropped_races": set(),
                }
            )

    if scoring == "traditional":
        standings.sort(key=lambda r: (r["total_points"], -r["race_count"], r["sailor"]))
    else:
        standings.sort(key=lambda r: (-r["total_points"], -r["race_count"], r["sailor"]))

    prev_points: float | None = None
    prev_races: int | None = None
    prev_place = 0
    for idx, row in enumerate(standings, start=1):
        if prev_points is not None and row["total_points"] == prev_points and row["race_count"] == prev_races:
            row["position"] = f"={prev_place}"
        else:
            row["position"] = str(idx)
            prev_place = idx
            prev_points = row["total_points"]
            prev_races = row["race_count"]

    return standings, race_groups


@bp.route('/')
def index():
    return redirect(url_for('main.races'))


@bp.route('/races')
def races():
    season = request.args.get('season') or None
    all_races = _load_all_races()
    seasons = sorted({r.get('season') for r in all_races if r.get('season')}, reverse=True)
    if season:
        race_list = [r for r in all_races if str(r.get('season')) == str(season)]
    else:
        race_list = all_races
    breadcrumbs = [('Races', None)]
    return render_template(
        'races.html',
        title='Races',
        breadcrumbs=breadcrumbs,
        races=race_list,
        seasons=seasons,
        selected_season=season,
    )


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
        fleet_adjustment=0,
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
    fleet_adjustment = 0

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
        # Load baseline handicaps from fleet register
        fleet_path = DATA_DIR / 'fleet.json'
        with fleet_path.open() as f:
            fleet = json.load(f).get('competitors', [])
        handicap_map = {
            comp.get('competitor_id'): comp.get('starting_handicap_s_per_hr', 0)
            for comp in fleet
            if comp.get('competitor_id')
        }

        # Load all races and process them chronologically until target race
        race_objs = []
        for race_path in DATA_DIR.rglob('RACE_*.json'):
            with race_path.open() as rf:
                race = json.load(rf)
            race_objs.append(race)
        race_objs.sort(key=lambda r: (r.get('date'), r.get('start_time')))

        pre_race_handicaps = handicap_map
        results: dict[str, dict] = {}

        for race in race_objs:
            start_seconds = _parse_hms(race.get('start_time'))
            entrants = race.get('entrants', [])
            entrants_map = {
                e.get('competitor_id'): e for e in entrants if e.get('competitor_id')
            }
            snapshot = handicap_map.copy()

            if race.get('race_id') == race_id:
                # Build entries for full fleet using handicaps prior to this race
                calc_entries: list[dict] = []
                for comp in fleet:
                    cid = comp.get('competitor_id')
                    if not cid:
                        continue
                    entry = {
                        'competitor_id': cid,
                        'start': start_seconds or 0,
                        'initial_handicap': snapshot.get(cid, 0),
                    }
                    entrant = entrants_map.get(cid)
                    if entrant:
                        ft = _parse_hms(entrant.get('finish_time'))
                        if ft is not None:
                            entry['finish'] = ft
                        status = entrant.get('status')
                        if status:
                            entry['status'] = status
                    calc_entries.append(entry)

                results_list = calculate_race_results(calc_entries)
                finisher_count = sum(
                    1 for r in results_list if r.get('finish') is not None
                )
                if finisher_count:
                    fleet_adjustment = int(
                        round(_scaling_factor(finisher_count) * 100)
                    )
                for res in results_list:
                    cid = res.get('competitor_id')
                    entrant = entrants_map.get(cid, {})
                    finish_str = entrant.get('finish_time')
                    is_non_finisher = res.get('finish') is None
                    results[cid] = {
                        'finish_time': finish_str,
                        'on_course_secs': res.get('elapsed_seconds'),
                        'abs_pos': res.get('absolute_position'),
                        'allowance': res.get('allowance_seconds'),
                        'adj_time_secs': res.get('adjusted_time_seconds'),
                        'adj_time': _format_hms(res.get('adjusted_time_seconds')),
                        'hcp_pos': res.get('handicap_position'),
                        'race_pts': res.get('traditional_points')
                        if res.get('traditional_points') is not None
                        else (finisher_count + 1 if is_non_finisher else None),
                        'league_pts': res.get('points')
                        if res.get('points') is not None
                        else (0.0 if is_non_finisher else None),
                        'full_delta': res.get('full_delta')
                        if res.get('full_delta') is not None
                        else (0 if is_non_finisher else None),
                        'scaled_delta': res.get('scaled_delta')
                        if res.get('scaled_delta') is not None
                        else (0 if is_non_finisher else None),
                        'actual_delta': res.get('actual_delta')
                        if res.get('actual_delta') is not None
                        else (0 if is_non_finisher else None),
                        'revised_hcp': res.get('revised_handicap')
                        if res.get('revised_handicap') is not None
                        else (
                            res.get('initial_handicap') if is_non_finisher else None
                        ),
                        'place': res.get('status'),
                    }

                selected_race = race
                pre_race_handicaps = snapshot

                # Update map for completeness then stop processing
                for res in results_list:
                    cid = res.get('competitor_id')
                    revised = res.get('revised_handicap')
                    if revised is not None:
                        handicap_map[cid] = revised
                break

            # Process prior races to update handicap map
            calc_entries: list[dict] = []
            for cid, entrant in entrants_map.items():
                entry = {
                    'competitor_id': cid,
                    'start': start_seconds or 0,
                    'initial_handicap': snapshot.get(cid, 0),
                }
                ft = _parse_hms(entrant.get('finish_time'))
                if ft is not None:
                    entry['finish'] = ft
                status = entrant.get('status')
                if status:
                    entry['status'] = status
                calc_entries.append(entry)

            prior_results = calculate_race_results(calc_entries)
            for res in prior_results:
                cid = res.get('competitor_id')
                revised = res.get('revised_handicap')
                if revised is not None:
                    handicap_map[cid] = revised

        # Update fleet handicaps for display using pre-race values
        for comp in fleet:
            cid = comp.get('competitor_id')
            comp['current_handicap_s_per_hr'] = pre_race_handicaps.get(
                cid, comp.get('current_handicap_s_per_hr', 0)
            )

        if selected_race:
            selected_race['results'] = results

    finisher_display = f"Number of Finishers: {finisher_count}"

    # When viewing an individual race, suppress breadcrumbs and provide a list
    # of all races for navigation. Otherwise show the standard breadcrumb trail.
    if selected_race:
        breadcrumbs = None
        all_races = _load_all_races()
    else:
        breadcrumbs = [('Races', url_for('main.races')), (series.get('name', series_id), None)]
        all_races = []

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
        fleet_adjustment=fleet_adjustment,
        all_races=all_races,
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


@bp.route('/standings')
def standings():
    scoring = request.args.get('format', 'league').lower()
    season_param = request.args.get('season')
    seasons = sorted(
        {int(json.load(p.open()).get('season')) for p in _series_meta_paths()},
        reverse=True,
    )
    if not seasons:
        season_val = None
        table = []
        race_groups = []
    else:
        try:
            season_int = int(season_param) if season_param is not None else None
        except ValueError:
            season_int = None
        if season_int is None or season_int not in seasons:
            season_val = seasons[0]
        else:
            season_val = season_int
        table, race_groups = _season_standings(season_val, scoring)
    breadcrumbs = [('Standings', None)]
    return render_template(
        'standings.html',
        title='Standings',
        breadcrumbs=breadcrumbs,
        seasons=seasons,
        selected_season=season_val,
        scoring_format=scoring,
        standings=table,
        race_groups=race_groups,
    )


@bp.route('/fleet')
def fleet():
    breadcrumbs = [('Fleet', None)]
    data_path = Path(__file__).resolve().parent.parent / 'data' / 'fleet.json'
    with data_path.open() as f:
        data = json.load(f)
    competitors = data.get('competitors', [])
    return render_template('fleet.html', title='Fleet', breadcrumbs=breadcrumbs, fleet=competitors)


@bp.route('/api/fleet', methods=['POST'])
def update_fleet():
    """Persist fleet edits and refresh handicaps."""
    fleet_path = DATA_DIR / 'fleet.json'
    payload = request.get_json() or {}
    comps = payload.get('competitors', [])
    try:
        with fleet_path.open() as f:
            fleet_data = json.load(f)
    except FileNotFoundError:
        fleet_data = {'competitors': []}
    existing = {
        c.get('competitor_id'): c
        for c in fleet_data.get('competitors', [])
        if c.get('competitor_id')
    }
    ids = set(existing.keys())
    for comp in comps:
        cid = comp.get('competitor_id')
        if not cid:
            cid = _next_competitor_id(ids)
            ids.add(cid)
        entry = existing.get(
            cid,
            {
                'competitor_id': cid,
                'current_handicap_s_per_hr': comp.get('starting_handicap_s_per_hr', 0),
                'active': True,
                'notes': '',
            },
        )
        entry.update(
            {
                'sailor_name': comp.get('sailor_name', ''),
                'boat_name': comp.get('boat_name', ''),
                'sail_no': comp.get('sail_no', ''),
                'starting_handicap_s_per_hr': comp.get('starting_handicap_s_per_hr', 0),
            }
        )
        if 'current_handicap_s_per_hr' not in entry:
            entry['current_handicap_s_per_hr'] = entry['starting_handicap_s_per_hr']
        existing[cid] = entry
    # Ensure sail numbers are unique
    sail_counts: dict[str, int] = {}
    for c in existing.values():
        sail_no = c.get('sail_no', '').strip()
        if not sail_no:
            continue
        sail_counts[sail_no] = sail_counts.get(sail_no, 0) + 1
    duplicates = [sn for sn, count in sail_counts.items() if count > 1]
    if duplicates:
        return {'error': f"Duplicate sail numbers: {', '.join(sorted(duplicates))}"}, 400

    fleet_data['competitors'] = list(existing.values())
    fleet_data['updated_at'] = datetime.utcnow().isoformat() + 'Z'
    with fleet_path.open('w') as f:
        json.dump(fleet_data, f, indent=2)
    recalculate_handicaps()
    return {'status': 'ok'}


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


@bp.route('/api/settings', methods=['POST'])
def save_settings():
    """Persist updated settings to the JSON configuration file."""
    data_path = DATA_DIR / 'settings.json'
    payload = request.get_json() or {}
    # Preserve versioning information and update timestamp
    try:
        with data_path.open() as f:
            existing = json.load(f)
    except FileNotFoundError:
        existing = {"version": 0}

    payload["version"] = int(existing.get("version", 0)) + 1
    payload["updated_at"] = datetime.utcnow().isoformat() + "Z"

    with data_path.open('w') as f:
        json.dump(payload, f, indent=2)

    # Reload scoring settings so future calculations use the new values
    importlib.reload(scoring_module)

    return {"status": "ok"}


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
        recalculate_handicaps()
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
    recalculate_handicaps()

    finisher_count = sum(1 for e in race_data.get('entrants', []) if e.get('finish_time'))
    return {'finisher_count': finisher_count, 'redirect': redirect_url}


@bp.route('/api/races/<race_id>', methods=['DELETE'])
def delete_race(race_id):
    race_path = _race_path(race_id)
    if race_path is None:
        abort(404)
    with race_path.open() as f:
        race_data = json.load(f)
    series_id = race_data.get('series_id')
    race_path.unlink()
    redirect_url = url_for('main.series_detail', series_id=series_id)
    return {'redirect': redirect_url}
