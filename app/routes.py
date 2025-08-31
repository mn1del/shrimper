from flask import Blueprint, redirect, render_template, url_for, abort, request
import json
import importlib
from datetime import datetime
from pathlib import Path

from .scoring import calculate_race_results, _scaling_factor
from . import scoring as scoring_module
from .datastore import (
    load_data,
    save_data,
    list_all_races as ds_list_all_races,
    list_seasons as ds_list_seasons,
    find_series as ds_find_series,
    find_race as ds_find_race,
    ensure_series as ds_ensure_series,
    renumber_races as ds_renumber_races,
    get_fleet as ds_get_fleet,
    set_fleet as ds_set_fleet,
    get_settings as ds_get_settings,
    set_settings as ds_set_settings,
)


bp = Blueprint('main', __name__)


#<getdata>
def _series_meta_paths():
    """Deprecated: file-based data no longer used (single data.json)."""
    return []
#</getdata>


#<getdata>
def _load_series_entries():
    """Return list of series (metadata + races) from data.json."""
    data = load_data()
    entries = []
    for season in data.get("seasons", []):
        for series in season.get("series", []):
            meta = {"series_id": series.get("series_id"), "name": series.get("name"), "season": series.get("season")}
            entries.append({"series": meta, "races": list(series.get("races", []))})
    return entries
#</getdata>


#<getdata>
def _load_all_races():
    """Return a flat list of all races with series info from data.json."""
    return ds_list_all_races()
#</getdata>


#<getdata>
def _find_series(series_id: str):
    """Return (series_meta, races) for the given series id or (None, None)."""
    _season, series = ds_find_series(series_id)
    if not series:
        return None, None
    meta = {"series_id": series.get("series_id"), "name": series.get("name"), "season": series.get("season")}
    return meta, list(series.get("races", []))
#</getdata>


#<getdata>
def _find_race(race_id: str):
    """Return race data for the given race id or None if not found."""
    _season, _series, race = ds_find_race(race_id)
    return race
#</getdata>


#<getdata>
def _race_path(race_id: str):
    """Deprecated: now stored inside data.json; kept for compatibility."""
    return None
#</getdata>


def _parse_hms(t: str | None) -> int | None:
    """Return seconds for an ``HH:MM:SS`` timestamp or ``None``."""
    if not t:
        return None
    h, m, s = map(int, t.split(":"))
    return h * 3600 + m * 60 + s


#<getdata>
def _fleet_lookup() -> dict[str, dict]:
    """Return mapping of competitor id to fleet details from data.json."""
    fleet = ds_get_fleet()
    competitors = fleet.get("competitors", [])
    return {c.get("competitor_id"): c for c in competitors if c.get("competitor_id")}
#</getdata>


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


#<getdata>
def recalculate_handicaps() -> None:
    """Recompute starting handicaps for all races from revised results.

    The fleet register provides the baseline starting handicaps. Each race is
    processed in chronological order and the entrants' ``initial_handicap``
    values are replaced with the current handicap prior to that race. Revised
    handicaps produced from the race are then fed forward to subsequent races
    and ultimately written back to the fleet register.
    """
    data = load_data()
    fleet_data = data.get("fleet", {"competitors": []})
    competitors = fleet_data.get("competitors", [])
    handicap_map = {
        c.get("competitor_id"): c.get("starting_handicap_s_per_hr", 0)
        for c in competitors
        if c.get("competitor_id")
    }

    # Build list of (race_obj) across all seasons/series
    race_list: list[dict] = []
    for season in data.get("seasons", []):
        for series in season.get("series", []):
            for race in series.get("races", []):
                race_list.append(race)

    race_list.sort(key=lambda r: (r.get("date"), r.get("start_time")))

    for race in race_list:
        start_seconds = _parse_hms(race.get("start_time")) or 0
        calc_entries: list[dict] = []
        for ent in race.get("competitors", []):
            cid = ent.get("competitor_id")
            if not cid:
                continue
            override = ent.get("handicap_override")
            if override is not None:
                initial = int(override)
                handicap_map[cid] = initial
            else:
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

    for comp in competitors:
        cid = comp.get("competitor_id")
        if cid:
            comp["current_handicap_s_per_hr"] = handicap_map.get(
                cid, comp.get("current_handicap_s_per_hr", 0)
            )

    data["fleet"] = fleet_data
    save_data(data)
#</getdata>


#<getdata>
def _season_standings(season: int, scoring: str) -> tuple[list[dict], list[dict]]:
    """Compute standings and per-race metadata for a season."""
    fleet = _fleet_lookup()
    race_groups: list[dict] = []

    data = load_data()
    for season_obj in data.get("seasons", []):
        if int(season_obj.get("year", 0)) != int(season):
            continue
        for series in season_obj.get("series", []):
            group = {
                "series_name": series.get("name"),
                "series_id": series.get("series_id"),
                "races": [],
            }
            for race in series.get("races", []):
                start_seconds = _parse_hms(race.get("start_time")) or 0
                entrants_map = {
                ent.get("competitor_id"): ent
                    for ent in race.get("competitors", [])
                    if ent.get("competitor_id")
                }
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
                        "race_finished": {},
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
                agg["race_finished"][race["race_id"]] = finished

    standings: list[dict] = []
    for agg in aggregates.values():
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
                    "race_finished": agg["race_finished"],
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
                    "race_finished": agg["race_finished"],
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
#</getdata>


@bp.route('/')
def index():
    return redirect(url_for('main.races'))


@bp.route('/races')
def races():
    season = request.args.get('season') or None
    #<getdata>
    all_races = _load_all_races()
    #</getdata>
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


#<getdata>
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
#</getdata>


#<getdata>
def _renumber_races(series_dir: Path) -> dict[str, str]:
    """Renumber races in ``series_dir`` based on date and start time.

    Returns mapping of old race_id to new race_id for the series.
    """
    races_dir = series_dir / "races"
    if not races_dir.exists():
        return {}

    meta_path = series_dir / "series_metadata.json"
    try:
        with meta_path.open() as f:
            meta = json.load(f)
    except FileNotFoundError:
        return {}

    series_name = meta.get("name")
    series_id = meta.get("series_id")

    race_entries: list[tuple[dict, Path]] = []
    for path in races_dir.glob("RACE_*.json"):
        with path.open() as rf:
            race_entries.append((json.load(rf), path))

    race_entries.sort(key=lambda r: (r[0].get("date") or "", r[0].get("start_time") or ""))

    temp_entries: list[tuple[dict, Path]] = []
    for data, path in race_entries:
        tmp = path.with_name(path.name + ".__tmp__")
        path.rename(tmp)
        temp_entries.append((data, tmp))

    mapping: dict[str, str] = {}
    for idx, (data, tmp) in enumerate(temp_entries, start=1):
        old_id = data.get("race_id")
        date = data.get("date", "")
        new_id = f"RACE_{date}_{series_name}_{idx}"
        data["race_id"] = new_id
        data["race_no"] = idx
        if series_id:
            data["name"] = f"{series_id}_{idx}"
        new_path = races_dir / f"{new_id}.json"
        with tmp.open("w") as f:
            json.dump(data, f, indent=2)
        tmp.rename(new_path)
        if old_id:
            mapping[old_id] = new_id

    return mapping
#</getdata>


#<getdata>
@bp.route('/races/new')
def race_new():
    series_list = [entry['series'] for entry in _load_series_entries()]
    fleet = ds_get_fleet().get('competitors', [])
    blank_race = {
        'race_id': '__new__',
        'series_id': '',
        'date': '',
        'start_time': '',
        'competitors': [],
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
#</getdata>


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
        #<getdata>
        # Load baseline handicaps from fleet register
        fleet = ds_get_fleet().get('competitors', [])
        handicap_map = {
            comp.get('competitor_id'): comp.get('starting_handicap_s_per_hr', 0)
            for comp in fleet
            if comp.get('competitor_id')
        }

        # Load all races from data.json and process them chronologically until target race
        data = load_data()
        race_objs: list[dict] = []
        for season in data.get('seasons', []):
            for s in season.get('series', []):
                for r in s.get('races', []):
                    race_objs.append(r)
        race_objs.sort(key=lambda r: (r.get('date'), r.get('start_time')))

        pre_race_handicaps = handicap_map
        results: dict[str, dict] = {}

        for race in race_objs:
            start_seconds = _parse_hms(race.get('start_time'))
            entrants = race.get('competitors', [])
            entrants_map = {
                e.get('competitor_id'): e for e in entrants if e.get('competitor_id')
            }
            snapshot = handicap_map.copy()

            if race.get('race_id') == race_id:
                # Build entries for full fleet using handicaps prior to this race
                calc_entries: list[dict] = []
                # Prefer building from the race entrants so we don't depend on
                # fleet competitor_id presence to show finish times/results.
                if entrants:
                    for entrant in entrants:
                        cid = entrant.get('competitor_id')
                        if not cid:
                            continue
                        initial = snapshot.get(cid)
                        if initial is None:
                            # Fall back to any initial handicap on the entrant
                            # (useful when fleet register lacks competitor IDs)
                            initial = entrant.get('initial_handicap', 0)
                        # Apply any per-race override
                        if entrant.get('handicap_override') is not None:
                            try:
                                initial = int(entrant['handicap_override'])
                                snapshot[cid] = initial
                            except (ValueError, TypeError):
                                pass
                        entry = {
                            'competitor_id': cid,
                            'start': start_seconds or 0,
                            'initial_handicap': initial,
                        }
                        ft = _parse_hms(entrant.get('finish_time'))
                        if ft is not None:
                            entry['finish'] = ft
                        status = entrant.get('status')
                        if status:
                            entry['status'] = status
                        calc_entries.append(entry)
                else:
                    # Fallback: no entrants list present, use fleet (legacy)
                    for comp in fleet:
                        cid = comp.get('competitor_id')
                        if not cid:
                            continue
                        entrant = entrants_map.get(cid)
                        initial = snapshot.get(cid, 0)
                        if entrant and entrant.get('handicap_override') is not None:
                            try:
                                initial = int(entrant['handicap_override'])
                                snapshot[cid] = initial
                            except (ValueError, TypeError):
                                pass
                        entry = {
                            'competitor_id': cid,
                            'start': start_seconds or 0,
                            'initial_handicap': initial,
                        }
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
                        'handicap_override': entrant.get('handicap_override'),
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
                initial = snapshot.get(cid, 0)
                if entrant.get('handicap_override') is not None:
                    try:
                        initial = int(entrant['handicap_override'])
                        snapshot[cid] = initial
                    except (ValueError, TypeError):
                        pass
                entry = {
                    'competitor_id': cid,
                    'start': start_seconds or 0,
                    'initial_handicap': initial,
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

        # For the race view we want to show the boats that actually
        # participated. Build a display list from the race's entrants and enrich
        # with any available fleet details. This also ensures finish times are
        # shown even when the fleet register lacks competitor IDs.
        entrants_for_display = []
        # Map competitor_id -> fleet entry (when available)
        fleet_by_id = {c.get('competitor_id'): c for c in fleet if c.get('competitor_id')}
        if selected_race:
            for ent in selected_race.get('competitors', []) or []:
                cid = ent.get('competitor_id')
                if not cid:
                    continue
                f = fleet_by_id.get(cid, {})
                entrants_for_display.append({
                    'competitor_id': cid,
                    'sailor_name': f.get('sailor_name', ''),
                    'boat_name': f.get('boat_name', ''),
                    'sail_no': f.get('sail_no', ''),
                    'current_handicap_s_per_hr': pre_race_handicaps.get(cid, ent.get('initial_handicap', 0)),
                })
            # Replace the fleet list used by the template with entrants
            fleet = entrants_for_display

        if selected_race:
            # Primary path: full results computed above
            selected_race['results'] = results
            # Fallback: if for any reason results is empty, at least surface finish times
            if not selected_race['results']:
                basic: dict[str, dict] = {}
                for ent in selected_race.get('competitors', []) or []:
                    cid = ent.get('competitor_id')
                    if not cid:
                        continue
                    basic[cid] = {
                        'finish_time': ent.get('finish_time')
                    }
                selected_race['results'] = basic
        #</getdata>

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


#<getdata>
@bp.route('/standings')
def standings():
    scoring = request.args.get('format', 'league').lower()
    season_param = request.args.get('season')
    data = load_data()
    seasons = sorted({int(season.get('year')) for season in data.get('seasons', [])}, reverse=True)
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
#</getdata>


#<getdata>
@bp.route('/fleet')
def fleet():
    breadcrumbs = [('Fleet', None)]
    competitors = ds_get_fleet().get('competitors', [])
    return render_template('fleet.html', title='Fleet', breadcrumbs=breadcrumbs, fleet=competitors)
#</getdata>


#<getdata>
@bp.route('/api/fleet', methods=['POST'])
def update_fleet():
    """Persist fleet edits and refresh handicaps."""
    payload = request.get_json() or {}
    comps = payload.get('competitors', [])
    data = load_data()
    fleet_data = data.get('fleet', {'competitors': []})
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
    data['fleet'] = fleet_data
    save_data(data)
    recalculate_handicaps()
    return {'status': 'ok'}
#</getdata>


@bp.route('/rules')
def rules():
    breadcrumbs = [('Rules', None)]
    return render_template('rules.html', title='Rules', breadcrumbs=breadcrumbs)


#<getdata>
@bp.route('/settings')
def settings():
    breadcrumbs = [('Settings', None)]
    settings_data = ds_get_settings()
    return render_template('settings.html', title='Settings', breadcrumbs=breadcrumbs, settings=settings_data)
#</getdata>


#<getdata>
@bp.route('/api/settings', methods=['POST'])
def save_settings():
    """Persist updated settings to the JSON configuration file."""
    payload = request.get_json() or {}
    # Preserve versioning information and update timestamp
    existing = ds_get_settings() or {"version": 0}

    payload["version"] = int(existing.get("version", 0)) + 1
    payload["updated_at"] = datetime.utcnow().isoformat() + "Z"

    data = load_data()
    data['settings'] = payload
    save_data(data)

    # Reload scoring settings so future calculations use the new values
    importlib.reload(scoring_module)

    return {"status": "ok"}
#</getdata>


#<getdata>
@bp.route('/api/races/<race_id>', methods=['POST'])
def update_race(race_id):
    data = request.get_json() or {}
    series_choice = data.get('series_id')
    new_series_name = data.get('new_series_name')
    race_date = data.get('date')
    start_time = data.get('start_time')
    finish_times = data.get('finish_times', [])
    handicap_overrides = data.get('handicap_overrides', [])

    store = load_data()

    def _apply_overrides(entrants_list: list[dict]):
        if not handicap_overrides:
            return entrants_list
        ov_map = {o['competitor_id']: o.get('handicap') for o in handicap_overrides}
        for ent in entrants_list:
            cid = ent.get('competitor_id')
            if cid in ov_map:
                val = ov_map[cid]
                if val in (None, ''):
                    ent.pop('handicap_override', None)
                else:
                    try:
                        ent['handicap_override'] = int(val)
                    except (ValueError, TypeError):
                        ent.pop('handicap_override', None)
        return entrants_list

    if race_id == '__new__':
        if series_choice is None or not race_date:
            abort(400)
        start_time = start_time or ''
        timestamp = datetime.utcnow().isoformat() + 'Z'
        try:
            season_year = int(datetime.strptime(race_date, '%Y-%m-%d').year)
        except ValueError:
            abort(400)
        if series_choice == '__new__':
            if not new_series_name:
                abort(400)
            store, season_obj, series_obj = ds_ensure_series(season_year, new_series_name, data=store)
        else:
            _season_obj, series_obj = ds_find_series(series_choice, data=store)
            if not series_obj:
                abort(400)
        series_id_val = series_obj.get('series_id')
        # Build competitors from finish_times
        competitors: list[dict] = []
        for ft in finish_times:
            ent = {'competitor_id': ft['competitor_id'], 'finish_time': ft.get('finish_time')}
            competitors.append(ent)
        competitors = _apply_overrides(competitors)
        # Append new race, then renumber to assign id and sequence
        series_obj.setdefault('races', []).append({
            'race_id': '',
            'series_id': series_id_val,
            'name': '',
            'date': race_date,
            'start_time': start_time,
            'status': 'draft',
            'created_at': timestamp,
            'updated_at': timestamp,
            'competitors': competitors,
            'results': {},
            'race_no': 0,
        })
        mapping = ds_renumber_races(series_obj)
        # The last race is the one we added
        new_race = series_obj['races'][-1]
        new_race_id = new_race.get('race_id')
        # Persist
        save_data(store)
        recalculate_handicaps()
        finisher_count = sum(1 for ft in finish_times if ft.get('finish_time'))
        redirect_url = url_for('main.series_detail', series_id=series_id_val, race_id=new_race_id)
        return {'finisher_count': finisher_count, 'redirect': redirect_url}

    # Editing an existing race
    season_obj, series_obj, race_obj = ds_find_race(race_id, data=store)
    if not race_obj or not series_obj:
        abort(404)

    current_series_id = series_obj.get('series_id')
    redirect_url = None
    target_series = series_obj

    if series_choice:
        if series_choice == '__new__':
            if not new_series_name:
                abort(400)
            date_str = race_date or race_obj.get('date')
            if not date_str:
                abort(400)
            try:
                season_year = int(datetime.strptime(date_str, '%Y-%m-%d').year)
            except ValueError:
                abort(400)
            store, _season_new, target_series = ds_ensure_series(season_year, new_series_name, data=store)
        else:
            _s, ts = ds_find_series(series_choice, data=store)
            if not ts:
                abort(400)
            target_series = ts

        if target_series.get('series_id') != current_series_id:
            # Move race to target series
            series_obj['races'].remove(race_obj)
            race_obj['series_id'] = target_series.get('series_id')
            target_series.setdefault('races', []).append(race_obj)

    # Apply field edits
    if race_date is not None:
        race_obj['date'] = race_date
    if start_time is not None:
        race_obj['start_time'] = start_time
    if finish_times:
        ft_map = {ft['competitor_id']: ft.get('finish_time') for ft in finish_times}
        for entrant in race_obj.get('competitors', []):
            cid = entrant.get('competitor_id')
            if cid in ft_map:
                entrant['finish_time'] = ft_map[cid]
    _apply_overrides(race_obj.get('competitors', []))

    race_obj['updated_at'] = datetime.utcnow().isoformat() + 'Z'

    # Renumber races in affected series (and original if moved)
    mapping_target = ds_renumber_races(target_series)
    if target_series is not series_obj:
        ds_renumber_races(series_obj)

    # Persist and recalc
    save_data(store)
    recalculate_handicaps()

    # Determine final race id after any renumber
    final_race_id = mapping_target.get(race_id, race_obj.get('race_id'))
    redirect_series_id = target_series.get('series_id')
    redirect_url = url_for('main.series_detail', series_id=redirect_series_id, race_id=final_race_id)
    finisher_count = sum(1 for e in race_obj.get('competitors', []) if e.get('finish_time'))
    return {'finisher_count': finisher_count, 'redirect': redirect_url}
#</getdata>


#<getdata>
@bp.route('/api/races/<race_id>', methods=['DELETE'])
def delete_race(race_id):
    store = load_data()
    season_obj, series_obj, race_obj = ds_find_race(race_id, data=store)
    if not race_obj or not series_obj:
        abort(404)
    series_id = series_obj.get('series_id')
    series_obj['races'].remove(race_obj)
    ds_renumber_races(series_obj)
    save_data(store)
    redirect_url = url_for('main.series_detail', series_id=series_id)
    return {'redirect': redirect_url}
#</getdata>
