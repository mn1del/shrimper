"""Scoring utilities implementing personal handicap calculations."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

SECONDS_PER_HOUR = 3600

# Data directory lives at the project root under ``data``.
DATA_DIR = Path(__file__).resolve().parent.parent / "data"


def _build_lookup(entries: List[Dict], key_field: str, value_field: str) -> Tuple[Dict[int, float], float]:
    """Build lookup dict and default value from settings entries."""
    lookup: Dict[int, float] = {}
    default = 0.0
    for item in entries:
        key = item[key_field]
        value = item[value_field]
        if isinstance(key, int):
            lookup[int(key)] = value
        elif key == "default_or_higher":
            default = value
    return lookup, default


# Load configuration from settings.json.
with (DATA_DIR / "settings.json").open() as f:
    _SETTINGS = json.load(f)

_HANDICAP_DELTAS, _HANDICAP_DEFAULT = _build_lookup(
    _SETTINGS["handicap_delta_by_rank"], "rank", "delta_s_per_hr"
)
_LEAGUE_POINTS, _POINTS_DEFAULT = _build_lookup(
    _SETTINGS["league_points_by_rank"], "rank", "points"
)
_FLEET_FACTORS, _FLEET_DEFAULT = _build_lookup(
    _SETTINGS["fleet_size_factor"], "finishers", "factor"
)


def _full_delta(position: int) -> int:
    """Return the base handicap delta for the given finishing position."""
    return int(_HANDICAP_DELTAS.get(position, _HANDICAP_DEFAULT))


def _scaling_factor(fleet_size: int) -> float:
    """Return the fleet size scaling factor for the number of finishers."""
    return float(_FLEET_FACTORS.get(fleet_size, _FLEET_DEFAULT))


def _base_points(position: int) -> float:
    """Return the base league points for the given finishing position."""
    return float(_LEAGUE_POINTS.get(position, _POINTS_DEFAULT))


def adjusted_time(start: int, finish: int, handicap: int) -> Dict[str, float]:
    """Compute elapsed and handicap adjusted times.

    Args:
        start: Start time in seconds.
        finish: Finish time in seconds.
        handicap: Personal handicap in seconds per hour.

    Returns:
        Dictionary containing elapsed_seconds, allowance_seconds, and
        adjusted_time_seconds.
    """
    elapsed_seconds = finish - start
    elapsed_hours = elapsed_seconds / SECONDS_PER_HOUR
    allowance_seconds = handicap * elapsed_hours
    handicap_adjusted_time_s = elapsed_seconds - allowance_seconds
    return {
        "elapsed_seconds": elapsed_seconds,
        "allowance_seconds": allowance_seconds,
        "adjusted_time_seconds": handicap_adjusted_time_s,
    }


def calculate_race_results(entries: Iterable[Dict]) -> List[Dict]:
    """Calculate race results using the PHC system.

    Each entry must provide ``start`` and ``finish`` times in seconds and an
    ``initial_handicap`` value.  Non-finishers can be indicated by setting
    ``status`` to ``"DNF"``, ``"DNS"`` or ``"DSQ"`` or by omitting a finish
    time.  The function returns a list of results sorted by adjusted time for
    finishers and appends non-finisher entries afterwards.  Each result includes
    revised handicaps, league points and traditional low-point scores.
    """

    finishers: List[Dict] = []
    non_finishers: List[Dict] = []
    race_start: int | None = None
    for entry in entries:
        status = entry.get("status")
        finish = entry.get("finish")
        if race_start is None:
            race_start = entry.get("start")
        if status in {"DNF", "DNS", "DSQ"} or finish is None:
            # Record the entry with zeroed timing values so downstream
            # consumers can display consistent fields for all boats even when
            # they do not finish.
            times = {
                "elapsed_seconds": 0,
                "allowance_seconds": 0.0,
                "adjusted_time_seconds": 0.0,
            }
            non_finishers.append({**entry, **times, "status": status, "finish": None})
            continue

        times = adjusted_time(entry["start"], finish, entry["initial_handicap"])
        result = {**entry, **times, "status": status}
        finishers.append(result)

    # Rank by adjusted time (lower is better)
    # Determine absolute finishing positions based on raw elapsed time
    finishers.sort(key=lambda r: r["elapsed_seconds"])
    last_elapsed = None
    abs_position = 0
    for idx, result in enumerate(finishers, start=1):
        if last_elapsed is None or result["elapsed_seconds"] > last_elapsed:
            abs_position = idx
            last_elapsed = result["elapsed_seconds"]
        result["absolute_position"] = abs_position

    # Rank by adjusted time (lower is better) for handicap results
    finishers.sort(key=lambda r: r["adjusted_time_seconds"])

    fleet_size = len(finishers)
    factor = _scaling_factor(fleet_size)

    last_adjusted = None
    handicap_position = 0
    for idx, result in enumerate(finishers, start=1):
        if last_adjusted is None or result["adjusted_time_seconds"] > last_adjusted:
            handicap_position = idx
            last_adjusted = result["adjusted_time_seconds"]

        base_delta = _full_delta(handicap_position)
        scaled_delta = base_delta * factor
        actual_delta = int(round(scaled_delta))
        base_points = _base_points(handicap_position)
        race_points = base_points * factor
        result.update(
            {
                "handicap_position": handicap_position,
                "full_delta": base_delta,
                "scaled_delta": scaled_delta,
                "actual_delta": actual_delta,
                "revised_handicap": result["initial_handicap"] + actual_delta,
                "points": race_points,
                # Traditional scoring assigns points equal to finishing position
                "traditional_points": handicap_position,
            }
        )

    # Non-finishers score fleet_size + 1 points in traditional system
    non_finisher_points = fleet_size + 1
    for entry in non_finishers:
        entry.update(
            {
                "handicap_position": None,
                # For non-finishers, handicaps and points do not change
                "full_delta": 0,
                "scaled_delta": 0,
                "actual_delta": 0,
                "revised_handicap": entry.get("initial_handicap"),
                "points": 0.0,
                "traditional_points": non_finisher_points,
            }
        )

    results = finishers + non_finishers
    if (race_start in (0, None)) or not finishers:
        for res in results:
            res["traditional_points"] = 0.0
    return results


def compute_league_standings(races: Iterable[Iterable[Dict]]) -> List[Dict]:
    """Aggregate race points to produce league standings.

    Args:
        races: Iterable of race result iterables as returned by
            :func:`calculate_race_results`.

    Returns:
        List of standings dictionaries sorted by total points (high points wins).
    """
    totals: Dict[str, float] = {}
    names: Dict[str, Dict] = {}
    for race in races:
        for res in race:
            sailor = res.get("sailor")
            points = res.get("points", 0.0)
            totals[sailor] = totals.get(sailor, 0.0) + points
            names[sailor] = {
                "sailor": sailor,
                "boat": res.get("boat"),
                "sail_number": res.get("sail_number"),
            }

    standings = []
    for sailor, total_points in totals.items():
        entry = {**names[sailor], "total_points": total_points}
        standings.append(entry)

    standings.sort(key=lambda r: (-r["total_points"], r["sailor"]))

    for place, entry in enumerate(standings, start=1):
        entry["place"] = place

    return standings


def compute_traditional_standings(races: Iterable[Iterable[Dict]]) -> List[Dict]:
    """Aggregate race points for traditional low-point scoring.

    Args:
        races: Iterable of race result iterables as returned by
            :func:`calculate_race_results`.

    Returns:
        List of standings dictionaries sorted by ascending total points
        (low points wins).
    """

    totals: Dict[str, float] = {}
    names: Dict[str, Dict] = {}
    for race in races:
        finisher_count = sum(1 for r in race if r.get("finish") is not None)
        for res in race:
            sailor = res.get("sailor")
            points = res.get("traditional_points")
            if points is None and res.get("finish") is None:
                points = finisher_count + 1
            elif points is None:
                points = 0.0
            totals[sailor] = totals.get(sailor, 0.0) + points
            names[sailor] = {
                "sailor": sailor,
                "boat": res.get("boat"),
                "sail_number": res.get("sail_number"),
            }

    standings: List[Dict] = []
    for sailor, total_points in totals.items():
        entry = {**names[sailor], "total_points": total_points}
        standings.append(entry)

    standings.sort(key=lambda r: (r["total_points"], r["sailor"]))

    for place, entry in enumerate(standings, start=1):
        entry["place"] = place

    return standings

__all__ = [
    "adjusted_time",
    "calculate_race_results",
    "compute_league_standings",
    "compute_traditional_standings",
]
