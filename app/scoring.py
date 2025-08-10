"""Scoring utilities implementing personal handicap calculations."""

from __future__ import annotations

import json
from pathlib import Path
from typing import List, Dict, Iterable

SECONDS_PER_HOUR = 3600

DATA_DIR = Path(__file__).resolve().parent / "data"

# Load handicap deltas by finishing position (index corresponds to place).
with (DATA_DIR / "handicap_deltas.json").open() as f:
    HANDICAP_DELTAS: List[int] = json.load(f)

# Load fleet size scaling factors (index corresponds to fleet size).
with (DATA_DIR / "fleet_size_scaling.json").open() as f:
    FLEET_SCALING: List[float] = json.load(f)

# League points awarded to the first ten finishers (index corresponds to place).
LEAGUE_BASE_POINTS: List[float] = [0.0, 25.0, 18.0, 12.0, 9.0, 7.0, 4.0, 3.0, 2.0, 1.0, 0.0]

MAX_DELTA_POS = len(HANDICAP_DELTAS) - 1
MAX_SCALING_SIZE = len(FLEET_SCALING) - 1
MAX_POINTS_POS = len(LEAGUE_BASE_POINTS) - 1


def _full_delta(position: int) -> int:
    """Return the base handicap delta for the given finishing position."""
    idx = position if position <= MAX_DELTA_POS else MAX_DELTA_POS
    return HANDICAP_DELTAS[idx]


def _scaling_factor(fleet_size: int) -> float:
    """Return the fleet size scaling factor for the number of finishers."""
    idx = fleet_size if fleet_size <= MAX_SCALING_SIZE else MAX_SCALING_SIZE
    return FLEET_SCALING[idx]


def _base_points(position: int) -> float:
    """Return the base league points for the given finishing position."""
    idx = position if position <= MAX_POINTS_POS else MAX_POINTS_POS
    return LEAGUE_BASE_POINTS[idx]


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
    for entry in entries:
        status = entry.get("status")
        finish = entry.get("finish")
        if status in {"DNF", "DNS", "DSQ"} or finish is None:
            # Record the entry without timing information
            non_finishers.append({**entry, "status": status})
            continue

        times = adjusted_time(entry["start"], finish, entry["initial_handicap"])
        result = {**entry, **times, "status": status}
        finishers.append(result)

    # Rank by adjusted time (lower is better)
    finishers.sort(key=lambda r: r["adjusted_time_seconds"])

    fleet_size = len(finishers)
    factor = _scaling_factor(fleet_size)

    for position, result in enumerate(finishers, start=1):
        base_delta = _full_delta(position)
        scaled_delta = base_delta * factor
        actual_delta = int(round(scaled_delta))
        base_points = _base_points(position)
        race_points = base_points * factor
        result.update(
            {
                "handicap_position": position,
                "full_delta": base_delta,
                "scaled_delta": scaled_delta,
                "actual_delta": actual_delta,
                "revised_handicap": result["initial_handicap"] + actual_delta,
                "points": race_points,
                # Traditional scoring assigns points equal to finishing position
                "traditional_points": position,
            }
        )

    # Non-finishers score fleet_size + 1 points in traditional system
    non_finisher_points = fleet_size + 1
    for entry in non_finishers:
        entry.update(
            {
                "handicap_position": None,
                "full_delta": None,
                "scaled_delta": None,
                "actual_delta": None,
                "revised_handicap": entry.get("initial_handicap"),
                "points": 0.0,
                "traditional_points": non_finisher_points,
            }
        )

    return finishers + non_finishers


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
        for res in race:
            sailor = res.get("sailor")
            points = res.get("traditional_points", 0.0)
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
