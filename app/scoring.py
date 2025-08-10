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

MAX_DELTA_POS = len(HANDICAP_DELTAS) - 1
MAX_SCALING_SIZE = len(FLEET_SCALING) - 1


def _full_delta(position: int) -> int:
    """Return the base handicap delta for the given finishing position."""
    idx = position if position <= MAX_DELTA_POS else MAX_DELTA_POS
    return HANDICAP_DELTAS[idx]


def _scaling_factor(fleet_size: int) -> float:
    """Return the fleet size scaling factor for the number of finishers."""
    idx = fleet_size if fleet_size <= MAX_SCALING_SIZE else MAX_SCALING_SIZE
    return FLEET_SCALING[idx]


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
    ``initial_handicap`` value. The function returns a list of results sorted by
    adjusted time, including revised handicaps and handicap positions.
    """
    results: List[Dict] = []
    for entry in entries:
        times = adjusted_time(entry["start"], entry["finish"], entry["initial_handicap"])
        result = {**entry, **times}
        results.append(result)

    # Rank by adjusted time (lower is better)
    results.sort(key=lambda r: r["adjusted_time_seconds"])

    fleet_size = len(results)
    factor = _scaling_factor(fleet_size)

    for position, result in enumerate(results, start=1):
        base_delta = _full_delta(position)
        scaled_delta = base_delta * factor
        actual_delta = int(round(scaled_delta))
        result.update(
            {
                "handicap_position": position,
                "full_delta": base_delta,
                "scaled_delta": scaled_delta,
                "actual_delta": actual_delta,
                "revised_handicap": result["initial_handicap"] + actual_delta,
            }
        )

    return results


def compute_league_standings(races: Iterable[Iterable[Dict]]) -> List[Dict]:
    """Aggregate race points to produce league standings.

    Args:
        races: Iterable of race result iterables as returned by
            :func:`calculate_race_results`.

    Returns:
        List of standings dictionaries sorted by total points (low points wins).
    """
    totals: Dict[str, int] = {}
    names: Dict[str, Dict] = {}
    for race in races:
        for res in race:
            sailor = res.get("sailor")
            points = res.get("handicap_position")
            totals[sailor] = totals.get(sailor, 0) + points
            names[sailor] = {
                "sailor": sailor,
                "boat": res.get("boat"),
                "sail_number": res.get("sail_number"),
            }

    standings = []
    for sailor, total_points in totals.items():
        entry = {**names[sailor], "total_points": total_points}
        standings.append(entry)

    standings.sort(key=lambda r: (r["total_points"], r["sailor"]))

    for place, entry in enumerate(standings, start=1):
        entry["place"] = place

    return standings

__all__ = ["adjusted_time", "calculate_race_results", "compute_league_standings"]
