import os
from typing import Dict, List

import pytest


def _parse_hms(t: str | None) -> int | None:
    if not t:
        return None
    h, m, s = map(int, t.split(":"))
    return h * 3600 + m * 60 + s


@pytest.mark.skipif(not os.environ.get("DATABASE_URL"), reason="DATABASE_URL not set; skipping DB-based handicap chain test")
def test_handicap_chain_chronological_integrity():
    """Asserts initial_handicap matches expected chronological seed for all races.

    Expected seed per entrant per race:
      - If handicap_override present: equals override
      - Else equals the competitor's current handicap prior to that race
    Chronology starts from fleet starting handicaps and feeds forward revised values.
    """
    # Lazy imports to avoid impacting environments without DB
    from app import datastore_pg as pg
    from app.scoring import calculate_race_results

    data = pg.load_data()

    # Seed handicap map from fleet starting handicaps
    fleet = (data.get("fleet") or {}).get("competitors", [])
    start_map: Dict[str, int] = {
        c.get("competitor_id"): int(c.get("starting_handicap_s_per_hr") or 0)
        for c in fleet
        if c.get("competitor_id")
    }
    handicap_map: Dict[str, int] = dict(start_map)

    # Flatten races and build id -> race mapping
    seasons: List[dict] = data.get("seasons") or []
    races_by_id: Dict[str, dict] = {}
    for season in seasons:
        for series in (season.get("series") or []):
            for race in (series.get("races") or []):
                rid = race.get("race_id")
                if rid:
                    races_by_id[str(rid)] = race

    # Determine chronological order
    try:
        order_ids = pg.get_races() or []
    except Exception:
        # Fallback to sort by (date, start_time)
        def _key(r: dict):
            d = r.get("date") or "9999-12-31"
            t = r.get("start_time") or "23:59:59"
            return (d, t)

        order_ids = [rid for rid, r in sorted(races_by_id.items(), key=lambda it: _key(it[1]))]

    mismatches: List[dict] = []

    for rid in order_ids:
        race = races_by_id.get(rid)
        if not race:
            continue
        start_sec = _parse_hms(race.get("start_time")) or 0
        entrants = race.get("competitors") or []

        calc_entries: List[dict] = []
        for ent in entrants:
            cid = ent.get("competitor_id")
            if not cid:
                continue
            ov = ent.get("handicap_override")
            if ov is not None:
                try:
                    expected = int(ov)
                except Exception:
                    expected = handicap_map.get(cid, 0)
            else:
                expected = handicap_map.get(cid, 0)

            stored = ent.get("initial_handicap")
            try:
                stored_int = None if stored is None else int(stored)
            except Exception:
                stored_int = None
            if stored_int != int(expected):
                mismatches.append(
                    {
                        "race_id": rid,
                        "competitor_id": cid,
                        "stored_initial": stored,
                        "expected_initial": expected,
                        "override": ov,
                    }
                )

            entry = {"competitor_id": cid, "start": start_sec, "initial_handicap": int(expected)}
            ft = _parse_hms(ent.get("finish_time"))
            if ft is not None:
                entry["finish"] = ft
            status = ent.get("status")
            if status:
                entry["status"] = status
            calc_entries.append(entry)

        if calc_entries:
            results = calculate_race_results(calc_entries)
            for res in results:
                cid2 = res.get("competitor_id")
                rev = res.get("revised_handicap")
                if cid2 and rev is not None:
                    handicap_map[cid2] = int(rev)

    assert not mismatches, f"Found {len(mismatches)} initial_handicap mismatches. Examples: {mismatches[:5]}"

