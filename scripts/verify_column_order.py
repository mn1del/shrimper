import os
import sys
from pathlib import Path
from html.parser import HTMLParser

# Ensure DATABASE_URL is present so app.init doesn't abort
os.environ.setdefault("DATABASE_URL", "postgresql://user:pass@localhost/db")

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app import create_app  # type: ignore  # noqa: E402
import app.datastore_pg as pg  # type: ignore  # noqa: E402


def patch_datastore(memory_store):
    # In-memory stand-ins mirroring tests/conftest.py
    def load_data():
        import copy
        return copy.deepcopy(memory_store)

    def save_data(data):
        import copy
        memory_store.clear()
        memory_store.update(copy.deepcopy(data or {}))

    def list_seasons(data=None):
        d = data or memory_store
        return list(d.get("seasons", []))

    def list_series(data=None):
        d = data or memory_store
        out = []
        for season in d.get("seasons", []):
            out.extend(season.get("series", []) or [])
        return out

    def find_series(series_id, data=None):
        d = data or memory_store
        target = (series_id or "").lower()
        for season in d.get("seasons", []):
            for series in season.get("series", []) or []:
                if (series.get("series_id") or "").lower() == target:
                    return season, series
        return None, None

    def find_race(race_id, data=None):
        d = data or memory_store
        for season in d.get("seasons", []):
            for series in season.get("series", []) or []:
                for race in series.get("races", []) or []:
                    if race.get("race_id") == race_id:
                        return season, series, race
        return None, None, None

    def list_all_races(data=None):
        d = data or memory_store
        out = []
        for season in d.get("seasons", []):
            for series in season.get("series", []) or []:
                for race in series.get("races", []) or []:
                    out.append({
                        "race_id": race.get("race_id"),
                        "date": race.get("date"),
                        "start_time": race.get("start_time"),
                        "series_name": series.get("name"),
                        "series_id": series.get("series_id"),
                        "finishers": sum(1 for e in race.get("competitors", []) if e.get("finish_time")),
                        "season": season.get("year"),
                    })
        out.sort(key=lambda r: (r.get("date") or "", r.get("start_time") or ""), reverse=True)
        return out

    def list_season_races_with_results(season_year, data=None):
        d = data or memory_store
        for s in d.get("seasons", []):
            if int(s.get("year", 0)) == int(season_year):
                return s
        return {"year": int(season_year), "series": []}

    def get_fleet(data=None):
        d = data or memory_store
        return d.get("fleet", {"competitors": []})

    def set_fleet(fleet, data=None):
        d = data or memory_store
        d["fleet"] = fleet or {"competitors": []}
        return d

    def get_settings(data=None):
        d = data or memory_store
        return d.get("settings", {})

    def set_settings(settings, data=None):
        d = data or memory_store
        d["settings"] = settings or {}
        return d

    # Patch functions on pg module
    for name, fn in [
        ("load_data", load_data),
        ("save_data", save_data),
        ("list_seasons", list_seasons),
        ("list_series", list_series),
        ("find_series", find_series),
        ("find_race", find_race),
        ("list_all_races", list_all_races),
        ("list_season_races_with_results", list_season_races_with_results),
        ("get_fleet", get_fleet),
        ("set_fleet", set_fleet),
        ("get_settings", get_settings),
        ("set_settings", set_settings),
    ]:
        setattr(pg, name, fn)


def build_memory_store():
    return {
        "settings": {
            "version": 1,
            "handicap_delta_by_rank": [
                {"rank": 1, "delta_s_per_hr": -30},
                {"rank": 2, "delta_s_per_hr": -20},
                {"rank": 3, "delta_s_per_hr": -10},
                {"rank": 4, "delta_s_per_hr": 0},
                {"rank": "default_or_higher", "delta_s_per_hr": 60},
            ],
            "league_points_by_rank": [
                {"rank": 1, "points": 25},
                {"rank": 2, "points": 18},
                {"rank": 3, "points": 12},
                {"rank": "default_or_higher", "points": 3},
            ],
            "fleet_size_factor": [
                {"finishers": 1, "factor": 0.0},
                {"finishers": 3, "factor": 0.0},
                {"finishers": 7, "factor": 1.0},
                {"finishers": "default_or_higher", "factor": 1.0},
            ],
        },
        "fleet": {
            "competitors": [
                {
                    "competitor_id": 1,
                    "sailor_name": "Alice",
                    "boat_name": "Boaty",
                    "sail_no": "1",
                    "starting_handicap_s_per_hr": 100,
                    "current_handicap_s_per_hr": 100,
                },
                {
                    "competitor_id": 2,
                    "sailor_name": "Bob",
                    "boat_name": "Crafty",
                    "sail_no": "2",
                    "starting_handicap_s_per_hr": 100,
                    "current_handicap_s_per_hr": 100,
                },
            ]
        },
        "seasons": [
            {
                "year": 2025,
                "series": [
                    {
                        "series_id": "SER_2025_Test",
                        "name": "Test",
                        "season": 2025,
                        "races": [
                            {
                                "race_id": "RACE_2025-01-01_Test_1",
                                "series_id": "SER_2025_Test",
                                "name": "SER_2025_Test_1",
                                "date": "2025-01-01",
                                "start_time": "00:00:00",
                                "competitors": [
                                    {"competitor_id": 1, "finish_time": "00:30:00"},
                                    {"competitor_id": 2, "finish_time": "00:31:00"},
                                ],
                                "race_no": 1,
                            }
                        ],
                    }
                ],
            }
        ],
    }


def main():
    mem = build_memory_store()
    patch_datastore(mem)
    app = create_app()
    app.config.update({"TESTING": True})
    with app.test_client() as c:
        res = c.get("/series/SER_2025_Test?race_id=RACE_2025-01-01_Test_1")
        html = res.get_data(as_text=True)
    class TableGrabber(HTMLParser):
        def __init__(self):
            super().__init__()
            self.in_table = False
            self.in_thead = False
            self.in_tbody = False
            self.in_th = False
            self.in_td = False
            self.headers = []
            self.first_row = []
            self.current_row = []
            self.has_first = False
        def handle_starttag(self, tag, attrs):
            if tag == 'table':
                for k, v in attrs:
                    if k == 'class' and v and 'race-table-wrapper' in html:
                        # crude: we'll assume first table is our table
                        self.in_table = True
                if not self.in_table:
                    self.in_table = True  # fallback
            elif self.in_table and tag == 'thead':
                self.in_thead = True
            elif self.in_table and tag == 'tbody':
                self.in_tbody = True
            elif self.in_thead and tag == 'th':
                self.in_th = True
                self._buf = []
            elif self.in_tbody and not self.has_first and tag == 'td':
                self.in_td = True
                self._buf = []
        def handle_endtag(self, tag):
            if tag == 'table' and self.in_table:
                self.in_table = False
            elif tag == 'thead' and self.in_thead:
                self.in_thead = False
            elif tag == 'tbody' and self.in_tbody:
                self.in_tbody = False
            elif tag == 'th' and self.in_th:
                text = ''.join(self._buf).strip()
                self.headers.append(text)
                self.in_th = False
            elif tag == 'td' and self.in_td:
                text = ''.join(self._buf).strip()
                self.current_row.append(text)
                self.in_td = False
            elif tag == 'tr' and self.in_tbody and not self.has_first and self.current_row:
                self.first_row = self.current_row
                self.current_row = []
                self.has_first = True
        def handle_data(self, data):
            if (self.in_th or self.in_td) and hasattr(self, '_buf'):
                self._buf.append(data)
    parser = TableGrabber()
    parser.feed(html)
    headers = parser.headers
    first_row = parser.first_row
    print("Headers:", " | ".join(headers))
    print("First row:", " | ".join(first_row))
    # Simple assertion output
    try:
        fi = headers.index("Finish Time (hh:mm:ss)")
        ai = headers.index("Abs Pos")
        assert ai == fi + 1
        print("OK: Abs Pos immediately follows Finish Time")
    except Exception as e:
        print("Order check failed:", e)


if __name__ == "__main__":
    main()
