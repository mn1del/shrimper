/* Client-side scoring utilities mirroring server logic (app/scoring.py).
 *
 * Exposes a Scorer with identical formulas and rounding behavior.
 * - Ties by elapsed and adjusted time use shared places (first of tie).
 * - actual_delta uses Python's round-half-to-even via roundHalfToEven.
 * - Traditional points = handicap position for finishers; non-finishers get
 *   fleet_size + 1; if start==0 or no finishers → 0 for all traditional.
 */
(function (global) {
  'use strict';

  const SECONDS_PER_HOUR = 3600;

  function isNumber(x) {
    return typeof x === 'number' && isFinite(x);
  }

  // Python 3 round-half-to-even parity
  function roundHalfToEven(x) {
    if (!isNumber(x)) return Math.round(x);
    const floor = Math.floor(x);
    const diff = x - floor;
    if (diff < 0.5) return floor;
    if (diff > 0.5) return Math.ceil(x);
    // exactly .5
    // For negatives, Math.floor(-1.5) = -2 (even), diff=0.5 → return -2
    return (floor % 2 === 0) ? floor : floor + 1;
  }

  function hmsToSeconds(hms) {
    if (hms == null || hms === '') return null;
    const parts = String(hms).split(':');
    if (parts.length !== 3) throw new Error('Invalid HH:MM:SS');
    const [h, m, s] = parts.map(v => {
      const n = parseInt(v, 10);
      if (!isFinite(n)) throw new Error('Invalid HH:MM:SS');
      return n;
    });
    return h * 3600 + m * 60 + s;
  }

  function secondsToHMS(seconds) {
    if (seconds == null) return null;
    const total = Math.round(Number(seconds));
    const h = Math.floor(total / 3600).toString().padStart(2, '0');
    const m = Math.floor((total % 3600) / 60).toString().padStart(2, '0');
    const s = Math.floor(total % 60).toString().padStart(2, '0');
    return `${h}:${m}:${s}`;
  }

  function buildLookup(entries, keyField, valueField) {
    const lookup = new Map();
    let def = 0.0;
    (entries || []).forEach(item => {
      const key = item && item[keyField];
      const val = item && item[valueField];
      if (Number.isInteger(key)) {
        lookup.set(key, Number(val));
      } else if (key === 'default_or_higher') {
        def = Number(val);
      }
    });
    return { lookup, def };
  }

  class Scorer {
    constructor(settings) {
      const s = settings || {};
      const d = buildLookup(s.handicap_delta_by_rank || [], 'rank', 'delta_s_per_hr');
      const p = buildLookup(s.league_points_by_rank || [], 'rank', 'points');
      const f = buildLookup(s.fleet_size_factor || [], 'finishers', 'factor');
      this._deltaLookup = d.lookup; this._deltaDefault = Number(d.def || 0);
      this._pointsLookup = p.lookup; this._pointsDefault = Number(p.def || 0);
      this._fleetLookup = f.lookup; this._fleetDefault = Number(f.def || 0);
    }

    fullDelta(position) {
      const v = this._deltaLookup.get(Number(position));
      return Number.isFinite(v) ? Number(v) : this._deltaDefault;
    }

    basePoints(position) {
      const v = this._pointsLookup.get(Number(position));
      return Number.isFinite(v) ? Number(v) : this._pointsDefault;
    }

    scalingFactor(finishers) {
      const v = this._fleetLookup.get(Number(finishers));
      return Number.isFinite(v) ? Number(v) : this._fleetDefault;
    }

    adjustedTime(start, finish, handicap) {
      const elapsedSeconds = Number(finish) - Number(start);
      const elapsedHours = elapsedSeconds / SECONDS_PER_HOUR;
      const allowanceSeconds = Number(handicap) * elapsedHours;
      const adjustedSeconds = elapsedSeconds - allowanceSeconds;
      return { elapsed_seconds: elapsedSeconds, allowance_seconds: allowanceSeconds, adjusted_time_seconds: adjustedSeconds };
    }

    // entries: [{competitor_id, start, finish?, status?, initial_handicap}]
    calculateRaceResults(entries) {
      const finishers = [];
      const nonFinishers = [];
      let raceStart = null;
      for (const entry of (entries || [])) {
        const status = entry.status;
        const finish = Object.prototype.hasOwnProperty.call(entry, 'finish') ? entry.finish : undefined;
        if (raceStart === null) raceStart = Number(entry.start);
        if (status === 'DNF' || status === 'DNS' || status === 'DSQ' || finish == null) {
          nonFinishers.push({ ...entry, elapsed_seconds: 0, allowance_seconds: 0.0, adjusted_time_seconds: 0.0, status, finish: null });
          continue;
        }
        const times = this.adjustedTime(entry.start, finish, entry.initial_handicap);
        finishers.push({ ...entry, ...times, status });
      }

      // Absolute positions by elapsed time
      finishers.sort((a, b) => a.elapsed_seconds - b.elapsed_seconds);
      let lastElapsed = null;
      let absPos = 0;
      finishers.forEach((r, idx) => {
        if (lastElapsed === null || r.elapsed_seconds > lastElapsed) {
          absPos = idx + 1;
          lastElapsed = r.elapsed_seconds;
        }
        r.absolute_position = absPos;
      });

      // Handicap positions by adjusted time
      finishers.sort((a, b) => a.adjusted_time_seconds - b.adjusted_time_seconds);
      const fleetSize = finishers.length;
      const factor = this.scalingFactor(fleetSize);
      let lastAdj = null;
      let hcpPos = 0;
      finishers.forEach((r, idx) => {
        if (lastAdj === null || r.adjusted_time_seconds > lastAdj) {
          hcpPos = idx + 1;
          lastAdj = r.adjusted_time_seconds;
        }
        const baseDelta = this.fullDelta(hcpPos);
        const scaledDelta = baseDelta * factor;
        const actualDelta = roundHalfToEven(scaledDelta);
        const basePts = this.basePoints(hcpPos);
        const racePts = basePts * factor;
        r.handicap_position = hcpPos;
        r.full_delta = baseDelta;
        r.scaled_delta = scaledDelta;
        r.actual_delta = actualDelta;
        r.revised_handicap = Number(r.initial_handicap) + Number(actualDelta);
        r.points = racePts;
        r.traditional_points = hcpPos;
      });

      // Non finishers
      const nfPoints = fleetSize + 1;
      nonFinishers.forEach(r => {
        r.handicap_position = null;
        r.full_delta = 0; r.scaled_delta = 0; r.actual_delta = 0;
        r.revised_handicap = r.initial_handicap;
        r.points = 0.0;
        r.traditional_points = nfPoints;
      });

      const results = finishers.concat(nonFinishers);
      if (raceStart === 0 || raceStart === null || finishers.length === 0) {
        results.forEach(r => { r.traditional_points = 0.0; });
      }
      return results;
    }
  }

  // Export API
  global.Scorer = {
    Scorer,
    hmsToSeconds,
    secondsToHMS,
    roundHalfToEven,
  };

})(window);

