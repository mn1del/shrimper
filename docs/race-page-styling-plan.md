Race Page Styling Audit and Plan

Scope: app/templates/series_detail.html (race sheet view)

Audit Summary
- Template: app/templates/series_detail.html
- Table wrapper: `.race-table-wrapper` with sticky headers and inline `<style>` block.
- Existing header style: `.race-table-wrapper thead th { background-color: #343a40; color: #fff; font-family: var(--bs-body-font-family); }`
- Existing override: `thead th:nth-child(n+10)` changes headers to light background and dark text; `tbody td:nth-child(n+10)` inverts body cells to dark background. This starts at the Finish Time column (index 10) and currently affects all columns from Finish Time onwards.

Column Order and Classes (current)
1. Hcp Pos — `<td class="hcp-pos">`
2. Elapsed Time — `<td class="elapsed-time">`
3. Adj Time — `<td class="adj-time">`
4. Sailor — plain `<td>`
5. Boat — plain `<td>`
6. Sail No. — plain `<td>`
7. Hcp (s/hr) — contains `<input.handicap-override>`
8. Adjusted Hcp Change (s/hr) — `<td class="actual-delta ...">`
9. Revised Handicap — `<th class="col-revised-hcp">` and `<td class="revised-hcp">`
10. Finish Time (hh:mm:ss) — `<td class="finish-time-cell">` with `<input.finish-time>`
11. Abs Pos — `<td class="abs-pos">`
12. Race Pts (Trad) — `<th class="col-race-pts">` and `<td class="race-pts">`
13. League Pts (Adj) — `<th class="col-league-pts">` and `<td class="league-pts">`
14. On Course Time (s) — `<td class="on-course">`
15. Hcp Allowance (s) — `<td class="allowance">`
16. Full Hcp Change (s/hr) — `<td class="full-delta">`
17. Fleet Adj (%) — `<td class="fleet-adjustment-cell">`

Decisions and Target Selectors
- Header normalization: remove header-specific `nth-child(n+10)` overrides so all `<th>` use the same dark header style as the first columns (e.g., “Hcp Pos”). No change to toggle/rotation behavior.
- Finish Time highlighting: use the existing `.finish-time-cell` for body cells; add a header hook `th.col-finish-time` for symmetry if needed. Apply a light blue background to body cells only.
- Post-Finish light grey: add a shared class to all body cells to the right of Finish Time, e.g., `.post-finish-col`, applied to: `.abs-pos, .race-pts, .league-pts, .on-course, .allowance, .full-delta, .fleet-adjustment-cell`. This avoids reliance on `nth-child` and is resilient to column order changes.

Proposed CSS Tokens (custom properties)
- Within `.race-table-wrapper` context:
  - `--race-header-bg: #343a40;`
  - `--race-header-fg: #ffffff;`
  - `--race-finish-bg: #e6f7ff;`  /* light blue */
  - `--race-post-finish-bg: #f5f5f5;`  /* light grey */

Planned CSS Changes (future steps)
- Step 2: Normalize header styles
  - Remove `thead th:nth-child(n+10)` override so all headers use `--race-header-bg/fg`.
- Step 3: Finish Time column
  - Add `.race-table-wrapper tbody td.finish-time-cell { background-color: var(--race-finish-bg); }`.
- Step 4: Post-Finish columns
  - Add class `.post-finish-col` to relevant `<td>` elements in the template and style with `background-color: var(--race-post-finish-bg);`.
  - Remove existing body inversion rule `tbody td:nth-child(n+10)` to avoid conflicts.

Non-Goals and Invariants
- Do not alter time input behavior (selection, validation, normalization).
- Do not change column visibility/toggle mechanics or header rotation behavior.
- Limit scope to the race page table; standings and other pages unaffected.

Testing Notes
- Add an HTML assertions test to confirm presence of class hooks:
  - Finish Time column cells include `.finish-time-cell` (existing) and headers may include `th.col-finish-time` if added.
  - Post-Finish columns include `.post-finish-col` once applied.
- No visual regression tests added; rely on class presence and server-side rendering checks.

