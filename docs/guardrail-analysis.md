Guardrail: baseline-change prompt when saving a race

Summary
- The edit page shows a confirm dialog if it believes the “race baseline” (pre-race handicap seeds + scoring content) changed since the page loaded and that this change “affects results”.
- In practice this frequently produces false positives because the check compares a fresh server preview to rounded and sometimes stale DOM numbers that the client renders for convenience.
- The save flow already always fetches a fresh server preview before showing the confirmation modal, so the guard is redundant. Removing it would eliminate the noisy prompt without reducing safety.

Where the logic lives
- Client guardrail entry: `app/templates/series_detail.html:1199` inside the Save button handler.
  - Fetches `GET /api/races/<race_id>/snapshot_version` with optional `cids` and `finishers`.
  - Compares against `window.PRE_SNAPSHOT_VERSION` injected at render: `app/templates/series_detail.html:94`.
  - If changed, performs `POST /api/races/<race_id>/preview` and compares selected fields from the server payload to a “client snapshot” built from table text.
  - If any difference detected, shows the prompt: “The race baseline changed and affects results. Refresh now to recompute preview?”.
- Server snapshot version API: `app/routes.py:2200-2273`.
  - Version is a SHA1 of entrant-only seeds (`build_pre_race_snapshot`) plus a filtered scoring content hash (`_scoring_content_hash_filtered`) that depends on finisher count.
- Server injects initial snapshot/version when rendering the page: `app/routes.py:1969-2010` and `:2009-2017` (via `pre_race_seeds` and `pre_snapshot_version`).

What the guard tries to do
1) Detect concurrent baseline changes (prior race edit, fleet change, or scoring setting change) after the page was opened.
2) Warn the user only if that baseline change would alter the currently displayed results.

Why it false-positives
- Presentation rounding mismatch:
  - UI rounds several numbers to integers for readability (e.g., league points, deltas). See `applyPreviewResults.toRounded` in `app/templates/series_detail.html:1371-1380` and its usage updating `.league-pts`, `.race-pts`, etc.
  - The guard compares these rounded values against unrounded numeric values from the server preview (`keys = ['abs_pos','hcp_pos','actual_delta','race_pts','league_pts','revised_hcp']` at `series_detail.html:1293`). Any fractional server value vs. rounded DOM value triggers a difference and therefore a prompt.
- Delta parsing bug (loss of decimal point):
  - Client snapshot reads `actual_delta` from the DOM and strips non-digits with `/[^\d\-]/g` at `series_detail.html:1241`.
  - This drops the decimal point (e.g., `12.3` becomes `123`), causing large spurious mismatches against server values.
- Stale DOM vs. fresh preview:
  - The guard’s comparison happens just before performing a server preview inside the Save handler. DOM values may be mid-keystroke and live preview is debounced (`scheduleLivePreview` → 80ms), so the DOM can be out of sync with the payload that the guard posts to preview.
  - The DOM also might still show initial table values for rows not touched since load; comparing those against a fresh preview (which reflects current seeds/settings) yields differences.
- Finisher-count asymmetry:
  - `finishers` sent to `snapshot_version` is computed as “non-empty input count” (`series_detail.html:1216`), not validated finishers after start-time or HH:MM:SS parsing. The server’s filtered scoring hash uses validated finisher counts (via `_scoring_content_hash_filtered`). This can cause version changes that don’t actually reflect meaningful result changes.

Why the guard is no longer required
- The Save flow already performs a server-side preview and then shows the confirmation modal populated from that preview (`preview_race` at `app/routes.py:2714-3218`). See Save handler sequence in `series_detail.html:1315-1329` which calls `previewChanges()` to fetch server results and updates the DOM prior to final confirmation.
- If seeds or settings changed mid-session, the server preview naturally incorporates them, so the user sees correct, current results before saving. The prompt adds no safety beyond what the preview already guarantees.
- Additional protections exist for scoring settings changes across tabs (versioned settings and storage/visibility listeners in `series_detail.html:1688-1720`), further reducing the original need for the guard.

Conclusion and recommendation
- Root causes: DOM rounding, delta parsing bug, and timing mismatches between DOM and server preview lead to frequent false positives.
- Recommendation: Remove the guardrail block in the Save handler and rely on the always-fresh server preview to populate the confirmation modal. Keep the snapshot version API for other uses and retain injected `PRE_SNAPSHOT_VERSION` until template cleanup.
- If we preferred to keep a minimal check, it must compare server preview against server preview (previous vs. latest) rather than DOM text; however, that reintroduces complexity with little benefit given the current save flow.

