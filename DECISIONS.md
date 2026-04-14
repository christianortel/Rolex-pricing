# Decisions

## 2026-04-13 - Preserve Legacy Inputs, Build A New Historical Core

Decision:
Keep the existing `AD_List/`, `AD_Count/`, `Prices/`, and `Code/` directories intact while introducing a modern package under `src/rolex_market_intelligence`.

Why:

- The repo already contains valuable historical data and domain-specific logic.
- Throwing away the legacy layout would lose context and make validation harder.
- A new package layer lets us stabilize processing without pretending the old collectors are production ready.

Consequence:

- Backward context is preserved.
- Future work can replace live collectors incrementally instead of rewriting the repo all at once.

## 2026-04-13 - Make The Core Historical Pipeline Dependency-Light

Decision:
Implement the new processing, reporting, and test layers with Python standard library components only.

Why:

- The local environment did not have a working Python setup on `PATH` or common third-party packages installed.
- A dependency-light core makes the repo runnable immediately while still allowing optional collector dependencies later.

Consequence:

- The pipeline is easier to run in constrained environments.
- More advanced plotting and dataframe ergonomics are deferred until the environment is standardized.

## 2026-04-13 - Repair Historical Dealer Location Gaps Using Cross-Snapshot Evidence

Decision:
Repair blank city fields and malformed one-letter states only when the same normalized dealer name and ZIP code appear with stable location values elsewhere in the historical dataset.

Why:

- The legacy parser introduced repeated location gaps in several snapshots.
- These defects are small enough to repair deterministically without inventing new data.
- A cross-snapshot rule is safer than a looser heuristic that could merge unrelated dealers.

Consequence:

- Validation noise is reduced and the normalized history is more analytically usable.
- More advanced entity-resolution logic is still deferred to later work.

## 2026-04-14 - Seed The New Rolex AD Collector From State Pages And Capture Raw HTML First

Decision:
Implement the live Rolex AD collector as a state-seeded crawl that saves every fetched HTML response before parsing detail pages into a normalized current snapshot.

Why:

- The current Rolex site is heavily JS-driven and unstable enough that selector-only parsing would be brittle again.
- Raw snapshot capture is necessary for debugging parser drift and for later replay in tests.
- State-page seeding is a practical starting point that does not depend on private APIs.

Consequence:

- The repo now has a modern, auditable ingestion boundary for Rolex AD collection.
- Live validation still depends on running the collector from an environment with normal outbound network access.

## 2026-04-14 - Fail Fast When The Collector Encounters Systemic Network Failure

Decision:
Abort the Rolex AD crawl after a configurable number of consecutive seed-page failures when no successful fetches have occurred.

Why:

- In blocked environments, a naive crawl wastes time across all 51 state pages while learning nothing new.
- Fast failure produces a clearer manifest and makes scheduled automation safer.

Consequence:

- The collector now records blocked-network runs quickly and transparently.
- The fail-fast threshold is configurable through environment settings.

## 2026-04-14 - Collect Chrono24 Pricing One Reference Page At A Time

Decision:
Implement the new Chrono24 collector as a reference-by-reference crawl using canonical Chrono24 reference pages instead of the old Selenium search-results workflow.

Why:

- The repo already tracks a finite reference catalog, so crawling each tracked reference directly keeps the collector bounded and understandable.
- Raw HTML capture plus per-reference manifests make parser debugging far easier than opaque browser automation.
- A reference-page approach naturally yields one current row per tracked watch, which matches the repo's analytical needs.

Consequence:

- The pricing collector now has a modern ingestion boundary with manifest and fail-fast behavior.
- Live parsing still needs one tuning pass against real Chrono24 responses outside this blocked sandbox.

## 2026-04-13 - Treat Live Collectors As Follow-Up, Not As Silent Production Code

Decision:
Do not pretend the legacy Selenium collectors are production safe. Preserve them for reference and prioritize transparent documentation plus a stable historical pipeline first.

Why:

- The existing collectors rely on brittle CSS selectors, hard-coded driver configuration, and relative-path writes.
- Shipping a polished historical core is more valuable than masking unstable live ingestion.

Consequence:

- The repo now provides honest, useful outputs from existing data.
- The next run should target durable live ingestion modules with raw snapshot capture and parser health checks.

## 2026-04-13 - Promote Live Snapshots Only From Validated Collector Manifests

Decision:
Add explicit `promote-ad` and `promote-prices` workflows that read the latest collector manifest first, refuse early-aborted or empty runs, and back up any overwritten historical file before replacing data.

Why:

- A live collector can fail partially or systemically, especially in blocked or changing environments.
- Writing directly from an interim snapshot without checking its manifest would make the checked-in history too easy to corrupt.
- The repo still depends on legacy monthly and weekly history files, so promotion needs to preserve backward compatibility while adding safety rails.

Consequence:

- Live collection and historical mutation are now separate steps with a visible approval boundary.
- Historical updates are idempotent when the incoming snapshot already matches the target file.
- Promotion commands now emit their own manifest files under `data/interim/` so the repo records what was promoted and when.

## 2026-04-13 - Validate Parser Changes By Replaying Saved Raw Runs Offline

Decision:
Add explicit `replay-ad` and `replay-prices` workflows that rebuild collector snapshots from saved raw HTML directories and compare the replayed result to the original collector snapshot when present.

Why:

- Live network access is blocked in some environments, but parser development still needs a realistic regression path.
- Fixture-level parser tests are useful but too narrow once real source HTML starts drifting.
- Replaying a saved raw run creates a stable bridge between one successful network crawl and future parser tuning without forcing repeated source traffic.

Consequence:

- The repo can now validate parser changes against archived collector output offline.
- Debugging parser drift becomes more reproducible because replay manifests capture whether counts and snapshots still match the original collector run.
- A real successful live crawl is still required before replay becomes maximally informative.

## 2026-04-13 - Represent Replay Drift As Row-Level Diff Artifacts

Decision:
When a replay run has access to the original collector snapshot, emit a dedicated diff CSV that records row key, field name, diff type, and source versus replay values instead of only reporting a boolean match flag.

Why:

- A simple `snapshot_matches_source` flag is not enough to debug parser regressions efficiently.
- Parser drift usually needs exact field-level inspection before heuristics can be tuned safely.
- CSV diff artifacts are easy to inspect locally, archive with runs, and feed into future summary checks.

Consequence:

- Replay commands now produce actionable diagnostics even when a replay fails to match the original snapshot.
- Future quality work can aggregate replay diff files across runs without re-parsing manifests.

## 2026-04-13 - Persist Historical Pipeline Metadata Beside Processed Outputs

Decision:
Write `data/processed/pipeline_run_manifest.json` and `data/processed/schema_version.json` on every historical pipeline run, capturing source input coverage, output artifact paths, and output record counts.

Why:

- Processed CSVs and reports are useful, but without persisted run metadata it is harder to verify what source inventory produced them.
- A stable manifest makes reproducibility and future automation safer because downstream checks can validate coverage windows and output presence without re-deriving everything.
- Schema/version metadata needs to live close to the processed layer so later runs can reason about compatibility explicitly.

Consequence:

- The historical pipeline now leaves behind a self-describing record of each processed-layer build.
- Future CI or scheduled jobs can diff pipeline manifests to detect unexpected source or coverage changes.

## 2026-04-13 - Aggregate Replay QA Across All Saved Raw Runs

Decision:
Add explicit replay summary commands that iterate across every saved raw run per collector and write per-run plus aggregate QA outputs under `data/quality/`.

Why:

- Single-run replay manifests are useful for debugging one capture, but parser maintenance needs a collector-level view once multiple raw runs exist.
- Aggregating mismatched fields across runs makes it much easier to see whether drift is isolated or systematic before changing parsing heuristics.
- This improves parser QA immediately without requiring live outbound access in the current sandbox.

Consequence:

- The repo now surfaces repeat parser drift patterns through `replay-summary-ad` and `replay-summary-prices`.
- Future work can extend these summaries into time-series retention or CI thresholds without redesigning replay output formats.

## 2026-04-13 - Keep Append-Only History For Processed-Layer Builds

Decision:
When the historical pipeline runs, append a summarized row to `data/processed/pipeline_run_history.csv` and archive a timestamped copy of the full manifest under `data/processed/history/`.

Why:

- A single latest manifest is not enough once processed outputs start evolving over multiple runs.
- The project needs a lightweight built-in audit trail for coverage changes, row-count jumps, and schema-compatible output growth.
- This keeps historical storage/versioning moving forward even while live-source validation is blocked in the current sandbox.

Consequence:

- The processed layer now has both a latest-state manifest and a durable cross-run history surface.
- Future automation can compare the newest run against prior history without reconstructing metadata from generated CSVs.
