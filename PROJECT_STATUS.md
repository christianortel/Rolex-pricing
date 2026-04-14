# Project Status

Last updated: 2026-04-13

## Current Repo Assessment

The repository began as a small set of one-off scripts for collecting Rolex U.S. authorized dealer counts/lists and weekly Chrono24 pricing. The project contained useful historical data, but the operational layer was stale:

- no package structure
- no project tracking files
- no reproducible command surface
- no tests
- no validation layer
- no processed analytical datasets
- no clean reporting artifacts
- legacy collectors depended on hard-coded Selenium, local ChromeDriver config, fragile CSS selectors, and relative paths

The strongest asset was the historical data already checked in:

- 28 monthly U.S. AD list snapshots from July 2021 through October 2023
- monthly AD count history in `AD_Count/AD_Count.txt`
- 163 weekly grey-market pricing observations through September 13, 2024

Project health is now materially better than the starting point: historical processing, persisted pipeline metadata with cross-run retention, live collection boundaries, guarded promotion workflows, offline replay validation with field-level diffing and cross-run rollups, tests, and repo-level documentation are all in place. The biggest remaining gap is live-network validation against current source HTML outside this blocked sandbox.

## What The Project Currently Does

After this run, the repo now has a modernized historical processing layer that:

- ingests the existing legacy AD and pricing datasets
- normalizes dealer records into canonical IDs
- computes monthly AD openings, closures, and move/detail-change events
- builds state-level monthly AD counts
- converts wide pricing history into long-form reference/week observations
- produces a monthly AD/pricing panel for downstream analysis
- generates a validation summary and a written market report with charts
- writes a persisted pipeline manifest and schema/version metadata for the processed historical layer
- writes append-only processed-layer history so pipeline coverage and output counts can be compared across runs
- provides a new live Rolex AD collector that captures raw snapshots, writes run manifests, and emits a normalized current snapshot in `data/interim/`
- provides a new live Chrono24 pricing collector that captures raw snapshots, writes run manifests, and emits a normalized current pricing snapshot in `data/interim/`
- provides replay commands that can rebuild snapshots from saved raw collector runs, compare them to the original collector outputs, and emit field-level diff artifacts
- provides replay summary commands that aggregate parser drift across all saved raw runs for each collector
- provides append-safe promotion commands that can write validated live AD and pricing snapshots back into the legacy monthly and weekly history files
- exposes a clean CLI entrypoint and test suite

## What Is Broken, Risky, Or Outdated

- The old Rolex store locator parser in `Code/Authorized_Dealers.py` uses brittle class selectors and Selenium assumptions.
- The old Chrono24 collectors in `Code/Prices.py` and `Code/Prices_V2.py` rely on browser automation and a page structure that may have changed.
- Historical AD files contain parser artifacts and encoding issues that still need better remediation.
- No automated job or CI is configured yet.
- No metro/geospatial enrichment exists yet.
- Modeling remains descriptive rather than causal.

## Immediate Priorities

1. Validate the new Rolex AD and Chrono24 pricing collectors against live network access and tune parsing heuristics if current HTML differs from the tested fixture paths.
2. Capture at least one successful live raw run for each collector, then use the new replay commands to compare parser output against the saved collector snapshot before promoting.
3. Run the new promotion commands on validated live snapshots and inspect the resulting monthly and weekly history updates.
4. Enrich AD history with better relocation/entity-resolution logic.
5. Wire the repo into scheduled automation and CI once live collectors are stable.
6. Add geo-enrichment and richer entity-resolution features to deepen the analytical layer.

## This Run

Completed:

- Extended processed-layer history retention with:
  - `data/processed/pipeline_run_history.csv`
  - timestamped archived manifests under `data/processed/history/`
  - schema metadata that now points to both the latest manifest and the append-only history file
- Added replay summary rollups with:
  - `replay-summary-ad` for all saved Rolex AD raw runs
  - `replay-summary-prices` for all saved Chrono24 pricing raw runs
  - per-run replay QA CSV outputs under `data/quality/`
  - aggregate JSON summaries that surface the most frequently mismatched parsed fields across captured runs
- Added persisted historical pipeline metadata with:
  - `data/processed/pipeline_run_manifest.json`
  - `data/processed/schema_version.json`
  - source coverage, output file paths, and output record counts for each pipeline run
- Added offline replay workflows with:
  - `replay-ad` for saved raw Rolex AD runs
  - `replay-prices` for saved raw Chrono24 pricing runs
  - replay manifests that compare replayed output against the original collector snapshot when available
  - row-level diff CSV outputs that pinpoint mismatched fields when replay output drifts from the source snapshot
  - deterministic replay snapshot CSV outputs under `data/interim/`
- Added append-safe promotion workflows with:
  - `promote-ad` for validated live dealer snapshots
  - `promote-prices` for validated live pricing snapshots
  - collector-manifest checks that refuse early-aborted or empty runs
  - automatic backups before historical file replacement
  - promotion manifests under `data/interim/`
- Wired promotion commands into the main CLI and `Makefile`.
- Wired replay commands into the main CLI and `Makefile`.
- Wired replay summary commands into the main CLI and `Makefile`.
- Added unit tests covering promotion success, replay reconstruction, row-level replay diffs, persisted pipeline metadata, idempotent noops, and blocked-manifest rejection.
- Added unit tests covering replay rollup aggregation for both collectors.
- Extended the pipeline integration test to validate history-file and archived-manifest creation.
- Verified the full test suite passes with 20 tests.
- Verified the historical pipeline writes manifest and schema metadata successfully in both test and real-repo runs.
- Verified the historical pipeline now appends run history successfully in the real repo and archives a timestamped manifest copy for the current run.
- Verified the new replay CLI commands can rebuild the current blocked-network raw runs into matching zero-row replay artifacts instead of failing ambiguously.
- Verified the replay CLI commands now emit empty diff CSVs for matching runs and structured diff metadata in replay manifests.
- Verified the replay summary CLI commands can aggregate the current blocked-network raw-run inventory into zero-drift QA summaries instead of requiring manual inspection run by run.
- Verified the new promotion CLI commands still fail clearly against the current blocked-network manifests instead of mutating checked-in history.

Open blockers:

- The sandbox blocks outbound requests to rolex.com, so the new collector could only be validated structurally; it still needs a live-network verification pass.
- The sandbox also blocks outbound requests to chrono24.com, so the new pricing collector could only be validated structurally; it still needs a live-network verification pass.
- The local machine did not have a standard Python environment on `PATH`; commands currently rely on an explicit interpreter path unless the user installs one.
- Because both latest manifests in this sandbox are early-aborted blocked-network runs, the new promotion commands currently and correctly refuse to touch the checked-in historical files here.
- The replay layer can validate saved raw runs offline, but it cannot create those raw runs; one real networked crawl per collector is still required before parser tuning can be grounded in current production HTML.

## Current Next Priority

Run both new live collectors from an environment with normal outbound network access, save at least one successful raw run for each source, execute `replay-ad` and `replay-prices` against those saved runs to validate parser determinism and inspect any replay diff CSVs, tune any parsing heuristics that differ from the captured HTML, and then execute `promote-ad` and `promote-prices` on the validated snapshots.
