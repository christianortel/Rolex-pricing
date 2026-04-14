# TODO Backlog

Last updated: 2026-04-13

## P0 = Critical Foundations

- [x] Audit the legacy repository structure and source data inventory.
- [x] Add `PROJECT_STATUS.md`, `TODO_BACKLOG.md`, and `DECISIONS.md`.
- [x] Create a modern package/CLI entrypoint for reproducible processing.
- [x] Build normalized historical datasets from the existing AD and pricing files.
- [x] Add a first-pass validation layer for counts, duplicates, blank fields, and legacy count mismatches.
- [x] Repair recurring blank-city and malformed-state artifacts where historical evidence makes the fix deterministic.
- [x] Expand the README and contributor setup guidance.
- [x] Replace the legacy live Rolex AD collector with a resilient raw-snapshot-preserving ingestion module.
- [x] Replace the legacy Chrono24 pricing collector with a resilient source adapter and parser diagnostics.
- [x] Add shared source freshness checks and run metadata conventions across every live ingestion run, including promotion rules for current snapshots.
- [ ] Validate the new Rolex AD collector against live network access and tune state/city discovery heuristics using current rolex.com responses.
- [ ] Validate the new Chrono24 pricing collector against live network access and tune price/listing parsing heuristics using current chrono24.com responses.

## P1 = Major Enhancements

- [x] Generate an analytical monthly panel joining AD history and grey-market pricing.
- [x] Generate a markdown report and chart outputs from the pipeline.
- [x] Add unit tests for core normalization and change-detection logic.
- [x] Promote validated live Chrono24 pricing snapshots into the historical weekly pricing append workflow.
- [x] Promote validated live Rolex AD snapshots into the historical monthly snapshot append workflow.
- [ ] Add state-to-region and metro-level enrichment.
- [ ] Add more robust dealer entity resolution beyond exact normalized-name matching.
- [ ] Add reference-family aggregates and model-level leaderboards.
- [ ] Add CI for tests, linting, and scheduled dry-run validation.
- [x] Add a historical run manifest and schema version metadata file.
- [x] Add a replayable fixture-validation harness for saved live collector HTML so parser tuning can be regression tested after the first real network validation pass.
- [x] Add replay-vs-source diff reporting that pinpoints which parsed fields changed when a collector replay no longer matches the original snapshot.
- [x] Add replay summary rollups that flag the highest-frequency mismatched fields across captured raw runs.
- [x] Extend the pipeline manifest with cross-run history or retention so processed-layer changes can be compared over time instead of only inspecting the latest run.
- [ ] Persist replay rollup history over time so parser drift can be trended across collection dates instead of only summarized from the current saved raw-run inventory.
- [ ] Add automated diff checks or thresholds that compare the latest pipeline run against `pipeline_run_history.csv` and fail loudly on suspicious processed-layer changes.

## P2 = Valuable But Optional Improvements

- [ ] Add panel regressions, lag analysis, and sensitivity checks.
- [ ] Add event-study style datasets around major AD openings/closures.
- [ ] Add a lightweight dashboard or notebook-backed visual app.
- [ ] Add geospatial layers for state and metro mapping.
- [ ] Add richer model metadata, including Rolex collection hierarchy and release/discontinuation events.
