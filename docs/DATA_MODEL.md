# Data Model

## Legacy Inputs

- `AD_List/Rolex_AD_List_<month>_<year>.csv`: monthly U.S. authorized dealer snapshots
- `AD_Count/AD_Count.txt`: monthly dealer counts logged by the original project
- `Prices/Weekly_Median_Prices.csv`: wide weekly grey-market pricing history

## Generated Datasets

### `data/interim/rolex_ad_live_snapshot.csv`

Current live Rolex AD collection output from the modern collector. This file is intended as the normalized current snapshot before it is promoted into longer-term history.

### `data/interim/rolex_ad_latest_manifest.json`

Latest collector run metadata, including source freshness, fetch failures, parser diagnostics, and raw snapshot file references.

### `data/interim/rolex_ad_promotion_latest.json`

Latest AD promotion metadata. Written only after `promote-ad` validates the most recent collector manifest and either writes or noops the monthly historical snapshot.

### `data/interim/rolex_ad_replay_snapshot.csv`

Replayed Rolex AD snapshot reconstructed from a saved raw run directory. Useful for parser regression checks after a live crawl has been captured.

### `data/interim/rolex_ad_replay_latest.json`

Replay manifest for the latest offline Rolex AD validation run, including source run references and whether the replayed snapshot matches the original collector snapshot.

### `data/interim/rolex_ad_replay_diff.csv`

Row- and field-level diff output for the latest Rolex AD replay run. Each row identifies the replay key, field name, diff type, and source versus replay value.

### `data/interim/chrono24_pricing_live_snapshot.csv`

Current live Chrono24 pricing collection output, one row per tracked Rolex reference with observed sample count, listing count, median price, and markup against MSRP.

### `data/interim/chrono24_pricing_latest_manifest.json`

Latest Chrono24 collector run metadata, including per-reference fetch results and parser diagnostics.

### `data/interim/chrono24_pricing_promotion_latest.json`

Latest pricing promotion metadata. Written only after `promote-prices` validates the latest collector manifest and writes or noops the weekly pricing history update.

### `data/interim/chrono24_pricing_replay_snapshot.csv`

Replayed Chrono24 pricing snapshot reconstructed from a saved raw run directory for offline parser validation.

### `data/interim/chrono24_pricing_replay_latest.json`

Replay manifest for the latest offline Chrono24 validation run, including source run references and replay-versus-source snapshot comparisons.

### `data/interim/chrono24_pricing_replay_diff.csv`

Row- and field-level diff output for the latest Chrono24 replay run. This is the first file to inspect when `snapshot_matches_source` becomes `false`.

### `data/raw/history_backups/`

Automatic backups of overwritten legacy history files created by promotion commands before they replace an existing monthly AD snapshot or weekly pricing row.

### `data/processed/ad_snapshot_history.csv`

One row per dealer per monthly snapshot with cleaned text fields and a canonical dealer ID.

### `data/processed/ad_change_log.csv`

Month-over-month dealer events:

- `opening`
- `closure`
- `move_or_detail_change`

### `data/processed/ad_state_monthly_counts.csv`

Monthly dealer counts by state.

### `data/processed/grey_market_weekly_long.csv`

One row per weekly observation per Rolex reference with median price, listing count, markup, and family metadata.

### `data/processed/grey_reference_summary.csv`

Reference-level summary metrics based on the full weekly history and latest observations.

### `data/processed/monthly_market_panel.csv`

Monthly reference-level analytical panel joining pricing aggregates with U.S. AD counts where available.

### `data/processed/pipeline_run_manifest.json`

Persisted historical pipeline manifest describing source input files, coverage windows, quality rollups, and generated output artifacts with record counts.

### `data/processed/pipeline_run_history.csv`

Append-only processed-layer run history with one row per pipeline execution, including coverage windows, row counts, QC rollups, and the linked latest-manifest path.

### `data/processed/history/pipeline_run_manifest_<timestamp>.json`

Timestamped archived copies of each processed-layer manifest so changes in source coverage or output sizes can be compared across runs.

### `data/processed/schema_version.json`

Lightweight schema/version metadata for the currently generated processed layer, including the linked latest-manifest and pipeline-history paths.

### `data/quality/validation_summary.csv`

Per-month validation metrics for AD snapshots, including duplicate rate, blank fields, count deltas, and mismatch versus `AD_Count.txt`.

### `data/quality/rolex_ad_replay_rollup.csv`

Per-run replay QA table for all saved Rolex AD raw runs, including snapshot-match status, diff counts, missing raw file counts, and the fields that differed for each run.

### `data/quality/rolex_ad_replay_rollup_summary.json`

Aggregate replay QA summary for saved Rolex AD raw runs, including top mismatched fields and total drift counts across runs.

### `data/quality/chrono24_pricing_replay_rollup.csv`

Per-run replay QA table for all saved Chrono24 pricing raw runs, including snapshot-match status, diff counts, missing raw file counts, and differing fields.

### `data/quality/chrono24_pricing_replay_rollup_summary.json`

Aggregate replay QA summary for saved Chrono24 pricing raw runs, including top mismatched fields and total drift counts across runs.
