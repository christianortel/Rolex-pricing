# Rolex Market Intelligence

Rolex Market Intelligence is a research-oriented Python repository for tracking the U.S. Rolex authorized dealer network alongside grey-market pricing history. The original project captured monthly U.S. AD snapshots and weekly Chrono24 pricing medians. This modernization pass preserves those legacy assets, wraps them in a reproducible pipeline, and adds normalized datasets, validation outputs, change tracking, and a repeatable report-generation workflow.

## Why This Exists

The core research question is whether changes in Rolex's U.S. authorized dealer footprint relate to grey-market pricing behavior. To answer that responsibly, the repo needs more than ad hoc scripts. It needs:

- Reliable ingestion boundaries
- Historical snapshot preservation
- Clean, comparable analytical datasets
- Validation checks that surface broken parsers or suspicious drops
- Clear documentation and repeatable commands

## Current State

This repo now supports a dependency-light historical pipeline built on top of the existing local data:

- Monthly U.S. AD snapshots from `AD_List/`
- Legacy monthly AD count history from `AD_Count/AD_Count.txt`
- Weekly grey-market pricing history from `Prices/Weekly_Median_Prices.csv`

The live collectors in `Code/` are intentionally preserved for reference, but they remain legacy code. Their Selenium and CSS-selector assumptions are fragile and are documented as follow-up work in `PROJECT_STATUS.md` and `TODO_BACKLOG.md`.

The modern collectors under `src/rolex_market_intelligence/collectors/` now feed guarded promotion workflows. A successful live snapshot can be promoted into the legacy monthly AD or weekly pricing history only if its latest manifest shows successful fetches, parsed rows, and no early abort. The repo also now includes offline replay commands that re-parse saved raw collector runs, emit row-level diff artifacts, and make parser tuning regression-testable after the first successful live crawl.

## What The Pipeline Produces

Running the pipeline builds:

- Normalized AD snapshot history
- Monthly AD change log with openings, closures, and move/detail-change detection
- State-level monthly AD counts
- Long-form grey-market weekly pricing data
- Reference-level grey-market summary metrics
- Monthly market panel joining AD counts with pricing observations
- Validation summary with row-count, duplicate, null, and legacy-count comparisons
- Replay rollup summaries that aggregate parser drift across captured raw runs
- A persisted pipeline run manifest with source coverage, output paths, and record counts
- An append-only pipeline run history file plus archived manifests for cross-run comparisons
- A schema/version metadata file for the current processed layer
- Markdown market summary report
- Lightweight SVG charts for quick inspection

## Repository Structure

```text
.
|-- AD_Count/                     # Legacy monthly dealer counts
|-- AD_List/                      # Legacy monthly U.S. AD snapshots
|-- Code/                         # Legacy collectors and analysis scripts kept for reference
|-- Models/                       # Legacy Rolex model exports
|-- Prices/                       # Legacy weekly pricing history
|-- config/
|   `-- reference_catalog.json    # Reference metadata used by the modern pipeline
|-- data/
|   |-- interim/
|   |-- processed/                # Pipeline-generated analytical datasets
|   |-- quality/                  # Validation and QC outputs
|   `-- raw/
|-- reports/
|   |-- figures/                  # Pipeline-generated SVG charts
|   `-- monthly_market_summary.md
|-- src/rolex_market_intelligence/
|   |-- cli.py                    # Entrypoint
|   |-- collectors/               # Live source collectors
|   |-- legacy.py                 # Legacy source loaders
|   |-- normalize.py              # Cleaning and canonicalization helpers
|   |-- pipeline.py               # Main processing and reporting workflow
|   `-- settings.py               # Environment-driven configuration
|-- tests/
|-- CONTRIBUTING.md
|-- DECISIONS.md
|-- Makefile
|-- PROJECT_STATUS.md
|-- TODO_BACKLOG.md
`-- pyproject.toml
```

## Quick Start

This modernization pass keeps the core pipeline free of third-party runtime dependencies so it can run in constrained environments. The recommended local flow is:

1. Choose a Python 3.10+ interpreter.
2. Optionally create a virtual environment.
3. Optionally copy `.env.example` to `.env` if you want to override defaults.
4. Run the pipeline.

Example commands on Windows PowerShell:

```powershell
$env:PYTHONUTF8 = "1"
$env:PYTHONPATH = "src"
& "C:\path\to\python.exe" -m rolex_market_intelligence pipeline
& "C:\path\to\python.exe" -m rolex_market_intelligence collect-ad
& "C:\path\to\python.exe" -m rolex_market_intelligence collect-prices
& "C:\path\to\python.exe" -m rolex_market_intelligence promote-ad
& "C:\path\to\python.exe" -m rolex_market_intelligence promote-prices
& "C:\path\to\python.exe" -m rolex_market_intelligence replay-ad
& "C:\path\to\python.exe" -m rolex_market_intelligence replay-prices
& "C:\path\to\python.exe" -m rolex_market_intelligence replay-summary-ad
& "C:\path\to\python.exe" -m rolex_market_intelligence replay-summary-prices
& "C:\path\to\python.exe" -m unittest discover -s tests -v
```

If you install the project in editable mode, you can omit `PYTHONPATH` and run:

```powershell
python -m rolex_market_intelligence pipeline
```

## Configuration

Environment variables are optional unless you need to override paths:

- `ROLEX_PROJECT_ROOT`: explicit repository root
- `ROLEX_AD_LIST_DIR`: override the legacy AD snapshot directory
- `ROLEX_AD_COUNT_FILE`: override the legacy AD count file
- `ROLEX_PRICING_FILE`: override the legacy pricing CSV
- `ROLEX_REFERENCE_CATALOG`: override the reference metadata JSON
- `ROLEX_REPORT_DIR`: override generated report location
- `ROLEX_PROCESSED_DIR`: override processed dataset location
- `ROLEX_QUALITY_DIR`: override QC output location
- `ROLEX_USER_AGENT`: reserved for future live collectors
- `ROLEX_REQUEST_TIMEOUT_SECONDS`: reserved for future live collectors
- `ROLEX_REQUEST_DELAY_SECONDS`: polite delay between live collector requests
- `ROLEX_COLLECTOR_FAIL_FAST_AFTER_ERRORS`: abort a live crawl early if repeated seed-page failures suggest a blocked or broken environment

## Methodology Notes

### Authorized Dealer History

- Legacy monthly snapshot files are parsed from their filenames.
- Dealer records are normalized to canonical text fields.
- A canonical dealer ID is built from cleaned dealer name, address, city, state, and ZIP.
- The new live Rolex AD collector crawls state seed pages, captures raw HTML responses, writes a run manifest, and emits a normalized current snapshot to `data/interim/`.
- `replay-ad` re-parses a saved raw Rolex run directory and writes replay artifacts plus row-level diff outputs so parser drift can be debugged offline.
- `replay-summary-ad` aggregates replay results across all saved Rolex raw runs and highlights the most frequently mismatched fields.
- `promote-ad` converts a validated live AD snapshot into the legacy `AD_List/Rolex_AD_List_<month>_<year>.csv` format, updates `AD_Count/AD_Count.txt`, and writes a promotion manifest.
- Month-to-month change detection classifies:
  - `opening`
  - `closure`
  - `move_or_detail_change`

### Grey-Market Pricing

- The legacy wide pricing file is converted to long format at the reference/week level.
- Reference metadata is attached from `config/reference_catalog.json`.
- Monthly averages are calculated for pricing, listing count, and premium/discount.
- The new live Chrono24 collector fetches one reference page per tracked watch, saves raw HTML responses, emits a manifest, and writes a normalized current pricing snapshot to `data/interim/`.
- `replay-prices` re-parses a saved raw Chrono24 run directory and writes replay artifacts plus row-level diff outputs so parser adjustments can be checked without new network traffic.
- `replay-summary-prices` aggregates replay results across all saved Chrono24 raw runs and highlights the most frequently mismatched fields.
- `promote-prices` converts a validated live pricing snapshot into the legacy wide weekly pricing history, replacing only the matching date row when necessary and backing up overwritten history files first.

### Statistical Posture

This repo is currently strongest for descriptive analytics. The new monthly panel supports later correlation and regression work, but the project does not yet claim causal identification. Any future causal framing should be explicit about data coverage limits, timing assumptions, and omitted-variable risk.

## Known Caveats

- The live Rolex and Chrono24 collectors in `Code/` are legacy and may not run today without repair.
- The new Rolex AD collector is wired under `src/rolex_market_intelligence/collectors/rolex_ad.py`, but the current sandbox blocks outbound access to rolex.com, so live validation still needs to happen in an environment with normal network access.
- The new Chrono24 pricing collector is wired under `src/rolex_market_intelligence/collectors/chrono24_pricing.py`, but the current sandbox also blocks outbound access to chrono24.com, so live validation still needs to happen in an environment with normal network access.
- The replay commands validate parsing against saved raw runs, but they cannot substitute for a first real networked crawl that captures current production HTML.
- The new promotion commands intentionally refuse to write historical files if the latest collector manifest aborted early or produced zero parsed rows.
- Current AD coverage ends with October 2023.
- Current weekly pricing coverage ends with September 13, 2024.
- The pipeline normalizes messy historical records, but some legacy parser artifacts remain visible in source data and are surfaced via QC outputs instead of being silently discarded.
- State and metro enrichment is still limited; metro logic is a planned follow-up.

## Developer Workflow

Useful commands:

```bash
make pipeline
make collect-ad
make collect-prices
make promote-ad
make promote-prices
make replay-ad
make replay-prices
make replay-summary-ad
make replay-summary-prices
make test
make clean
```

If `make` is unavailable on your system, use the equivalent Python commands described above and keep `PYTHONPATH=src` unless the package is installed in editable mode.

Replay commands default to the latest saved raw run for each collector. You can target a specific captured run with `--raw-run-dir data/raw/<collector>/<run_id>`. Each replay also writes a diff CSV under `data/interim/` so mismatched fields can be inspected directly instead of inferred from a boolean summary. The replay summary commands aggregate all saved raw runs into per-run CSVs and JSON summaries under `data/quality/`, which is the fastest way to see whether one field is drifting repeatedly across captured source HTML.

The historical pipeline also now appends one row per run to `data/processed/pipeline_run_history.csv` and writes timestamped manifest copies under `data/processed/history/`. Those files are meant for comparing processed-layer coverage and row counts over time instead of only inspecting the latest manifest.

## Legal And Ethical Notes

- Respect source terms of service before re-enabling live collectors.
- Use rate limiting, caching, retries, and polite user-agent strings for any future scraping.
- Preserve raw responses before transformation when live ingestion is restored.
- Treat grey-market pricing as observational market data, not an official Rolex source.

## Roadmap

- Stabilize live collectors with raw snapshot capture and parser diagnostics
- Add richer geo-normalization and metro-level aggregation
- Expand monthly analytical features and lag structures
- Add reproducible modeling notebooks and regression outputs
- Add automation and CI once the live ingestion layer is dependable
