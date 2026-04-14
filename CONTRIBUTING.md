# Contributing

## Development Principles

- Preserve legacy data and context unless a component is clearly obsolete or harmful.
- Prefer explicit, testable transformations over notebook-only logic.
- Fail loudly when source assumptions break.
- Separate descriptive analysis from causal claims.

## Local Setup

1. Use Python 3.10 or newer.
2. Optionally copy `.env.example` to `.env`.
3. Either export `PYTHONPATH=src` for local runs or install the project in editable mode:

```bash
python -m pip install -e .
```

4. Run the pipeline:

```bash
PYTHONPATH=src python -m rolex_market_intelligence pipeline
```

5. Run the live Rolex AD collector:

```bash
PYTHONPATH=src python -m rolex_market_intelligence collect-ad
```

6. Run the live Chrono24 pricing collector:

```bash
PYTHONPATH=src python -m rolex_market_intelligence collect-prices
```

7. Promote a validated live AD snapshot into historical monthly files:

```bash
PYTHONPATH=src python -m rolex_market_intelligence promote-ad
```

8. Promote a validated live pricing snapshot into historical weekly history:

```bash
PYTHONPATH=src python -m rolex_market_intelligence promote-prices
```

9. Replay a saved raw Rolex AD run to validate parser behavior offline:

```bash
PYTHONPATH=src python -m rolex_market_intelligence replay-ad --raw-run-dir data/raw/rolex_ad/<run_id>
```

10. Replay a saved raw Chrono24 pricing run to validate parser behavior offline:

```bash
PYTHONPATH=src python -m rolex_market_intelligence replay-prices --raw-run-dir data/raw/chrono24_pricing/<run_id>
```

11. Run tests:

```bash
PYTHONPATH=src python -m unittest discover -s tests -v
```

## Contribution Checklist

- Add or update tests when changing normalization or change-detection logic.
- Prefer replaying saved raw runs before loosening parser heuristics after a live source change, and inspect the replay diff CSVs when snapshots no longer match.
- Check the replay rollup summaries under `data/quality/` before changing parser heuristics so repeated field drift is handled deliberately instead of one run at a time.
- Keep promotion workflows append-safe and back up overwritten historical files.
- Keep the processed-layer manifest outputs accurate when changing pipeline outputs or source coverage assumptions.
- Keep `data/processed/pipeline_run_history.csv` and archived manifest behavior stable when changing processed-layer metadata fields.
- Update `PROJECT_STATUS.md`, `TODO_BACKLOG.md`, and `DECISIONS.md` for meaningful project changes.
- Document any new data source assumptions in `README.md`.
- Keep generated analytical artifacts deterministic where possible.

## Code Style

- Prefer typed functions and small reusable helpers.
- Add brief docstrings for non-obvious modules and functions.
- Use logging instead of print debugging for pipeline code.
- Avoid adding hard-coded local paths or secrets.
