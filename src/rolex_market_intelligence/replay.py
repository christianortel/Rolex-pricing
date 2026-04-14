"""Replay saved raw collector runs to validate parser behavior offline."""

from __future__ import annotations

from collections import Counter
from pathlib import Path
from urllib.parse import urljoin, urlparse
import csv
import datetime as dt
import json
import re

from .collectors.chrono24_pricing import (
    CHRONO24_REF_URL_TEMPLATE,
    parse_chrono24_reference_html,
)
from .collectors.rolex_ad import (
    CITY_URL_PATTERN,
    DEALER_URL_PATTERN,
    STORE_LOCATOR_BASE_URL,
    extract_links,
    parse_retailer_detail_html,
)
from .legacy import load_reference_catalog
from .promotion import AD_INTERIM_FIELDS, PRICING_INTERIM_FIELDS, read_csv_rows
from .settings import Settings


CHRONO24_REFERENCE_FROM_URL_PATTERN = re.compile(r"/rolex/ref-([0-9a-z]+)\.htm$", re.IGNORECASE)


class ReplayError(RuntimeError):
    """Raised when a saved raw run cannot be replayed safely."""


def write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, object]]) -> None:
    """Write CSV rows deterministically."""

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def write_json(path: Path, payload: dict[str, object]) -> None:
    """Write JSON content deterministically."""

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def read_json(path: Path) -> dict[str, object]:
    """Read a JSON file into a dictionary."""

    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def resolve_raw_run_dir(settings: Settings, collector_name: str, raw_run_dir: str | None = None) -> Path:
    """Resolve an explicit or latest raw collector run directory."""

    if raw_run_dir:
        path = Path(raw_run_dir)
        return path if path.is_absolute() else (settings.root_dir / path).resolve()

    collector_root = settings.raw_dir / collector_name
    candidates = [path for path in collector_root.iterdir() if path.is_dir()] if collector_root.exists() else []
    if not candidates:
        raise ReplayError(f"No raw runs found for {collector_name}")
    return sorted(candidates)[-1]


def list_raw_run_dirs(settings: Settings, collector_name: str) -> list[Path]:
    """List saved raw run directories for a collector in deterministic order."""

    collector_root = settings.raw_dir / collector_name
    candidates = [path for path in collector_root.iterdir() if path.is_dir()] if collector_root.exists() else []
    return sorted(candidates)


def stringify_rows(rows: list[dict[str, object]]) -> list[dict[str, str]]:
    """Normalize dictionaries for deterministic equality checks."""

    normalized: list[dict[str, str]] = []
    for row in rows:
        normalized.append({key: "" if value is None else str(value) for key, value in row.items()})
    return normalized


def load_source_snapshot_rows(settings: Settings, source_snapshot_file: str | None) -> list[dict[str, str]] | None:
    """Load the original collector snapshot rows when available."""

    if not source_snapshot_file:
        return None
    source_path = (settings.root_dir / source_snapshot_file).resolve()
    if not source_path.exists():
        return None
    return stringify_rows(read_csv_rows(source_path))


def compare_to_source_snapshot(
    settings: Settings,
    source_snapshot_file: str | None,
    replay_rows: list[dict[str, object]],
) -> bool | None:
    """Compare a replayed snapshot to the original collector snapshot when available."""

    source_rows = load_source_snapshot_rows(settings, source_snapshot_file)
    if source_rows is None:
        return None
    normalized_replay_rows = stringify_rows(replay_rows)
    source_sorted = sorted(source_rows, key=lambda row: json.dumps(row, sort_keys=True))
    replay_sorted = sorted(normalized_replay_rows, key=lambda row: json.dumps(row, sort_keys=True))
    return source_sorted == replay_sorted


def build_row_diffs(
    source_rows: list[dict[str, str]] | None,
    replay_rows: list[dict[str, object]],
    row_key_field: str,
) -> list[dict[str, str]]:
    """Build row- and field-level diffs between source and replay snapshots."""

    if source_rows is None:
        return []

    replay_rows_normalized = stringify_rows(replay_rows)
    source_by_key = {row.get(row_key_field, ""): row for row in source_rows}
    replay_by_key = {row.get(row_key_field, ""): row for row in replay_rows_normalized}
    diffs: list[dict[str, str]] = []

    all_keys = sorted(set(source_by_key) | set(replay_by_key))
    for row_key in all_keys:
        source_row = source_by_key.get(row_key)
        replay_row = replay_by_key.get(row_key)
        if source_row is None:
            diffs.append(
                {
                    "row_key": row_key,
                    "field_name": "__row__",
                    "diff_type": "missing_in_source",
                    "source_value": "",
                    "replay_value": json.dumps(replay_row, sort_keys=True) if replay_row is not None else "",
                }
            )
            continue
        if replay_row is None:
            diffs.append(
                {
                    "row_key": row_key,
                    "field_name": "__row__",
                    "diff_type": "missing_in_replay",
                    "source_value": json.dumps(source_row, sort_keys=True),
                    "replay_value": "",
                }
            )
            continue

        fields = sorted(set(source_row) | set(replay_row))
        for field_name in fields:
            source_value = source_row.get(field_name, "")
            replay_value = replay_row.get(field_name, "")
            if source_value == replay_value:
                continue
            diffs.append(
                {
                    "row_key": row_key,
                    "field_name": field_name,
                    "diff_type": "field_mismatch",
                    "source_value": source_value,
                    "replay_value": replay_value,
                }
            )
    return diffs


def summarize_diffs(diffs: list[dict[str, str]]) -> dict[str, object]:
    """Summarize replay diffs for manifest output."""

    field_mismatch_counts = Counter(
        diff["field_name"]
        for diff in diffs
        if diff["diff_type"] == "field_mismatch" and diff["field_name"] != "__row__"
    )
    return {
        "diff_row_count": len(diffs),
        "field_mismatch_count": sum(1 for diff in diffs if diff["diff_type"] == "field_mismatch"),
        "missing_in_source_count": sum(1 for diff in diffs if diff["diff_type"] == "missing_in_source"),
        "missing_in_replay_count": sum(1 for diff in diffs if diff["diff_type"] == "missing_in_replay"),
        "differing_fields": sorted(
            {
                diff["field_name"]
                for diff in diffs
                if diff["diff_type"] == "field_mismatch" and diff["field_name"] != "__row__"
            }
        ),
        "field_mismatch_counts": dict(sorted(field_mismatch_counts.items())),
    }


def load_raw_html_map(run_dir: Path, fetch_records: list[dict[str, object]]) -> tuple[dict[str, str], list[str]]:
    """Load saved raw HTML by URL from a collector run directory."""

    html_by_url: dict[str, str] = {}
    missing_raw_files: list[str] = []

    for record in fetch_records:
        relative_path = str(record.get("relative_path", "")).replace("/", "\\")
        if not relative_path:
            continue
        raw_file = run_dir / Path(relative_path)
        if not raw_file.exists():
            if record.get("ok"):
                missing_raw_files.append(str(record.get("relative_path", "")))
            continue
        html = raw_file.read_text(encoding="utf-8", errors="replace")
        if record.get("ok"):
            html_by_url[str(record.get("url", ""))] = html

    return html_by_url, missing_raw_files


def replay_rolex_ad_run(
    settings: Settings,
    raw_run_dir: str | None = None,
    persist_outputs: bool = True,
) -> dict[str, object]:
    """Replay a saved Rolex AD raw run without making network requests."""

    run_dir = resolve_raw_run_dir(settings, "rolex_ad", raw_run_dir)
    manifest_path = run_dir / "manifest.json"
    if not manifest_path.exists():
        raise ReplayError(f"Raw Rolex AD run is missing manifest.json: {run_dir}")

    source_manifest = read_json(manifest_path)
    if source_manifest.get("collector") != "rolex_ad":
        raise ReplayError(f"Manifest collector mismatch for replay: {source_manifest.get('collector')}")

    fetch_records = [
        record for record in source_manifest.get("fetch_records", [])
        if isinstance(record, dict)
    ]
    html_by_url, missing_raw_files = load_raw_html_map(run_dir, fetch_records)

    state_urls = [
        str(entry.get("state_url", ""))
        for entry in source_manifest.get("state_page_diagnostics", [])
        if isinstance(entry, dict) and entry.get("state_url")
    ]
    if not state_urls:
        state_urls = [
            str(record.get("url", ""))
            for record in fetch_records
            if DEALER_URL_PATTERN.fullmatch(urlparse(str(record.get("url", ""))).path) is None
            and CITY_URL_PATTERN.fullmatch(urlparse(str(record.get("url", ""))).path) is None
        ]

    city_urls: set[str] = set()
    discovered_detail_urls: set[str] = set()
    for state_url in state_urls:
        html = html_by_url.get(state_url, "")
        if not html:
            continue
        city_urls.update(urljoin(STORE_LOCATOR_BASE_URL, path) for path in extract_links(html, CITY_URL_PATTERN))
        discovered_detail_urls.update(urljoin(STORE_LOCATOR_BASE_URL, path) for path in extract_links(html, DEALER_URL_PATTERN))

    city_pages_replayed = 0
    for url, html in html_by_url.items():
        if CITY_URL_PATTERN.fullmatch(urlparse(url).path):
            city_pages_replayed += 1
            discovered_detail_urls.update(urljoin(STORE_LOCATOR_BASE_URL, path) for path in extract_links(html, DEALER_URL_PATTERN))

    detail_urls = sorted(
        str(record.get("url", ""))
        for record in fetch_records
        if record.get("ok") and DEALER_URL_PATTERN.fullmatch(urlparse(str(record.get("url", ""))).path)
    )
    collected_at_utc = str(source_manifest.get("collected_at_utc", ""))
    parsed_records = [
        parse_retailer_detail_html(html_by_url[detail_url], detail_url, collected_at_utc)
        for detail_url in detail_urls
        if detail_url in html_by_url
    ]
    parsed_records.sort(key=lambda row: str(row["detail_url"]))

    replay_snapshot = settings.interim_dir / "rolex_ad_replay_snapshot.csv"
    if persist_outputs:
        write_csv(replay_snapshot, AD_INTERIM_FIELDS, parsed_records)

    diagnostic_counter: dict[str, int] = {}
    for row in parsed_records:
        for diagnostic in filter(None, str(row["parser_diagnostics"]).split(",")):
            diagnostic_counter[diagnostic] = diagnostic_counter.get(diagnostic, 0) + 1

    snapshot_matches_source = compare_to_source_snapshot(
        settings,
        str(source_manifest.get("interim_snapshot_file", "")) or None,
        parsed_records,
    )
    source_rows = load_source_snapshot_rows(settings, str(source_manifest.get("interim_snapshot_file", "")) or None)
    diffs = build_row_diffs(source_rows, parsed_records, row_key_field="detail_url")
    diff_file = settings.interim_dir / "rolex_ad_replay_diff.csv"
    if persist_outputs:
        write_csv(diff_file, ["row_key", "field_name", "diff_type", "source_value", "replay_value"], diffs)
    replay_manifest = {
        "collector": "rolex_ad_replay",
        "replayed_at_utc": dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat(),
        "source_run_id": source_manifest.get("run_id", ""),
        "source_manifest_file": str(manifest_path.relative_to(settings.root_dir)).replace("\\", "/"),
        "source_parsed_record_count": int(source_manifest.get("parsed_record_count", 0)),
        "source_records_with_parser_diagnostics": int(source_manifest.get("records_with_parser_diagnostics", 0)),
        "source_interim_snapshot_file": source_manifest.get("interim_snapshot_file", ""),
        "replay_snapshot_file": (
            str(replay_snapshot.relative_to(settings.root_dir)).replace("\\", "/") if persist_outputs else None
        ),
        "state_pages_replayed": sum(1 for state_url in state_urls if state_url in html_by_url),
        "city_pages_replayed": city_pages_replayed,
        "detail_pages_replayed": len(detail_urls),
        "discovered_city_urls": len(city_urls),
        "discovered_detail_urls": len(discovered_detail_urls),
        "missing_raw_files": missing_raw_files,
        "replay_parsed_record_count": len(parsed_records),
        "replay_records_with_parser_diagnostics": sum(1 for row in parsed_records if row["parser_diagnostics"]),
        "replay_parser_diagnostic_counts": diagnostic_counter,
        "parsed_record_count_matches_source": len(parsed_records) == int(source_manifest.get("parsed_record_count", 0)),
        "records_with_parser_diagnostics_match_source": (
            sum(1 for row in parsed_records if row["parser_diagnostics"])
            == int(source_manifest.get("records_with_parser_diagnostics", 0))
        ),
        "snapshot_matches_source": snapshot_matches_source,
        "diff_file": str(diff_file.relative_to(settings.root_dir)).replace("\\", "/") if persist_outputs else None,
        **summarize_diffs(diffs),
    }
    if persist_outputs:
        write_json(settings.interim_dir / "rolex_ad_replay_latest.json", replay_manifest)
    return replay_manifest


def replay_chrono24_pricing_run(
    settings: Settings,
    raw_run_dir: str | None = None,
    persist_outputs: bool = True,
) -> dict[str, object]:
    """Replay a saved Chrono24 pricing raw run without making network requests."""

    run_dir = resolve_raw_run_dir(settings, "chrono24_pricing", raw_run_dir)
    manifest_path = run_dir / "manifest.json"
    if not manifest_path.exists():
        raise ReplayError(f"Raw Chrono24 pricing run is missing manifest.json: {run_dir}")

    source_manifest = read_json(manifest_path)
    if source_manifest.get("collector") != "chrono24_pricing":
        raise ReplayError(f"Manifest collector mismatch for replay: {source_manifest.get('collector')}")

    reference_catalog = load_reference_catalog(settings.reference_catalog_file)
    fetch_records = [
        record for record in source_manifest.get("fetch_records", [])
        if isinstance(record, dict)
    ]
    html_by_url, missing_raw_files = load_raw_html_map(run_dir, fetch_records)
    collected_at_utc = str(source_manifest.get("collected_at_utc", ""))

    parsed_records: list[dict[str, object]] = []
    for record in fetch_records:
        if not record.get("ok"):
            continue
        url = str(record.get("url", ""))
        html = html_by_url.get(url, "")
        if not html:
            continue
        match = CHRONO24_REFERENCE_FROM_URL_PATTERN.search(urlparse(url).path)
        if not match:
            continue
        reference = match.group(1).upper()
        metadata = reference_catalog.get(reference, {})
        parsed_records.append(
            parse_chrono24_reference_html(
                html=html,
                reference=reference,
                family=str(metadata.get("family", "Unknown")),
                msrp_usd=metadata.get("msrp_usd"),
                collected_at_utc=collected_at_utc,
                detail_url=CHRONO24_REF_URL_TEMPLATE.format(reference_lower=reference.lower()),
            )
        )

    parsed_records.sort(key=lambda row: str(row["reference"]))
    replay_snapshot = settings.interim_dir / "chrono24_pricing_replay_snapshot.csv"
    if persist_outputs:
        write_csv(replay_snapshot, PRICING_INTERIM_FIELDS, parsed_records)

    diagnostic_counter: dict[str, int] = {}
    for row in parsed_records:
        for diagnostic in filter(None, str(row["parser_diagnostics"]).split(",")):
            diagnostic_counter[diagnostic] = diagnostic_counter.get(diagnostic, 0) + 1

    snapshot_matches_source = compare_to_source_snapshot(
        settings,
        str(source_manifest.get("interim_snapshot_file", "")) or None,
        parsed_records,
    )
    source_rows = load_source_snapshot_rows(settings, str(source_manifest.get("interim_snapshot_file", "")) or None)
    diffs = build_row_diffs(source_rows, parsed_records, row_key_field="reference")
    diff_file = settings.interim_dir / "chrono24_pricing_replay_diff.csv"
    if persist_outputs:
        write_csv(diff_file, ["row_key", "field_name", "diff_type", "source_value", "replay_value"], diffs)
    replay_manifest = {
        "collector": "chrono24_pricing_replay",
        "replayed_at_utc": dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat(),
        "source_run_id": source_manifest.get("run_id", ""),
        "source_manifest_file": str(manifest_path.relative_to(settings.root_dir)).replace("\\", "/"),
        "source_parsed_record_count": int(source_manifest.get("parsed_record_count", 0)),
        "source_records_with_parser_diagnostics": int(source_manifest.get("records_with_parser_diagnostics", 0)),
        "source_interim_snapshot_file": source_manifest.get("interim_snapshot_file", ""),
        "replay_snapshot_file": (
            str(replay_snapshot.relative_to(settings.root_dir)).replace("\\", "/") if persist_outputs else None
        ),
        "references_available": len(parsed_records),
        "missing_raw_files": missing_raw_files,
        "replay_parsed_record_count": len(parsed_records),
        "replay_records_with_parser_diagnostics": sum(1 for row in parsed_records if row["parser_diagnostics"]),
        "replay_parser_diagnostic_counts": diagnostic_counter,
        "parsed_record_count_matches_source": len(parsed_records) == int(source_manifest.get("parsed_record_count", 0)),
        "records_with_parser_diagnostics_match_source": (
            sum(1 for row in parsed_records if row["parser_diagnostics"])
            == int(source_manifest.get("records_with_parser_diagnostics", 0))
        ),
        "snapshot_matches_source": snapshot_matches_source,
        "diff_file": str(diff_file.relative_to(settings.root_dir)).replace("\\", "/") if persist_outputs else None,
        **summarize_diffs(diffs),
    }
    if persist_outputs:
        write_json(settings.interim_dir / "chrono24_pricing_replay_latest.json", replay_manifest)
    return replay_manifest


def build_replay_rollup_rows(
    replay_manifests: list[dict[str, object]],
    extra_fieldnames: list[str],
) -> list[dict[str, object]]:
    """Build per-run replay rollup rows for CSV output."""

    rows: list[dict[str, object]] = []
    for manifest in replay_manifests:
        row = {
            "source_run_id": manifest.get("source_run_id", ""),
            "source_manifest_file": manifest.get("source_manifest_file", ""),
            "source_parsed_record_count": manifest.get("source_parsed_record_count", 0),
            "replay_parsed_record_count": manifest.get("replay_parsed_record_count", 0),
            "snapshot_matches_source": manifest.get("snapshot_matches_source", ""),
            "diff_row_count": manifest.get("diff_row_count", 0),
            "field_mismatch_count": manifest.get("field_mismatch_count", 0),
            "missing_in_source_count": manifest.get("missing_in_source_count", 0),
            "missing_in_replay_count": manifest.get("missing_in_replay_count", 0),
            "missing_raw_file_count": len(manifest.get("missing_raw_files", [])),
            "differing_fields": ",".join(manifest.get("differing_fields", [])),
        }
        for fieldname in extra_fieldnames:
            row[fieldname] = manifest.get(fieldname, 0)
        rows.append(row)
    return rows


def summarize_replay_rollup(
    settings: Settings,
    collector_name: str,
    replay_manifests: list[dict[str, object]],
    rollup_rows: list[dict[str, object]],
    rollup_csv_path: Path,
    rollup_summary_path: Path,
) -> dict[str, object]:
    """Write aggregate replay summary outputs for a collector."""

    field_mismatch_counts: Counter[str] = Counter()
    parser_diagnostic_counts: Counter[str] = Counter()
    runs_with_snapshot_mismatch = 0
    runs_missing_source_snapshot = 0
    total_diff_rows = 0

    for manifest in replay_manifests:
        field_mismatch_counts.update(
            {
                str(field_name): int(count)
                for field_name, count in dict(manifest.get("field_mismatch_counts", {})).items()
            }
        )
        parser_diagnostic_counts.update(
            {
                str(diagnostic): int(count)
                for diagnostic, count in dict(manifest.get("replay_parser_diagnostic_counts", {})).items()
            }
        )
        if manifest.get("snapshot_matches_source") is False:
            runs_with_snapshot_mismatch += 1
        if manifest.get("snapshot_matches_source") is None:
            runs_missing_source_snapshot += 1
        total_diff_rows += int(manifest.get("diff_row_count", 0))

    write_csv(rollup_csv_path, list(rollup_rows[0].keys()) if rollup_rows else [], rollup_rows)
    summary = {
        "collector": collector_name,
        "generated_at_utc": dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat(),
        "run_count": len(replay_manifests),
        "runs_with_snapshot_mismatch": runs_with_snapshot_mismatch,
        "runs_missing_source_snapshot": runs_missing_source_snapshot,
        "runs_with_diff_rows": sum(1 for manifest in replay_manifests if int(manifest.get("diff_row_count", 0)) > 0),
        "total_diff_rows": total_diff_rows,
        "field_mismatch_counts": dict(sorted(field_mismatch_counts.items())),
        "top_mismatched_fields": [
            {"field_name": field_name, "count": count}
            for field_name, count in field_mismatch_counts.most_common(10)
        ],
        "parser_diagnostic_counts": dict(sorted(parser_diagnostic_counts.items())),
        "rollup_csv_file": str(rollup_csv_path.relative_to(settings.root_dir)).replace("\\", "/"),
    }
    write_json(rollup_summary_path, summary)
    return summary


def summarize_rolex_ad_replays(settings: Settings) -> dict[str, object]:
    """Aggregate replay QA across all saved Rolex AD raw runs."""

    raw_run_dirs = list_raw_run_dirs(settings, "rolex_ad")
    if not raw_run_dirs:
        raise ReplayError("No raw runs found for rolex_ad")

    replay_manifests = [
        replay_rolex_ad_run(settings, raw_run_dir=str(raw_run_dir), persist_outputs=False)
        for raw_run_dir in raw_run_dirs
    ]
    rollup_rows = build_replay_rollup_rows(
        replay_manifests,
        extra_fieldnames=["state_pages_replayed", "city_pages_replayed", "detail_pages_replayed"],
    )
    return summarize_replay_rollup(
        settings=settings,
        collector_name="rolex_ad",
        replay_manifests=replay_manifests,
        rollup_rows=rollup_rows,
        rollup_csv_path=settings.quality_dir / "rolex_ad_replay_rollup.csv",
        rollup_summary_path=settings.quality_dir / "rolex_ad_replay_rollup_summary.json",
    )


def summarize_chrono24_pricing_replays(settings: Settings) -> dict[str, object]:
    """Aggregate replay QA across all saved Chrono24 pricing raw runs."""

    raw_run_dirs = list_raw_run_dirs(settings, "chrono24_pricing")
    if not raw_run_dirs:
        raise ReplayError("No raw runs found for chrono24_pricing")

    replay_manifests = [
        replay_chrono24_pricing_run(settings, raw_run_dir=str(raw_run_dir), persist_outputs=False)
        for raw_run_dir in raw_run_dirs
    ]
    rollup_rows = build_replay_rollup_rows(
        replay_manifests,
        extra_fieldnames=["references_available"],
    )
    return summarize_replay_rollup(
        settings=settings,
        collector_name="chrono24_pricing",
        replay_manifests=replay_manifests,
        rollup_rows=rollup_rows,
        rollup_csv_path=settings.quality_dir / "chrono24_pricing_replay_rollup.csv",
        rollup_summary_path=settings.quality_dir / "chrono24_pricing_replay_rollup_summary.json",
    )
