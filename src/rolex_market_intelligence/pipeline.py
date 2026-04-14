"""Historical processing pipeline for Rolex Market Intelligence."""

from __future__ import annotations

from collections import Counter, defaultdict
from pathlib import Path
from statistics import correlation
from typing import Sequence
import csv
import datetime as dt
import json
import logging
import math

from .legacy import load_ad_count_history, load_ad_history, load_pricing_history
from .promotion import read_csv_rows
from .settings import Settings


LOGGER = logging.getLogger(__name__)


def mean(values: Sequence[float]) -> float | None:
    """Return the arithmetic mean or None for empty sequences."""

    if not values:
        return None
    return sum(values) / len(values)


def safe_round(value: float | None, digits: int = 2) -> float | None:
    """Round floats while preserving None."""

    if value is None:
        return None
    return round(value, digits)


def pct_change(old: float | None, new: float | None) -> float | None:
    """Compute percent change when both values are available and the baseline is non-zero."""

    if old in (None, 0) or new is None:
        return None
    return ((new / old) - 1.0) * 100.0


def write_csv(path: Path, rows: Sequence[dict[str, object]], fieldnames: Sequence[str]) -> None:
    """Write rows to CSV with deterministic header order."""

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def write_json(path: Path, payload: dict[str, object]) -> None:
    """Write JSON with stable formatting."""

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def build_ad_monthly_counts(ad_rows: Sequence[dict[str, object]]) -> list[dict[str, object]]:
    """Aggregate total U.S. AD counts by snapshot month."""

    grouped: dict[str, set[str]] = defaultdict(set)
    for row in ad_rows:
        grouped[str(row["snapshot_month"])].add(str(row["canonical_id"]))

    return [
        {"snapshot_month": snapshot_month, "ad_count_us": len(grouped[snapshot_month])}
        for snapshot_month in sorted(grouped)
    ]


def build_ad_state_counts(ad_rows: Sequence[dict[str, object]]) -> list[dict[str, object]]:
    """Aggregate dealer counts by month and state."""

    grouped: dict[tuple[str, str], set[str]] = defaultdict(set)
    for row in ad_rows:
        grouped[(str(row["snapshot_month"]), str(row["state"]))].add(str(row["canonical_id"]))

    output: list[dict[str, object]] = []
    for snapshot_month, state in sorted(grouped):
        output.append(
            {
                "snapshot_month": snapshot_month,
                "state": state,
                "dealer_count": len(grouped[(snapshot_month, state)]),
            }
        )
    return output


def build_ad_change_log(ad_rows: Sequence[dict[str, object]]) -> list[dict[str, object]]:
    """Compute month-to-month AD openings, closures, and likely moves/detail changes."""

    snapshots: dict[str, list[dict[str, object]]] = defaultdict(list)
    for row in ad_rows:
        snapshots[str(row["snapshot_month"])].append(dict(row))

    sorted_months = sorted(snapshots)
    changes: list[dict[str, object]] = []

    for prior_month, current_month in zip(sorted_months, sorted_months[1:]):
        prior_rows = snapshots[prior_month]
        current_rows = snapshots[current_month]
        prior_by_id = {str(row["canonical_id"]): row for row in prior_rows}
        current_by_id = {str(row["canonical_id"]): row for row in current_rows}

        unmatched_prior = {
            canonical_id: row for canonical_id, row in prior_by_id.items() if canonical_id not in current_by_id
        }
        unmatched_current = {
            canonical_id: row for canonical_id, row in current_by_id.items() if canonical_id not in prior_by_id
        }

        prior_by_name: dict[str, list[tuple[str, dict[str, object]]]] = defaultdict(list)
        current_by_name: dict[str, list[tuple[str, dict[str, object]]]] = defaultdict(list)
        for canonical_id, row in unmatched_prior.items():
            prior_by_name[str(row["normalized_name"])].append((canonical_id, row))
        for canonical_id, row in unmatched_current.items():
            current_by_name[str(row["normalized_name"])].append((canonical_id, row))

        resolved_prior: set[str] = set()
        resolved_current: set[str] = set()

        for normalized_name in sorted(set(prior_by_name) & set(current_by_name)):
            if len(prior_by_name[normalized_name]) == 1 and len(current_by_name[normalized_name]) == 1:
                prior_id, prior_row = prior_by_name[normalized_name][0]
                current_id, current_row = current_by_name[normalized_name][0]
                resolved_prior.add(prior_id)
                resolved_current.add(current_id)
                changed_fields = []
                for field_name in ("address", "city", "state", "zip_code"):
                    if str(prior_row[field_name]) != str(current_row[field_name]):
                        changed_fields.append(field_name)
                changes.append(
                    {
                        "change_month": current_month,
                        "prior_snapshot_month": prior_month,
                        "change_type": "move_or_detail_change",
                        "dealer_name": current_row["dealer_name"],
                        "state": current_row["state"],
                        "city": current_row["city"],
                        "prior_canonical_id": prior_id,
                        "current_canonical_id": current_id,
                        "prior_address": prior_row["address"],
                        "current_address": current_row["address"],
                        "detail": ",".join(changed_fields) or "normalized_name_match",
                    }
                )

        for canonical_id, row in sorted(unmatched_current.items()):
            if canonical_id in resolved_current:
                continue
            changes.append(
                {
                    "change_month": current_month,
                    "prior_snapshot_month": prior_month,
                    "change_type": "opening",
                    "dealer_name": row["dealer_name"],
                    "state": row["state"],
                    "city": row["city"],
                    "prior_canonical_id": "",
                    "current_canonical_id": canonical_id,
                    "prior_address": "",
                    "current_address": row["address"],
                    "detail": "new_canonical_id",
                }
            )

        for canonical_id, row in sorted(unmatched_prior.items()):
            if canonical_id in resolved_prior:
                continue
            changes.append(
                {
                    "change_month": current_month,
                    "prior_snapshot_month": prior_month,
                    "change_type": "closure",
                    "dealer_name": row["dealer_name"],
                    "state": row["state"],
                    "city": row["city"],
                    "prior_canonical_id": canonical_id,
                    "current_canonical_id": "",
                    "prior_address": row["address"],
                    "current_address": "",
                    "detail": "missing_from_current_snapshot",
                }
            )

    return sorted(changes, key=lambda row: (str(row["change_month"]), str(row["change_type"]), str(row["dealer_name"])))


def build_validation_summary(
    ad_rows: Sequence[dict[str, object]],
    ad_count_history: dict[str, int],
) -> list[dict[str, object]]:
    """Summarize data quality metrics for each AD snapshot."""

    snapshots: dict[str, list[dict[str, object]]] = defaultdict(list)
    for row in ad_rows:
        snapshots[str(row["snapshot_month"])].append(dict(row))

    output: list[dict[str, object]] = []
    prior_count: int | None = None

    for snapshot_month in sorted(snapshots):
        rows = snapshots[snapshot_month]
        canonical_ids = [str(row["canonical_id"]) for row in rows]
        counts = Counter(canonical_ids)
        duplicate_rows = sum(count - 1 for count in counts.values() if count > 1)
        row_count = len(rows)
        unique_canonical_dealers = len(counts)
        legacy_count = ad_count_history.get(snapshot_month)
        count_gap = None if legacy_count is None else unique_canonical_dealers - legacy_count
        delta = None if prior_count is None else unique_canonical_dealers - prior_count
        blank_city_rows = sum(1 for row in rows if not str(row["city"]).strip())
        blank_zip_rows = sum(1 for row in rows if not str(row["zip_code"]).strip())
        blank_address_rows = sum(1 for row in rows if not str(row["address"]).strip())

        flags = []
        if duplicate_rows:
            flags.append("duplicates_present")
        if count_gap not in (None, 0):
            flags.append("legacy_count_mismatch")
        if delta is not None and abs(delta) >= 10:
            flags.append("count_jump")
        if blank_city_rows:
            flags.append("blank_city")
        if blank_zip_rows:
            flags.append("blank_zip")
        if blank_address_rows:
            flags.append("blank_address")

        output.append(
            {
                "snapshot_month": snapshot_month,
                "row_count": row_count,
                "unique_canonical_dealers": unique_canonical_dealers,
                "duplicate_rows": duplicate_rows,
                "blank_city_rows": blank_city_rows,
                "blank_zip_rows": blank_zip_rows,
                "blank_address_rows": blank_address_rows,
                "legacy_ad_count": legacy_count,
                "count_gap_vs_legacy": count_gap,
                "month_over_month_delta": delta,
                "flags": ",".join(flags),
            }
        )
        prior_count = unique_canonical_dealers

    return output


def build_reference_summary(pricing_rows: Sequence[dict[str, object]]) -> list[dict[str, object]]:
    """Summarize grey-market history by Rolex reference."""

    grouped: dict[str, list[dict[str, object]]] = defaultdict(list)
    for row in pricing_rows:
        grouped[str(row["reference"])].append(dict(row))

    output: list[dict[str, object]] = []
    for reference in sorted(grouped):
        rows = sorted(grouped[reference], key=lambda row: str(row["snapshot_date"]))
        latest = rows[-1]
        trailing_rows = rows[-12:] if len(rows) >= 12 else rows
        output.append(
            {
                "reference": reference,
                "family": latest["family"],
                "observation_count": len(rows),
                "latest_date": latest["snapshot_date"],
                "latest_median_price_usd": safe_round(latest.get("median_price_usd")),
                "latest_markup_pct": safe_round(latest.get("markup_pct")),
                "latest_listing_count": safe_round(latest.get("listing_count")),
                "price_change_12w_pct": safe_round(
                    pct_change(rows[-12]["median_price_usd"], latest["median_price_usd"]) if len(rows) >= 12 else None
                ),
                "markup_change_12w_pct": safe_round(
                    latest["markup_pct"] - rows[-12]["markup_pct"] if len(rows) >= 12 and rows[-12]["markup_pct"] is not None else None
                ),
                "avg_listing_count_12w": safe_round(
                    mean([float(row["listing_count"]) for row in trailing_rows if row["listing_count"] is not None])
                ),
                "msrp_usd": latest["msrp_usd"],
            }
        )
    return output


def build_monthly_market_panel(
    ad_monthly_counts: Sequence[dict[str, object]],
    pricing_rows: Sequence[dict[str, object]],
) -> list[dict[str, object]]:
    """Join monthly AD counts with monthly pricing aggregates."""

    ad_lookup = {str(row["snapshot_month"]): int(row["ad_count_us"]) for row in ad_monthly_counts}
    sorted_ad_months = sorted(ad_lookup)
    ad_delta_lookup: dict[str, int | None] = {}
    prior_count: int | None = None
    for snapshot_month in sorted_ad_months:
        current_count = ad_lookup[snapshot_month]
        ad_delta_lookup[snapshot_month] = None if prior_count is None else current_count - prior_count
        prior_count = current_count

    grouped: dict[tuple[str, str], list[dict[str, object]]] = defaultdict(list)
    for row in pricing_rows:
        grouped[(str(row["snapshot_month"]), str(row["reference"]))].append(dict(row))

    output: list[dict[str, object]] = []
    for snapshot_month, reference in sorted(grouped):
        rows = grouped[(snapshot_month, reference)]
        family = str(rows[0]["family"])
        msrp_usd = rows[0]["msrp_usd"]
        avg_price = mean([float(row["median_price_usd"]) for row in rows if row["median_price_usd"] is not None])
        avg_listings = mean([float(row["listing_count"]) for row in rows if row["listing_count"] is not None])
        avg_markup = mean([float(row["markup_pct"]) for row in rows if row["markup_pct"] is not None])
        output.append(
            {
                "snapshot_month": snapshot_month,
                "reference": reference,
                "family": family,
                "msrp_usd": msrp_usd,
                "avg_median_price_usd": safe_round(avg_price),
                "avg_listing_count": safe_round(avg_listings),
                "avg_markup_pct": safe_round(avg_markup),
                "ad_count_us": ad_lookup.get(snapshot_month),
                "ad_count_mom_change": ad_delta_lookup.get(snapshot_month),
            }
        )

    by_reference: dict[str, list[dict[str, object]]] = defaultdict(list)
    for row in output:
        by_reference[str(row["reference"])].append(row)

    for rows in by_reference.values():
        rows.sort(key=lambda row: str(row["snapshot_month"]))
        prior_price: float | None = None
        for row in rows:
            current_price = row["avg_median_price_usd"]
            row["avg_price_mom_change_pct"] = safe_round(pct_change(prior_price, current_price))
            prior_price = current_price

    return sorted(output, key=lambda row: (str(row["snapshot_month"]), str(row["reference"])))


def build_correlation_summary(monthly_panel: Sequence[dict[str, object]]) -> list[dict[str, object]]:
    """Compute simple descriptive correlations between AD counts and pricing metrics."""

    by_reference: dict[str, list[dict[str, object]]] = defaultdict(list)
    for row in monthly_panel:
        if row["ad_count_us"] is None:
            continue
        by_reference[str(row["reference"])].append(dict(row))

    output: list[dict[str, object]] = []
    for reference in sorted(by_reference):
        rows = sorted(by_reference[reference], key=lambda row: str(row["snapshot_month"]))
        same_month_x = [float(row["ad_count_us"]) for row in rows if row["avg_markup_pct"] is not None]
        same_month_y = [float(row["avg_markup_pct"]) for row in rows if row["avg_markup_pct"] is not None]

        next_month_pairs = []
        for current_row, next_row in zip(rows, rows[1:]):
            if current_row["ad_count_mom_change"] is None or next_row["avg_price_mom_change_pct"] is None:
                continue
            next_month_pairs.append((float(current_row["ad_count_mom_change"]), float(next_row["avg_price_mom_change_pct"])))

        same_month_corr = correlation(same_month_x, same_month_y) if len(same_month_x) >= 3 and len(set(same_month_x)) > 1 and len(set(same_month_y)) > 1 else None
        next_month_corr = (
            correlation([pair[0] for pair in next_month_pairs], [pair[1] for pair in next_month_pairs])
            if len(next_month_pairs) >= 3
            and len({pair[0] for pair in next_month_pairs}) > 1
            and len({pair[1] for pair in next_month_pairs}) > 1
            else None
        )

        output.append(
            {
                "reference": reference,
                "family": rows[0]["family"],
                "overlap_months": len(rows),
                "same_month_ad_count_vs_markup_corr": safe_round(same_month_corr, 4),
                "ad_count_change_vs_next_month_price_change_corr": safe_round(next_month_corr, 4),
            }
        )
    return output


def build_average_markup_series(pricing_rows: Sequence[dict[str, object]]) -> list[tuple[str, float]]:
    """Build a weekly average markup series across all references."""

    grouped: dict[str, list[float]] = defaultdict(list)
    for row in pricing_rows:
        markup = row.get("markup_pct")
        if markup is None:
            continue
        grouped[str(row["snapshot_date"])].append(float(markup))

    return [(snapshot_date, mean(values) or 0.0) for snapshot_date, values in sorted(grouped.items())]


def write_line_chart_svg(path: Path, title: str, points: Sequence[tuple[str, float]], y_suffix: str = "") -> None:
    """Write a lightweight SVG line chart without external plotting dependencies."""

    width = 920
    height = 420
    left = 70
    right = 30
    top = 45
    bottom = 55
    plot_width = width - left - right
    plot_height = height - top - bottom

    if not points:
        path.write_text("<svg xmlns='http://www.w3.org/2000/svg' width='920' height='420'></svg>", encoding="utf-8")
        return

    values = [value for _, value in points]
    min_value = min(values)
    max_value = max(values)
    if math.isclose(min_value, max_value):
        min_value -= 1
        max_value += 1

    def x_position(index: int) -> float:
        if len(points) == 1:
            return left + plot_width / 2
        return left + (plot_width * index / (len(points) - 1))

    def y_position(value: float) -> float:
        ratio = (value - min_value) / (max_value - min_value)
        return top + plot_height - (ratio * plot_height)

    polyline_points = " ".join(f"{x_position(index):.2f},{y_position(value):.2f}" for index, (_, value) in enumerate(points))
    y_ticks = [min_value + ((max_value - min_value) * step / 4) for step in range(5)]
    x_labels = [points[0][0], points[len(points) // 2][0], points[-1][0]]

    svg = [
        f"<svg xmlns='http://www.w3.org/2000/svg' width='{width}' height='{height}' viewBox='0 0 {width} {height}'>",
        "<style>",
        "text { font-family: Arial, sans-serif; fill: #1f2937; }",
        ".title { font-size: 22px; font-weight: 700; }",
        ".axis { stroke: #9ca3af; stroke-width: 1; }",
        ".grid { stroke: #e5e7eb; stroke-width: 1; }",
        ".line { fill: none; stroke: #0f766e; stroke-width: 3; }",
        "</style>",
        f"<text class='title' x='{left}' y='28'>{title}</text>",
        f"<rect x='{left}' y='{top}' width='{plot_width}' height='{plot_height}' fill='#f8fafc' stroke='#e5e7eb' />",
    ]

    for tick_value in y_ticks:
        y = y_position(tick_value)
        svg.append(f"<line class='grid' x1='{left}' y1='{y:.2f}' x2='{left + plot_width}' y2='{y:.2f}' />")
        svg.append(f"<text x='12' y='{y + 4:.2f}' font-size='12'>{tick_value:.1f}{y_suffix}</text>")

    svg.extend(
        [
            f"<line class='axis' x1='{left}' y1='{top + plot_height}' x2='{left + plot_width}' y2='{top + plot_height}' />",
            f"<line class='axis' x1='{left}' y1='{top}' x2='{left}' y2='{top + plot_height}' />",
            f"<polyline class='line' points='{polyline_points}' />",
        ]
    )

    x_positions = [left, left + plot_width / 2, left + plot_width]
    for x_value, label in zip(x_positions, x_labels):
        svg.append(f"<text x='{x_value - 20:.2f}' y='{height - 18}' font-size='12'>{label}</text>")

    svg.append("</svg>")
    path.write_text("\n".join(svg), encoding="utf-8")


def write_market_report(
    path: Path,
    ad_monthly_counts: Sequence[dict[str, object]],
    state_counts: Sequence[dict[str, object]],
    reference_summary: Sequence[dict[str, object]],
    validation_summary: Sequence[dict[str, object]],
    correlation_summary: Sequence[dict[str, object]],
) -> None:
    """Write a concise markdown summary of the current historical dataset."""

    first_count = ad_monthly_counts[0]
    latest_count = ad_monthly_counts[-1]
    count_change = latest_count["ad_count_us"] - first_count["ad_count_us"]
    count_change_pct = pct_change(float(first_count["ad_count_us"]), float(latest_count["ad_count_us"]))

    latest_snapshot_month = str(latest_count["snapshot_month"])
    latest_state_counts = [row for row in state_counts if row["snapshot_month"] == latest_snapshot_month]
    latest_state_counts = sorted(latest_state_counts, key=lambda row: (-int(row["dealer_count"]), str(row["state"])))[:5]

    latest_reference = max(reference_summary, key=lambda row: float(row["latest_markup_pct"] or float("-inf")))
    weakest_reference = min(reference_summary, key=lambda row: float(row["latest_markup_pct"] or float("inf")))
    flagged_months = [row for row in validation_summary if row["flags"]]
    strongest_corr = (
        max(
            correlation_summary,
            key=lambda row: abs(float(row["same_month_ad_count_vs_markup_corr"]))
            if row["same_month_ad_count_vs_markup_corr"] is not None
            else -1,
        )
        if correlation_summary
        else None
    )

    lines = [
        "# Monthly Market Summary",
        "",
        "## Coverage",
        "",
        f"- AD snapshots cover **{ad_monthly_counts[0]['snapshot_month']}** through **{ad_monthly_counts[-1]['snapshot_month']}**.",
        f"- Grey-market pricing covers references through **{max(row['latest_date'] for row in reference_summary)}**.",
        "",
        "## Dealer Network Snapshot",
        "",
        f"- U.S. authorized dealer count moved from **{first_count['ad_count_us']}** to **{latest_count['ad_count_us']}** ({safe_round(count_change_pct)}%).",
        f"- Net dealer change over the observed AD period: **{count_change}** stores.",
        "",
        "Top states in the latest AD snapshot:",
    ]

    for row in latest_state_counts:
        lines.append(f"- {row['state']}: {row['dealer_count']} dealers")

    lines.extend(
        [
            "",
            "## Grey-Market Snapshot",
            "",
            f"- Highest latest premium in the tracked reference set: **{latest_reference['reference']} ({latest_reference['family']})** at **{latest_reference['latest_markup_pct']}%**.",
            f"- Lowest latest premium in the tracked reference set: **{weakest_reference['reference']} ({weakest_reference['family']})** at **{weakest_reference['latest_markup_pct']}%**.",
            "",
            "## Data Quality",
            "",
            f"- Snapshot months with at least one QC flag: **{len(flagged_months)}** of **{len(validation_summary)}**.",
            (
                f"- Largest descriptive same-month AD-count/markup correlation in the overlap period: **{strongest_corr['reference']}** at **{strongest_corr['same_month_ad_count_vs_markup_corr']}**."
                if strongest_corr is not None
                else "- No descriptive AD-count/markup correlation could be computed yet because the overlap period is too short."
            ),
            "- Correlation outputs are descriptive only and should not be treated as causal evidence.",
            "",
            "## Figures",
            "",
            "![U.S. AD Count](figures/us_ad_count.svg)",
            "",
            "![Average Grey Premium](figures/average_grey_markup.svg)",
        ]
    )

    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def configure_logging() -> None:
    """Configure a simple pipeline logger."""

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")


def relative_to_root(settings: Settings, path: Path) -> str:
    """Render a repo-relative path for manifests."""

    return str(path.relative_to(settings.root_dir)).replace("\\", "/")


def file_mtime_iso(path: Path) -> str | None:
    """Return file mtime in ISO 8601 UTC when the file exists."""

    if not path.exists():
        return None
    return dt.datetime.fromtimestamp(path.stat().st_mtime, tz=dt.timezone.utc).replace(microsecond=0).isoformat()


def append_pipeline_history(
    settings: Settings,
    pipeline_manifest: dict[str, object],
    history_csv_path: Path,
    history_manifest_path: Path,
) -> None:
    """Persist append-only pipeline run history for cross-run comparisons."""

    source_inputs = dict(pipeline_manifest.get("source_inputs", {}))
    coverage = dict(pipeline_manifest.get("coverage", {}))
    quality_summary = dict(pipeline_manifest.get("quality_summary", {}))
    outputs = dict(pipeline_manifest.get("outputs", {}))

    history_row = {
        "pipeline_run_at_utc": pipeline_manifest.get("pipeline_run_at_utc", ""),
        "schema_version": pipeline_manifest.get("schema_version", ""),
        "ad_snapshot_file_count": source_inputs.get("ad_snapshot_file_count", 0),
        "ad_snapshot_month_start": coverage.get("ad_snapshot_month_start", ""),
        "ad_snapshot_month_end": coverage.get("ad_snapshot_month_end", ""),
        "pricing_date_start": coverage.get("pricing_date_start", ""),
        "pricing_date_end": coverage.get("pricing_date_end", ""),
        "pricing_reference_count": coverage.get("pricing_reference_count", 0),
        "flagged_snapshot_months": quality_summary.get("flagged_snapshot_months", 0),
        "latest_ad_count_us": quality_summary.get("latest_ad_count_us", 0),
        "ad_snapshot_history_rows": dict(outputs.get("ad_snapshot_history", {})).get("record_count", 0),
        "ad_change_log_rows": dict(outputs.get("ad_change_log", {})).get("record_count", 0),
        "ad_state_monthly_counts_rows": dict(outputs.get("ad_state_monthly_counts", {})).get("record_count", 0),
        "grey_market_weekly_long_rows": dict(outputs.get("grey_market_weekly_long", {})).get("record_count", 0),
        "grey_reference_summary_rows": dict(outputs.get("grey_reference_summary", {})).get("record_count", 0),
        "monthly_market_panel_rows": dict(outputs.get("monthly_market_panel", {})).get("record_count", 0),
        "validation_summary_rows": dict(outputs.get("validation_summary", {})).get("record_count", 0),
        "monthly_network_pricing_correlation_rows": dict(outputs.get("monthly_network_pricing_correlation", {})).get(
            "record_count", 0
        ),
        "manifest_file": relative_to_root(settings, settings.processed_dir / "pipeline_run_manifest.json"),
    }

    existing_rows = read_csv_rows(history_csv_path) if history_csv_path.exists() else []
    existing_rows = [dict(row) for row in existing_rows]
    if not any(str(row.get("pipeline_run_at_utc", "")) == str(history_row["pipeline_run_at_utc"]) for row in existing_rows):
        existing_rows.append(history_row)
        existing_rows.sort(key=lambda row: str(row["pipeline_run_at_utc"]))
        write_csv(history_csv_path, existing_rows, list(history_row.keys()))

    write_json(history_manifest_path, pipeline_manifest)


def build_pipeline_manifest(
    settings: Settings,
    ad_rows: Sequence[dict[str, object]],
    pricing_rows: Sequence[dict[str, object]],
    ad_monthly_counts: Sequence[dict[str, object]],
    validation_summary: Sequence[dict[str, object]],
    output_files: dict[str, Path],
    output_record_counts: dict[str, int | None],
) -> dict[str, object]:
    """Build a persisted manifest for the historical processing pipeline."""

    ad_source_files = sorted({str(row["source_file"]) for row in ad_rows})
    ad_snapshot_months = sorted({str(row["snapshot_month"]) for row in ad_rows})
    pricing_dates = sorted({str(row["snapshot_date"]) for row in pricing_rows})
    pricing_references = sorted({str(row["reference"]) for row in pricing_rows})
    flagged_months = [row for row in validation_summary if row["flags"]]

    return {
        "pipeline_run_at_utc": dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat(),
        "schema_version": settings.schema_version,
        "source_inputs": {
            "ad_snapshot_directory": relative_to_root(settings, settings.ad_list_dir),
            "ad_snapshot_file_count": len(ad_source_files),
            "ad_snapshot_files": ad_source_files,
            "ad_count_file": relative_to_root(settings, settings.ad_count_file),
            "pricing_file": relative_to_root(settings, settings.pricing_file),
            "reference_catalog_file": relative_to_root(settings, settings.reference_catalog_file),
            "ad_snapshot_directory_mtime_utc": file_mtime_iso(settings.ad_list_dir),
            "ad_count_file_mtime_utc": file_mtime_iso(settings.ad_count_file),
            "pricing_file_mtime_utc": file_mtime_iso(settings.pricing_file),
            "reference_catalog_mtime_utc": file_mtime_iso(settings.reference_catalog_file),
        },
        "coverage": {
            "ad_snapshot_month_start": ad_snapshot_months[0] if ad_snapshot_months else None,
            "ad_snapshot_month_end": ad_snapshot_months[-1] if ad_snapshot_months else None,
            "pricing_date_start": pricing_dates[0] if pricing_dates else None,
            "pricing_date_end": pricing_dates[-1] if pricing_dates else None,
            "pricing_reference_count": len(pricing_references),
        },
        "quality_summary": {
            "flagged_snapshot_months": len(flagged_months),
            "latest_ad_count_us": ad_monthly_counts[-1]["ad_count_us"] if ad_monthly_counts else None,
        },
        "outputs": {
            name: {
                "path": relative_to_root(settings, path),
                "mtime_utc": file_mtime_iso(path),
                "record_count": output_record_counts.get(name),
            }
            for name, path in output_files.items()
        },
    }


def run_pipeline(settings: Settings) -> dict[str, object]:
    """Execute the historical data processing pipeline."""

    configure_logging()
    settings.ensure_directories()
    LOGGER.info("Loading legacy AD and pricing data")
    ad_rows = load_ad_history(settings)
    ad_count_history = load_ad_count_history(settings.ad_count_file)
    pricing_rows = load_pricing_history(settings)

    LOGGER.info("Building analytical datasets")
    ad_monthly_counts = build_ad_monthly_counts(ad_rows)
    state_counts = build_ad_state_counts(ad_rows)
    change_log = build_ad_change_log(ad_rows)
    validation_summary = build_validation_summary(ad_rows, ad_count_history)
    reference_summary = build_reference_summary(pricing_rows)
    monthly_panel = build_monthly_market_panel(ad_monthly_counts, pricing_rows)
    correlation_summary = build_correlation_summary(monthly_panel)

    LOGGER.info("Writing datasets and reports")
    ad_snapshot_history_path = settings.processed_dir / "ad_snapshot_history.csv"
    ad_change_log_path = settings.processed_dir / "ad_change_log.csv"
    ad_state_counts_path = settings.processed_dir / "ad_state_monthly_counts.csv"
    grey_weekly_path = settings.processed_dir / "grey_market_weekly_long.csv"
    grey_summary_path = settings.processed_dir / "grey_reference_summary.csv"
    monthly_panel_path = settings.processed_dir / "monthly_market_panel.csv"
    validation_summary_path = settings.quality_dir / "validation_summary.csv"
    correlation_summary_path = settings.quality_dir / "monthly_network_pricing_correlation.csv"
    ad_count_chart_path = settings.figure_dir / "us_ad_count.svg"
    markup_chart_path = settings.figure_dir / "average_grey_markup.svg"
    market_report_path = settings.report_dir / "monthly_market_summary.md"
    pipeline_manifest_path = settings.processed_dir / "pipeline_run_manifest.json"
    schema_metadata_path = settings.processed_dir / "schema_version.json"
    pipeline_history_dir = settings.processed_dir / "history"
    pipeline_history_csv_path = settings.processed_dir / "pipeline_run_history.csv"

    write_csv(
        ad_snapshot_history_path,
        ad_rows,
        [
            "snapshot_month",
            "dealer_name",
            "address",
            "city",
            "state",
            "zip_code",
            "legacy_id",
            "normalized_name",
            "normalized_address",
            "normalized_city",
            "normalized_state",
            "canonical_id",
            "source_file",
            "source_row_number",
        ],
    )
    write_csv(
        ad_change_log_path,
        change_log,
        [
            "change_month",
            "prior_snapshot_month",
            "change_type",
            "dealer_name",
            "state",
            "city",
            "prior_canonical_id",
            "current_canonical_id",
            "prior_address",
            "current_address",
            "detail",
        ],
    )
    write_csv(
        ad_state_counts_path,
        state_counts,
        ["snapshot_month", "state", "dealer_count"],
    )
    write_csv(
        grey_weekly_path,
        pricing_rows,
        [
            "snapshot_date",
            "snapshot_month",
            "reference",
            "family",
            "msrp_usd",
            "median_price_usd",
            "listing_count",
            "markup_pct",
        ],
    )
    write_csv(
        grey_summary_path,
        reference_summary,
        [
            "reference",
            "family",
            "observation_count",
            "latest_date",
            "latest_median_price_usd",
            "latest_markup_pct",
            "latest_listing_count",
            "price_change_12w_pct",
            "markup_change_12w_pct",
            "avg_listing_count_12w",
            "msrp_usd",
        ],
    )
    write_csv(
        monthly_panel_path,
        monthly_panel,
        [
            "snapshot_month",
            "reference",
            "family",
            "msrp_usd",
            "avg_median_price_usd",
            "avg_listing_count",
            "avg_markup_pct",
            "ad_count_us",
            "ad_count_mom_change",
            "avg_price_mom_change_pct",
        ],
    )
    write_csv(
        validation_summary_path,
        validation_summary,
        [
            "snapshot_month",
            "row_count",
            "unique_canonical_dealers",
            "duplicate_rows",
            "blank_city_rows",
            "blank_zip_rows",
            "blank_address_rows",
            "legacy_ad_count",
            "count_gap_vs_legacy",
            "month_over_month_delta",
            "flags",
        ],
    )
    write_csv(
        correlation_summary_path,
        correlation_summary,
        [
            "reference",
            "family",
            "overlap_months",
            "same_month_ad_count_vs_markup_corr",
            "ad_count_change_vs_next_month_price_change_corr",
        ],
    )

    write_line_chart_svg(
        ad_count_chart_path,
        "U.S. Rolex Authorized Dealer Count",
        [(str(row["snapshot_month"]), float(row["ad_count_us"])) for row in ad_monthly_counts],
    )
    write_line_chart_svg(
        markup_chart_path,
        "Average Grey-Market Premium Across Tracked References",
        build_average_markup_series(pricing_rows),
        y_suffix="%",
    )
    write_market_report(
        market_report_path,
        ad_monthly_counts,
        state_counts,
        reference_summary,
        validation_summary,
        correlation_summary,
    )

    output_files = {
        "ad_snapshot_history": ad_snapshot_history_path,
        "ad_change_log": ad_change_log_path,
        "ad_state_monthly_counts": ad_state_counts_path,
        "grey_market_weekly_long": grey_weekly_path,
        "grey_reference_summary": grey_summary_path,
        "monthly_market_panel": monthly_panel_path,
        "validation_summary": validation_summary_path,
        "monthly_network_pricing_correlation": correlation_summary_path,
        "us_ad_count_chart": ad_count_chart_path,
        "average_grey_markup_chart": markup_chart_path,
        "monthly_market_report": market_report_path,
    }
    output_record_counts = {
        "ad_snapshot_history": len(ad_rows),
        "ad_change_log": len(change_log),
        "ad_state_monthly_counts": len(state_counts),
        "grey_market_weekly_long": len(pricing_rows),
        "grey_reference_summary": len(reference_summary),
        "monthly_market_panel": len(monthly_panel),
        "validation_summary": len(validation_summary),
        "monthly_network_pricing_correlation": len(correlation_summary),
        "us_ad_count_chart": None,
        "average_grey_markup_chart": None,
        "monthly_market_report": None,
    }
    pipeline_manifest = build_pipeline_manifest(
        settings=settings,
        ad_rows=ad_rows,
        pricing_rows=pricing_rows,
        ad_monthly_counts=ad_monthly_counts,
        validation_summary=validation_summary,
        output_files=output_files,
        output_record_counts=output_record_counts,
    )
    write_json(pipeline_manifest_path, pipeline_manifest)
    pipeline_history_manifest_path = (
        pipeline_history_dir
        / f"pipeline_run_manifest_{str(pipeline_manifest['pipeline_run_at_utc']).replace(':', '').replace('-', '').replace('+00:00', 'Z')}.json"
    )
    append_pipeline_history(
        settings=settings,
        pipeline_manifest=pipeline_manifest,
        history_csv_path=pipeline_history_csv_path,
        history_manifest_path=pipeline_history_manifest_path,
    )
    write_json(
        schema_metadata_path,
        {
            "schema_version": settings.schema_version,
            "generated_at_utc": dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat(),
            "pipeline_manifest_file": relative_to_root(settings, pipeline_manifest_path),
            "pipeline_history_file": relative_to_root(settings, pipeline_history_csv_path),
        },
    )

    summary = {
        "ad_snapshot_rows": len(ad_rows),
        "ad_snapshot_months": len({row["snapshot_month"] for row in ad_rows}),
        "pricing_rows": len(pricing_rows),
        "pricing_references": len({row["reference"] for row in pricing_rows}),
        "latest_ad_snapshot_month": ad_monthly_counts[-1]["snapshot_month"] if ad_monthly_counts else None,
        "latest_pricing_date": max((row["snapshot_date"] for row in pricing_rows), default=None),
        "pipeline_manifest_file": relative_to_root(settings, pipeline_manifest_path),
        "pipeline_history_file": relative_to_root(settings, pipeline_history_csv_path),
        "schema_metadata_file": relative_to_root(settings, schema_metadata_path),
    }
    LOGGER.info("Pipeline complete: %s", summary)
    return summary
