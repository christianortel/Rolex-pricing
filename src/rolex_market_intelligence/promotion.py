"""Append-safe promotion workflows for live collector outputs."""

from __future__ import annotations

from collections import OrderedDict
from pathlib import Path
import csv
import datetime as dt
import json

from .normalize import build_canonical_id, clean_text, normalize_state, normalize_zip
from .settings import Settings


AD_INTERIM_FIELDS = [
    "collected_at_utc",
    "detail_url",
    "dealer_name",
    "address",
    "city",
    "state",
    "zip_code",
    "phone",
    "website",
    "source_title",
    "parser_diagnostics",
]

PRICING_INTERIM_FIELDS = [
    "collected_at_utc",
    "reference",
    "family",
    "msrp_usd",
    "detail_url",
    "source_title",
    "observed_price_sample_count",
    "listing_count",
    "median_price_usd",
    "markup_pct",
    "min_price_usd",
    "max_price_usd",
    "sample_prices_preview",
    "parser_diagnostics",
]


class PromotionError(RuntimeError):
    """Raised when a live snapshot cannot be promoted safely."""


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    """Read a CSV file into dictionaries."""

    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def read_json(path: Path) -> dict[str, object]:
    """Read a JSON file into a dictionary."""

    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, object]]) -> None:
    """Write CSV rows deterministically."""

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def write_json(path: Path, payload: dict[str, object]) -> None:
    """Write JSON payloads deterministically."""

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def backup_file(source: Path, backup_root: Path, prefix: str, timestamp: str) -> Path | None:
    """Copy an existing file into the raw backup area before overwriting."""

    if not source.exists():
        return None
    backup_root.mkdir(parents=True, exist_ok=True)
    backup_path = backup_root / f"{prefix}_{timestamp}{source.suffix}"
    backup_path.write_bytes(source.read_bytes())
    return backup_path


def latest_collector_paths(settings: Settings, collector_name: str) -> tuple[Path, Path]:
    """Resolve the latest interim snapshot and manifest paths for a collector."""

    if collector_name == "rolex_ad":
        return (
            settings.interim_dir / "rolex_ad_live_snapshot.csv",
            settings.interim_dir / "rolex_ad_latest_manifest.json",
        )
    if collector_name == "chrono24_pricing":
        return (
            settings.interim_dir / "chrono24_pricing_live_snapshot.csv",
            settings.interim_dir / "chrono24_pricing_latest_manifest.json",
        )
    raise ValueError(f"Unsupported collector: {collector_name}")


def ensure_manifest_ready(manifest: dict[str, object], collector_name: str) -> None:
    """Ensure a collector manifest is suitable for promotion."""

    if manifest.get("collector") != collector_name:
        raise PromotionError(f"Manifest collector mismatch: expected {collector_name}")
    if manifest.get("aborted_early"):
        raise PromotionError(f"{collector_name} collector aborted early: {manifest.get('abort_reason', '')}")
    if int(manifest.get("fetch_success_count", 0)) <= 0:
        raise PromotionError(f"{collector_name} collector had no successful fetches")
    if int(manifest.get("parsed_record_count", 0)) <= 0:
        raise PromotionError(f"{collector_name} collector produced no parsed records")


def parse_iso_timestamp(value: str) -> dt.datetime:
    """Parse an ISO 8601 timestamp into a timezone-aware datetime."""

    return dt.datetime.fromisoformat(value)


def normalize_ad_promotion_rows(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    """Normalize live AD rows into the legacy snapshot schema."""

    output: list[dict[str, str]] = []
    seen: set[str] = set()
    for row in rows:
        if clean_text(row.get("parser_diagnostics", "")):
            raise PromotionError(
                f"AD promotion row for {clean_text(row.get('dealer_name', '')) or '<unknown>'} has parser diagnostics"
            )
        name = clean_text(row.get("dealer_name", ""))
        address = clean_text(row.get("address", ""))
        city = clean_text(row.get("city", ""))
        state = normalize_state(row.get("state", ""))
        zip_code = normalize_zip(row.get("zip_code", ""))
        if not all([name, address, city, state, zip_code]):
            raise PromotionError(f"AD promotion row missing required location fields for dealer {name or '<unknown>'}")
        canonical_id = build_canonical_id(name, address, city, state, zip_code)
        if canonical_id in seen:
            continue
        seen.add(canonical_id)
        output.append(
            {
                "Name": name,
                "Address": address,
                "City": city,
                "State": state,
                "Zip": zip_code,
                "ID": canonical_id,
            }
        )
    output.sort(key=lambda row: (row["State"], row["City"], row["Name"], row["Address"]))
    return output


def update_ad_count_file(path: Path, snapshot_date: dt.date, dealer_count: int) -> str:
    """Insert or update a monthly AD count line idempotently."""

    lines: OrderedDict[str, int] = OrderedDict()
    if path.exists():
        for raw_line in path.read_text(encoding="utf-8", errors="replace").splitlines():
            if ":" not in raw_line:
                continue
            month_year, count_text = raw_line.split(":", 1)
            lines[month_year.strip()] = int(count_text.strip())

    key = f"{snapshot_date.month}/{snapshot_date.year}"
    prior_value = lines.get(key)
    if prior_value == dealer_count:
        return "unchanged"

    lines[key] = dealer_count
    sorted_items = sorted(
        lines.items(),
        key=lambda item: (int(item[0].split("/")[1]), int(item[0].split("/")[0])),
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "\n".join(f"{month_year}: {count}" for month_year, count in sorted_items) + "\n",
        encoding="utf-8",
    )
    return "updated" if prior_value is not None else "inserted"


def promote_ad_snapshot(settings: Settings) -> dict[str, object]:
    """Promote the latest live AD snapshot into the historical monthly files."""

    snapshot_path, manifest_path = latest_collector_paths(settings, "rolex_ad")
    if not snapshot_path.exists() or not manifest_path.exists():
        raise PromotionError("Latest Rolex AD interim snapshot or manifest is missing")

    manifest = read_json(manifest_path)
    ensure_manifest_ready(manifest, "rolex_ad")
    live_rows = read_csv_rows(snapshot_path)
    promoted_rows = normalize_ad_promotion_rows(live_rows)
    if not promoted_rows:
        raise PromotionError("No promotable Rolex AD rows found")

    collected_at = parse_iso_timestamp(str(manifest["collected_at_utc"]))
    snapshot_date = dt.date(collected_at.year, collected_at.month, 1)
    target_file = settings.ad_list_dir / f"Rolex_AD_List_{snapshot_date.month}_{snapshot_date.year}.csv"
    backup_root = settings.raw_dir / "history_backups" / "ad_list"
    timestamp = collected_at.strftime("%Y%m%dT%H%M%SZ")

    if target_file.exists():
        existing_rows = read_csv_rows(target_file)
        if existing_rows == promoted_rows:
            ad_count_status = update_ad_count_file(settings.ad_count_file, snapshot_date, len(promoted_rows))
            promotion_manifest = {
                "status": "noop",
                "target_file": str(target_file.relative_to(settings.root_dir)).replace("\\", "/"),
                "snapshot_month": snapshot_date.isoformat(),
                "dealer_count": len(promoted_rows),
                "ad_count_status": ad_count_status,
            }
            write_json(settings.interim_dir / "rolex_ad_promotion_latest.json", promotion_manifest)
            return promotion_manifest
        backup_file(target_file, backup_root, f"Rolex_AD_List_{snapshot_date.month}_{snapshot_date.year}", timestamp)

    write_csv(target_file, ["Name", "Address", "City", "State", "Zip", "ID"], promoted_rows)
    ad_count_status = update_ad_count_file(settings.ad_count_file, snapshot_date, len(promoted_rows))
    promotion_manifest = {
        "promoted_at_utc": dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat(),
        "source_manifest_file": str(manifest_path.relative_to(settings.root_dir)).replace("\\", "/"),
        "target_file": str(target_file.relative_to(settings.root_dir)).replace("\\", "/"),
        "snapshot_month": snapshot_date.isoformat(),
        "dealer_count": len(promoted_rows),
        "ad_count_status": ad_count_status,
    }
    write_json(settings.interim_dir / "rolex_ad_promotion_latest.json", promotion_manifest)
    return promotion_manifest


def build_pricing_history_row(
    live_rows: list[dict[str, str]],
    history_fieldnames: list[str],
    snapshot_date: dt.date,
) -> dict[str, object]:
    """Transform live pricing rows into the legacy wide-history row format."""

    row_map = {clean_text(row["reference"]): row for row in live_rows}
    output: dict[str, object] = {"Date": f"{snapshot_date.month:02d}/{snapshot_date.day:02d}/{snapshot_date.year}"}
    for field in history_fieldnames:
        if field == "Date":
            continue
        if field.endswith(" Listings"):
            reference = field[: -len(" Listings")]
            value = row_map.get(reference, {}).get("listing_count", "")
            output[field] = value
        elif field.endswith(" Markup"):
            reference = field[: -len(" Markup")]
            value = row_map.get(reference, {}).get("markup_pct", "")
            output[field] = value
        else:
            value = row_map.get(field, {}).get("median_price_usd", "")
            output[field] = value
    return output


def promote_pricing_snapshot(settings: Settings) -> dict[str, object]:
    """Promote the latest live pricing snapshot into the historical weekly CSV."""

    snapshot_path, manifest_path = latest_collector_paths(settings, "chrono24_pricing")
    if not snapshot_path.exists() or not manifest_path.exists():
        raise PromotionError("Latest Chrono24 pricing interim snapshot or manifest is missing")

    manifest = read_json(manifest_path)
    ensure_manifest_ready(manifest, "chrono24_pricing")
    live_rows = read_csv_rows(snapshot_path)
    if not live_rows:
        raise PromotionError("No promotable Chrono24 pricing rows found")

    for row in live_rows:
        if row.get("parser_diagnostics"):
            raise PromotionError(f"Pricing row for {row.get('reference', '<unknown>')} has parser diagnostics")
        if not row.get("reference") or not row.get("median_price_usd"):
            raise PromotionError("Pricing snapshot missing required reference or median price fields")

    with settings.pricing_file.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        fieldnames = reader.fieldnames or []
        history_rows = list(reader)
    if not fieldnames:
        raise PromotionError("Pricing history file is missing a header row")

    collected_at = parse_iso_timestamp(str(manifest["collected_at_utc"]))
    snapshot_date = collected_at.date()
    new_row = build_pricing_history_row(live_rows, fieldnames, snapshot_date)
    existing_by_date = {row["Date"]: row for row in history_rows}
    key = new_row["Date"]

    if key in existing_by_date:
        if existing_by_date[key] == {field: str(new_row.get(field, "")) for field in fieldnames}:
            promotion_manifest = {
                "status": "noop",
                "target_file": str(settings.pricing_file.relative_to(settings.root_dir)).replace("\\", "/"),
                "snapshot_date": snapshot_date.isoformat(),
            }
            write_json(settings.interim_dir / "chrono24_pricing_promotion_latest.json", promotion_manifest)
            return promotion_manifest
        backup_root = settings.raw_dir / "history_backups" / "pricing"
        backup_file(settings.pricing_file, backup_root, "Weekly_Median_Prices", collected_at.strftime("%Y%m%dT%H%M%SZ"))
        history_rows = [row for row in history_rows if row["Date"] != key]

    history_rows.append({field: str(new_row.get(field, "")) for field in fieldnames})
    history_rows.sort(
        key=lambda row: dt.datetime.strptime(row["Date"], "%m/%d/%Y").date()
    )
    write_csv(settings.pricing_file, fieldnames, history_rows)

    promotion_manifest = {
        "promoted_at_utc": dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat(),
        "source_manifest_file": str(manifest_path.relative_to(settings.root_dir)).replace("\\", "/"),
        "target_file": str(settings.pricing_file.relative_to(settings.root_dir)).replace("\\", "/"),
        "snapshot_date": snapshot_date.isoformat(),
        "reference_count": len(live_rows),
    }
    write_json(settings.interim_dir / "chrono24_pricing_promotion_latest.json", promotion_manifest)
    return promotion_manifest
