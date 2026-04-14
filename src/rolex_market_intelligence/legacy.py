"""Legacy source loaders for historical Rolex project inputs."""

from __future__ import annotations

from pathlib import Path
from collections import Counter, defaultdict
import csv
import datetime as dt
import json
import re

from .normalize import build_canonical_id, canonical_token, normalize_dealer_row, normalize_state
from .settings import Settings


AD_FILENAME_PATTERN = re.compile(r"Rolex_AD_List_(\d{1,2})_(\d{4})\.csv$")
REFERENCE_PATTERN = re.compile(r"^[0-9A-Z]{5,12}$")


def parse_snapshot_date(filename: str) -> dt.date:
    """Parse a monthly AD snapshot date from a legacy filename."""

    match = AD_FILENAME_PATTERN.fullmatch(filename)
    if not match:
        raise ValueError(f"Unrecognized AD snapshot filename: {filename}")
    month, year = (int(part) for part in match.groups())
    return dt.date(year, month, 1)


def parse_legacy_date(value: str) -> dt.date:
    """Parse legacy m/d/yyyy dates without relying on platform-specific strptime behavior."""

    month_text, day_text, year_text = value.split("/")
    return dt.date(int(year_text), int(month_text), int(day_text))


def parse_float(value: str | None) -> float | None:
    """Convert a numeric string to float when possible."""

    if value is None:
        return None
    text = value.strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def load_reference_catalog(path: Path) -> dict[str, dict[str, object]]:
    """Load reference metadata from JSON."""

    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def repair_dealer_history(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    """Repair recurring blank or malformed location fields using stable cross-snapshot matches."""

    candidates: dict[tuple[str, str], dict[str, Counter[str]]] = defaultdict(
        lambda: {"city": Counter(), "state": Counter()}
    )

    for row in rows:
        key = (str(row["normalized_name"]), str(row["zip_code"]))
        city = str(row["city"]).strip()
        state = str(row["state"]).strip()
        if city:
            candidates[key]["city"][city] += 1
        if state and len(state) > 1:
            candidates[key]["state"][state] += 1

    for row in rows:
        key = (str(row["normalized_name"]), str(row["zip_code"]))
        city = str(row["city"]).strip()
        state = str(row["state"]).strip()

        if not city and candidates[key]["city"]:
            row["city"] = candidates[key]["city"].most_common(1)[0][0]
            row["normalized_city"] = canonical_token(row["city"])

        if (not state or len(state) == 1) and candidates[key]["state"]:
            row["state"] = normalize_state(candidates[key]["state"].most_common(1)[0][0])
            row["normalized_state"] = canonical_token(row["state"])

        row["canonical_id"] = build_canonical_id(
            str(row["dealer_name"]),
            str(row["address"]),
            str(row["city"]),
            str(row["state"]),
            str(row["zip_code"]),
        )

    return rows


def load_ad_history(settings: Settings) -> list[dict[str, object]]:
    """Load and normalize all legacy monthly AD snapshots."""

    rows: list[dict[str, object]] = []
    snapshot_files = sorted(settings.ad_list_dir.glob("Rolex_AD_List_*.csv"), key=lambda item: parse_snapshot_date(item.name))
    for snapshot_file in snapshot_files:
        snapshot_date = parse_snapshot_date(snapshot_file.name)
        with snapshot_file.open("r", encoding="utf-8-sig", errors="replace", newline="") as handle:
            reader = csv.DictReader(handle)
            for index, row in enumerate(reader, start=1):
                normalized = normalize_dealer_row(row)
                normalized.update(
                    {
                        "snapshot_month": snapshot_date.isoformat(),
                        "source_file": snapshot_file.name,
                        "source_row_number": index,
                    }
                )
                rows.append(normalized)
    return repair_dealer_history(rows)


def load_ad_count_history(path: Path) -> dict[str, int]:
    """Load the legacy AD count text file into a monthly dictionary."""

    counts: dict[str, int] = {}
    if not path.exists():
        return counts

    with path.open("r", encoding="utf-8", errors="replace") as handle:
        for raw_line in handle:
            line = raw_line.strip()
            if not line or ":" not in line:
                continue
            month_year, count_text = line.split(":", 1)
            month_text, year_text = month_year.strip().split("/")
            snapshot_date = dt.date(int(year_text), int(month_text), 1)
            counts[snapshot_date.isoformat()] = int(count_text.strip())
    return counts


def load_pricing_history(settings: Settings) -> list[dict[str, object]]:
    """Load legacy weekly pricing data and reshape it to long form."""

    reference_catalog = load_reference_catalog(settings.reference_catalog_file)
    rows: list[dict[str, object]] = []

    with settings.pricing_file.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        fieldnames = reader.fieldnames or []
        references = [
            name
            for name in fieldnames
            if name != "Date" and not name.endswith(" Listings") and not name.endswith(" Markup") and REFERENCE_PATTERN.fullmatch(name)
        ]

        for raw_row in reader:
            snapshot_date = parse_legacy_date(raw_row["Date"])
            snapshot_month = dt.date(snapshot_date.year, snapshot_date.month, 1).isoformat()
            for reference in references:
                median_price = parse_float(raw_row.get(reference))
                listing_count = parse_float(raw_row.get(f"{reference} Listings"))
                markup_pct = parse_float(raw_row.get(f"{reference} Markup"))
                if median_price is None and listing_count is None and markup_pct is None:
                    continue
                metadata = reference_catalog.get(reference, {})
                rows.append(
                    {
                        "snapshot_date": snapshot_date.isoformat(),
                        "snapshot_month": snapshot_month,
                        "reference": reference,
                        "family": metadata.get("family", "Unknown"),
                        "msrp_usd": metadata.get("msrp_usd"),
                        "median_price_usd": median_price,
                        "listing_count": listing_count,
                        "markup_pct": markup_pct,
                    }
                )
    return rows
