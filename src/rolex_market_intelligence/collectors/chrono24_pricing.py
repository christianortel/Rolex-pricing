"""Live Chrono24 pricing collector with raw snapshot capture."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
from statistics import median
from urllib.parse import urlparse
from urllib.request import Request, urlopen
import csv
import datetime as dt
import hashlib
import json
import logging
import re
import time

from ..legacy import load_reference_catalog
from ..normalize import clean_text
from ..settings import Settings


LOGGER = logging.getLogger(__name__)
CHRONO24_REF_URL_TEMPLATE = "https://www.chrono24.com/rolex/ref-{reference_lower}.htm"
LISTING_COUNT_PATTERN = re.compile(r"(\d[\d,]*)\s+listings including promoted listings", re.IGNORECASE)
PRICE_PATTERN = re.compile(r"\$([0-9][0-9,]*(?:\.[0-9]{2})?)")
TITLE_PATTERN = re.compile(r"<title>\s*(.*?)\s*</title>", re.IGNORECASE | re.DOTALL)


@dataclass(slots=True)
class FetchRecord:
    """Metadata about a fetched Chrono24 reference page."""

    url: str
    relative_path: str
    status_code: int | None
    ok: bool
    error: str
    content_type: str
    bytes_written: int
    sha256_hex: str
    fetched_at_utc: str


def slugify_url_path(url: str) -> str:
    """Create a filesystem-safe identifier from a URL path."""

    parsed = urlparse(url)
    path = parsed.path.strip("/") or "root"
    return re.sub(r"[^a-zA-Z0-9/_-]", "_", path)


def parse_price_text(text: str) -> float:
    """Convert a dollar-denominated string to float."""

    return float(text.replace(",", ""))


def dedupe_preserve_order(values: list[float]) -> list[float]:
    """Deduplicate values while preserving the original order."""

    seen: set[float] = set()
    output: list[float] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        output.append(value)
    return output


def parse_chrono24_reference_html(
    html: str,
    reference: str,
    family: str,
    msrp_usd: int | None,
    collected_at_utc: str,
    detail_url: str,
) -> dict[str, object]:
    """Parse a Chrono24 reference page into a pricing snapshot."""

    cleaned_html = clean_text(html)
    title_match = TITLE_PATTERN.search(html)
    title = clean_text(title_match.group(1)) if title_match else ""
    observed_prices = dedupe_preserve_order([parse_price_text(match) for match in PRICE_PATTERN.findall(cleaned_html)])
    listing_count_match = LISTING_COUNT_PATTERN.search(cleaned_html)
    listing_count = int(listing_count_match.group(1).replace(",", "")) if listing_count_match else None
    median_price_usd = round(float(median(observed_prices)), 2) if observed_prices else None
    markup_pct = None
    if median_price_usd is not None and msrp_usd not in (None, 0):
        markup_pct = round(((median_price_usd / msrp_usd) - 1.0) * 100.0, 2)

    diagnostics = []
    if listing_count is None:
        diagnostics.append("missing_listing_count")
    if not observed_prices:
        diagnostics.append("missing_price_samples")

    return {
        "collected_at_utc": collected_at_utc,
        "reference": reference,
        "family": family,
        "msrp_usd": msrp_usd,
        "detail_url": detail_url,
        "source_title": title,
        "observed_price_sample_count": len(observed_prices),
        "listing_count": listing_count,
        "median_price_usd": median_price_usd,
        "markup_pct": markup_pct,
        "min_price_usd": round(min(observed_prices), 2) if observed_prices else None,
        "max_price_usd": round(max(observed_prices), 2) if observed_prices else None,
        "sample_prices_preview": "|".join(str(int(value) if value.is_integer() else value) for value in observed_prices[:10]),
        "parser_diagnostics": ",".join(diagnostics),
    }


class Chrono24PricingCollector:
    """Collect current Chrono24 pricing snapshots for tracked Rolex references."""

    def __init__(self, settings: Settings, logger: logging.Logger | None = None) -> None:
        self.settings = settings
        self.logger = logger or LOGGER

    def fetch_url(self, url: str, raw_dir: Path) -> tuple[FetchRecord, str]:
        """Fetch a URL and persist the raw response body."""

        fetched_at = dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat()
        relative_path = f"{slugify_url_path(url)}.html"
        output_path = raw_dir / relative_path
        output_path.parent.mkdir(parents=True, exist_ok=True)
        request = Request(
            url,
            headers={
                "User-Agent": self.settings.user_agent,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            },
        )

        try:
            with urlopen(request, timeout=self.settings.request_timeout_seconds) as response:
                body = response.read()
                output_path.write_bytes(body)
                record = FetchRecord(
                    url=url,
                    relative_path=str(relative_path).replace("\\", "/"),
                    status_code=getattr(response, "status", None),
                    ok=True,
                    error="",
                    content_type=response.headers.get("Content-Type", ""),
                    bytes_written=len(body),
                    sha256_hex=hashlib.sha256(body).hexdigest(),
                    fetched_at_utc=fetched_at,
                )
                return record, body.decode("utf-8", errors="replace")
        except Exception as exc:  # pragma: no cover - network errors vary by environment
            output_path.write_text("", encoding="utf-8")
            record = FetchRecord(
                url=url,
                relative_path=str(relative_path).replace("\\", "/"),
                status_code=None,
                ok=False,
                error=f"{exc.__class__.__name__}: {exc}",
                content_type="",
                bytes_written=0,
                sha256_hex="",
                fetched_at_utc=fetched_at,
            )
            return record, ""

    def collect(self) -> dict[str, object]:
        """Run the live Chrono24 pricing collector and write raw/interim artifacts."""

        reference_catalog = load_reference_catalog(self.settings.reference_catalog_file)
        collected_at = dt.datetime.now(dt.timezone.utc).replace(microsecond=0)
        run_id = collected_at.strftime("%Y%m%dT%H%M%SZ")
        raw_dir = self.settings.raw_dir / "chrono24_pricing" / run_id
        raw_dir.mkdir(parents=True, exist_ok=True)
        interim_file = self.settings.interim_dir / "chrono24_pricing_live_snapshot.csv"
        manifest_file = raw_dir / "manifest.json"

        fetch_records: list[FetchRecord] = []
        parsed_records: list[dict[str, object]] = []
        aborted_early = False
        abort_reason = ""
        consecutive_failures = 0

        for reference, metadata in sorted(reference_catalog.items()):
            detail_url = CHRONO24_REF_URL_TEMPLATE.format(reference_lower=reference.lower())
            record, html = self.fetch_url(detail_url, raw_dir)
            fetch_records.append(record)
            if record.ok and html:
                consecutive_failures = 0
                parsed_records.append(
                    parse_chrono24_reference_html(
                        html=html,
                        reference=reference,
                        family=str(metadata.get("family", "Unknown")),
                        msrp_usd=metadata.get("msrp_usd"),
                        collected_at_utc=collected_at.isoformat(),
                        detail_url=detail_url,
                    )
                )
            else:
                consecutive_failures += 1
                if (
                    self.settings.collector_fail_fast_after_errors > 0
                    and consecutive_failures >= self.settings.collector_fail_fast_after_errors
                    and not any(item.ok for item in fetch_records)
                ):
                    aborted_early = True
                    abort_reason = (
                        f"Aborted after {consecutive_failures} consecutive reference-page failures with no successful fetches."
                    )
                    break
            if self.settings.request_delay_seconds:
                time.sleep(self.settings.request_delay_seconds)

        fieldnames = [
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
        with interim_file.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            for row in parsed_records:
                writer.writerow(row)

        diagnostic_counter: dict[str, int] = {}
        for row in parsed_records:
            for diagnostic in filter(None, str(row["parser_diagnostics"]).split(",")):
                diagnostic_counter[diagnostic] = diagnostic_counter.get(diagnostic, 0) + 1

        manifest = {
            "run_id": run_id,
            "collected_at_utc": collected_at.isoformat(),
            "collector": "chrono24_pricing",
            "schema_version": self.settings.schema_version,
            "references_targeted": len(reference_catalog),
            "aborted_early": aborted_early,
            "abort_reason": abort_reason,
            "fetch_success_count": sum(1 for item in fetch_records if item.ok),
            "fetch_failure_count": sum(1 for item in fetch_records if not item.ok),
            "parsed_record_count": len(parsed_records),
            "records_with_parser_diagnostics": sum(1 for row in parsed_records if row["parser_diagnostics"]),
            "parser_diagnostic_counts": diagnostic_counter,
            "fetch_records": [asdict(item) for item in fetch_records],
            "interim_snapshot_file": str(interim_file.relative_to(self.settings.root_dir)).replace("\\", "/"),
            "source_freshness_utc": collected_at.isoformat(),
            "source_freshness_date": collected_at.date().isoformat(),
        }
        manifest_file.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

        latest_manifest = self.settings.interim_dir / "chrono24_pricing_latest_manifest.json"
        latest_manifest.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
        return manifest
