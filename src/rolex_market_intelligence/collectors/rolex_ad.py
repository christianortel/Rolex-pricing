"""Live Rolex U.S. authorized dealer collector with raw snapshot capture."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
from urllib.parse import urljoin, urlparse
from urllib.request import Request, urlopen
import csv
import datetime as dt
import hashlib
import json
import logging
import re
import time

from ..normalize import clean_text, normalize_state, normalize_zip
from ..settings import Settings


LOGGER = logging.getLogger(__name__)
STORE_LOCATOR_BASE_URL = "https://www.rolex.com/en-us/store-locator/unitedstates"
DEALER_URL_PATTERN = re.compile(
    r"/en-us/rolex-dealers/[^\"'#?]+/rswi_[^\"'#?]+?(?:/(?:buying-a-rolex|servicing-your-rolex))?(?=[\"'#?]|$)"
)
CITY_URL_PATTERN = re.compile(r"/en-us/store-locator/unitedstates/[a-z-]+/[a-z0-9-]+(?=[\"'#?]|$)")
TITLE_PATTERN = re.compile(r"<title>\s*(.*?)\s*</title>", re.IGNORECASE | re.DOTALL)
SCRIPT_JSON_PATTERN = re.compile(
    r"<script[^>]+type=[\"']application/ld\+json[\"'][^>]*>\s*(.*?)\s*</script>",
    re.IGNORECASE | re.DOTALL,
)
PHONE_PATTERN = re.compile(r"\+\d[\d\s().-]{7,}\d")
CITY_STATE_ZIP_PATTERN = re.compile(
    r"^(?P<city>.+?)\s+(?P<state>[A-Za-z][A-Za-z .'-]+?)\s+(?P<zip>\d{5}(?:-\d{4})?)$"
)

US_STATE_SLUGS = [
    "alabama",
    "alaska",
    "arizona",
    "arkansas",
    "california",
    "colorado",
    "connecticut",
    "delaware",
    "district-of-columbia",
    "florida",
    "georgia",
    "hawaii",
    "idaho",
    "illinois",
    "indiana",
    "iowa",
    "kansas",
    "kentucky",
    "louisiana",
    "maine",
    "maryland",
    "massachusetts",
    "michigan",
    "minnesota",
    "mississippi",
    "missouri",
    "montana",
    "nebraska",
    "nevada",
    "new-hampshire",
    "new-jersey",
    "new-mexico",
    "new-york",
    "north-carolina",
    "north-dakota",
    "ohio",
    "oklahoma",
    "oregon",
    "pennsylvania",
    "rhode-island",
    "south-carolina",
    "south-dakota",
    "tennessee",
    "texas",
    "utah",
    "vermont",
    "virginia",
    "washington",
    "west-virginia",
    "wisconsin",
    "wyoming",
]


@dataclass(slots=True)
class FetchRecord:
    """Metadata about a fetched remote page."""

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


def extract_links(html: str, pattern: re.Pattern[str]) -> list[str]:
    """Extract normalized internal links matching a regex."""

    matches = []
    for match in pattern.findall(html):
        normalized = re.sub(r"/(?:buying-a-rolex|servicing-your-rolex)$", "", match)
        matches.append(normalized)
    return sorted(set(matches))


def parse_json_ld_objects(html: str) -> list[dict[str, object]]:
    """Parse embedded JSON-LD blocks when present."""

    objects: list[dict[str, object]] = []
    for raw_block in SCRIPT_JSON_PATTERN.findall(html):
        block = clean_text(raw_block)
        if not block:
            continue
        try:
            parsed = json.loads(block)
        except json.JSONDecodeError:
            continue
        if isinstance(parsed, list):
            objects.extend(item for item in parsed if isinstance(item, dict))
        elif isinstance(parsed, dict):
            objects.append(parsed)
    return objects


def find_store_json_ld(html: str) -> dict[str, object] | None:
    """Return the first likely LocalBusiness / store JSON-LD object."""

    for candidate in parse_json_ld_objects(html):
        candidate_type = candidate.get("@type")
        if candidate_type in {"Store", "JewelryStore", "LocalBusiness"}:
            return candidate
    return None


def strip_html_to_lines(html: str) -> list[str]:
    """Reduce HTML to cleaned text lines for heuristic parsing."""

    text = re.sub(r"(?is)<(script|style).*?>.*?</\1>", " ", html)
    text = re.sub(r"(?i)<br\s*/?>", "\n", text)
    text = re.sub(r"(?i)</(p|div|section|article|li|ul|ol|h1|h2|h3|h4|h5|h6|title)>", "\n", text)
    text = re.sub(r"(?s)<[^>]+>", " ", text)
    text = text.replace("&nbsp;", " ")
    text = text.replace("&amp;", "&")
    raw_lines = [clean_text(line) for line in text.splitlines()]
    return [line for line in raw_lines if line]


def parse_retailer_detail_html(html: str, detail_url: str, collected_at_utc: str) -> dict[str, object]:
    """Parse a Rolex retailer detail page into a normalized record."""

    title_match = TITLE_PATTERN.search(html)
    title = clean_text(title_match.group(1)) if title_match else ""
    store_json = find_store_json_ld(html)

    dealer_name = ""
    address = ""
    city = ""
    state = ""
    zip_code = ""
    phone = ""
    website = ""

    if store_json:
        dealer_name = clean_text(store_json.get("name", ""))
        phone = clean_text(store_json.get("telephone", ""))
        website = clean_text(store_json.get("url", ""))
        raw_address = store_json.get("address")
        if isinstance(raw_address, dict):
            address = clean_text(raw_address.get("streetAddress", ""))
            city = clean_text(raw_address.get("addressLocality", ""))
            state = normalize_state(raw_address.get("addressRegion", ""))
            zip_code = normalize_zip(raw_address.get("postalCode", ""))

    lines = strip_html_to_lines(html)
    if not dealer_name:
        for line in lines:
            if "Official Rolex" in line and "Welcome to" in line:
                dealer_name = clean_text(line.split("Welcome to", 1)[1].split("Official Rolex", 1)[0])
                break
        if not dealer_name and title:
            dealer_name = clean_text(title.split(" - ", 1)[0])

    if not phone:
        phone_match = PHONE_PATTERN.search(" ".join(lines))
        phone = clean_text(phone_match.group(0)) if phone_match else ""

    if not website:
        website_match = re.search(r'href=["\'](https?://[^"\']+)["\'][^>]*>\s*Visit website', html, re.IGNORECASE)
        website = clean_text(website_match.group(1)) if website_match else ""

    if not address or not city or not state or not zip_code:
        for index, line in enumerate(lines):
            if line == "United States" and index >= 2:
                maybe_city_line = lines[index - 1]
                maybe_address_line = lines[index - 2]
                city_match = CITY_STATE_ZIP_PATTERN.match(maybe_city_line)
                if city_match:
                    address = address or maybe_address_line
                    city = city or clean_text(city_match.group("city"))
                    state = state or normalize_state(city_match.group("state"))
                    zip_code = zip_code or normalize_zip(city_match.group("zip"))
                    break

    diagnostics = []
    if not dealer_name:
        diagnostics.append("missing_dealer_name")
    if not address:
        diagnostics.append("missing_address")
    if not city:
        diagnostics.append("missing_city")
    if not state:
        diagnostics.append("missing_state")
    if not zip_code:
        diagnostics.append("missing_zip_code")
    if not phone:
        diagnostics.append("missing_phone")

    return {
        "collected_at_utc": collected_at_utc,
        "detail_url": detail_url,
        "dealer_name": dealer_name,
        "address": address,
        "city": city,
        "state": state,
        "zip_code": zip_code,
        "phone": phone,
        "website": website,
        "source_title": title,
        "parser_diagnostics": ",".join(diagnostics),
    }


class RolexADCollector:
    """Collect the current U.S. Rolex AD network from rolex.com."""

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
        """Run the live collector and write raw/interim artifacts."""

        collected_at = dt.datetime.now(dt.timezone.utc).replace(microsecond=0)
        run_id = collected_at.strftime("%Y%m%dT%H%M%SZ")
        raw_dir = self.settings.raw_dir / "rolex_ad" / run_id
        raw_dir.mkdir(parents=True, exist_ok=True)
        interim_file = self.settings.interim_dir / "rolex_ad_live_snapshot.csv"
        manifest_file = raw_dir / "manifest.json"

        state_urls = [f"{STORE_LOCATOR_BASE_URL}/{slug}" for slug in US_STATE_SLUGS]
        city_urls: set[str] = set()
        detail_urls: set[str] = set()
        fetch_records: list[FetchRecord] = []
        state_page_diagnostics: list[dict[str, object]] = []
        aborted_early = False
        abort_reason = ""
        consecutive_failures = 0

        for state_url in state_urls:
            record, html = self.fetch_url(state_url, raw_dir)
            fetch_records.append(record)
            found_dealers = extract_links(html, DEALER_URL_PATTERN)
            found_cities = extract_links(html, CITY_URL_PATTERN)
            detail_urls.update(urljoin(STORE_LOCATOR_BASE_URL, path) for path in found_dealers)
            city_urls.update(urljoin(STORE_LOCATOR_BASE_URL, path) for path in found_cities)
            state_page_diagnostics.append(
                {
                    "state_url": state_url,
                    "ok": record.ok,
                    "dealer_links_found": len(found_dealers),
                    "city_links_found": len(found_cities),
                    "error": record.error,
                }
            )
            if record.ok:
                consecutive_failures = 0
            else:
                consecutive_failures += 1
                if (
                    self.settings.collector_fail_fast_after_errors > 0
                    and consecutive_failures >= self.settings.collector_fail_fast_after_errors
                    and not any(item.ok for item in fetch_records)
                ):
                    aborted_early = True
                    abort_reason = (
                        f"Aborted after {consecutive_failures} consecutive seed-page failures with no successful fetches."
                    )
                    break
            if self.settings.request_delay_seconds:
                time.sleep(self.settings.request_delay_seconds)

        if not aborted_early:
            for city_url in sorted(city_urls):
                record, html = self.fetch_url(city_url, raw_dir)
                fetch_records.append(record)
                detail_urls.update(urljoin(STORE_LOCATOR_BASE_URL, path) for path in extract_links(html, DEALER_URL_PATTERN))
                if self.settings.request_delay_seconds:
                    time.sleep(self.settings.request_delay_seconds)

        parsed_records: list[dict[str, object]] = []
        if not aborted_early:
            for detail_url in sorted(detail_urls):
                record, html = self.fetch_url(detail_url, raw_dir)
                fetch_records.append(record)
                if record.ok and html:
                    parsed_records.append(parse_retailer_detail_html(html, detail_url, collected_at.isoformat()))
                if self.settings.request_delay_seconds:
                    time.sleep(self.settings.request_delay_seconds)

        fieldnames = [
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
            "collector": "rolex_ad",
            "schema_version": self.settings.schema_version,
            "seed_state_pages": len(state_urls),
            "city_pages_discovered": len(city_urls),
            "dealer_detail_pages_discovered": len(detail_urls),
            "aborted_early": aborted_early,
            "abort_reason": abort_reason,
            "fetch_success_count": sum(1 for item in fetch_records if item.ok),
            "fetch_failure_count": sum(1 for item in fetch_records if not item.ok),
            "parsed_record_count": len(parsed_records),
            "records_with_parser_diagnostics": sum(1 for row in parsed_records if row["parser_diagnostics"]),
            "parser_diagnostic_counts": diagnostic_counter,
            "fetch_records": [asdict(item) for item in fetch_records],
            "state_page_diagnostics": state_page_diagnostics,
            "interim_snapshot_file": str(interim_file.relative_to(self.settings.root_dir)).replace("\\", "/"),
            "source_freshness_utc": collected_at.isoformat(),
            "source_freshness_date": collected_at.date().isoformat(),
        }
        manifest_file.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

        latest_manifest = self.settings.interim_dir / "rolex_ad_latest_manifest.json"
        latest_manifest.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
        return manifest
