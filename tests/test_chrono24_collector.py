"""Tests for the Chrono24 pricing collector."""

from pathlib import Path
from tempfile import TemporaryDirectory

from rolex_market_intelligence.collectors.chrono24_pricing import (
    Chrono24PricingCollector,
    FetchRecord,
    parse_chrono24_reference_html,
)
from rolex_market_intelligence.settings import Settings
import json
import unittest


class _AlwaysFailChrono24Collector(Chrono24PricingCollector):
    def fetch_url(self, url: str, raw_dir: Path) -> tuple[FetchRecord, str]:
        relative_path = "failed.html"
        (raw_dir / relative_path).parent.mkdir(parents=True, exist_ok=True)
        (raw_dir / relative_path).write_text("", encoding="utf-8")
        return (
            FetchRecord(
                url=url,
                relative_path=relative_path,
                status_code=None,
                ok=False,
                error="simulated failure",
                content_type="",
                bytes_written=0,
                sha256_hex="",
                fetched_at_utc="2026-04-14T00:00:00+00:00",
            ),
            "",
        )


class Chrono24CollectorTests(unittest.TestCase):
    def test_parse_chrono24_reference_html_extracts_summary(self) -> None:
        html = Path("tests/fixtures/chrono24_reference_page.html").read_text(encoding="utf-8")

        parsed = parse_chrono24_reference_html(
            html=html,
            reference="126610LN",
            family="Submariner Date",
            msrp_usd=10250,
            collected_at_utc="2026-04-14T00:00:00+00:00",
            detail_url="https://www.chrono24.com/rolex/ref-126610ln.htm",
        )

        self.assertEqual(parsed["listing_count"], 126)
        self.assertEqual(parsed["observed_price_sample_count"], 4)
        self.assertEqual(parsed["median_price_usd"], 15500.0)
        self.assertEqual(parsed["markup_pct"], 51.22)
        self.assertEqual(parsed["parser_diagnostics"], "")

    def test_collect_aborts_early_after_reference_failures(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            reference_catalog = root / "reference_catalog.json"
            reference_catalog.write_text(
                json.dumps(
                    {
                        "126610LN": {"family": "Submariner Date", "msrp_usd": 10250},
                        "126710BLRO": {"family": "GMT-Master II", "msrp_usd": 10900},
                        "124300": {"family": "Oyster Perpetual", "msrp_usd": 6400},
                    }
                ),
                encoding="utf-8",
            )
            settings = Settings(
                root_dir=root,
                ad_list_dir=root / "AD_List",
                ad_count_file=root / "AD_Count.txt",
                pricing_file=root / "Prices.csv",
                reference_catalog_file=reference_catalog,
                processed_dir=root / "data" / "processed",
                quality_dir=root / "data" / "quality",
                report_dir=root / "reports",
                figure_dir=root / "reports" / "figures",
                raw_dir=root / "data" / "raw",
                interim_dir=root / "data" / "interim",
                request_delay_seconds=0.0,
                collector_fail_fast_after_errors=2,
            )
            settings.ensure_directories()
            collector = _AlwaysFailChrono24Collector(settings)

            manifest = collector.collect()

            self.assertTrue(manifest["aborted_early"])
            self.assertEqual(manifest["fetch_failure_count"], 2)
            self.assertEqual(manifest["parsed_record_count"], 0)
            self.assertTrue((settings.interim_dir / "chrono24_pricing_latest_manifest.json").exists())


if __name__ == "__main__":
    unittest.main()
