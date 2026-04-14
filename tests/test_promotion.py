"""Tests for append-safe promotion workflows."""

from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory
import csv
import json
import unittest

from rolex_market_intelligence.promotion import (
    AD_INTERIM_FIELDS,
    PRICING_INTERIM_FIELDS,
    PromotionError,
    promote_ad_snapshot,
    promote_pricing_snapshot,
    read_csv_rows,
)
from rolex_market_intelligence.settings import Settings


def write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, object]]) -> None:
    """Write a CSV file for test setup."""

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def build_settings(root: Path) -> Settings:
    """Build isolated settings for promotion tests."""

    reference_catalog = root / "config" / "reference_catalog.json"
    reference_catalog.parent.mkdir(parents=True, exist_ok=True)
    reference_catalog.write_text("{}", encoding="utf-8")
    settings = Settings(
        root_dir=root,
        ad_list_dir=root / "AD_List",
        ad_count_file=root / "AD_Count" / "AD_Count.txt",
        pricing_file=root / "Prices" / "Weekly_Median_Prices.csv",
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
    return settings


class PromotionTests(unittest.TestCase):
    def test_promote_ad_snapshot_writes_monthly_snapshot_and_count(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            settings = build_settings(root)
            write_csv(
                settings.interim_dir / "rolex_ad_live_snapshot.csv",
                AD_INTERIM_FIELDS,
                [
                    {
                        "collected_at_utc": "2026-04-14T12:00:00+00:00",
                        "detail_url": "https://example.com/dealer-a",
                        "dealer_name": "Dealer A",
                        "address": "123 Main Street",
                        "city": "New York",
                        "state": "New York",
                        "zip_code": "10001",
                        "phone": "",
                        "website": "",
                        "source_title": "Dealer A",
                        "parser_diagnostics": "",
                    },
                    {
                        "collected_at_utc": "2026-04-14T12:00:00+00:00",
                        "detail_url": "https://example.com/dealer-b",
                        "dealer_name": "Dealer B",
                        "address": "456 Oak Avenue",
                        "city": "Boston",
                        "state": "Massachusetts",
                        "zip_code": "02108",
                        "phone": "",
                        "website": "",
                        "source_title": "Dealer B",
                        "parser_diagnostics": "",
                    },
                ],
            )
            (settings.interim_dir / "rolex_ad_latest_manifest.json").write_text(
                json.dumps(
                    {
                        "collector": "rolex_ad",
                        "collected_at_utc": "2026-04-14T12:00:00+00:00",
                        "aborted_early": False,
                        "fetch_success_count": 5,
                        "parsed_record_count": 2,
                    }
                ),
                encoding="utf-8",
            )

            summary = promote_ad_snapshot(settings)

            self.assertEqual(summary["dealer_count"], 2)
            target_file = settings.ad_list_dir / "Rolex_AD_List_4_2026.csv"
            self.assertTrue(target_file.exists())
            promoted_rows = read_csv_rows(target_file)
            self.assertEqual(len(promoted_rows), 2)
            self.assertIn("4/2026: 2", settings.ad_count_file.read_text(encoding="utf-8"))
            self.assertTrue((settings.interim_dir / "rolex_ad_promotion_latest.json").exists())

    def test_promote_ad_snapshot_noops_when_month_file_matches(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            settings = build_settings(root)
            write_csv(
                settings.interim_dir / "rolex_ad_live_snapshot.csv",
                AD_INTERIM_FIELDS,
                [
                    {
                        "collected_at_utc": "2026-04-14T12:00:00+00:00",
                        "detail_url": "https://example.com/dealer-a",
                        "dealer_name": "Dealer A",
                        "address": "123 Main Street",
                        "city": "New York",
                        "state": "New York",
                        "zip_code": "10001",
                        "phone": "",
                        "website": "",
                        "source_title": "Dealer A",
                        "parser_diagnostics": "",
                    }
                ],
            )
            (settings.interim_dir / "rolex_ad_latest_manifest.json").write_text(
                json.dumps(
                    {
                        "collector": "rolex_ad",
                        "collected_at_utc": "2026-04-14T12:00:00+00:00",
                        "aborted_early": False,
                        "fetch_success_count": 5,
                        "parsed_record_count": 1,
                    }
                ),
                encoding="utf-8",
            )
            write_csv(
                settings.ad_list_dir / "Rolex_AD_List_4_2026.csv",
                ["Name", "Address", "City", "State", "Zip", "ID"],
                [
                    {
                        "Name": "Dealer A",
                        "Address": "123 Main Street",
                        "City": "New York",
                        "State": "New York",
                        "Zip": "10001",
                        "ID": "DEALERA123MAINSTREETNEWYORKNEWYORK10001",
                    }
                ],
            )

            summary = promote_ad_snapshot(settings)

            self.assertEqual(summary["status"], "noop")
            self.assertTrue((settings.interim_dir / "rolex_ad_promotion_latest.json").exists())

    def test_promote_pricing_snapshot_appends_row_and_noops_on_repeat(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            settings = build_settings(root)
            write_csv(
                settings.pricing_file,
                [
                    "Date",
                    "124300",
                    "126610LN",
                    "124300 Listings",
                    "126610LN Listings",
                    "124300 Markup",
                    "126610LN Markup",
                ],
                [
                    {
                        "Date": "04/07/2026",
                        "124300": "6400",
                        "126610LN": "15000",
                        "124300 Listings": "20",
                        "126610LN Listings": "50",
                        "124300 Markup": "0.00",
                        "126610LN Markup": "46.34",
                    }
                ],
            )
            write_csv(
                settings.interim_dir / "chrono24_pricing_live_snapshot.csv",
                PRICING_INTERIM_FIELDS,
                [
                    {
                        "collected_at_utc": "2026-04-14T12:00:00+00:00",
                        "reference": "124300",
                        "family": "Oyster Perpetual",
                        "msrp_usd": "6400",
                        "detail_url": "https://example.com/124300",
                        "source_title": "124300",
                        "observed_price_sample_count": "4",
                        "listing_count": "18",
                        "median_price_usd": "7000",
                        "markup_pct": "9.38",
                        "min_price_usd": "6800",
                        "max_price_usd": "7200",
                        "sample_prices_preview": "6800|7000|7200",
                        "parser_diagnostics": "",
                    },
                    {
                        "collected_at_utc": "2026-04-14T12:00:00+00:00",
                        "reference": "126610LN",
                        "family": "Submariner Date",
                        "msrp_usd": "10250",
                        "detail_url": "https://example.com/126610LN",
                        "source_title": "126610LN",
                        "observed_price_sample_count": "4",
                        "listing_count": "55",
                        "median_price_usd": "15800",
                        "markup_pct": "54.15",
                        "min_price_usd": "15400",
                        "max_price_usd": "16200",
                        "sample_prices_preview": "15400|15800|16200",
                        "parser_diagnostics": "",
                    },
                ],
            )
            (settings.interim_dir / "chrono24_pricing_latest_manifest.json").write_text(
                json.dumps(
                    {
                        "collector": "chrono24_pricing",
                        "collected_at_utc": "2026-04-14T12:00:00+00:00",
                        "aborted_early": False,
                        "fetch_success_count": 2,
                        "parsed_record_count": 2,
                    }
                ),
                encoding="utf-8",
            )

            first_summary = promote_pricing_snapshot(settings)
            second_summary = promote_pricing_snapshot(settings)

            self.assertEqual(first_summary["reference_count"], 2)
            self.assertEqual(second_summary["status"], "noop")
            rows = read_csv_rows(settings.pricing_file)
            self.assertEqual(len(rows), 2)
            self.assertEqual(rows[-1]["Date"], "04/14/2026")
            self.assertEqual(rows[-1]["126610LN"], "15800")
            self.assertEqual(rows[-1]["126610LN Listings"], "55")
            self.assertTrue((settings.interim_dir / "chrono24_pricing_promotion_latest.json").exists())

    def test_promote_pricing_snapshot_rejects_aborted_manifest(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            settings = build_settings(root)
            write_csv(settings.pricing_file, ["Date", "124300"], [])
            write_csv(settings.interim_dir / "chrono24_pricing_live_snapshot.csv", PRICING_INTERIM_FIELDS, [])
            (settings.interim_dir / "chrono24_pricing_latest_manifest.json").write_text(
                json.dumps(
                    {
                        "collector": "chrono24_pricing",
                        "collected_at_utc": "2026-04-14T12:00:00+00:00",
                        "aborted_early": True,
                        "abort_reason": "blocked",
                        "fetch_success_count": 0,
                        "parsed_record_count": 0,
                    }
                ),
                encoding="utf-8",
            )

            with self.assertRaises(PromotionError):
                promote_pricing_snapshot(settings)


if __name__ == "__main__":
    unittest.main()
