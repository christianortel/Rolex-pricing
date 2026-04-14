"""Tests for offline raw-run replay workflows."""

from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory
import csv
import json
import unittest

from rolex_market_intelligence.collectors.chrono24_pricing import parse_chrono24_reference_html
from rolex_market_intelligence.collectors.rolex_ad import parse_retailer_detail_html
from rolex_market_intelligence.promotion import AD_INTERIM_FIELDS, PRICING_INTERIM_FIELDS
from rolex_market_intelligence.replay import (
    replay_chrono24_pricing_run,
    replay_rolex_ad_run,
    summarize_chrono24_pricing_replays,
    summarize_rolex_ad_replays,
)
from rolex_market_intelligence.settings import Settings


def write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, object]]) -> None:
    """Write a CSV fixture file."""

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def build_settings(root: Path, reference_catalog_payload: dict[str, object] | None = None) -> Settings:
    """Build isolated settings for replay tests."""

    reference_catalog = root / "config" / "reference_catalog.json"
    reference_catalog.parent.mkdir(parents=True, exist_ok=True)
    reference_catalog.write_text(json.dumps(reference_catalog_payload or {}), encoding="utf-8")
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


class ReplayTests(unittest.TestCase):
    def test_summarize_rolex_ad_replays_aggregates_mismatched_fields(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            settings = build_settings(root)

            for run_id, source_city in [("20260414T120000Z", "Miami"), ("20260415T120000Z", "Coral Gables")]:
                raw_run_dir = settings.raw_dir / "rolex_ad" / run_id
                raw_run_dir.mkdir(parents=True, exist_ok=True)

                state_html = Path("tests/fixtures/rolex_state_page.html").read_text(encoding="utf-8")
                detail_html = Path("tests/fixtures/rolex_detail_page.html").read_text(encoding="utf-8")
                (raw_run_dir / "en-us/store-locator/unitedstates/florida.html").parent.mkdir(parents=True, exist_ok=True)
                (raw_run_dir / "en-us/store-locator/unitedstates/florida.html").write_text(state_html, encoding="utf-8")
                detail_path = raw_run_dir / "en-us/rolex-dealers/rolexboutiqueluxuryswiss-3762/rswi_12409-miami-unitedstates.html"
                detail_path.parent.mkdir(parents=True, exist_ok=True)
                detail_path.write_text(detail_html, encoding="utf-8")

                write_csv(
                    settings.interim_dir / f"rolex_ad_live_snapshot_{run_id}.csv",
                    AD_INTERIM_FIELDS,
                    [
                        {
                            "collected_at_utc": "2026-04-14T12:00:00+00:00",
                            "detail_url": "https://www.rolex.com/en-us/rolex-dealers/rolexboutiqueluxuryswiss-3762/rswi_12409-miami-unitedstates",
                            "dealer_name": "ROLEX BOUTIQUE LUXURY SWISS MIAMI",
                            "address": "182 NE 39th Street",
                            "city": source_city,
                            "state": "Florida",
                            "zip_code": "33137",
                            "phone": "+1 305-576-5391",
                            "website": "https://rolexboutique-designdistrict.com",
                            "source_title": "Rolex Boutique Luxury Swiss - Official Rolex Retailer | Rolex",
                            "parser_diagnostics": "",
                        }
                    ],
                )
                (raw_run_dir / "manifest.json").write_text(
                    json.dumps(
                        {
                            "run_id": run_id,
                            "collector": "rolex_ad",
                            "collected_at_utc": "2026-04-14T12:00:00+00:00",
                            "parsed_record_count": 1,
                            "records_with_parser_diagnostics": 0,
                            "interim_snapshot_file": f"data/interim/rolex_ad_live_snapshot_{run_id}.csv",
                            "state_page_diagnostics": [{"state_url": "https://www.rolex.com/en-us/store-locator/unitedstates/florida"}],
                            "fetch_records": [
                                {
                                    "url": "https://www.rolex.com/en-us/store-locator/unitedstates/florida",
                                    "relative_path": "en-us/store-locator/unitedstates/florida.html",
                                    "ok": True,
                                },
                                {
                                    "url": "https://www.rolex.com/en-us/rolex-dealers/rolexboutiqueluxuryswiss-3762/rswi_12409-miami-unitedstates",
                                    "relative_path": "en-us/rolex-dealers/rolexboutiqueluxuryswiss-3762/rswi_12409-miami-unitedstates.html",
                                    "ok": True,
                                },
                            ],
                        }
                    ),
                    encoding="utf-8",
                )

            summary = summarize_rolex_ad_replays(settings)
            with (settings.quality_dir / "rolex_ad_replay_rollup.csv").open("r", encoding="utf-8", newline="") as handle:
                rollup_rows = list(csv.DictReader(handle))

            self.assertEqual(summary["run_count"], 2)
            self.assertEqual(summary["runs_with_snapshot_mismatch"], 1)
            self.assertEqual(summary["field_mismatch_counts"]["city"], 1)
            self.assertEqual(summary["top_mismatched_fields"][0]["field_name"], "city")
            self.assertEqual(len(rollup_rows), 2)

    def test_summarize_chrono24_pricing_replays_aggregates_mismatched_fields(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            settings = build_settings(
                root,
                reference_catalog_payload={"126610LN": {"family": "Submariner Date", "msrp_usd": 10250}},
            )

            for index, run_id in enumerate(["20260414T120000Z", "20260415T120000Z"]):
                raw_run_dir = settings.raw_dir / "chrono24_pricing" / run_id
                raw_run_dir.mkdir(parents=True, exist_ok=True)

                pricing_html = Path("tests/fixtures/chrono24_reference_page.html").read_text(encoding="utf-8")
                price_path = raw_run_dir / "rolex/ref-126610ln_htm.html"
                price_path.parent.mkdir(parents=True, exist_ok=True)
                price_path.write_text(pricing_html, encoding="utf-8")

                expected_row = parse_chrono24_reference_html(
                    html=pricing_html,
                    reference="126610LN",
                    family="Submariner Date",
                    msrp_usd=10250,
                    collected_at_utc="2026-04-14T12:00:00+00:00",
                    detail_url="https://www.chrono24.com/rolex/ref-126610ln.htm",
                )
                if index == 1:
                    expected_row["median_price_usd"] = 15000.0
                write_csv(
                    settings.interim_dir / f"chrono24_pricing_live_snapshot_{run_id}.csv",
                    PRICING_INTERIM_FIELDS,
                    [expected_row],
                )
                (raw_run_dir / "manifest.json").write_text(
                    json.dumps(
                        {
                            "run_id": run_id,
                            "collector": "chrono24_pricing",
                            "collected_at_utc": "2026-04-14T12:00:00+00:00",
                            "parsed_record_count": 1,
                            "records_with_parser_diagnostics": 0,
                            "interim_snapshot_file": f"data/interim/chrono24_pricing_live_snapshot_{run_id}.csv",
                            "fetch_records": [
                                {
                                    "url": "https://www.chrono24.com/rolex/ref-126610ln.htm",
                                    "relative_path": "rolex/ref-126610ln_htm.html",
                                    "ok": True,
                                }
                            ],
                        }
                    ),
                    encoding="utf-8",
                )

            summary = summarize_chrono24_pricing_replays(settings)
            with (settings.quality_dir / "chrono24_pricing_replay_rollup.csv").open("r", encoding="utf-8", newline="") as handle:
                rollup_rows = list(csv.DictReader(handle))

            self.assertEqual(summary["run_count"], 2)
            self.assertEqual(summary["runs_with_snapshot_mismatch"], 1)
            self.assertEqual(summary["field_mismatch_counts"]["median_price_usd"], 1)
            self.assertEqual(summary["top_mismatched_fields"][0]["field_name"], "median_price_usd")
            self.assertEqual(len(rollup_rows), 2)

    def test_replay_rolex_ad_run_rebuilds_snapshot_from_saved_html(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            settings = build_settings(root)
            raw_run_dir = settings.raw_dir / "rolex_ad" / "20260414T120000Z"
            raw_run_dir.mkdir(parents=True, exist_ok=True)

            state_html = Path("tests/fixtures/rolex_state_page.html").read_text(encoding="utf-8")
            detail_html = Path("tests/fixtures/rolex_detail_page.html").read_text(encoding="utf-8")
            state_relative_path = Path("en-us/store-locator/unitedstates/florida.html")
            detail_relative_path = Path(
                "en-us/rolex-dealers/rolexboutiqueluxuryswiss-3762/rswi_12409-miami-unitedstates.html"
            )
            (raw_run_dir / state_relative_path).parent.mkdir(parents=True, exist_ok=True)
            (raw_run_dir / state_relative_path).write_text(state_html, encoding="utf-8")
            (raw_run_dir / detail_relative_path).parent.mkdir(parents=True, exist_ok=True)
            (raw_run_dir / detail_relative_path).write_text(detail_html, encoding="utf-8")

            expected_row = parse_retailer_detail_html(
                detail_html,
                "https://www.rolex.com/en-us/rolex-dealers/rolexboutiqueluxuryswiss-3762/rswi_12409-miami-unitedstates",
                "2026-04-14T12:00:00+00:00",
            )
            write_csv(settings.interim_dir / "rolex_ad_live_snapshot.csv", AD_INTERIM_FIELDS, [expected_row])
            (raw_run_dir / "manifest.json").write_text(
                json.dumps(
                    {
                        "run_id": "20260414T120000Z",
                        "collector": "rolex_ad",
                        "collected_at_utc": "2026-04-14T12:00:00+00:00",
                        "parsed_record_count": 1,
                        "records_with_parser_diagnostics": 0,
                        "interim_snapshot_file": "data/interim/rolex_ad_live_snapshot.csv",
                        "state_page_diagnostics": [
                            {
                                "state_url": "https://www.rolex.com/en-us/store-locator/unitedstates/florida",
                            }
                        ],
                        "fetch_records": [
                            {
                                "url": "https://www.rolex.com/en-us/store-locator/unitedstates/florida",
                                "relative_path": "en-us/store-locator/unitedstates/florida.html",
                                "ok": True,
                            },
                            {
                                "url": "https://www.rolex.com/en-us/rolex-dealers/rolexboutiqueluxuryswiss-3762/rswi_12409-miami-unitedstates",
                                "relative_path": "en-us/rolex-dealers/rolexboutiqueluxuryswiss-3762/rswi_12409-miami-unitedstates.html",
                                "ok": True,
                            },
                        ],
                    }
                ),
                encoding="utf-8",
            )

            summary = replay_rolex_ad_run(settings)

            self.assertEqual(summary["replay_parsed_record_count"], 1)
            self.assertTrue(summary["parsed_record_count_matches_source"])
            self.assertTrue(summary["snapshot_matches_source"])
            self.assertEqual(summary["diff_row_count"], 0)
            self.assertTrue((settings.interim_dir / "rolex_ad_replay_snapshot.csv").exists())
            self.assertTrue((settings.interim_dir / "rolex_ad_replay_latest.json").exists())
            self.assertTrue((settings.interim_dir / "rolex_ad_replay_diff.csv").exists())

    def test_replay_chrono24_pricing_run_rebuilds_snapshot_from_saved_html(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            settings = build_settings(
                root,
                reference_catalog_payload={
                    "126610LN": {"family": "Submariner Date", "msrp_usd": 10250},
                },
            )
            raw_run_dir = settings.raw_dir / "chrono24_pricing" / "20260414T120000Z"
            raw_run_dir.mkdir(parents=True, exist_ok=True)

            pricing_html = Path("tests/fixtures/chrono24_reference_page.html").read_text(encoding="utf-8")
            pricing_relative_path = Path("rolex/ref-126610ln_htm.html")
            (raw_run_dir / pricing_relative_path).parent.mkdir(parents=True, exist_ok=True)
            (raw_run_dir / pricing_relative_path).write_text(pricing_html, encoding="utf-8")

            expected_row = parse_chrono24_reference_html(
                html=pricing_html,
                reference="126610LN",
                family="Submariner Date",
                msrp_usd=10250,
                collected_at_utc="2026-04-14T12:00:00+00:00",
                detail_url="https://www.chrono24.com/rolex/ref-126610ln.htm",
            )
            write_csv(settings.interim_dir / "chrono24_pricing_live_snapshot.csv", PRICING_INTERIM_FIELDS, [expected_row])
            (raw_run_dir / "manifest.json").write_text(
                json.dumps(
                    {
                        "run_id": "20260414T120000Z",
                        "collector": "chrono24_pricing",
                        "collected_at_utc": "2026-04-14T12:00:00+00:00",
                        "parsed_record_count": 1,
                        "records_with_parser_diagnostics": 0,
                        "interim_snapshot_file": "data/interim/chrono24_pricing_live_snapshot.csv",
                        "fetch_records": [
                            {
                                "url": "https://www.chrono24.com/rolex/ref-126610ln.htm",
                                "relative_path": "rolex/ref-126610ln_htm.html",
                                "ok": True,
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )

            summary = replay_chrono24_pricing_run(settings)

            self.assertEqual(summary["replay_parsed_record_count"], 1)
            self.assertTrue(summary["parsed_record_count_matches_source"])
            self.assertTrue(summary["snapshot_matches_source"])
            self.assertEqual(summary["diff_row_count"], 0)
            self.assertTrue((settings.interim_dir / "chrono24_pricing_replay_snapshot.csv").exists())
            self.assertTrue((settings.interim_dir / "chrono24_pricing_replay_latest.json").exists())
            self.assertTrue((settings.interim_dir / "chrono24_pricing_replay_diff.csv").exists())

    def test_replay_rolex_ad_run_reports_field_level_diff_against_source_snapshot(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            settings = build_settings(root)
            raw_run_dir = settings.raw_dir / "rolex_ad" / "20260414T120000Z"
            raw_run_dir.mkdir(parents=True, exist_ok=True)

            state_html = Path("tests/fixtures/rolex_state_page.html").read_text(encoding="utf-8")
            detail_html = Path("tests/fixtures/rolex_detail_page.html").read_text(encoding="utf-8")
            (raw_run_dir / "en-us/store-locator/unitedstates/florida.html").parent.mkdir(parents=True, exist_ok=True)
            (raw_run_dir / "en-us/store-locator/unitedstates/florida.html").write_text(state_html, encoding="utf-8")
            (raw_run_dir / "en-us/rolex-dealers/rolexboutiqueluxuryswiss-3762/rswi_12409-miami-unitedstates.html").parent.mkdir(parents=True, exist_ok=True)
            (raw_run_dir / "en-us/rolex-dealers/rolexboutiqueluxuryswiss-3762/rswi_12409-miami-unitedstates.html").write_text(detail_html, encoding="utf-8")

            write_csv(
                settings.interim_dir / "rolex_ad_live_snapshot.csv",
                AD_INTERIM_FIELDS,
                [
                    {
                        "collected_at_utc": "2026-04-14T12:00:00+00:00",
                        "detail_url": "https://www.rolex.com/en-us/rolex-dealers/rolexboutiqueluxuryswiss-3762/rswi_12409-miami-unitedstates",
                        "dealer_name": "ROLEX BOUTIQUE LUXURY SWISS MIAMI",
                        "address": "182 NE 39th Street",
                        "city": "Coral Gables",
                        "state": "Florida",
                        "zip_code": "33137",
                        "phone": "+1 305-576-5391",
                        "website": "https://rolexboutique-designdistrict.com",
                        "source_title": "Rolex Boutique Luxury Swiss - Official Rolex Retailer | Rolex",
                        "parser_diagnostics": "",
                    }
                ],
            )
            (raw_run_dir / "manifest.json").write_text(
                json.dumps(
                    {
                        "run_id": "20260414T120000Z",
                        "collector": "rolex_ad",
                        "collected_at_utc": "2026-04-14T12:00:00+00:00",
                        "parsed_record_count": 1,
                        "records_with_parser_diagnostics": 0,
                        "interim_snapshot_file": "data/interim/rolex_ad_live_snapshot.csv",
                        "state_page_diagnostics": [{"state_url": "https://www.rolex.com/en-us/store-locator/unitedstates/florida"}],
                        "fetch_records": [
                            {
                                "url": "https://www.rolex.com/en-us/store-locator/unitedstates/florida",
                                "relative_path": "en-us/store-locator/unitedstates/florida.html",
                                "ok": True,
                            },
                            {
                                "url": "https://www.rolex.com/en-us/rolex-dealers/rolexboutiqueluxuryswiss-3762/rswi_12409-miami-unitedstates",
                                "relative_path": "en-us/rolex-dealers/rolexboutiqueluxuryswiss-3762/rswi_12409-miami-unitedstates.html",
                                "ok": True,
                            },
                        ],
                    }
                ),
                encoding="utf-8",
            )

            summary = replay_rolex_ad_run(settings)
            with (settings.interim_dir / "rolex_ad_replay_diff.csv").open("r", encoding="utf-8", newline="") as handle:
                diff_rows = list(csv.DictReader(handle))

            self.assertFalse(summary["snapshot_matches_source"])
            self.assertEqual(summary["diff_row_count"], 1)
            self.assertIn("city", summary["differing_fields"])
            self.assertEqual(diff_rows[0]["field_name"], "city")
            self.assertEqual(diff_rows[0]["source_value"], "Coral Gables")
            self.assertEqual(diff_rows[0]["replay_value"], "Miami")

    def test_replay_chrono24_run_reports_field_level_diff_against_source_snapshot(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            settings = build_settings(
                root,
                reference_catalog_payload={"126610LN": {"family": "Submariner Date", "msrp_usd": 10250}},
            )
            raw_run_dir = settings.raw_dir / "chrono24_pricing" / "20260414T120000Z"
            raw_run_dir.mkdir(parents=True, exist_ok=True)

            pricing_html = Path("tests/fixtures/chrono24_reference_page.html").read_text(encoding="utf-8")
            (raw_run_dir / "rolex/ref-126610ln_htm.html").parent.mkdir(parents=True, exist_ok=True)
            (raw_run_dir / "rolex/ref-126610ln_htm.html").write_text(pricing_html, encoding="utf-8")

            expected_row = parse_chrono24_reference_html(
                html=pricing_html,
                reference="126610LN",
                family="Submariner Date",
                msrp_usd=10250,
                collected_at_utc="2026-04-14T12:00:00+00:00",
                detail_url="https://www.chrono24.com/rolex/ref-126610ln.htm",
            )
            expected_row["median_price_usd"] = 15000.0
            write_csv(
                settings.interim_dir / "chrono24_pricing_live_snapshot.csv",
                PRICING_INTERIM_FIELDS,
                [expected_row],
            )
            (raw_run_dir / "manifest.json").write_text(
                json.dumps(
                    {
                        "run_id": "20260414T120000Z",
                        "collector": "chrono24_pricing",
                        "collected_at_utc": "2026-04-14T12:00:00+00:00",
                        "parsed_record_count": 1,
                        "records_with_parser_diagnostics": 0,
                        "interim_snapshot_file": "data/interim/chrono24_pricing_live_snapshot.csv",
                        "fetch_records": [
                            {
                                "url": "https://www.chrono24.com/rolex/ref-126610ln.htm",
                                "relative_path": "rolex/ref-126610ln_htm.html",
                                "ok": True,
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )

            summary = replay_chrono24_pricing_run(settings)
            with (settings.interim_dir / "chrono24_pricing_replay_diff.csv").open("r", encoding="utf-8", newline="") as handle:
                diff_rows = list(csv.DictReader(handle))

            self.assertFalse(summary["snapshot_matches_source"])
            self.assertEqual(summary["diff_row_count"], 1)
            self.assertIn("median_price_usd", summary["differing_fields"])
            self.assertEqual(diff_rows[0]["row_key"], "126610LN")
            self.assertEqual(diff_rows[0]["field_name"], "median_price_usd")


if __name__ == "__main__":
    unittest.main()
