"""Tests for pipeline and historical repair helpers."""

from pathlib import Path
from tempfile import TemporaryDirectory

from rolex_market_intelligence.collectors.rolex_ad import extract_links, parse_retailer_detail_html, DEALER_URL_PATTERN, CITY_URL_PATTERN
from rolex_market_intelligence.legacy import repair_dealer_history
from rolex_market_intelligence.pipeline import build_ad_change_log, run_pipeline
from rolex_market_intelligence.settings import Settings
import csv
import json
import unittest


class PipelineTests(unittest.TestCase):
    def test_run_pipeline_writes_manifest_and_schema_metadata(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            (root / "AD_List").mkdir(parents=True, exist_ok=True)
            (root / "AD_Count").mkdir(parents=True, exist_ok=True)
            (root / "Prices").mkdir(parents=True, exist_ok=True)
            (root / "config").mkdir(parents=True, exist_ok=True)

            with (root / "AD_List" / "Rolex_AD_List_10_2023.csv").open("w", encoding="utf-8", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=["Name", "Address", "City", "State", "Zip", "ID"])
                writer.writeheader()
                writer.writerow(
                    {
                        "Name": "Sample Jeweler",
                        "Address": "100 Main Street",
                        "City": "Boston",
                        "State": "Massachusetts",
                        "Zip": "02110",
                        "ID": "SAMPLEJEWELER100MAINSTREETBOSTONMASSACHUSETTS02110",
                    }
                )

            (root / "AD_Count" / "AD_Count.txt").write_text("10/2023: 1\n", encoding="utf-8")
            (root / "Prices" / "Weekly_Median_Prices.csv").write_text(
                "Date,126610LN,126610LN Listings,126610LN Markup\n"
                "09/13/2024,15500,126,51.22\n",
                encoding="utf-8",
            )
            (root / "config" / "reference_catalog.json").write_text(
                json.dumps({"126610LN": {"family": "Submariner Date", "msrp_usd": 10250}}),
                encoding="utf-8",
            )

            settings = Settings(
                root_dir=root,
                ad_list_dir=root / "AD_List",
                ad_count_file=root / "AD_Count" / "AD_Count.txt",
                pricing_file=root / "Prices" / "Weekly_Median_Prices.csv",
                reference_catalog_file=root / "config" / "reference_catalog.json",
                processed_dir=root / "data" / "processed",
                quality_dir=root / "data" / "quality",
                report_dir=root / "reports",
                figure_dir=root / "reports" / "figures",
                raw_dir=root / "data" / "raw",
                interim_dir=root / "data" / "interim",
            )

            summary = run_pipeline(settings)

            manifest_path = settings.processed_dir / "pipeline_run_manifest.json"
            schema_path = settings.processed_dir / "schema_version.json"
            history_path = settings.processed_dir / "pipeline_run_history.csv"
            history_manifest_dir = settings.processed_dir / "history"
            self.assertTrue(manifest_path.exists())
            self.assertTrue(schema_path.exists())
            self.assertTrue(history_path.exists())
            self.assertEqual(len(list(history_manifest_dir.glob("pipeline_run_manifest_*.json"))), 1)
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            schema_metadata = json.loads(schema_path.read_text(encoding="utf-8"))
            with history_path.open("r", encoding="utf-8", newline="") as handle:
                history_rows = list(csv.DictReader(handle))
            self.assertEqual(manifest["schema_version"], settings.schema_version)
            self.assertEqual(manifest["coverage"]["ad_snapshot_month_end"], "2023-10-01")
            self.assertEqual(manifest["outputs"]["ad_snapshot_history"]["record_count"], 1)
            self.assertEqual(schema_metadata["pipeline_manifest_file"], "data/processed/pipeline_run_manifest.json")
            self.assertEqual(schema_metadata["pipeline_history_file"], "data/processed/pipeline_run_history.csv")
            self.assertEqual(history_rows[0]["ad_snapshot_history_rows"], "1")
            self.assertEqual(summary["pipeline_manifest_file"], "data/processed/pipeline_run_manifest.json")
            self.assertEqual(summary["pipeline_history_file"], "data/processed/pipeline_run_history.csv")

    def test_extract_links_finds_city_and_dealer_paths(self) -> None:
        html = Path("tests/fixtures/rolex_state_page.html").read_text(encoding="utf-8")

        dealer_links = extract_links(html, DEALER_URL_PATTERN)
        city_links = extract_links(html, CITY_URL_PATTERN)

        self.assertEqual(len(dealer_links), 2)
        self.assertIn("/en-us/store-locator/unitedstates/florida/miami", city_links)
        self.assertTrue(all(not link.endswith("/buying-a-rolex") for link in dealer_links))

    def test_parse_retailer_detail_html_reads_json_ld(self) -> None:
        html = Path("tests/fixtures/rolex_detail_page.html").read_text(encoding="utf-8")

        parsed = parse_retailer_detail_html(
            html,
            "https://www.rolex.com/en-us/rolex-dealers/rolexboutiqueluxuryswiss-3762/rswi_12409-miami-unitedstates",
            "2026-04-13T20:00:00+00:00",
        )

        self.assertEqual(parsed["dealer_name"], "ROLEX BOUTIQUE LUXURY SWISS MIAMI")
        self.assertEqual(parsed["city"], "Miami")
        self.assertEqual(parsed["state"], "Florida")
        self.assertEqual(parsed["zip_code"], "33137")
        self.assertEqual(parsed["parser_diagnostics"], "")

    def test_repair_dealer_history_fills_blank_city_and_state(self) -> None:
        rows = [
            {
                "dealer_name": "Sample Jeweler",
                "address": "100 Main Street",
                "city": "Boston",
                "state": "Massachusetts",
                "zip_code": "02110",
                "legacy_id": "a",
                "normalized_name": "SAMPLEJEWELER",
                "normalized_address": "100MAINSTREET",
                "normalized_city": "BOSTON",
                "normalized_state": "MASSACHUSETTS",
                "canonical_id": "one",
                "snapshot_month": "2023-09-01",
                "source_file": "a.csv",
                "source_row_number": 1,
            },
            {
                "dealer_name": "Sample Jeweler",
                "address": "100 Main Street",
                "city": "",
                "state": "M",
                "zip_code": "02110",
                "legacy_id": "b",
                "normalized_name": "SAMPLEJEWELER",
                "normalized_address": "100MAINSTREET",
                "normalized_city": "",
                "normalized_state": "M",
                "canonical_id": "two",
                "snapshot_month": "2023-10-01",
                "source_file": "b.csv",
                "source_row_number": 1,
            },
        ]

        repaired = repair_dealer_history(rows)

        self.assertEqual(repaired[1]["city"], "Boston")
        self.assertEqual(repaired[1]["state"], "Massachusetts")

    def test_change_log_detects_move_for_same_normalized_name(self) -> None:
        rows = [
            {
                "snapshot_month": "2023-09-01",
                "dealer_name": "Sample Jeweler",
                "address": "100 Main Street",
                "city": "Boston",
                "state": "Massachusetts",
                "zip_code": "02110",
                "legacy_id": "old",
                "normalized_name": "SAMPLEJEWELER",
                "normalized_address": "100MAINSTREET",
                "normalized_city": "BOSTON",
                "normalized_state": "MASSACHUSETTS",
                "canonical_id": "SAMPLEJEWELER100MAINSTREETBOSTONMASSACHUSETTS02110",
                "source_file": "old.csv",
                "source_row_number": 1,
            },
            {
                "snapshot_month": "2023-10-01",
                "dealer_name": "Sample Jeweler",
                "address": "200 Main Street",
                "city": "Boston",
                "state": "Massachusetts",
                "zip_code": "02110",
                "legacy_id": "new",
                "normalized_name": "SAMPLEJEWELER",
                "normalized_address": "200MAINSTREET",
                "normalized_city": "BOSTON",
                "normalized_state": "MASSACHUSETTS",
                "canonical_id": "SAMPLEJEWELER200MAINSTREETBOSTONMASSACHUSETTS02110",
                "source_file": "new.csv",
                "source_row_number": 1,
            },
        ]

        changes = build_ad_change_log(rows)

        self.assertEqual(len(changes), 1)
        self.assertEqual(changes[0]["change_type"], "move_or_detail_change")
        self.assertIn("address", changes[0]["detail"])


if __name__ == "__main__":
    unittest.main()
