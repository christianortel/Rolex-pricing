"""Tests for live collector behavior."""

from pathlib import Path
from tempfile import TemporaryDirectory

from rolex_market_intelligence.collectors.rolex_ad import FetchRecord, RolexADCollector
from rolex_market_intelligence.settings import Settings
import unittest


class _AlwaysFailCollector(RolexADCollector):
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


class CollectorTests(unittest.TestCase):
    def test_collect_aborts_early_after_seed_failures(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            settings = Settings(
                root_dir=root,
                ad_list_dir=root / "AD_List",
                ad_count_file=root / "AD_Count.txt",
                pricing_file=root / "Prices.csv",
                reference_catalog_file=root / "reference_catalog.json",
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
            collector = _AlwaysFailCollector(settings)

            manifest = collector.collect()

            self.assertTrue(manifest["aborted_early"])
            self.assertEqual(manifest["fetch_failure_count"], 2)
            self.assertEqual(manifest["fetch_success_count"], 0)
            self.assertTrue((settings.interim_dir / "rolex_ad_latest_manifest.json").exists())


if __name__ == "__main__":
    unittest.main()
