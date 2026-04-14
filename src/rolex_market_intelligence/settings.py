"""Configuration helpers for the Rolex Market Intelligence pipeline."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import os


DEFAULT_ROOT = Path(__file__).resolve().parents[2]


def load_dotenv(path: Path) -> None:
    """Load simple KEY=VALUE pairs from a local .env file if it exists."""

    if not path.exists():
        return

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


@dataclass(slots=True)
class Settings:
    """Resolved filesystem settings for the project."""

    root_dir: Path
    ad_list_dir: Path
    ad_count_file: Path
    pricing_file: Path
    reference_catalog_file: Path
    processed_dir: Path
    quality_dir: Path
    report_dir: Path
    figure_dir: Path
    raw_dir: Path
    interim_dir: Path
    schema_version: str = "2026-04-13"
    user_agent: str = "RolexMarketIntelligence/1.0"
    request_timeout_seconds: int = 30
    request_delay_seconds: float = 0.25
    collector_fail_fast_after_errors: int = 5

    def ensure_directories(self) -> None:
        """Create output directories needed by the pipeline."""

        for directory in (
            self.raw_dir,
            self.interim_dir,
            self.processed_dir,
            self.quality_dir,
            self.report_dir,
            self.figure_dir,
        ):
            directory.mkdir(parents=True, exist_ok=True)


def from_env() -> Settings:
    """Build settings from environment variables and repo defaults."""

    load_dotenv(DEFAULT_ROOT / ".env")
    root_dir = Path(os.getenv("ROLEX_PROJECT_ROOT", DEFAULT_ROOT)).resolve()
    data_dir = root_dir / "data"
    report_dir = Path(os.getenv("ROLEX_REPORT_DIR", root_dir / "reports")).resolve()
    processed_dir = Path(os.getenv("ROLEX_PROCESSED_DIR", data_dir / "processed")).resolve()
    quality_dir = Path(os.getenv("ROLEX_QUALITY_DIR", data_dir / "quality")).resolve()

    return Settings(
        root_dir=root_dir,
        ad_list_dir=Path(os.getenv("ROLEX_AD_LIST_DIR", root_dir / "AD_List")).resolve(),
        ad_count_file=Path(os.getenv("ROLEX_AD_COUNT_FILE", root_dir / "AD_Count" / "AD_Count.txt")).resolve(),
        pricing_file=Path(os.getenv("ROLEX_PRICING_FILE", root_dir / "Prices" / "Weekly_Median_Prices.csv")).resolve(),
        reference_catalog_file=Path(
            os.getenv("ROLEX_REFERENCE_CATALOG", root_dir / "config" / "reference_catalog.json")
        ).resolve(),
        processed_dir=processed_dir,
        quality_dir=quality_dir,
        report_dir=report_dir,
        figure_dir=(report_dir / "figures").resolve(),
        raw_dir=(data_dir / "raw").resolve(),
        interim_dir=(data_dir / "interim").resolve(),
        user_agent=os.getenv("ROLEX_USER_AGENT", "RolexMarketIntelligence/1.0"),
        request_timeout_seconds=int(os.getenv("ROLEX_REQUEST_TIMEOUT_SECONDS", "30")),
        request_delay_seconds=float(os.getenv("ROLEX_REQUEST_DELAY_SECONDS", "0.25")),
        collector_fail_fast_after_errors=int(os.getenv("ROLEX_COLLECTOR_FAIL_FAST_AFTER_ERRORS", "5")),
    )
