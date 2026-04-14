"""Command-line entrypoint for Rolex Market Intelligence."""

from __future__ import annotations

import argparse
import json
import sys

from .collectors.chrono24_pricing import Chrono24PricingCollector
from .collectors.rolex_ad import RolexADCollector
from .pipeline import run_pipeline
from .promotion import PromotionError, promote_ad_snapshot, promote_pricing_snapshot
from .replay import (
    ReplayError,
    replay_chrono24_pricing_run,
    replay_rolex_ad_run,
    summarize_chrono24_pricing_replays,
    summarize_rolex_ad_replays,
)
from .settings import from_env


def build_parser() -> argparse.ArgumentParser:
    """Create the command-line parser."""

    parser = argparse.ArgumentParser(description="Rolex Market Intelligence CLI")
    parser.add_argument(
        "command",
        choices=[
            "pipeline",
            "collect-ad",
            "collect-prices",
            "promote-ad",
            "promote-prices",
            "replay-ad",
            "replay-prices",
            "replay-summary-ad",
            "replay-summary-prices",
        ],
        help="Pipeline command to execute.",
    )
    parser.add_argument(
        "--raw-run-dir",
        help="Optional saved raw collector run directory for replay commands.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """Run the CLI."""

    parser = build_parser()
    args = parser.parse_args(argv)
    settings = from_env()

    try:
        if args.command == "pipeline":
            summary = run_pipeline(settings)
            print(json.dumps(summary, indent=2))
            return 0

        if args.command == "collect-ad":
            summary = RolexADCollector(settings).collect()
            print(json.dumps(summary, indent=2))
            return 0

        if args.command == "collect-prices":
            summary = Chrono24PricingCollector(settings).collect()
            print(json.dumps(summary, indent=2))
            return 0

        if args.command == "promote-ad":
            summary = promote_ad_snapshot(settings)
            print(json.dumps(summary, indent=2))
            return 0

        if args.command == "promote-prices":
            summary = promote_pricing_snapshot(settings)
            print(json.dumps(summary, indent=2))
            return 0

        if args.command == "replay-ad":
            summary = replay_rolex_ad_run(settings, raw_run_dir=args.raw_run_dir)
            print(json.dumps(summary, indent=2))
            return 0

        if args.command == "replay-prices":
            summary = replay_chrono24_pricing_run(settings, raw_run_dir=args.raw_run_dir)
            print(json.dumps(summary, indent=2))
            return 0

        if args.command == "replay-summary-ad":
            summary = summarize_rolex_ad_replays(settings)
            print(json.dumps(summary, indent=2))
            return 0

        if args.command == "replay-summary-prices":
            summary = summarize_chrono24_pricing_replays(settings)
            print(json.dumps(summary, indent=2))
            return 0
    except (PromotionError, ReplayError) as exc:
        print(json.dumps({"error": str(exc)}), file=sys.stderr)
        return 1

    parser.error(f"Unsupported command: {args.command}")
    return 2


if __name__ == "__main__":
    sys.exit(main())
