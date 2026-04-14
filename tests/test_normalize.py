"""Tests for normalization helpers."""

from rolex_market_intelligence.normalize import build_canonical_id, clean_text, normalize_dealer_row
import unittest


class NormalizeTests(unittest.TestCase):
    def test_clean_text_removes_known_mojibake(self) -> None:
        self.assertEqual(clean_text("â€­A.R. Morrisâ€¬"), "A.R. Morris")

    def test_normalize_dealer_row_builds_stable_canonical_id(self) -> None:
        row = {
            "Name": "A.R. Morris",
            "Address": "3848 Kennett Pike",
            "City": "Greenville",
            "State": "Delaware",
            "Zip": "19807",
            "ID": "legacy",
        }
        normalized = normalize_dealer_row(row)
        self.assertEqual(
            normalized["canonical_id"],
            build_canonical_id("A.R. Morris", "3848 Kennett Pike", "Greenville", "Delaware", "19807"),
        )


if __name__ == "__main__":
    unittest.main()
