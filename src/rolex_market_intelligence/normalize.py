"""Normalization helpers for legacy Rolex market data."""

from __future__ import annotations

from typing import Mapping
import re
import unicodedata


MOJIBAKE_REPLACEMENTS = {
    "â€­": "",
    "â€¬": "",
    "â€™": "'",
    "â€œ": '"',
    "â€�": '"',
    "â€“": "-",
    "â€”": "-",
    "Â": "",
    "\ufeff": "",
}

WHITESPACE_PATTERN = re.compile(r"\s+")
NON_ALNUM_PATTERN = re.compile(r"[^A-Z0-9]")


def clean_text(value: object) -> str:
    """Normalize legacy text by removing common encoding artifacts and extra whitespace."""

    if value is None:
        return ""

    text = str(value)
    for bad, good in MOJIBAKE_REPLACEMENTS.items():
        text = text.replace(bad, good)
    text = unicodedata.normalize("NFKD", text)
    text = "".join(character for character in text if character.isprintable())
    text = WHITESPACE_PATTERN.sub(" ", text)
    return text.strip(" ,")


def normalize_state(value: object) -> str:
    """Normalize state names into title case."""

    text = clean_text(value)
    return " ".join(part.capitalize() for part in text.split())


def normalize_zip(value: object) -> str:
    """Keep the five-digit ZIP component when present."""

    digits = "".join(character for character in clean_text(value) if character.isdigit())
    return digits[:5]


def canonical_token(value: object) -> str:
    """Convert text into a canonical uppercase alphanumeric token."""

    cleaned = clean_text(value).upper()
    return NON_ALNUM_PATTERN.sub("", cleaned)


def build_canonical_id(name: str, address: str, city: str, state: str, zip_code: str) -> str:
    """Construct a stable dealer identifier from normalized core fields."""

    return "".join(
        [
            canonical_token(name),
            canonical_token(address),
            canonical_token(city),
            canonical_token(state),
            canonical_token(zip_code),
        ]
    )


def normalize_dealer_row(row: Mapping[str, object]) -> dict[str, str]:
    """Normalize a legacy dealer record into canonical fields."""

    name = clean_text(row.get("Name", ""))
    address = clean_text(row.get("Address", ""))
    city = clean_text(row.get("City", ""))
    state = normalize_state(row.get("State", ""))
    zip_code = normalize_zip(row.get("Zip", ""))
    legacy_id = clean_text(row.get("ID", ""))

    return {
        "dealer_name": name,
        "address": address,
        "city": city,
        "state": state,
        "zip_code": zip_code,
        "legacy_id": legacy_id,
        "normalized_name": canonical_token(name),
        "normalized_address": canonical_token(address),
        "normalized_city": canonical_token(city),
        "normalized_state": canonical_token(state),
        "canonical_id": build_canonical_id(name, address, city, state, zip_code),
    }
