import logging

import pytest

from datahub.ingestion.source.bigid.bigid_utils import (
    _map_field_type,
    _parse_iso_to_ms,
    _rank_to_float,
    _slugify,
)
from datahub.metadata.schema_classes import (
    BooleanTypeClass,
    NumberTypeClass,
    StringTypeClass,
    TimeTypeClass,
)


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("Customer Data", "customer_data"),
        ("PII / PHI", "pii_phi"),
        ("  hello  ", "hello"),
    ],
)
def test_slugify(raw, expected):
    assert _slugify(raw) == expected


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("2024-01-15T10:30:00.000Z", 1705314600000),  # ISO with trailing Z
        ("", None),
        (None, None),
    ],
)
def test_parse_iso_to_ms(raw, expected):
    assert _parse_iso_to_ms(raw) == expected


@pytest.mark.parametrize(
    ("field_type", "expected_type"),
    [
        ("varchar(255)", StringTypeClass),
        ("int", NumberTypeClass),
        ("boolean", BooleanTypeClass),
        ("timestamp", TimeTypeClass),
        ("jsonb", StringTypeClass),  # unknown falls back to string
    ],
)
def test_map_field_type(field_type, expected_type):
    assert isinstance(_map_field_type(field_type).type, expected_type)


@pytest.mark.parametrize(
    ("rank", "expected"),
    [("HIGH", 0.75), ("medium", 0.50), ("Low", 0.25)],
)
def test_rank_to_float_known_ranks(rank, expected):
    assert _rank_to_float(rank) == expected


def test_rank_to_float_unknown_rank_warns_and_returns_zero(caplog):
    with caplog.at_level(logging.WARNING):
        assert _rank_to_float("VERY_HIGH") == 0.0
    assert any("Unknown BigID confidence rank" in r.message for r in caplog.records)
