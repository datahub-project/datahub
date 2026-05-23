"""Unit tests for bigid_utils: _slugify, _parse_iso_to_ms, _map_field_type."""

from datahub.metadata.schema_classes import (
    BooleanTypeClass,
    NumberTypeClass,
    SchemaFieldDataTypeClass,
    StringTypeClass,
    TimeTypeClass,
)

from datahub.ingestion.source.bigid.bigid_utils import _map_field_type, _parse_iso_to_ms, _slugify


# ---------------------------------------------------------------------------
# _slugify
# ---------------------------------------------------------------------------


def test_slugify_basic():
    assert _slugify("Customer Data") == "customer_data"


def test_slugify_special_chars():
    assert _slugify("PII / PHI") == "pii_phi"


def test_slugify_leading_trailing_underscores():
    assert _slugify("  hello  ") == "hello"


# ---------------------------------------------------------------------------
# _parse_iso_to_ms
# ---------------------------------------------------------------------------


def test_parse_iso_to_ms_with_z():
    ms = _parse_iso_to_ms("2024-01-15T10:30:00.000Z")
    assert ms == 1705314600000  # 2024-01-15T10:30:00Z in Unix milliseconds


def test_parse_iso_to_ms_empty():
    assert _parse_iso_to_ms("") is None


def test_parse_iso_to_ms_none_value():
    assert _parse_iso_to_ms(None) is None


# ---------------------------------------------------------------------------
# _map_field_type
# ---------------------------------------------------------------------------


def test_map_field_type_varchar():
    ft = _map_field_type("varchar(255)")
    assert isinstance(ft, SchemaFieldDataTypeClass)
    assert isinstance(ft.type, StringTypeClass)


def test_map_field_type_int():
    ft = _map_field_type("int")
    assert isinstance(ft, SchemaFieldDataTypeClass)
    assert isinstance(ft.type, NumberTypeClass)


def test_map_field_type_boolean():
    ft = _map_field_type("boolean")
    assert isinstance(ft, SchemaFieldDataTypeClass)
    assert isinstance(ft.type, BooleanTypeClass)


def test_map_field_type_timestamp():
    ft = _map_field_type("timestamp")
    assert isinstance(ft, SchemaFieldDataTypeClass)
    assert isinstance(ft.type, TimeTypeClass)


def test_map_field_type_unknown_falls_back_to_string():
    ft = _map_field_type("jsonb")
    assert isinstance(ft, SchemaFieldDataTypeClass)
    assert isinstance(ft.type, StringTypeClass)
