"""Tests for DAX calculated-table reference extraction."""

from datahub.ingestion.source.powerbi.m_query.dax_resolver import (
    extract_dax_table_references,
)


def test_summarize_single_table() -> None:
    refs = extract_dax_table_references("summarize('FMS Lookup', 'FMS Lookup'[FMSID])")
    assert refs == ["FMS Lookup"]


def test_multiple_tables() -> None:
    refs = extract_dax_table_references(
        "SUMMARIZE('Sales', 'Sales'[Region], 'Customers'[Name])"
    )
    assert refs == ["Customers", "Sales"]


def test_expression_without_table_reference() -> None:
    assert extract_dax_table_references("1 + 1") == []


def test_unparseable_expression_returns_empty() -> None:
    assert extract_dax_table_references("#(*&@ not dax") == []
