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


def test_bare_table_reference_forms() -> None:
    # Standalone table references (no [Column]) land on `table_references`, which
    # covers the most common calculated-table shapes.
    assert extract_dax_table_references("'Sales'") == ["Sales"]
    assert extract_dax_table_references("DISTINCT('Customers')") == ["Customers"]
    assert extract_dax_table_references("VALUES('Region')") == ["Region"]
    assert extract_dax_table_references("UNION('A', 'B')") == ["A", "B"]
    assert extract_dax_table_references("NATURALINNERJOIN('A', 'B')") == ["A", "B"]


def test_expression_without_table_reference() -> None:
    assert extract_dax_table_references("1 + 1") == []


def test_malformed_expression_is_handled_gracefully() -> None:
    # PyDAX does not raise on malformed input; extraction is best-effort (any
    # stray tokens it lexes are validated away by the mapper), so it must return
    # a list without raising. The try/except is defensive against future changes.
    for expr in ["#(*&@ not dax", "SUMMARIZE(", "", "(((("]:
        assert isinstance(extract_dax_table_references(expr), list)
