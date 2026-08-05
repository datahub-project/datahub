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


def test_quoted_standalone_table_reference_forms() -> None:
    # Quoted standalone table references land on `table_references` and are
    # provably tables (single quotes are DAX's table-name syntax).
    assert extract_dax_table_references("'Sales'") == ["Sales"]
    assert extract_dax_table_references("DISTINCT('Customers')") == ["Customers"]
    assert extract_dax_table_references("VALUES('Region')") == ["Region"]
    assert extract_dax_table_references("UNION('A', 'B')") == ["A", "B"]
    assert extract_dax_table_references("NATURALINNERJOIN('A', 'B')") == ["A", "B"]


def test_unquoted_bare_identifiers_are_not_treated_as_tables() -> None:
    # DAX allows an unqualified *column* in the same position as a table, so a
    # bare identifier is ambiguous. Emitting it would fabricate an edge whenever
    # a dimension table and a column share a name (Date, Region, Product), which
    # name validation cannot detect.
    assert extract_dax_table_references('SUMMARIZE(Sales, Region, "T", SUM(Amt))') == []
    assert (
        extract_dax_table_references('DISTINCT(SELECTCOLUMNS(Sales, "r", Region))')
        == []
    )


def test_quoted_table_still_found_alongside_unquoted_columns() -> None:
    # The quoted table is kept; the unqualified columns beside it are not.
    assert extract_dax_table_references(
        "SUMMARIZE('Sales', Region, 'Cust'[Name], Amt)"
    ) == ["Cust", "Sales"]


def test_expression_without_table_reference() -> None:
    assert extract_dax_table_references("1 + 1") == []


def test_malformed_expression_is_handled_gracefully() -> None:
    # PyDAX does not raise on malformed input; extraction is best-effort (any
    # stray tokens it lexes are validated away by the mapper), so it must return
    # a list without raising. The try/except is defensive against future changes.
    for expr in ["#(*&@ not dax", "SUMMARIZE(", "", "(((("]:
        assert isinstance(extract_dax_table_references(expr), list)
