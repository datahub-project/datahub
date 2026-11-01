from typing import List, Optional, Tuple

from datahub.ingestion.source.powerbi.rest_api_wrapper.dax_parser import (
    extract_dax_references,
    strip_comments_and_strings,
)


def _fields(expression: str) -> List[Tuple[Optional[str], str]]:
    return [(r.table, r.field) for r in extract_dax_references(expression).fields]


def _tables(expression: str) -> List[str]:
    return extract_dax_references(expression).tables


def test_qualified_bare_and_measure_references() -> None:
    expression = (
        "DIVIDE(SUM('Claims'[PaidAmount]), Policies[WrittenPremium]) + [Total Premium]"
    )
    assert _fields(expression) == [
        ("Claims", "PaidAmount"),
        ("Policies", "WrittenPremium"),
        (None, "Total Premium"),
    ]
    assert _tables(expression) == ["Claims", "Policies"]


def test_quoted_table_with_spaces_and_escaped_quote() -> None:
    expression = "SUM('Insurance KPIs'[Value]) + COUNTROWS('O''Brien Claims')"
    assert _fields(expression) == [("Insurance KPIs", "Value")]
    assert _tables(expression) == ["Insurance KPIs", "O'Brien Claims"]


def test_references_are_distinct_and_in_order_of_appearance() -> None:
    expression = "'A'[x] + 'B'[y] + 'A'[x] + COUNTROWS('B') + COUNTROWS('C')"
    assert _fields(expression) == [("A", "x"), ("B", "y")]
    assert _tables(expression) == ["A", "B", "C"]


def test_keyword_before_bare_measure_is_not_a_table() -> None:
    expression = "VAR Losses = [Total Paid Losses]\nRETURN [Loss Ratio]"
    assert _fields(expression) == [
        (None, "Total Paid Losses"),
        (None, "Loss Ratio"),
    ]
    assert _tables(expression) == []


def test_row_context_variables_are_not_fields() -> None:
    expression = "ADDCOLUMNS(SUMMARIZE('Policies', 'Policies'[State]), \"@Count\", 1)"
    assert _fields(expression) == [("Policies", "State")]
    assert _tables(expression) == ["Policies"]


def test_table_arguments_of_table_functions() -> None:
    expression = (
        "RELATED(Policies[State]) + COUNTROWS(RELATEDTABLE(Claims)) "
        "+ COUNTROWS(FILTER(Policies, Policies[State] = 1)) "
        "+ LOOKUPVALUE(Rates[Rate], Rates[State], [State]) "
        "+ COUNTROWS(CALCULATETABLE(VALUES(Regions))) + COUNTROWS(ALL(Dates))"
    )
    assert _tables(expression) == [
        "Policies",
        "Rates",
        "Claims",
        "Regions",
        "Dates",
    ]
    assert (None, "State") in _fields(expression)


def test_comments_and_string_literals_are_ignored() -> None:
    expression = """
        // 'Ghost'[Comment] is not a reference
        -- Neither is Other[Field]
        /* nor
           'Multi'[Line] */
        SUM('Claims'[PaidAmount]) + CALCULATE(COUNTROWS('Claims'), 'Claims'[Status] = "Open[Bracket]")
        + IF([Flag] = "it's 'quoted'", 1, 0) + "" & "a""b" & 'Real'[Col]
    """
    assert _fields(expression) == [
        ("Claims", "PaidAmount"),
        ("Claims", "Status"),
        (None, "Flag"),
        ("Real", "Col"),
    ]
    assert _tables(expression) == ["Claims", "Real"]


def test_strip_keeps_code_outside_comments_and_strings() -> None:
    assert strip_comments_and_strings('a // c\n"x""y" + b /* d */ -- e\nf').split() == [
        "a",
        "+",
        "b",
        "f",
    ]


def test_quoted_identifiers_survive_the_stripper() -> None:
    expression = (
        "'My \"Quoted\" Table'[Col] + 'Sales // Returns'[Amount] "
        '+ "a""[x]" + MYFUNC(Policies)'
    )
    assert _fields(expression) == [
        ('My "Quoted" Table', "Col"),
        ("Sales // Returns", "Amount"),
    ]
    # MYFUNC is not a known table function, so its bare argument is not a table.
    assert _tables(expression) == ['My "Quoted" Table', "Sales // Returns"]


def test_row_context_iterators_are_flagged() -> None:
    assert not extract_dax_references(
        "[Premium] * 2 + RELATED(Policies[State])"
    ).has_row_context_iterators
    iterated = extract_dax_references("SUMX(RELATEDTABLE(Policies), [Premium])")
    assert iterated.has_row_context_iterators
    assert iterated.tables == ["Policies"]
    assert _fields("SUMX(RELATEDTABLE(Policies), [Premium])") == [(None, "Premium")]
    # A comment or string naming an iterator does not count.
    assert not extract_dax_references(
        '[A] // SUMX(...)\n+ "FILTER("'
    ).has_row_context_iterators


def test_empty_expression() -> None:
    assert extract_dax_references(None).fields == []
    assert extract_dax_references("").tables == []
