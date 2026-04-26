"""Tests for the Sigma formula bracket-reference extractor."""

from datahub.ingestion.source.sigma.formula_parser import extract_bracket_refs


def test_none_formula_returns_empty() -> None:
    assert extract_bracket_refs(None) == []


def test_empty_formula_returns_empty() -> None:
    assert extract_bracket_refs("") == []


def test_literal_only_returns_empty() -> None:
    assert extract_bracket_refs("42") == []


def test_sibling_ref() -> None:
    result = extract_bracket_refs("[col]")
    assert len(result) == 1
    ref = result[0]
    assert ref.source == "col"
    assert ref.column is None
    assert ref.is_parameter is False


def test_cross_element_ref() -> None:
    result = extract_bracket_refs("[Source/col]")
    assert len(result) == 1
    ref = result[0]
    assert ref.source == "Source"
    assert ref.column == "col"
    assert ref.is_parameter is False


def test_real_tenant_sample_m0() -> None:
    """Verifies the M0 probe sample: 97.6% of 127 observed formulas follow this pattern."""
    result = extract_bracket_refs("[Union of 1 Sources/win_by_runs]")
    assert len(result) == 1
    ref = result[0]
    assert ref.source == "Union of 1 Sources"
    assert ref.column == "win_by_runs"


def test_two_refs_in_order() -> None:
    result = extract_bracket_refs("[a] + [b]")
    assert len(result) == 2
    assert result[0].source == "a"
    assert result[1].source == "b"


def test_ref_inside_function() -> None:
    result = extract_bracket_refs("Count([x])")
    assert len(result) == 1
    assert result[0].source == "x"
    assert result[0].column is None


def test_two_refs_around_literal_slash_operator() -> None:
    """The `/` between brackets is an arithmetic operator, not a source/column separator."""
    result = extract_bracket_refs("[Selected Metric] / [Successful Runs]")
    assert len(result) == 2
    assert result[0].source == "Selected Metric"
    assert result[0].column is None
    assert result[1].source == "Successful Runs"
    assert result[1].column is None


def test_escaped_slash_in_column_name_no_split() -> None:
    r"""A `\/` inside a bracket body is a literal `/`, not a source/column separator."""
    result = extract_bracket_refs(r"[Rank of MAR \/ Modified]")
    assert len(result) == 1
    ref = result[0]
    assert ref.source == "Rank of MAR / Modified"
    assert ref.column is None


def test_escaped_slash_only_in_column_part() -> None:
    r"""Source ends at the first unescaped `/`; the `\/` in the column part becomes a literal `/`."""
    result = extract_bracket_refs(r"[Source/with \/ slash]")
    assert len(result) == 1
    ref = result[0]
    assert ref.source == "Source"
    assert ref.column == "with / slash"


def test_parameter_ref() -> None:
    result = extract_bracket_refs("[P_MyParam]")
    assert len(result) == 1
    ref = result[0]
    assert ref.is_parameter is True


def test_parameter_with_slash_is_not_parameter() -> None:
    """A `P_*` ref with a column part is NOT treated as a parameter per our defensive rule."""
    result = extract_bracket_refs("[P_X/col]")
    assert len(result) == 1
    ref = result[0]
    assert ref.source == "P_X"
    assert ref.column == "col"
    assert ref.is_parameter is False


def test_multiline_formula() -> None:
    formula = "If(\n  [a] > 0,\n  [b],\n  [c]\n)"
    result = extract_bracket_refs(formula)
    assert len(result) == 3
    assert result[0].source == "a"
    assert result[1].source == "b"
    assert result[2].source == "c"


def test_empty_bracket_skipped() -> None:
    """Empty brackets `[]` don't match — the body must be 1+ chars."""
    assert extract_bracket_refs("[]") == []
