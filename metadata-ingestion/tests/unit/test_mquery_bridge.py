"""Tests for the M-Query V8 bridge (py_mini_racer + bundle.js.gz)."""

import pytest


def test_bridge_starts_on_current_platform():
    """Bridge binary exists and starts without error."""
    from datahub.ingestion.source.powerbi.m_query._bridge import (
        _clear_bridge,
        get_bridge,
    )

    _clear_bridge()
    bridge = get_bridge()
    assert bridge is not None
    _clear_bridge()


def test_bridge_parses_simple_let_expression():
    """A valid M-Query let expression returns a nodeIdMap with a LetExpression root."""
    from datahub.ingestion.source.powerbi.m_query._bridge import (
        _clear_bridge,
        get_bridge,
    )

    _clear_bridge()
    bridge = get_bridge()
    node_map = bridge.parse("let Source = 1 in Source")
    kinds = {node["kind"] for node in node_map.values()}
    assert "LetExpression" in kinds
    _clear_bridge()


def test_bridge_parses_minimal_section_document():
    """DefaultSettings use ParseEitherExpressionOrSection — a bare section parses."""
    from datahub.ingestion.source.powerbi.m_query._bridge import (
        _clear_bridge,
        get_bridge,
    )

    _clear_bridge()
    bridge = get_bridge()
    node_map = bridge.parse("section;")
    kinds = {node["kind"] for node in node_map.values()}
    assert "Section" in kinds
    _clear_bridge()


@pytest.mark.parametrize(
    ("expression", "required_substrings"),
    [
        # Lex stage (tokenization) — inputs that never reach the parser
        (
            "###",
            ("lex:", "line"),
        ),
        (
            '"unterminated',
            ("lex:", "unterminated"),
        ),
        # Parse stage
        (
            "let x =",
            ("parse:", "end-of-stream"),
        ),
        (
            "",
            ("parse:", "end-of-stream"),
        ),
        (
            "let x = ) in x",
            ("parse:", "parenthesis"),
        ),
        (
            "let Source = 1 in Source extra",
            ("parse:", "tokens remain"),
        ),
        (
            "let Source = Sql.Database( in Source",
            ("parse:", "keyword"),
        ),
        (
            "shared x = 1;",
            ("parse:", "shared"),
        ),
    ],
)
def test_bridge_errors_include_stage_and_details(
    expression: str, required_substrings: tuple[str, ...]
) -> None:
    """Lex/Parse failures surface as MQueryParseError with Lex:/Parse: prefix and message text."""
    from datahub.ingestion.source.powerbi.m_query._bridge import (
        MQueryParseError,
        _clear_bridge,
        get_bridge,
    )

    _clear_bridge()
    bridge = get_bridge()
    with pytest.raises(MQueryParseError) as exc_info:
        bridge.parse(expression)
    message = str(exc_info.value).lower()
    for needle in required_substrings:
        assert needle in message, f"expected {needle!r} in {message!r}"
    _clear_bridge()


def test_bridge_restart_after_clear():
    """After _clear_bridge(), a fresh get_bridge() call succeeds."""
    from datahub.ingestion.source.powerbi.m_query._bridge import (
        _clear_bridge,
        get_bridge,
    )

    _clear_bridge()
    b1 = get_bridge()
    _clear_bridge()
    b2 = get_bridge()
    assert b2 is not b1
    _clear_bridge()
