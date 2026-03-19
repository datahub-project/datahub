"""Tests for the M-Query binary bridge subprocess manager."""

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
    # At least one node must be a LetExpression
    kinds = {node["kind"] for node in node_map.values()}
    assert "LetExpression" in kinds
    _clear_bridge()


def test_bridge_raises_on_invalid_expression():
    """A syntax error returns MQueryParseError, not a crash."""
    from datahub.ingestion.source.powerbi.m_query._bridge import (
        MQueryParseError,
        _clear_bridge,
        get_bridge,
    )

    _clear_bridge()
    bridge = get_bridge()
    with pytest.raises(MQueryParseError) as exc_info:
        bridge.parse("### not valid M-Query ###")
    assert str(exc_info.value)  # message is non-empty
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
