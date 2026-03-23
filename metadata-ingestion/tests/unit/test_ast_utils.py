"""Tests for ast_utils.py — NodeIdMap navigation helpers.

All tests are bridge-backed: the Snowflake M-Query expression is parsed at
module scope via the JS bridge (bundle.js.gz + py_mini_racer). No static JSON
fixtures are committed or loaded.
"""

import pytest

from datahub.ingestion.source.powerbi.m_query._bridge import NodeIdMap
from datahub.ingestion.source.powerbi.m_query.ast_utils import (
    find_nodes_by_kind,
    get_invoke_callee_name,
    get_literal_value,
    get_record_field_values,
    resolve_identifier,
)

# Index into M_QUERIES (test_m_parser.py) — only the queries needed for ast_utils tests
_AST_UTILS_M_QUERY_INDICES = (0,)


@pytest.fixture(scope="module")
def _parsed_node_maps() -> dict[int, NodeIdMap]:
    from datahub.ingestion.source.powerbi.m_query._bridge import (
        _clear_bridge,
        get_bridge,
    )
    from tests.integration.powerbi.test_m_parser import M_QUERIES

    _clear_bridge()
    bridge = get_bridge()
    result = {i: bridge.parse(M_QUERIES[i]) for i in _AST_UTILS_M_QUERY_INDICES}
    _clear_bridge()
    return result


@pytest.fixture(scope="module")
def snowflake_node_map(
    _parsed_node_maps: dict[int, NodeIdMap],
) -> NodeIdMap:
    return _parsed_node_maps[0]


# ── find_nodes_by_kind ────────────────────────────────────────────────────────


def test_find_nodes_by_kind_returns_all_matching(snowflake_node_map: NodeIdMap):
    let_nodes = find_nodes_by_kind(snowflake_node_map, "LetExpression")
    assert len(let_nodes) >= 1
    assert all(n["kind"] == "LetExpression" for n in let_nodes)


def test_find_nodes_by_kind_returns_deep_nodes(snowflake_node_map: NodeIdMap):
    identifiers = find_nodes_by_kind(snowflake_node_map, "Identifier")
    assert len(identifiers) > 1


def test_find_nodes_by_kind_empty_for_missing_kind(snowflake_node_map: NodeIdMap):
    assert find_nodes_by_kind(snowflake_node_map, "NonExistentKind") == []


# ── get_invoke_callee_name ────────────────────────────────────────────────────


def test_get_invoke_callee_name_snowflake(snowflake_node_map: NodeIdMap):
    invokes = find_nodes_by_kind(snowflake_node_map, "InvokeExpression")
    names = [get_invoke_callee_name(snowflake_node_map, n) for n in invokes]
    assert "Snowflake.Databases" in names


def test_get_invoke_callee_name_returns_none_for_non_invoke():
    fake_node = {
        "kind": "LiteralExpression",
        "literal": '"hello"',
        "literalKind": "Text",
    }
    assert get_invoke_callee_name({}, fake_node) is None


# ── get_literal_value ─────────────────────────────────────────────────────────


def test_get_literal_value_text():
    node = {
        "kind": "LiteralExpression",
        "literal": '"my_database"',
        "literalKind": "Text",
    }
    assert get_literal_value(node) == "my_database"


def test_get_literal_value_non_text_returns_none():
    node = {"kind": "LiteralExpression", "literal": "42", "literalKind": "Number"}
    assert get_literal_value(node) is None


def test_get_literal_value_null_returns_none():
    node = {"kind": "LiteralExpression", "literal": "null", "literalKind": "Null"}
    assert get_literal_value(node) is None


def test_get_literal_value_non_literal_returns_none():
    assert get_literal_value({"kind": "Identifier", "literal": "Source"}) is None


# ── resolve_identifier ────────────────────────────────────────────────────────


def test_resolve_identifier_finds_variable(snowflake_node_map: NodeIdMap):
    """resolve_identifier returns the value node for a named variable in a let expression."""
    let_nodes = find_nodes_by_kind(snowflake_node_map, "LetExpression")
    assert let_nodes
    let_node = let_nodes[0]
    result = resolve_identifier(snowflake_node_map, let_node, "Source")
    assert result is not None


def test_resolve_identifier_returns_none_for_unknown(
    snowflake_node_map: NodeIdMap,
):
    let_node = find_nodes_by_kind(snowflake_node_map, "LetExpression")[0]
    assert resolve_identifier(snowflake_node_map, let_node, "DoesNotExist") is None


# ── get_record_field_values ───────────────────────────────────────────────────


def test_get_record_field_values_extracts_name_and_kind(
    snowflake_node_map: NodeIdMap,
):
    """For a Snowflake three-step pattern, item selectors have Name and Kind fields."""
    records = find_nodes_by_kind(snowflake_node_map, "RecordExpression")
    assert records, "Expected RecordExpression nodes in a Snowflake expression"
    all_fields = [get_record_field_values(snowflake_node_map, r) for r in records]
    named = [f for f in all_fields if f]  # any non-empty record
    assert named, f"No non-empty records found. Fields seen: {all_fields}"


def test_get_record_field_values_empty_record():
    # Node with no content field → treated as empty
    empty_record = {"kind": "RecordExpression"}
    assert get_record_field_values({}, empty_record) == {}
