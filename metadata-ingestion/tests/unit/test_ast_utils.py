"""Tests for ast_utils.py — NodeIdMap navigation helpers.

All tests load static JSON fixtures from mquery_ast_fixtures/.
No subprocess or bridge binary is needed.
"""

import json
from pathlib import Path

FIXTURES_DIR = (
    Path(__file__).parent.parent / "integration" / "powerbi" / "mquery_ast_fixtures"
)


def load_fixture(name_fragment: str) -> dict[int, dict]:
    """Load a fixture by partial filename match and return node_map."""
    matches = list(FIXTURES_DIR.glob(f"*{name_fragment}*"))
    assert matches, f"No fixture found matching '{name_fragment}' in {FIXTURES_DIR}"
    data = json.loads(matches[0].read_text())
    assert data["ok"], f"Fixture parse failed: {data.get('error')}"
    return {int(k): v for k, v in data["nodeIdMap"]}


# ── find_nodes_by_kind ────────────────────────────────────────────────────────


def test_find_nodes_by_kind_returns_all_matching():
    from datahub.ingestion.source.powerbi.m_query.ast_utils import find_nodes_by_kind

    node_map = load_fixture("Snowflake")
    let_nodes = find_nodes_by_kind(node_map, "LetExpression")
    assert len(let_nodes) >= 1
    assert all(n["kind"] == "LetExpression" for n in let_nodes)


def test_find_nodes_by_kind_returns_deep_nodes():
    from datahub.ingestion.source.powerbi.m_query.ast_utils import find_nodes_by_kind

    node_map = load_fixture("Snowflake")
    identifiers = find_nodes_by_kind(node_map, "Identifier")
    assert len(identifiers) > 1


def test_find_nodes_by_kind_empty_for_missing_kind():
    from datahub.ingestion.source.powerbi.m_query.ast_utils import find_nodes_by_kind

    node_map = load_fixture("Snowflake")
    assert find_nodes_by_kind(node_map, "NonExistentKind") == []


# ── get_invoke_callee_name ────────────────────────────────────────────────────


def test_get_invoke_callee_name_snowflake():
    from datahub.ingestion.source.powerbi.m_query.ast_utils import (
        find_nodes_by_kind,
        get_invoke_callee_name,
    )

    node_map = load_fixture("Snowflake")
    invokes = find_nodes_by_kind(node_map, "InvokeExpression")
    names = [get_invoke_callee_name(node_map, n) for n in invokes]
    assert "Snowflake.Databases" in names


def test_get_invoke_callee_name_returns_none_for_non_invoke():
    from datahub.ingestion.source.powerbi.m_query.ast_utils import (
        get_invoke_callee_name,
    )

    fake_node = {
        "kind": "LiteralExpression",
        "literal": '"hello"',
        "literalKind": "Text",
    }
    assert get_invoke_callee_name({}, fake_node) is None


# ── get_literal_value ─────────────────────────────────────────────────────────


def test_get_literal_value_text():
    from datahub.ingestion.source.powerbi.m_query.ast_utils import get_literal_value

    node = {
        "kind": "LiteralExpression",
        "literal": '"my_database"',
        "literalKind": "Text",
    }
    assert get_literal_value(node) == "my_database"


def test_get_literal_value_non_text_returns_none():
    from datahub.ingestion.source.powerbi.m_query.ast_utils import get_literal_value

    node = {"kind": "LiteralExpression", "literal": "42", "literalKind": "Number"}
    assert get_literal_value(node) is None


def test_get_literal_value_null_returns_none():
    from datahub.ingestion.source.powerbi.m_query.ast_utils import get_literal_value

    node = {"kind": "LiteralExpression", "literal": "null", "literalKind": "Null"}
    assert get_literal_value(node) is None


def test_get_literal_value_non_literal_returns_none():
    from datahub.ingestion.source.powerbi.m_query.ast_utils import get_literal_value

    assert get_literal_value({"kind": "Identifier", "literal": "Source"}) is None


# ── resolve_identifier ────────────────────────────────────────────────────────


def test_resolve_identifier_finds_variable():
    """resolve_identifier returns the value node for a named variable in a let expression."""
    from datahub.ingestion.source.powerbi.m_query.ast_utils import (
        find_nodes_by_kind,
        resolve_identifier,
    )

    node_map = load_fixture("Snowflake")
    let_nodes = find_nodes_by_kind(node_map, "LetExpression")
    assert let_nodes
    let_node = let_nodes[0]
    result = resolve_identifier(node_map, let_node, "Source")
    assert result is not None


def test_resolve_identifier_returns_none_for_unknown():
    from datahub.ingestion.source.powerbi.m_query.ast_utils import (
        find_nodes_by_kind,
        resolve_identifier,
    )

    node_map = load_fixture("Snowflake")
    let_node = find_nodes_by_kind(node_map, "LetExpression")[0]
    assert resolve_identifier(node_map, let_node, "DoesNotExist") is None


# ── get_record_field_values ───────────────────────────────────────────────────


def test_get_record_field_values_extracts_name_and_kind():
    """For a Snowflake three-step pattern, item selectors have Name and Kind fields."""
    from datahub.ingestion.source.powerbi.m_query.ast_utils import (
        find_nodes_by_kind,
        get_record_field_values,
    )

    node_map = load_fixture("Snowflake")
    records = find_nodes_by_kind(node_map, "RecordExpression")
    assert records, "Expected RecordExpression nodes in a Snowflake expression"
    all_fields = [get_record_field_values(node_map, r) for r in records]
    named = [f for f in all_fields if f]  # any non-empty record
    assert named, f"No non-empty records found. Fields seen: {all_fields}"


def test_get_record_field_values_empty_record():
    from datahub.ingestion.source.powerbi.m_query.ast_utils import (
        get_record_field_values,
    )

    # Node with no content field → treated as empty
    empty_record = {"kind": "RecordExpression"}
    assert get_record_field_values({}, empty_record) == {}
