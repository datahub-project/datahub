from unittest.mock import MagicMock

from graphql import build_schema

from lib.graphql import resolve_operation_document
from lib.graphql_adapt import GraphqlQueryRegistry
from lib.query_build import build_operation_document
from lib.query_spec import QuerySpec

MINIMAL_SCHEMA = build_schema(
    """
    type Query {
      me: CorpUser
    }
    type CorpUser {
      urn: String
      username: String
    }
    """
)


def test_build_operation_document_minimal() -> None:
    spec = QuerySpec(
        operation_name="getMe",
        root="me",
        paths=["me.urn", "me.username"],
        variables=[],
        root_args={},
        expand_types=[],
        field_overrides={},
        required_paths=["me.urn"],
    )
    doc, omitted = build_operation_document(spec, MINIMAL_SCHEMA)
    assert omitted == []
    assert "query getMe" in doc
    assert "urn" in doc
    assert "username" in doc


def test_build_omits_missing_paths() -> None:
    spec = QuerySpec(
        operation_name="getMe",
        root="me",
        paths=["me.urn", "me.missingField"],
        variables=[],
        root_args={},
        expand_types=[],
        field_overrides={},
        required_paths=["me.urn"],
    )
    doc, omitted = build_operation_document(spec, MINIMAL_SCHEMA)
    assert "me.missingField" in omitted
    assert "missingField" not in doc


def test_resolve_operation_document_with_registry() -> None:
    spec = QuerySpec(
        operation_name="getMe",
        root="me",
        paths=["me.urn"],
        variables=[],
        root_args={},
        expand_types=[],
        field_overrides={},
        required_paths=["me.urn"],
    )
    registry = GraphqlQueryRegistry.__new__(GraphqlQueryRegistry)
    registry._documents = {}
    registry._omitted = {}
    registry._schema = MINIMAL_SCHEMA
    registry.specs = {"getMe": spec}
    registry.build_all = MagicMock()  # type: ignore[method-assign]

    doc = resolve_operation_document(registry, "getMe")
    assert "urn" in doc
