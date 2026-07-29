from unittest.mock import MagicMock

from lib.graphql import resolve_operation_document


def test_resolve_get_me() -> None:
    registry = MagicMock()
    registry.document.return_value = "query getMe { me { platformPrivileges { managePolicies } } }"
    doc = resolve_operation_document(registry, "getMe")
    assert "query getMe" in doc
    assert "platformPrivileges" in doc
