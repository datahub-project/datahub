from unittest.mock import MagicMock

from lib.graphql import resolve_operation_document


def test_resolve_operation_document_delegates_to_registry() -> None:
    registry = MagicMock()
    registry.document.return_value = "query getMe { me { corpUser { urn } } }"
    doc = resolve_operation_document(registry, "getMe")
    assert doc.startswith("query getMe")
    registry.document.assert_called_once_with("getMe")
