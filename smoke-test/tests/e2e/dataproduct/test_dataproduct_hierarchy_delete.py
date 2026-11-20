import logging
from typing import Optional

import pytest

from datahub.configuration.common import OperationalError
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import DataProductPropertiesClass, StatusClass
from tests.e2e.utils import delete_urn, execute_graphql, unique_suffix, with_test_retry
from utilities.domains import Domain

logger = logging.getLogger(__name__)

pytestmark = pytest.mark.domain(Domain.CATALOG)

DELETE_DATA_PRODUCT_QUERY = """
mutation deleteDataProduct($urn: String!) {
  deleteDataProduct(urn: $urn)
}
"""

CHILD_COUNT_QUERY = """
query childCount($urn: String!) {
  dataProduct(urn: $urn) {
    childDataProducts(input: { count: 0, query: "*" }) {
      total
    }
  }
}
"""


def _emit_data_product(
    graph_client: DataHubGraph, urn: str, name: str, parent: Optional[str] = None
) -> None:
    graph_client.emit(
        MetadataChangeProposalWrapper(
            entityUrn=urn,
            aspect=DataProductPropertiesClass(name=name, parentDataProduct=parent),
        )
    )


@with_test_retry()
def _wait_for_child_count(auth_session, parent: str, expected: int) -> None:
    # The delete check finds children through search, so wait until the index reflects them.
    res = execute_graphql(auth_session, CHILD_COUNT_QUERY, {"urn": parent})
    assert res["data"]["dataProduct"]["childDataProducts"]["total"] == expected


def _is_removed(graph_client: DataHubGraph, urn: str) -> bool:
    status = graph_client.get_aspect(urn, StatusClass)
    return status is not None and status.removed


@with_test_retry()
def _assert_gone(graph_client: DataHubGraph, urn: str) -> None:
    # Reference cleanup runs after the GraphQL call returns, so the parent can outlive the response.
    assert not graph_client.exists(urn)


@with_test_retry()
def _assert_child_released(graph_client: DataHubGraph, child: str) -> None:
    child_properties = graph_client.get_aspect(child, DataProductPropertiesClass)
    assert child_properties is not None
    assert child_properties.parentDataProduct is None


@with_test_retry()
def _assert_no_parent_edge(graph_client: DataHubGraph, child: str, parent: str) -> None:
    parents = [
        e.urn
        for e in graph_client.get_related_entities(
            child,
            relationship_types=["IsPartOf"],
            direction=DataHubGraph.RelationshipDirection.OUTGOING,
        )
    ]
    assert parent not in parents


def test_parent_with_child_is_deleted_and_child_released(auth_session, graph_client):
    suffix = unique_suffix()
    parent = f"urn:li:dataProduct:delete_guard_parent_{suffix}"
    child = f"urn:li:dataProduct:delete_guard_child_{suffix}"

    try:
        _emit_data_product(graph_client, parent, f"Parent {suffix}")
        _emit_data_product(graph_client, child, f"Child {suffix}", parent=parent)
        _wait_for_child_count(auth_session, parent, 1)

        # Soft delete leaves the child's pointer in place, so the guard still rejects it.
        with pytest.raises(OperationalError):
            graph_client.soft_delete_entity(parent)
        assert not _is_removed(graph_client, parent)

        # The REST hard delete is guarded while the child still points at the parent.
        with pytest.raises(OperationalError):
            graph_client.hard_delete_entity(parent)
        assert graph_client.exists(parent)
        child_properties = graph_client.get_aspect(child, DataProductPropertiesClass)
        assert child_properties is not None
        assert child_properties.parentDataProduct == parent

        # One child is under the reference-cleanup limit, so the GraphQL delete clears the
        # child's pointer and then removes the parent.
        res = execute_graphql(
            auth_session,
            DELETE_DATA_PRODUCT_QUERY,
            {"urn": parent},
        )
        assert not res.get("errors"), res.get("errors")
        assert res["data"]["deleteDataProduct"] is True

        _assert_gone(graph_client, parent)
        _assert_child_released(graph_client, child)
        _assert_no_parent_edge(graph_client, child, parent)
    finally:
        for urn in (child, parent):
            try:
                delete_urn(graph_client, urn)
            except Exception:
                logger.warning("cleanup failed for %s", urn, exc_info=True)
