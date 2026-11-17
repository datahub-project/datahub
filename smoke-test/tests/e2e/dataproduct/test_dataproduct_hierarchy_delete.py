import logging
from typing import Optional

import pytest

from datahub.configuration.common import OperationalError
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import DataProductPropertiesClass, StatusClass
from tests.e2e.utils import (
    delete_urn,
    execute_graphql,
    unique_suffix,
    wait_for_writes_to_sync,
    with_test_retry,
)
from utilities.domains import Domain

logger = logging.getLogger(__name__)

pytestmark = pytest.mark.domain(Domain.CATALOG)

DELETE_DATA_PRODUCT_QUERY = """
mutation deleteDataProduct($urn: String!) {
  deleteDataProduct(urn: $urn)
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


def _is_removed(graph_client: DataHubGraph, urn: str) -> bool:
    status = graph_client.get_aspect(urn, StatusClass)
    return status is not None and status.removed


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


def test_parent_with_children_cannot_be_deleted(auth_session, graph_client):
    suffix = unique_suffix()
    parent = f"urn:li:dataProduct:delete_guard_parent_{suffix}"
    child = f"urn:li:dataProduct:delete_guard_child_{suffix}"

    try:
        _emit_data_product(graph_client, parent, f"Parent {suffix}")
        _emit_data_product(graph_client, child, f"Child {suffix}", parent=parent)
        # The delete check finds children through search.
        wait_for_writes_to_sync()

        res = execute_graphql(
            auth_session,
            DELETE_DATA_PRODUCT_QUERY,
            {"urn": parent},
            expect_errors=True,
        )
        assert res.get("errors"), "Deleting a parent with children should fail"
        # The error names the parent so the user knows which product is blocked.
        assert f"Parent {suffix}" in res["errors"][0]["message"]

        with pytest.raises(OperationalError):
            graph_client.soft_delete_entity(parent)
        with pytest.raises(OperationalError):
            graph_client.hard_delete_entity(parent)

        assert graph_client.exists(parent)
        assert not _is_removed(graph_client, parent)
        child_properties = graph_client.get_aspect(child, DataProductPropertiesClass)
        assert child_properties is not None
        assert child_properties.parentDataProduct == parent

        # Moving the child to the root releases the parent.
        _emit_data_product(graph_client, child, f"Child {suffix}")
        wait_for_writes_to_sync()

        graph_client.hard_delete_entity(parent)
        wait_for_writes_to_sync()

        assert not graph_client.exists(parent)
        _assert_no_parent_edge(graph_client, child, parent)
    finally:
        for urn in (child, parent):
            try:
                delete_urn(graph_client, urn)
            except Exception:
                logger.warning("cleanup failed for %s", urn, exc_info=True)
