import logging
from random import randint

import pytest

from datahub.configuration.common import OperationalError
from datahub.emitter.mce_builder import make_dataset_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import (
    StructuredPropertiesClass,
    StructuredPropertyDefinitionClass,
    StructuredPropertyValueAssignmentClass,
)
from datahub.utilities.urns.urn import Urn
from tests.consistency_utils import wait_for_writes_to_sync
from tests.utils import delete_urn

logger = logging.getLogger(__name__)

dataset_urn = make_dataset_urn(
    "snowflake", f"validator_error_test_{randint(10, 10000)}"
)


def create_property_with_id(
    property_id: str, graph: DataHubGraph, value_type: str = "number"
) -> str:
    """Emits a propertyDefinition MCP directly under the given raw urn id (bypassing any
    client-side name sanitization) so server-side validators are the only thing standing
    between this call and success."""
    property_urn = f"urn:li:structuredProperty:{property_id}"
    mcp = MetadataChangeProposalWrapper(
        entityUrn=property_urn,
        aspect=StructuredPropertyDefinitionClass(
            qualifiedName=property_id,
            valueType=Urn.make_data_type_urn(value_type),
            description="Test property for validator error message propagation",
            entityTypes=[Urn.make_entity_type_urn("dataset")],
            cardinality="SINGLE",
        ),
    )
    graph.emit_mcp(mcp)
    wait_for_writes_to_sync()
    return property_urn


def test_validator_rejection_message_propagates_cleanly(graph_client):
    """Regression guard for PR #16336: a validator rejection (urnIdCheck's 'Urn ID cannot have
    spaces') must reach the client as the validator's own message, not the verbose
    ValidationExceptionCollection{EntityAspect:... Exceptions: [...]} dump that used to be
    thrown as the exception's message."""
    with pytest.raises(OperationalError) as excinfo:
        create_property_with_id("test id with spaces", graph_client)

    message = excinfo.value.message
    assert "Urn ID cannot have spaces" in message, (
        f"expected the validator's own message to reach the client, got: {message}"
    )
    assert "ValidationExceptionCollection" not in message, (
        f"verbose collection dump leaked into the client-facing message: {message}"
    )
    assert "EntityAspect:" not in message


def _numeric_metric_facet(graph: DataHubGraph, property_id: str):
    query = """
    query aggregateAcrossEntities($input: AggregateAcrossEntitiesInput!) {
      aggregateAcrossEntities(input: $input) {
        facets {
          field
          aggregations {
            value
          }
        }
      }
    }
    """
    facet_field = f"structuredProperties.{property_id}"
    result = graph.execute_graphql(
        query,
        variables={
            "input": {
                "query": "*",
                "facets": [facet_field],
                "types": ["DATASET"],
            }
        },
    )
    facets = result["aggregateAcrossEntities"]["facets"] or []
    return next((f for f in facets if f["field"] == facet_field), None)


def test_hard_deleted_structured_property_rolls_up_out_of_search(graph_client):
    """A hard-deleted structured property definition must stop appearing as a search facet -
    the delete needs to roll up into the search index, not just remove the entity's own aspect."""
    property_id = f"delete_rollup_test_{randint(10, 10000)}"
    property_urn = create_property_with_id(property_id, graph_client)

    mcp = MetadataChangeProposalWrapper(
        entityUrn=dataset_urn,
        aspect=StructuredPropertiesClass(
            properties=[
                StructuredPropertyValueAssignmentClass(
                    propertyUrn=property_urn, values=[42.0]
                )
            ]
        ),
    )
    graph_client.emit_mcp(mcp)
    wait_for_writes_to_sync()

    facet_before = _numeric_metric_facet(graph_client, property_id)
    assert facet_before is not None, "property should be aggregatable before delete"
    assert any(agg["value"] == "42.0" for agg in facet_before["aggregations"])

    delete_urn(graph_client, property_urn)
    wait_for_writes_to_sync()

    facet_after = _numeric_metric_facet(graph_client, property_id)
    assert facet_after is None or not facet_after["aggregations"], (
        f"deleted property still has aggregatable values in search: {facet_after}"
    )

    delete_urn(graph_client, dataset_urn)
