import logging
from typing import List

import pytest

from datahub.configuration.common import OperationalError
from datahub.emitter.mce_builder import make_dataset_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import StructuredPropertyDefinitionClass
from datahub.utilities.urns.urn import Urn
from tests.e2e.utils import delete_urns, execute_graphql, unique_suffix
from utilities.consistency_utils import wait_for_writes_to_sync
from utilities.domains import Domain

logger = logging.getLogger(__name__)

pytestmark = pytest.mark.domain(Domain.CATALOG)

dataset_urn = make_dataset_urn("snowflake", f"validator_error_test_{unique_suffix()}")

# Everything this module creates, so cleanup runs even when an assertion aborts a test.
generated_urns: List[str] = []


@pytest.fixture(scope="module", autouse=True)
def cleanup_generated_urns(graph_client):
    try:
        yield
    finally:
        logger.info("removing structured property validator test data")
        delete_urns(graph_client, generated_urns)
        wait_for_writes_to_sync()


def create_property_with_id(
    property_id: str, graph: DataHubGraph, value_type: str = "number"
) -> str:
    """Emits a propertyDefinition MCP directly under the given raw urn id (bypassing any
    client-side name sanitization) so server-side validators are the only thing standing
    between this call and success."""
    property_urn = f"urn:li:structuredProperty:{property_id}"
    # Register before emitting: a partially-applied write still needs cleaning up.
    generated_urns.append(property_urn)
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


def test_validator_rejection_over_graphql_is_400_with_validation_source(auth_session):
    """The GraphQL path end to end: a validator rejection comes back as BAD_REQUEST with the
    validator's own text and the errorSource extension the UI uses to show it verbatim."""
    property_id = f"petshop weight {unique_suffix()}"
    generated_urns.append(f"urn:li:structuredProperty:{property_id}")
    res = execute_graphql(
        auth_session,
        """
        mutation createStructuredProperty($input: CreateStructuredPropertyInput!) {
          createStructuredProperty(input: $input) { urn }
        }
        """,
        variables={
            "input": {
                "id": property_id,
                "qualifiedName": property_id,
                "valueType": "urn:li:dataType:datahub.string",
                "entityTypes": ["urn:li:entityType:datahub.dataset"],
                "cardinality": "SINGLE",
            }
        },
        expect_errors=True,
    )

    errors = res.get("errors") or []
    assert len(errors) == 1, f"expected one error, got: {res}"
    error = errors[0]
    assert "Urn ID cannot have spaces" in error["message"]
    assert "ValidationExceptionCollection" not in error["message"]
    assert error["extensions"]["code"] == 400
    assert error["extensions"]["errorSource"] == "VALIDATION"


def test_bulk_validator_rejection_reports_message_once(auth_session, graph_client):
    """One validator rejecting every item of a batch reports its message once, not per item."""
    property_id = f"petshop_weight_kg_{unique_suffix()}"
    property_urn = create_property_with_id(property_id, graph_client)
    dataset_urns = [
        make_dataset_urn("hive", f"petshop.pet_orders_{i}_{unique_suffix()}")
        for i in range(3)
    ]
    generated_urns.extend(dataset_urns)

    response = auth_session.post(
        f"{auth_session.gms_url()}/openapi/v3/entity/dataset?async=false",
        json=[
            {
                "urn": urn,
                "structuredProperties": {
                    "value": {
                        "properties": [
                            {
                                "propertyUrn": property_urn,
                                "values": [{"string": "heavy"}],
                            }
                        ]
                    }
                },
            }
            for urn in dataset_urns
        ],
    )

    assert response.status_code == 400, response.text
    message = response.json()["message"]
    assert message.count("should be a number") == 1, message
