import pytest

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.graph.config import DatahubClientConfig
from datahub.metadata.schema_classes import (
    DatasetPropertiesClass,
    DatasetSnapshotClass,
    MetadataChangeEventClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
)

# Test configuration
MOCK_GMS_ENDPOINT = "http://localhost:8080"
TEST_DATASET_URN = "urn:li:dataset:(urn:li:dataPlatform:bigquery,test_dataset,PROD)"
TEST_USER_URN = "urn:li:corpuser:test_user"


@pytest.fixture(scope="module")
def vcr_config(vcr_config):
    vcr_config["filter_headers"] = ["authorization"]
    return vcr_config


@pytest.fixture
def datahub_graph():
    """Create a DataHubGraph instance for testing."""
    return DataHubGraph(
        config=DatahubClientConfig(
            server=MOCK_GMS_ENDPOINT,
            token="test-token",
        )
    )


@pytest.mark.dependency()
@pytest.mark.vcr
def test_dataset_creation(datahub_graph):
    """Test creating a dataset using DataHubGraph."""
    # Create a dataset with basic properties
    dataset_properties = DatasetPropertiesClass(
        name="test_dataset",
        description="A test dataset",
        customProperties={},
    )

    dataset_snapshot = DatasetSnapshotClass(
        urn=TEST_DATASET_URN,
        aspects=[dataset_properties],
    )

    mce = MetadataChangeEventClass(proposedSnapshot=dataset_snapshot)

    # Emit the MCE
    datahub_graph.emit_mce(mce)

    # Verify the dataset was created by getting its properties
    properties = datahub_graph.get_aspect(
        entity_urn=TEST_DATASET_URN,
        aspect_type=DatasetPropertiesClass,
    )

    assert properties is not None
    assert properties.name == "test_dataset"
    assert properties.description == "A test dataset"


@pytest.mark.dependency(depends=["test_dataset_creation"])
@pytest.mark.vcr
def test_graphql_query(datahub_graph):
    """Test executing a GraphQL query."""
    # Define a simple GraphQL query to get dataset properties
    query = """
    query getDataset($urn: String!) {
        dataset(urn: $urn) {
            properties {
                name
                description
            }
        }
    }
    """

    # Execute the query
    result = datahub_graph.execute_graphql(
        query=query,
        variables={"urn": TEST_DATASET_URN},
    )

    # Verify the response
    assert "dataset" in result
    assert result["dataset"]["properties"]["name"] == "test_dataset"
    assert result["dataset"]["properties"]["description"] == "A test dataset"


@pytest.mark.dependency(depends=["test_dataset_creation"])
@pytest.mark.vcr
def test_ownership_update(datahub_graph):
    """Test updating ownership of a dataset."""
    # Create an ownership aspect
    ownership = OwnershipClass(
        owners=[
            OwnerClass(
                owner=TEST_USER_URN,
                type=OwnershipTypeClass.DATAOWNER,
            )
        ]
    )

    # Create an MCP to update ownership
    mcp = MetadataChangeProposalWrapper(
        entityUrn=TEST_DATASET_URN,
        aspect=ownership,
    )

    # Emit the MCP
    datahub_graph.emit_mcp(mcp)

    # Verify the ownership was updated
    ownership_aspect = datahub_graph.get_aspect(
        entity_urn=TEST_DATASET_URN,
        aspect_type=OwnershipClass,
    )

    assert ownership_aspect is not None
    assert len(ownership_aspect.owners) == 1
    assert ownership_aspect.owners[0].owner == TEST_USER_URN
    assert ownership_aspect.owners[0].type == OwnershipTypeClass.DATAOWNER


@pytest.mark.dependency(
    depends=["test_dataset_creation", "test_graphql_query", "test_ownership_update"]
)
@pytest.mark.vcr
def test_entity_deletion(datahub_graph):
    """Test deleting an entity."""
    # Delete the test dataset
    datahub_graph.delete_entity(TEST_DATASET_URN, hard=True)

    # Verify the entity was deleted by trying to get its properties
    properties = datahub_graph.get_aspect(
        entity_urn=TEST_DATASET_URN,
        aspect_type=DatasetPropertiesClass,
    )

    assert properties is None
