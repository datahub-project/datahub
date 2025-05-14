import pathlib
from unittest.mock import Mock

import pytest

import datahub.metadata.schema_classes as models
from datahub.emitter.mcp_builder import DatabaseKey, SchemaKey
from datahub.errors import ItemNotFoundError, SdkUsageError
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.urns import DatasetUrn, TagUrn
from datahub.sdk.container import Container
from datahub.sdk.dataset import Dataset
from datahub.sdk.main_client import DataHubClient
from tests.test_helpers import mce_helpers

_GOLDEN_DIR = pathlib.Path(__file__).parent / "entity_client_goldens"


@pytest.fixture
def mock_graph() -> Mock:
    graph = Mock(spec=DataHubGraph)
    graph.exists.return_value = False
    return graph


@pytest.fixture
def client(mock_graph: Mock) -> DataHubClient:
    return DataHubClient(graph=mock_graph)


def assert_client_golden(client: DataHubClient, golden_path: pathlib.Path) -> None:
    mcps = client._graph.emit_mcps.call_args[0][0]  # type: ignore
    mce_helpers.check_goldens_stream(
        outputs=mcps,
        golden_path=golden_path,
        ignore_order=False,
    )


def test_container_creation_flow(client: DataHubClient, mock_graph: Mock) -> None:
    # Create database and schema containers
    db = DatabaseKey(platform="snowflake", database="test_db")
    schema = SchemaKey(**db.dict(), schema="test_schema")

    db_container = Container(db, display_name="test_db", subtype="Database")
    schema_container = Container(schema, display_name="test_schema", subtype="Schema")

    # Test database container creation
    client.entities.upsert(db_container)
    assert_client_golden(client, _GOLDEN_DIR / "test_container_db_golden.json")

    # Test schema container creation
    client.entities.upsert(schema_container)
    assert_client_golden(client, _GOLDEN_DIR / "test_container_schema_golden.json")


def test_dataset_creation(client: DataHubClient, mock_graph: Mock) -> None:
    schema = SchemaKey(platform="snowflake", database="test_db", schema="test_schema")

    dataset = Dataset(
        platform="snowflake",
        name="test_db.test_schema.table_1",
        env="prod",
        parent_container=schema,
        schema=[
            ("col1", "string"),
            ("col2", "int"),
        ],
        description="test description",
        tags=[TagUrn("tag1")],
    )

    client.entities.create(dataset)
    assert_client_golden(client, _GOLDEN_DIR / "test_dataset_creation_golden.json")


def test_dataset_read_modify_write(client: DataHubClient, mock_graph: Mock) -> None:
    # Setup mock for existing dataset
    mock_graph.exists.return_value = True
    dataset_urn = DatasetUrn(
        platform="snowflake", name="test_db.test_schema.table_1", env="prod"
    )

    # Mock the get_entity_semityped response with initial state
    mock_graph.get_entity_semityped.return_value = {
        "datasetProperties": models.DatasetPropertiesClass(
            description="original description",
            customProperties={},
            tags=[],
        )
    }

    # Get and update dataset
    dataset = client.entities.get(dataset_urn)
    dataset.set_description("updated description")

    client.entities.update(dataset)
    assert_client_golden(client, _GOLDEN_DIR / "test_dataset_update_golden.json")


def test_container_read_modify_write(client: DataHubClient, mock_graph: Mock) -> None:
    database_key = DatabaseKey(platform="snowflake", database="test_db")
    container_urn = database_key.as_urn_typed()

    # Setup mocks for the container.
    mock_graph.exists.return_value = True
    mock_graph.get_entity_semityped.return_value = {
        "containerProperties": models.ContainerPropertiesClass(
            name="test_db",
        )
    }

    # Get and update the container
    container = client.entities.get(container_urn)
    container.set_description("updated description")

    client.entities.update(container)
    assert_client_golden(client, _GOLDEN_DIR / "test_container_update_golden.json")


def test_create_existing_dataset_fails(client: DataHubClient, mock_graph: Mock) -> None:
    mock_graph.exists.return_value = True

    dataset = Dataset(
        platform="snowflake",
        name="test_db.test_schema.table_1",
        env="prod",
        schema=[("col1", "string")],
    )

    with pytest.raises(SdkUsageError, match="Entity .* already exists"):
        client.entities.create(dataset)


def test_get_nonexistent_dataset_fails(client: DataHubClient, mock_graph: Mock) -> None:
    mock_graph.exists.return_value = False

    dataset_urn = DatasetUrn(
        platform="snowflake", name="test_db.test_schema.missing_table", env="prod"
    )

    with pytest.raises(ItemNotFoundError, match="Entity .* not found"):
        client.entities.get(dataset_urn)
