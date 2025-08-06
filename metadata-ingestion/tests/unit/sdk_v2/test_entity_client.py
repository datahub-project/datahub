import pathlib
from dataclasses import dataclass
from typing import Optional, Tuple, Type, Union
from unittest.mock import Mock, patch

import pytest

import datahub.metadata.schema_classes as models
from datahub.emitter.mcp_builder import DatabaseKey, SchemaKey
from datahub.emitter.mcp_patch_builder import MetadataPatchProposal
from datahub.emitter.rest_emitter import EmitMode
from datahub.errors import ItemNotFoundError, SdkUsageError
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.urns import DatasetUrn, TagUrn, Urn
from datahub.sdk.container import Container
from datahub.sdk.dataset import Dataset
from datahub.sdk.main_client import DataHubClient
from datahub.testing import mce_helpers

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


@dataclass
class EntityClientDeleteTestParams:
    """Test parameters for the delete method."""

    urn: Union[str, Urn]
    check_exists: bool = True
    cascade: bool = False
    hard: bool = False
    entity_exists: bool = True
    expected_exception: Optional[Type[Exception]] = None
    expected_graph_exists_call: bool = True
    expected_delete_call: Optional[Tuple[str, bool]] = None


@pytest.mark.parametrize(
    "params",
    [
        pytest.param(
            EntityClientDeleteTestParams(
                urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.table,PROD)",
                check_exists=True,
                cascade=False,
                hard=False,
                entity_exists=True,
                expected_exception=None,
                expected_graph_exists_call=True,
                expected_delete_call=(
                    "urn:li:dataset:(urn:li:dataPlatform:snowflake,test.table,PROD)",
                    False,
                ),
            ),
            id="successful_soft_delete_with_exists_check",
        ),
        pytest.param(
            EntityClientDeleteTestParams(
                urn=DatasetUrn(platform="snowflake", name="test.table", env="prod"),
                check_exists=True,
                cascade=False,
                hard=True,
                entity_exists=True,
                expected_exception=None,
                expected_graph_exists_call=True,
                expected_delete_call=(
                    "urn:li:dataset:(urn:li:dataPlatform:snowflake,test.table,PROD)",
                    True,
                ),
            ),
            id="successful_hard_delete_with_urn_object",
        ),
        pytest.param(
            EntityClientDeleteTestParams(
                urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.table,PROD)",
                check_exists=False,
                cascade=False,
                hard=False,
                entity_exists=False,
                expected_exception=None,
                expected_graph_exists_call=False,
                expected_delete_call=(
                    "urn:li:dataset:(urn:li:dataPlatform:snowflake,test.table,PROD)",
                    False,
                ),
            ),
            id="delete_without_exists_check",
        ),
        pytest.param(
            EntityClientDeleteTestParams(
                urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.table,PROD)",
                check_exists=True,
                cascade=False,
                hard=False,
                entity_exists=False,
                expected_exception=SdkUsageError,
                expected_graph_exists_call=True,
                expected_delete_call=None,
            ),
            id="delete_nonexistent_entity_with_check",
        ),
        pytest.param(
            EntityClientDeleteTestParams(
                urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,test.table,PROD)",
                check_exists=True,
                cascade=True,
                hard=False,
                entity_exists=True,
                expected_exception=SdkUsageError,
                expected_graph_exists_call=True,
                expected_delete_call=None,
            ),
            id="cascade_delete_not_supported",
        ),
    ],
)
def test_delete_entity(
    client: DataHubClient,
    mock_graph: Mock,
    params: EntityClientDeleteTestParams,
) -> None:
    """Test delete method with various parameter combinations."""
    # Setup mock
    mock_graph.exists.return_value = params.entity_exists
    mock_graph.delete_entity = Mock()

    if params.expected_exception:
        # Test that expected exception is raised
        with pytest.raises(params.expected_exception):
            client.entities.delete(
                urn=params.urn,
                check_exists=params.check_exists,
                cascade=params.cascade,
                hard=params.hard,
            )
    else:
        # Test successful deletion
        client.entities.delete(
            urn=params.urn,
            check_exists=params.check_exists,
            cascade=params.cascade,
            hard=params.hard,
        )

    # Verify graph.exists was called correctly
    if params.expected_graph_exists_call:
        expected_urn_str = str(params.urn)
        mock_graph.exists.assert_called_once_with(entity_urn=expected_urn_str)
    else:
        mock_graph.exists.assert_not_called()

    # Verify graph.delete_entity was called correctly
    if params.expected_delete_call:
        expected_urn_str, expected_hard = params.expected_delete_call
        mock_graph.delete_entity.assert_called_once_with(
            urn=expected_urn_str, hard=expected_hard
        )
    else:
        mock_graph.delete_entity.assert_not_called()


def test_upsert_with_emit_mode(client: DataHubClient, mock_graph: Mock) -> None:
    """Test upsert method with emit_mode parameter."""
    dataset = Dataset(
        platform="snowflake",
        name="test_db.test_schema.table_1",
        env="prod",
        schema=[("col1", "string")],
    )

    # Test with ASYNC emit mode
    client.entities.upsert(dataset, emit_mode=EmitMode.ASYNC)

    # Verify emit_mcps was called with emit_mode
    mock_graph.emit_mcps.assert_called_once()
    call_args = mock_graph.emit_mcps.call_args
    assert "emit_mode" in call_args.kwargs
    assert call_args.kwargs["emit_mode"] == EmitMode.ASYNC


def test_upsert_without_emit_mode(client: DataHubClient, mock_graph: Mock) -> None:
    """Test upsert method without emit_mode parameter (default behavior)."""
    dataset = Dataset(
        platform="snowflake",
        name="test_db.test_schema.table_1",
        env="prod",
        schema=[("col1", "string")],
    )

    # Test without emit_mode (should use default)
    client.entities.upsert(dataset)

    # Verify emit_mcps was called without emit_mode
    mock_graph.emit_mcps.assert_called_once()
    call_args = mock_graph.emit_mcps.call_args
    assert (
        "emit_mode" not in call_args.kwargs or call_args.kwargs.get("emit_mode") is None
    )


def test_update_with_emit_mode(client: DataHubClient, mock_graph: Mock) -> None:
    """Test update method with emit_mode parameter."""
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

    # Get dataset and modify it
    dataset = client.entities.get(dataset_urn)
    dataset.set_description("updated description")

    # Test update with SYNC_WAIT emit mode
    client.entities.update(dataset, emit_mode=EmitMode.SYNC_WAIT)

    # Verify emit_mcps was called with emit_mode
    # Note: There are multiple calls (get + update), so we check the last call
    call_args = mock_graph.emit_mcps.call_args
    assert "emit_mode" in call_args.kwargs
    assert call_args.kwargs["emit_mode"] == EmitMode.SYNC_WAIT


def test_update_without_emit_mode(client: DataHubClient, mock_graph: Mock) -> None:
    """Test update method without emit_mode parameter (default behavior)."""
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

    # Get dataset and modify it
    dataset = client.entities.get(dataset_urn)
    dataset.set_description("updated description")

    # Test update without emit_mode (should use default)
    client.entities.update(dataset)

    # Verify emit_mcps was called without emit_mode
    call_args = mock_graph.emit_mcps.call_args
    assert (
        "emit_mode" not in call_args.kwargs or call_args.kwargs.get("emit_mode") is None
    )


def test_update_with_metadata_patch_proposal_and_emit_mode(
    client: DataHubClient, mock_graph: Mock
) -> None:
    """Test update method with MetadataPatchProposal and emit_mode parameter."""
    # Setup existing entity
    mock_graph.exists.return_value = True
    dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,test.table,PROD)"

    # Create a minimal MetadataPatchProposal - we don't need to add real patches for this test
    patch_proposal = MetadataPatchProposal(dataset_urn)
    # Mock the build method to return a valid MCP list
    with patch.object(patch_proposal, "build", return_value=[]):
        # Test update with patch proposal - emit_mode should be ignored for patch proposals
        client.entities.update(patch_proposal, emit_mode=EmitMode.ASYNC)

        # Verify the patch was processed (emit_mcps should be called)
        mock_graph.emit_mcps.assert_called_once()
        # For patch proposals, emit_mode is not passed through _update_patch method


def test_create_with_emit_mode(client: DataHubClient, mock_graph: Mock) -> None:
    """Test create method with emit_mode parameter."""
    dataset = Dataset(
        platform="snowflake",
        name="test_db.test_schema.table_1",
        env="prod",
        schema=[("col1", "string")],
    )

    # Test with ASYNC emit mode
    client.entities.create(dataset, emit_mode=EmitMode.ASYNC)

    # Verify emit_mcps was called with emit_mode
    mock_graph.emit_mcps.assert_called_once()
    call_args = mock_graph.emit_mcps.call_args
    assert "emit_mode" in call_args.kwargs
    assert call_args.kwargs["emit_mode"] == EmitMode.ASYNC


def test_create_without_emit_mode(client: DataHubClient, mock_graph: Mock) -> None:
    """Test create method without emit_mode parameter (default behavior)."""
    dataset = Dataset(
        platform="snowflake",
        name="test_db.test_schema.table_1",
        env="prod",
        schema=[("col1", "string")],
    )

    # Test without emit_mode (should use default)
    client.entities.create(dataset)

    # Verify emit_mcps was called without emit_mode
    mock_graph.emit_mcps.assert_called_once()
    call_args = mock_graph.emit_mcps.call_args
    assert (
        "emit_mode" not in call_args.kwargs or call_args.kwargs.get("emit_mode") is None
    )
