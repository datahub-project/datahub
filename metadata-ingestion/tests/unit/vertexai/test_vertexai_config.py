from unittest.mock import MagicMock, patch

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.common.gcp_project_filter import GcpProject
from datahub.ingestion.source.vertexai.vertexai import VertexAIConfig, VertexAISource


@patch("datahub.ingestion.source.vertexai.vertexai.resolve_gcp_projects")
def test_multi_project_initialization_with_explicit_ids(
    mock_resolve: MagicMock,
) -> None:
    """Test initialization with explicit project_ids"""
    mock_resolve.return_value = [
        GcpProject(id="project-1", name="Project 1"),
        GcpProject(id="project-2", name="Project 2"),
    ]

    config = VertexAIConfig.model_validate(
        {
            "project_id": "test-project",
            "region": "us-central1",
            "project_ids": ["project-1", "project-2"],
        }
    )
    source = VertexAISource(ctx=PipelineContext(run_id="test"), config=config)

    assert source._projects == ["project-1", "project-2"]
    mock_resolve.assert_called_once()


@patch("datahub.ingestion.source.vertexai.vertexai.resolve_gcp_projects")
def test_multi_project_initialization_with_labels(mock_resolve: MagicMock) -> None:
    """Test initialization with project labels"""
    mock_resolve.return_value = [
        GcpProject(id="dev-project", name="Development"),
        GcpProject(id="prod-project", name="Production"),
    ]

    config = VertexAIConfig.model_validate(
        {
            "project_id": "test-project",
            "region": "us-central1",
            "project_labels": ["env:prod", "team:ml"],
        }
    )
    source = VertexAISource(ctx=PipelineContext(run_id="test"), config=config)

    assert source._projects == ["dev-project", "prod-project"]


@patch("datahub.ingestion.source.vertexai.vertexai.resolve_gcp_projects")
def test_multi_project_fallback_to_project_id_when_resolution_fails(
    mock_resolve: MagicMock,
) -> None:
    """Test fallback to project_id when multi-project resolution returns empty"""
    mock_resolve.return_value = []

    config = VertexAIConfig.model_validate(
        {
            "project_id": "fallback-project",
            "region": "us-central1",
            "project_ids": ["invalid-project"],
        }
    )
    source = VertexAISource(ctx=PipelineContext(run_id="test"), config=config)

    assert source._projects == ["fallback-project"]


def test_ml_metadata_helper_disabled_by_config() -> None:
    """Test that ML Metadata helper is not initialized when disabled"""
    config = VertexAIConfig.model_validate(
        {
            "project_id": "test-project",
            "region": "us-central1",
            "use_ml_metadata_for_lineage": False,
        }
    )
    source = VertexAISource(ctx=PipelineContext(run_id="test"), config=config)

    assert source._ml_metadata_helper is None


@patch(
    "datahub.ingestion.source.vertexai.vertexai_ml_metadata_helper.MetadataServiceClient"
)
def test_ml_metadata_helper_handles_init_errors(mock_client: MagicMock) -> None:
    """Test ML Metadata helper gracefully handles initialization errors"""
    mock_client.side_effect = Exception("Auth error")

    config = VertexAIConfig.model_validate(
        {
            "project_id": "test-project",
            "region": "us-central1",
            "use_ml_metadata_for_lineage": True,
        }
    )
    source = VertexAISource(ctx=PipelineContext(run_id="test"), config=config)

    assert source._ml_metadata_helper is None
