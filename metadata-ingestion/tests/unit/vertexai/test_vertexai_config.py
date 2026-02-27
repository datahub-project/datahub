from unittest.mock import MagicMock, patch

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.common.gcp_project_filter import GcpProject
from datahub.ingestion.source.vertexai.vertexai import VertexAIConfig, VertexAISource
from datahub.utilities.ratelimiter import RateLimiter


@patch("datahub.ingestion.source.vertexai.vertexai.resolve_gcp_projects")
def test_multi_project_initialization_with_explicit_ids(
    mock_resolve: MagicMock,
) -> None:
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


def test_rate_limit_disabled_by_default() -> None:
    config = VertexAIConfig.model_validate(
        {"project_id": "test-project", "region": "us-central1"}
    )
    source = VertexAISource(ctx=PipelineContext(run_id="test"), config=config)

    assert not isinstance(source._rate_limiter, RateLimiter)


def test_rate_limit_creates_shared_rate_limiter() -> None:
    config = VertexAIConfig.model_validate(
        {
            "project_id": "test-project",
            "region": "us-central1",
            "rate_limit": True,
            "requests_per_min": 120,
        }
    )
    source = VertexAISource(ctx=PipelineContext(run_id="test"), config=config)

    assert isinstance(source._rate_limiter, RateLimiter)
    assert source._rate_limiter.max_calls == 120
    # All extractors share the same instance so the budget is enforced globally.
    assert source.model_extractor.rate_limiter is source._rate_limiter
    assert source.training_extractor.rate_limiter is source._rate_limiter
    assert source.pipeline_extractor.rate_limiter is source._rate_limiter
    assert source.experiment_extractor.rate_limiter is source._rate_limiter
