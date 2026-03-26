from unittest.mock import MagicMock, patch

import pytest

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
            "region": "us-central1",
            "project_labels": ["env:prod", "team:ml"],
        }
    )
    source = VertexAISource(ctx=PipelineContext(run_id="test"), config=config)

    assert source._projects == ["dev-project", "prod-project"]


def test_deprecated_project_id_migrates_to_project_ids() -> None:
    """project_id (singular) should be auto-migrated to project_ids list."""
    config = VertexAIConfig.model_validate(
        {"project_id": "my-gcp-project", "region": "us-central1"}
    )

    assert not hasattr(config, "project_id")
    assert config.project_ids == ["my-gcp-project"]


def test_conflicting_project_id_and_project_ids_raises() -> None:
    """Setting project_id and a different project_ids list should fail at config time."""
    with pytest.raises(ValueError, match="Conflicting config"):
        VertexAIConfig.model_validate(
            {
                "project_id": "fallback-project",
                "region": "us-central1",
                "project_ids": ["different-project"],
            }
        )


def test_project_id_already_in_project_ids_is_deduplicated() -> None:
    """project_id that matches an entry in project_ids should be silently ignored."""
    config = VertexAIConfig.model_validate(
        {
            "project_id": "my-gcp-project",
            "region": "us-central1",
            "project_ids": ["my-gcp-project", "other-project"],
        }
    )
    assert not hasattr(config, "project_id")
    assert config.project_ids == ["my-gcp-project", "other-project"]


def test_project_ids_format_validation() -> None:
    """Invalid project ID formats should be rejected."""
    with pytest.raises(ValueError, match="Invalid project_ids format"):
        VertexAIConfig.model_validate(
            {"project_ids": ["UPPERCASE-ID"], "region": "us-central1"}
        )


def test_project_labels_format_validation() -> None:
    """Labels must use key:value format with lowercase chars."""
    with pytest.raises(ValueError, match="Invalid project_labels format"):
        VertexAIConfig.model_validate(
            {"project_labels": ["env=prod"], "region": "us-central1"}
        )

    with pytest.raises(ValueError, match="Invalid project_labels format"):
        VertexAIConfig.model_validate(
            {"project_labels": ["Env:prod"], "region": "us-central1"}
        )


def test_project_labels_empty_string_rejected() -> None:
    with pytest.raises(ValueError, match="Invalid project_labels format"):
        VertexAIConfig.model_validate(
            {"project_labels": ["env:prod", ""], "region": "us-central1"}
        )


def test_project_id_pattern_invalid_regex_rejected() -> None:
    with pytest.raises(ValueError, match="Invalid regex in project_id_pattern"):
        VertexAIConfig.model_validate(
            {
                "project_ids": ["test-project-1"],
                "project_id_pattern": {"allow": ["[invalid"]},
                "region": "us-central1",
            }
        )


def test_project_id_pattern_filters_all_explicit_ids_raises() -> None:
    with pytest.raises(ValueError, match="filtered out"):
        VertexAIConfig.model_validate(
            {
                "project_ids": ["dev-project-1", "dev-project-2"],
                "project_id_pattern": {"allow": ["^prod-.*"]},
                "region": "us-central1",
            }
        )


def test_project_id_pattern_partial_filter_is_valid() -> None:
    """Filtering some (but not all) project_ids should be allowed."""
    config = VertexAIConfig.model_validate(
        {
            "project_ids": ["prod-project-1", "dev-project-1"],
            "project_id_pattern": {"deny": ["^dev-.*"]},
            "region": "us-central1",
        }
    )
    assert "prod-project-1" in config.project_ids
    assert "dev-project-1" in config.project_ids  # filter applied at runtime, not here


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


def test_region_validator_accepts_legacy_region() -> None:
    config = VertexAIConfig.model_validate({"project_id": "p", "region": "us-central1"})
    assert config.region == "us-central1"


def test_region_validator_accepts_regions_list() -> None:
    config = VertexAIConfig.model_validate(
        {"project_id": "p", "regions": ["us-central1", "europe-west1"]}
    )
    assert config.regions == ["us-central1", "europe-west1"]


def test_region_validator_accepts_discover_regions() -> None:
    config = VertexAIConfig.model_validate(
        {"project_id": "p", "discover_regions": True}
    )
    assert config.discover_regions is True


def test_region_validator_rejects_no_region_source() -> None:
    with pytest.raises(Exception, match="region"):
        VertexAIConfig.model_validate({"project_id": "p"})


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
