from typing import Any
from unittest.mock import patch

import pytest

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.matillion_dpc.config import MatillionSourceConfig
from datahub.ingestion.source.matillion_dpc.matillion import MatillionSource
from datahub.ingestion.source.matillion_dpc.matillion_api import MatillionAPIClient


def mock_oauth_generation(self: Any) -> None:
    pass


@pytest.fixture
def source_with_project_filter():
    config = MatillionSourceConfig.model_validate(
        {
            "api_config": {
                "client_id": "test",
                "client_secret": "test",
            },
            "project_patterns": {
                "allow": ["^prod-.*"],
                "deny": [".*-temp$"],
            },
        }
    )
    with patch.object(
        MatillionAPIClient, "_generate_oauth_token", mock_oauth_generation
    ):
        return MatillionSource(config, PipelineContext(run_id="test"))


@pytest.fixture
def source_with_environment_filter():
    config = MatillionSourceConfig.model_validate(
        {
            "api_config": {
                "client_id": "test",
                "client_secret": "test",
            },
            "environment_patterns": {
                "allow": ["^production$", "^staging$"],
            },
        }
    )
    with patch.object(
        MatillionAPIClient, "_generate_oauth_token", mock_oauth_generation
    ):
        return MatillionSource(config, PipelineContext(run_id="test"))


@pytest.fixture
def source_with_pipeline_filter():
    config = MatillionSourceConfig.model_validate(
        {
            "api_config": {
                "client_id": "test",
                "client_secret": "test",
            },
            "pipeline_patterns": {
                "deny": ["^test_.*", ".*_backup$"],
            },
        }
    )
    with patch.object(
        MatillionAPIClient, "_generate_oauth_token", mock_oauth_generation
    ):
        return MatillionSource(config, PipelineContext(run_id="test"))


@pytest.fixture
def source_with_streaming_filter():
    config = MatillionSourceConfig.model_validate(
        {
            "api_config": {
                "client_id": "test",
                "client_secret": "test",
            },
            "streaming_pipeline_patterns": {
                "allow": ["^cdc_.*"],
            },
        }
    )
    with patch.object(
        MatillionAPIClient, "_generate_oauth_token", mock_oauth_generation
    ):
        return MatillionSource(config, PipelineContext(run_id="test"))


@pytest.mark.parametrize(
    "project_name,should_be_allowed",
    [
        ("prod-analytics", True),
        ("prod-etl", True),
        ("dev-analytics", False),
        ("test-project", False),
        ("prod-temp", False),  # Matches deny pattern
        ("staging-temp", False),  # Matches deny pattern
    ],
)
def test_project_filtering(source_with_project_filter, project_name, should_be_allowed):
    assert (
        source_with_project_filter.config.project_patterns.allowed(project_name)
        == should_be_allowed
    )


@pytest.mark.parametrize(
    "environment_name,should_be_allowed",
    [
        ("production", True),
        ("staging", True),
        ("development", False),
        ("test", False),
    ],
)
def test_environment_filtering(
    source_with_environment_filter, environment_name, should_be_allowed
):
    assert (
        source_with_environment_filter.config.environment_patterns.allowed(
            environment_name
        )
        == should_be_allowed
    )


@pytest.mark.parametrize(
    "pipeline_name,should_be_allowed",
    [
        ("daily_etl", True),
        ("analytics_pipeline", True),
        ("test_pipeline", False),  # Matches deny pattern ^test_.*
        ("daily_etl_backup", False),  # Matches deny pattern .*_backup$
    ],
)
def test_pipeline_filtering(
    source_with_pipeline_filter, pipeline_name, should_be_allowed
):
    assert (
        source_with_pipeline_filter.config.pipeline_patterns.allowed(pipeline_name)
        == should_be_allowed
    )


@pytest.mark.parametrize(
    "pipeline_name,should_be_allowed",
    [
        ("cdc_users", True),
        ("cdc_orders", True),
        ("etl_users", False),
        ("analytics", False),
    ],
)
def test_streaming_pipeline_filtering(
    source_with_streaming_filter, pipeline_name, should_be_allowed
):
    assert (
        source_with_streaming_filter.config.streaming_pipeline_patterns.allowed(
            pipeline_name
        )
        == should_be_allowed
    )


def test_filtering_report_tracking(source_with_project_filter):
    # Verify that filtering report fields exist and are initialized as lists
    report = source_with_project_filter.report

    # These should be list attributes that exist on the report
    assert hasattr(report, "filtered_projects")
    assert hasattr(report, "filtered_environments")
    assert hasattr(report, "filtered_pipelines")
    assert hasattr(report, "filtered_streaming_pipelines")


def test_default_patterns_allow_all():
    config = MatillionSourceConfig.model_validate(
        {
            "api_config": {
                "client_id": "test",
                "client_secret": "test",
            },
        }
    )
    with patch.object(
        MatillionAPIClient, "_generate_oauth_token", mock_oauth_generation
    ):
        source = MatillionSource(config, PipelineContext(run_id="test"))

    # All patterns should allow everything by default
    assert source.config.project_patterns.allowed("any-project")
    assert source.config.environment_patterns.allowed("any-environment")
    assert source.config.pipeline_patterns.allowed("any-pipeline")
    assert source.config.streaming_pipeline_patterns.allowed("any-streaming")


@pytest.mark.parametrize(
    "pattern_type,name,should_be_allowed",
    [
        # Project filtering
        ("project", "prod-analytics", True),
        ("project", "staging-etl", True),
        ("project", "dev-analytics", False),
        ("project", "prod-deprecated", False),  # Matches deny pattern
        ("project", "staging-archived", False),  # Matches deny pattern
        # Pipeline filtering
        ("pipeline", "daily_etl", True),
        ("pipeline", "analytics_pipeline", True),
        ("pipeline", "_private_pipeline", False),  # Matches deny pattern ^_.*
        ("pipeline", "migration_temp", False),  # Matches deny pattern .*_temp$
        ("pipeline", "unit_test", False),  # Matches deny pattern .*_test$
    ],
)
def test_complex_pattern_combinations(pattern_type, name, should_be_allowed):
    config = MatillionSourceConfig.model_validate(
        {
            "api_config": {
                "client_id": "test",
                "client_secret": "test",
            },
            "project_patterns": {
                "allow": ["^prod-.*", "^staging-.*"],
                "deny": [".*-deprecated$", ".*-archived$"],
            },
            "pipeline_patterns": {
                "allow": [".*"],
                "deny": ["^_.*", ".*_temp$", ".*_test$"],
            },
        }
    )
    with patch.object(
        MatillionAPIClient, "_generate_oauth_token", mock_oauth_generation
    ):
        source = MatillionSource(config, PipelineContext(run_id="test"))

    if pattern_type == "project":
        assert source.config.project_patterns.allowed(name) == should_be_allowed
    elif pattern_type == "pipeline":
        assert source.config.pipeline_patterns.allowed(name) == should_be_allowed
