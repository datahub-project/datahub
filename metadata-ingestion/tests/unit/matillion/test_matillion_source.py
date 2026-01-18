from unittest.mock import patch

import pytest
from pydantic import SecretStr

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import TestConnectionReport
from datahub.ingestion.source.matillion.config import (
    MatillionAPIConfig,
    MatillionSourceConfig,
    MatillionSourceReport,
)
from datahub.ingestion.source.matillion.matillion import MatillionSource
from datahub.ingestion.source.matillion.models import (
    MatillionEnvironment,
    MatillionPipeline,
    MatillionPipelineExecution,
    MatillionProject,
)


@pytest.fixture
def config() -> MatillionSourceConfig:
    return MatillionSourceConfig(
        api_config=MatillionAPIConfig(
            api_token=SecretStr("test_token"),
            custom_base_url="http://test.com",
        ),
    )


@pytest.fixture
def pipeline_context() -> PipelineContext:
    return PipelineContext(run_id="test-run")


def test_source_initialization(
    config: MatillionSourceConfig, pipeline_context: PipelineContext
) -> None:
    source = MatillionSource(config, pipeline_context)

    assert source.config == config
    assert source.platform == "matillion"
    assert source.api_client is not None
    assert source.urn_builder is not None
    assert source.container_handler is not None
    assert source.lineage_handler is not None
    assert source.streaming_handler is not None


def test_test_connection_success(
    config: MatillionSourceConfig, pipeline_context: PipelineContext
) -> None:
    source = MatillionSource(config, pipeline_context)

    with patch.object(
        source.api_client,
        "get_projects",
        return_value=[],
    ):
        report = source.test_connection()

        assert isinstance(report, TestConnectionReport)
        assert report.basic_connectivity is not None
        assert report.basic_connectivity.capable is True
        assert report.basic_connectivity.failure_reason is None


def test_test_connection_failure(
    config: MatillionSourceConfig, pipeline_context: PipelineContext
) -> None:
    source = MatillionSource(config, pipeline_context)

    with patch.object(
        source.api_client,
        "get_projects",
        side_effect=Exception("Connection failed"),
    ):
        report = source.test_connection()

        assert isinstance(report, TestConnectionReport)
        assert report.basic_connectivity is not None
        assert report.basic_connectivity.capable is False
        assert report.basic_connectivity.failure_reason is not None
        assert "Connection failed" in report.basic_connectivity.failure_reason


def test_source_with_project_patterns_configured(
    config: MatillionSourceConfig, pipeline_context: PipelineContext
) -> None:
    config.project_patterns.deny = ["test-.*"]
    source = MatillionSource(config, pipeline_context)

    assert source.config.project_patterns.deny == ["test-.*"]
    assert not source.config.project_patterns.allowed("test-project")
    assert source.config.project_patterns.allowed("prod-project")


def test_get_workunits_internal_lineage_error(
    config: MatillionSourceConfig, pipeline_context: PipelineContext
) -> None:
    config.include_lineage = True

    source = MatillionSource(config, pipeline_context)

    mock_projects = [MatillionProject(id="proj-1", name="Test Project")]
    mock_environments = [
        MatillionEnvironment(id="env-1", name="Production", project_id="proj-1")
    ]
    mock_pipelines = [
        MatillionPipeline(
            id="pipe-1",
            name="Test Pipeline",
            project_id="proj-1",
            pipeline_type="orchestration",
        )
    ]

    with (
        patch.object(source.api_client, "get_projects", return_value=mock_projects),
        patch.object(
            source.api_client, "get_environments", return_value=mock_environments
        ),
        patch.object(source.api_client, "get_connections", return_value=[]),
        patch.object(source.api_client, "get_pipelines", return_value=mock_pipelines),
        patch.object(
            source.api_client,
            "get_pipeline_lineage",
            side_effect=Exception("Lineage API error"),
        ),
        patch.object(source.api_client, "get_streaming_pipelines", return_value=[]),
        patch.object(source.api_client, "get_pipeline_executions", return_value=[]),
        patch.object(source.api_client, "get_schedules", return_value=[]),
        patch.object(source.api_client, "get_repository_by_id", return_value=None),
        patch.object(source.api_client, "get_audit_events", return_value=[]),
        patch.object(source.api_client, "get_consumption", return_value=[]),
    ):
        workunits = list(source.get_workunits_internal())

    assert len(workunits) > 0


def test_create_method(pipeline_context: PipelineContext) -> None:
    config_dict = {
        "api_config": {
            "api_token": "test_token",
            "custom_base_url": "http://test.com",
        }
    }

    source = MatillionSource.create(config_dict, pipeline_context)

    assert isinstance(source, MatillionSource)
    assert source.config.api_config.api_token.get_secret_value() == "test_token"


def test_get_report(
    config: MatillionSourceConfig, pipeline_context: PipelineContext
) -> None:
    source = MatillionSource(config, pipeline_context)

    report = source.get_report()

    assert report is not None
    assert isinstance(report, MatillionSourceReport)
    assert report.projects_scanned == 0
    assert report.pipelines_scanned == 0


def test_capabilities(
    config: MatillionSourceConfig, pipeline_context: PipelineContext
) -> None:
    source = MatillionSource(config, pipeline_context)

    assert hasattr(source, "get_workunits_internal")
    assert hasattr(source, "test_connection")
    assert hasattr(source, "get_report")


def test_source_with_disabled_features(
    config: MatillionSourceConfig, pipeline_context: PipelineContext
) -> None:
    config.include_lineage = False
    config.include_streaming_pipelines = False
    config.include_pipeline_executions = False

    source = MatillionSource(config, pipeline_context)

    assert source.config.include_lineage is False
    assert source.config.include_streaming_pipelines is False
    assert source.config.include_pipeline_executions is False


def test_workunit_processors(
    config: MatillionSourceConfig, pipeline_context: PipelineContext
) -> None:
    source = MatillionSource(config, pipeline_context)
    processors = source.get_workunit_processors()

    assert processors is not None
    assert len(processors) > 0


def test_execution_workunits_with_no_timestamps(
    config: MatillionSourceConfig, pipeline_context: PipelineContext
) -> None:
    config.include_pipeline_executions = True

    source = MatillionSource(config, pipeline_context)

    mock_projects = [MatillionProject(id="proj-1", name="Test Project")]
    mock_environments = [
        MatillionEnvironment(id="env-1", name="Production", project_id="proj-1")
    ]
    mock_pipelines = [
        MatillionPipeline(
            id="pipe-1",
            name="Test Pipeline",
            project_id="proj-1",
            pipeline_type="orchestration",
        )
    ]
    mock_executions = [
        MatillionPipelineExecution(
            id="exec-1",
            pipeline_id="pipe-1",
            status="success",
            started_at=None,
            completed_at=None,
        )
    ]

    with (
        patch.object(source.api_client, "get_projects", return_value=mock_projects),
        patch.object(
            source.api_client, "get_environments", return_value=mock_environments
        ),
        patch.object(source.api_client, "get_connections", return_value=[]),
        patch.object(source.api_client, "get_pipelines", return_value=mock_pipelines),
        patch.object(source.api_client, "get_streaming_pipelines", return_value=[]),
        patch.object(
            source.api_client, "get_pipeline_executions", return_value=mock_executions
        ),
        patch.object(source.api_client, "get_schedules", return_value=[]),
        patch.object(source.api_client, "get_repository_by_id", return_value=None),
        patch.object(source.api_client, "get_audit_events", return_value=[]),
        patch.object(source.api_client, "get_consumption", return_value=[]),
    ):
        workunits = list(source.get_workunits_internal())

    assert len(workunits) > 0


if __name__ == "__main__":
    pytest.main([__file__, "-vv"])
