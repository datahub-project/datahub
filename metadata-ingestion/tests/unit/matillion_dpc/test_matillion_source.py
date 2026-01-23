from unittest.mock import patch

import pytest
from pydantic import SecretStr
from requests.exceptions import ConnectionError

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import TestConnectionReport
from datahub.ingestion.source.matillion_dpc.config import (
    MatillionAPIConfig,
    MatillionSourceConfig,
    MatillionSourceReport,
)
from datahub.ingestion.source.matillion_dpc.matillion import MatillionSource
from datahub.ingestion.source.matillion_dpc.models import (
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
    assert source.openlineage_parser is not None
    assert source.streaming_handler is not None


@pytest.mark.parametrize(
    "should_succeed,expected_capable",
    [
        pytest.param(True, True, id="success"),
        pytest.param(False, False, id="failure"),
    ],
)
def test_test_connection(
    config: MatillionSourceConfig,
    pipeline_context: PipelineContext,
    should_succeed: bool,
    expected_capable: bool,
) -> None:
    source = MatillionSource(config, pipeline_context)

    if should_succeed:
        with patch.object(
            source.api_client,
            "get_projects",
            return_value=[],
        ):
            report = source.test_connection()
    else:
        with patch.object(
            source.api_client,
            "get_projects",
            side_effect=ConnectionError("Connection failed"),
        ):
            report = source.test_connection()

    assert isinstance(report, TestConnectionReport)
    assert report.basic_connectivity is not None
    assert report.basic_connectivity.capable is expected_capable

    if not should_succeed:
        assert report.basic_connectivity.failure_reason is not None
        assert "Network connection failed" in report.basic_connectivity.failure_reason
        assert "Connection failed" in report.basic_connectivity.failure_reason
    else:
        assert report.basic_connectivity.failure_reason is None


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
        MatillionEnvironment(name="Production", default_agent_id="agent-1")
    ]
    mock_pipelines = [
        MatillionPipeline(
            name="Test Pipeline",
        )
    ]

    with (
        patch.object(source.api_client, "get_projects", return_value=mock_projects),
        patch.object(
            source.api_client, "get_environments", return_value=mock_environments
        ),
        patch.object(source.api_client, "get_pipelines", return_value=mock_pipelines),
        patch.object(source.api_client, "get_streaming_pipelines", return_value=[]),
        patch.object(source.api_client, "get_pipeline_executions", return_value=[]),
        patch.object(source.api_client, "get_schedules", return_value=[]),
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


@pytest.mark.parametrize(
    "include_lineage,include_streaming,include_executions",
    [
        pytest.param(False, False, False, id="all_disabled"),
        pytest.param(True, True, True, id="all_enabled"),
        pytest.param(True, False, True, id="lineage_and_executions"),
    ],
)
def test_source_with_feature_flags(
    config: MatillionSourceConfig,
    pipeline_context: PipelineContext,
    include_lineage: bool,
    include_streaming: bool,
    include_executions: bool,
) -> None:
    config.include_lineage = include_lineage
    config.include_streaming_pipelines = include_streaming
    config.include_pipeline_executions = include_executions

    source = MatillionSource(config, pipeline_context)

    assert source.config.include_lineage is include_lineage
    assert source.config.include_streaming_pipelines is include_streaming
    assert source.config.include_pipeline_executions is include_executions


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
        MatillionEnvironment(name="Production", default_agent_id="agent-1")
    ]
    mock_pipelines = [
        MatillionPipeline(
            name="Test Pipeline",
        )
    ]
    mock_executions = [
        MatillionPipelineExecution(
            pipeline_execution_id="exec-1",
            pipeline_name="Test Pipeline",
            status="SUCCESS",
            started_at=None,
            finished_at=None,
        )
    ]

    with (
        patch.object(source.api_client, "get_projects", return_value=mock_projects),
        patch.object(
            source.api_client, "get_environments", return_value=mock_environments
        ),
        patch.object(source.api_client, "get_pipelines", return_value=mock_pipelines),
        patch.object(source.api_client, "get_streaming_pipelines", return_value=[]),
        patch.object(
            source.api_client, "get_pipeline_executions", return_value=mock_executions
        ),
        patch.object(source.api_client, "get_schedules", return_value=[]),
    ):
        workunits = list(source.get_workunits_internal())

    assert len(workunits) > 0
