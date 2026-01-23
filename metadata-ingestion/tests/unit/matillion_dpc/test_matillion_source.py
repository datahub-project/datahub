from typing import Any, Dict, List
from unittest.mock import patch

import pytest
from pydantic import SecretStr
from requests.exceptions import ConnectionError
from requests_mock import Mocker

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import TestConnectionReport
from datahub.ingestion.source.matillion_dpc.config import (
    MatillionAPIConfig,
    MatillionSourceConfig,
    MatillionSourceReport,
)
from datahub.ingestion.source.matillion_dpc.constants import (
    MATILLION_NAMESPACE_PREFIX,
)
from datahub.ingestion.source.matillion_dpc.matillion import MatillionSource
from datahub.ingestion.source.matillion_dpc.models import (
    MatillionEnvironment,
    MatillionPipeline,
    MatillionPipelineExecution,
    MatillionProject,
)


@pytest.fixture(autouse=True)
def mock_oauth_token(requests_mock: Mocker) -> None:
    """Mock OAuth2 token endpoint for all tests"""
    requests_mock.post(
        "https://id.core.matillion.com/oauth/dpc/token",
        json={"access_token": "test_token", "token_type": "Bearer"},
    )


@pytest.fixture
def config() -> MatillionSourceConfig:
    return MatillionSourceConfig(
        api_config=MatillionAPIConfig(
            client_id=SecretStr("test_client_id"),
            client_secret=SecretStr("test_client_secret"),
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
            "client_id": "test_client_id",
            "client_secret": "test_client_secret",
            "custom_base_url": "http://test.com",
        }
    }

    source = MatillionSource.create(config_dict, pipeline_context)

    assert isinstance(source, MatillionSource)
    assert source.config.api_config.client_id.get_secret_value() == "test_client_id"
    assert (
        source.config.api_config.client_secret.get_secret_value()
        == "test_client_secret"
    )


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


@pytest.mark.parametrize(
    "job_name,expected",
    [
        ("device-health-analysis.tran.yaml", "device-health-analysis"),
        ("device-health-analysis.orch.yaml", "device-health-analysis"),
        ("folder/device-health-analysis.tran.yaml", "device-health-analysis"),
        ("folder/subfolder/pipeline.orch.yaml", "pipeline"),
        ("pipeline.yaml", "pipeline"),
        ("pipeline.yml", "pipeline"),
        ("Pipeline 1", "Pipeline 1"),
        ("simple-pipeline", "simple-pipeline"),
        ("folder/no-extension", "no-extension"),
    ],
)
def test_extract_base_pipeline_name(
    config: MatillionSourceConfig,
    pipeline_context: PipelineContext,
    job_name: str,
    expected: str,
) -> None:
    source = MatillionSource(config, pipeline_context)
    result = source._extract_base_pipeline_name(job_name)
    assert result == expected


@pytest.mark.parametrize(
    "pipeline_name,expected",
    [
        ("device-health-analysis.tran.yaml", "device-health-analysis"),
        ("device-health-analysis.orch.yaml", "device-health-analysis"),
        ("pipeline.yaml", "pipeline"),
        ("pipeline.yml", "pipeline"),
        ("Pipeline 1", "Pipeline 1"),
        ("simple-pipeline", "simple-pipeline"),
    ],
)
def test_normalize_pipeline_name(
    config: MatillionSourceConfig,
    pipeline_context: PipelineContext,
    pipeline_name: str,
    expected: str,
) -> None:
    source = MatillionSource(config, pipeline_context)
    result = source._normalize_pipeline_name(pipeline_name)
    assert result == expected


@pytest.mark.parametrize(
    "published_name,job_name,should_match",
    [
        ("device-health-analysis", "device-health-analysis", True),
        ("device-health-analysis", "device-health-analysis.tran.yaml", True),
        ("device-health-analysis", "folder/device-health-analysis.tran.yaml", True),
        ("device-health-analysis", "device-health-analysis.orch.yaml", True),
        (
            "device-health-analysis",
            "folder/subfolder/device-health-analysis.tran.yaml",
            True,
        ),
        ("Pipeline 1", "Pipeline 1", True),
        ("pipeline", "pipeline.yaml", True),
        ("pipeline", "folder/pipeline.orch.yaml", True),
        ("different-pipeline", "device-health-analysis", False),
        ("short", "very-long-different-name", False),
    ],
)
def test_match_pipeline_name(
    config: MatillionSourceConfig,
    pipeline_context: PipelineContext,
    published_name: str,
    job_name: str,
    should_match: bool,
) -> None:
    source = MatillionSource(config, pipeline_context)
    result = source._match_pipeline_name(published_name, job_name)
    assert result == should_match


def test_discover_unpublished_pipelines_basic(
    config: MatillionSourceConfig, pipeline_context: PipelineContext
) -> None:
    source = MatillionSource(config, pipeline_context)

    assert MATILLION_NAMESPACE_PREFIX == "matillion://", (
        f"Expected 'matillion://' but got '{MATILLION_NAMESPACE_PREFIX}'"
    )

    lineage_events = [
        {
            "event": {
                "job": {
                    "namespace": "matillion://account-123.project-456",
                    "name": "unpublished-pipeline",
                    "facets": {"jobType": {"jobType": "TRANSFORMATION"}},
                }
            }
        },
        {
            "event": {
                "job": {
                    "namespace": "matillion://account-123.project-456",
                    "name": "published-pipeline",
                    "facets": {},
                }
            }
        },
    ]

    published_pipeline_names = {"published-pipeline"}

    # Test that the matching function works correctly first
    assert not source._match_pipeline_name("published-pipeline", "unpublished-pipeline")
    assert source._match_pipeline_name("published-pipeline", "published-pipeline")

    result = source._discover_unpublished_pipelines(
        lineage_events, published_pipeline_names
    )

    assert len(result) == 1, f"Expected 1 result but got {len(result)}: {result}"
    assert result[0]["job_name"] == "unpublished-pipeline"
    assert result[0]["base_pipeline_name"] == "unpublished-pipeline"
    assert result[0]["project_id"] == "project-456"
    assert result[0]["account_id"] == "account-123"
    assert result[0]["pipeline_type"] == "TRANSFORMATION"


def test_discover_unpublished_pipelines_with_folder_path(
    config: MatillionSourceConfig, pipeline_context: PipelineContext
) -> None:
    source = MatillionSource(config, pipeline_context)

    lineage_events = [
        {
            "event": {
                "job": {
                    "namespace": "matillion://account-123.project-456",
                    "name": "analytics/reports/sales-report.orch.yaml",
                    "facets": {},
                }
            }
        }
    ]

    published_pipeline_names: set[str] = set()

    result = source._discover_unpublished_pipelines(
        lineage_events, published_pipeline_names
    )

    assert len(result) == 1
    assert result[0]["job_name"] == "analytics/reports/sales-report.orch.yaml"
    assert result[0]["folder_path"] == "analytics/reports"
    assert result[0]["base_pipeline_name"] == "sales-report.orch.yaml"


def test_discover_unpublished_pipelines_filters_non_matillion_namespace(
    config: MatillionSourceConfig, pipeline_context: PipelineContext
) -> None:
    source = MatillionSource(config, pipeline_context)

    lineage_events = [
        {
            "event": {
                "job": {
                    "namespace": "snowflake://account.region",
                    "name": "some-table",
                    "facets": {},
                }
            }
        },
        {
            "event": {
                "job": {
                    "namespace": "matillion://account-123.project-456",
                    "name": "real-pipeline",
                    "facets": {},
                }
            }
        },
    ]

    published_pipeline_names: set[str] = set()

    result = source._discover_unpublished_pipelines(
        lineage_events, published_pipeline_names
    )

    assert len(result) == 1
    assert result[0]["job_name"] == "real-pipeline"


def test_discover_unpublished_pipelines_skips_published(
    config: MatillionSourceConfig, pipeline_context: PipelineContext
) -> None:
    source = MatillionSource(config, pipeline_context)

    lineage_events = [
        {
            "event": {
                "job": {
                    "namespace": "matillion://account-123.project-456",
                    "name": "published-pipeline.tran.yaml",
                    "facets": {},
                }
            }
        }
    ]

    published_pipeline_names = {"published-pipeline"}

    result = source._discover_unpublished_pipelines(
        lineage_events, published_pipeline_names
    )

    assert len(result) == 0


def test_discover_unpublished_pipelines_infers_type_from_extension(
    config: MatillionSourceConfig, pipeline_context: PipelineContext
) -> None:
    source = MatillionSource(config, pipeline_context)

    lineage_events = [
        {
            "event": {
                "job": {
                    "namespace": "matillion://account-123.project-456",
                    "name": "transformation-pipeline.tran.yaml",
                    "facets": {},
                }
            }
        },
        {
            "event": {
                "job": {
                    "namespace": "matillion://account-123.project-456",
                    "name": "orchestration-pipeline.orch.yaml",
                    "facets": {},
                }
            }
        },
    ]

    published_pipeline_names: set[str] = set()

    result = source._discover_unpublished_pipelines(
        lineage_events, published_pipeline_names
    )

    assert len(result) == 2
    tran_pipeline = next(
        p for p in result if p["job_name"] == "transformation-pipeline.tran.yaml"
    )
    orch_pipeline = next(
        p for p in result if p["job_name"] == "orchestration-pipeline.orch.yaml"
    )

    assert tran_pipeline["pipeline_type"] == "TRANSFORMATION"
    assert orch_pipeline["pipeline_type"] == "ORCHESTRATION"


def test_generate_unpublished_pipeline_workunits(
    config: MatillionSourceConfig, pipeline_context: PipelineContext
) -> None:
    source = MatillionSource(config, pipeline_context)

    job_metadata = {
        "job_name": "unpublished-pipeline.tran.yaml",
        "base_pipeline_name": "unpublished-pipeline.tran.yaml",
        "folder_path": None,
        "project_id": "project-456",
        "account_id": "account-123",
        "pipeline_type": "TRANSFORMATION",
        "namespace": "matillion://account-123.project-456",
    }

    lineage_events: List[Dict[str, Any]] = []

    workunits = list(
        source._generate_unpublished_pipeline_workunits(job_metadata, lineage_events)
    )

    assert len(workunits) > 0
    assert source.report.unpublished_pipelines_emitted == 1


def test_unpublished_pipelines_feature_flag_disabled(
    config: MatillionSourceConfig, pipeline_context: PipelineContext
) -> None:
    config.include_unpublished_pipelines = False
    source = MatillionSource(config, pipeline_context)

    mock_projects = [MatillionProject(id="proj-1", name="Test Project")]

    with (
        patch.object(source.api_client, "get_projects", return_value=mock_projects),
        patch.object(source.api_client, "get_environments", return_value=[]),
        patch.object(source.api_client, "get_pipelines", return_value=[]),
        patch.object(source.api_client, "get_streaming_pipelines", return_value=[]),
        patch.object(source.api_client, "get_lineage_events", return_value=[]),
    ):
        list(source.get_workunits_internal())

    assert source.report.unpublished_pipelines_discovered == 0
    assert source.report.unpublished_pipelines_emitted == 0


def test_unpublished_pipelines_feature_flag_enabled(
    config: MatillionSourceConfig, pipeline_context: PipelineContext
) -> None:
    config.include_unpublished_pipelines = True
    source = MatillionSource(config, pipeline_context)

    mock_projects = [MatillionProject(id="proj-1", name="Test Project")]
    mock_lineage_events = [
        {
            "event": {
                "job": {
                    "namespace": "matillion://account-123.proj-1",
                    "name": "unpublished-pipeline",
                    "facets": {},
                }
            }
        }
    ]

    with (
        patch.object(source.api_client, "get_projects", return_value=mock_projects),
        patch.object(source.api_client, "get_environments", return_value=[]),
        patch.object(source.api_client, "get_pipelines", return_value=[]),
        patch.object(source.api_client, "get_streaming_pipelines", return_value=[]),
        patch.object(
            source.api_client, "get_lineage_events", return_value=mock_lineage_events
        ),
    ):
        list(source.get_workunits_internal())

    assert source.report.unpublished_pipelines_discovered == 1
    assert source.report.unpublished_pipelines_emitted == 1
