from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest
from pydantic import SecretStr
from requests.exceptions import ConnectionError, Timeout
from requests_mock import Mocker

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import TestConnectionReport
from datahub.ingestion.source.matillion_dpc.config import (
    MatillionAPIConfig,
    MatillionSourceConfig,
    MatillionSourceReport,
)
from datahub.ingestion.source.matillion_dpc.matillion import MatillionSource
from datahub.ingestion.source.matillion_dpc.matillion_utils import (
    extract_base_pipeline_name,
    make_dataset_urn_from_matillion_dataset,
    match_pipeline_name,
    normalize_pipeline_name,
)
from datahub.ingestion.source.matillion_dpc.models import (
    MatillionEnvironment,
    MatillionPipeline,
    MatillionPipelineExecution,
    MatillionProject,
)
from datahub.metadata.schema_classes import (
    DataJobInputOutputClass,
    UpstreamLineageClass,
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
    "include_streaming,include_unpublished",
    [
        pytest.param(False, False, id="all_disabled"),
        pytest.param(True, True, id="all_enabled"),
        pytest.param(False, True, id="unpublished_only"),
        pytest.param(True, False, id="published_streaming_only"),
    ],
)
def test_source_with_feature_flags(
    config: MatillionSourceConfig,
    pipeline_context: PipelineContext,
    include_streaming: bool,
    include_unpublished: bool,
) -> None:
    config.include_streaming_pipelines = include_streaming
    config.include_unpublished_pipelines = include_unpublished

    source = MatillionSource(config, pipeline_context)

    assert source.config.include_streaming_pipelines is include_streaming
    assert source.config.include_unpublished_pipelines is include_unpublished


def test_workunit_processors(
    config: MatillionSourceConfig, pipeline_context: PipelineContext
) -> None:
    source = MatillionSource(config, pipeline_context)
    processors = source.get_workunit_processors()

    assert processors is not None
    assert len(processors) > 0


def test_published_pipelines_emitted_when_unpublished_disabled(
    config: MatillionSourceConfig, pipeline_context: PipelineContext
) -> None:
    """Published pipelines must be emitted when include_unpublished_pipelines=False."""
    config.include_unpublished_pipelines = False
    source = MatillionSource(config, pipeline_context)

    mock_projects = [MatillionProject(id="proj-1", name="Test Project")]
    mock_environments = [
        MatillionEnvironment(name="Production", default_agent_id="agent-1")
    ]
    mock_pipelines = [
        MatillionPipeline(name="Pipeline A"),
        MatillionPipeline(name="Pipeline B"),
    ]

    with (
        patch.object(source.api_client, "get_projects", return_value=mock_projects),
        patch.object(
            source.api_client, "get_environments", return_value=mock_environments
        ),
        patch.object(source.api_client, "get_pipelines", return_value=mock_pipelines),
        patch.object(source.api_client, "get_streaming_pipelines", return_value=[]),
        patch.object(source.api_client, "get_schedules", return_value=[]),
        patch.object(
            source.api_client, "get_pipeline_execution_steps", return_value=[]
        ),
    ):
        workunits = list(
            source._discover_and_process_pipelines_from_executions(mock_projects)
        )

    assert source.report.pipelines_emitted == 2
    assert source.report.pipelines_scanned == 2
    assert len(workunits) > 0


def test_execution_discovery_bounded_by_time_window(
    config: MatillionSourceConfig, pipeline_context: PipelineContext
) -> None:
    """Unpublished discovery must bound the executions fetch by the configured
    start_time/end_time window rather than scrolling the whole account."""
    config.include_unpublished_pipelines = True
    source = MatillionSource(config, pipeline_context)

    mock_projects = [MatillionProject(id="proj-1", name="Test Project")]
    exec_mock = MagicMock(return_value=[])

    with (
        patch.object(source.api_client, "get_projects", return_value=mock_projects),
        patch.object(source.api_client, "get_environments", return_value=[]),
        patch.object(source.api_client, "get_pipelines", return_value=[]),
        patch.object(source.api_client, "get_streaming_pipelines", return_value=[]),
        patch.object(source.api_client, "get_pipeline_executions", exec_mock),
    ):
        list(source._discover_and_process_pipelines_from_executions(mock_projects))

    exec_mock.assert_called_once()
    _, kwargs = exec_mock.call_args
    assert kwargs["started_after"].endswith("Z")
    assert kwargs["started_before"].endswith("Z")


def test_execution_discovery_survives_timeout(
    config: MatillionSourceConfig, pipeline_context: PipelineContext
) -> None:
    """A timeout while discovering pipelines from executions must degrade
    gracefully (report a warning, no executions) rather than aborting ingestion,
    so published pipelines are still emitted."""
    config.include_unpublished_pipelines = True
    source = MatillionSource(config, pipeline_context)

    mock_projects = [MatillionProject(id="proj-1", name="Test Project")]

    with (
        patch.object(source.api_client, "get_projects", return_value=mock_projects),
        patch.object(source.api_client, "get_environments", return_value=[]),
        patch.object(source.api_client, "get_pipelines", return_value=[]),
        patch.object(source.api_client, "get_streaming_pipelines", return_value=[]),
        patch.object(
            source.api_client,
            "get_pipeline_executions",
            side_effect=Timeout("read timed out"),
        ),
    ):
        # Must not raise despite the timeout.
        list(source._discover_and_process_pipelines_from_executions(mock_projects))

    assert source.report.executions_scanned == 0
    assert any(
        "pipeline-executions-timeout" in str(warning)
        for warning in source.report.warnings
    )


def test_lineage_events_fetched_in_max_31_day_windows(
    config: MatillionSourceConfig, pipeline_context: PipelineContext
) -> None:
    """A wide lineage window must be split into <=31-day sub-windows; the lineage
    API rejects wider ranges with HTTP 400."""
    config.end_time = datetime(2026, 6, 1, tzinfo=timezone.utc)
    config.start_time = config.end_time - timedelta(days=90)
    source = MatillionSource(config, pipeline_context)

    lineage_mock = MagicMock(return_value=[])
    with patch.object(source.api_client, "get_lineage_events", lineage_mock):
        events = source._fetch_lineage_events()

    assert events == []
    # 90 days split into 31-day chunks -> 3 requests
    assert lineage_mock.call_count == 3
    for call in lineage_mock.call_args_list:
        args, _ = call
        d_from = datetime.fromisoformat(args[0].replace("Z", "+00:00"))
        d_before = datetime.fromisoformat(args[1].replace("Z", "+00:00"))
        assert d_before - d_from <= timedelta(days=31)


def test_lineage_emitted_from_events_for_unexecuted_child_pipeline(
    config: MatillionSourceConfig, pipeline_context: PipelineContext
) -> None:
    """Lineage must be emitted from OpenLineage job events even when the job is a
    child orchestration that never appears in execution discovery, keyed off the
    job's own namespace/name rather than the executed pipeline names."""
    config.extract_projects_to_containers = False
    source = MatillionSource(config, pipeline_context)
    source._projects_cache = {"proj-1": MatillionProject(id="proj-1", name="Proj")}
    source._lineage_events_cache = [
        {
            "event": {
                "job": {
                    "namespace": "matillion://proj-1.env-1",
                    "name": "SOURCE_MAPPINGS/SFDC/INCIDENT_SRC_ORCH NRT.orch.yaml",
                },
                "inputs": [
                    {
                        "namespace": "salesforce://00D70000000KG1jEAG",
                        "name": "Incident__c",
                        "facets": {},
                    }
                ],
                "outputs": [
                    {
                        "namespace": "snowflake://ACME-WAREHOUSE",
                        "name": "RAW.SALESFORCE.INCIDENT_LND",
                        "facets": {
                            "columnLineage": {
                                "fields": {
                                    "ID": {
                                        "inputFields": [
                                            {
                                                "namespace": "salesforce://00D70000000KG1jEAG",
                                                "name": "Incident__c",
                                                "field": "Id",
                                            }
                                        ]
                                    }
                                }
                            }
                        },
                    }
                ],
            }
        }
    ]

    projects = [MatillionProject(id="proj-1", name="Proj")]
    workunits = list(source._generate_lineage_from_events(projects))

    upstream_aspects = [
        wu.metadata.aspect
        for wu in workunits
        if isinstance(wu.metadata, MetadataChangeProposalWrapper)
        and isinstance(wu.metadata.aspect, UpstreamLineageClass)
    ]
    assert len(upstream_aspects) == 1
    upstream_urns = [u.dataset for u in upstream_aspects[0].upstreams]
    assert any("salesforce" in urn for urn in upstream_urns)
    assert upstream_aspects[0].fineGrainedLineages  # column lineage carried through

    input_output_aspects = [
        wu.metadata.aspect
        for wu in workunits
        if isinstance(wu.metadata, MetadataChangeProposalWrapper)
        and isinstance(wu.metadata.aspect, DataJobInputOutputClass)
    ]
    assert len(input_output_aspects) == 1
    assert input_output_aspects[0].inputDatasets
    assert input_output_aspects[0].outputDatasets
    assert source.report.lineage_emitted == 1


def _sql_lineage_event(with_column_lineage: bool) -> dict:
    output_facets: dict = {}
    if with_column_lineage:
        output_facets["columnLineage"] = {
            "fields": {
                "ID": {
                    "inputFields": [
                        {
                            "namespace": "snowflake://ACME",
                            "name": "DB.SCH.SRC",
                            "field": "ID",
                        }
                    ]
                }
            }
        }
    return {
        "event": {
            "job": {
                "namespace": "matillion://proj-1.env-1",
                "name": "TRANSFORM.tran.yaml",
                "facets": {
                    "sql": {"query": "INSERT INTO DB.SCH.TGT SELECT * FROM DB.SCH.SRC"}
                },
            },
            "inputs": [
                {"namespace": "snowflake://ACME", "name": "DB.SCH.SRC", "facets": {}}
            ],
            "outputs": [
                {
                    "namespace": "snowflake://ACME",
                    "name": "DB.SCH.TGT",
                    "facets": output_facets,
                }
            ],
        }
    }


def _registered_output_urn(source: MatillionSource, event: dict) -> str:
    output_dict = event["event"]["outputs"][0]
    output_info = source.openlineage_parser._extract_dataset_info(output_dict, "output")
    assert output_info is not None
    return make_dataset_urn_from_matillion_dataset(output_info)


def test_sql_gapfill_registers_query_when_no_column_lineage(
    config: MatillionSourceConfig, pipeline_context: PipelineContext
) -> None:
    """When OpenLineage carries a SQL query but no column lineage for a known
    output, the SQL is handed to the aggregator to parse additional lineage."""
    config.extract_projects_to_containers = False
    source = MatillionSource(config, pipeline_context)
    source._projects_cache = {"proj-1": MatillionProject(id="proj-1", name="Proj")}

    event = _sql_lineage_event(with_column_lineage=False)
    source._lineage_events_cache = [event]
    # Mark the output as already present in DataHub so gap-fill is allowed.
    source._registered_urns.add(_registered_output_urn(source, event))

    with patch.object(source, "_register_sql_query") as mock_register:
        list(
            source._generate_lineage_from_events(
                [MatillionProject(id="proj-1", name="Proj")]
            )
        )

    mock_register.assert_called_once()
    assert (
        mock_register.call_args.args[1]
        == "INSERT INTO DB.SCH.TGT SELECT * FROM DB.SCH.SRC"
    )


def test_sql_gapfill_skipped_when_column_lineage_present(
    config: MatillionSourceConfig, pipeline_context: PipelineContext
) -> None:
    """OpenLineage column lineage takes precedence: SQL parsing is not invoked for
    outputs that already have column-level lineage, avoiding clobbering."""
    config.extract_projects_to_containers = False
    source = MatillionSource(config, pipeline_context)
    source._projects_cache = {"proj-1": MatillionProject(id="proj-1", name="Proj")}

    event = _sql_lineage_event(with_column_lineage=True)
    source._lineage_events_cache = [event]
    source._registered_urns.add(_registered_output_urn(source, event))

    with patch.object(source, "_register_sql_query") as mock_register:
        list(
            source._generate_lineage_from_events(
                [MatillionProject(id="proj-1", name="Proj")]
            )
        )

    mock_register.assert_not_called()


def test_execution_workunits_with_no_timestamps(
    config: MatillionSourceConfig, pipeline_context: PipelineContext
) -> None:
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
    result = extract_base_pipeline_name(job_name)
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
    result = normalize_pipeline_name(pipeline_name)
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
    result = match_pipeline_name(published_name, job_name)
    assert result == should_match
