from typing import Dict, List, Optional, Tuple
from unittest.mock import MagicMock, patch

import pytest
import requests
from pydantic import ValidationError

from datahub.emitter.mce_builder import (
    make_data_flow_urn,
    make_data_job_urn_with_flow,
    make_dataset_urn_with_platform_instance,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.source.kafka.kafka import KafkaSource
from datahub.ingestion.source.kafka.kafka_config import (
    FlinkLineageConfig,
    KafkaSourceConfig,
)
from datahub.ingestion.source.kafka.kafka_report import KafkaSourceReport
from datahub.ingestion.source.kafka.stream_processing import flink as flink_module
from datahub.ingestion.source.kafka.stream_processing.builder import (
    build_stream_processing_workunits,
)
from datahub.ingestion.source.kafka.stream_processing.constants import (
    ENGINE_FLOW_METADATA,
    FLINK_SQL_DIALECT,
    StreamProcessingEngine,
)
from datahub.ingestion.source.kafka.stream_processing.flink import (
    FlinkLineageExtractor,
    FlinkStatementsClient,
)
from datahub.ingestion.source.kafka.stream_processing.flink_sql import (
    FlinkDmlStatement,
)
from datahub.ingestion.source.kafka.stream_processing.models import StreamProcessingJob
from datahub.ingestion.source.kafka.stream_processing.sql import (
    column_lineage_fine_grained,
)
from datahub.metadata.schema_classes import (
    DataFlowInfoClass,
    DataJobInfoClass,
    DataJobInputOutputClass,
)
from datahub.sql_parsing.schema_resolver import SchemaResolver

PLATFORM = "kafka"
ENV = "PROD"


def _aspect_for_urn(
    workunits: List[MetadataWorkUnit], urn: str, aspect_cls: type
) -> object:
    for wu in workunits:
        mcp = wu.metadata
        assert isinstance(mcp, MetadataChangeProposalWrapper)
        if mcp.entityUrn == urn and isinstance(mcp.aspect, aspect_cls):
            return mcp.aspect
    raise AssertionError(f"missing {aspect_cls.__name__} for {urn}")


class _FakeFlinkClient:
    def __init__(self, statements: List[Dict[str, object]]) -> None:
        self._statements = statements

    def list_statements(self) -> List[Dict[str, object]]:
        return self._statements

    def close(self) -> None:
        pass


ENV_ID = "env-1"
CLUSTER_ID = "lkc-1"


def _statement(
    sql: str,
    name: str = "stmt",
    phase: str = "RUNNING",
    properties: Optional[Dict[str, str]] = None,
    compute_pool_id: Optional[str] = None,
) -> Dict[str, object]:
    spec: Dict[str, object] = {
        "statement": sql,
        "properties": {
            "sql.current-catalog": ENV_ID,
            "sql.current-database": CLUSTER_ID,
        }
        if properties is None
        else properties,
    }
    if compute_pool_id:
        spec["compute_pool_id"] = compute_pool_id
    return {"name": name, "spec": spec, "status": {"phase": phase}}


def _flink_config(**overrides: object) -> FlinkLineageConfig:
    values: Dict[str, object] = {
        "enabled": True,
        "organization_id": "org-1",
        "environment_id": ENV_ID,
        "environment_name": "prod",
        "kafka_cluster_id": CLUSTER_ID,
        "kafka_cluster_name": "main",
        "region": "us-east-1",
        "cloud": "aws",
        "api_key": "k",
        "api_secret": "s",
        **overrides,
    }
    return FlinkLineageConfig.model_validate(values)


def _extract(
    statements: List[Dict[str, object]], **overrides: object
) -> Tuple[List[StreamProcessingJob], KafkaSourceReport]:
    report = KafkaSourceReport()
    jobs = FlinkLineageExtractor(
        _FakeFlinkClient(statements),  # type: ignore[arg-type]
        report,
        _flink_config(**overrides),
    ).extract()
    return jobs, report


def test_flink_maps_tables_in_this_cluster_to_topics() -> None:
    jobs, _ = _extract(
        [
            _statement(
                "INSERT INTO `prod`.`main`.`sink_topic` "
                "SELECT id FROM `env-1`.`lkc-1`.`src_topic`"
            ),
            _statement("CREATE TABLE foo (id INT)", name="ddl"),
            _statement("SELECT * FROM src_topic", name="query"),
        ]
    )

    assert len(jobs) == 1
    job = jobs[0]
    assert job.engine == StreamProcessingEngine.FLINK
    assert job.input_topics == ["src_topic"]
    assert job.output_topics == ["sink_topic"]
    assert job.custom_properties.get("state") == "RUNNING"
    assert job.parse_query is not None
    assert "`prod`" not in job.parse_query and "`env-1`" not in job.parse_query


def test_flink_does_not_take_topics_from_functions_windows_or_ctes() -> None:
    jobs, _ = _extract(
        [
            _statement(
                "INSERT INTO hourly SELECT EXTRACT(HOUR FROM ts) AS h, id FROM orders",
                name="extract",
            ),
            _statement(
                "INSERT INTO agg SELECT window_start, COUNT(*) FROM TABLE(TUMBLE("
                "TABLE orders, DESCRIPTOR(ts), INTERVAL '1' MINUTES)) "
                "GROUP BY window_start, window_end",
                name="tumble",
            ),
            _statement(
                "INSERT INTO out_t WITH big AS (SELECT * FROM orders WHERE amount > 10) "
                "SELECT * FROM big",
                name="cte",
            ),
        ]
    )

    assert {job.name: job.input_topics for job in jobs} == {
        "extract": ["orders"],
        "tumble": ["orders"],
        "cte": ["orders"],
    }


def test_flink_drops_tables_in_other_clusters_and_reports_them() -> None:
    jobs, report = _extract(
        [
            _statement(
                "INSERT INTO sink SELECT * FROM `env-1`.`lkc-other`.`orders` o "
                "JOIN customers c ON o.cid = c.id",
                name="mixed",
            ),
            _statement(
                "INSERT INTO `env-1`.`lkc-other`.`sink` SELECT * FROM orders",
                name="writes-elsewhere",
            ),
        ]
    )

    assert len(jobs) == 1
    assert jobs[0].input_topics == ["customers"]
    assert jobs[0].output_topics == ["sink"]
    # Column lineage would resolve the other cluster's table onto this cluster.
    assert jobs[0].parse_query is None
    assert report.warnings


def test_flink_skips_unqualified_tables_without_current_catalog() -> None:
    jobs, report = _extract(
        [_statement("INSERT INTO sink SELECT * FROM src", properties={})]
    )

    assert jobs == []
    assert list(report.stream_processing_statements_skipped) == ["stmt"]


def test_flink_skips_statements_that_are_not_running_by_default() -> None:
    statements = [
        _statement("INSERT INTO sink SELECT * FROM src", phase="STOPPED"),
    ]

    jobs, report = _extract(statements)
    assert jobs == []
    assert list(report.stream_processing_statements_not_running) == ["stmt"]

    jobs, _ = _extract(statements, include_non_running_statements=True)
    assert [job.custom_properties["state"] for job in jobs] == ["STOPPED"]


def test_flink_reads_every_insert_in_a_statement_set() -> None:
    jobs, _ = _extract(
        [
            _statement(
                "EXECUTE STATEMENT SET BEGIN INSERT INTO a SELECT * FROM x; "
                "INSERT INTO b SELECT * FROM y; END;"
            )
        ]
    )

    assert jobs[0].input_topics == ["x", "y"]
    assert jobs[0].output_topics == ["a", "b"]
    assert jobs[0].parse_query is None


def test_flink_skips_and_reports_unparseable_sql() -> None:
    jobs, report = _extract([_statement("INSERT INTO sink SELECT FROM WHERE")])

    assert jobs == []
    assert list(report.stream_processing_statements_skipped) == ["stmt"]


def test_flink_parse_query_keeps_string_literals_for_column_lineage() -> None:
    jobs, _ = _extract(
        [
            _statement(
                "INSERT INTO `orders-paid` SELECT id, name FROM `orders-raw` "
                "WHERE status = 'PAID'"
            )
        ]
    )
    parse_query = jobs[0].parse_query
    assert parse_query is not None
    assert "'PAID'" in parse_query

    fine_grained = column_lineage_fine_grained(
        query=parse_query,
        platform=PLATFORM,
        platform_instance=None,
        env=ENV,
        graph=None,
        dialect=FLINK_SQL_DIALECT,
        report=KafkaSourceReport(),
        job_name="stmt",
    )
    downstreams = {urn for edge in fine_grained for urn in edge.downstreams or []}
    assert {urn.rsplit(",", 1)[1] for urn in downstreams} == {"id)", "name)"}


def test_build_workunits_emits_flow_job_io_and_filters_denied_topics() -> None:
    report = KafkaSourceReport()
    jobs = [
        StreamProcessingJob(
            engine=StreamProcessingEngine.FLINK,
            job_id="enrich-statement",
            name="enrich-statement",
            input_topics=["src_topic", "denied_topic"],
            output_topics=["sink_topic"],
            query="INSERT INTO sink_topic SELECT id FROM src_topic",
        )
    ]

    workunits = build_stream_processing_workunits(
        jobs=jobs,
        platform=PLATFORM,
        platform_instance=None,
        env=ENV,
        report=report,
        topic_allowed=lambda topic: topic != "denied_topic",
        graph=None,
        include_column_lineage=False,
    )

    flow_urn = make_data_flow_urn(
        PLATFORM, ENGINE_FLOW_METADATA[StreamProcessingEngine.FLINK][0], ENV
    )
    job_urn = make_data_job_urn_with_flow(flow_urn, "enrich-statement")
    src_urn = make_dataset_urn_with_platform_instance(
        platform=PLATFORM, name="src_topic", platform_instance=None, env=ENV
    )
    sink_urn = make_dataset_urn_with_platform_instance(
        platform=PLATFORM, name="sink_topic", platform_instance=None, env=ENV
    )

    aspects_by_urn: Dict[str, List[object]] = {}
    for wu in workunits:
        mcp = wu.metadata
        assert isinstance(mcp, MetadataChangeProposalWrapper)
        assert mcp.entityUrn is not None
        aspects_by_urn.setdefault(mcp.entityUrn, []).append(mcp.aspect)

    assert isinstance(aspects_by_urn[flow_urn][0], DataFlowInfoClass)
    job_info = next(
        a for a in aspects_by_urn[job_urn] if isinstance(a, DataJobInfoClass)
    )
    assert job_info.customProperties["engine"] == "flink"
    io = next(
        a for a in aspects_by_urn[job_urn] if isinstance(a, DataJobInputOutputClass)
    )
    # Denied topic filtered out of the inputs.
    assert io.inputDatasets == [src_urn]
    assert io.outputDatasets == [sink_urn]
    assert report.stream_processing_jobs_with_lineage == 1
    assert report.stream_processing_lineage_edges == 2


def test_column_lineage_parses_explicit_projection_without_graph() -> None:
    fine_grained = column_lineage_fine_grained(
        query="INSERT INTO sink_topic SELECT id FROM src_topic",
        platform=PLATFORM,
        platform_instance=None,
        env=ENV,
        graph=None,
        dialect=FLINK_SQL_DIALECT,
        report=KafkaSourceReport(),
        job_name="stmt",
    )

    downstream = make_dataset_urn_with_platform_instance(
        platform=PLATFORM, name="sink_topic", platform_instance=None, env=ENV
    )
    upstream = make_dataset_urn_with_platform_instance(
        platform=PLATFORM, name="src_topic", platform_instance=None, env=ENV
    )

    assert len(fine_grained) == 1
    edge = fine_grained[0]
    assert edge.downstreams == [f"urn:li:schemaField:({downstream},id)"]
    assert edge.upstreams == [f"urn:li:schemaField:({upstream},id)"]


def test_build_workunits_emits_column_lineage_when_all_topics_allowed() -> None:
    report = KafkaSourceReport()
    jobs = [
        StreamProcessingJob(
            engine=StreamProcessingEngine.FLINK,
            job_id="enrich-cll",
            name="enrich-cll",
            input_topics=["src_topic"],
            output_topics=["sink_topic"],
            parse_query="INSERT INTO sink_topic SELECT id FROM src_topic",
            sql_dialect=FLINK_SQL_DIALECT,
        )
    ]

    workunits = build_stream_processing_workunits(
        jobs=jobs,
        platform=PLATFORM,
        platform_instance=None,
        env=ENV,
        report=report,
        topic_allowed=lambda _topic: True,
        graph=None,
        include_column_lineage=True,
    )

    flow_urn = make_data_flow_urn(
        PLATFORM, ENGINE_FLOW_METADATA[StreamProcessingEngine.FLINK][0], ENV
    )
    job_urn = make_data_job_urn_with_flow(flow_urn, "enrich-cll")
    io = _aspect_for_urn(workunits, job_urn, DataJobInputOutputClass)
    assert isinstance(io, DataJobInputOutputClass)
    assert io.fineGrainedLineages
    assert report.stream_processing_column_lineage_edges == 1


def test_build_workunits_skips_column_lineage_when_any_topic_denied() -> None:
    report = KafkaSourceReport()
    jobs = [
        StreamProcessingJob(
            engine=StreamProcessingEngine.FLINK,
            job_id="enrich-mixed",
            name="enrich-mixed",
            input_topics=["src_topic", "denied_topic"],
            output_topics=["sink_topic"],
            parse_query="INSERT INTO sink_topic SELECT id FROM src_topic",
            sql_dialect=FLINK_SQL_DIALECT,
        )
    ]

    workunits = build_stream_processing_workunits(
        jobs=jobs,
        platform=PLATFORM,
        platform_instance=None,
        env=ENV,
        report=report,
        topic_allowed=lambda topic: topic != "denied_topic",
        graph=None,
        include_column_lineage=True,
    )

    flow_urn = make_data_flow_urn(
        PLATFORM, ENGINE_FLOW_METADATA[StreamProcessingEngine.FLINK][0], ENV
    )
    job_urn = make_data_job_urn_with_flow(flow_urn, "enrich-mixed")
    io = _aspect_for_urn(workunits, job_urn, DataJobInputOutputClass)
    assert isinstance(io, DataJobInputOutputClass)
    assert io.fineGrainedLineages is None
    assert report.stream_processing_column_lineage_edges == 0


def test_flink_requires_credentials_when_enabled() -> None:
    with pytest.raises(ValueError, match="must both be set"):
        FlinkLineageConfig(
            enabled=True,
            organization_id="org-1",
            environment_id="env-1",
            kafka_cluster_id="lkc-1",
            region="us-east-1",
            cloud="aws",
        )


def test_flink_rejects_unknown_cloud() -> None:
    with pytest.raises(ValueError, match="must be one of"):
        FlinkLineageConfig(
            enabled=True,
            organization_id="org-1",
            environment_id="env-1",
            kafka_cluster_id="lkc-1",
            region="us-east-1",
            cloud="digitalocean",
            api_key="k",
            api_secret="s",
        )


def test_flink_normalizes_cloud_case() -> None:
    config = FlinkLineageConfig(
        enabled=True,
        organization_id="org-1",
        environment_id="env-1",
        kafka_cluster_id="lkc-1",
        region="us-east-1",
        cloud="AWS",
        api_key="k",
        api_secret="s",
    )
    assert config.cloud == "aws"


def test_parse_query_requires_sql_dialect() -> None:
    with pytest.raises(ValidationError, match="sql_dialect"):
        StreamProcessingJob(
            engine=StreamProcessingEngine.FLINK,
            job_id="j",
            name="j",
            parse_query="SELECT 1",
        )


def test_flink_filters_compute_pool_and_malformed_statements() -> None:
    sql = "INSERT INTO sink SELECT id FROM src"
    jobs, _ = _extract(
        [
            _statement(sql, name="other-pool", compute_pool_id="pool-2"),
            {"name": "no-spec"},
            {"spec": {"statement": sql}, "status": {"phase": "RUNNING"}},
            _statement(sql, name="ok", compute_pool_id="pool-1"),
        ],
        compute_pool_id="pool-1",
    )

    assert [job.job_id for job in jobs] == ["ok"]


def _flink_client_with_session(session: MagicMock) -> FlinkStatementsClient:
    session.headers = {}
    return FlinkStatementsClient(
        statements_url="https://flink.example/sql",
        credentials=None,
        timeout_seconds=1,
        report=KafkaSourceReport(),
        session=session,
    )


def test_flink_list_statements_reraises_http_errors() -> None:
    session = MagicMock()
    session.get.side_effect = requests.ConnectionError("boom")
    client = _flink_client_with_session(session)
    with pytest.raises(requests.ConnectionError):
        client.list_statements()


def test_flink_list_statements_reraises_non_dict_payload() -> None:
    session = MagicMock()
    response = MagicMock()
    response.json.return_value = ["not", "a", "dict"]
    session.get.return_value = response
    client = _flink_client_with_session(session)
    with pytest.raises(ValueError, match="payload type"):
        client.list_statements()


def test_flink_list_statements_follows_pagination() -> None:
    session = MagicMock()
    page1 = MagicMock()
    page1.json.return_value = {
        "data": [_statement("INSERT INTO sink SELECT id FROM src", name="s1")],
        "metadata": {"next": "https://flink.example/sql?page=2"},
    }
    page2 = MagicMock()
    page2.json.return_value = {
        "data": [_statement("INSERT INTO sink2 SELECT id FROM src2", name="s2")]
    }
    session.get.side_effect = [page1, page2]
    client = _flink_client_with_session(session)

    jobs = FlinkLineageExtractor(client, KafkaSourceReport(), _flink_config()).extract()

    assert [job.job_id for job in jobs] == ["s1", "s2"]
    assert session.get.call_count == 2


def test_flink_close_only_closes_owned_session() -> None:
    injected = MagicMock()
    client = _flink_client_with_session(injected)
    client.close()
    injected.close.assert_not_called()

    owned_session = MagicMock()
    owned_session.headers = {}
    with patch(
        "datahub.ingestion.source.kafka.stream_processing.flink.requests.Session",
        return_value=owned_session,
    ):
        owned = FlinkStatementsClient(
            statements_url="https://flink.example/sql",
            credentials=None,
            timeout_seconds=1,
            report=KafkaSourceReport(),
        )
    owned.close()
    owned_session.close.assert_called_once()


def test_flink_listing_truncated_at_page_cap_is_a_failure() -> None:
    session = MagicMock()
    page = MagicMock()
    page.json.return_value = {
        "data": [_statement("INSERT INTO sink SELECT id FROM src", name="s1")],
        "metadata": {"next": "https://flink.example/sql?page=2"},
    }
    session.get.return_value = page
    client = _flink_client_with_session(session)

    with patch(
        "datahub.ingestion.source.kafka.stream_processing.flink.FLINK_MAX_PAGES", 1
    ):
        statements = client.list_statements()

    assert [statement["name"] for statement in statements] == ["s1"]
    assert client.report.failures


def test_flink_error_in_one_statement_keeps_the_others_and_fails_the_run() -> None:
    real_parse = flink_module.parse_flink_statement

    def parse(sql: str) -> List[FlinkDmlStatement]:
        if "poison" in sql:
            raise RuntimeError("boom")
        return real_parse(sql)

    with patch.object(flink_module, "parse_flink_statement", side_effect=parse):
        jobs, report = _extract(
            [
                _statement("INSERT INTO a SELECT * FROM x", name="ok-1"),
                _statement("INSERT INTO poison SELECT * FROM x", name="bad"),
                _statement("INSERT INTO b SELECT * FROM y", name="ok-2"),
            ]
        )

    assert [job.name for job in jobs] == ["ok-1", "ok-2"]
    assert report.failures


def test_flink_tvf_argument_after_a_comment() -> None:
    jobs, _ = _extract(
        [
            _statement(
                "INSERT INTO agg SELECT window_start, COUNT(*) FROM TABLE(TUMBLE( -- in\n"
                "  /* windowed */ TABLE orders, DESCRIPTOR(ts), INTERVAL '1' MINUTES)) "
                "GROUP BY window_start"
            )
        ]
    )

    assert jobs[0].input_topics == ["orders"]


def _graph_with_schema(columns: List[str]) -> MagicMock:
    resolver = SchemaResolver(platform=PLATFORM, env=ENV)
    resolver.add_raw_schema_info(
        make_dataset_urn_with_platform_instance(
            platform=PLATFORM, name="src_topic", platform_instance=None, env=ENV
        ),
        {column: "VARCHAR" for column in columns},
    )
    graph = MagicMock(spec=DataHubGraph)
    graph._make_schema_resolver.return_value = resolver
    return graph


def test_column_lineage_expands_select_star_from_graph_schema() -> None:
    fine_grained = column_lineage_fine_grained(
        query="INSERT INTO sink_topic SELECT * FROM src_topic",
        platform=PLATFORM,
        platform_instance=None,
        env=ENV,
        graph=_graph_with_schema(["id", "name"]),
        dialect=FLINK_SQL_DIALECT,
        report=KafkaSourceReport(),
        job_name="stmt",
    )

    downstreams = {urn for edge in fine_grained for urn in edge.downstreams or []}
    assert {urn.rsplit(",", 1)[1] for urn in downstreams} == {"id)", "name)"}


def test_column_lineage_parse_error_is_reported_and_returns_nothing() -> None:
    report = KafkaSourceReport()

    fine_grained = column_lineage_fine_grained(
        query="INSERT INTO sink_topic SELEC id FRM src_topic",
        platform=PLATFORM,
        platform_instance=None,
        env=ENV,
        graph=None,
        dialect=FLINK_SQL_DIALECT,
        report=report,
        job_name="stmt",
    )

    assert fine_grained == []
    assert report.warnings


def test_schema_resolver_failure_keeps_topic_edges() -> None:
    graph = MagicMock(spec=DataHubGraph)
    graph._make_schema_resolver.side_effect = RuntimeError("graph down")
    report = KafkaSourceReport()
    job = StreamProcessingJob(
        engine=StreamProcessingEngine.FLINK,
        job_id="stmt",
        name="stmt",
        input_topics=["src_topic"],
        output_topics=["sink_topic"],
        parse_query="INSERT INTO sink_topic SELECT id FROM src_topic",
        sql_dialect=FLINK_SQL_DIALECT,
    )

    workunits = build_stream_processing_workunits(
        jobs=[job],
        platform=PLATFORM,
        platform_instance=None,
        env=ENV,
        report=report,
        topic_allowed=lambda _topic: True,
        graph=graph,
        include_column_lineage=True,
    )

    flow_urn = make_data_flow_urn(
        PLATFORM, ENGINE_FLOW_METADATA[StreamProcessingEngine.FLINK][0], ENV
    )
    io = _aspect_for_urn(
        workunits,
        make_data_job_urn_with_flow(flow_urn, "stmt"),
        DataJobInputOutputClass,
    )
    assert isinstance(io, DataJobInputOutputClass)
    assert len(io.inputDatasets) == 1 and len(io.outputDatasets) == 1
    assert io.fineGrainedLineages is None
    assert report.warnings


def _kafka_source_with_flink() -> KafkaSource:
    return KafkaSource(
        KafkaSourceConfig.model_validate(
            {
                "connection": {"bootstrap": "localhost:9092"},
                "stream_processing_lineage": {
                    "flink": _flink_config().model_dump(mode="json")
                    | {"api_key": "k", "api_secret": "s"},
                },
            }
        ),
        PipelineContext(run_id="test"),
    )


@patch("datahub.ingestion.source.kafka.kafka.AdminClient", autospec=True)
@patch("datahub.ingestion.source.kafka.kafka.confluent_kafka.Consumer", autospec=True)
def test_kafka_source_emits_flink_jobs(mock_consumer: MagicMock, _: MagicMock) -> None:
    mock_consumer.return_value.list_topics.return_value.topics = {}
    client = _FakeFlinkClient([_statement("INSERT INTO sink SELECT id FROM src")])

    with patch(
        "datahub.ingestion.source.kafka.kafka.build_flink_client", return_value=client
    ):
        source = _kafka_source_with_flink()
        urns = {wu.get_urn() for wu in source.get_workunits()}

    flow_urn = make_data_flow_urn(
        PLATFORM, ENGINE_FLOW_METADATA[StreamProcessingEngine.FLINK][0], ENV
    )
    assert make_data_job_urn_with_flow(flow_urn, "stmt") in urns
    assert not source.report.failures


@patch("datahub.ingestion.source.kafka.kafka.AdminClient", autospec=True)
@patch("datahub.ingestion.source.kafka.kafka.confluent_kafka.Consumer", autospec=True)
def test_kafka_source_reports_a_failed_flink_listing_as_a_failure(
    mock_consumer: MagicMock, _: MagicMock
) -> None:
    mock_consumer.return_value.list_topics.return_value.topics = {}
    client = MagicMock()
    client.list_statements.side_effect = requests.HTTPError("503")

    with patch(
        "datahub.ingestion.source.kafka.kafka.build_flink_client", return_value=client
    ):
        source = _kafka_source_with_flink()
        workunits = list(source.get_workunits())

    # The failure is what stops stale-entity removal from soft-deleting the jobs.
    assert source.report.failures
    assert not [wu for wu in workunits if "dataJob" in wu.get_urn()]
    client.close.assert_called_once()
