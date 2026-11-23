from typing import Callable, Dict, List, Optional, Tuple
from unittest.mock import MagicMock, patch

import pytest
import requests
import requests_mock
from pydantic import ValidationError

from datahub.emitter.mce_builder import (
    make_data_flow_urn,
    make_data_job_urn_with_flow,
    make_dataset_urn_with_platform_instance,
    make_schema_field_urn,
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
from datahub.ingestion.source.kafka.stream_processing import (
    builder as builder_module,
    flink as flink_module,
)
from datahub.ingestion.source.kafka.stream_processing.builder import (
    build_flink_workunits,
)
from datahub.ingestion.source.kafka.stream_processing.flink import (
    FlinkLineageExtractor,
    FlinkListingError,
    FlinkStatementsClient,
    build_flink_client,
)
from datahub.ingestion.source.kafka.stream_processing.flink_sql import (
    FlinkParsedStatement,
)
from datahub.ingestion.source.kafka.stream_processing.models import FlinkJob
from datahub.ingestion.source.kafka.stream_processing.sql import (
    column_lineage_fine_grained,
)
from datahub.metadata.schema_classes import DataJobInputOutputClass
from datahub.sql_parsing.schema_resolver import SchemaResolver

PLATFORM = "kafka"
ENV = "PROD"
ENV_ID = "env-1"
CLUSTER_ID = "lkc-1"
FLOW_ID = f"{ENV_ID}.{CLUSTER_ID}"
STATEMENTS_URL = (
    "https://flink.us-east-1.aws.confluent.cloud/sql/v1/organizations/org-1/"
    "environments/env-1/statements"
)


class _FakeFlinkClient:
    def __init__(self, statements: List[Dict[str, object]]) -> None:
        self._statements = statements

    def list_statements(self) -> List[Dict[str, object]]:
        return self._statements

    def close(self) -> None:
        pass


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
) -> Tuple[List[FlinkJob], KafkaSourceReport]:
    report = KafkaSourceReport()
    jobs = FlinkLineageExtractor(
        _FakeFlinkClient(statements),  # type: ignore[arg-type]
        report,
        _flink_config(**overrides),
    ).extract()
    return jobs, report


def _job(
    input_topics: List[str],
    output_topics: List[str],
    parse_query: Optional[str] = None,
    name: str = "stmt",
) -> FlinkJob:
    return FlinkJob(
        name=name,
        input_topics=input_topics,
        output_topics=output_topics,
        query=parse_query or "INSERT INTO ...",
        parse_query=parse_query,
    )


def _build(
    jobs: List[FlinkJob],
    report: KafkaSourceReport,
    platform_instance: Optional[str] = None,
    graph: Optional[DataHubGraph] = None,
    topic_allowed: Callable[[str], bool] = lambda _topic: True,
    include_column_lineage: bool = True,
    kafka_cluster_id: str = CLUSTER_ID,
) -> List[MetadataWorkUnit]:
    return list(
        build_flink_workunits(
            jobs=jobs,
            environment_id=ENV_ID,
            kafka_cluster_id=kafka_cluster_id,
            topic_platform=PLATFORM,
            platform_instance=platform_instance,
            env=ENV,
            report=report,
            topic_allowed=topic_allowed,
            graph=graph,
            include_column_lineage=include_column_lineage,
        )
    )


def _io(workunits: List[MetadataWorkUnit]) -> DataJobInputOutputClass:
    for wu in workunits:
        assert isinstance(wu.metadata, MetadataChangeProposalWrapper)
        if isinstance(wu.metadata.aspect, DataJobInputOutputClass):
            return wu.metadata.aspect
    raise AssertionError("no DataJobInputOutput emitted")


def _topic(name: str, platform_instance: Optional[str] = None) -> str:
    return make_dataset_urn_with_platform_instance(
        platform=PLATFORM, name=name, platform_instance=platform_instance, env=ENV
    )


def _field_edges(workunits: List[MetadataWorkUnit]) -> List[Tuple[str, str]]:
    return sorted(
        (upstream, downstream)
        for edge in _io(workunits).fineGrainedLineages or []
        for upstream in edge.upstreams or []
        for downstream in edge.downstreams or []
    )


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

    assert [(job.input_topics, job.output_topics) for job in jobs] == [
        (["src_topic"], ["sink_topic"])
    ]
    assert jobs[0].custom_properties["state"] == "RUNNING"
    assert jobs[0].parse_query is not None
    assert "`prod`" not in jobs[0].parse_query


@pytest.mark.parametrize(
    "sql,inputs",
    [
        pytest.param(
            "INSERT INTO hourly SELECT EXTRACT(HOUR FROM ts) AS h, id FROM orders",
            ["orders"],
            id="EXTRACT ... FROM",
        ),
        pytest.param(
            "INSERT INTO agg SELECT window_start, COUNT(*) FROM TABLE(TUMBLE("
            "TABLE orders, DESCRIPTOR(ts), INTERVAL '1' MINUTES)) "
            "GROUP BY window_start, window_end",
            ["orders"],
            id="window TVF",
        ),
        pytest.param(
            "INSERT INTO agg SELECT window_start, COUNT(*) FROM TABLE(TUMBLE( -- in\n"
            "  /* windowed */ TABLE orders, DESCRIPTOR(ts), INTERVAL '1' MINUTES)) "
            "GROUP BY window_start",
            ["orders"],
            id="window TVF with comments",
        ),
        pytest.param(
            "INSERT INTO out_t WITH big AS (SELECT * FROM orders WHERE amount > 10) "
            "SELECT * FROM big",
            ["orders"],
            id="CTE",
        ),
        pytest.param(
            "EXECUTE INSERT INTO out_t SELECT * FROM orders",
            ["orders"],
            id="EXECUTE INSERT",
        ),
        pytest.param(
            "CREATE TABLE out_t AS SELECT * FROM orders", ["orders"], id="CTAS"
        ),
    ],
)
def test_flink_input_topics(sql: str, inputs: List[str]) -> None:
    jobs, _ = _extract([_statement(sql)])

    assert jobs[0].input_topics == inputs


@pytest.mark.parametrize(
    "sql",
    [
        "EXECUTE STATEMENT SET BEGIN INSERT INTO a SELECT * FROM x; "
        "INSERT INTO b SELECT * FROM y; END;",
        "-- lead\nEXECUTE STATEMENT SET BEGIN INSERT INTO a SELECT * FROM x;\n"
        "INSERT INTO b SELECT * FROM y; END; -- trail",
    ],
)
def test_flink_reads_every_insert_in_a_statement_set(sql: str) -> None:
    jobs, _ = _extract([_statement(sql)])

    assert jobs[0].input_topics == ["x", "y"]
    assert jobs[0].output_topics == ["a", "b"]
    assert jobs[0].parse_query is None


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

    assert [(job.input_topics, job.output_topics) for job in jobs] == [
        (["customers"], ["sink"])
    ]
    # Column lineage would resolve the other cluster's table onto this cluster.
    assert jobs[0].parse_query is None
    assert list(report.flink_statements_other_cluster) == ["writes-elsewhere"]
    assert report.warnings


def test_flink_warns_when_no_statement_matches_this_cluster() -> None:
    jobs, report = _extract(
        [
            _statement(
                "INSERT INTO `Prod Env`.`Main Cluster`.`sink` SELECT * FROM src",
                properties={
                    "sql.current-catalog": "Prod Env",
                    "sql.current-database": "Main Cluster",
                },
            )
        ]
    )

    assert jobs == []
    assert any(
        "Prod Env.Main Cluster.sink" in str(entry.context) for entry in report.warnings
    )


@pytest.mark.parametrize(
    "statement",
    [
        pytest.param(
            _statement("INSERT INTO sink SELECT * FROM src", properties={}),
            id="unresolved table",
        ),
        pytest.param(_statement("INSERT INTO sink SELECT FROM WHERE"), id="bad SQL"),
        pytest.param(
            _statement(
                "CREATE TABLE t DISTRIBUTED BY (id) INTO 4 BUCKETS AS SELECT * FROM x"
            ),
            id="CTAS sqlglot cannot read",
        ),
    ],
)
def test_flink_unreadable_statements_are_failures(
    statement: Dict[str, object],
) -> None:
    jobs, report = _extract([statement])

    assert jobs == []
    assert list(report.flink_statements_skipped) == ["stmt"]
    # A failure keeps stale-entity removal from deleting the statement's old job.
    assert report.failures


def test_flink_skips_statements_that_are_not_running_by_default() -> None:
    statements = [_statement("INSERT INTO sink SELECT * FROM src", phase="STOPPED")]

    jobs, report = _extract(statements)
    assert jobs == []
    assert list(report.flink_statements_not_running) == ["stmt"]

    jobs, _ = _extract(statements, include_non_running_statements=True)
    assert [job.custom_properties["state"] for job in jobs] == ["STOPPED"]


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

    assert [job.name for job in jobs] == ["ok"]


def test_flink_error_in_one_statement_keeps_the_others_and_fails_the_run() -> None:
    real_parse = flink_module.parse_flink_statement

    def parse(sql: str) -> FlinkParsedStatement:
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


def test_build_emits_jobs_on_the_flink_platform_scoped_by_platform_instance() -> None:
    report = KafkaSourceReport()

    workunits = _build(
        [_job(["src_topic", "denied"], ["sink_topic"])],
        report,
        platform_instance="pi",
        topic_allowed=lambda topic: topic != "denied",
    )

    flow_urn = make_data_flow_urn("flink", FLOW_ID, ENV, platform_instance="pi")
    job_urn = make_data_job_urn_with_flow(flow_urn, "stmt")
    assert {wu.get_urn() for wu in workunits} == {flow_urn, job_urn}
    io = _io(workunits)
    assert io.inputDatasets == [_topic("src_topic", "pi")]
    assert io.outputDatasets == [_topic("sink_topic", "pi")]
    assert io.fineGrainedLineages is None
    assert report.flink_jobs_emitted == 1


def test_column_lineage_keeps_the_real_case_of_topics_and_columns() -> None:
    # The parser lowercases both; Flink SQL is case-sensitive, so the query's own
    # spelling is the field name.
    workunits = _build(
        [
            _job(
                ["Orders-In"],
                ["Orders-Out"],
                "INSERT INTO `Orders-Out` SELECT order_id, `Amount` FROM `Orders-In`",
            )
        ],
        KafkaSourceReport(),
    )

    assert _field_edges(workunits) == [
        (
            make_schema_field_urn(_topic("Orders-In"), column),
            make_schema_field_urn(_topic("Orders-Out"), column),
        )
        for column in ["Amount", "order_id"]
    ]


def test_column_lineage_drops_columns_spelled_two_ways() -> None:
    workunits = _build(
        [
            _job(
                ["src"],
                ["sink"],
                "INSERT INTO sink SELECT Amount, id FROM src WHERE amount > 0",
            )
        ],
        KafkaSourceReport(),
    )

    assert [down.rsplit(",", 1)[1] for _, down in _field_edges(workunits)] == ["id)"]


def _graph_with_schema(columns: List[str]) -> MagicMock:
    resolver = SchemaResolver(platform=PLATFORM, env=ENV)
    resolver.add_raw_schema_info(
        _topic("src_topic"), {column: "VARCHAR" for column in columns}
    )
    graph = MagicMock(spec=DataHubGraph)
    graph._make_schema_resolver.return_value = resolver
    return graph


def test_column_lineage_uses_schema_case_and_expands_select_star() -> None:
    workunits = _build(
        [
            _job(
                ["src_topic"],
                ["sink_topic"],
                "INSERT INTO sink_topic SELECT * FROM src_topic",
            )
        ],
        KafkaSourceReport(),
        graph=_graph_with_schema(["OrderId", "name"]),
    )

    assert [down.rsplit(",", 1)[1] for _, down in _field_edges(workunits)] == [
        "OrderId)",
        "name)",
    ]


def test_column_lineage_parse_error_is_reported_and_topic_edges_survive() -> None:
    report = KafkaSourceReport()

    workunits = _build(
        [
            _job(
                ["src_topic"],
                ["sink_topic"],
                "INSERT INTO sink_topic SELEC id FRM src_topic",
            )
        ],
        report,
    )

    io = _io(workunits)
    assert io.inputDatasets == [_topic("src_topic")]
    assert io.fineGrainedLineages is None
    assert report.warnings


def test_column_lineage_without_a_query_is_empty() -> None:
    lineage = column_lineage_fine_grained(
        _job(["a"], ["b"]),
        {"a": _topic("a"), "b": _topic("b")},
        SchemaResolver(platform=PLATFORM, env=ENV),
        None,
        KafkaSourceReport(),
    )

    assert lineage == []


@pytest.mark.parametrize(
    "config,url",
    [
        pytest.param({}, STATEMENTS_URL, id="region and cloud"),
        pytest.param(
            {"endpoint": "https://flink.example.com/"},
            "https://flink.example.com/sql/v1/organizations/org-1/"
            "environments/env-1/statements",
            id="endpoint with trailing slash",
        ),
    ],
)
def test_build_flink_client_url_and_auth(config: Dict[str, str], url: str) -> None:
    client = build_flink_client(_flink_config(**config), KafkaSourceReport())

    with requests_mock.Mocker() as m:
        m.get(url, json={"data": [{"name": "s1"}]})
        statements = client.list_statements()

    assert statements == [{"name": "s1"}]
    assert m.last_request.headers["Authorization"].startswith("Basic ")
    assert m.last_request.qs == {"page_size": ["100"]}


def _client(
    m: requests_mock.Mocker, pages: List[Dict[str, object]]
) -> FlinkStatementsClient:
    m.get(STATEMENTS_URL, [{"json": page} for page in pages])
    return build_flink_client(_flink_config(), KafkaSourceReport())


def test_flink_list_statements_follows_same_host_pagination() -> None:
    with requests_mock.Mocker() as m:
        client = _client(
            m,
            [
                {
                    "data": [{"name": "s1"}],
                    "metadata": {"next": f"{STATEMENTS_URL}?page=2"},
                },
                {"data": [{"name": "s2"}]},
            ],
        )
        statements = client.list_statements()

    assert statements == [{"name": "s1"}, {"name": "s2"}]


@pytest.mark.parametrize(
    "next_link",
    [
        "http://flink.us-east-1.aws.confluent.cloud/sql/v1/next",
        "https://attacker.example.com/sql/v1/next",
    ],
)
def test_flink_refuses_next_links_off_the_https_host(next_link: str) -> None:
    with requests_mock.Mocker() as m:
        client = _client(m, [{"data": [], "metadata": {"next": next_link}}])
        with pytest.raises(FlinkListingError):
            client.list_statements()

    assert m.call_count == 1


def test_flink_does_not_follow_redirects() -> None:
    with requests_mock.Mocker() as m:
        m.get(
            STATEMENTS_URL,
            status_code=302,
            headers={"Location": "http://elsewhere.example.com/"},
        )
        client = build_flink_client(_flink_config(), KafkaSourceReport())
        with pytest.raises(FlinkListingError):
            client.list_statements()

    assert m.call_count == 1


def test_flink_listing_truncated_at_page_cap_is_a_failure() -> None:
    with requests_mock.Mocker() as m:
        client = _client(
            m,
            [{"data": [{"name": "s1"}], "metadata": {"next": f"{STATEMENTS_URL}?p=2"}}],
        )
        with patch.object(flink_module, "FLINK_MAX_PAGES", 1):
            statements = client.list_statements()

    assert statements == [{"name": "s1"}]
    assert client.report.failures


@pytest.mark.parametrize(
    "overrides",
    [
        pytest.param({"endpoint": "http://flink.example.com"}, id="http endpoint"),
        pytest.param({"region": None}, id="no endpoint or region"),
        pytest.param({"cloud": "digitalocean"}, id="unknown cloud"),
        pytest.param({"organization_id": None}, id="missing organization"),
        pytest.param({"environment_id": None}, id="missing environment"),
        pytest.param({"api_secret": None}, id="half a key pair"),
    ],
)
def test_flink_config_rejects(overrides: Dict[str, object]) -> None:
    with pytest.raises(ValidationError):
        _flink_config(**overrides)


def test_flink_cloud_is_case_insensitive() -> None:
    assert _flink_config(cloud="AWS").cloud == "aws"


def _flink_dict(**overrides: object) -> Dict[str, object]:
    values = _flink_config().model_dump(mode="json")
    values.update({"api_key": "k", "api_secret": "s", **overrides})
    return values


def _source_config(
    flink: Dict[str, object], catalog_cluster_id: Optional[str] = None
) -> KafkaSourceConfig:
    return KafkaSourceConfig.model_validate(
        {
            "connection": {"bootstrap": "localhost:9092"},
            "confluent_catalog": {"cluster_id": catalog_cluster_id},
            "stream_processing_lineage": {"flink": flink},
        }
    )


def test_flink_kafka_cluster_id_defaults_to_the_catalog_one() -> None:
    config = _source_config(_flink_dict(kafka_cluster_id=None), "lkc-9")

    assert config.stream_processing_lineage.flink.kafka_cluster_id == "lkc-9"


@pytest.mark.parametrize(
    "flink_cluster_id,catalog_cluster_id",
    [
        pytest.param("lkc-1", "lkc-2", id="different"),
        pytest.param(None, None, id="none"),
    ],
)
def test_flink_kafka_cluster_id_conflict_or_missing_is_rejected(
    flink_cluster_id: Optional[str], catalog_cluster_id: Optional[str]
) -> None:
    with pytest.raises(ValidationError):
        _source_config(
            _flink_dict(kafka_cluster_id=flink_cluster_id), catalog_cluster_id
        )


def _kafka_source_with_flink() -> KafkaSource:
    return KafkaSource(_source_config(_flink_dict()), PipelineContext(run_id="test"))


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

    flow_urn = make_data_flow_urn("flink", FLOW_ID, ENV)
    assert make_data_job_urn_with_flow(flow_urn, "stmt") in urns
    assert not source.report.failures


@pytest.mark.parametrize("status", [401, 503])
@patch("datahub.ingestion.source.kafka.kafka.AdminClient", autospec=True)
@patch("datahub.ingestion.source.kafka.kafka.confluent_kafka.Consumer", autospec=True)
def test_kafka_source_reports_a_failed_flink_listing_as_a_failure(
    mock_consumer: MagicMock, _: MagicMock, status: int
) -> None:
    mock_consumer.return_value.list_topics.return_value.topics = {}
    response = requests.Response()
    response.status_code = status
    client = MagicMock()
    client.list_statements.side_effect = requests.HTTPError(response=response)

    with patch(
        "datahub.ingestion.source.kafka.kafka.build_flink_client", return_value=client
    ):
        source = _kafka_source_with_flink()
        workunits = list(source.get_workunits())

    # The failure is what stops stale-entity removal from soft-deleting the jobs.
    assert source.report.failures
    assert not [wu for wu in workunits if "dataJob" in wu.get_urn()]
    client.close.assert_called_once()


@pytest.mark.parametrize(
    "ddl",
    [
        "CREATE TABLE t (price INT, qty INT, total AS (price * qty), ts TIMESTAMP(3), "
        "WATERMARK FOR ts AS (ts - INTERVAL '5' SECOND)) "
        "DISTRIBUTED BY (price) INTO 4 BUCKETS",
        "CREATE TABLE t (price INT, total AS (price * 2)) "
        "WITH ('changelog.mode' = 'append')",
    ],
)
def test_flink_ddl_with_computed_columns_is_not_a_failure(ddl: str) -> None:
    jobs, report = _extract([_statement(ddl)])

    assert jobs == []
    assert not report.failures


def test_flink_named_statement_without_sql_is_a_failure() -> None:
    jobs, report = _extract([{"name": "stmt", "status": {"phase": "RUNNING"}}, {}])

    assert jobs == []
    assert list(report.flink_statements_skipped) == ["stmt"]
    assert len(report.failures) == 1


def test_column_spelling_ignores_table_names() -> None:
    workunits = _build(
        [_job(["foo"], ["sink"], "INSERT INTO sink SELECT Foo FROM foo")],
        KafkaSourceReport(),
    )

    assert _field_edges(workunits) == [
        (
            make_schema_field_urn(_topic("foo"), "Foo"),
            make_schema_field_urn(_topic("sink"), "Foo"),
        )
    ]


def test_column_lineage_with_a_dotted_topic_under_a_platform_instance() -> None:
    # Topic URNs carry the platform instance as a name prefix (`pi.pi.orders`), so
    # stripping it once gives back the topic `pi.orders`.
    workunits = _build(
        [_job(["pi.orders"], ["out"], "INSERT INTO out SELECT id FROM `pi.orders`")],
        KafkaSourceReport(),
        platform_instance="pi",
    )

    assert _field_edges(workunits) == [
        (
            make_schema_field_urn(_topic("pi.orders", "pi"), "id"),
            make_schema_field_urn(_topic("out", "pi"), "id"),
        )
    ]


def test_one_job_failing_to_build_does_not_stop_the_others() -> None:
    real_properties = builder_module._job_properties

    def properties(job: FlinkJob) -> Dict[str, str]:
        if job.name == "bad":
            raise RuntimeError("boom")
        return real_properties(job)

    report = KafkaSourceReport()
    with patch.object(builder_module, "_job_properties", side_effect=properties):
        workunits = _build(
            [
                _job(["a"], ["b"], name="ok-1"),
                _job(["a"], ["b"], name="bad"),
                _job(["a"], ["b"], name="ok-2"),
            ],
            report,
        )

    job_urns = {wu.get_urn() for wu in workunits if "dataJob" in wu.get_urn()}
    flow_urn = make_data_flow_urn("flink", FLOW_ID, ENV)
    assert job_urns == {
        make_data_job_urn_with_flow(flow_urn, "ok-1"),
        make_data_job_urn_with_flow(flow_urn, "ok-2"),
    }
    assert report.failures


def test_column_lineage_error_keeps_the_job_and_warns() -> None:
    report = KafkaSourceReport()
    with patch.object(
        builder_module, "column_lineage_fine_grained", side_effect=RuntimeError("boom")
    ):
        workunits = _build(
            [_job(["a"], ["b"], "INSERT INTO b SELECT id FROM a")], report
        )

    assert _io(workunits).outputDatasets == [_topic("b")]
    assert report.warnings and not report.failures


def test_no_schema_resolver_without_column_lineage() -> None:
    graph = MagicMock(spec=DataHubGraph)

    _build(
        [_job(["a"], ["b"], "INSERT INTO b SELECT id FROM a")],
        KafkaSourceReport(),
        graph=graph,
        include_column_lineage=False,
    )

    graph._make_schema_resolver.assert_not_called()


def test_statement_set_jobs_are_scoped_to_each_cluster() -> None:
    # The same statement writes topics in two clusters; each recipe emits its own job.
    urns = {
        wu.get_urn()
        for cluster in ("lkc-1", "lkc-2")
        for wu in _build(
            [_job(["a"], ["b"], name="set")],
            KafkaSourceReport(),
            kafka_cluster_id=cluster,
        )
        if "dataJob" in wu.get_urn()
    }

    assert urns == {
        make_data_job_urn_with_flow(
            make_data_flow_urn("flink", f"{ENV_ID}.{cluster}", ENV), "set"
        )
        for cluster in ("lkc-1", "lkc-2")
    }


def test_report_counts_a_job_only_after_it_is_yielded() -> None:
    report = KafkaSourceReport()
    workunits = build_flink_workunits(
        jobs=[_job(["a"], ["b"])],
        environment_id=ENV_ID,
        kafka_cluster_id=CLUSTER_ID,
        topic_platform=PLATFORM,
        platform_instance=None,
        env=ENV,
        report=report,
        topic_allowed=lambda _topic: True,
        graph=None,
        include_column_lineage=False,
    )

    next(iter(workunits))  # the DataFlow
    assert report.flink_jobs_emitted == 0
    list(workunits)
    assert report.flink_jobs_emitted == 1
    assert report.flink_topic_lineage_edges == 2


@pytest.mark.parametrize(
    "ddl",
    [
        "CREATE FUNCTION my_udf AS 'com.example.MyUdf' "
        "USING JAR 'confluent-artifact://cfa-1'",
        "CREATE VIEW v AS SELECT * FROM orders",
        "CREATE TEMPORARY VIEW v AS SELECT * FROM orders",
    ],
)
def test_flink_functions_and_views_move_no_data(ddl: str) -> None:
    jobs, report = _extract([_statement(ddl)])

    assert jobs == []
    assert not report.failures


def test_flink_unquoted_mixed_case_names_keep_their_case() -> None:
    jobs, _ = _extract(
        [
            _statement(
                "INSERT INTO OrdersOut SELECT * FROM Prod.MainCluster.OrdersIn",
                properties={
                    "sql.current-catalog": "Prod",
                    "sql.current-database": "MainCluster",
                },
            )
        ],
        environment_name="Prod",
        kafka_cluster_name="MainCluster",
    )

    assert [(job.input_topics, job.output_topics) for job in jobs] == [
        (["OrdersIn"], ["OrdersOut"])
    ]


@pytest.mark.parametrize(
    "phase,emitted",
    [
        ("PENDING", True),
        ("RUNNING", True),
        ("DEGRADED", True),
        ("FAILING", True),
        ("STOPPING", True),
        ("STOPPED", False),
        ("COMPLETED", False),
        ("FAILED", False),
        ("DELETING", False),
    ],
)
def test_flink_live_phases_are_read_by_default(phase: str, emitted: bool) -> None:
    jobs, _ = _extract([_statement("INSERT INTO sink SELECT * FROM src", phase=phase)])

    assert bool(jobs) == emitted


def test_flink_null_properties_and_page_data_are_empty() -> None:
    statement = _statement("INSERT INTO sink SELECT * FROM src")
    statement["spec"]["properties"] = None  # type: ignore[index]
    statement["spec"]["statement"] = (  # type: ignore[index]
        "INSERT INTO `env-1`.`lkc-1`.sink SELECT * FROM `env-1`.`lkc-1`.src"
    )

    jobs, report = _extract([statement])
    assert [job.output_topics for job in jobs] == [["sink"]]
    assert not report.failures

    with requests_mock.Mocker() as m:
        client = _client(m, [{"data": None, "metadata": None}])
        assert client.list_statements() == []


@pytest.mark.parametrize(
    "sql",
    [
        "CREATE TEMPORARY TABLE t AS SELECT * FROM orders",
        "CREATE TEMPORARY TABLE t DISTRIBUTED BY (id) INTO 2 BUCKETS AS SELECT * FROM x",
    ],
)
def test_flink_temporary_ctas_writes_no_topic(sql: str) -> None:
    jobs, report = _extract([_statement(sql)])

    assert jobs == []
    assert not report.failures


def test_flink_temporary_views_read_later_are_not_topics() -> None:
    jobs, _ = _extract(
        [
            _statement(
                "EXECUTE STATEMENT SET BEGIN "
                "CREATE TEMPORARY VIEW big AS SELECT * FROM orders WHERE amount > 10; "
                "INSERT INTO out_t SELECT * FROM big JOIN customers ON big.cid = customers.id; "
                "END;"
            )
        ]
    )

    assert [(job.input_topics, job.output_topics) for job in jobs] == [
        (["customers"], ["out_t"])
    ]


def test_flink_pool_filter_runs_before_empty_sql_failures() -> None:
    jobs, report = _extract(
        [{"name": "other-pool", "spec": {"compute_pool_id": "pool-2"}}],
        compute_pool_id="pool-1",
    )

    assert jobs == []
    assert not report.failures


def test_flink_listing_is_filtered_by_compute_pool() -> None:
    client = build_flink_client(
        _flink_config(compute_pool_id="lfcp-1"), KafkaSourceReport()
    )

    with requests_mock.Mocker() as m:
        m.get(STATEMENTS_URL, json={"data": []})
        client.list_statements()

    assert m.last_request.qs["spec.compute_pool_id"] == ["lfcp-1"]


def test_flink_temporary_ctas_keeps_its_topic_inputs_in_a_statement_set() -> None:
    jobs, _ = _extract(
        [
            _statement(
                "EXECUTE STATEMENT SET BEGIN "
                "CREATE TEMPORARY TABLE staged AS SELECT * FROM orders; "
                "INSERT INTO out_t SELECT * FROM staged; "
                "END;"
            )
        ]
    )

    assert [(job.input_topics, job.output_topics) for job in jobs] == [
        (["orders"], ["out_t"])
    ]


def test_flink_created_objects_are_matched_with_their_namespace() -> None:
    jobs, report = _extract(
        [
            _statement(
                "EXECUTE STATEMENT SET BEGIN "
                "CREATE TEMPORARY VIEW big AS SELECT * FROM orders; "
                "INSERT INTO out_t SELECT * FROM `env-1`.`lkc-1`.big "
                "JOIN `env-1`.`lkc-2`.big AS other ON other.id = big.id; "
                "END;"
            )
        ]
    )

    # The qualified read of the view is not a topic; the same name in another
    # cluster is a different table and is reported as such.
    assert [(job.input_topics, job.output_topics) for job in jobs] == [([], ["out_t"])]
    assert any("env-1.lkc-2.big" in str(entry.context) for entry in report.warnings)


def test_column_lineage_is_kept_when_created_objects_are_not_read() -> None:
    jobs, _ = _extract(
        [
            _statement(
                "EXECUTE STATEMENT SET BEGIN "
                "CREATE TEMPORARY VIEW unused AS SELECT * FROM orders; "
                "INSERT INTO out_t SELECT id FROM orders; "
                "END;"
            )
        ]
    )

    assert jobs[0].parse_query is not None


def test_column_lineage_is_dropped_when_a_created_object_is_read() -> None:
    jobs, _ = _extract(
        [
            _statement(
                "EXECUTE STATEMENT SET BEGIN "
                "CREATE TEMPORARY VIEW big AS SELECT * FROM orders; "
                "INSERT INTO out_t SELECT id FROM big JOIN customers ON big.id = customers.id; "
                "END;"
            )
        ]
    )

    assert jobs[0].parse_query is None


def test_created_objects_match_across_id_and_display_name() -> None:
    # Created under the environment / cluster ids, read under their display names.
    jobs, _ = _extract(
        [
            _statement(
                "EXECUTE STATEMENT SET BEGIN "
                "CREATE TEMPORARY VIEW `env-1`.`lkc-1`.big AS SELECT * FROM orders; "
                "INSERT INTO out_t SELECT * FROM `prod`.`main`.big; "
                "END;"
            )
        ]
    )

    assert [(job.input_topics, job.output_topics) for job in jobs] == [([], ["out_t"])]
