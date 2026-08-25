from typing import Dict, List
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
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.kafka.consumer_group_lineage import (
    ConsumerGroupInfo,
    ConsumerGroupMember,
)
from datahub.ingestion.source.kafka.kafka_config import (
    FlinkLineageConfig,
    KafkaStreamsLineageConfig,
    KsqlDBLineageConfig,
)
from datahub.ingestion.source.kafka.kafka_report import KafkaSourceReport
from datahub.ingestion.source.kafka.stream_processing.builder import (
    build_stream_processing_workunits,
)
from datahub.ingestion.source.kafka.stream_processing.constants import (
    ENGINE_FLOW_METADATA,
    PROP_LOW_CONFIDENCE,
    StreamProcessingEngine,
    from_join_identifiers,
    last_identifier_segment,
    strip_sql_noise,
)
from datahub.ingestion.source.kafka.stream_processing.flink import (
    FlinkLineageExtractor,
    FlinkStatementsClient,
)
from datahub.ingestion.source.kafka.stream_processing.kafka_streams import (
    KafkaStreamsLineageExtractor,
)
from datahub.ingestion.source.kafka.stream_processing.ksqldb import (
    KsqlDbClient,
    KsqlDBLineageExtractor,
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


class _FakeKsqlClient:
    def __init__(self, responses: Dict[str, List[Dict[str, object]]]) -> None:
        self._responses = responses

    def execute(self, statement: str) -> List[Dict[str, object]]:
        return self._responses.get(statement, [])

    def close(self) -> None:
        pass


class _FakeFlinkClient:
    def __init__(self, statements: List[Dict[str, object]]) -> None:
        self._statements = statements

    def list_statements(self) -> List[Dict[str, object]]:
        return self._statements

    def close(self) -> None:
        pass


class _FakeGroups:
    def __init__(self, groups: List[ConsumerGroupInfo]) -> None:
        self._groups = groups

    def extract(self) -> List[ConsumerGroupInfo]:
        return self._groups


def test_ksqldb_maps_source_and_sink_topics_and_rewrites_parse_query() -> None:
    report = KafkaSourceReport()
    client = _FakeKsqlClient(
        {
            "SHOW QUERIES;": [
                {
                    "@type": "queries",
                    "queries": [
                        {
                            "id": "CTAS_ENRICHED",
                            "queryType": "PERSISTENT",
                            "queryString": "CREATE TABLE ENRICHED AS SELECT id FROM ORDERS;",
                            "sinkKafkaTopics": ["enriched_topic"],
                        },
                        # Push query with no sink topic — not durable lineage, skipped.
                        {
                            "id": "transient_1",
                            "queryType": "PUSH",
                            "queryString": "SELECT * FROM ORDERS EMIT CHANGES;",
                            "sinkKafkaTopics": [],
                        },
                    ],
                }
            ],
            "LIST STREAMS;": [
                {
                    "@type": "streams",
                    "streams": [{"name": "ORDERS", "topic": "orders_topic"}],
                }
            ],
            "LIST TABLES;": [
                {
                    "@type": "tables",
                    "tables": [{"name": "ENRICHED", "topic": "enriched_topic"}],
                }
            ],
        }
    )

    jobs = KsqlDBLineageExtractor(client, report).extract()  # type: ignore[arg-type]

    assert len(jobs) == 1
    job = jobs[0]
    assert job.engine == StreamProcessingEngine.KSQLDB
    assert job.job_id == "CTAS_ENRICHED"
    assert job.input_topics == ["orders_topic"]
    assert job.output_topics == ["enriched_topic"]
    # Stream/table identifiers rewritten to their backing topics for column parsing.
    assert job.parse_query is not None
    assert "orders_topic" in job.parse_query
    assert "enriched_topic" in job.parse_query
    assert report.stream_processing_jobs_scanned == 1


def test_flink_extracts_insert_into_topics_and_collapses_identifiers() -> None:
    report = KafkaSourceReport()
    client = _FakeFlinkClient(
        [
            {
                "name": "enrich-statement",
                "spec": {
                    "statement": "INSERT INTO `cat`.`db`.`sink_topic` "
                    "SELECT id FROM `cat`.`db`.`src_topic`",
                    "compute_pool_id": "pool-1",
                },
                "status": {"phase": "RUNNING"},
            },
            # DDL statement (no INSERT INTO) is skipped.
            {
                "name": "ddl-statement",
                "spec": {"statement": "CREATE TABLE foo (id INT)"},
                "status": {"phase": "COMPLETED"},
            },
        ]
    )

    jobs = FlinkLineageExtractor(client, report).extract()  # type: ignore[arg-type]

    assert len(jobs) == 1
    job = jobs[0]
    assert job.engine == StreamProcessingEngine.FLINK
    assert job.input_topics == ["src_topic"]
    assert job.output_topics == ["sink_topic"]
    assert job.custom_properties.get("state") == "RUNNING"
    # 3-part identifiers collapsed to bare topic names for the SQL parser.
    assert job.parse_query is not None
    assert "`cat`.`db`" not in job.parse_query


def test_kafka_streams_detects_apps_by_internal_topics() -> None:
    report = KafkaSourceReport()
    extractor = KafkaStreamsLineageExtractor(
        admin_client=object(),  # type: ignore[arg-type]
        config=KafkaStreamsLineageConfig(enabled=True),
        report=report,
        timeout_seconds=10,
    )
    extractor._groups = _FakeGroups(  # type: ignore[assignment]
        [
            ConsumerGroupInfo(
                group_id="wordcount",
                state="STABLE",
                topics=[
                    "input-topic",
                    "wordcount-counts-store-changelog",
                    "wordcount-KSTREAM-AGGREGATE-repartition",
                ],
                members=[ConsumerGroupMember(client_id="c1", host="h1")],
            ),
            ConsumerGroupInfo(
                group_id="plain-consumer",
                state="STABLE",
                topics=["input-topic"],
                members=[],
            ),
        ]
    )

    jobs = extractor.extract()

    assert len(jobs) == 1
    job = jobs[0]
    assert job.engine == StreamProcessingEngine.KAFKA_STREAMS
    assert job.job_id == "wordcount"
    assert job.low_confidence is True
    assert job.input_topics == ["input-topic"]
    assert set(job.output_topics) == {
        "wordcount-counts-store-changelog",
        "wordcount-KSTREAM-AGGREGATE-repartition",
    }
    assert job.custom_properties["application_id"] == "wordcount"


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
        dialect="postgres",
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


def test_ksqldb_quotes_hyphenated_topic_names_in_parse_query() -> None:
    report = KafkaSourceReport()
    client = _FakeKsqlClient(
        {
            "SHOW QUERIES;": [
                {
                    "@type": "queries",
                    "queries": [
                        {
                            "id": "CSAS_ORDERS",
                            "queryType": "PERSISTENT",
                            "queryString": "CREATE STREAM ORDERS AS SELECT id FROM USERS;",
                            "sinkKafkaTopics": ["orders-enriched"],
                        }
                    ],
                }
            ],
            "LIST STREAMS;": [
                {
                    "@type": "streams",
                    "streams": [
                        {"name": "USERS", "topic": "users-raw"},
                        {"name": "ORDERS", "topic": "orders-enriched"},
                    ],
                }
            ],
            "LIST TABLES;": [{"@type": "tables", "tables": []}],
        }
    )

    jobs = KsqlDBLineageExtractor(client, report).extract()  # type: ignore[arg-type]

    assert len(jobs) == 1
    assert jobs[0].parse_query is not None
    assert '"users-raw"' in jobs[0].parse_query
    assert '"orders-enriched"' in jobs[0].parse_query


def test_flink_quotes_hyphenated_topic_names_in_parse_query() -> None:
    report = KafkaSourceReport()
    client = _FakeFlinkClient(
        [
            {
                "name": "enrich-hyphen",
                "spec": {
                    "statement": "INSERT INTO `cat`.`db`.`orders-enriched` "
                    "SELECT id FROM `cat`.`db`.`users-raw`",
                },
            }
        ]
    )

    jobs = FlinkLineageExtractor(client, report).extract()  # type: ignore[arg-type]

    assert len(jobs) == 1
    assert jobs[0].parse_query is not None
    assert '"orders-enriched"' in jobs[0].parse_query
    assert '"users-raw"' in jobs[0].parse_query


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
            sql_dialect="postgres",
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
            sql_dialect="postgres",
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


def test_build_workunits_emits_low_confidence_on_kafka_streams_jobs() -> None:
    report = KafkaSourceReport()
    jobs = [
        StreamProcessingJob(
            engine=StreamProcessingEngine.KAFKA_STREAMS,
            job_id="wordcount",
            name="wordcount",
            input_topics=["input-topic"],
            output_topics=["wordcount-counts-store-changelog"],
            low_confidence=True,
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
        include_column_lineage=False,
    )

    flow_urn = make_data_flow_urn(
        PLATFORM, ENGINE_FLOW_METADATA[StreamProcessingEngine.KAFKA_STREAMS][0], ENV
    )
    job_urn = make_data_job_urn_with_flow(flow_urn, "wordcount")
    job_info = _aspect_for_urn(workunits, job_urn, DataJobInfoClass)
    assert isinstance(job_info, DataJobInfoClass)
    assert job_info.customProperties[PROP_LOW_CONFIDENCE] == "true"


def test_ksqldb_rejects_partial_credentials() -> None:
    with pytest.raises(ValueError, match="must be provided together"):
        KsqlDBLineageConfig(
            enabled=True,
            endpoint="https://ksql.example:443",
            api_key="key-only",
        )


def test_ksqldb_allows_http_without_credentials() -> None:
    config = KsqlDBLineageConfig(enabled=True, endpoint="http://localhost:8088/")
    assert config.endpoint == "http://localhost:8088"


def test_ksqldb_rejects_http_with_credentials() -> None:
    with pytest.raises(ValueError, match="must use HTTPS"):
        KsqlDBLineageConfig(
            enabled=True,
            endpoint="http://localhost:8088",
            api_key="k",
            api_secret="s",
        )


def test_flink_requires_credentials_when_enabled() -> None:
    with pytest.raises(ValueError, match="must both be set"):
        FlinkLineageConfig(
            enabled=True,
            organization_id="org-1",
            environment_id="env-1",
            region="us-east-1",
            cloud="aws",
        )


def test_flink_rejects_unknown_cloud() -> None:
    with pytest.raises(ValueError, match="must be one of"):
        FlinkLineageConfig(
            enabled=True,
            organization_id="org-1",
            environment_id="env-1",
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


def test_last_identifier_segment_keeps_dots_inside_quotes() -> None:
    assert last_identifier_segment("`customer.events`") == "customer.events"
    assert last_identifier_segment("`cat`.`db`.`customer.events`") == "customer.events"
    assert last_identifier_segment("cat.db.table") == "table"


def test_from_join_identifiers_splits_comma_from_list() -> None:
    assert from_join_identifiers("SELECT * FROM t1, t2 JOIN t3") == ["t1", "t2", "t3"]


def test_sql_noise_does_not_match_from_in_comments_or_literals() -> None:
    sql = (
        "INSERT INTO sink SELECT id FROM src -- FROM decoy\n"
        " WHERE x = 'from other' /* FROM hidden */"
    )
    assert from_join_identifiers(sql) == ["src"]
    stripped = strip_sql_noise(sql)
    assert "decoy" not in stripped
    assert "hidden" not in stripped


def test_ksqldb_maps_comma_separated_from_tables() -> None:
    report = KafkaSourceReport()
    client = _FakeKsqlClient(
        {
            "SHOW QUERIES;": [
                {
                    "@type": "queries",
                    "queries": [
                        {
                            "id": "CTAS_JOINED",
                            "queryType": "PERSISTENT",
                            "queryString": "CREATE TABLE JOINED AS SELECT * FROM ORDERS, USERS;",
                            "sinkKafkaTopics": ["joined_topic"],
                        }
                    ],
                }
            ],
            "LIST STREAMS;": [
                {
                    "@type": "streams",
                    "streams": [
                        {"name": "ORDERS", "topic": "orders_topic"},
                        {"name": "USERS", "topic": "users_topic"},
                    ],
                }
            ],
            "LIST TABLES;": [{"@type": "tables", "tables": []}],
        }
    )

    jobs = KsqlDBLineageExtractor(client, report).extract()  # type: ignore[arg-type]

    assert len(jobs) == 1
    assert set(jobs[0].input_topics) == {"orders_topic", "users_topic"}


def test_flink_filters_compute_pool_and_malformed_statements() -> None:
    report = KafkaSourceReport()
    client = _FakeFlinkClient(
        [
            {
                "name": "other-pool",
                "spec": {
                    "statement": "INSERT INTO sink SELECT id FROM src",
                    "compute_pool_id": "pool-2",
                },
            },
            {"name": "no-spec"},
            {"spec": {"statement": "INSERT INTO sink SELECT id FROM src"}},
            {
                "name": "ok",
                "spec": {
                    "statement": "INSERT INTO sink SELECT id FROM src",
                    "compute_pool_id": "pool-1",
                },
            },
        ]
    )

    jobs = FlinkLineageExtractor(
        client, report, compute_pool_id="pool-1"
    ).extract()  # type: ignore[arg-type]

    assert [job.job_id for job in jobs] == ["ok"]


def test_flink_extracts_comma_from_and_quoted_dot_topic() -> None:
    report = KafkaSourceReport()
    client = _FakeFlinkClient(
        [
            {
                "name": "quoted-dot",
                "spec": {
                    "statement": "INSERT INTO `customer.events` "
                    "SELECT id FROM t1, t2",
                },
            }
        ]
    )

    jobs = FlinkLineageExtractor(client, report).extract()  # type: ignore[arg-type]

    assert len(jobs) == 1
    assert jobs[0].output_topics == ["customer.events"]
    assert jobs[0].input_topics == ["t1", "t2"]


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
        "data": [
            {
                "name": "s1",
                "spec": {"statement": "INSERT INTO sink SELECT id FROM src"},
            }
        ],
        "metadata": {"next": "https://flink.example/sql?page=2"},
    }
    page2 = MagicMock()
    page2.json.return_value = {
        "data": [
            {
                "name": "s2",
                "spec": {"statement": "INSERT INTO sink2 SELECT id FROM src2"},
            }
        ]
    }
    session.get.side_effect = [page1, page2]
    client = _flink_client_with_session(session)

    jobs = FlinkLineageExtractor(client, KafkaSourceReport()).extract()

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


def test_ksqldb_close_only_closes_owned_session() -> None:
    injected = MagicMock()
    injected.headers = {}
    client = KsqlDbClient(
        endpoint="https://ksql.example",
        credentials=None,
        timeout_seconds=1,
        report=KafkaSourceReport(),
        session=injected,
    )
    client.close()
    injected.close.assert_not_called()
