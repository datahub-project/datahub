from typing import Dict, List

from datahub.emitter.mce_builder import (
    make_data_flow_urn,
    make_data_job_urn_with_flow,
    make_dataset_urn_with_platform_instance,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.source.kafka.consumer_group_lineage import (
    ConsumerGroupInfo,
    ConsumerGroupMember,
)
from datahub.ingestion.source.kafka.kafka_config import KafkaStreamsLineageConfig
from datahub.ingestion.source.kafka.kafka_report import KafkaSourceReport
from datahub.ingestion.source.kafka.stream_processing.builder import (
    build_stream_processing_workunits,
)
from datahub.ingestion.source.kafka.stream_processing.constants import (
    ENGINE_FLOW_METADATA,
    StreamProcessingEngine,
)
from datahub.ingestion.source.kafka.stream_processing.flink import FlinkLineageExtractor
from datahub.ingestion.source.kafka.stream_processing.kafka_streams import (
    KafkaStreamsLineageExtractor,
)
from datahub.ingestion.source.kafka.stream_processing.ksqldb import (
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
