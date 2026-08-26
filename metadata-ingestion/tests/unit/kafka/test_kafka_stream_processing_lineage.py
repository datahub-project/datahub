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
from datahub.ingestion.source.kafka.kafka_config import (
    FlinkLineageConfig,
)
from datahub.ingestion.source.kafka.kafka_report import KafkaSourceReport
from datahub.ingestion.source.kafka.stream_processing.builder import (
    build_stream_processing_workunits,
)
from datahub.ingestion.source.kafka.stream_processing.constants import (
    ENGINE_FLOW_METADATA,
    StreamProcessingEngine,
    from_join_identifiers,
    last_identifier_segment,
    strip_sql_noise,
)
from datahub.ingestion.source.kafka.stream_processing.flink import (
    FlinkLineageExtractor,
    FlinkStatementsClient,
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


class _FakeFlinkClient:
    def __init__(self, statements: List[Dict[str, object]]) -> None:
        self._statements = statements

    def list_statements(self) -> List[Dict[str, object]]:
        return self._statements

    def close(self) -> None:
        pass


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

    jobs = FlinkLineageExtractor(client, report, compute_pool_id="pool-1").extract()  # type: ignore[arg-type]

    assert [job.job_id for job in jobs] == ["ok"]


def test_flink_extracts_comma_from_and_quoted_dot_topic() -> None:
    report = KafkaSourceReport()
    client = _FakeFlinkClient(
        [
            {
                "name": "quoted-dot",
                "spec": {
                    "statement": "INSERT INTO `customer.events` SELECT id FROM t1, t2",
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
