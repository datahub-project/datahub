from types import SimpleNamespace
from typing import Dict, List, Optional

from datahub.emitter.mce_builder import (
    make_data_flow_urn,
    make_data_job_urn_with_flow,
    make_dataset_urn_with_platform_instance,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.source.kafka.consumer_group_lineage import (
    CONSUMER_GROUP_FLOW_ID,
    ConsumerGroupInfo,
    ConsumerGroupLineageExtractor,
    ConsumerGroupMember,
    build_consumer_group_lineage_workunits,
)
from datahub.ingestion.source.kafka.kafka_config import KafkaConsumerGroupLineageConfig
from datahub.ingestion.source.kafka.kafka_report import KafkaSourceReport
from datahub.metadata.schema_classes import (
    DataFlowInfoClass,
    DataJobInfoClass,
    DataJobInputOutputClass,
)

PLATFORM = "kafka"
ENV = "PROD"


class _FakeFuture:
    def __init__(
        self, value: object = None, error: Optional[BaseException] = None
    ) -> None:
        self._value = value
        self._error = error

    def result(self, timeout: float = 0) -> object:
        if self._error is not None:
            raise self._error
        return self._value


def _topic_partition(topic: str) -> SimpleNamespace:
    return SimpleNamespace(topic=topic, partition=0, offset=0)


def _member(client_id: str, host: str, topics: List[str]) -> SimpleNamespace:
    return SimpleNamespace(
        client_id=client_id,
        host=host,
        assignment=SimpleNamespace(
            topic_partitions=[_topic_partition(t) for t in topics]
        ),
    )


class _FakeAdminClient:
    def __init__(
        self,
        listings: List[SimpleNamespace],
        described: Dict[str, SimpleNamespace],
        committed: Dict[str, List[str]],
        list_error: Optional[BaseException] = None,
    ) -> None:
        self._listings = listings
        self._described = described
        self._committed = committed
        self._list_error = list_error

    def list_consumer_groups(self, **kwargs: object) -> _FakeFuture:
        if self._list_error is not None:
            return _FakeFuture(error=self._list_error)
        return _FakeFuture(SimpleNamespace(valid=self._listings, errors=[]))

    def describe_consumer_groups(
        self, group_ids: List[str], **kwargs: object
    ) -> Dict[str, _FakeFuture]:
        return {
            group_id: _FakeFuture(self._described[group_id])
            for group_id in group_ids
            if group_id in self._described
        }

    def list_consumer_group_offsets(
        self, request: List[object], **kwargs: object
    ) -> Dict[str, _FakeFuture]:
        futures: Dict[str, _FakeFuture] = {}
        for item in request:
            group_id = item.group_id  # type: ignore[attr-defined]
            topics = self._committed.get(group_id, [])
            futures[group_id] = _FakeFuture(
                SimpleNamespace(topic_partitions=[_topic_partition(t) for t in topics])
            )
        return futures


def _stable(group_id: str) -> SimpleNamespace:
    return SimpleNamespace(group_id=group_id, state=SimpleNamespace(name="STABLE"))


def test_extract_unions_active_and_committed_topics_and_filters_internal_groups() -> (
    None
):
    admin = _FakeAdminClient(
        listings=[
            _stable("app-consumer"),
            _stable("idle-consumer"),
            _stable("_confluent-monitoring"),
        ],
        described={
            "app-consumer": SimpleNamespace(
                members=[_member("client-a", "10.0.0.1", ["orders"])],
                state=SimpleNamespace(name="STABLE"),
            ),
            "idle-consumer": SimpleNamespace(
                members=[], state=SimpleNamespace(name="EMPTY")
            ),
        },
        committed={
            "app-consumer": ["orders", "payments"],
            "idle-consumer": ["shipments"],
        },
    )
    report = KafkaSourceReport()
    extractor = ConsumerGroupLineageExtractor(
        admin_client=admin,  # type: ignore[arg-type]
        config=KafkaConsumerGroupLineageConfig(enabled=True),
        report=report,
        timeout_seconds=10,
    )

    groups = {group.group_id: group for group in extractor.extract()}

    # Internal group excluded by the default deny pattern; scanned count reflects the filter.
    assert set(groups) == {"app-consumer", "idle-consumer"}
    assert report.consumer_groups_scanned == 2

    # Active assignment (orders) unions with committed offsets (orders, payments).
    assert groups["app-consumer"].topics == ["orders", "payments"]
    assert groups["app-consumer"].members == [
        ConsumerGroupMember(client_id="client-a", host="10.0.0.1")
    ]
    # Idle group has no active members but committed offsets still yield its topic.
    assert groups["idle-consumer"].topics == ["shipments"]
    assert groups["idle-consumer"].members == []


def test_list_failure_is_reported_and_yields_no_groups() -> None:
    admin = _FakeAdminClient(
        listings=[], described={}, committed={}, list_error=RuntimeError("boom")
    )
    report = KafkaSourceReport()
    extractor = ConsumerGroupLineageExtractor(
        admin_client=admin,  # type: ignore[arg-type]
        config=KafkaConsumerGroupLineageConfig(enabled=True),
        report=report,
        timeout_seconds=10,
    )

    assert extractor.extract() == []
    assert len(report.warnings) >= 1


def test_build_workunits_emits_flow_and_filters_denied_topics() -> None:
    groups = [
        ConsumerGroupInfo(
            group_id="app-consumer",
            state="STABLE",
            topics=["orders", "internal_topic"],
            members=[ConsumerGroupMember(client_id="client-a", host="10.0.0.1")],
        ),
        ConsumerGroupInfo(
            group_id="only-denied",
            state="EMPTY",
            topics=["internal_topic"],
            members=[],
        ),
    ]
    report = KafkaSourceReport()

    workunits = build_consumer_group_lineage_workunits(
        groups=groups,
        platform=PLATFORM,
        platform_instance=None,
        env=ENV,
        report=report,
        topic_allowed=lambda topic: topic != "internal_topic",
    )

    flow_urn = make_data_flow_urn(PLATFORM, CONSUMER_GROUP_FLOW_ID, ENV)
    job_urn = make_data_job_urn_with_flow(flow_urn, "app-consumer")
    orders_urn = make_dataset_urn_with_platform_instance(
        platform=PLATFORM, name="orders", platform_instance=None, env=ENV
    )

    aspects_by_urn: Dict[str, List[object]] = {}
    for wu in workunits:
        mcp = wu.metadata
        assert isinstance(mcp, MetadataChangeProposalWrapper)
        assert mcp.entityUrn is not None
        aspects_by_urn.setdefault(mcp.entityUrn, []).append(mcp.aspect)

    # A single shared flow, and only the group with an allowed topic becomes a job.
    flow_aspects = aspects_by_urn[flow_urn]
    assert len(flow_aspects) == 1
    assert isinstance(flow_aspects[0], DataFlowInfoClass)
    assert make_data_job_urn_with_flow(flow_urn, "only-denied") not in aspects_by_urn

    job_aspects = aspects_by_urn[job_urn]
    info = next(a for a in job_aspects if isinstance(a, DataJobInfoClass))
    io = next(a for a in job_aspects if isinstance(a, DataJobInputOutputClass))
    assert info.customProperties["client_ids"] == "client-a"
    assert info.customProperties["state"] == "STABLE"
    assert io.inputDatasets == [orders_urn]

    assert report.consumer_groups_with_lineage == 1
    assert report.consumer_group_lineage_edges == 1
