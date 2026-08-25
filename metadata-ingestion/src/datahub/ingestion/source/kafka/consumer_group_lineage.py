import logging
from typing import Callable, Dict, Final, List, Optional, Set, Tuple

from confluent_kafka import ConsumerGroupTopicPartitions
from confluent_kafka.admin import AdminClient
from pydantic import BaseModel

from datahub.emitter.mce_builder import (
    make_data_flow_urn,
    make_data_job_urn_with_flow,
    make_dataset_urn_with_platform_instance,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.kafka.kafka_config import KafkaConsumerGroupLineageConfig
from datahub.ingestion.source.kafka.kafka_report import KafkaSourceReport
from datahub.metadata.schema_classes import (
    DataFlowInfoClass,
    DataJobInfoClass,
    DataJobInputOutputClass,
)

logger = logging.getLogger(__name__)

# All consumer-group DataJobs hang off a single synthetic flow so they group together
# in the UI instead of scattering one flow per group.
CONSUMER_GROUP_FLOW_ID: Final[str] = "consumer_groups"
CONSUMER_GROUP_FLOW_NAME: Final[str] = "Kafka Consumer Groups"
CONSUMER_GROUP_FLOW_DESCRIPTION: Final[str] = (
    "Consumer groups reading from this Kafka cluster, discovered via the Kafka Admin API."
)
# DataJobInfo.type mirrors the value the kafka-connect source uses for its jobs.
CONSUMER_GROUP_JOB_TYPE: Final[str] = "COMMAND"


class ConsumerGroupMember(BaseModel):
    client_id: Optional[str] = None
    host: Optional[str] = None


class ConsumerGroupInfo(BaseModel):
    group_id: str
    state: Optional[str] = None
    # Topics the group consumes: the union of currently-assigned partitions (active
    # members) and topics with committed offsets (covers idle groups within retention).
    topics: List[str]
    members: List[ConsumerGroupMember]


class ConsumerGroupLineageExtractor:
    def __init__(
        self,
        admin_client: AdminClient,
        config: KafkaConsumerGroupLineageConfig,
        report: KafkaSourceReport,
        timeout_seconds: float,
    ) -> None:
        self.admin_client = admin_client
        self.config = config
        self.report = report
        self.timeout_seconds = timeout_seconds
        self._group_state: Dict[str, str] = {}

    def extract(self) -> List[ConsumerGroupInfo]:
        group_ids = self._list_group_ids()
        allowed = [
            group_id
            for group_id in group_ids
            if self.config.consumer_group_patterns.allowed(group_id)
        ]
        self.report.consumer_groups_scanned += len(allowed)
        if not allowed:
            return []

        described = self._describe_groups(allowed)
        committed = self._committed_topics(allowed)

        groups: List[ConsumerGroupInfo] = []
        for group_id in allowed:
            active_topics, members = described.get(group_id, (set(), []))
            topics = sorted(active_topics | committed.get(group_id, set()))
            if not topics:
                continue
            state = self._group_state.get(group_id)
            groups.append(
                ConsumerGroupInfo(
                    group_id=group_id,
                    state=state,
                    topics=topics,
                    members=members,
                )
            )
        return groups

    def _list_group_ids(self) -> List[str]:
        try:
            result = self.admin_client.list_consumer_groups().result(
                timeout=self.timeout_seconds
            )
        except Exception as e:
            self.report.warning(
                message="Failed to list Kafka consumer groups; consumer-group lineage will be skipped",
                context="consumer-group-lineage",
                exc=e,
                log=False,
            )
            return []

        group_ids: List[str] = []
        for listing in result.valid:
            group_ids.append(listing.group_id)
            state = getattr(listing, "state", None)
            if state is not None:
                self._group_state[listing.group_id] = state.name
        for error in result.errors:
            self.report.warning(
                message="Error while listing a Kafka consumer group",
                context=str(error),
                log=False,
            )
        return group_ids

    def _describe_groups(
        self, group_ids: List[str]
    ) -> Dict[str, Tuple[Set[str], List[ConsumerGroupMember]]]:
        results: Dict[str, Tuple[Set[str], List[ConsumerGroupMember]]] = {}
        try:
            futures = self.admin_client.describe_consumer_groups(group_ids)
        except Exception as e:
            self.report.warning(
                message="Failed to describe Kafka consumer groups; active member assignments "
                "will be missing from consumer-group lineage",
                context="consumer-group-lineage",
                exc=e,
                log=False,
            )
            return results

        for group_id, future in futures.items():
            try:
                description = future.result(timeout=self.timeout_seconds)
            except Exception as e:
                self.report.warning(
                    message="Failed to describe a Kafka consumer group",
                    context=group_id,
                    exc=e,
                    log=False,
                )
                continue

            topics: Set[str] = set()
            members: List[ConsumerGroupMember] = []
            for member in description.members:
                members.append(
                    ConsumerGroupMember(
                        client_id=getattr(member, "client_id", None),
                        host=getattr(member, "host", None),
                    )
                )
                assignment = getattr(member, "assignment", None)
                for topic_partition in (
                    getattr(assignment, "topic_partitions", []) or []
                ):
                    if topic_partition.topic:
                        topics.add(topic_partition.topic)
            state = getattr(description, "state", None)
            if state is not None:
                self._group_state[group_id] = state.name
            results[group_id] = (topics, members)
        return results

    def _committed_topics(self, group_ids: List[str]) -> Dict[str, Set[str]]:
        results: Dict[str, Set[str]] = {}
        try:
            futures = self.admin_client.list_consumer_group_offsets(
                [ConsumerGroupTopicPartitions(group_id) for group_id in group_ids]
            )
        except Exception as e:
            self.report.warning(
                message="Failed to list committed offsets for Kafka consumer groups; idle "
                "groups may be missing from consumer-group lineage",
                context="consumer-group-lineage",
                exc=e,
                log=False,
            )
            return results

        for group_id, future in futures.items():
            try:
                group_offsets = future.result(timeout=self.timeout_seconds)
            except Exception as e:
                self.report.warning(
                    message="Failed to list committed offsets for a Kafka consumer group",
                    context=group_id,
                    exc=e,
                    log=False,
                )
                continue
            topics = {
                topic_partition.topic
                for topic_partition in group_offsets.topic_partitions or []
                if topic_partition.topic
            }
            if topics:
                results[group_id] = topics
        return results


def build_consumer_group_lineage_workunits(
    groups: List[ConsumerGroupInfo],
    platform: str,
    platform_instance: Optional[str],
    env: str,
    report: KafkaSourceReport,
    topic_allowed: Callable[[str], bool],
) -> List[MetadataWorkUnit]:
    workunits: List[MetadataWorkUnit] = []
    if not groups:
        return workunits

    flow_urn = make_data_flow_urn(
        orchestrator=platform,
        flow_id=CONSUMER_GROUP_FLOW_ID,
        cluster=env,
        platform_instance=platform_instance,
    )
    flow_emitted = False

    for group in groups:
        input_datasets = [
            make_dataset_urn_with_platform_instance(
                platform=platform,
                name=topic,
                platform_instance=platform_instance,
                env=env,
            )
            for topic in group.topics
            if topic_allowed(topic)
        ]
        if not input_datasets:
            continue

        if not flow_emitted:
            workunits.append(
                MetadataChangeProposalWrapper(
                    entityUrn=flow_urn,
                    aspect=DataFlowInfoClass(
                        name=CONSUMER_GROUP_FLOW_NAME,
                        description=CONSUMER_GROUP_FLOW_DESCRIPTION,
                    ),
                ).as_workunit()
            )
            flow_emitted = True

        job_urn = make_data_job_urn_with_flow(flow_urn, group.group_id)
        workunits.append(
            MetadataChangeProposalWrapper(
                entityUrn=job_urn,
                aspect=DataJobInfoClass(
                    name=group.group_id,
                    type=CONSUMER_GROUP_JOB_TYPE,
                    customProperties=_group_custom_properties(group),
                ),
            ).as_workunit()
        )
        workunits.append(
            MetadataChangeProposalWrapper(
                entityUrn=job_urn,
                aspect=DataJobInputOutputClass(
                    inputDatasets=input_datasets,
                    outputDatasets=[],
                ),
            ).as_workunit()
        )
        report.consumer_groups_with_lineage += 1
        report.consumer_group_lineage_edges += len(input_datasets)

    return workunits


def _group_custom_properties(group: ConsumerGroupInfo) -> Dict[str, str]:
    properties: Dict[str, str] = {}
    if group.state:
        properties["state"] = group.state
    # The client ids and hosts are the actual consuming applications (Java, Python, etc.)
    # behind the group — surface them so operators can see who reads a topic.
    client_ids = sorted({m.client_id for m in group.members if m.client_id})
    if client_ids:
        properties["client_ids"] = ", ".join(client_ids)
    hosts = sorted({m.host for m in group.members if m.host})
    if hosts:
        properties["hosts"] = ", ".join(hosts)
    return properties
