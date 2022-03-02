import json
import logging
from dataclasses import dataclass, field
from hashlib import md5
from typing import Dict, Iterable, List, Optional, Tuple, cast

import confluent_kafka
from confluent_kafka.schema_registry.schema_registry_client import Schema

from datahub.configuration.common import AllowDenyPattern, ConfigurationError
from datahub.configuration.kafka import KafkaConsumerConnectionConfig
from datahub.configuration.source_common import DatasetSourceConfigBase
from datahub.emitter.mce_builder import (
    DEFAULT_ENV,
    make_data_platform_urn,
    make_dataplatform_instance_urn,
    make_dataset_urn_with_platform_instance,
    make_domain_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import add_domain_to_entity_wu
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.extractor import schema_util
from datahub.ingestion.source.state.checkpoint import Checkpoint
from datahub.ingestion.source.state.kafka_state import KafkaCheckpointState
from datahub.ingestion.source.state.stateful_ingestion_base import (
    JobId,
    StatefulIngestionConfig,
    StatefulIngestionConfigBase,
    StatefulIngestionSourceBase,
)
from datahub.metadata.com.linkedin.pegasus2avro.common import Status
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import DatasetSnapshot
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    KafkaSchema,
    SchemaField,
    SchemaMetadata,
)
from datahub.metadata.schema_classes import (
    BrowsePathsClass,
    ChangeTypeClass,
    DataPlatformInstanceClass,
    JobStatusClass,
)

logger = logging.getLogger(__name__)


class KafkaSourceStatefulIngestionConfig(StatefulIngestionConfig):
    """
    Specialization of the basic StatefulIngestionConfig to add custom config.
    This will be used to override the stateful_ingestion config param of StatefulIngestionConfigBase
    in the KafkaSourceConfig.
    """

    remove_stale_metadata: bool = True


class KafkaSourceConfig(StatefulIngestionConfigBase, DatasetSourceConfigBase):
    env: str = DEFAULT_ENV
    # TODO: inline the connection config
    connection: KafkaConsumerConnectionConfig = KafkaConsumerConnectionConfig()
    topic_patterns: AllowDenyPattern = AllowDenyPattern(allow=[".*"], deny=["^_.*"])
    domain: Dict[str, AllowDenyPattern] = dict()
    topic_subject_map: Dict[str, str] = dict()
    # Custom Stateful Ingestion settings
    stateful_ingestion: Optional[KafkaSourceStatefulIngestionConfig] = None


@dataclass
class KafkaSourceReport(SourceReport):
    topics_scanned: int = 0
    filtered: List[str] = field(default_factory=list)
    soft_deleted_stale_entities: List[str] = field(default_factory=list)

    def report_topic_scanned(self, topic: str) -> None:
        self.topics_scanned += 1

    def report_dropped(self, topic: str) -> None:
        self.filtered.append(topic)

    def report_stale_entity_soft_deleted(self, urn: str) -> None:
        self.soft_deleted_stale_entities.append(urn)


@dataclass
class KafkaSource(StatefulIngestionSourceBase):
    source_config: KafkaSourceConfig
    consumer: confluent_kafka.Consumer
    report: KafkaSourceReport
    platform: str = "kafka"

    def __init__(self, config: KafkaSourceConfig, ctx: PipelineContext):
        super().__init__(config, ctx)
        self.source_config = config
        if (
            self.is_stateful_ingestion_configured()
            and not self.source_config.platform_instance
        ):
            raise ConfigurationError(
                "Enabling kafka stateful ingestion requires to specify a platform instance."
            )

        self.consumer = confluent_kafka.Consumer(
            {
                "group.id": "test",
                "bootstrap.servers": self.source_config.connection.bootstrap,
                **self.source_config.connection.consumer_config,
            }
        )
        # Use the fully qualified name for SchemaRegistryClient to make it mock patchable for testing.
        self.schema_registry_client = (
            confluent_kafka.schema_registry.schema_registry_client.SchemaRegistryClient(
                {
                    "url": self.source_config.connection.schema_registry_url,
                    **self.source_config.connection.schema_registry_config,
                }
            )
        )
        self.report = KafkaSourceReport()
        self.known_schema_registry_subjects: List[str] = []
        try:
            self.known_schema_registry_subjects.extend(
                self.schema_registry_client.get_subjects()
            )
        except Exception as e:
            logger.warning(f"Failed to get subjects from schema registry: {e}")

    def _get_subject_for_topic(self, topic: str, is_key_schema: bool) -> Optional[str]:
        subject_key_suffix: str = "-key" if is_key_schema else "-value"
        # For details on schema registry subject name strategy,
        # see: https://docs.confluent.io/platform/current/schema-registry/serdes-develop/index.html#how-the-naming-strategies-work

        # User-provided subject for the topic overrides the rest, regardless of the subject name strategy.
        # However, it is a must when the RecordNameStrategy is used as the schema registry subject name strategy.
        # The subject name format for RecordNameStrategy is: <fully-qualified record name>-<key/value> (cannot be inferred from topic name).
        subject_key = topic + subject_key_suffix
        if subject_key in self.source_config.topic_subject_map:
            return self.source_config.topic_subject_map[subject_key]

        # Subject name format when the schema registry subject name strategy is
        #  (a) TopicNameStrategy(default strategy): <topic name>-<key/value>
        #  (b) TopicRecordNameStrategy: <topic name>-<fully-qualified record name>-<key/value>
        for subject in self.known_schema_registry_subjects:
            if subject.startswith(topic) and subject.endswith(subject_key_suffix):
                return subject
        return None

    def is_checkpointing_enabled(self, job_id: JobId) -> bool:
        if (
            job_id == self.get_default_ingestion_job_id()
            and self.is_stateful_ingestion_configured()
            and self.source_config.stateful_ingestion
            and self.source_config.stateful_ingestion.remove_stale_metadata
        ):
            return True

        return False

    def get_default_ingestion_job_id(self) -> JobId:
        """
        Default ingestion job name that kafka provides.
        """
        return JobId("ingest_from_kafka_source")

    def create_checkpoint(self, job_id: JobId) -> Optional[Checkpoint]:
        """
        Create a custom checkpoint with empty state for the job.
        """
        assert self.ctx.pipeline_name is not None
        if job_id == self.get_default_ingestion_job_id():
            return Checkpoint(
                job_name=job_id,
                pipeline_name=self.ctx.pipeline_name,
                platform_instance_id=self.get_platform_instance_id(),
                run_id=self.ctx.run_id,
                config=self.source_config,
                state=KafkaCheckpointState(),
            )
        return None

    def get_platform_instance_id(self) -> str:
        assert self.source_config.platform_instance is not None
        return self.source_config.platform_instance

    @classmethod
    def create(cls, config_dict, ctx):
        config = KafkaSourceConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def gen_removed_entity_workunits(self) -> Iterable[MetadataWorkUnit]:
        last_checkpoint = self.get_last_checkpoint(
            self.get_default_ingestion_job_id(), KafkaCheckpointState
        )
        cur_checkpoint = self.get_current_checkpoint(
            self.get_default_ingestion_job_id()
        )
        if (
            self.source_config.stateful_ingestion
            and self.source_config.stateful_ingestion.remove_stale_metadata
            and last_checkpoint is not None
            and last_checkpoint.state is not None
            and cur_checkpoint is not None
            and cur_checkpoint.state is not None
        ):
            logger.debug("Checking for stale entity removal.")

            def soft_delete_dataset(urn: str, type: str) -> Iterable[MetadataWorkUnit]:
                logger.info(f"Soft-deleting stale entity of type {type} - {urn}.")
                mcp = MetadataChangeProposalWrapper(
                    entityType="dataset",
                    entityUrn=urn,
                    changeType=ChangeTypeClass.UPSERT,
                    aspectName="status",
                    aspect=Status(removed=True),
                )
                wu = MetadataWorkUnit(id=f"soft-delete-{type}-{urn}", mcp=mcp)
                self.report.report_workunit(wu)
                self.report.report_stale_entity_soft_deleted(urn)
                yield wu

            last_checkpoint_state = cast(KafkaCheckpointState, last_checkpoint.state)
            cur_checkpoint_state = cast(KafkaCheckpointState, cur_checkpoint.state)

            for topic_urn in last_checkpoint_state.get_topic_urns_not_in(
                cur_checkpoint_state
            ):
                yield from soft_delete_dataset(topic_urn, "topic")

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        topics = self.consumer.list_topics().topics
        for t in topics:
            self.report.report_topic_scanned(t)
            if self.source_config.topic_patterns.allowed(t):
                yield from self._extract_record(t)
                # add topic to checkpoint if stateful ingestion is enabled
                if self.is_stateful_ingestion_configured():
                    self._add_topic_to_checkpoint(t)
            else:
                self.report.report_dropped(t)
        if self.is_stateful_ingestion_configured():
            # Clean up stale entities.
            yield from self.gen_removed_entity_workunits()

    @staticmethod
    def _compact_schema(schema_str: str) -> str:
        # Eliminate all white-spaces for a compact representation.
        return json.dumps(json.loads(schema_str), separators=(",", ":"))

    def get_schema_str_replace_confluent_ref_avro(
        self, schema: Schema, schema_seen: Optional[set] = None
    ) -> str:
        if not schema.references:
            return self._compact_schema(schema.schema_str)

        if schema_seen is None:
            schema_seen = set()
        schema_str = self._compact_schema(schema.schema_str)
        for schema_ref in schema.references:
            ref_subject = schema_ref["subject"]
            if ref_subject in schema_seen:
                continue

            if ref_subject not in self.known_schema_registry_subjects:
                logger.warning(
                    f"{ref_subject} is not present in the list of registered subjects with schema registry!"
                )

            reference_schema = self.schema_registry_client.get_latest_version(
                subject_name=ref_subject
            )
            schema_seen.add(ref_subject)
            logger.debug(
                f"ref for {ref_subject} is {reference_schema.schema.schema_str}"
            )
            # Replace only external type references with the reference schema recursively.
            # NOTE: The type pattern is dependent on _compact_schema.
            avro_type_kwd = '"type"'
            ref_name = schema_ref["name"]
            # Try by name first
            pattern_to_replace = f'{avro_type_kwd}:"{ref_name}"'
            if pattern_to_replace not in schema_str:
                # Try by subject
                pattern_to_replace = f'{avro_type_kwd}:"{ref_subject}"'
                if pattern_to_replace not in schema_str:
                    logger.warning(
                        f"Not match for external schema type: {{name:{ref_name}, subject:{ref_subject}}} in schema:{schema_str}"
                    )
                else:
                    logger.debug(
                        f"External schema matches by subject, {pattern_to_replace}"
                    )
            else:
                logger.debug(f"External schema matches by name, {pattern_to_replace}")
            schema_str = schema_str.replace(
                pattern_to_replace,
                f"{avro_type_kwd}:{self.get_schema_str_replace_confluent_ref_avro(reference_schema.schema, schema_seen)}",
            )
        return schema_str

    def _add_topic_to_checkpoint(self, topic: str) -> None:
        cur_checkpoint = self.get_current_checkpoint(
            self.get_default_ingestion_job_id()
        )
        if cur_checkpoint is not None:
            checkpoint_state = cast(KafkaCheckpointState, cur_checkpoint.state)
            checkpoint_state.add_topic_urn(
                make_dataset_urn_with_platform_instance(
                    platform=self.platform,
                    name=topic,
                    platform_instance=self.source_config.platform_instance,
                    env=self.source_config.env,
                )
            )

    def _get_schema_and_fields(
        self, topic: str, is_key_schema: bool
    ) -> Tuple[Optional[Schema], List[SchemaField]]:
        schema: Optional[Schema] = None
        schema_type_str: str = "key" if is_key_schema else "value"
        topic_subject: Optional[str] = self._get_subject_for_topic(
            topic=topic, is_key_schema=is_key_schema
        )
        if topic_subject is not None:
            logger.debug(
                f"The {schema_type_str} schema subject:'{topic_subject}' is found for topic:'{topic}'."
            )
            try:
                registered_schema = self.schema_registry_client.get_latest_version(
                    subject_name=topic_subject
                )
                schema = registered_schema.schema
            except Exception as e:
                logger.warning(
                    f"For topic: {topic}, failed to get {schema_type_str} schema from schema registry using subject:'{topic_subject}': {e}."
                )
                self.report.report_warning(
                    topic,
                    f"failed to get {schema_type_str} schema from schema registry using subject:'{topic_subject}': {e}.",
                )
        else:
            logger.debug(
                f"For topic: {topic}, the schema registry subject for the {schema_type_str} schema is not found."
            )
            if not is_key_schema:
                # Value schema is always expected. Report a warning.
                self.report.report_warning(
                    topic,
                    f"The schema registry subject for the {schema_type_str} schema is not found."
                    f" The topic is either schema-less, or no messages have been written to the topic yet.",
                )

        # Obtain the schema fields from schema for the topic.
        fields: List[SchemaField] = []
        if schema is not None:
            fields = self._get_schema_fields(
                topic=topic, schema=schema, is_key_schema=is_key_schema
            )
        return (schema, fields)

    def _get_schema_fields(
        self, topic: str, schema: Schema, is_key_schema: bool
    ) -> List[SchemaField]:
        # Parse the schema and convert it to SchemaFields.
        fields: List[SchemaField] = []
        if schema.schema_type == "AVRO":
            cleaned_str = self.get_schema_str_replace_confluent_ref_avro(schema)
            # "value.id" or "value.[type=string]id"
            fields = schema_util.avro_schema_to_mce_fields(
                cleaned_str, is_key_schema=is_key_schema
            )
        else:
            self.report.report_warning(
                topic,
                f"Parsing kafka schema type {schema.schema_type} is currently not implemented",
            )
        return fields

    def _get_schema_metadata(
        self, topic: str, platform_urn: str
    ) -> Optional[SchemaMetadata]:
        # Process the value schema
        schema, fields = self._get_schema_and_fields(
            topic=topic, is_key_schema=False
        )  # type: Tuple[Optional[Schema], List[SchemaField]]

        # Process the key schema
        key_schema, key_fields = self._get_schema_and_fields(
            topic=topic, is_key_schema=True
        )  # type:Tuple[Optional[Schema], List[SchemaField]]

        # Create the schemaMetadata aspect.
        if schema is not None or key_schema is not None:
            # create a merged string for the combined schemas and compute an md5 hash across
            schema_as_string = (schema.schema_str if schema is not None else "") + (
                key_schema.schema_str if key_schema is not None else ""
            )
            md5_hash = md5(schema_as_string.encode()).hexdigest()

            return SchemaMetadata(
                schemaName=topic,
                version=0,
                hash=md5_hash,
                platform=platform_urn,
                platformSchema=KafkaSchema(
                    documentSchema=schema.schema_str if schema is not None else "",
                    keySchema=key_schema.schema_str if key_schema else None,
                ),
                fields=key_fields + fields,
            )
        return None

    def _extract_record(self, topic: str) -> Iterable[MetadataWorkUnit]:  # noqa: C901
        logger.debug(f"topic = {topic}")

        # 1. Create the default dataset snapshot for the topic.
        dataset_name = topic
        platform_urn = make_data_platform_urn(self.platform)
        dataset_urn = make_dataset_urn_with_platform_instance(
            platform=self.platform,
            name=dataset_name,
            platform_instance=self.source_config.platform_instance,
            env=self.source_config.env,
        )
        dataset_snapshot = DatasetSnapshot(
            urn=dataset_urn,
            aspects=[Status(removed=False)],  # we append to this list later on
        )

        # 2. Attach schemaMetadata aspect
        schema_metadata = self._get_schema_metadata(topic, platform_urn)
        if schema_metadata is not None:
            dataset_snapshot.aspects.append(schema_metadata)

        # 3. Attach browsePaths aspect
        browse_path_suffix = (
            f"{self.source_config.platform_instance}/{topic}"
            if self.source_config.platform_instance
            else topic
        )
        browse_path = BrowsePathsClass(
            [f"/{self.source_config.env.lower()}/{self.platform}/{browse_path_suffix}"]
        )
        dataset_snapshot.aspects.append(browse_path)

        # 4. Attach dataPlatformInstance aspect.
        if self.source_config.platform_instance:
            dataset_snapshot.aspects.append(
                DataPlatformInstanceClass(
                    platform=platform_urn,
                    instance=make_dataplatform_instance_urn(
                        self.platform, self.source_config.platform_instance
                    ),
                )
            )

        # 4. Emit the datasetSnapshot MCE
        mce = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
        wu = MetadataWorkUnit(id=f"kafka-{topic}", mce=mce)
        self.report.report_workunit(wu)
        yield wu

        domain_urn: Optional[str] = None

        # 5. Emit domains aspect MCPW
        for domain, pattern in self.source_config.domain.items():
            if pattern.allowed(dataset_name):
                domain_urn = make_domain_urn(domain)

        if domain_urn:
            wus = add_domain_to_entity_wu(
                entity_type="dataset",
                entity_urn=dataset_urn,
                domain_urn=domain_urn,
            )
            for wu in wus:
                self.report.report_workunit(wu)
                yield wu

    def get_report(self):
        return self.report

    def update_default_job_run_summary(self) -> None:
        summary = self.get_job_run_summary(self.get_default_ingestion_job_id())
        if summary is not None:
            # For now just add the config and the report.
            summary.config = self.source_config.json()
            summary.custom_summary = self.report.as_string()
            summary.runStatus = (
                JobStatusClass.FAILED
                if self.get_report().failures
                else JobStatusClass.COMPLETED
            )

    def close(self):
        self.update_default_job_run_summary()
        self.prepare_for_commit()
        if self.consumer:
            self.consumer.close()
