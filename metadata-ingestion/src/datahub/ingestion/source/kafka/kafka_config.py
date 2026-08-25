from typing import Dict, Optional

from pydantic import Field, PositiveFloat, PositiveInt, SecretStr, model_validator

from datahub.configuration.common import AllowDenyPattern, ConfigModel
from datahub.configuration.kafka import KafkaConsumerConnectionConfig
from datahub.configuration.source_common import (
    DatasetSourceConfigMixin,
    LowerCaseDatasetUrnConfigMixin,
)
from datahub.ingestion.source.confluent.config import ConfluentStreamCatalogConfig
from datahub.ingestion.source.ge_profiling_config import GEProfilingConfig
from datahub.ingestion.source.kafka.kafka_constants import (
    DEFAULT_BATCH_SIZE,
    DEFAULT_MAX_MESSAGES_PER_TOPIC,
    DEFAULT_MAX_SAMPLE_TIME_SECONDS,
    DEFAULT_SAMPLE_SIZE,
    SCHEMA_REGISTRY_BASIC_AUTH_KEY,
    OffsetResetStrategy,
    SamplingStrategy,
)
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
)
from datahub.ingestion.source_config.operation_config import is_profiling_enabled
from datahub.masking.secret_registry import SecretRegistry, is_masking_enabled

_FLINK_CLOUDS = frozenset({"aws", "gcp", "azure"})


def _normalize_endpoint(endpoint: str, field: str, *, require_https: bool) -> str:
    if require_https and not endpoint.lower().startswith("https://"):
        raise ValueError(
            f"Configuration error: '{field}' must use HTTPS "
            f"to protect credentials in transit. Got: '{endpoint}'."
        )
    return endpoint.rstrip("/")


def _secret_configured(value: Optional[SecretStr]) -> bool:
    return value is not None and bool(value.get_secret_value().strip())


def _require_secret_pair(
    api_key: Optional[SecretStr],
    api_secret: Optional[SecretStr],
    prefix: str,
    *,
    required: bool,
) -> None:
    key_set = _secret_configured(api_key)
    secret_set = _secret_configured(api_secret)
    if required and not (key_set and secret_set):
        raise ValueError(
            f"Configuration error: '{prefix}.api_key' and '{prefix}.api_secret' must both be set."
        )
    if key_set != secret_set:
        raise ValueError(
            f"Configuration error: '{prefix}.api_key' and '{prefix}.api_secret' must be provided together."
        )


class SchemaResolutionFallback(ConfigModel):
    enabled: bool = Field(
        default=False,
        description="Enable comprehensive schema resolution with multiple fallback strategies for topics where schema registry lookup fails.",
    )

    sample_timeout_seconds: PositiveFloat = Field(
        default=2.0,
        description="Maximum time to spend sampling messages from a single topic (in seconds) for record name extraction and schema inference. Must be positive.",
    )
    offset_reset_strategy: OffsetResetStrategy = Field(
        default=OffsetResetStrategy.HYBRID,
        description="Where to start reading when sampling messages for schema inference: 'earliest' (scan from beginning), 'latest' (recent messages only), or 'hybrid' (try latest first, fallback to earliest). Distinct from the profiler's `sampling_strategy`.",
    )
    max_messages_per_topic: PositiveInt = Field(
        default=DEFAULT_MAX_MESSAGES_PER_TOPIC,
        description="Maximum number of messages to sample per topic for record name extraction and schema inference. Must be positive.",
    )


class ProfilerConfig(GEProfilingConfig):
    max_sample_time_seconds: PositiveInt = Field(
        default=DEFAULT_MAX_SAMPLE_TIME_SECONDS,
        description="Maximum time to spend sampling messages in seconds. Must be positive.",
    )
    sampling_strategy: SamplingStrategy = Field(
        default=SamplingStrategy.LATEST,
        description="Strategy for sampling messages: 'latest' (from end of topic), 'random' (random offsets), 'stratified' (evenly distributed), 'full' (entire topic, respects sample_size)",
    )
    batch_size: PositiveInt = Field(
        default=DEFAULT_BATCH_SIZE,
        description="Number of messages to fetch in a single batch (for more efficient reading). Must be positive.",
    )

    sample_size: PositiveInt = Field(
        default=DEFAULT_SAMPLE_SIZE,
        description="Number of messages to sample for profiling. Higher values provide more accurate statistics but take longer to process. Must be positive.",
    )


class KsqlDBLineageConfig(ConfigModel):
    enabled: bool = Field(
        default=False,
        description="Emit topic-to-topic lineage from Confluent Cloud / self-managed ksqlDB "
        "persistent queries. Each CREATE STREAM/TABLE AS SELECT query is modeled as a DataJob "
        "whose input datasets are the source topics and whose output dataset is the sink topic.",
    )
    endpoint: Optional[str] = Field(
        default=None,
        description="ksqlDB server endpoint, e.g. `https://pksqlc-xxxxx.us-east-1.aws.confluent.cloud:443`.",
    )
    api_key: Optional[SecretStr] = Field(
        default=None,
        description="ksqlDB API key (Basic auth). For Confluent Cloud this is a ksqlDB cluster "
        "API key, distinct from the Kafka and Schema Registry keys.",
    )
    api_secret: Optional[SecretStr] = Field(
        default=None, description="ksqlDB API secret (Basic auth)."
    )
    timeout_seconds: PositiveInt = Field(
        default=30, description="Timeout in seconds for each ksqlDB REST request."
    )

    @model_validator(mode="after")
    def validate_ksqldb(self) -> "KsqlDBLineageConfig":
        if not self.enabled:
            return self
        if not self.endpoint:
            raise ValueError(
                "Configuration error: 'stream_processing_lineage.ksqldb.enabled' is true but "
                "'stream_processing_lineage.ksqldb.endpoint' is not set."
            )
        _require_secret_pair(
            self.api_key,
            self.api_secret,
            "stream_processing_lineage.ksqldb",
            required=False,
        )
        has_creds = _secret_configured(self.api_key) or _secret_configured(
            self.api_secret
        )
        self.endpoint = _normalize_endpoint(
            self.endpoint,
            "stream_processing_lineage.ksqldb.endpoint",
            require_https=has_creds,
        )
        return self


class FlinkLineageConfig(ConfigModel):
    enabled: bool = Field(
        default=False,
        description="Emit topic-to-topic lineage from Confluent Cloud Flink SQL statements. Each "
        "`INSERT INTO sink SELECT ... FROM source` statement is modeled as a DataJob mapping the "
        "source topics to the sink topic. Requires a Confluent Cloud (resource-management) API key.",
    )
    organization_id: Optional[str] = Field(
        default=None, description="Confluent Cloud organization id."
    )
    environment_id: Optional[str] = Field(
        default=None, description="Confluent Cloud environment id, e.g. `env-xxxxx`."
    )
    region: Optional[str] = Field(
        default=None,
        description="Confluent Cloud region, e.g. `us-east-1`. Used to build the Flink REST host "
        "`https://flink.<region>.<cloud>.confluent.cloud` unless `endpoint` is set.",
    )
    cloud: Optional[str] = Field(
        default=None, description="Confluent Cloud provider: `aws`, `gcp`, or `azure`."
    )
    endpoint: Optional[str] = Field(
        default=None,
        description="Optional explicit Flink REST base URL, overriding the region/cloud-derived host.",
    )
    compute_pool_id: Optional[str] = Field(
        default=None,
        description="Optional Flink compute pool id to restrict statements to a single pool.",
    )
    api_key: Optional[SecretStr] = Field(
        default=None,
        description="Confluent Cloud resource-management API key (Basic auth against the Flink REST API).",
    )
    api_secret: Optional[SecretStr] = Field(
        default=None, description="Confluent Cloud resource-management API secret."
    )
    timeout_seconds: PositiveInt = Field(
        default=30, description="Timeout in seconds for each Flink REST request."
    )

    @model_validator(mode="after")
    def validate_flink(self) -> "FlinkLineageConfig":
        if not self.enabled:
            return self
        if self.endpoint:
            self.endpoint = _normalize_endpoint(
                self.endpoint,
                "stream_processing_lineage.flink.endpoint",
                require_https=True,
            )
        elif not (self.region and self.cloud):
            raise ValueError(
                "Configuration error: 'stream_processing_lineage.flink.enabled' is true but the "
                "Flink REST host cannot be resolved. Set either 'endpoint' or both 'region' and 'cloud'."
            )
        else:
            cloud = self.cloud.lower()
            if cloud not in _FLINK_CLOUDS:
                raise ValueError(
                    "Configuration error: 'stream_processing_lineage.flink.cloud' must be one of "
                    f"{sorted(_FLINK_CLOUDS)}. Got: '{self.cloud}'."
                )
            self.cloud = cloud
        missing = [
            name
            for name, value in (
                ("organization_id", self.organization_id),
                ("environment_id", self.environment_id),
            )
            if not value
        ]
        if missing:
            raise ValueError(
                "Configuration error: 'stream_processing_lineage.flink.enabled' is true but "
                f"{', '.join(repr(f'stream_processing_lineage.flink.{name}') for name in missing)} "
                f"{'is' if len(missing) == 1 else 'are'} not set."
            )
        _require_secret_pair(
            self.api_key,
            self.api_secret,
            "stream_processing_lineage.flink",
            required=True,
        )
        return self


class KafkaStreamsLineageConfig(ConfigModel):
    enabled: bool = Field(
        default=False,
        description="Best-effort lineage for Kafka Streams applications, discovered via the Kafka "
        "Admin API over the existing broker connection. Detects apps by their internal "
        "changelog/repartition topics and emits input topics plus those internal topics. True "
        "downstream output topics require the app topology, which no broker API exposes.",
    )
    application_patterns: AllowDenyPattern = Field(
        default_factory=lambda: AllowDenyPattern(allow=[".*"], deny=["^_.*"]),
        description="Regex patterns for Kafka Streams application ids (consumer group ids) to include.",
    )


class StreamProcessingLineageConfig(ConfigModel):
    ksqldb: KsqlDBLineageConfig = Field(default_factory=KsqlDBLineageConfig)
    flink: FlinkLineageConfig = Field(default_factory=FlinkLineageConfig)
    kafka_streams: KafkaStreamsLineageConfig = Field(
        default_factory=KafkaStreamsLineageConfig
    )
    include_column_lineage: bool = Field(
        default=True,
        description="Parse the transform SQL (ksqlDB/Flink) to emit best-effort column-level "
        "lineage, resolving topic schemas from DataHub when available.",
    )

    def any_enabled(self) -> bool:
        return self.ksqldb.enabled or self.flink.enabled or self.kafka_streams.enabled


class KafkaConfluentCatalogConfig(ConfluentStreamCatalogConfig):
    cluster_id: Optional[str] = Field(
        default=None,
        description="Kafka cluster id, e.g. `lkc-xxxxx`. Only needed when the environment "
        "behind this Schema Registry holds more than one Kafka cluster, since the catalog "
        "covers the whole environment and topic names can repeat across clusters.",
    )
    include_tags: bool = Field(
        default=True,
        description="Emit Confluent Cloud tags on topics as DataHub tags.",
    )
    include_business_metadata: bool = Field(
        default=True,
        description="Emit Confluent Cloud business metadata attributes on topics as DataHub "
        "custom properties.",
    )
    include_lineage: bool = Field(
        default=False,
        description="Emit topic-to-topic lineage from Stream Catalog replication metadata "
        "(cluster links / mirror topics). Each topic that mirrors another gets an upstream "
        "edge to its source topic. Connector and external-system lineage is handled by the "
        "kafka-connect source, not here.",
    )


class KafkaSourceConfig(
    StatefulIngestionConfigBase,
    DatasetSourceConfigMixin,
    LowerCaseDatasetUrnConfigMixin,
):
    connection: KafkaConsumerConnectionConfig = Field(
        default_factory=KafkaConsumerConnectionConfig
    )

    topic_patterns: AllowDenyPattern = Field(
        default_factory=lambda: AllowDenyPattern(allow=[".*"], deny=["^_.*"])
    )
    domain: Dict[str, AllowDenyPattern] = Field(
        default={},
        description="A map of domain names to allow deny patterns. Domains can be urn-based (`urn:li:domain:13ae4d85-d955-49fc-8474-9004c663a810`) or bare (`13ae4d85-d955-49fc-8474-9004c663a810`).",
    )
    topic_subject_map: Dict[str, str] = Field(
        default={},
        description="Provides the mapping for the `key` and the `value` schemas of a topic to the corresponding schema registry subject name. Each entry of this map has the form `<topic_name>-key`:`<schema_registry_subject_name_for_key_schema>` and `<topic_name>-value`:`<schema_registry_subject_name_for_value_schema>` for the key and the value schemas associated with the topic, respectively. This parameter is mandatory when the [RecordNameStrategy](https://docs.confluent.io/platform/current/schema-registry/serdes-develop/index.html#how-the-naming-strategies-work) is used as the subject naming strategy in the kafka schema registry. NOTE: When provided, this overrides the default subject name resolution even when the `TopicNameStrategy` or the `TopicRecordNameStrategy` are used.",
    )
    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = None
    schema_registry_class: str = Field(
        default="datahub.ingestion.source.confluent_schema_registry.ConfluentSchemaRegistry",
        description="The fully qualified implementation class(custom) that implements the KafkaSchemaRegistryBase interface.",
    )
    schema_tags_field: str = Field(
        default="tags",
        description="The field name in the schema metadata that contains the tags to be added to the dataset.",
    )
    enable_meta_mapping: bool = Field(
        default=True,
        description="When enabled, applies the mappings that are defined through the meta_mapping directives.",
    )

    meta_mapping: Dict = Field(
        default={},
        description="mapping rules that will be executed against top-level schema properties. Refer to the section below on meta automated mappings.",
    )
    field_meta_mapping: Dict = Field(
        default={},
        description="mapping rules that will be executed against field-level schema properties. Refer to the section below on meta automated mappings.",
    )
    strip_user_ids_from_email: bool = Field(
        default=False,
        description="Whether or not to strip email id while adding owners using meta mappings.",
    )
    tag_prefix: str = Field(
        default="", description="Prefix added to tags during ingestion."
    )
    ignore_warnings_on_schema_type: bool = Field(
        default=False,
        description="Disables warnings reported for non-AVRO/Protobuf value or key schemas if set.",
    )
    schema_resolution: SchemaResolutionFallback = Field(
        default_factory=SchemaResolutionFallback,
        description="Configuration for comprehensive schema resolution with multiple fallback strategies.",
    )
    disable_topic_record_naming_strategy: bool = Field(
        default=False,
        description="Disables the utilization of the TopicRecordNameStrategy for Schema Registry subjects. For more information, visit: https://docs.confluent.io/platform/current/schema-registry/serdes-develop/index.html#handling-differences-between-preregistered-and-client-derived-schemas:~:text=io.confluent.kafka.serializers.subject.TopicRecordNameStrategy",
    )
    ingest_schemas_as_entities: bool = Field(
        default=False,
        description="Enables ingesting schemas from schema registry as separate entities, in addition to the topics",
    )
    external_url_base: Optional[str] = Field(
        default=None,
        description="Base URL for external platform (e.g. Aiven) where topics can be viewed. The topic name will be appended to this base URL.",
    )
    profiling: ProfilerConfig = Field(
        default_factory=ProfilerConfig,
        description="Settings for message sampling and profiling",
    )
    confluent_catalog: KafkaConfluentCatalogConfig = Field(
        default_factory=KafkaConfluentCatalogConfig,
        description="Read topic tags and business metadata from the Confluent Cloud Stream Catalog. "
        "Connection details default to the Schema Registry ones already set under `connection`.",
    )
    stream_processing_lineage: StreamProcessingLineageConfig = Field(
        default_factory=StreamProcessingLineageConfig,
        description="Emit topic-to-topic transform lineage from stream-processing engines "
        "(ksqlDB persistent queries, Confluent Cloud Flink SQL statements, and Kafka Streams "
        "applications), each modeled as a DataJob with input and output topics.",
    )

    @model_validator(mode="after")
    def inherit_catalog_connection_from_schema_registry(self) -> "KafkaSourceConfig":
        if not self.confluent_catalog.enabled:
            return self

        catalog = self.confluent_catalog
        if not catalog.schema_registry_url:
            catalog.schema_registry_url = self.connection.schema_registry_url

        basic_auth = self.connection.schema_registry_config.get(
            SCHEMA_REGISTRY_BASIC_AUTH_KEY
        )
        if isinstance(basic_auth, str) and ":" in basic_auth:
            key, _, secret = basic_auth.partition(":")
            if not catalog.api_key:
                catalog.api_key = SecretStr(key)
            if not catalog.api_secret:
                catalog.api_secret = SecretStr(secret)
            # _register_secret_fields already ran (it is a mode="after" validator),
            # so credentials inherited here must be registered for redaction by hand.
            if is_masking_enabled():
                SecretRegistry.get_instance().register_secrets_batch(
                    {
                        "confluent_catalog.api_key": key,
                        "confluent_catalog.api_secret": secret,
                    }
                )

        catalog.validate_connection()
        return self

    def is_profiling_enabled(self) -> bool:
        """Check if profiling is enabled, respecting operation_config like SQL connectors."""
        return self.profiling.enabled and is_profiling_enabled(
            self.profiling.operation_config
        )
