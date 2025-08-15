from typing import Dict, Optional

from pydantic import Field, PositiveInt

from datahub.configuration.common import AllowDenyPattern
from datahub.configuration.kafka import KafkaConsumerConnectionConfig
from datahub.configuration.source_common import (
    DatasetSourceConfigMixin,
    LowerCaseDatasetUrnConfigMixin,
)
from datahub.ingestion.source.ge_profiling_config import GEProfilingConfig
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
)
from datahub.ingestion.source_config.operation_config import is_profiling_enabled


class ProfilerConfig(GEProfilingConfig):
    """
    Kafka-specific profiling configuration that extends GEProfilingConfig.

    Inherits ALL standard profiling functionality from GEProfilingConfig including:
    - operation_config.lower_freq_profile_enabled: Whether to do profiling at lower frequency
    - operation_config.profile_day_of_week: Day of week to run profiling (0=Monday, 6=Sunday)
    - operation_config.profile_date_of_month: Date of month to run profiling (1-31)
    - All include_field_* flags (null_count, distinct_count, min/max/mean/median/stddev, etc.)
    - turn_off_expensive_profiling_metrics, field_sample_values_limit, max_number_of_fields_to_profile
    - report_dropped_profiles, catch_exceptions, tags_to_ignore_sampling
    - profile_nested_fields (useful for complex JSON/Avro), query_combiner_enabled

    Additional Kafka-specific profiling options:
    """

    max_sample_time_seconds: PositiveInt = Field(
        default=60,
        description="Maximum time to spend sampling messages in seconds",
    )
    sampling_strategy: str = Field(
        default="latest",
        description="Strategy for sampling messages: 'latest' (from end of topic), 'random' (random offsets), 'stratified' (evenly distributed), 'full' (entire topic, respects sample_size)",
    )
    cache_sample_results: bool = Field(
        default=True,
        description="Whether to cache sample results between runs for the same topic",
    )
    cache_ttl_seconds: PositiveInt = Field(
        default=3600,
        description="How long to keep cached sample results in seconds",
    )
    batch_size: PositiveInt = Field(
        default=100,
        description="Number of messages to fetch in a single batch (for more efficient reading)",
    )

    # Override sample_size from base class with Kafka-appropriate default
    sample_size: int = Field(
        default=1000,
        description="Number of messages to sample for profiling. Higher values provide more accurate statistics but take longer to process.",
    )

    # Override max_workers with Kafka-appropriate default (GEProfilingConfig defaults to 5 * cpu_count)
    max_workers: int = Field(
        default=1,
        description="Number of worker threads to use for profiling. Kafka profiling can now be parallelized.",
    )

    # Kafka-specific field for handling complex nested JSON/Avro structures
    flatten_max_depth: int = Field(
        default=5,
        description="Maximum recursion depth when flattening nested JSON structures. Lower values prevent recursion errors but may truncate deeply nested data.",
    )


class KafkaSourceConfig(
    StatefulIngestionConfigBase,
    DatasetSourceConfigMixin,
    LowerCaseDatasetUrnConfigMixin,
):
    connection: KafkaConsumerConnectionConfig = KafkaConsumerConnectionConfig()

    topic_patterns: AllowDenyPattern = AllowDenyPattern(allow=[".*"], deny=["^_.*"])
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
    enable_schemaless_fallback: bool = Field(
        default=True,
        description="When enabled, automatically falls back to schema-less processing for topics not found in the schema registry. This allows DataHub to extract schema information from actual message data instead of failing.",
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
        default=ProfilerConfig(),
        description="Settings for message sampling and profiling",
    )

    def is_profiling_enabled(self) -> bool:
        """Check if profiling is enabled, respecting operation_config like SQL connectors."""
        return self.profiling.enabled and is_profiling_enabled(
            self.profiling.operation_config
        )
