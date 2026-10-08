import json
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import avro.schema
from confluent_kafka.schema_registry.schema_registry_client import (
    Schema,
    SchemaRegistryClient,
)

from datahub.ingestion.source.kafka.kafka_constants import (
    SCHEMA_TYPE_AVRO,
    SCHEMA_TYPE_JSON,
)
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    KafkaSchema,
    SchemaField,
    SchemaMetadata,
)

logger = logging.getLogger(__name__)


@dataclass
class SchemaAndFields:
    schema: Optional[Schema] = None
    fields: List[SchemaField] = field(default_factory=list)


@dataclass
class DocumentSchemaMeta:
    """Topic-level description and annotations read from the value schema."""

    description: Optional[str] = None
    props: Dict[str, Any] = field(default_factory=dict)
    # Avro record full name, JSON Schema `title`, or the Protobuf main message.
    name: Optional[str] = None


def get_schema_prop(props: Dict[str, Any], key: str) -> Any:
    """`props[key]`, else the value at the dotted path `key` (`acme.event.tags`)."""
    if key in props:
        return props[key]
    value: Any = props
    for part in key.split("."):
        if not isinstance(value, dict) or part not in value:
            return None
        value = value[part]
    return value


class KafkaSchemaRegistryBase(ABC):
    @abstractmethod
    def get_schema_metadata(
        self, topic: str, platform_urn: str, is_subject: bool
    ) -> Optional[SchemaMetadata]:
        pass

    @abstractmethod
    def get_subjects(self) -> List[str]:
        pass

    @abstractmethod
    def _get_subject_for_topic(
        self, dataset_subtype: str, is_key_schema: bool
    ) -> Optional[str]:
        pass

    @abstractmethod
    def get_schema_registry_client(self) -> SchemaRegistryClient:
        pass

    def get_schema_and_fields_batch(
        self, topics: List[str], is_key_schema: bool = False
    ) -> Dict[str, SchemaAndFields]:
        # Default: per-topic lookups. Subclasses should override for batch performance.
        result: Dict[str, SchemaAndFields] = {}
        for topic in topics:
            try:
                schema_metadata = self.get_schema_metadata(topic, "", False)
                if schema_metadata and schema_metadata.fields:
                    result[topic] = SchemaAndFields(fields=schema_metadata.fields)
                else:
                    result[topic] = SchemaAndFields()
            except Exception as e:
                logger.warning(f"Failed to get schema metadata for topic {topic}: {e}")
                result[topic] = SchemaAndFields()
        return result

    def build_schema_metadata_with_key(
        self,
        topic: str,
        platform_urn: str,
        value_schema: Optional[Schema],
        value_fields: List[SchemaField],
        key_schema: Optional[Schema],
        key_fields: List[SchemaField],
    ) -> Optional[SchemaMetadata]:
        # Default ignores the pre-fetched schemas; subclasses should override to use them.
        return self.get_schema_metadata(topic, platform_urn, False)

    def get_document_schema_meta(
        self, schema_metadata: SchemaMetadata
    ) -> Optional[DocumentSchemaMeta]:
        """Description and annotations of the value schema, for any schema type.

        Avro: the record `doc` and its custom props. JSON Schema: `description` and the
        top-level keywords. Protobuf needs the referenced schemas to read its options,
        so registries that resolve references override this.
        """
        platform_schema = schema_metadata.platformSchema
        if (
            not isinstance(platform_schema, KafkaSchema)
            or not platform_schema.documentSchema
        ):
            return None
        if platform_schema.documentSchemaType == SCHEMA_TYPE_AVRO:
            avro_schema = avro.schema.parse(
                platform_schema.documentSchema, validate_names=False
            )
            return DocumentSchemaMeta(
                description=getattr(avro_schema, "doc", None),
                props=dict(avro_schema.other_props),
                name=getattr(avro_schema, "fullname", None),
            )
        if platform_schema.documentSchemaType == SCHEMA_TYPE_JSON:
            json_schema = json.loads(platform_schema.documentSchema)
            if not isinstance(json_schema, dict):
                return None
            description = json_schema.get("description")
            title = json_schema.get("title")
            return DocumentSchemaMeta(
                description=description if isinstance(description, str) else None,
                props=json_schema,
                name=title if isinstance(title, str) else None,
            )
        return None
