import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from confluent_kafka.schema_registry.schema_registry_client import (
    Schema,
    SchemaRegistryClient,
)

from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    SchemaField,
    SchemaMetadata,
)

logger = logging.getLogger(__name__)


@dataclass
class SchemaAndFields:
    schema: Optional[Schema] = None
    fields: List[SchemaField] = field(default_factory=list)


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
