import logging
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Tuple

from confluent_kafka.schema_registry.schema_registry_client import Schema

from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    SchemaField,
    SchemaMetadata,
)

logger = logging.getLogger(__name__)


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

    def get_schema_and_fields_batch(
        self, topics: List[str], is_key_schema: bool = False
    ) -> Dict[str, Tuple[Optional[Schema], List[SchemaField]]]:
        """
        Get schemas and fields for multiple topics in batch.
        Default implementation falls back to individual schema lookups for backward compatibility.
        Subclasses should override this for better performance.
        """
        result = {}
        for topic in topics:
            try:
                schema_metadata = self.get_schema_metadata(topic, "", False)
                if schema_metadata and schema_metadata.fields:
                    result[topic] = (None, schema_metadata.fields)
                else:
                    result[topic] = (None, [])
            except Exception as e:
                logger.warning(f"Failed to get schema metadata for topic {topic}: {e}")
                result[topic] = (None, [])
        return result

    def build_schema_metadata(
        self,
        topic: str,
        platform_urn: str,
        schema: Optional[Schema],
        fields: List[SchemaField],
    ) -> Optional[SchemaMetadata]:
        """
        Build SchemaMetadata from pre-fetched schema and fields.
        Default implementation falls back to the original method for backward compatibility.
        Subclasses should override this for better performance.
        """
        return self.get_schema_metadata(topic, platform_urn, False)

    def build_schema_metadata_with_key(
        self,
        topic: str,
        platform_urn: str,
        value_schema: Optional[Schema],
        value_fields: List[SchemaField],
        key_schema: Optional[Schema],
        key_fields: List[SchemaField],
    ) -> Optional[SchemaMetadata]:
        """
        Build SchemaMetadata from pre-fetched value and key schemas and fields.
        Default implementation falls back to the original method for backward compatibility.
        Subclasses should override this for better performance.
        """
        return self.get_schema_metadata(topic, platform_urn, False)
