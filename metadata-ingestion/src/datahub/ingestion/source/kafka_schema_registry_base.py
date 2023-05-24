from abc import ABC, abstractmethod
from typing import Any, List, Optional

from datahub.metadata.com.linkedin.pegasus2avro.schema import SchemaMetadata


class KafkaSchemaRegistryBase(ABC):
    def get_schema_metadata(
        self, topic: str, platform_urn: str
    ) -> Optional[SchemaMetadata]:
        """@deprecated: use get_aspects_from_schema instead."""
        aspects = self.get_aspects_from_schema(topic, platform_urn)
        try:
            return next(
                aspect for aspect in aspects if isinstance(aspect, SchemaMetadata)
            )
        except StopIteration:
            return None

    @abstractmethod
    def get_aspects_from_schema(self, topic: str, platform_urn: str) -> List[Any]:
        """Returns a list of aspects that can be attached to a dataset"""
        pass
