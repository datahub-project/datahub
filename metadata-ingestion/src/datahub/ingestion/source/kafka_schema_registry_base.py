from abc import ABC, abstractmethod
from typing import Optional

from datahub.metadata.com.linkedin.pegasus2avro.schema import SchemaMetadata


class KafkaSchemaRegistryBase(ABC):
    @abstractmethod
    def get_schema_metadata(
        self, topic: str, platform_urn: str
    ) -> Optional[SchemaMetadata]:
        pass
