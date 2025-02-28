from abc import ABC, abstractmethod
from typing import List, Optional

from datahub.metadata.com.linkedin.pegasus2avro.schema import SchemaMetadata


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
