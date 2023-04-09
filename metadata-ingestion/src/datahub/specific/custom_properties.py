from typing import Generic, Iterable, Optional, TypeVar

from datahub.emitter.mcp_patch_builder import MetadataPatchProposal
from datahub.metadata.schema_classes import (
    KafkaAuditHeaderClass,
    MetadataChangeProposalClass,
    SystemMetadataClass,
)

T = TypeVar("T", bound=MetadataPatchProposal)


class CustomPropertiesPatchBuilder(Generic[T], MetadataPatchProposal):
    def __init__(
        self,
        parent: T,
        entity_urn: str,
        entity_type: str,
        aspect_name: str,
        system_metadata: Optional[SystemMetadataClass] = None,
        audit_header: Optional[KafkaAuditHeaderClass] = None,
    ) -> None:
        super().__init__(
            entity_urn,
            entity_type,
            system_metadata=system_metadata,
            audit_header=audit_header,
        )
        self.aspect_name = aspect_name
        self._parent = parent
        self.aspect_field = "customProperties"

    def parent(self) -> T:
        return self._parent

    def add_property(self, key: str, value: str) -> "CustomPropertiesPatchBuilder":
        self._parent._add_patch(
            self.aspect_name,
            "add",
            path=f"/{self.aspect_field}/{key}",
            value=value,
        )
        return self

    def remove_property(self, key: str) -> "CustomPropertiesPatchBuilder":
        self._parent._add_patch(
            self.aspect_name,
            "remove",
            path=f"/{self.aspect_field}/{key}",
            value={},
        )
        return self

    def build(self) -> Iterable[MetadataChangeProposalClass]:
        return self._parent.build()
