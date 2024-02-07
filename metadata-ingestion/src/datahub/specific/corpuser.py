from typing import Generic, Optional, TypeVar

from datahub.emitter.mcp_patch_builder import MetadataPatchProposal
from datahub.metadata.schema_classes import KafkaAuditHeaderClass, SystemMetadataClass

T = TypeVar("T", bound=MetadataPatchProposal)


class CorpUserEditableInfoPatchHelper(Generic[T]):
    def __init__(
        self,
        parent: T,
        aspect_name: str,
    ) -> None:
        self.aspect_name = aspect_name
        self._parent = parent

    def parent(self) -> T:
        return self._parent

    def add_slack(self, value: str) -> "CorpUserEditableInfoPatchHelper":
        self._parent._add_patch(
            self.aspect_name,
            "add",
            path="/slack",
            value=value,
        )
        return self


class CorpUserPatchBuilder(MetadataPatchProposal):
    def __init__(
        self,
        urn: str,
        system_metadata: Optional[SystemMetadataClass] = None,
        audit_header: Optional[KafkaAuditHeaderClass] = None,
    ) -> None:
        super().__init__(
            urn, system_metadata=system_metadata, audit_header=audit_header
        )
        self.editable_info_patch_helper = CorpUserEditableInfoPatchHelper(
            self, aspect_name="corpUserEditableInfo"
        )
