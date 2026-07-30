from typing import Optional

from datahub.emitter.mcp_patch_builder import MetadataPatchProposal
from datahub.metadata.schema_classes import (
    KafkaAuditHeaderClass,
    StatusClass,
    SystemMetadataClass,
)


class StatusPatchBuilder(MetadataPatchProposal):
    def __init__(
        self,
        urn: str,
        system_metadata: Optional[SystemMetadataClass] = None,
        audit_header: Optional[KafkaAuditHeaderClass] = None,
    ) -> None:
        super().__init__(
            urn, system_metadata=system_metadata, audit_header=audit_header
        )

    def set_removed(self, removed: bool) -> "StatusPatchBuilder":
        self._add_patch(
            aspect_name=StatusClass.ASPECT_NAME,
            op="add",
            path=("removed",),
            value=removed,
        )
        return self
