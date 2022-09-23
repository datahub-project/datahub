import json
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional

from datahub.emitter.serialization_helper import pre_json_transform
from datahub.metadata.schema_classes import (
    ChangeTypeClass,
    GenericAspectClass,
    KafkaAuditHeaderClass,
    MetadataChangeProposalClass,
    SystemMetadataClass,
)


def _recursive_to_obj(obj: Any) -> Any:
    if isinstance(obj, list):
        return [_recursive_to_obj(v) for v in obj]
    elif hasattr(obj, "to_obj"):
        return obj.to_obj()
    else:
        return obj


@dataclass
class _Patch:
    op: str  # one of ['add', 'remove', 'replace']; we don't support move, copy or test
    path: str
    value: Any

    def to_obj(self) -> Dict:
        return {
            "op": self.op,
            "path": self.path,
            "value": _recursive_to_obj(self.value),
        }


class MetadataPatchProposal:
    urn: str
    entity_type: str

    # mapping: aspectName -> list of patches
    patches: Dict[str, List[_Patch]]

    def __init__(
        self,
        urn: str,
        entity_type: str,
        system_metadata: Optional[SystemMetadataClass] = None,
        audit_header: Optional[KafkaAuditHeaderClass] = None,
    ) -> None:
        self.urn = urn
        self.entity_type = entity_type
        self.system_metadata = system_metadata
        self.audit_header = audit_header
        self.patches = defaultdict(list)

    def _add_patch(self, aspect_name: str, op: str, path: str, value: Any) -> None:
        # TODO: Validate that aspectName is a valid aspect for this entityType
        self.patches[aspect_name].append(_Patch(op, path, value))

    def build(self) -> Iterable[MetadataChangeProposalClass]:
        return [
            MetadataChangeProposalClass(
                entityUrn=self.urn,
                entityType=self.entity_type,
                changeType=ChangeTypeClass.PATCH,
                aspectName=aspect_name,
                aspect=GenericAspectClass(
                    value=json.dumps(
                        pre_json_transform(_recursive_to_obj(patches))
                    ).encode(),
                    contentType="application/json-patch+json",
                ),
                auditHeader=self.audit_header,
                systemMetadata=self.system_metadata,
            )
            for aspect_name, patches in self.patches.items()
        ]
