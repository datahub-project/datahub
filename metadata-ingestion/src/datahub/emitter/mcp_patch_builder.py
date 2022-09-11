import json
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List

from datahub.emitter.serialization_helper import pre_json_transform
from datahub.metadata.schema_classes import (
    ChangeTypeClass,
    GenericAspectClass,
    MetadataChangeProposalClass,
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
    entityType: str

    # mapping: aspectName -> list of patches
    patches: Dict[str, List[_Patch]]

    def __init__(self, urn: str, entityType: str) -> None:
        self.urn = urn
        self.entityType = entityType
        self.patches = defaultdict(list)

    def _add_patch(self, aspectName: str, op: str, path: str, value: Any) -> None:
        # TODO: Validate that aspectName is a valid aspect for this entityType
        self.patches[aspectName].append(_Patch(op, path, value))

    def build(self) -> Iterable[MetadataChangeProposalClass]:
        return [
            MetadataChangeProposalClass(
                entityUrn=self.urn,
                entityType=self.entityType,
                changeType=ChangeTypeClass.PATCH,
                aspectName=aspectName,
                aspect=GenericAspectClass(
                    value=json.dumps(
                        pre_json_transform(_recursive_to_obj(patches))
                    ).encode(),
                    contentType="application/jsonpatch",
                ),
                # TODO: entityKeyAspect=serializedEntityKeyAspect,
                # TODO auditHeader
                # TODO systemMetadata
            )
            for aspectName, patches in self.patches.items()
        ]
