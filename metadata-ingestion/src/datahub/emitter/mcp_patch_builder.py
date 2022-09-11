import json
from collections import defaultdict
from dataclasses import dataclass
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.emitter.serialization_helper import pre_json_transform
from datahub.metadata.schema_classes import (
    ChangeTypeClass,
    GenericAspectClass,
    MetadataChangeProposalClass,
    OwnerClass,
    UpstreamClass,
)
from typing import Any, Dict, List


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
        self.patches[aspectName].append(_Patch(op, path, value))

    def build(self) -> List[MetadataChangeProposalClass]:
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

    def apply(self, emitter: DatahubRestEmitter) -> None:
        for mcp in self.build():
            emitter.emit_mcp(mcp)


class DatasetPatchBuilder(MetadataPatchProposal):
    def __init__(self, urn: str) -> None:
        super().__init__(urn, "dataset")

    def add_owner(self, owner: OwnerClass) -> "DatasetPatchBuilder":
        self._add_patch("ownership", "add", path="/owners/0", value=owner)
        return self

    def remove_owner(self, owner: OwnerClass) -> "DatasetPatchBuilder":
        self._add_patch("ownership", "remove", path="/owners/0", value=owner)
        return self

    def set_owners(self, owners: List[OwnerClass]) -> "DatasetPatchBuilder":
        self._add_patch("ownership", "replace", path="/owners", value=owners)
        return self

    def add_upstream_lineage(self, upstream: UpstreamClass) -> "DatasetPatchBuilder":
        self._add_patch(
            "upstreamLineage", "add", path="/upstreams/0", value=upstream
        )
        return self

    def remove_upstream_lineage(self, upstream: UpstreamClass) -> "DatasetPatchBuilder":
        self._add_patch(
            "upstreamLineage", "remove", path="/upstreams/0", value=upstream
        )
        return self

    def set_upstream_lineages(
        self, upstreams: List[UpstreamClass]
    ) -> "DatasetPatchBuilder":
        self._add_patch(
            "upstreamLineage", "replace", path="/upstreams", value=upstreams
        )
        return self
