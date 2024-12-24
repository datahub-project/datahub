import json
import time
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Sequence, Union

from datahub.emitter.aspect import JSON_PATCH_CONTENT_TYPE
from datahub.emitter.serialization_helper import pre_json_transform
from datahub.metadata.schema_classes import (
    AuditStampClass,
    ChangeTypeClass,
    EdgeClass,
    GenericAspectClass,
    KafkaAuditHeaderClass,
    MetadataChangeProposalClass,
    SystemMetadataClass,
)
from datahub.metadata.urns import Urn
from datahub.utilities.urns.urn import guess_entity_type


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
        system_metadata: Optional[SystemMetadataClass] = None,
        audit_header: Optional[KafkaAuditHeaderClass] = None,
    ) -> None:
        self.urn = urn
        self.entity_type = guess_entity_type(urn)
        self.system_metadata = system_metadata
        self.audit_header = audit_header
        self.patches = defaultdict(list)

    # Json Patch quoting based on https://jsonpatch.com/#json-pointer
    @classmethod
    def quote(cls, value: str) -> str:
        return value.replace("~", "~0").replace("/", "~1")

    def _add_patch(
        self, aspect_name: str, op: str, path: Union[str, Sequence[str]], value: Any
    ) -> None:
        if not isinstance(path, str):
            path = "/" + "/".join(self.quote(p) for p in path)

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
                    contentType=JSON_PATCH_CONTENT_TYPE,
                ),
                auditHeader=self.audit_header,
                systemMetadata=self.system_metadata,
            )
            for aspect_name, patches in self.patches.items()
        ]

    @classmethod
    def _mint_auditstamp(cls, message: Optional[str] = None) -> AuditStampClass:
        """
        Creates an AuditStampClass instance with the current timestamp and other default values.

        Args:
            message: The message associated with the audit stamp (optional).

        Returns:
            An instance of AuditStampClass.
        """
        return AuditStampClass(
            time=int(time.time() * 1000.0),
            actor="urn:li:corpuser:datahub",
            message=message,
        )

    @classmethod
    def _ensure_urn_type(
        cls, entity_type: str, edges: List[EdgeClass], context: str
    ) -> None:
        """
        Ensures that the destination URNs in the given edges have the specified entity type.

        Args:
            entity_type: The entity type to check against.
            edges: A list of Edge objects.
            context: The context or description of the operation.

        Raises:
            ValueError: If any of the destination URNs is not of the specified entity type.
        """
        for e in edges:
            urn = Urn.from_string(e.destinationUrn)
            if not urn.entity_type == entity_type:
                raise ValueError(
                    f"{context}: {e.destinationUrn} is not of type {entity_type}"
                )
