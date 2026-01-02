import json
import time
from collections import defaultdict
from dataclasses import dataclass
from typing import (
    Any,
    Dict,
    List,
    Literal,
    Optional,
    Protocol,
    Tuple,
    Union,
    runtime_checkable,
)

from typing_extensions import LiteralString

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

# Unit Separator character (U+241F) used in arrayPrimaryKeys for nested paths
UNIT_SEPARATOR = "\u241f"


@runtime_checkable
class SupportsToObj(Protocol):
    def to_obj(self) -> Any: ...


def _recursive_to_obj(obj: Any) -> Any:
    if isinstance(obj, list):
        return [_recursive_to_obj(v) for v in obj]
    elif isinstance(obj, SupportsToObj):
        return obj.to_obj()
    else:
        return obj


PatchPath = Tuple[Union[LiteralString, Urn], ...]
PatchOp = Literal["add", "remove", "replace"]


@dataclass
class _Patch(SupportsToObj):
    op: PatchOp
    path: PatchPath
    value: Any

    def to_obj(self) -> Dict:
        quoted_path = "/" + "/".join(MetadataPatchProposal.quote(p) for p in self.path)
        return {
            "op": self.op,
            "path": quoted_path,
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
    def quote(cls, value: Union[str, Urn]) -> str:
        return str(value).replace("~", "~0").replace("/", "~1")

    def _add_patch(
        self,
        aspect_name: str,
        op: PatchOp,
        path: PatchPath,
        value: Any,
    ) -> None:
        # TODO: Validate that aspectName is a valid aspect for this entityType
        self.patches[aspect_name].append(_Patch(op, path, value))

    def build(self) -> List[MetadataChangeProposalClass]:
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


def parse_patch_path(path_str: str) -> PatchPath:
    """
    Parse a JSON Patch path string into a PatchPath tuple.

    Args:
        path_str: A JSON Patch path string (e.g., "/tags/urn:li:tag:test")

    Returns:
        A PatchPath tuple representing the path components
    """
    # Remove leading slash and split
    if not path_str.startswith("/"):
        raise ValueError(f"Patch path must start with '/': {path_str}")
    parts = path_str[1:].split("/")
    # Unquote each part
    unquoted_parts = []
    for part in parts:
        if not part:
            continue
        # Unquote: ~1 -> /, ~0 -> ~
        unquoted = part.replace("~1", "/").replace("~0", "~")
        unquoted_parts.append(unquoted)
    return tuple(unquoted_parts)


@dataclass
class GenericJsonPatch:
    """
    Represents a GenericJsonPatch structure with arrayPrimaryKeys support.

    This extends standard JSON Patch to support idempotent array operations
    using composite keys instead of array indices.
    """

    array_primary_keys: Dict[str, List[str]]
    patch: List[_Patch]
    force_generic_patch: bool = False

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "arrayPrimaryKeys": self.array_primary_keys,
            "patch": [op.to_obj() for op in self.patch],
            "forceGenericPatch": self.force_generic_patch,
        }

    def to_generic_aspect(self) -> GenericAspectClass:
        """Serialize to GenericAspectClass for use in MetadataChangeProposalClass."""
        return GenericAspectClass(
            value=json.dumps(
                pre_json_transform(_recursive_to_obj(self.to_dict()))
            ).encode(),
            contentType=JSON_PATCH_CONTENT_TYPE,
        )

    @classmethod
    def from_dict(cls, patch_dict: Dict[str, Any]) -> "GenericJsonPatch":
        """
        Create a GenericJsonPatch from a dictionary (e.g., from parsed JSON).

        Args:
            patch_dict: Dictionary containing arrayPrimaryKeys, patch, and optionally forceGenericPatch

        Returns:
            A GenericJsonPatch instance
        """
        array_primary_keys = patch_dict.get("arrayPrimaryKeys", {})
        force_generic_patch = patch_dict.get("forceGenericPatch", False)
        patch_ops = patch_dict.get("patch", [])

        # Convert dict-based patch operations to _Patch instances
        patch_list = []
        for op_dict in patch_ops:
            op_str = op_dict.get("op")
            path_str = op_dict.get("path", "")
            value = op_dict.get("value")

            # Validate op
            if op_str not in ("add", "remove", "replace"):
                raise ValueError(f"Unsupported patch operation: {op_str}")

            # Parse path string to tuple
            path_tuple = parse_patch_path(path_str)

            patch_list.append(_Patch(op=op_str, path=path_tuple, value=value))

        return cls(
            array_primary_keys=array_primary_keys,
            patch=patch_list,
            force_generic_patch=force_generic_patch,
        )
