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
    get_args,
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
_VALID_PATCH_OPS = set(get_args(PatchOp))


@dataclass
class _Patch(SupportsToObj):
    op: PatchOp
    path: PatchPath
    value: Any
    # When set, this patch is emitted inside a GenericJsonPatch envelope rather
    # than as a plain JSON array element.  All patches for the same aspect that
    # share the same array_primary_keys are grouped into one MCP.
    array_primary_keys: Optional[Dict[str, List[str]]] = None

    @classmethod
    def quote_path_component(cls, value: Union[str, Urn]) -> str:
        """Quote a single path component for JSON Patch, based on https://jsonpatch.com/#json-pointer"""
        return str(value).replace("~", "~0").replace("/", "~1")

    @classmethod
    def unquote_path_component(cls, value: str) -> str:
        """Unquote a single path component from JSON Patch, based on https://jsonpatch.com/#json-pointer"""
        return value.replace("~1", "/").replace("~0", "~")

    def to_obj(self) -> Dict:
        quoted_path = "/" + "/".join(self.quote_path_component(p) for p in self.path)
        return {
            "op": self.op,
            "path": quoted_path,
            "value": _recursive_to_obj(self.value),
        }


class MetadataPatchProposal:
    urn: str
    entity_type: str

    # mapping: aspectName -> list of patches
    # Patches with array_primary_keys set are grouped separately in build()
    # and emitted as GenericJsonPatch envelopes.
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

    @classmethod
    def quote(cls, value: Union[str, Urn]) -> str:
        return _Patch.quote_path_component(value)

    def _add_patch(
        self,
        aspect_name: str,
        op: PatchOp,
        path: PatchPath,
        value: Any,
        array_primary_keys: Optional[Dict[str, List[str]]] = None,
    ) -> None:
        # TODO: Validate that aspectName is a valid aspect for this entityType
        self.patches[aspect_name].append(_Patch(op, path, value, array_primary_keys))

    def build(self) -> List[MetadataChangeProposalClass]:
        mcps = []
        for aspect_name, patches in self.patches.items():
            # Group patches by array_primary_keys.  None = plain JSON array;
            # anything else = GenericJsonPatch envelope.
            groups: Dict[
                Optional[str], Tuple[Optional[Dict[str, List[str]]], List[_Patch]]
            ] = {}
            for patch in patches:
                apk = patch.array_primary_keys
                apk_key = json.dumps(apk) if apk is not None else None
                if apk_key not in groups:
                    groups[apk_key] = (apk, [])
                groups[apk_key][1].append(patch)

            for apk, group in groups.values():
                if apk is None:
                    payload: Any = _recursive_to_obj(group)
                else:
                    payload = {
                        "arrayPrimaryKeys": apk,
                        "patch": _recursive_to_obj(group),
                    }
                mcps.append(
                    MetadataChangeProposalClass(
                        entityUrn=self.urn,
                        entityType=self.entity_type,
                        changeType=ChangeTypeClass.PATCH,
                        aspectName=aspect_name,
                        aspect=GenericAspectClass(
                            value=json.dumps(pre_json_transform(payload)).encode(),
                            contentType=JSON_PATCH_CONTENT_TYPE,
                        ),
                        auditHeader=self.audit_header,
                        systemMetadata=self.system_metadata,
                    )
                )
        return mcps

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


def determine_array_primary_keys(
    *, field_name: str, path: List[Optional[str]], default_key_fields: List[str]
) -> Tuple[List[str], Optional[Dict[str, List[str]]]]:
    """Determines the array primary keys for a given list of path values.

    Allows us to efficiently bulk delete multiple entries.
    Our patch backend supports the following mechanism:
      For primary keys (A, B, C), we can delete all entries with A=a and B=b regardless of C by passing path (a, b).

    However, if we want to delete all entries with C=c regardless of A and B,
    we need to change the order of the primary keys.

    This method determines if we need to change the order of the primary keys
    based on the presence of None values in the path values.

    Args:
        field_name: The name of the aspect field being patched, e.g. `tags`.
        default_key_fields: The default key fields for this aspect in the backend's Template class
        path: A list of path values in the order of the default primary keys.
              Does _not_ include the field name of the aspect being patched, e.g. `tags`.
    Returns:
        A tuple containing:
          - path: The path values ordered according to the determined array primary keys
          - array_primary_keys: the key fields to be sent in the patch, or None if the default order can be used
    """
    is_none = [v is None for v in path]
    if sorted(is_none) == is_none:
        # All None values are at the end, we can use the default primary key order
        remove_nones = [v for v in path if v is not None]
        return list(remove_nones), None
    else:
        # Reorder the primary keys so that None values are at the end, and specify the new order in arrayPrimaryKeys
        path_with_fields = list(zip(path, default_key_fields, strict=False))
        path_with_fields.sort(key=lambda x: x[0] is None)
        new_path, new_key_fields = zip(*path_with_fields, strict=False)
        # Strip trailing Nones — backend treats truncated paths as prefix matches.
        new_path_list = [v for v in new_path if v is not None]
        return new_path_list, {field_name: list(new_key_fields)}


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
    unquoted_parts = []
    for part in parts:
        if not part:
            raise ValueError(
                f"Patch path has empty component (invalid '//' or leading/trailing '/'): {path_str!r}"
            )
        unquoted = _Patch.unquote_path_component(part)
        unquoted_parts.append(unquoted)
    return tuple(unquoted_parts)


@dataclass
class GenericJsonPatch:
    """
    Represents a GenericJsonPatch structure with arrayPrimaryKeys support.

    This extends standard JSON Patch to support idempotent array operations
    using composite keys instead of array indices.

    Mirrors the Java class: com.linkedin.metadata.aspect.patch.GenericJsonPatch
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
        try:
            payload = pre_json_transform(_recursive_to_obj(self.to_dict()))
            serialized = json.dumps(payload).encode()
        except (TypeError, ValueError) as e:
            raise ValueError(
                f"Failed to serialize GenericJsonPatch to JSON: {e}"
            ) from e
        return GenericAspectClass(
            value=serialized,
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
            if op_str not in _VALID_PATCH_OPS:
                raise ValueError(f"Unsupported patch operation: {op_str}")

            # Parse path string to tuple
            path_tuple = parse_patch_path(path_str)

            patch_list.append(_Patch(op=op_str, path=path_tuple, value=value))

        return cls(
            array_primary_keys=array_primary_keys,
            patch=patch_list,
            force_generic_patch=force_generic_patch,
        )
