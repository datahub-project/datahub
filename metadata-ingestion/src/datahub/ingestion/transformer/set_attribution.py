import json
import logging
import time
from typing import Any, Dict, Iterable, List, Optional, cast

import datahub.emitter.mce_builder as builder
from datahub.configuration.common import ConfigModel
from datahub.emitter.aspect import ASPECT_MAP, JSON_PATCH_CONTENT_TYPE
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_patch_builder import (
    UNIT_SEPARATOR,
    GenericJsonPatch,
    PatchPath,
    _Patch,
)
from datahub.ingestion.api.common import EndOfStream, PipelineContext, RecordEnvelope
from datahub.ingestion.transformer.base_transformer import (
    BaseTransformer,
    _update_work_unit_id,
)
from datahub.metadata.schema_classes import (
    ChangeTypeClass,
    GenericAspectClass,
    GlobalTagsClass,
    GlossaryTermsClass,
    MetadataChangeEventClass,
    MetadataChangeProposalClass,
    OwnershipClass,
)
from datahub.utilities.urns.urn import guess_entity_type

logger = logging.getLogger(__name__)

# Supported aspects configuration
SUPPORTED_ASPECTS = ["globalTags", "ownership", "glossaryTerms"]

ASPECT_CONFIG = {
    "globalTags": {
        "array_field": "tags",
        "item_id_field": "tag",
        "attribution_field": "attribution",
    },
    "ownership": {
        "array_field": "owners",
        "item_id_field": "owner",
        "attribution_field": "attribution",
    },
    "glossaryTerms": {
        "array_field": "terms",
        "item_id_field": "urn",
        "attribution_field": "attribution",
    },
}


def build_patch_path_tuple(array_field: str, *keys: str) -> PatchPath:
    """Build PatchPath tuple for composite keys."""
    return (array_field, *keys)


class SetAttributionConfig(ConfigModel):
    """Configuration for SetAttributionTransformer."""

    attribution_source: str  # Required: e.g., "urn:li:platformResource:ingestion"
    actor: Optional[str] = "urn:li:corpuser:datahub"  # Default actor
    source_detail: Optional[Dict[str, str]] = None  # Optional sourceDetail map
    patch_mode: bool = False  # False = upsert mode, True = patch mode


class SetAttributionTransformer(BaseTransformer):
    """
    Transformer that converts UPSERT aspects (globalTags, ownership, glossaryTerms)
    into PATCH operations with attribution metadata.

    This transformer adds attribution information to each item in the aspect and
    supports two modes:
    - Default (upsert mode, patch_mode=False): Replaces all items for a given attribution source
      with a single PATCH operation containing a map of all items
    - Patch mode (patch_mode=True): Creates individual PATCH operations for each item,
      allowing incremental updates

    The transformer works at the MCP/MCPW level to change changeType from UPSERT
    to PATCH, which requires working at this abstraction level rather than the
    Aspect level.

    Supported aspects:
    - globalTags: Adds attribution to TagAssociation items
    - ownership: Adds attribution to Owner items
    - glossaryTerms: Adds attribution to GlossaryTermAssociation items

    For PATCH inputs, the transformer adds attribution to existing patch operations
    while preserving the original patch semantics and structure.

    Example configuration:
    ```yaml
    transformers:
      - type: set_attribution
        config:
          attribution_source: "urn:li:platformResource:ingestion"
          actor: "urn:li:corpuser:datahub"
          source_detail:
            pipeline_id: "my-pipeline"
            version: "1.0"
          patch_mode: false  # or true for patch mode
    ```
    """

    def __init__(self, config: SetAttributionConfig, ctx: PipelineContext):
        super().__init__()
        self.config = config
        self.ctx = ctx

    @classmethod
    def create(
        cls, config_dict: dict, ctx: PipelineContext
    ) -> "SetAttributionTransformer":
        """Create transformer instance from configuration."""
        config = SetAttributionConfig.model_validate(config_dict)
        return cls(config, ctx)

    def entity_types(self) -> List[str]:
        """Return all entity types - transformer works on aspect level."""
        return ["*"]

    def _build_attribution(
        self, item: Any, existing_attribution: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Build attribution metadata for an item."""
        if existing_attribution:
            logger.debug(
                f"Overwriting existing attribution for item: {existing_attribution}"
            )

        attribution: Dict[str, Any] = {
            "time": int(time.time() * 1000),
            "actor": self.config.actor,
            "source": self.config.attribution_source,
        }

        if self.config.source_detail:
            attribution["sourceDetail"] = self.config.source_detail

        return attribution

    def _extract_items_from_aspect(self, aspect_name: str, aspect: Any) -> List[Any]:
        """Extract items array from aspect based on aspect name."""
        if aspect_name == "globalTags":
            global_tags = cast(GlobalTagsClass, aspect)
            return list(global_tags.tags) if global_tags.tags else []
        elif aspect_name == "ownership":
            ownership = cast(OwnershipClass, aspect)
            return list(ownership.owners) if ownership.owners else []
        elif aspect_name == "glossaryTerms":
            glossary_terms = cast(GlossaryTermsClass, aspect)
            return list(glossary_terms.terms) if glossary_terms.terms else []
        else:
            return []

    def _get_item_id(self, aspect_name: str, item: Any) -> str:
        """Get the identifier field value from an item."""
        config = ASPECT_CONFIG[aspect_name]
        item_id_field = config["item_id_field"]

        # Access the field - items are typically RecordTemplate objects
        if hasattr(item, item_id_field):
            value = getattr(item, item_id_field)
            # Convert URN objects to strings
            # TagUrn, GlossaryTermUrn, and Urn objects have urn() method
            if hasattr(value, "urn"):
                return value.urn()
            # If it's already a string, return as-is
            if isinstance(value, str):
                return value
            # Fallback to string conversion
            return str(value)
        return ""

    def _build_item_with_attribution(
        self, aspect_name: str, item: Any
    ) -> Dict[str, Any]:
        """Build item dictionary with attribution metadata."""
        config = ASPECT_CONFIG[aspect_name]
        attribution_field = config["attribution_field"]

        # Convert item to dict
        item_dict = item.to_obj() if hasattr(item, "to_obj") else {}

        # Get existing attribution if present
        existing_attribution = item_dict.get(attribution_field)

        # Build new attribution
        attribution = self._build_attribution(item, existing_attribution)

        # Update item dict with attribution
        item_dict[attribution_field] = attribution

        return item_dict

    def _create_patch_ops_for_aspect(
        self, aspect_name: str, items: List[Any]
    ) -> List[_Patch]:
        """Create patch operations for items in an aspect."""
        config = ASPECT_CONFIG[aspect_name]
        array_field = config["array_field"]

        if self.config.patch_mode:
            # Patch mode: individual operations for each item
            if not items:
                return []  # No items, no operations in patch mode
            # Patch mode: individual operations for each item
            patch_ops = []
            for item in items:
                item_id = self._get_item_id(aspect_name, item)
                path = build_patch_path_tuple(
                    array_field, self.config.attribution_source, item_id
                )
                value = self._build_item_with_attribution(aspect_name, item)
                patch_ops.append(_Patch(op="add", path=path, value=value))
            return patch_ops
        else:
            # Upsert mode: single operation with map of all items
            # Even if empty, create a patch operation with empty map
            items_map = {}
            for item in items:
                item_id = self._get_item_id(aspect_name, item)
                items_map[item_id] = self._build_item_with_attribution(
                    aspect_name, item
                )

            path = build_patch_path_tuple(array_field, self.config.attribution_source)
            return [_Patch(op="add", path=path, value=items_map)]

    def _get_array_primary_keys(self, aspect_name: str) -> List[str]:
        """Get array primary keys for an aspect."""
        config = ASPECT_CONFIG[aspect_name]
        item_id_field = config["item_id_field"]
        # Use UNIT_SEPARATOR for nested path in attribution.source
        return [f"attribution{UNIT_SEPARATOR}source", item_id_field]

    def _add_attribution_to_patch_mcp(
        self,
        entity_urn: str,
        entity_type: str,
        aspect_name: str,
        patch_aspect: Any,
        system_metadata: Optional[Any] = None,
        audit_header: Optional[Any] = None,
    ) -> Optional[MetadataChangeProposalClass]:
        """Add attribution to an existing PATCH MCP while preserving original semantics."""
        if not isinstance(patch_aspect, GenericAspectClass):
            logger.warning(
                f"PATCH aspect is not GenericAspectClass, cannot add attribution: {aspect_name}"
            )
            return None

        if patch_aspect.contentType != JSON_PATCH_CONTENT_TYPE:
            logger.warning(
                f"PATCH aspect has unexpected content type: {patch_aspect.contentType}"
            )
            return None

        try:
            # Parse existing GenericJsonPatch
            patch_dict = json.loads(patch_aspect.value.decode())

            # Parse existing GenericJsonPatch to get _Patch instances
            existing_generic_patch = GenericJsonPatch.from_dict(patch_dict)

            # Process each patch operation to add attribution
            config = ASPECT_CONFIG[aspect_name]
            array_field = config["array_field"]
            attribution_field = config["attribution_field"]
            new_patch_ops = []
            for patch_op in existing_generic_patch.patch:
                op = patch_op.op
                path_tuple = patch_op.path
                value = patch_op.value

                # Check if this path is for our array field
                # Path tuple format: (array_field, attribution_source, ...)
                is_our_array_field = (
                    len(path_tuple) > 0
                    and path_tuple[0] == array_field
                    and len(path_tuple) > 1
                    and path_tuple[1] == self.config.attribution_source
                )

                # For "add" operations that add items to our array field, add attribution
                if op == "add" and value is not None and is_our_array_field:
                    # Add attribution to the value
                    if isinstance(value, dict):
                        # Single item or map of items
                        updated_value = self._add_attribution_to_value(
                            aspect_name, value, attribution_field
                        )
                        new_patch_ops.append(
                            _Patch(op=op, path=path_tuple, value=updated_value)
                        )
                    else:
                        # Keep original value if we can't process it
                        new_patch_ops.append(
                            _Patch(op=op, path=path_tuple, value=value)
                        )
                else:
                    # Not our array field or not an "add" operation, keep original
                    new_patch_ops.append(_Patch(op=op, path=path_tuple, value=value))

            # Create new GenericJsonPatch with updated operations
            generic_json_patch = GenericJsonPatch(
                array_primary_keys=existing_generic_patch.array_primary_keys,
                patch=new_patch_ops,
                force_generic_patch=existing_generic_patch.force_generic_patch,
            )

            # Create MCP
            return MetadataChangeProposalClass(
                entityUrn=entity_urn,
                entityType=entity_type,
                changeType=ChangeTypeClass.PATCH,
                aspectName=aspect_name,
                aspect=generic_json_patch.to_generic_aspect(),
                systemMetadata=system_metadata,
                auditHeader=audit_header,
            )
        except (json.JSONDecodeError, KeyError, ValueError) as e:
            logger.warning(
                f"Failed to parse PATCH aspect for {aspect_name}: {e}. Passing through unchanged."
            )
            return None

    def _add_attribution_to_value(
        self, aspect_name: str, value: Any, attribution_field: str
    ) -> Any:
        """Add attribution to a value (item or map of items)."""
        if isinstance(value, dict):
            # Check if this is a map of items (upsert mode) or a single item
            # If keys look like URNs, it's likely a map
            if any(key.startswith("urn:li:") for key in value if isinstance(key, str)):
                # Map of items - add attribution to each
                updated_map = {}
                for key, item_value in value.items():
                    if isinstance(item_value, dict):
                        existing_attribution = item_value.get(attribution_field)
                        attribution = self._build_attribution(
                            None, existing_attribution
                        )
                        updated_item = item_value.copy()
                        updated_item[attribution_field] = attribution
                        updated_map[key] = updated_item
                    else:
                        updated_map[key] = item_value
                return updated_map
            else:
                # Single item - add attribution
                existing_attribution = value.get(attribution_field)
                attribution = self._build_attribution(None, existing_attribution)
                updated_value = value.copy()
                updated_value[attribution_field] = attribution
                return updated_value
        return value

    def _convert_aspect_to_patch_mcp(
        self,
        entity_urn: str,
        entity_type: str,
        aspect_name: str,
        aspect: Any,
        system_metadata: Optional[Any] = None,
        audit_header: Optional[Any] = None,
    ) -> MetadataChangeProposalClass:
        """Convert an aspect to a PATCH MCP with attribution."""
        # Extract items from aspect
        items = self._extract_items_from_aspect(aspect_name, aspect)

        # Create patch operations
        patch_ops = self._create_patch_ops_for_aspect(aspect_name, items)

        # Build array primary keys
        array_primary_keys = {
            ASPECT_CONFIG[aspect_name]["array_field"]: self._get_array_primary_keys(
                aspect_name
            )
        }

        # Create GenericJsonPatch
        generic_json_patch = GenericJsonPatch(
            array_primary_keys=array_primary_keys,
            patch=patch_ops,
            force_generic_patch=False,
        )

        # Create MCP
        return MetadataChangeProposalClass(
            entityUrn=entity_urn,
            entityType=entity_type,
            changeType=ChangeTypeClass.PATCH,
            aspectName=aspect_name,
            aspect=generic_json_patch.to_generic_aspect(),
            systemMetadata=system_metadata,
            auditHeader=audit_header,
        )

    def transform(
        self, record_envelopes: Iterable[RecordEnvelope]
    ) -> Iterable[RecordEnvelope]:
        """Transform records by converting UPSERT aspects to PATCH with attribution."""
        for envelope in record_envelopes:
            if isinstance(envelope.record, EndOfStream):
                yield envelope
                continue

            # Check if this is a supported aspect
            aspect_name: Optional[str] = None
            entity_urn: Optional[str] = None
            entity_type: Optional[str] = None
            aspect: Optional[Any] = None
            system_metadata: Optional[Any] = None
            audit_header: Optional[Any] = None

            if isinstance(envelope.record, MetadataChangeProposalWrapper):
                # Handle MCPW
                mcpw = envelope.record
                if mcpw.aspectName not in SUPPORTED_ASPECTS:
                    yield envelope
                    continue

                aspect_name = mcpw.aspectName
                entity_urn = mcpw.entityUrn
                entity_type = mcpw.entityType
                aspect = mcpw.aspect
                system_metadata = mcpw.systemMetadata
                audit_header = mcpw.auditHeader

                # If already PATCH, add attribution to existing patch operations
                if mcpw.changeType == ChangeTypeClass.PATCH:
                    if entity_urn and entity_type:
                        patch_mcp = self._add_attribution_to_patch_mcp(
                            entity_urn=entity_urn,
                            entity_type=entity_type,
                            aspect_name=aspect_name,
                            patch_aspect=aspect,
                            system_metadata=system_metadata,
                            audit_header=audit_header,
                        )
                        if patch_mcp:
                            record_metadata = _update_work_unit_id(
                                envelope=envelope,
                                urn=entity_urn,
                                aspect_name=aspect_name,
                            )
                            yield RecordEnvelope(
                                record=patch_mcp, metadata=record_metadata
                            )
                        else:
                            # If we can't parse the patch, pass through unchanged
                            yield envelope
                    else:
                        # Missing required fields, pass through unchanged
                        yield envelope
                    continue

            elif isinstance(envelope.record, MetadataChangeProposalClass):
                # Handle MCP (for PATCH operations with GenericAspectClass)
                mcp = envelope.record
                if mcp.aspectName not in SUPPORTED_ASPECTS:
                    yield envelope
                    continue

                aspect_name = mcp.aspectName
                entity_urn = mcp.entityUrn
                entity_type = mcp.entityType
                aspect = mcp.aspect
                system_metadata = mcp.systemMetadata
                audit_header = mcp.auditHeader if hasattr(mcp, "auditHeader") else None

                # If already PATCH, add attribution to existing patch operations
                if mcp.changeType == ChangeTypeClass.PATCH:
                    if entity_urn and entity_type:
                        patch_mcp = self._add_attribution_to_patch_mcp(
                            entity_urn=entity_urn,
                            entity_type=entity_type,
                            aspect_name=aspect_name,
                            patch_aspect=aspect,
                            system_metadata=system_metadata,
                            audit_header=audit_header,
                        )
                        if patch_mcp:
                            record_metadata = _update_work_unit_id(
                                envelope=envelope,
                                urn=entity_urn,
                                aspect_name=aspect_name,
                            )
                            yield RecordEnvelope(
                                record=patch_mcp, metadata=record_metadata
                            )
                        else:
                            # If we can't parse the patch, pass through unchanged
                            yield envelope
                    else:
                        # Missing required fields, pass through unchanged
                        yield envelope
                    continue

            elif isinstance(envelope.record, MetadataChangeEventClass):
                # Handle MCE
                mce = envelope.record
                if not mce.proposedSnapshot:
                    yield envelope
                    continue

                # Extract aspect from MCE
                snapshot = mce.proposedSnapshot
                entity_urn = snapshot.urn
                entity_type = guess_entity_type(entity_urn)

                # Check each supported aspect
                for supported_aspect in SUPPORTED_ASPECTS:
                    aspect_type = ASPECT_MAP.get(supported_aspect)
                    if aspect_type:
                        aspect_obj = builder.get_aspect_if_available(mce, aspect_type)
                        if aspect_obj:
                            aspect_name = supported_aspect
                            aspect = aspect_obj
                            system_metadata = mce.systemMetadata
                            # MCE doesn't have auditHeader, use None
                            audit_header = None
                            break

                if not aspect_name:
                    yield envelope
                    continue
            else:
                # Not a supported record type, pass through
                yield envelope
                continue

            # Convert to PATCH MCP
            if aspect_name and entity_urn and entity_type and aspect:
                patch_mcp = self._convert_aspect_to_patch_mcp(
                    entity_urn=entity_urn,
                    entity_type=entity_type,
                    aspect_name=aspect_name,
                    aspect=aspect,
                    system_metadata=system_metadata,
                    audit_header=audit_header,
                )

                # Update envelope metadata
                record_metadata = _update_work_unit_id(
                    envelope=envelope,
                    urn=entity_urn,
                    aspect_name=aspect_name,
                )

                # Create new envelope with PATCH MCP
                yield RecordEnvelope(
                    record=patch_mcp,
                    metadata=record_metadata,
                )
            else:
                # Something went wrong, pass through unchanged
                yield envelope
