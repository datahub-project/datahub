import json
import logging
from typing import Any, Dict, Iterable, List, Literal, Optional, Union, cast

from pydantic.fields import Field
from typing_extensions import assert_never

import datahub.emitter.mce_builder as builder
from datahub.configuration.common import ConfigModel
from datahub.emitter.aspect import ASPECT_MAP, JSON_PATCH_CONTENT_TYPE
from datahub.emitter.mce_builder import get_sys_time
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
    GlossaryTermAssociationClass,
    GlossaryTermsClass,
    KafkaAuditHeaderClass,
    MetadataChangeEventClass,
    MetadataChangeProposalClass,
    OwnerClass,
    OwnershipClass,
    SystemMetadataClass,
    TagAssociationClass,
)
from datahub.utilities.urns.urn import guess_entity_type

logger = logging.getLogger(__name__)

# Supported aspects configuration
SUPPORTED_ASPECTS = ["globalTags", "ownership", "glossaryTerms"]

# Mapping of aspect names to their field structure configuration.
# This mapping is needed because each aspect has different field names for:
# - The array field containing items (e.g., "tags" in GlobalTags, "owners" in Ownership)
# - The identifier field within each item (e.g., "tag" in TagAssociation, "urn" in GlossaryTermAssociation)
# - The attribution field name (all use "attribution" but this centralizes the configuration)
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

    attribution_source: str = Field(
        description="Attribution source URN, e.g., 'urn:li:platformResource:ingestion'"
    )
    actor: Optional[str] = Field(
        default="urn:li:corpuser:datahub",
        description="Actor URN for attribution metadata",
    )
    source_detail: Optional[Dict[str, str]] = Field(
        default=None,
        description="Optional sourceDetail map for MetadataAttribution",
    )
    patch_mode: bool = Field(
        default=False,
        description="If False (upsert mode), replaces all items for the attribution source. If True (patch mode), creates patch operations within the scope of the attribution source.",
    )


class SetAttributionTransformer(BaseTransformer):
    """
    Transformer that converts UPSERT aspects (globalTags, ownership, glossaryTerms)
    into PATCH operations with attribution metadata.

    **Inputs:**
    - MetadataChangeProposalWrapper (MCPW): Supports both UPSERT and PATCH changeTypes
    - MetadataChangeProposalClass (MCP): Supports both UPSERT and PATCH changeTypes
    - MetadataChangeEventClass (MCE): Always UPSERT (MCEs are inherently upsert operations)

    **Outputs:**
    - Always produces MetadataChangeProposalClass (MCP) with:
      - changeType: PATCH
      - aspect: GenericAspectClass containing GenericJsonPatch serialized as JSON

    **Modes:**
    This transformer supports two modes controlled by the `patch_mode` configuration:
    - Default (upsert mode, patch_mode=False): Replaces all items for a given attribution source
      with a single PATCH operation containing a map of all items
    - Patch mode (patch_mode=True): Creates patch operations within the scope of the attribution source,
      allowing incremental updates

    **Behavior:**
    - For UPSERT inputs: Converts the aspect to a PATCH operation with attribution metadata
    - For PATCH inputs: Adds attribution to existing patch operations while preserving the original
      patch semantics and structure

    **Supported aspects:**
    - globalTags: Adds attribution to TagAssociation items
    - ownership: Adds attribution to Owner items
    - glossaryTerms: Adds attribution to GlossaryTermAssociation items

    The transformer works at the MCP/MCPW/MCE level to change changeType from UPSERT
    to PATCH, which requires working at this abstraction level rather than the
    Aspect level, so SingleAspectTransformer and MultipleAspectTransformer mixins are not used.

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
        # TODO: Make entity_types configurable via SetAttributionConfig to allow filtering
        # by specific entity types (e.g., ["dataset", "dashboard"]) instead of processing all.
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
            "time": get_sys_time(),
            "actor": self.config.actor,
            "source": self.config.attribution_source,
        }

        if self.config.source_detail:
            attribution["sourceDetail"] = self.config.source_detail

        return attribution

    def _extract_items_from_aspect(
        self,
        aspect_name: Literal["globalTags", "ownership", "glossaryTerms"],
        aspect: Union[GlobalTagsClass, OwnershipClass, GlossaryTermsClass],
    ) -> List[Union[TagAssociationClass, OwnerClass, GlossaryTermAssociationClass]]:
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
            assert_never(aspect_name)

    def _get_item_id(
        self,
        aspect_name: Literal["globalTags", "ownership", "glossaryTerms"],
        item: Union[TagAssociationClass, OwnerClass, GlossaryTermAssociationClass],
    ) -> str:
        """Get the identifier field value from an item."""
        config = ASPECT_CONFIG[aspect_name]
        item_id_field = config["item_id_field"]

        # Access the field - items are RecordTemplate objects with required fields
        # Type system guarantees item has the correct field for its aspect type
        assert hasattr(item, item_id_field), (
            f"Item from {aspect_name} must have {item_id_field} field, "
            f"got type: {type(item).__name__}"
        )
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

    def _build_item_with_attribution(
        self,
        aspect_name: Literal["globalTags", "ownership", "glossaryTerms"],
        item: Union[TagAssociationClass, OwnerClass, GlossaryTermAssociationClass],
    ) -> Dict[str, Any]:
        """
        Build item dictionary with attribution metadata.

        Args:
            aspect_name: The aspect name (e.g., 'globalTags', 'ownership', 'glossaryTerms')
            item: An item from the aspect's array field:
                - For globalTags: TagAssociationClass instance
                - For ownership: OwnerClass instance
                - For glossaryTerms: GlossaryTermAssociationClass instance
                These are all DictWrapper objects that have a to_obj() method.

        Returns:
            A dictionary representation of the item with attribution metadata added
        """
        config = ASPECT_CONFIG[aspect_name]
        attribution_field = config["attribution_field"]

        # Convert item to dict
        # Items are DictWrapper objects (TagAssociationClass, OwnerClass, GlossaryTermAssociationClass)
        # which always have to_obj() method
        assert hasattr(item, "to_obj"), (
            f"Item from {aspect_name} must have to_obj() method, "
            f"got type: {type(item).__name__}"
        )
        item_dict = item.to_obj()

        # Get existing attribution if present
        existing_attribution = item_dict.get(attribution_field)

        # Build new attribution
        attribution = self._build_attribution(item, existing_attribution)

        # Update item dict with attribution
        item_dict[attribution_field] = attribution

        return item_dict

    def _create_patch_ops_for_aspect(
        self,
        aspect_name: Literal["globalTags", "ownership", "glossaryTerms"],
        items: List[
            Union[TagAssociationClass, OwnerClass, GlossaryTermAssociationClass]
        ],
    ) -> List[_Patch]:
        """Create patch operations for items in an aspect."""
        config = ASPECT_CONFIG[aspect_name]
        array_field = config["array_field"]

        if self.config.patch_mode:
            # Patch mode: operations within the scope of the attribution source
            if not items:
                return []  # No items, no operations in patch mode
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

    def _get_array_primary_keys(
        self, aspect_name: Literal["globalTags", "ownership", "glossaryTerms"]
    ) -> List[str]:
        """Get array primary keys for an aspect."""
        config = ASPECT_CONFIG[aspect_name]
        item_id_field = config["item_id_field"]
        # Use UNIT_SEPARATOR for nested path in attribution.source
        return [f"attribution{UNIT_SEPARATOR}source", item_id_field]

    def _add_attribution_to_patch_mcp(
        self,
        entity_urn: str,
        entity_type: str,
        aspect_name: Literal["globalTags", "ownership", "glossaryTerms"],
        patch_aspect: Any,
        system_metadata: Optional[SystemMetadataClass] = None,
        audit_header: Optional[KafkaAuditHeaderClass] = None,
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
                    # Add attribution to the patch operation value
                    # The value is either a single item (dict) or a map of items (dict with URN keys)
                    if isinstance(value, dict):
                        updated_value = self._add_attribution_to_patch_operation_value(
                            aspect_name, value, attribution_field
                        )
                        new_patch_ops.append(
                            _Patch(op=op, path=path_tuple, value=updated_value)
                        )
                    else:
                        # Keep original value if we can't process it (shouldn't happen for our aspects)
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

    def _add_attribution_to_patch_operation_value(
        self,
        aspect_name: Literal["globalTags", "ownership", "glossaryTerms"],
        patch_operation_value: Dict[str, Any],
        attribution_field: str,
    ) -> Dict[str, Any]:
        """
        Add attribution to a JSON Patch operation value.

        The patch operation value can be:
        - A single item (dict with item fields like 'tag', 'owner', etc.)
        - A map of items (dict where keys are item IDs/URNs and values are item dicts)

        Args:
            aspect_name: The aspect name (e.g., 'globalTags', 'ownership', 'glossaryTerms')
            patch_operation_value: The value from a JSON Patch "add" operation (must be a dict)
            attribution_field: The field name where attribution should be added (e.g., 'attribution')

        Returns:
            The updated value dict with attribution added
        """
        assert isinstance(patch_operation_value, dict), (
            "patch_operation_value must be a dict"
        )
        # Check if this is a map of items (upsert mode) or a single item
        # If keys look like URNs, it's likely a map
        urn_keys = [
            key
            for key in patch_operation_value
            if isinstance(key, str) and key.startswith("urn:li:")
        ]
        if urn_keys:
            # Map of items - add attribution to each
            logger.debug(
                f"Detected map of items for {aspect_name} (keys look like URNs): "
                f"{len(urn_keys)} URN keys found, total {len(patch_operation_value)} keys"
            )
            updated_map = {}
            for key, item_value in patch_operation_value.items():
                if isinstance(item_value, dict):
                    existing_attribution = item_value.get(attribution_field)
                    attribution = self._build_attribution(None, existing_attribution)
                    updated_item = item_value.copy()
                    updated_item[attribution_field] = attribution
                    updated_map[key] = updated_item
                else:
                    updated_map[key] = item_value
            return updated_map
        else:
            # Single item - add attribution
            logger.debug(
                f"Detected single item for {aspect_name} (no URN-like keys found, "
                f"total {len(patch_operation_value)} keys)"
            )
            existing_attribution = patch_operation_value.get(attribution_field)
            attribution = self._build_attribution(None, existing_attribution)
            updated_value = patch_operation_value.copy()
            updated_value[attribution_field] = attribution
            return updated_value

    def _convert_aspect_to_patch_mcp(
        self,
        entity_urn: str,
        entity_type: str,
        aspect_name: Literal["globalTags", "ownership", "glossaryTerms"],
        aspect: Union[GlobalTagsClass, OwnershipClass, GlossaryTermsClass],
        system_metadata: Optional[SystemMetadataClass] = None,
        audit_header: Optional[KafkaAuditHeaderClass] = None,
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
                logger.debug("Received EndOfStream, passing through")
                yield envelope
                continue

            # Check if this is a supported aspect
            aspect_name: Optional[
                Literal["globalTags", "ownership", "glossaryTerms"]
            ] = None
            entity_urn: Optional[str] = None
            entity_type: Optional[str] = None
            aspect: Optional[
                Union[GlobalTagsClass, OwnershipClass, GlossaryTermsClass]
            ] = None
            system_metadata: Optional[SystemMetadataClass] = None
            audit_header: Optional[KafkaAuditHeaderClass] = None

            if isinstance(envelope.record, MetadataChangeProposalWrapper):
                # Handle MCPW
                mcpw = envelope.record
                logger.debug(
                    f"Processing MCPW: entity={mcpw.entityUrn}, "
                    f"aspect={mcpw.aspectName}, changeType={mcpw.changeType}"
                )
                if mcpw.aspectName not in SUPPORTED_ASPECTS:
                    logger.debug(
                        f"Aspect '{mcpw.aspectName}' in MCPW is not supported by this transformer, passing through"
                    )
                    yield envelope
                    continue

                # Type narrowing: aspect_name is guaranteed to be in SUPPORTED_ASPECTS
                aspect_name = cast(
                    Literal["globalTags", "ownership", "glossaryTerms"], mcpw.aspectName
                )
                entity_urn = mcpw.entityUrn
                entity_type = mcpw.entityType
                # Type narrowing: aspect is guaranteed to be one of the supported aspects
                aspect = cast(
                    Union[GlobalTagsClass, OwnershipClass, GlossaryTermsClass],
                    mcpw.aspect,
                )
                system_metadata = mcpw.systemMetadata
                audit_header = mcpw.auditHeader

                # If already PATCH, add attribution to existing patch operations
                if mcpw.changeType == ChangeTypeClass.PATCH:
                    logger.debug(
                        f"MCPW is already PATCH for {aspect_name}, adding attribution to existing patch"
                    )
                    if entity_urn and entity_type:
                        # Type narrowing: aspect_name is guaranteed to be in SUPPORTED_ASPECTS
                        aspect_name_literal = cast(
                            Literal["globalTags", "ownership", "glossaryTerms"],
                            aspect_name,
                        )
                        patch_mcp = self._add_attribution_to_patch_mcp(
                            entity_urn=entity_urn,
                            entity_type=entity_type,
                            aspect_name=aspect_name_literal,
                            patch_aspect=aspect,
                            system_metadata=system_metadata,
                            audit_header=audit_header,
                        )
                        if patch_mcp:
                            logger.debug(
                                f"Successfully added attribution to PATCH MCPW for {aspect_name}"
                            )
                            record_metadata = _update_work_unit_id(
                                envelope=envelope,
                                urn=entity_urn,
                                aspect_name=str(aspect_name),
                            )
                            yield RecordEnvelope(
                                record=patch_mcp, metadata=record_metadata
                            )
                        else:
                            # If we can't parse the patch, pass through unchanged
                            logger.warning(
                                f"Failed to parse PATCH aspect for {aspect_name} in MCPW, passing through unchanged"
                            )
                            yield envelope
                    else:
                        # Missing required fields, pass through unchanged
                        logger.warning(
                            f"Missing required fields in MCPW (entity_urn={entity_urn}, "
                            f"entity_type={entity_type}), passing through unchanged"
                        )
                        yield envelope
                    continue
                else:
                    logger.debug(
                        f"MCPW is UPSERT for {aspect_name}, will convert to PATCH"
                    )

            elif isinstance(envelope.record, MetadataChangeProposalClass):
                # Handle MCP (for PATCH operations with GenericAspectClass)
                mcp = envelope.record
                logger.debug(
                    f"Processing MCP: entity={mcp.entityUrn}, "
                    f"aspect={mcp.aspectName}, changeType={mcp.changeType}"
                )
                if mcp.aspectName not in SUPPORTED_ASPECTS:
                    logger.debug(
                        f"Aspect '{mcp.aspectName}' in MCP is not supported by this transformer, passing through"
                    )
                    yield envelope
                    continue

                # Type narrowing: aspect_name is guaranteed to be in SUPPORTED_ASPECTS
                aspect_name = cast(
                    Literal["globalTags", "ownership", "glossaryTerms"], mcp.aspectName
                )
                entity_urn = mcp.entityUrn
                entity_type = mcp.entityType
                # For MCP with PATCH, aspect is GenericAspectClass, not a typed aspect
                # We'll handle it in _add_attribution_to_patch_mcp
                # For UPSERT, aspect could be a typed aspect, but we'll convert it to PATCH
                aspect = None  # type: ignore[assignment]
                system_metadata = mcp.systemMetadata
                audit_header = mcp.auditHeader if hasattr(mcp, "auditHeader") else None

                # If already PATCH, add attribution to existing patch operations
                if mcp.changeType == ChangeTypeClass.PATCH:
                    logger.debug(
                        f"MCP is already PATCH for {aspect_name}, adding attribution to existing patch"
                    )
                    if entity_urn and entity_type:
                        # Type narrowing: aspect_name is guaranteed to be in SUPPORTED_ASPECTS
                        aspect_name_literal = cast(
                            Literal["globalTags", "ownership", "glossaryTerms"],
                            aspect_name,
                        )
                        # For PATCH MCP, aspect is GenericAspectClass
                        patch_mcp = self._add_attribution_to_patch_mcp(
                            entity_urn=entity_urn,
                            entity_type=entity_type,
                            aspect_name=aspect_name_literal,
                            patch_aspect=mcp.aspect,
                            system_metadata=system_metadata,
                            audit_header=audit_header,
                        )
                        if patch_mcp:
                            logger.debug(
                                f"Successfully added attribution to PATCH MCP for {aspect_name}"
                            )
                            record_metadata = _update_work_unit_id(
                                envelope=envelope,
                                urn=entity_urn,
                                aspect_name=str(aspect_name),
                            )
                            yield RecordEnvelope(
                                record=patch_mcp, metadata=record_metadata
                            )
                        else:
                            # If we can't parse the patch, pass through unchanged
                            logger.warning(
                                f"Failed to parse PATCH aspect for {aspect_name} in MCP, passing through unchanged"
                            )
                            yield envelope
                    else:
                        # Missing required fields, pass through unchanged
                        logger.warning(
                            f"Missing required fields in MCP (entity_urn={entity_urn}, "
                            f"entity_type={entity_type}), passing through unchanged"
                        )
                        yield envelope
                    continue
                else:
                    logger.debug(
                        f"MCP is UPSERT for {aspect_name}, will convert to PATCH"
                    )

            elif isinstance(envelope.record, MetadataChangeEventClass):
                # Handle MCE
                mce = envelope.record
                logger.debug("Processing MCE")
                if not mce.proposedSnapshot:
                    logger.warning(
                        "MCE has no proposedSnapshot, passing through unchanged"
                    )
                    yield envelope
                    continue

                # Extract aspect from MCE
                snapshot = mce.proposedSnapshot
                entity_urn = snapshot.urn
                entity_type = guess_entity_type(entity_urn)
                logger.debug(f"MCE entity={entity_urn}, entity_type={entity_type}")

                # Check each supported aspect
                for supported_aspect in SUPPORTED_ASPECTS:
                    aspect_type = ASPECT_MAP.get(supported_aspect)
                    if aspect_type:
                        aspect_obj = builder.get_aspect_if_available(mce, aspect_type)
                        if aspect_obj:
                            # Type narrowing: supported_aspect is guaranteed to be in SUPPORTED_ASPECTS
                            aspect_name = cast(
                                Literal["globalTags", "ownership", "glossaryTerms"],
                                supported_aspect,
                            )
                            # Type narrowing: aspect_obj is guaranteed to be one of the supported aspects
                            aspect = cast(
                                Union[
                                    GlobalTagsClass, OwnershipClass, GlossaryTermsClass
                                ],
                                aspect_obj,
                            )
                            system_metadata = mce.systemMetadata
                            # MCE doesn't have auditHeader, use None
                            audit_header = None
                            logger.debug(
                                f"Found {aspect_name} aspect in MCE, will convert to PATCH"
                            )
                            break

                if not aspect_name:
                    logger.debug(
                        f"No aspects supported by this transformer found in MCE for entity {entity_urn}, passing through"
                    )
                    yield envelope
                    continue
            else:
                # Not a supported record type, pass through
                logger.debug(
                    f"Record type {type(envelope.record).__name__} is not supported by this transformer, passing through"
                )
                yield envelope
                continue

            # Convert to PATCH MCP
            if aspect_name and entity_urn and entity_type and aspect:
                logger.debug(
                    f"Converting UPSERT {aspect_name} to PATCH MCP for entity {entity_urn}"
                )
                # Type narrowing: aspect_name is guaranteed to be in SUPPORTED_ASPECTS
                aspect_name_literal = cast(
                    Literal["globalTags", "ownership", "glossaryTerms"], aspect_name
                )
                patch_mcp = self._convert_aspect_to_patch_mcp(
                    entity_urn=entity_urn,
                    entity_type=entity_type,
                    aspect_name=aspect_name_literal,
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
                logger.debug(
                    f"Successfully converted {aspect_name} to PATCH MCP for entity {entity_urn}"
                )
                yield RecordEnvelope(
                    record=patch_mcp,
                    metadata=record_metadata,
                )
            else:
                # Something went wrong, pass through unchanged
                logger.warning(
                    f"Missing required fields for conversion (aspect_name={aspect_name}, "
                    f"entity_urn={entity_urn}, entity_type={entity_type}, aspect={aspect is not None}), "
                    f"passing through unchanged"
                )
                yield envelope
