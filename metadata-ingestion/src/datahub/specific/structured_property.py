from typing import Dict, List, Optional, Union

from datahub.emitter.mcp_patch_builder import MetadataPatchProposal
from datahub.metadata.schema_classes import (
    KafkaAuditHeaderClass,
    PropertyValueClass,
    StructuredPropertyDefinitionClass as StructuredPropertyDefinition,
    SystemMetadataClass,
)
from datahub.utilities.urns.urn import Urn


# This patch builder is for structured property entities. Not for the aspect on assets.
class StructuredPropertyPatchBuilder(MetadataPatchProposal):
    def __init__(
        self,
        urn: str,
        system_metadata: Optional[SystemMetadataClass] = None,
        audit_header: Optional[KafkaAuditHeaderClass] = None,
    ) -> None:
        super().__init__(
            urn, system_metadata=system_metadata, audit_header=audit_header
        )

    # can only be used when creating a new structured property
    def set_qualified_name(
        self, qualified_name: str
    ) -> "StructuredPropertyPatchBuilder":
        self._add_patch(
            StructuredPropertyDefinition.ASPECT_NAME,
            "add",
            path=("qualifiedName",),
            value=qualified_name,
        )
        return self

    def set_display_name(
        self, display_name: Optional[str] = None
    ) -> "StructuredPropertyPatchBuilder":
        if display_name is not None:
            self._add_patch(
                StructuredPropertyDefinition.ASPECT_NAME,
                "add",
                path=("displayName",),
                value=display_name,
            )
        return self

    # can only be used when creating a new structured property
    def set_value_type(
        self, value_type: Union[str, Urn]
    ) -> "StructuredPropertyPatchBuilder":
        self._add_patch(
            StructuredPropertyDefinition.ASPECT_NAME,
            "add",
            path=("valueType",),
            value=value_type,
        )
        return self

    # can only be used when creating a new structured property
    def set_type_qualifier(
        self, type_qualifier: Optional[Dict[str, List[str]]] = None
    ) -> "StructuredPropertyPatchBuilder":
        if type_qualifier is not None:
            self._add_patch(
                StructuredPropertyDefinition.ASPECT_NAME,
                "add",
                path=("typeQualifier",),
                value=type_qualifier,
            )
        return self

    # can only be used when creating a new structured property
    def add_allowed_value(
        self, allowed_value: PropertyValueClass
    ) -> "StructuredPropertyPatchBuilder":
        self._add_patch(
            StructuredPropertyDefinition.ASPECT_NAME,
            "add",
            path=("allowedValues", str(allowed_value.get("value"))),
            value=allowed_value,
        )
        return self

    def set_cardinality(self, cardinality: str) -> "StructuredPropertyPatchBuilder":
        self._add_patch(
            StructuredPropertyDefinition.ASPECT_NAME,
            "add",
            path=("cardinality",),
            value=cardinality,
        )
        return self

    def add_entity_type(
        self, entity_type: Union[str, Urn]
    ) -> "StructuredPropertyPatchBuilder":
        self._add_patch(
            StructuredPropertyDefinition.ASPECT_NAME,
            "add",
            path=("entityTypes", str(entity_type)),
            value=entity_type,
        )
        return self

    def set_description(
        self, description: Optional[str] = None
    ) -> "StructuredPropertyPatchBuilder":
        if description is not None:
            self._add_patch(
                StructuredPropertyDefinition.ASPECT_NAME,
                "add",
                path=("description",),
                value=description,
            )
        return self

    def set_immutable(self, immutable: bool) -> "StructuredPropertyPatchBuilder":
        self._add_patch(
            StructuredPropertyDefinition.ASPECT_NAME,
            "add",
            path=("immutable",),
            value=immutable,
        )
        return self
