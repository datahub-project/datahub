from typing import Generic, List, Optional, TypeVar, Union

from datahub.emitter.mcp_patch_builder import MetadataPatchProposal
from datahub.metadata._schema_classes import KafkaAuditHeaderClass, SystemMetadataClass
from datahub.metadata.schema_classes import StructuredPropertyValueAssignmentClass
from datahub.utilities.urns.structured_properties_urn import (
    make_structured_property_urn,
)

_Parent = TypeVar("_Parent", bound=MetadataPatchProposal)


class StructuredPropertiesPatchHelper(Generic[_Parent]):
    DEFAULT_ASPECT_NAME = "structuredProperties"

    def __init__(
        self,
        parent: _Parent,
        aspect_name: str = DEFAULT_ASPECT_NAME,
    ) -> None:
        self.aspect_name = aspect_name
        self._parent = parent
        self.aspect_field = "properties"

    def parent(self) -> _Parent:
        return self._parent

    def set_property(
        self, key: str, value: Union[str, float, List[Union[str, float]]]
    ) -> "StructuredPropertiesPatchHelper":
        self.remove_property(key)
        self.add_property(key, value)
        return self

    def remove_property(self, key: str) -> "StructuredPropertiesPatchHelper":
        self._parent._add_patch(
            self.aspect_name,
            "remove",
            path=(self.aspect_field, make_structured_property_urn(key)),
            value={},
        )
        return self

    def add_property(
        self, key: str, value: Union[str, float, List[Union[str, float]]]
    ) -> "StructuredPropertiesPatchHelper":
        self._parent._add_patch(
            self.aspect_name,
            "add",
            path=(self.aspect_field, make_structured_property_urn(key)),
            value=StructuredPropertyValueAssignmentClass(
                propertyUrn=make_structured_property_urn(key),
                values=value if isinstance(value, list) else [value],
            ),
        )
        return self


class StructuredPropertiesAssetPatchBuilder(MetadataPatchProposal):
    """
    A Patch builder for the structuredProperties aspect of any asset.
    """

    def __init__(
        self,
        urn: str,
        system_metadata: Optional[SystemMetadataClass] = None,
        audit_header: Optional[KafkaAuditHeaderClass] = None,
    ) -> None:
        super().__init__(
            urn, system_metadata=system_metadata, audit_header=audit_header
        )
        self.structured_properties_patch_helper = StructuredPropertiesPatchHelper(self)

    def set_structured_property(
        self, property_name: str, value: Union[str, float, List[Union[str, float]]]
    ) -> "StructuredPropertiesAssetPatchBuilder":
        """
        This is a helper method to set a structured property.
        @param property_name: the name of the property (either bare or urn form)
        @param value: the value of the property (for multi-valued properties, this can be a list)
        """
        self.structured_properties_patch_helper.set_property(property_name, value)
        return self

    def add_structured_property(
        self, property_name: str, value: Union[str, float]
    ) -> "StructuredPropertiesAssetPatchBuilder":
        """
        This is a helper method to add a structured property.
        @param property_name: the name of the property (either bare or urn form)
        @param value: the value of the property (for multi-valued properties, this value will be appended to the list)
        """
        self.structured_properties_patch_helper.add_property(property_name, value)
        return self

    def remove_structured_property(
        self, property_name: str
    ) -> "StructuredPropertiesAssetPatchBuilder":
        """
        This is a helper method to remove a structured property.
        @param property_name: the name of the property (either bare or urn form)
        """
        self.structured_properties_patch_helper.remove_property(property_name)
        return self
