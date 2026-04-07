from typing import List, Optional, Union

from typing_extensions import Self

from datahub.emitter.mcp_patch_builder import (
    UNIT_SEPARATOR,
    MetadataPatchProposal,
    determine_array_primary_keys,
)
from datahub.metadata.schema_classes import (
    StructuredPropertiesClass,
    StructuredPropertyValueAssignmentClass,
)
from datahub.metadata.urns import Urn
from datahub.utilities.urns.structured_properties_urn import (
    make_structured_property_urn,
)

_PROPERTIES_KEY_FIELDS = ["propertyUrn", "values", f"attribution{UNIT_SEPARATOR}source"]


class HasStructuredPropertiesPatch(MetadataPatchProposal):
    def set_structured_property(
        self,
        key: str,
        value: Union[str, float, List[Union[str, float]]],
        attribution_source: Optional[Union[str, Urn]] = None,
    ) -> Self:
        """Add or update a structured property.

        Args:
            key: the name of the property (either bare or urn form)
            value: the value of the property (for multi-valued properties, this can be a list)

        Returns:
            The patch builder instance.
        """
        property_urn = make_structured_property_urn(key)
        source = str(attribution_source) if attribution_source is not None else ""
        self._add_patch(
            StructuredPropertiesClass.ASPECT_NAME,
            "add",
            path=("properties", property_urn, source),
            value=StructuredPropertyValueAssignmentClass(
                propertyUrn=property_urn,
                values=value if isinstance(value, list) else [value],
            ),
        )
        return self

    def remove_structured_property(
        self,
        key: str,
        attribution_source: Optional[Union[str, Urn]] = None,
    ) -> Self:
        """Remove a structured property.

        Args:
            key: the name of the property (either bare or urn form)
            attribution_source: When set, only that source's entry is removed.
                When omitted, all entries for this property URN are removed.

        Returns:
            The patch builder instance.
        """
        property_urn = make_structured_property_urn(key)
        source = str(attribution_source) if attribution_source is not None else None
        path, array_primary_keys = determine_array_primary_keys(
            field_name="properties",
            default_key_fields=_PROPERTIES_KEY_FIELDS,
            path=[property_urn, source],
        )
        self._add_patch(
            StructuredPropertiesClass.ASPECT_NAME,
            "remove",
            path=("properties", *path),
            value={},
            array_primary_keys=array_primary_keys,
        )
        return self

    def add_structured_property(
        self,
        key: str,
        value: Union[str, float, List[Union[str, float]]],
        attribution_source: Optional[Union[str, Urn]] = None,
    ) -> Self:
        """Add a structured property.

        Currently equivalent to set_structured_property: overwrites all values for the given property.

        Args:
            key: the name of the property (either bare or urn form)
            value: the value of the property (for multi-valued properties, this value will be appended to the list)

        Returns:
            The patch builder instance.
        """
        return self.set_structured_property(key, value, attribution_source)

    def set_structured_property_manual(
        self, property: StructuredPropertyValueAssignmentClass
    ) -> Self:
        """Add or update a structured property, using a StructuredPropertyValueAssignmentClass object."""
        source = (
            property.attribution.source
            if (property.attribution and property.attribution.source)
            else ""
        )
        # JSON Patch `add` replaces an existing value at the same path, so no
        # explicit remove is needed. See set_structured_property for details.
        self._add_patch(
            StructuredPropertiesClass.ASPECT_NAME,
            "add",
            path=("properties", property.propertyUrn, source),
            value=property,
        )
        return self

    def add_structured_property_manual(
        self, property: StructuredPropertyValueAssignmentClass
    ) -> Self:
        """Add a structured property, using a StructuredPropertyValueAssignmentClass object.

        Currently equivalent to set_structured_property_manual: overwrites all values for the given property.
        """
        return self.set_structured_property_manual(property)
