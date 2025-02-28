from typing import List, Union

from typing_extensions import Self

from datahub.emitter.mcp_patch_builder import MetadataPatchProposal
from datahub.metadata.schema_classes import (
    StructuredPropertiesClass,
    StructuredPropertyValueAssignmentClass,
)
from datahub.utilities.urns.structured_properties_urn import (
    make_structured_property_urn,
)


class HasStructuredPropertiesPatch(MetadataPatchProposal):
    def set_structured_property(
        self, key: str, value: Union[str, float, List[Union[str, float]]]
    ) -> Self:
        """Add or update a structured property.

        Args:
            key: the name of the property (either bare or urn form)
            value: the value of the property (for multi-valued properties, this can be a list)

        Returns:
            The patch builder instance.
        """
        self.remove_structured_property(key)
        self.add_structured_property(key, value)
        return self

    def remove_structured_property(self, key: str) -> Self:
        """Remove a structured property.

        Args:
            key: the name of the property (either bare or urn form)

        Returns:
            The patch builder instance.
        """

        self._add_patch(
            StructuredPropertiesClass.ASPECT_NAME,
            "remove",
            path=("properties", make_structured_property_urn(key)),
            value={},
        )
        return self

    def add_structured_property(
        self, key: str, value: Union[str, float, List[Union[str, float]]]
    ) -> Self:
        """Add a structured property.

        Args:
            key: the name of the property (either bare or urn form)
            value: the value of the property (for multi-valued properties, this value will be appended to the list)

        Returns:
            The patch builder instance.
        """

        self._add_patch(
            StructuredPropertiesClass.ASPECT_NAME,
            "add",
            path=("properties", make_structured_property_urn(key)),
            value=StructuredPropertyValueAssignmentClass(
                propertyUrn=make_structured_property_urn(key),
                values=value if isinstance(value, list) else [value],
            ),
        )
        return self
