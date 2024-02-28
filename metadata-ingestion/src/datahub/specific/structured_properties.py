from typing import Generic, List, TypeVar, Union

from datahub.emitter.mcp_patch_builder import MetadataPatchProposal
from datahub.metadata.schema_classes import StructuredPropertyValueAssignmentClass
from datahub.utilities.urns.structured_properties_urn import (
    make_structured_property_urn,
)

_Parent = TypeVar("_Parent", bound=MetadataPatchProposal)


class StructuredPropertiesPatchHelper(Generic[_Parent]):
    def __init__(
        self,
        parent: _Parent,
        aspect_name: str = "structuredProperties",
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
