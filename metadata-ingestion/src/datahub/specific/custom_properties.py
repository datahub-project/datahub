from typing import Generic, TypeVar

from datahub.emitter.mcp_patch_builder import MetadataPatchProposal

_Parent = TypeVar("_Parent", bound=MetadataPatchProposal)


class CustomPropertiesPatchHelper(Generic[_Parent]):
    def __init__(
        self,
        parent: _Parent,
        aspect_name: str,
    ) -> None:
        self.aspect_name = aspect_name
        self._parent = parent
        self.aspect_field = "customProperties"

    def parent(self) -> _Parent:
        return self._parent

    def add_property(self, key: str, value: str) -> "CustomPropertiesPatchHelper":
        self._parent._add_patch(
            self.aspect_name,
            "add",
            path=f"/{self.aspect_field}/{key}",
            value=value,
        )
        return self

    def remove_property(self, key: str) -> "CustomPropertiesPatchHelper":
        self._parent._add_patch(
            self.aspect_name,
            "remove",
            path=f"/{self.aspect_field}/{key}",
            value={},
        )
        return self
