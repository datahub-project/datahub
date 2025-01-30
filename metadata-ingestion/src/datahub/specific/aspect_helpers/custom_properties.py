from abc import abstractmethod
from typing import Dict, Optional, Tuple

from typing_extensions import Self

from datahub.emitter.mcp_patch_builder import MetadataPatchProposal, PatchPath


class HasCustomPropertiesPatch(MetadataPatchProposal):
    @classmethod
    @abstractmethod
    def _custom_properties_location(self) -> Tuple[str, PatchPath]: ...

    def add_custom_property(self, key: str, value: str) -> Self:
        """Add a custom property to the entity.

        Args:
            key: The key of the custom property.
            value: The value of the custom property.

        Returns:
            The patch builder instance.
        """
        aspect_name, path = self._custom_properties_location()
        self._add_patch(
            aspect_name,
            "add",
            path=(*path, key),
            value=value,
        )
        return self

    def add_custom_properties(
        self, custom_properties: Optional[Dict[str, str]] = None
    ) -> Self:
        if custom_properties is not None:
            for key, value in custom_properties.items():
                self.add_custom_property(key, value)
        return self

    def remove_custom_property(self, key: str) -> Self:
        """Remove a custom property from the entity.

        Args:
            key: The key of the custom property to remove.

        Returns:
            The patch builder instance.
        """
        aspect_name, path = self._custom_properties_location()
        self._add_patch(
            aspect_name,
            "remove",
            path=(*path, key),
            value={},
        )
        return self

    def set_custom_properties(self, custom_properties: Dict[str, str]) -> Self:
        """Sets the custom properties of the entity.

        This method replaces all existing custom properties with the given dictionary.

        Args:
            custom_properties: A dictionary containing the custom properties to be set.

        Returns:
            The patch builder instance.
        """

        aspect_name, path = self._custom_properties_location()
        self._add_patch(
            aspect_name,
            "add",
            path=path,
            value=custom_properties,
        )
        return self
