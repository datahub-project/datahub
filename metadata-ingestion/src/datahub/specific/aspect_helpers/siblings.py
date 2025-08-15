from typing import List

from typing_extensions import Self

from datahub.emitter.mcp_patch_builder import MetadataPatchProposal
from datahub.metadata.com.linkedin.pegasus2avro.common import Siblings


class HasSiblingsPatch(MetadataPatchProposal):
    def add_sibling(self, sibling_urn: str, primary: bool = False) -> Self:
        """Add a sibling relationship to the entity.

        Args:
            sibling_urn: The URN of the sibling entity to add.
            primary: Whether this entity should be marked as primary in the relationship.

        Returns:
            The patch builder instance.
        """
        self._add_patch(
            Siblings.ASPECT_NAME,
            "add",
            path=("siblings", sibling_urn),
            value=sibling_urn,
        )

        # Set primary flag if specified
        if primary:
            self._add_patch(
                Siblings.ASPECT_NAME,
                "add",
                path=("primary",),
                value=primary,
            )

        return self

    def remove_sibling(self, sibling_urn: str) -> Self:
        """Remove a sibling relationship from the entity.

        Args:
            sibling_urn: The URN of the sibling entity to remove.

        Returns:
            The patch builder instance.
        """
        self._add_patch(
            Siblings.ASPECT_NAME,
            "remove",
            path=("siblings", sibling_urn),
            value={},
        )
        return self

    def set_siblings(self, sibling_urns: List[str], primary: bool = False) -> Self:
        """Set the complete list of siblings for the entity.

        This will replace all existing siblings with the new list.

        Args:
            sibling_urns: The list of sibling URNs to set.
            primary: Whether this entity should be marked as primary.

        Returns:
            The patch builder instance.
        """
        self._add_patch(
            Siblings.ASPECT_NAME, "add", path=("siblings",), value=sibling_urns
        )

        self._add_patch(Siblings.ASPECT_NAME, "add", path=("primary",), value=primary)

        return self
