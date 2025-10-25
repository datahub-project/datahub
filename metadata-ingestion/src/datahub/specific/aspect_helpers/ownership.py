from typing import List, Optional, Union

from typing_extensions import Self

from datahub.emitter.mcp_patch_builder import MetadataPatchProposal
from datahub.metadata.schema_classes import (
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
)


class HasOwnershipPatch(MetadataPatchProposal):
    base_path = ["owners"]

    def add_owner(self, owner: OwnerClass) -> Self:
        """Add an owner to the entity.

        Args:
            owner: The Owner object to add.

        Returns:
            The patch builder instance.
        """

        path = [
            *self.base_path,
            owner.owner,
            str(owner.type),
            "None" if owner.typeUrn is None else owner.typeUrn,
        ]

        self._add_patch(
            OwnershipClass.ASPECT_NAME,
            "add",
            path=tuple(path),
            value=owner,
        )
        return self

    def remove_owner(
        self,
        owner: str,
        owner_type: Optional[Union[str, OwnershipTypeClass]] = None,
        owner_type_urn: Optional[str] = None,
    ) -> Self:
        """Remove an owner from the entity.

        If owner_type is not provided, the owner will be removed regardless of ownership type.

        Args:
            owner: The owner to remove.
            owner_type: The ownership type of the owner (optional).
            owner_type_urn: The ownership type urn of the owner (optional).

        Returns:
            The patch builder instance.
        """

        path = [*self.base_path, owner]
        if owner_type:
            path.append(str(owner_type))
        if owner_type_urn:
            path.append(owner_type_urn)
        self._add_patch(
            OwnershipClass.ASPECT_NAME,
            "remove",
            path=tuple(path),
            value=owner,
        )
        return self

    def set_owners(self, owners: List[OwnerClass]) -> Self:
        """Set the owners of the entity.

        This will effectively replace all existing owners with the new list - it doesn't really patch things.

        Args:
            owners: The list of owners to set.

        Returns:
            The patch builder instance.
        """
        self._add_patch(
            OwnershipClass.ASPECT_NAME, "add", path=tuple(self.base_path), value=owners
        )
        return self
