from typing import Generic, List, Optional, TypeVar

from datahub.emitter.mcp_patch_builder import MetadataPatchProposal
from datahub.metadata.schema_classes import (
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
)

T = TypeVar("T", bound=MetadataPatchProposal)


class OwnershipPatchHelper(Generic[T]):
    def __init__(self, parent: T) -> None:
        self._parent = parent
        self.aspect_field = OwnershipClass.ASPECT_NAME

    def parent(self) -> T:
        return self._parent

    def add_owner(self, owner: OwnerClass) -> "OwnershipPatchHelper":
        self._parent._add_patch(
            OwnershipClass.ASPECT_NAME,
            "add",
            path=f"/owners/{owner.owner}/{owner.type}",
            value=owner,
        )
        return self

    def remove_owner(
        self, owner: str, owner_type: Optional[OwnershipTypeClass] = None
    ) -> "OwnershipPatchHelper":
        """
        param: owner_type is optional
        """
        self._parent._add_patch(
            OwnershipClass.ASPECT_NAME,
            "remove",
            path=f"/owners/{owner}" + (f"/{owner_type}" if owner_type else ""),
            value=owner,
        )
        return self

    def set_owners(self, owners: List[OwnerClass]) -> "OwnershipPatchHelper":
        self._parent._add_patch(
            OwnershipClass.ASPECT_NAME, "add", path="/owners", value=owners
        )
        return self
