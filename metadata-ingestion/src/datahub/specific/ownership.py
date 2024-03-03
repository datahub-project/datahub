from typing import Generic, List, Optional, TypeVar

from datahub.emitter.mcp_patch_builder import MetadataPatchProposal
from datahub.metadata.schema_classes import (
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
)

_Parent = TypeVar("_Parent", bound=MetadataPatchProposal)


class OwnershipPatchHelper(Generic[_Parent]):
    def __init__(self, parent: _Parent) -> None:
        self._parent = parent
        self.aspect_field = OwnershipClass.ASPECT_NAME

    def parent(self) -> _Parent:
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
