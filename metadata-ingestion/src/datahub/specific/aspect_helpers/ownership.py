from typing import List, Optional, Union

from typing_extensions import Self

from datahub.emitter.mcp_patch_builder import (
    UNIT_SEPARATOR,
    MetadataPatchProposal,
    determine_array_primary_keys,
)
from datahub.metadata.schema_classes import (
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
)
from datahub.metadata.urns import Urn

_OWNER = "owner"
_TYPE = "type"
_TYPE_URN = "typeUrn"
_ATTRIBUTION_SOURCE = f"attribution{UNIT_SEPARATOR}source"

_DEFAULT_OWNERS_KEY_FIELDS = [_OWNER, _TYPE, _TYPE_URN, _ATTRIBUTION_SOURCE]


class HasOwnershipPatch(MetadataPatchProposal):
    def add_owner(self, owner: OwnerClass) -> Self:
        """Add an owner to the entity.

        Uses compound-key semantics keyed by ``(owner, type, typeUrn, attribution.source)``
        so that entries with the same owner and type enum but different ``typeUrn`` values
        coexist independently.

        Args:
            owner: The Owner object to add.

        Returns:
            The patch builder instance.
        """
        source = (
            owner.attribution.source
            if (owner.attribution and owner.attribution.source)
            else ""
        )
        type_str = str(owner.type) if owner.type else ""
        type_urn_str = str(owner.typeUrn) if owner.typeUrn else ""
        self._add_patch(
            OwnershipClass.ASPECT_NAME,
            "add",
            path=("owners", owner.owner, type_str, type_urn_str, source),
            value=owner,
        )
        return self

    def remove_owner(
        self,
        owner: str,
        owner_type: Optional[Union[str, OwnershipTypeClass]] = None,
        type_urn: Optional[Union[str, Urn]] = None,
        attribution_source: Optional[Union[str, Urn]] = None,
    ) -> Self:
        """Remove ownership entries from the entity.

        All filter parameters are optional wildcards:

        * ``owner_type`` – restrict to this ownership-type enum value (``None`` = any type).
        * ``type_urn`` – restrict to this ownership-type URN (``None`` = any typeUrn).
        * ``attribution_source`` – restrict to entries from this source (``None`` = any source).

        When all three are ``None``, a plain-key remove is used which removes **all**
        ownership entries for this owner without touching other owners.

        Args:
            owner: The owner URN to remove entries for.
            owner_type: Filter by ownership-type enum (optional).
            type_urn: Filter by ownership-type URN (optional).
            attribution_source: Filter by attribution source (optional).

        Returns:
            The patch builder instance.
        """
        source = str(attribution_source) if attribution_source is not None else None
        type_str = str(owner_type) if owner_type is not None else None
        type_urn_str = str(type_urn) if type_urn is not None else None
        path, array_primary_keys = determine_array_primary_keys(
            field_name="owners",
            default_key_fields=_DEFAULT_OWNERS_KEY_FIELDS,
            path=[owner, type_str, type_urn_str, source],
        )
        self._add_patch(
            OwnershipClass.ASPECT_NAME,
            "remove",
            path=("owners", *path),
            value={},
            array_primary_keys=array_primary_keys,
        )
        return self

    def set_owners(self, owners: List[OwnerClass]) -> Self:
        """Set the owners of the entity, replacing all existing owners.

        Args:
            owners: The list of owners to set.

        Returns:
            The patch builder instance.
        """
        self._add_patch(
            OwnershipClass.ASPECT_NAME, "add", path=("owners",), value=owners
        )
        return self
