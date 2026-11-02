from typing import Optional, Tuple

from datahub.emitter.mcp_patch_builder import MetadataPatchProposal, PatchPath
from datahub.metadata.schema_classes import (
    DomainPropertiesClass as DomainProperties,
    KafkaAuditHeaderClass,
    SystemMetadataClass,
)
from datahub.specific.aspect_helpers.custom_properties import HasCustomPropertiesPatch
from datahub.specific.aspect_helpers.institutional_memory import (
    HasInstitutionalMemoryPatch,
)
from datahub.specific.aspect_helpers.ownership import HasOwnershipPatch
from datahub.specific.aspect_helpers.structured_properties import (
    HasStructuredPropertiesPatch,
)


class DomainPatchBuilder(
    HasOwnershipPatch,
    HasCustomPropertiesPatch,
    HasStructuredPropertiesPatch,
    HasInstitutionalMemoryPatch,
    MetadataPatchProposal,
):
    """Patch builder for the Domain entity."""

    def __init__(
        self,
        urn: str,
        system_metadata: Optional[SystemMetadataClass] = None,
        audit_header: Optional[KafkaAuditHeaderClass] = None,
    ) -> None:
        super().__init__(
            urn,
            system_metadata=system_metadata,
            audit_header=audit_header,
        )

    @classmethod
    def _custom_properties_location(cls) -> Tuple[str, PatchPath]:
        return DomainProperties.ASPECT_NAME, ("customProperties",)

    def set_name(self, name: str) -> "DomainPatchBuilder":
        self._add_patch(
            DomainProperties.ASPECT_NAME,
            "add",
            path=("name",),
            value=name,
        )
        return self

    def set_description(self, description: str) -> "DomainPatchBuilder":
        self._add_patch(
            DomainProperties.ASPECT_NAME,
            "add",
            path=("description",),
            value=description,
        )
        return self

    def set_parent_domain(self, parent_domain_urn: str) -> "DomainPatchBuilder":
        """Move this Domain under ``parent_domain_urn``."""
        if not parent_domain_urn:
            raise ValueError(
                "parent_domain_urn is required; use remove_parent_domain() to "
                "make the Domain top-level."
            )
        self._add_patch(
            DomainProperties.ASPECT_NAME,
            "add",
            path=("parentDomain",),
            value=parent_domain_urn,
        )
        return self

    def remove_parent_domain(self) -> "DomainPatchBuilder":
        """Make this Domain top-level.

        The JSON Patch ``remove`` fails when the Domain has no parent, so only
        call this for a Domain that has one.
        """
        self._add_patch(
            DomainProperties.ASPECT_NAME,
            "remove",
            path=("parentDomain",),
            value={},
        )
        return self
