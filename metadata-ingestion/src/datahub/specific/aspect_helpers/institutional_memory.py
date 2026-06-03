from typing_extensions import Self

from datahub.emitter.mcp_patch_builder import MetadataPatchProposal
from datahub.metadata.schema_classes import (
    InstitutionalMemoryClass,
    InstitutionalMemoryMetadataClass,
)

_ELEMENTS_APK = {"elements": ["url"]}


class HasInstitutionalMemoryPatch(MetadataPatchProposal):
    def add_institutional_memory(
        self, element: InstitutionalMemoryMetadataClass
    ) -> Self:
        """Adds a documentation link to the entity's institutional memory.

        Keyed by ``url``, so adding a link for the same URL replaces the
        existing entry while leaving other links untouched.

        Args:
            element: The ``InstitutionalMemoryMetadataClass`` to add.

        Returns:
            The patch builder instance.
        """
        self._add_patch(
            InstitutionalMemoryClass.ASPECT_NAME,
            "add",
            path=("elements", element.url),
            value=element,
            array_primary_keys=_ELEMENTS_APK,
        )
        return self
