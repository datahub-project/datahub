from typing_extensions import Self

from datahub.emitter.mcp_patch_builder import MetadataPatchProposal
from datahub.metadata.schema_classes import (
    InstitutionalMemoryClass,
    InstitutionalMemoryMetadataClass,
)

_ELEMENTS_APK = {"elements": ["url"]}
_ELEMENTS_BY_URL_AND_LABEL_APK = {"elements": ["url", "description"]}


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

    def add_institutional_memory_by_url_and_label(
        self, element: InstitutionalMemoryMetadataClass
    ) -> Self:
        """Adds the link keyed by url and label (``description``), keeping other labels at that url."""
        self._add_patch(
            InstitutionalMemoryClass.ASPECT_NAME,
            "add",
            path=("elements", element.url, element.description),
            value=element,
            array_primary_keys=_ELEMENTS_BY_URL_AND_LABEL_APK,
        )
        return self

    def remove_institutional_memory_by_url_and_label(
        self, url: str, label: str
    ) -> Self:
        """Removes only the link with this url and label (``description``)."""
        self._add_patch(
            InstitutionalMemoryClass.ASPECT_NAME,
            "remove",
            path=("elements", url, label),
            value={},
            array_primary_keys=_ELEMENTS_BY_URL_AND_LABEL_APK,
        )
        return self
