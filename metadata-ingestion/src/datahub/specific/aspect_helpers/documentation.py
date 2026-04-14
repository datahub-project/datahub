from typing import Optional, Union

from typing_extensions import Self

from datahub.emitter.mcp_patch_builder import (
    UNIT_SEPARATOR,
    MetadataPatchProposal,
    determine_array_primary_keys,
)
from datahub.metadata.schema_classes import (
    DocumentationAssociationClass,
    DocumentationClass,
)
from datahub.metadata.urns import Urn

_DOCS_KEY_FIELDS = [f"attribution{UNIT_SEPARATOR}source"]


class HasDocumentationPatch(MetadataPatchProposal):
    def add_documentation(self, doc: DocumentationAssociationClass) -> Self:
        """Adds or replaces a documentation entry for this entity.

        The entry is keyed by ``doc.attribution.source``.  Adding a second
        entry for the same source upserts (replaces) the existing one; entries
        from other sources are untouched.  Unattributed entries use ``""`` as
        the source key.

        Args:
            doc: The ``DocumentationAssociationClass`` to add.

        Returns:
            The patch builder instance.
        """
        source = (
            doc.attribution.source
            if (doc.attribution and doc.attribution.source)
            else ""
        )
        self._add_patch(
            DocumentationClass.ASPECT_NAME,
            "add",
            path=("documentations", source),
            value=doc,
        )
        return self

    def remove_documentation(
        self,
        attribution_source: Optional[Union[str, Urn]] = None,
    ) -> Self:
        """Removes documentation entries from this entity.

        Args:
            attribution_source: When set, only the entry for that specific
                source is removed.  When omitted, all documentation entries
                are removed.

        Returns:
            The patch builder instance.
        """
        source = str(attribution_source) if attribution_source is not None else None
        path, array_primary_keys = determine_array_primary_keys(
            field_name="documentations",
            default_key_fields=_DOCS_KEY_FIELDS,
            path=[source],
        )
        self._add_patch(
            DocumentationClass.ASPECT_NAME,
            "remove",
            path=("documentations", *path),
            value={},
            array_primary_keys=array_primary_keys,
        )
        return self
