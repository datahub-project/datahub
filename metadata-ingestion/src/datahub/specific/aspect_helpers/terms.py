from typing import Optional, Union

from typing_extensions import Self

from datahub.emitter.mcp_patch_builder import (
    UNIT_SEPARATOR,
    MetadataPatchProposal,
    determine_array_primary_keys,
)
from datahub.metadata.schema_classes import (
    GlossaryTermAssociationClass as Term,
    GlossaryTermsClass,
)
from datahub.metadata.urns import GlossaryTermUrn, Urn

DEFAULT_TERMS_KEY_FIELDS = ["urn", f"attribution{UNIT_SEPARATOR}source"]


class HasTermsPatch(MetadataPatchProposal):
    def add_term(self, term: Term) -> Self:
        """Adds a glossary term to the entity.

        Uses compound-key semantics keyed by ``(urn, attribution.source)``.
        Unattributed terms use ``source=""``; attributed terms use the actual source.

        Args:
            term: The Term object representing the glossary term to be added.

        Returns:
            The patch builder instance.
        """
        source = (
            term.attribution.source
            if (term.attribution and term.attribution.source)
            else ""
        )
        self._add_patch(
            GlossaryTermsClass.ASPECT_NAME,
            "add",
            path=("terms", term.urn, source),
            value=term,
        )
        return self

    def remove_term(
        self,
        term: Union[str, Urn],
        attribution_source: Optional[Union[str, Urn]] = None,
    ) -> Self:
        """Removes a glossary term from the entity.

        Args:
            term: The term to remove, specified as a string or Urn object.
            attribution_source: When set, only that source's entry is removed.
                When omitted, all entries for this term URN are removed.

        Returns:
            The patch builder instance.
        """
        if isinstance(term, str) and not term.startswith("urn:li:glossaryTerm:"):
            term = GlossaryTermUrn(term)
        source = str(attribution_source) if attribution_source is not None else None
        path, array_primary_keys = determine_array_primary_keys(
            field_name="terms",
            default_key_fields=DEFAULT_TERMS_KEY_FIELDS,
            path=[str(term), source],
        )
        self._add_patch(
            GlossaryTermsClass.ASPECT_NAME,
            "remove",
            path=("terms", *path),
            value={},
            array_primary_keys=array_primary_keys,
        )
        return self
