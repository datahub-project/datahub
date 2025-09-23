from typing import Union

from typing_extensions import Self

from datahub.emitter.mcp_patch_builder import MetadataPatchProposal
from datahub.metadata.schema_classes import (
    GlossaryTermAssociationClass as Term,
    GlossaryTermsClass,
)
from datahub.metadata.urns import GlossaryTermUrn, Urn


class HasTermsPatch(MetadataPatchProposal):
    def add_term(self, term: Term) -> Self:
        """Adds a glossary term to the entity.

        Args:
            term: The Term object representing the glossary term to be added.

        Returns:
            The patch builder instance.
        """
        # TODO: Make this support raw strings, in addition to Term objects.
        self._add_patch(
            GlossaryTermsClass.ASPECT_NAME, "add", path=("terms", term.urn), value=term
        )
        return self

    def remove_term(self, term: Union[str, Urn]) -> Self:
        """Removes a glossary term from the entity.

        Args:
            term: The term to remove, specified as a string or Urn object.

        Returns:
            The patch builder instance.
        """
        if isinstance(term, str) and not term.startswith("urn:li:glossaryTerm:"):
            term = GlossaryTermUrn(term)
        self._add_patch(
            GlossaryTermsClass.ASPECT_NAME, "remove", path=("terms", term), value={}
        )
        return self
