from typing import Union

from typing_extensions import Self

from datahub.emitter.mcp_patch_builder import MetadataPatchProposal
from datahub.metadata.schema_classes import (
    GlobalTagsClass as GlobalTags,
    TagAssociationClass as Tag,
)
from datahub.metadata.urns import TagUrn, Urn


class HasTagsPatch(MetadataPatchProposal):
    def add_tag(self, tag: Tag) -> Self:
        """Adds a tag to the entity.

        Args:
            tag: The Tag object representing the tag to be added.

        Returns:
            The patch builder instance.
        """

        # TODO: Make this support raw strings, in addition to Tag objects.
        self._add_patch(
            GlobalTags.ASPECT_NAME, "add", path=("tags", tag.tag), value=tag
        )
        return self

    def remove_tag(self, tag: Union[str, Urn]) -> Self:
        """Removes a tag from the entity.

        Args:
            tag: The tag to remove, specified as a string or Urn object.

        Returns:
            The patch builder instance.
        """
        if isinstance(tag, str) and not tag.startswith("urn:li:tag:"):
            tag = TagUrn.create_from_id(tag)
        self._add_patch(GlobalTags.ASPECT_NAME, "remove", path=("tags", tag), value={})
        return self
