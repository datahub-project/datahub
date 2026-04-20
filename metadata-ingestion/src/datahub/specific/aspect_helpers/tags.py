from typing import Optional, Union

from typing_extensions import Self

from datahub.emitter.mcp_patch_builder import (
    UNIT_SEPARATOR,
    MetadataPatchProposal,
    determine_array_primary_keys,
)
from datahub.metadata.schema_classes import (
    GlobalTagsClass as GlobalTags,
    TagAssociationClass as Tag,
)
from datahub.metadata.urns import TagUrn, Urn

DEFAULT_TAG_KEY_FIELDS = ["tag", f"attribution{UNIT_SEPARATOR}source"]


class HasTagsPatch(MetadataPatchProposal):
    def add_tag(self, tag: Tag) -> Self:
        """Adds a tag to the entity.

        Uses compound-key semantics keyed by ``(attribution.source, tag)``.
        Unattributed tags use ``source=""``; attributed tags use the actual source.

        Args:
            tag: The Tag object representing the tag to be added.

        Returns:
            The patch builder instance.
        """
        source = (
            tag.attribution.source if tag.attribution and tag.attribution.source else ""
        )
        self._add_patch(
            GlobalTags.ASPECT_NAME,
            "add",
            path=("tags", tag.tag, source),
            value=tag,
        )
        return self

    def remove_tag(
        self,
        tag: Union[str, Urn],
        attribution_source: Optional[Union[str, Urn]] = None,
    ) -> Self:
        """Removes a tag from the entity.

        Args:
            tag: The tag to remove, specified as a string or Urn object.
            attribution_source: When set, only the entry for that specific source is
                removed.  When omitted, ``*`` is used as the source component, which
                the backend expands to every source in the stored map — removing all
                entries for this tag URN without affecting other tag URNs.

        Returns:
            The patch builder instance.
        """
        if isinstance(tag, str) and not tag.startswith("urn:li:tag:"):
            tag = TagUrn.create_from_id(tag)
        source = str(attribution_source) if attribution_source is not None else None
        path, array_primary_keys = determine_array_primary_keys(
            field_name="tags",
            default_key_fields=DEFAULT_TAG_KEY_FIELDS,
            path=[str(tag), source],
        )
        self._add_patch(
            GlobalTags.ASPECT_NAME,
            "remove",
            path=("tags", *path),
            value={},
            array_primary_keys=array_primary_keys,
        )

        return self
