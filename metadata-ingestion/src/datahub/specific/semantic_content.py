from typing import Any

from typing_extensions import Self

from datahub.emitter.mcp_patch_builder import MetadataPatchProposal
from datahub.metadata.schema_classes import SemanticContentClass


class SemanticContentPatchBuilder(MetadataPatchProposal):
    """Patches one embedding profile and leaves every other profile in the aspect."""

    def set_profile(self, profile_key: str, profile: Any) -> Self:
        self._add_patch(
            SemanticContentClass.ASPECT_NAME,
            "add",
            path=("embeddings", profile_key),
            value=profile,
        )
        return self

    def remove_profile(self, profile_key: str) -> Self:
        self._add_patch(
            SemanticContentClass.ASPECT_NAME,
            "remove",
            path=("embeddings", profile_key),
            value={},
        )
        return self

    def set_profile_field(self, profile_key: str, field: str, value: Any) -> Self:
        self._add_patch(
            SemanticContentClass.ASPECT_NAME,
            "add",
            path=("embeddings", profile_key, field),
            value=value,
        )
        return self

    def clear_legacy_skip(self) -> Self:
        """Remove the entity-wide skip marker.

        The server ignores either remove when that field is already absent, so a document
        that was never skipped still accepts the profile write.
        """
        for field in ("skipReason", "skippedAt"):
            self._add_patch(
                SemanticContentClass.ASPECT_NAME,
                "remove",
                path=(field,),
                value=None,
            )
        return self
