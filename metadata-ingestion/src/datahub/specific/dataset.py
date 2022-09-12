from typing import List, Optional

from datahub.emitter.mcp_patch_builder import MetadataPatchProposal
from datahub.metadata.schema_classes import (
    OwnerClass as Owner,
    OwnershipTypeClass,
    UpstreamClass as Upstream,
)
from datahub.utilities.urns.urn import Urn


class DatasetPatchBuilder(MetadataPatchProposal):
    def __init__(self, urn: str) -> None:
        super().__init__(urn, "dataset")

    def add_owner(self, owner: Owner) -> "DatasetPatchBuilder":
        self._add_patch(
            "ownership", "add", path=f"/owners/{owner.owner}/{owner.type}", value=owner
        )
        return self

    def remove_owner(
        self, owner: Urn, owner_type: Optional[OwnershipTypeClass] = None
    ) -> "DatasetPatchBuilder":
        """
        param: owner_type is optional
        """
        self._add_patch(
            "ownership",
            "remove",
            path=f"/owners/{owner}" + (f"/{owner_type}" if owner_type else ""),
            value=owner,
        )
        return self

    def set_owners(self, owners: List[Owner]) -> "DatasetPatchBuilder":
        self._add_patch("ownership", "replace", path="/owners", value=owners)
        return self

    def add_upstream_lineage(self, upstream: Upstream) -> "DatasetPatchBuilder":
        self._add_patch(
            "upstreamLineage",
            "add",
            path=f"/upstreams/{upstream.dataset}",
            value=upstream,
        )
        return self

    def remove_upstream_lineage(self, upstream: Upstream) -> "DatasetPatchBuilder":
        self._add_patch(
            "upstreamLineage",
            "remove",
            path=f"/upstreams/{upstream.dataset}",
            value=upstream,
        )
        return self

    def set_upstream_lineages(self, upstreams: List[Upstream]) -> "DatasetPatchBuilder":
        self._add_patch(
            "upstreamLineage", "replace", path="/upstreams", value=upstreams
        )
        return self
