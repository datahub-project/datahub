from abc import abstractmethod
from typing import List, Tuple

from typing_extensions import Self

from datahub.emitter.mcp_patch_builder import MetadataPatchProposal, PatchPath
from datahub.metadata.schema_classes import (
    FineGrainedLineageClass as FineGrainedLineage,
)


class HasFineGrainedLineagePatch(MetadataPatchProposal):
    @abstractmethod
    def _fine_grained_lineage_location(self) -> Tuple[str, PatchPath]:
        """Return the aspect name where fine-grained lineage is stored."""
        raise NotImplementedError("Subclasses must implement this method.")

    @staticmethod
    def _get_fine_grained_key(
        fine_grained_lineage: FineGrainedLineage,
    ) -> Tuple[str, str, str]:
        downstreams = fine_grained_lineage.downstreams or []
        if len(downstreams) != 1:
            raise TypeError("Cannot patch with more or less than one downstream.")
        transform_op = fine_grained_lineage.transformOperation or "NONE"
        downstream_urn = downstreams[0]
        query_id = fine_grained_lineage.query or "NONE"
        return transform_op, downstream_urn, query_id

    def add_fine_grained_lineage(
        self, fine_grained_lineage: FineGrainedLineage
    ) -> Self:
        aspect_name, path = self._fine_grained_lineage_location()
        (
            transform_op,
            downstream_urn,
            query_id,
        ) = self._get_fine_grained_key(fine_grained_lineage)
        for upstream_urn in fine_grained_lineage.upstreams or []:
            self._add_patch(
                aspect_name,
                "add",
                path=(*path, transform_op, downstream_urn, query_id, upstream_urn),
                value={"confidenceScore": fine_grained_lineage.confidenceScore},
            )
        return self

    def remove_fine_grained_lineage(
        self, fine_grained_lineage: FineGrainedLineage
    ) -> Self:
        aspect_name, path = self._fine_grained_lineage_location()
        (
            transform_op,
            downstream_urn,
            query_id,
        ) = self._get_fine_grained_key(fine_grained_lineage)
        for upstream_urn in fine_grained_lineage.upstreams or []:
            self._add_patch(
                aspect_name,
                "remove",
                path=(*path, transform_op, downstream_urn, query_id, upstream_urn),
                value={},
            )
        return self

    def set_fine_grained_lineages(
        self, fine_grained_lineages: List[FineGrainedLineage]
    ) -> Self:
        aspect_name, path = self._fine_grained_lineage_location()
        self._add_patch(
            aspect_name,
            "add",
            path=path,
            value=fine_grained_lineages,
        )
        return self
