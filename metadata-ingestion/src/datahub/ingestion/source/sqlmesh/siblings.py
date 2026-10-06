from typing import TYPE_CHECKING, Iterable, Optional

from datahub.emitter.mce_builder import make_schema_field_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.sqlmesh.base import SqlmeshSourceBase
from datahub.ingestion.source.sqlmesh.models import _EffectiveProjectConfig
from datahub.metadata.schema_classes import (
    DatasetLineageTypeClass,
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
    SiblingsClass,
    UpstreamClass,
    UpstreamLineageClass,
)
from datahub.specific.dataset import DatasetPatchBuilder

if TYPE_CHECKING:
    from datahub.ingestion.source.sqlmesh.compat import SqlmeshModel


class SiblingsMixin(SqlmeshSourceBase):
    def _emit_siblings(
        self, sqlmesh_urn: str, warehouse_urn: str
    ) -> Iterable[MetadataWorkUnit]:
        """Link the SQLMesh entity and its warehouse counterpart as siblings.

        SQLMesh is primary by default (it owns the model definition, lineage and
        descriptions), matching dbt's ``dbt_is_primary_sibling=True``.

        The SQLMesh entity's aspect is written outright — this connector owns
        that entity. The warehouse entity is *patched* instead, so a sibling
        edge added by another connector (dbt, or a second SQLMesh project) isn't
        clobbered, and the workunit is marked non-authoritative because we are
        not the source of truth for warehouse metadata. Same split as dbt.
        """
        sqlmesh_is_primary = self.config.sqlmesh_is_primary_sibling

        # TODO: migrate to SDK V2 when SiblingsClass is supported
        yield MetadataChangeProposalWrapper(
            entityUrn=sqlmesh_urn,
            aspect=SiblingsClass(siblings=[warehouse_urn], primary=sqlmesh_is_primary),
        ).as_workunit()

        warehouse_patch = DatasetPatchBuilder(warehouse_urn)
        warehouse_patch.add_sibling(sqlmesh_urn, primary=not sqlmesh_is_primary)
        for mcp in warehouse_patch.build():
            yield MetadataWorkUnit(
                id=MetadataWorkUnit.generate_workunit_id(mcp),
                mcp_raw=mcp,
                is_primary_source=False,
            )

    def _emit_warehouse_links(
        self,
        sqlmesh_urn: str,
        warehouse_urn: str,
        model: "SqlmeshModel",
        effective: _EffectiveProjectConfig,
        is_external: bool,
    ) -> Iterable[MetadataWorkUnit]:
        """Everything that ties a SQLMesh entity to its warehouse table.

        Always the sibling link. A managed model also patches a ``model -> table``
        edge onto the warehouse table (what dbt does for its models), because the
        lineage graph draws siblings as separate nodes: without the edge, lineage
        from the table never reaches the model's upstreams. An external model's
        edge runs the other way and lives in its own upstreams, see
        ``_with_warehouse_upstream``.
        """
        yield from self._emit_siblings(sqlmesh_urn, warehouse_urn)
        if is_external or not self.config.include_lineage:
            return

        patch = DatasetPatchBuilder(warehouse_urn)
        patch.add_upstream_lineage(
            UpstreamClass(dataset=sqlmesh_urn, type=DatasetLineageTypeClass.COPY)
        )
        for column in self._warehouse_edge_columns(model, effective):
            patch.add_fine_grained_lineage(
                _column_copy(sqlmesh_urn, warehouse_urn, column)
            )
        # Patched and non-authoritative, like the sibling patch, so lineage the
        # warehouse connector stores on the table is kept.
        for mcp in patch.build():
            yield MetadataWorkUnit(
                id=MetadataWorkUnit.generate_workunit_id(mcp),
                mcp_raw=mcp,
                is_primary_source=False,
            )

    def _with_warehouse_upstream(
        self,
        upstreams: Optional[UpstreamLineageClass],
        sqlmesh_urn: str,
        warehouse_urn: str,
        model: "SqlmeshModel",
        effective: _EffectiveProjectConfig,
    ) -> UpstreamLineageClass:
        """An external model's upstreams plus its warehouse table.

        An external model stands for a warehouse table SQLMesh reads but doesn't
        build (dbt's "source"), so the table is its upstream. Kept even with
        ``skip_external_models_in_lineage``, so the external entity's own lineage
        reaches the table; the UI's ``hideSqlmeshSourceInLineage`` folds the
        entity into the table, as ``hideDbtSourceInLineage`` does for dbt.
        """
        column_edges = [
            _column_copy(warehouse_urn, sqlmesh_urn, column)
            for column in self._warehouse_edge_columns(model, effective)
        ]
        return UpstreamLineageClass(
            upstreams=[
                *(upstreams.upstreams if upstreams else []),
                UpstreamClass(dataset=warehouse_urn, type=DatasetLineageTypeClass.COPY),
            ],
            fineGrainedLineages=[
                *((upstreams.fineGrainedLineages or []) if upstreams else []),
                *column_edges,
            ]
            or None,
        )


def _column_copy(
    upstream_urn: str, downstream_urn: str, column: str
) -> FineGrainedLineageClass:
    """The same column, copied from one dataset to its counterpart."""
    return FineGrainedLineageClass(
        upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
        upstreams=[make_schema_field_urn(upstream_urn, column)],
        downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
        downstreams=[make_schema_field_urn(downstream_urn, column)],
    )
