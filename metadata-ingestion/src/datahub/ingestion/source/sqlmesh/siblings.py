from typing import Iterable

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.sqlmesh.base import SqlmeshSourceBase
from datahub.metadata.schema_classes import SiblingsClass
from datahub.specific.dataset import DatasetPatchBuilder


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
