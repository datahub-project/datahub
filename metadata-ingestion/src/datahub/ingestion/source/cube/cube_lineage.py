import logging
from typing import Dict, List, Optional

from datahub.emitter.mce_builder import make_dataset_urn_with_platform_instance
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.cube.config import CubeSourceConfig
from datahub.ingestion.source.cube.constants import CUBE_PLATFORM
from datahub.ingestion.source.cube.models import CubeEntity, CubeMember
from datahub.metadata.schema_classes import (
    DatasetLineageTypeClass,
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
    UpstreamClass,
    UpstreamLineageClass,
)
from datahub.metadata.urns import SchemaFieldUrn
from datahub.sql_parsing.sqlglot_lineage import create_lineage_sql_parsed_result

logger = logging.getLogger(__name__)


class CubeLineageBuilder:
    def __init__(
        self,
        config: CubeSourceConfig,
        ctx: PipelineContext,
        warehouse_platform: Optional[str],
        warehouse_database: Optional[str],
    ):
        self.config = config
        self.ctx = ctx
        self.warehouse_platform = warehouse_platform
        self.warehouse_database = warehouse_database
        self._sql_tables_cache: Dict[str, List[str]] = {}

    def _cube_urn(self, cube_name: str) -> str:
        return make_dataset_urn_with_platform_instance(
            platform=CUBE_PLATFORM,
            name=cube_name,
            platform_instance=self.config.platform_instance,
            env=self.config.env,
        )

    def _warehouse_urn(self, table_name: str) -> str:
        assert self.warehouse_platform is not None
        if self.config.convert_lineage_urns_to_lowercase:
            table_name = table_name.lower()
        return make_dataset_urn_with_platform_instance(
            platform=self.warehouse_platform,
            name=table_name,
            platform_instance=self.config.warehouse_platform_instance,
            env=self.config.warehouse_env,
        )

    def _warehouse_column(self, column: str) -> str:
        return (
            column.lower() if self.config.convert_lineage_urns_to_lowercase else column
        )

    def build(self, entity: CubeEntity) -> Optional[UpstreamLineageClass]:
        if not self.config.ingest_lineage:
            return None

        upstream_urns: List[str] = []
        fine_grained: List[FineGrainedLineageClass] = []
        downstream_urn = self._cube_urn(entity.name)

        upstream_urns.extend(self._warehouse_table_urns(entity))
        self._add_warehouse_column_lineage(
            entity, downstream_urn, upstream_urns, fine_grained
        )
        self._add_cube_reference_lineage(
            entity, downstream_urn, upstream_urns, fine_grained
        )

        deduped = list(dict.fromkeys(upstream_urns))
        if not deduped and not fine_grained:
            return None

        return UpstreamLineageClass(
            upstreams=[
                UpstreamClass(dataset=urn, type=DatasetLineageTypeClass.TRANSFORMED)
                for urn in deduped
            ],
            fineGrainedLineages=fine_grained or None,
        )

    def _warehouse_table_urns(self, entity: CubeEntity) -> List[str]:
        # Resolves the cube's upstream warehouse tables: directly from the Cloud
        # Metadata API references, or by parsing the Core cube SQL.
        if not self.warehouse_platform:
            return []
        if entity.table_references:
            return [
                self._warehouse_urn(ref.table_name(self.warehouse_database))
                for ref in entity.table_references
            ]
        if entity.sql and self.config.parse_sql_for_lineage:
            return self._parse_sql_tables(entity)
        return []

    def single_warehouse_table_urn(self, entity: CubeEntity) -> Optional[str]:
        # A cube is a 1:1 projection of a warehouse table only when it is not a
        # view, references no other cubes, and resolves to exactly one table.
        if entity.is_view or entity.cube_references:
            return None
        urns = list(dict.fromkeys(self._warehouse_table_urns(entity)))
        return urns[0] if len(urns) == 1 else None

    def _add_warehouse_column_lineage(
        self,
        entity: CubeEntity,
        downstream_urn: str,
        upstream_urns: List[str],
        fine_grained: List[FineGrainedLineageClass],
    ) -> None:
        if not (self.warehouse_platform and self.config.include_column_lineage):
            return
        for member in entity.visible_members(self.config.include_hidden):
            upstream_fields: List[str] = []
            for ref in member.column_references:
                if not ref.column:
                    continue
                table_urn = self._warehouse_urn(ref.table_name(self.warehouse_database))
                upstream_urns.append(table_urn)
                upstream_fields.append(
                    SchemaFieldUrn(table_urn, self._warehouse_column(ref.column)).urn()
                )
            if upstream_fields:
                fine_grained.append(
                    self._field_lineage(downstream_urn, member, upstream_fields)
                )

    def _add_cube_reference_lineage(
        self,
        entity: CubeEntity,
        downstream_urn: str,
        upstream_urns: List[str],
        fine_grained: List[FineGrainedLineageClass],
    ) -> None:
        for cube_name in entity.cube_references:
            upstream_urns.append(self._cube_urn(cube_name))

        if not self.config.include_column_lineage:
            return

        for member in entity.visible_members(self.config.include_hidden):
            upstream_fields: List[str] = []
            for ref in member.member_references:
                cube_name, _, member_name = ref.partition(".")
                if not member_name:
                    continue
                cube_urn = self._cube_urn(cube_name)
                upstream_urns.append(cube_urn)
                upstream_fields.append(SchemaFieldUrn(cube_urn, member_name).urn())
            if upstream_fields:
                fine_grained.append(
                    self._field_lineage(downstream_urn, member, upstream_fields)
                )

    def _parse_sql_tables(self, entity: CubeEntity) -> List[str]:
        # Cube Core only: parse the cube SQL to recover upstream warehouse tables.
        if entity.name in self._sql_tables_cache:
            return self._sql_tables_cache[entity.name]
        assert self.warehouse_platform is not None and entity.sql is not None

        # The warehouse platform drives the sqlglot dialect (via the canonical
        # get_dialect_str mapping) rather than a generic dialect, so dialect
        # quirks (e.g. mssql->tsql, athena->trino) are handled correctly.
        tables: List[str] = []
        try:
            result = create_lineage_sql_parsed_result(
                query=entity.sql,
                default_db=self.warehouse_database,
                platform=self.warehouse_platform,
                platform_instance=self.config.warehouse_platform_instance,
                env=self.config.warehouse_env,
                graph=self.ctx.graph,
                schema_aware=self.ctx.graph is not None,
                generate_column_lineage=False,
            )
            if result.debug_info.error:
                logger.debug(
                    f"SQL parsing for cube {entity.name} reported: {result.debug_info.error}"
                )
            tables = list(result.in_tables)
        except Exception as e:
            logger.warning(f"Failed to parse SQL for cube {entity.name}: {e}")

        self._sql_tables_cache[entity.name] = tables
        return tables

    @staticmethod
    def _field_lineage(
        downstream_urn: str, member: CubeMember, upstream_fields: List[str]
    ) -> FineGrainedLineageClass:
        return FineGrainedLineageClass(
            upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
            downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
            upstreams=upstream_fields,
            downstreams=[SchemaFieldUrn(downstream_urn, member.name).urn()],
        )
