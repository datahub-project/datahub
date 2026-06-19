import logging
from typing import Dict, List, Optional

from datahub.emitter.mce_builder import make_dataset_urn_with_platform_instance
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.cube.config import CubeSourceConfig
from datahub.ingestion.source.cube.constants import CUBE_PLATFORM
from datahub.ingestion.source.cube.models import (
    CubeColumnReference,
    CubeEntity,
    CubeMember,
    ResolvedWarehouseTable,
)
from datahub.metadata.schema_classes import (
    DatasetLineageTypeClass,
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
    UpstreamClass,
    UpstreamLineageClass,
)
from datahub.metadata.urns import SchemaFieldUrn
from datahub.sql_parsing._models import _TableName
from datahub.sql_parsing.schema_resolver import (
    SchemaInfo,
    SchemaResolver,
    match_columns_to_schema,
)
from datahub.sql_parsing.sqlglot_lineage import (
    create_and_cache_schema_resolver,
    create_lineage_sql_parsed_result,
)

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
        self._schema_resolver: Optional[SchemaResolver] = None
        self._resolver_ready = False
        self._table_resolution: Dict[str, ResolvedWarehouseTable] = {}

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

    def _get_resolver(self) -> Optional[SchemaResolver]:
        # None without a graph: there is no ingested schema to reconcile against.
        if not self._resolver_ready:
            self._resolver_ready = True
            if self.ctx.graph is not None and self.warehouse_platform:
                self._schema_resolver = create_and_cache_schema_resolver(
                    platform=self.warehouse_platform,
                    env=self.config.warehouse_env,
                    graph=self.ctx.graph,
                    platform_instance=self.config.warehouse_platform_instance,
                )
        return self._schema_resolver

    def _resolve_table(self, ref: CubeColumnReference) -> ResolvedWarehouseTable:
        # Snap the table URN and column casing to the warehouse's ingested
        # schema so Cube's identifiers (e.g. Snowflake upper, Postgres lower)
        # don't dangle. Falls back to configured lowercasing when unresolved.
        cache_key = ref.table_name(self.warehouse_database)
        if cache_key in self._table_resolution:
            return self._table_resolution[cache_key]

        resolver = self._get_resolver()
        if resolver is not None:
            urn, schema_info = resolver.resolve_table(
                _TableName(
                    database=self.warehouse_database,
                    db_schema=ref.schema_name,
                    table=ref.table,
                )
            )
            # Trust the resolver's URN only when it found a schema.
            if schema_info is None:
                urn = self._warehouse_urn(cache_key)
            resolved = ResolvedWarehouseTable(urn=urn, schema_info=schema_info)
        else:
            resolved = ResolvedWarehouseTable(urn=self._warehouse_urn(cache_key))

        self._table_resolution[cache_key] = resolved
        return resolved

    def _resolve_columns(
        self, schema_info: Optional[SchemaInfo], columns: List[str]
    ) -> List[str]:
        if schema_info:
            return match_columns_to_schema(schema_info, columns)
        return [self._warehouse_column(column) for column in columns]

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
            return [self._resolve_table(ref).urn for ref in entity.table_references]
        if entity.sql and self.config.parse_sql_for_lineage:
            return self._parse_sql_tables(entity)
        return []

    def _add_warehouse_column_lineage(
        self,
        entity: CubeEntity,
        downstream_urn: str,
        upstream_urns: List[str],
        fine_grained: List[FineGrainedLineageClass],
    ) -> None:
        if not (self.warehouse_platform and self.config.include_column_lineage):
            return
        members = entity.visible_members(self.config.include_hidden)

        # Cube Core's /v1/meta gives no per-member column references. As a
        # fallback, match member names to the columns of the cube's single
        # upstream table (Cube's convention: a member's name is its column);
        # members without a matching column (e.g. aggregate measures) are
        # skipped. Cloud carries explicit references, so this only runs when none
        # are present.
        has_explicit_refs = any(m.column_references for m in members)
        base = None if has_explicit_refs else self._single_warehouse_schema(entity)

        for member in members:
            upstream_fields: List[str] = []
            if member.column_references:
                for ref in member.column_references:
                    if not ref.column:
                        continue
                    resolved = self._resolve_table(ref)
                    upstream_urns.append(resolved.urn)
                    (resolved_column,) = self._resolve_columns(
                        resolved.schema_info, [ref.column]
                    )
                    upstream_fields.append(
                        SchemaFieldUrn(resolved.urn, resolved_column).urn()
                    )
            elif base is not None and base.schema_info is not None:
                column = self._match_member_to_column(base.schema_info, member.name)
                if column is not None:
                    upstream_urns.append(base.urn)
                    upstream_fields.append(SchemaFieldUrn(base.urn, column).urn())
            if upstream_fields:
                fine_grained.append(
                    self._field_lineage(downstream_urn, member, upstream_fields)
                )

    def _single_warehouse_schema(
        self, entity: CubeEntity
    ) -> Optional[ResolvedWarehouseTable]:
        # The cube's single upstream warehouse table, only when its schema is
        # known in DataHub. Used to name-match Core members to columns.
        urns = self._warehouse_table_urns(entity)
        if len(urns) != 1:
            return None
        resolver = self._get_resolver()
        if resolver is None:
            return None
        _, schema_info = resolver.resolve_urn(urns[0])
        if not schema_info:
            return None
        return ResolvedWarehouseTable(urn=urns[0], schema_info=schema_info)

    @staticmethod
    def _match_member_to_column(
        schema_info: SchemaInfo, member_name: str
    ) -> Optional[str]:
        by_lower = {column.lower(): column for column in schema_info}
        return by_lower.get(member_name.lower())

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
