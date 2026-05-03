import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Dict, List, Optional, Set, Tuple

from datahub.emitter.mce_builder import (
    make_dataset_urn_with_platform_instance,
    make_schema_field_urn,
)
from datahub.ingestion.source.hex.constants import CONNECTION_TYPE_TO_DATAHUB_PLATFORM
from datahub.ingestion.source.hex.model import SqlCell
from datahub.sql_parsing.schema_resolver import SchemaResolver
from datahub.sql_parsing.sql_parsing_common import get_dialect_str
from datahub.sql_parsing.sqlglot_lineage import sqlglot_lineage

if TYPE_CHECKING:
    from datahub.ingestion.graph.client import DataHubGraph

logger = logging.getLogger(__name__)


@dataclass
class SkippedCell:
    project_id: str
    cell_id: str
    cell_label: Optional[str]
    connection_id: str
    reason: str  # "unknown_connection_id" | "unknown_connection_type"


@dataclass
class LineageBuilderReport:
    sql_cells_attempted: int = 0
    sql_cells_succeeded: int = 0
    sql_cells_failed: int = 0
    sql_cells_no_upstreams: int = 0
    sql_cells_skipped_unknown_connection: int = 0
    upstream_datasets_found: int = 0
    skipped_cells: List[SkippedCell] = field(default_factory=list)
    projects_lineage_via_queried_tables: int = 0
    projects_lineage_via_sql_parsing: int = 0


class HexLineageBuilder:
    """
    Builds upstream dataset URN lists from two sources, in priority order:

    1. queriedTables API (ENTERPRISE tier) — Hex's own pre-resolved table list.
       No SQL parsing needed. Most accurate.

    2. Cells API + sqlglot SQL parsing (all tiers) — fallback when queriedTables
       is unavailable.

    Safety contract: lineage is only emitted when the platform can be resolved
    with confidence. Cells whose connection ID is missing from the connections map
    (and not covered by connection_platform_map) are skipped and reported —
    never emitted with a wrong platform.

    The sqlglot dialect is derived from the resolved platform, not from a separate
    config field. There is no global fallback platform.
    """

    def __init__(
        self,
        # {connection_id → connection_type string, e.g. "snowflake"}
        connections: Dict[str, str],
        platform_instance: Optional[str],
        env: str,
        report: LineageBuilderReport,
        # current project_id, updated per project by the caller
        project_id: str = "",
        # DataHub graph client for schema resolution. When provided, the
        # SchemaResolver fetches table schemas from DataHub on demand, enabling
        # SELECT * expansion and accurate column-level lineage. When None,
        # schema-less parsing is used (dataset-level lineage only).
        graph: Optional["DataHubGraph"] = None,
    ):
        self._connections = connections
        self._platform_instance = platform_instance
        self._env = env
        self._report = report
        self._project_id = project_id
        self._graph = graph
        # One SchemaResolver per platform — shared across cells of the same
        # platform so cached schemas are reused within a single ingestion run.
        self._schema_resolvers: Dict[str, SchemaResolver] = {}

    def set_project_id(self, project_id: str) -> None:
        self._project_id = project_id

    def build_from_queried_tables(self, queried_tables: List[dict]) -> List[str]:
        """
        Build upstream URN strings from queriedTables API response.

        {dataConnectionId, dataConnectionName, tableName} — tableName is already
        fully qualified; no SQL parsing needed. Only emits URNs for connections
        whose platform can be confidently resolved.
        """
        seen: Set[str] = set()
        result: List[str] = []

        for item in queried_tables:
            connection_id = item.get("dataConnectionId")
            table_name = item.get("tableName")
            if not table_name:
                continue

            platform, reason = self._resolve_platform(connection_id)
            if platform is None:
                self._record_skip(
                    connection_id=connection_id or "",
                    cell_id="queriedTables",
                    cell_label=table_name,
                    reason=reason or "unknown",
                )
                continue

            urn = make_dataset_urn_with_platform_instance(
                platform=platform,
                name=table_name,
                platform_instance=self._platform_instance,
                env=self._env,
            )
            if urn not in seen:
                seen.add(urn)
                result.append(urn)

        self._report.upstream_datasets_found += len(result)
        self._report.projects_lineage_via_queried_tables += 1
        return result

    def build_upstream_urns(
        self, sql_cells: List[SqlCell]
    ) -> Tuple[List[str], List[str]]:
        """
        Parse SQL from each cell and return:
          - de-duplicated upstream dataset URNs  (dataset-level lineage)
          - de-duplicated upstream schema field URNs  (column-level lineage)

        Column-level lineage requires a graph-backed SchemaResolver to expand
        SELECT * and resolve ambiguous column references. Without a graph the
        second list will be empty for SELECT * queries but populated for queries
        with explicit column references.

        Cells with unresolvable connections are skipped and recorded in the
        report — never emitted with a fallback platform.
        """
        seen_datasets: Set[str] = set()
        seen_fields: Set[str] = set()
        dataset_result: List[str] = []
        field_result: List[str] = []

        for cell in sql_cells:
            self._report.sql_cells_attempted += 1

            platform, reason = self._resolve_platform(cell.data_connection_id)
            if platform is None:
                self._report.sql_cells_skipped_unknown_connection += 1
                self._record_skip(
                    connection_id=cell.data_connection_id or "",
                    cell_id=cell.cell_id,
                    cell_label=cell.cell_label,
                    reason=reason or "unknown",
                )
                continue

            dataset_urns, field_urns = self._parse_cell(cell, platform)
            if dataset_urns:
                self._report.sql_cells_succeeded += 1
                for urn in dataset_urns:
                    if urn not in seen_datasets:
                        seen_datasets.add(urn)
                        dataset_result.append(urn)
                for urn in field_urns:
                    if urn not in seen_fields:
                        seen_fields.add(urn)
                        field_result.append(urn)
            else:
                self._report.sql_cells_no_upstreams += 1

        self._report.upstream_datasets_found += len(dataset_result)
        self._report.projects_lineage_via_sql_parsing += 1
        return dataset_result, field_result

    def _resolve_platform(
        self, connection_id: Optional[str]
    ) -> Tuple[Optional[str], Optional[str]]:
        """
        Return (platform, None) on success, or (None, reason) when the platform
        cannot be confidently determined.

        Never returns a fallback platform — callers must skip on None.
        """
        if not connection_id:
            return None, "unknown_connection_id"

        conn_type = self._connections.get(connection_id)
        if not conn_type:
            return None, "unknown_connection_id"

        platform = CONNECTION_TYPE_TO_DATAHUB_PLATFORM.get(conn_type.lower())
        if not platform:
            return None, f"unknown_connection_type:{conn_type}"

        return platform, None

    def _get_resolver(self, platform: str) -> SchemaResolver:
        """Return the cached SchemaResolver for this platform, creating it if needed."""
        if platform not in self._schema_resolvers:
            self._schema_resolvers[platform] = SchemaResolver(
                platform=platform,
                platform_instance=self._platform_instance,
                env=self._env,
                graph=self._graph,
            )
        return self._schema_resolvers[platform]

    def _parse_cell(self, cell: SqlCell, platform: str) -> Tuple[List[str], List[str]]:
        """
        Parse one SQL cell and return (dataset_urns, schema_field_urns).

        Uses DataHub's canonical platform → sqlglot dialect mapping.
        Column-level lineage is populated when a graph-backed SchemaResolver
        can resolve table schemas; otherwise the field list is empty.
        """
        dialect = get_dialect_str(platform)
        resolver = self._get_resolver(platform)
        try:
            result = sqlglot_lineage(
                sql=cell.sql_source,
                schema_resolver=resolver,
                override_dialect=dialect,
            )
        except Exception as e:
            self._report.sql_cells_failed += 1
            logger.debug(
                "SQL parsing failed for cell %s (%r): %s",
                cell.cell_id,
                cell.cell_label,
                e,
            )
            return [], []

        dataset_urns = [urn for urn in result.in_tables if isinstance(urn, str)]

        # Extract unique upstream schema field URNs from column lineage.
        # Each ColumnLineageInfo.upstreams entry is a ColumnRef with a
        # resolved dataset URN and column name.
        field_urns: List[str] = []
        if result.column_lineage:
            seen: Set[str] = set()
            for col_info in result.column_lineage:
                for upstream in col_info.upstreams:
                    if upstream.table and upstream.column:
                        furn = make_schema_field_urn(
                            str(upstream.table), upstream.column
                        )
                        if furn not in seen:
                            seen.add(furn)
                            field_urns.append(furn)

        return dataset_urns, field_urns

    def _record_skip(
        self,
        connection_id: str,
        cell_id: str,
        cell_label: Optional[str],
        reason: str,
    ) -> None:
        self._report.skipped_cells.append(
            SkippedCell(
                project_id=self._project_id,
                cell_id=cell_id,
                cell_label=cell_label,
                connection_id=connection_id,
                reason=reason,
            )
        )
        logger.debug(
            "Skipping lineage for cell %s in project %s — %s "
            "(add connection_id %r to connection_platform_map to recover)",
            cell_id,
            self._project_id,
            reason,
            connection_id,
        )
