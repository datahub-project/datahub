import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Dict, List, Optional, Set, Tuple

from datahub.emitter.mce_builder import (
    make_dataset_urn_with_platform_instance,
    make_schema_field_urn,
)
from datahub.ingestion.source.hex.model import HexConnection, SqlCell
from datahub.metadata.urns import SchemaFieldUrn
from datahub.sql_parsing.schema_resolver import SchemaResolver
from datahub.sql_parsing.sql_parsing_common import get_dialect_str
from datahub.sql_parsing.sqlglot_lineage import sqlglot_lineage

_MAX_SAMPLE_MISMATCHES = 5

if TYPE_CHECKING:
    from datahub.ingestion.graph.client import DataHubGraph

logger = logging.getLogger(__name__)


def _qualify_table_name(
    table_name: str,
    default_database: Optional[str],
    default_schema: Optional[str],
) -> str:
    """Pad an under-qualified table name with the connection's defaults.

    Hex's queriedTables API returns `tableName` with no guaranteed depth — it
    may be `table`, `schema.table`, or `database.schema.table`. We pad missing
    outer scopes from `default_database` / `default_schema` (positional,
    outermost first) so the resulting URN matches the warehouse-side ingestion.
    Names that already meet the expected depth are returned unchanged. For
    2-part platforms (mysql/mariadb/clickhouse) `default_database` is None, so
    only `default_schema` is prepended.
    """
    prefixes = [scope for scope in (default_database, default_schema) if scope]
    parts = table_name.split(".")
    missing = (len(prefixes) + 1) - len(parts)
    if missing <= 0:
        return table_name
    return ".".join(prefixes[:missing] + parts)


@dataclass
class SkippedCell:
    project_id: str
    cell_id: str
    cell_label: Optional[str]
    connection_id: str
    # "missing_connection_id"  — cell had no dataConnectionId
    # "unresolved_platform"    — connection_id present but no platform mapping
    reason: str


@dataclass
class MismatchedCell:
    """
    A SQL cell whose parsed table URNs did not fully match the queriedTables
    result for that project. Column lineage was skipped for unmatched tables.

    Common causes: unqualified table names in SQL that queriedTables resolves
    to a fully-qualified name, view references, dynamic SQL in Python cells
    that produces a different table path, or schema/database prefix differences.
    """

    project_id: str
    cell_id: str
    cell_label: Optional[str]
    # table URNs that SQL parsing produced but were absent from queriedTables
    unmatched_parsed_urns: List[str]
    # representative subset of queriedTables URNs for that project (for comparison)
    sample_queried_urns: List[str]


@dataclass
class LineageBuilderReport:
    sql_cells_attempted: int = 0
    sql_cells_succeeded: int = 0
    sql_cells_failed: int = 0
    sql_cells_no_upstreams: int = 0
    sql_cells_skipped_unresolved_platform: int = 0
    upstream_datasets_found: int = 0
    skipped_cells: List[SkippedCell] = field(default_factory=list)
    projects_lineage_via_queried_tables: int = 0
    projects_lineage_via_sql_parsing: int = 0
    # ENTERPRISE cross-validation: SQL parsing vs queriedTables
    enterprise_column_fields_emitted: int = 0
    enterprise_column_fields_skipped_mismatch: int = 0
    enterprise_cells_with_mismatch: int = 0
    enterprise_sample_mismatched_cells: List[MismatchedCell] = field(
        default_factory=list
    )


class HexLineageBuilder:
    """
    Builds upstream dataset URN lists from two sources, in priority order:

    1. queriedTables API (ENTERPRISE tier) — Hex's own pre-resolved table list.
       No SQL parsing needed. Most accurate.

    2. Cells API + sqlglot SQL parsing (all tiers) — fallback when queriedTables
       is unavailable.

    Safety contract: lineage is only emitted when the platform can be resolved
    with confidence. Cells whose connection ID is missing from `connections`,
    or whose HexConnection has platform=None, are skipped and reported — never
    emitted with a wrong platform.

    Type→platform translation is the caller's responsibility: this class accepts
    a pre-resolved {connection_id → HexConnection} map. The sqlglot dialect is
    derived from the resolved platform, not from a separate config field. There
    is no global fallback platform.
    """

    def __init__(
        self,
        # {connection_id → HexConnection}, pre-resolved by the caller
        connections: Dict[str, HexConnection],
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
        self._env = env
        self._report = report
        self._project_id = project_id
        self._graph = graph
        # Cached per (platform, platform_instance) — same-platform connections
        # with different instances must NOT share a resolver.
        self._schema_resolvers: Dict[Tuple[str, Optional[str]], SchemaResolver] = {}

    def set_project_id(self, project_id: str) -> None:
        self._project_id = project_id

    def build_from_queried_tables(self, queried_tables: List[dict]) -> List[str]:
        """
        Build upstream URN strings from queriedTables API response.

        {dataConnectionId, dataConnectionName, tableName} — tableName's level
        of qualification is not guaranteed by Hex, so under-qualified names
        are padded with the connection's default_database / default_schema
        before URN construction. Only emits URNs for connections whose
        platform can be confidently resolved.
        """
        seen: Set[str] = set()
        result: List[str] = []

        for item in queried_tables:
            connection_id = item.get("dataConnectionId")
            table_name = item.get("tableName")
            if not table_name:
                continue

            connection, reason = self._lookup_connection(connection_id)
            if connection is None or connection.platform is None:
                self._record_skip(
                    connection_id=connection_id or "",
                    cell_id="queriedTables",
                    cell_label=table_name,
                    reason=reason or "unresolved_platform",
                )
                continue

            qualified_name = _qualify_table_name(
                table_name,
                default_database=connection.default_database,
                default_schema=connection.default_schema,
            )
            urn = make_dataset_urn_with_platform_instance(
                platform=connection.platform,
                name=qualified_name,
                platform_instance=connection.platform_instance,
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

            connection, reason = self._lookup_connection(cell.data_connection_id)
            if connection is None:
                self._report.sql_cells_skipped_unresolved_platform += 1
                self._record_skip(
                    connection_id=cell.data_connection_id or "",
                    cell_id=cell.cell_id,
                    cell_label=cell.cell_label,
                    reason=reason or "unresolved_platform",
                )
                continue

            dataset_urns, field_urns = self._parse_cell(cell, connection)
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

    def build_validated_column_lineage(
        self,
        sql_cells: List[SqlCell],
        queried_table_urns: List[str],
    ) -> List[str]:
        """
        Extract column-level lineage from SQL cells, cross-validated against
        the queriedTables result set.

        For ENTERPRISE workspaces: queriedTables provides runtime-proven dataset
        URNs. SQL parsing may produce table URNs that differ (unqualified names,
        view references, dynamic SQL). A column reference is only emitted if its
        parent dataset URN appears in queriedTables — otherwise the SchemaFieldUrn
        would dangle (pointing to a dataset that may not exist in DataHub or that
        represents a different entity than intended).

        Mismatches are recorded in the report with sample cells for diagnostics.
        """
        queried_set = set(queried_table_urns)
        validated_fields: List[str] = []
        seen_fields: Set[str] = set()

        for cell in sql_cells:
            connection, _ = self._lookup_connection(cell.data_connection_id)
            if connection is None:
                continue

            _, field_urns = self._parse_cell(cell, connection)
            if not field_urns:
                continue

            matched: List[str] = []
            unmatched_tables: Set[str] = set()
            unmatched_field_count = 0

            for furn in field_urns:
                try:
                    parent_urn = SchemaFieldUrn.from_string(furn).parent
                except Exception:
                    logger.warning(
                        "Skipping malformed schema field URN during queriedTables cross-validation: %s",
                        furn,
                        exc_info=True,
                    )
                    continue
                if parent_urn in queried_set:
                    matched.append(furn)
                else:
                    unmatched_tables.add(parent_urn)
                    unmatched_field_count += 1

            if unmatched_tables:
                self._report.enterprise_cells_with_mismatch += 1
                self._report.enterprise_column_fields_skipped_mismatch += (
                    unmatched_field_count
                )
                if (
                    len(self._report.enterprise_sample_mismatched_cells)
                    < _MAX_SAMPLE_MISMATCHES
                ):
                    self._report.enterprise_sample_mismatched_cells.append(
                        MismatchedCell(
                            project_id=self._project_id,
                            cell_id=cell.cell_id,
                            cell_label=cell.cell_label,
                            unmatched_parsed_urns=sorted(unmatched_tables),
                            # show up to 5 queriedTables URNs for comparison
                            sample_queried_urns=queried_table_urns[:5],
                        )
                    )

            for furn in matched:
                if furn not in seen_fields:
                    seen_fields.add(furn)
                    validated_fields.append(furn)

        self._report.enterprise_column_fields_emitted += len(validated_fields)
        return validated_fields

    def _lookup_connection(
        self, connection_id: Optional[str]
    ) -> Tuple[Optional[HexConnection], Optional[str]]:
        """Return (connection, None) when the platform is confidently known,
        otherwise (None, reason). A HexConnection with platform=None is treated
        as unresolved — callers only need to check `connection is None`.
        """
        if not connection_id:
            return None, "missing_connection_id"

        connection = self._connections.get(connection_id)
        if connection is None or connection.platform is None:
            return None, "unresolved_platform"

        return connection, None

    def _get_resolver(
        self, platform: str, platform_instance: Optional[str]
    ) -> SchemaResolver:
        """Return the cached SchemaResolver for this (platform, instance)."""
        key = (platform, platform_instance)
        if key not in self._schema_resolvers:
            self._schema_resolvers[key] = SchemaResolver(
                platform=platform,
                platform_instance=platform_instance,
                env=self._env,
                graph=self._graph,
            )
        return self._schema_resolvers[key]

    def _parse_cell(
        self,
        cell: SqlCell,
        connection: HexConnection,
    ) -> Tuple[List[str], List[str]]:
        """
        Parse one SQL cell and return (dataset_urns, schema_field_urns).

        Uses DataHub's canonical platform → sqlglot dialect mapping. The
        connection's default_database / default_schema are forwarded to
        sqlglot so unqualified `FROM table` refs resolve to the canonical
        warehouse URN. Column-level lineage is populated when a graph-backed
        SchemaResolver can resolve table schemas; otherwise the field list
        is empty.
        """
        assert connection.platform is not None  # callers gate on this
        dialect = get_dialect_str(connection.platform)
        resolver = self._get_resolver(connection.platform, connection.platform_instance)
        try:
            result = sqlglot_lineage(
                sql=cell.sql_source,
                schema_resolver=resolver,
                default_db=connection.default_database,
                default_schema=connection.default_schema,
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
