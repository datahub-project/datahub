import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set

from datahub.emitter.mce_builder import make_dataset_urn_with_platform_instance
from datahub.ingestion.source.hex.constants import CONNECTION_TYPE_TO_DATAHUB_PLATFORM
from datahub.ingestion.source.hex.model import SqlCell
from datahub.sql_parsing.schema_resolver import SchemaResolver
from datahub.sql_parsing.sqlglot_lineage import sqlglot_lineage

logger = logging.getLogger(__name__)


@dataclass
class LineageBuilderReport:
    sql_cells_attempted: int = 0
    sql_cells_succeeded: int = 0
    sql_cells_failed: int = 0
    sql_cells_no_upstreams: int = 0
    upstream_datasets_found: int = 0
    unknown_connection_types: List[str] = field(default_factory=list)
    unknown_connection_ids: List[str] = field(default_factory=list)
    projects_lineage_via_queried_tables: int = 0
    projects_lineage_via_sql_parsing: int = 0


class HexLineageBuilder:
    """
    Builds upstream dataset URN lists from two sources, in priority order:

    1. queriedTables API (ENTERPRISE tier only) — Hex's own pre-resolved table
       list. No SQL parsing. Most accurate.

    2. Cells API + sqlglot SQL parsing (all tiers) — parses SQL source from
       each SQL cell with an external connection. Best-effort fallback.
    """

    def __init__(
        self,
        connections: Dict[str, str],
        platform_instance: Optional[str],
        env: str,
        sql_parsing_platform_default: str,
        report: LineageBuilderReport,
    ):
        # connections: {connection_id → connection_type}
        self._connections = connections
        self._platform_instance = platform_instance
        self._env = env
        self._default_platform = sql_parsing_platform_default
        self._report = report

    def build_from_queried_tables(self, queried_tables: List[dict]) -> List[str]:
        """
        Build upstream URN strings from queriedTables API response.

        Each item: {dataConnectionId, dataConnectionName, tableName}
        tableName is already fully qualified — no SQL parsing needed.
        """
        seen: Set[str] = set()
        result: List[str] = []

        for item in queried_tables:
            connection_id = item.get("dataConnectionId")
            table_name = item.get("tableName")
            if not table_name:
                continue

            platform = self._resolve_platform(connection_id)
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

    def build_upstream_urns(self, sql_cells: List[SqlCell]) -> List[str]:
        """
        Parse SQL from each cell and return de-duplicated upstream dataset URNs.
        """
        seen: Set[str] = set()
        result: List[str] = []

        for cell in sql_cells:
            self._report.sql_cells_attempted += 1
            platform = self._resolve_platform(cell.data_connection_id)
            upstream_urns = self._parse_cell(cell, platform)

            if upstream_urns:
                self._report.sql_cells_succeeded += 1
                for urn in upstream_urns:
                    if urn not in seen:
                        seen.add(urn)
                        result.append(urn)
            else:
                self._report.sql_cells_no_upstreams += 1

        self._report.upstream_datasets_found += len(result)
        self._report.projects_lineage_via_sql_parsing += 1
        return result

    def _resolve_platform(self, connection_id: Optional[str]) -> str:
        if not connection_id:
            return self._default_platform

        conn_type = self._connections.get(connection_id)
        if not conn_type:
            if connection_id not in self._report.unknown_connection_ids:
                self._report.unknown_connection_ids.append(connection_id)
                logger.warning(
                    "Unknown dataConnectionId %s — using default platform %s",
                    connection_id,
                    self._default_platform,
                )
            return self._default_platform

        platform = CONNECTION_TYPE_TO_DATAHUB_PLATFORM.get(conn_type.lower())
        if not platform:
            if conn_type not in self._report.unknown_connection_types:
                self._report.unknown_connection_types.append(conn_type)
                logger.warning(
                    "Unmapped connection type %r — using default platform %s",
                    conn_type,
                    self._default_platform,
                )
            return self._default_platform

        return platform

    def _parse_cell(self, cell: SqlCell, platform: str) -> List[str]:
        resolver = SchemaResolver(
            platform=platform,
            platform_instance=self._platform_instance,
            env=self._env,
        )
        try:
            result = sqlglot_lineage(
                sql=cell.sql_source,
                schema_resolver=resolver,
                override_dialect=platform,
            )
        except Exception as e:
            self._report.sql_cells_failed += 1
            logger.debug(
                "SQL parsing failed for cell %s (%r): %s",
                cell.cell_id,
                cell.cell_label,
                e,
            )
            return []

        return [urn for urn in result.in_tables if isinstance(urn, str)]
