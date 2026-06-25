import logging
import re
from dataclasses import dataclass
from typing import TYPE_CHECKING, Dict, List, Optional, Set

from datahub.emitter.mce_builder import make_dataset_urn_with_platform_instance
from datahub.ingestion.source.microstrategy.config import MicroStrategyConfig
from datahub.ingestion.source.microstrategy.models import (
    DashboardDefinition,
    DatasetObject,
    Datasource,
    DatasourceConnection,
    DatasourceReference,
    Visualization,
)

if TYPE_CHECKING:
    from datahub.ingestion.graph.client import DataHubGraph

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class WarehouseLineageContext:
    platform: str
    env: str
    platform_instance: Optional[str] = None
    database: Optional[str] = None
    schema: Optional[str] = None


class MicroStrategyLineageExtractor:
    def __init__(self, config: MicroStrategyConfig):
        self.config = config

    def dataset_urn(
        self,
        project_id: str,
        dashboard_id: str,
        dataset: DatasetObject,
    ) -> str:
        return make_dataset_urn_with_platform_instance(
            platform=self.config.platform,
            platform_instance=self.config.platform_instance,
            env=self.config.env,
            name=f"{project_id}.{dashboard_id}.{dataset.id}".lower(),
        )

    def visualization_inputs(
        self,
        project_id: str,
        dashboard: DashboardDefinition,
        visualization: Visualization,
    ) -> List[str]:
        dataset_by_id = {dataset.id: dataset for dataset in dashboard.datasets}
        if visualization.datasets:
            return sorted(
                {
                    self.dataset_urn(project_id, dashboard.id, dataset_by_id[dataset_id])
                    for dataset_id in visualization.datasets
                    if dataset_id in dataset_by_id
                }
            )

        if len(dashboard.datasets) == 1:
            return [self.dataset_urn(project_id, dashboard.id, dashboard.datasets[0])]

        inferred = self._infer_visualization_dataset_inputs(
            project_id, dashboard, visualization
        )
        if inferred:
            return inferred

        return []

    def dashboard_dataset_urns(
        self,
        project_id: str,
        dashboard: DashboardDefinition,
    ) -> List[str]:
        seen: Set[str] = set()
        urns: List[str] = []
        for dataset in dashboard.datasets:
            urn = self.dataset_urn(project_id, dashboard.id, dataset)
            if urn not in seen:
                urns.append(urn)
                seen.add(urn)
        return urns

    def unresolved_visualization_datasets(
        self, dashboard: DashboardDefinition
    ) -> Dict[str, List[str]]:
        dataset_ids = {dataset.id for dataset in dashboard.datasets}
        unresolved: Dict[str, List[str]] = {}
        for visualization in dashboard.visualizations:
            missing = [
                dataset_id
                for dataset_id in visualization.datasets
                if dataset_id not in dataset_ids
            ]
            if missing:
                unresolved[visualization.key] = sorted(missing)
        return unresolved

    def warehouse_upstream_urns_from_sql(
        self,
        sql: str,
        context: WarehouseLineageContext,
        graph: Optional["DataHubGraph"] = None,
    ) -> List[str]:
        if not sql.strip():
            return []

        try:
            from datahub.sql_parsing.sqlglot_lineage import (
                create_lineage_sql_parsed_result,
            )

            parsed = create_lineage_sql_parsed_result(
                query=sql,
                default_db=context.database,
                default_schema=context.schema,
                platform=context.platform,
                platform_instance=context.platform_instance,
                env=context.env,
                graph=graph,
                override_dialect=_sqlglot_dialect(context.platform),
                generate_column_lineage=False,
            )
            if parsed.in_tables:
                return sorted(set(parsed.in_tables))
        except Exception:
            logger.debug(
                "Falling back to MicroStrategy SQL-view table extraction",
                exc_info=True,
            )

        return sorted(
            {
                self.warehouse_dataset_urn(context, table_name)
                for table_name in extract_tables_from_sql(sql)
            }
        )

    @staticmethod
    def warehouse_dataset_urn(
        context: WarehouseLineageContext,
        table_name: str,
    ) -> str:
        qualified_name = qualify_table_name(
            table_name,
            database=context.database,
            schema=context.schema,
        )
        return make_dataset_urn_with_platform_instance(
            platform=context.platform,
            platform_instance=context.platform_instance,
            env=context.env,
            name=qualified_name,
        )

    def _infer_visualization_dataset_inputs(
        self,
        project_id: str,
        dashboard: DashboardDefinition,
        visualization: Visualization,
    ) -> List[str]:
        if not visualization.object_ids:
            return []

        visualization_object_ids = set(visualization.object_ids)
        visualization_tokens = _lineage_name_tokens(visualization.name)
        inputs: List[str] = []
        for dataset in dashboard.datasets:
            overlap = visualization_object_ids.intersection(dataset.object_ids)
            if not overlap:
                continue
            dataset_tokens = _lineage_name_tokens(dataset.name)
            if visualization_tokens.intersection(dataset_tokens):
                inputs.append(self.dataset_urn(project_id, dashboard.id, dataset))
        return sorted(set(inputs))


def datahub_platform_for_datasource(
    datasource: DatasourceReference,
) -> Optional[str]:
    return datahub_platform_for_source_type(
        datasource.database_type,
        datasource.datasource_type,
        datasource.dbms_name,
    )


def datahub_platform_for_connection(
    connection: DatasourceConnection,
) -> Optional[str]:
    return datahub_platform_for_source_type(
        connection.database_type,
        connection.driver_type,
    )


def datahub_platform_for_source_type(*values: Optional[str]) -> Optional[str]:
    for value in values:
        normalized = _normalize_source_type(value)
        if not normalized:
            continue
        platform = _MSTR_SOURCE_TYPE_TO_DATAHUB_PLATFORM.get(normalized)
        if platform:
            return platform
    return None


def warehouse_context_from_datasources(
    datasources: List[Datasource],
    env: str,
) -> Optional[WarehouseLineageContext]:
    for datasource in datasources:
        platform = datahub_platform_for_datasource(datasource)
        if platform:
            return WarehouseLineageContext(
                platform=platform,
                env=env,
                database=datasource.database_name,
                schema=datasource.schema_name,
            )
    return None


def warehouse_context_with_connection(
    context: WarehouseLineageContext,
    connection: DatasourceConnection,
) -> WarehouseLineageContext:
    return WarehouseLineageContext(
        platform=datahub_platform_for_connection(connection) or context.platform,
        env=context.env,
        platform_instance=context.platform_instance,
        database=connection.database_name or context.database,
        schema=connection.schema_name or context.schema,
    )


def sql_statement_from_sql_view_entry(entry: Dict[str, object]) -> str:
    for key in ("sqlStatement", "sql", "statement"):
        value = entry.get(key)
        if isinstance(value, str):
            return value
    return ""


def sql_view_dataset_key(entry: Dict[str, object]) -> Optional[str]:
    for key in ("id", "objectId", "datasetId", "dataSetId"):
        value = entry.get(key)
        if value:
            return str(value)
    return None


def sql_view_dataset_name(entry: Dict[str, object]) -> Optional[str]:
    for key in ("name", "title"):
        value = entry.get(key)
        if value:
            return str(value)
    return None


def extract_tables_from_sql(sql: str) -> List[str]:
    if not sql.strip():
        return []

    cte_names = _extract_cte_names(sql)
    tables: Set[str] = set()
    for match in _TABLE_REFERENCE_PATTERN.finditer(sql):
        reference = match.group("reference")
        if reference.startswith("("):
            continue
        parts = _identifier_parts(reference)
        if not parts:
            continue
        table_name = ".".join(parts)
        if _is_ignored_table_name(table_name, cte_names):
            continue
        tables.add(table_name)
    return sorted(tables)


def qualify_table_name(
    table_name: str,
    database: Optional[str] = None,
    schema: Optional[str] = None,
) -> str:
    parts = [part for part in table_name.split(".") if part]
    if len(parts) == 1:
        if database and schema:
            return f"{database}.{schema}.{parts[0]}"
        if schema:
            return f"{schema}.{parts[0]}"
    if len(parts) == 2 and database:
        return f"{database}.{parts[0]}.{parts[1]}"
    return table_name


_LINEAGE_STOP_WORDS = {
    "AND",
    "DASHBOARD",
    "DATA",
    "DATASET",
    "REPORT",
    "SALES",
    "SALON",
    "SERVICE",
    "TOTAL",
    "VISUALIZATION",
}


def _lineage_name_tokens(name: str) -> Set[str]:
    return {
        token
        for token in re.findall(r"[A-Za-z0-9]+", name.upper())
        if len(token) > 1 and token not in _LINEAGE_STOP_WORDS
    }


def _normalize_source_type(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    return re.sub(r"[^a-z0-9]+", "_", value.strip().lower()).strip("_")


def _sqlglot_dialect(platform: str) -> Optional[str]:
    return _DATAHUB_PLATFORM_TO_SQLGLOT_DIALECT.get(platform)


def _extract_cte_names(sql: str) -> Set[str]:
    cte_names: Set[str] = set()
    for match in re.finditer(
        r"(?:with|,)\s+(?P<name>[A-Za-z_][\w$#]*)\s+as\s*\(",
        sql,
        re.IGNORECASE,
    ):
        cte_names.add(match.group("name").lower())
    return cte_names


def _identifier_parts(reference: str) -> List[str]:
    parts: List[str] = []
    for match in _IDENTIFIER_PATTERN.finditer(reference):
        value = next(group for group in match.groups() if group)
        parts.append(value)
    return parts


def _is_ignored_table_name(table_name: str, cte_names: Set[str]) -> bool:
    lowered = table_name.lower()
    if lowered in _SQL_KEYWORDS or lowered in cte_names:
        return True
    if "." in table_name:
        return False
    return bool(_VOLATILE_TABLE_PATTERN.match(table_name.upper()))


_MSTR_SOURCE_TYPE_TO_DATAHUB_PLATFORM = {
    "athena": "athena",
    "big_query": "bigquery",
    "bigquery": "bigquery",
    "db2": "db2",
    "google_bigquery": "bigquery",
    "microsoft_sql_server": "mssql",
    "mysql": "mysql",
    "oracle": "oracle",
    "postgre_sql": "postgres",
    "postgres": "postgres",
    "postgresql": "postgres",
    "red_shift": "redshift",
    "redshift": "redshift",
    "snow_flake": "snowflake",
    "snowflake": "snowflake",
    "sql_server": "mssql",
    "sqlserver": "mssql",
    "synapse": "mssql",
    "teradata": "teradata",
}

_DATAHUB_PLATFORM_TO_SQLGLOT_DIALECT = {
    "athena": "presto",
    "bigquery": "bigquery",
    "db2": "db2",
    "mssql": "tsql",
    "mysql": "mysql",
    "oracle": "oracle",
    "postgres": "postgres",
    "redshift": "redshift",
    "snowflake": "snowflake",
    "teradata": "teradata",
}

_SQL_KEYWORDS = {
    "as",
    "cross",
    "delete",
    "full",
    "group",
    "having",
    "inner",
    "into",
    "join",
    "left",
    "on",
    "order",
    "outer",
    "right",
    "select",
    "set",
    "update",
    "where",
    "with",
}

_IDENTIFIER = r'(?:\"([^\"]+)\"|`([^`]+)`|\[([^\]]+)\]|([A-Za-z_][\w$#]*))'
_IDENTIFIER_PATH = rf"{_IDENTIFIER}(?:\s*\.\s*{_IDENTIFIER}){{0,2}}"
_TABLE_REFERENCE_PATTERN = re.compile(
    rf"\b(?:from|join)\s+(?P<reference>{_IDENTIFIER_PATH}|\()",
    re.IGNORECASE,
)
_IDENTIFIER_PATTERN = re.compile(_IDENTIFIER)
_VOLATILE_TABLE_PATTERN = re.compile(r"^T[A-Z0-9]{10,}$")
