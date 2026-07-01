import logging
import re
from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
    Dict,
    Iterable,
    List,
    MutableMapping,
    Optional,
    Set,
    Tuple,
    TypeVar,
)

import datahub.emitter.mce_builder as builder
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

LineageKey = TypeVar("LineageKey")


@dataclass(frozen=True)
class WarehouseLineageContext:
    platform: str
    env: str
    platform_instance: Optional[str] = None
    database: Optional[str] = None
    schema: Optional[str] = None


@dataclass(frozen=True)
class ModelFieldLineage:
    upstream_dataset_urn: str
    upstream_field_urns: List[str]


@dataclass
class ModelLineageIndex:
    fact_upstreams: Dict[str, List[ModelFieldLineage]]
    attribute_upstreams: Dict[str, List[ModelFieldLineage]]
    attribute_form_upstreams: Dict[Tuple[str, str], List[ModelFieldLineage]]
    table_count: int = 0

    def fact_field_urns(self, fact_ids: Iterable[str]) -> List[str]:
        upstreams: List[str] = []
        for fact_id in fact_ids:
            for lineage in self.fact_upstreams.get(_normalize_object_id(fact_id), []):
                upstreams.extend(lineage.upstream_field_urns)
        return _dedupe_sorted(upstreams)

    def attribute_field_urns(
        self,
        attribute_id: str,
        form_name: Optional[str] = None,
    ) -> List[str]:
        normalized_attribute_id = _normalize_object_id(attribute_id)
        upstreams: List[str] = []
        if form_name:
            upstreams.extend(
                field_urn
                for lineage in self.attribute_form_upstreams.get(
                    (normalized_attribute_id, _normalize_lineage_key(form_name)),
                    [],
                )
                for field_urn in lineage.upstream_field_urns
            )
        if not upstreams:
            upstreams.extend(
                field_urn
                for lineage in self.attribute_upstreams.get(
                    normalized_attribute_id,
                    [],
                )
                for field_urn in lineage.upstream_field_urns
            )
        return _dedupe_sorted(upstreams)


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
                    self.dataset_urn(
                        project_id,
                        dashboard.id,
                        dataset_by_id[dataset_id],
                    )
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

    def model_lineage_index_from_tables(
        self,
        model_tables: List[Dict[str, object]],
        context: WarehouseLineageContext,
    ) -> ModelLineageIndex:
        fact_upstreams: Dict[str, List[ModelFieldLineage]] = {}
        attribute_upstreams: Dict[str, List[ModelFieldLineage]] = {}
        attribute_form_upstreams: Dict[Tuple[str, str], List[ModelFieldLineage]] = {}

        for table in model_tables:
            if not isinstance(table, dict):
                continue
            physical_table = table.get("physicalTable")
            if not isinstance(physical_table, dict):
                continue
            physical_table_name = _physical_table_name(physical_table, context)
            if not physical_table_name:
                continue
            upstream_dataset_urn = self.warehouse_dataset_urn(
                context,
                physical_table_name,
            )

            for fact in _coerce_dicts(table.get("facts")):
                fact_id = _object_id(fact.get("information"))
                if not fact_id:
                    continue
                field_urns = _model_expression_field_urns(
                    fact,
                    upstream_dataset_urn,
                )
                if field_urns:
                    _append_lineage(
                        fact_upstreams,
                        _normalize_object_id(fact_id),
                        ModelFieldLineage(upstream_dataset_urn, field_urns),
                    )

            for attribute in _coerce_dicts(table.get("attributes")):
                attribute_id = _object_id(attribute.get("information"))
                if not attribute_id:
                    continue
                attribute_lineages: List[ModelFieldLineage] = []
                for form in _coerce_dicts(attribute.get("forms")):
                    field_urns = _model_expression_field_urns(
                        form,
                        upstream_dataset_urn,
                    )
                    if not field_urns:
                        continue
                    lineage = ModelFieldLineage(upstream_dataset_urn, field_urns)
                    attribute_lineages.append(lineage)
                    form_name = _form_name(form)
                    if form_name:
                        _append_lineage(
                            attribute_form_upstreams,
                            (_normalize_object_id(attribute_id), form_name),
                            lineage,
                        )
                for lineage in attribute_lineages:
                    _append_lineage(
                        attribute_upstreams,
                        _normalize_object_id(attribute_id),
                        lineage,
                    )

        return ModelLineageIndex(
            fact_upstreams=fact_upstreams,
            attribute_upstreams=attribute_upstreams,
            attribute_form_upstreams=attribute_form_upstreams,
            table_count=len(model_tables),
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
            name=qualified_name.lower(),
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
    contexts = {
        context
        for datasource in datasources
        if (context := warehouse_context_from_datasource(datasource, env)) is not None
    }
    if len(contexts) == 1:
        return next(iter(contexts))
    return None


def warehouse_context_from_datasource(
    datasource: DatasourceReference,
    env: str,
) -> Optional[WarehouseLineageContext]:
    platform = datahub_platform_for_datasource(datasource)
    if not platform:
        return None
    return WarehouseLineageContext(
        platform=platform,
        env=env,
        database=datasource.database_name,
        schema=datasource.schema_name,
    )


def matching_datasource_for_context(
    datasources: List[Datasource],
    context: WarehouseLineageContext,
) -> Optional[Datasource]:
    for datasource in datasources:
        if warehouse_context_from_datasource(datasource, context.env) == context:
            return datasource
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


def metric_fact_ids_from_model(model: Dict[str, object]) -> List[str]:
    fact_ids: Set[str] = set()
    for target in _expression_targets(model):
        subtype = str(target.get("subType") or target.get("subtype") or "").lower()
        if subtype == "fact":
            object_id = _object_id(target)
            if object_id:
                fact_ids.add(_normalize_object_id(object_id))
    return sorted(fact_ids)


def metric_metric_ids_from_model(model: Dict[str, object]) -> List[str]:
    metric_ids: Set[str] = set()
    for target in _expression_targets(model):
        subtype = str(target.get("subType") or target.get("subtype") or "").lower()
        if subtype == "metric":
            object_id = _object_id(target)
            if object_id:
                metric_ids.add(_normalize_object_id(object_id))
    return sorted(metric_ids)


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


def _physical_table_name(
    physical_table: Dict[str, object],
    context: WarehouseLineageContext,
) -> Optional[str]:
    table_name = _clean_identifier_part(physical_table.get("tableName"))
    if not table_name:
        information = physical_table.get("information")
        if isinstance(information, dict):
            table_name = _clean_identifier_part(information.get("name"))
    if not table_name:
        return None
    return qualify_table_name(
        table_name,
        database=_clean_identifier_part(physical_table.get("namespace"))
        or context.database,
        schema=_clean_identifier_part(physical_table.get("tablePrefix"))
        or context.schema,
    )


def _model_expression_field_urns(
    item: Dict[str, object],
    upstream_dataset_urn: str,
) -> List[str]:
    expression = item.get("expression")
    text = ""
    if isinstance(expression, dict):
        text = str(expression.get("text") or "")
    field_names = extract_field_names_from_expression(text)
    return [
        builder.make_schema_field_urn(upstream_dataset_urn, field_name)
        for field_name in field_names
    ]


def extract_field_names_from_expression(expression: str) -> List[str]:
    if not expression.strip():
        return []
    identifiers = [
        identifier
        for identifier in re.findall(r"[A-Za-z_][\w$#]*", expression)
        if identifier.lower() not in _SQL_KEYWORDS
    ]
    return _dedupe_sorted(identifiers)


def _expression_targets(model: Dict[str, object]) -> List[Dict[str, object]]:
    expression = model.get("expression")
    if not isinstance(expression, dict):
        return []
    targets: List[Dict[str, object]] = []
    tokens = expression.get("tokens")
    if not isinstance(tokens, list):
        return targets
    for token in tokens:
        if not isinstance(token, dict):
            continue
        target = token.get("target")
        if isinstance(target, dict):
            targets.append(target)
            continue
        value = token.get("value")
        if isinstance(value, dict):
            targets.append(value)
    return targets


def _form_name(form: Dict[str, object]) -> Optional[str]:
    for key in ("name", "title", "id"):
        value = form.get(key)
        if value:
            return _normalize_lineage_key(str(value))
    form_category = form.get("formCategory")
    if isinstance(form_category, dict):
        value = form_category.get("name")
        if value:
            return _normalize_lineage_key(str(value))
    return None


def _object_id(value: object) -> Optional[str]:
    if not isinstance(value, dict):
        return None
    for key in ("objectId", "id"):
        candidate = value.get(key)
        if candidate:
            return str(candidate)
    return None


def _coerce_dicts(value: object) -> List[Dict[str, object]]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, dict)]


def _append_lineage(
    target: MutableMapping[LineageKey, List[ModelFieldLineage]],
    key: LineageKey,
    lineage: ModelFieldLineage,
) -> None:
    target.setdefault(key, []).append(lineage)


def _normalize_object_id(value: str) -> str:
    return value.strip().upper()


def _normalize_lineage_key(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip().lower()


def _clean_identifier_part(value: object) -> Optional[str]:
    if value is None:
        return None
    cleaned = str(value).strip().strip(".")
    return cleaned or None


def _dedupe_sorted(values: Iterable[str]) -> List[str]:
    return sorted(set(values))


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

_IDENTIFIER = r"(?:\"([^\"]+)\"|`([^`]+)`|\[([^\]]+)\]|([A-Za-z_][\w$#]*))"
_IDENTIFIER_PATH = rf"{_IDENTIFIER}(?:\s*\.\s*{_IDENTIFIER}){{0,2}}"
_TABLE_REFERENCE_PATTERN = re.compile(
    rf"\b(?:from|join)\s+(?P<reference>{_IDENTIFIER_PATH}|\()",
    re.IGNORECASE,
)
_IDENTIFIER_PATTERN = re.compile(_IDENTIFIER)
_VOLATILE_TABLE_PATTERN = re.compile(r"^T[A-Z0-9]{10,}$")
