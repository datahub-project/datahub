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
from datahub.ingestion.source.microstrategy.report import MicroStrategyReport

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
    def __init__(self, config: MicroStrategyConfig, report: MicroStrategyReport):
        self.config = config
        self.report = report

    def dataset_urn(
        self,
        project_id: str,
        parent_id: str,
        dataset: DatasetObject,
    ) -> str:
        """URN for a MicroStrategy dataset scoped to its parent dashboard or report."""
        return make_dataset_urn_with_platform_instance(
            platform=self.config.platform,
            platform_instance=self.config.platform_instance,
            env=self.config.env,
            name=f"{project_id}.{parent_id}.{dataset.id}".lower(),
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

        # Deferred import: sqlglot_lineage pulls in the full sqlglot parser,
        # which is expensive to import and only needed for opt-in SQL lineage.
        from datahub.sql_parsing.split_statements import split_statements
        from datahub.sql_parsing.sqlglot_lineage import (
            create_lineage_sql_parsed_result,
        )

        dialect = _sqlglot_dialect(context.platform)
        # MicroStrategy SQL views contain multi-pass SQL: several statements
        # (typically CREATE VOLATILE/TEMP TABLE ... AS select) concatenated
        # without semicolons. Parse per statement and subtract the tables
        # CREATED within the script so volatile intermediates never appear as
        # warehouse upstreams. Only CREATE targets are subtracted: a table
        # that is read and written by the same DML statement (self-referencing
        # INSERT/MERGE) is a genuine upstream.
        in_urns: Set[str] = set()
        created_urns: Set[str] = set()
        had_parse_failure = False
        for statement in split_statements(sql, dialect=dialect):
            if not statement.strip():
                continue
            if _is_lineage_irrelevant_statement(statement):
                logger.debug(
                    "Skipping non-lineage SQL-view statement: %s",
                    statement.strip()[:120],
                )
                continue
            try:
                parsed = create_lineage_sql_parsed_result(
                    query=statement,
                    default_db=context.database,
                    default_schema=context.schema,
                    platform=context.platform,
                    platform_instance=context.platform_instance,
                    env=context.env,
                    graph=graph,
                    override_dialect=dialect,
                    generate_column_lineage=False,
                )
            except Exception as error:
                had_parse_failure = self._record_statement_parse_failure(
                    statement, context, error
                )
                continue
            if parsed.debug_info.table_error is not None:
                had_parse_failure = self._record_statement_parse_failure(
                    statement, context, parsed.debug_info.table_error
                )
                continue
            in_urns.update(parsed.in_tables)
            if _CREATE_STATEMENT.match(statement):
                created_urns.update(parsed.out_tables)

        upstream_urns = in_urns - created_urns
        if not upstream_urns and had_parse_failure:
            self.report.warning(
                title="Failed to parse MicroStrategy SQL view",
                message=(
                    "No statement in a MicroStrategy SQL view could be parsed; "
                    "warehouse lineage for this dataset was skipped."
                ),
                context=f"platform={context.platform}, sql_prefix={sql.strip()[:80]!r}",
            )
        return sorted(upstream_urns)

    def _record_statement_parse_failure(
        self,
        statement: str,
        context: WarehouseLineageContext,
        error: Exception,
    ) -> bool:
        self.report.report_sql_parse_failure(
            f"platform={context.platform}, "
            f"error={type(error).__name__}: {str(error)[:160]}, "
            f"sql_prefix={statement.strip()[:80]!r}"
        )
        logger.debug(
            "Failed to parse MicroStrategy SQL-view statement (%s): %s",
            error,
            statement.strip()[:200],
        )
        return True

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

        unique_inputs = sorted(set(inputs))
        # Dashboards built from many cubes that share one object catalog (e.g.
        # one cube per time period) make every dataset "match" every
        # visualization. An inference that cannot exclude anything carries no
        # signal, so treat it as unresolved rather than emit an all-to-all
        # lineage fan-out. Two-dataset dashboards are exempt: a visualization
        # combining both datasets is common and plausible there. Dashboard-level
        # fallback lineage remains available via `emit_dashboard_dataset_edges`.
        if len(dashboard.datasets) > 2 and len(unique_inputs) == len(
            dashboard.datasets
        ):
            self.report.report_unresolved_visualization()
            self.report.report_visualization_suppressed_ambiguous()
            logger.debug(
                "Visualization %s matched all %d datasets of dashboard %s; "
                "treating the binding as unresolved",
                visualization.key,
                len(dashboard.datasets),
                dashboard.id,
            )
            return []
        return unique_inputs


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
    platform_instance_map: Optional[Dict[str, str]] = None,
) -> Optional[WarehouseLineageContext]:
    contexts = {
        context
        for datasource in datasources
        if (
            context := warehouse_context_from_datasource(
                datasource, env, platform_instance_map
            )
        )
        is not None
    }
    if len(contexts) == 1:
        return next(iter(contexts))
    return None


def warehouse_context_from_datasource(
    datasource: DatasourceReference,
    env: str,
    platform_instance_map: Optional[Dict[str, str]] = None,
) -> Optional[WarehouseLineageContext]:
    platform = datahub_platform_for_datasource(datasource)
    if not platform:
        return None
    return WarehouseLineageContext(
        platform=platform,
        env=env,
        platform_instance=(platform_instance_map or {}).get(platform),
        database=datasource.database_name,
        schema=datasource.schema_name,
    )


def matching_datasource_for_context(
    datasources: List[Datasource],
    context: WarehouseLineageContext,
    platform_instance_map: Optional[Dict[str, str]] = None,
) -> Optional[Datasource]:
    for datasource in datasources:
        if (
            warehouse_context_from_datasource(
                datasource, context.env, platform_instance_map
            )
            == context
        ):
            return datasource
    return None


def warehouse_context_with_connection(
    context: WarehouseLineageContext,
    connection: DatasourceConnection,
    platform_instance_map: Optional[Dict[str, str]] = None,
) -> WarehouseLineageContext:
    platform = datahub_platform_for_connection(connection) or context.platform
    platform_instance = (platform_instance_map or {}).get(
        platform, context.platform_instance
    )
    return WarehouseLineageContext(
        platform=platform,
        env=context.env,
        platform_instance=platform_instance,
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
    return _expression_target_ids(model, "fact")


def metric_metric_ids_from_model(model: Dict[str, object]) -> List[str]:
    return _expression_target_ids(model, "metric")


def _expression_target_ids(model: Dict[str, object], subtype: str) -> List[str]:
    object_ids: Set[str] = set()
    for target in _expression_targets(model):
        target_subtype = str(
            target.get("subType") or target.get("subtype") or ""
        ).lower()
        if target_subtype == subtype:
            object_id = _object_id(target)
            if object_id:
                object_ids.add(_normalize_object_id(object_id))
    return sorted(object_ids)


# Generic BI vocabulary excluded from the name-token overlap heuristic used to
# infer visualization -> dataset lineage. Only genuinely non-discriminating
# words belong here; business terms (e.g. "SALES") must NOT be added because
# they legitimately distinguish datasets on real tenants.
_LINEAGE_STOP_WORDS = {
    "AND",
    "DASHBOARD",
    "DATA",
    "DATASET",
    "REPORT",
    "TOTAL",
    "VISUALIZATION",
}


# MicroStrategy SQL views append non-SQL commentary after the final pass
# ("[Analytical engine calculation steps: ...]", "with parameters: 1") and
# include DROP statements for its volatile tables. None of these carry
# lineage, so they are skipped instead of counted as parse failures. The
# "with parameters" match requires the trailing colon so a legitimate CTE
# named "parameters" is never skipped.
_LINEAGE_IRRELEVANT_STATEMENT = re.compile(
    r"^\s*(drop\s|\[|with\s+parameters\s*:)",
    re.IGNORECASE,
)

_CREATE_STATEMENT = re.compile(r"^\s*create\b", re.IGNORECASE)


def _is_lineage_irrelevant_statement(statement: str) -> bool:
    return bool(_LINEAGE_IRRELEVANT_STATEMENT.match(statement))


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
            continue
        # Some MicroStrategy versions inline the object reference directly on
        # the token instead of nesting it under target/value.
        if token.get("objectId") or token.get("id"):
            targets.append(token)
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


def unique_derived_object_owners(model_document: Dict[str, object]) -> Dict[str, str]:
    """Map derived-object ids to the id of the single dataset that defines them.

    The modeling document API lists each dataset's derived metrics/attributes;
    those objects (and their embedded helper objects) are scoped to one
    dataset, so a visualization grid referencing them must be reading that
    dataset. Only each derived entry's own id and its ``embeddedObjects`` ids
    are collected — ids merely *referenced* from expression trees may point at
    shared-catalog objects and would mis-attribute ownership. Ids defined in
    more than one dataset carry no signal and are dropped.

    Keys are normalized (upper-cased) object ids; values are dataset ids as
    reported by the API.
    """
    owners: Dict[str, str] = {}
    shared: Set[str] = set()
    for dataset in _coerce_dicts(model_document.get("datasets")):
        information = dataset.get("information")
        dataset_id = _object_id(information) if isinstance(information, dict) else None
        if not dataset_id:
            continue
        derived_ids: Set[str] = set()
        for derived_entry in _coerce_dicts(
            dataset.get("derivedMetrics")
        ) + _coerce_dicts(dataset.get("derivedAttributes")):
            entry_information = derived_entry.get("information")
            if isinstance(entry_information, dict):
                _add_hex_object_id(entry_information.get("objectId"), derived_ids)
                _add_hex_object_id(entry_information.get("id"), derived_ids)
            for embedded in _coerce_dicts(derived_entry.get("embeddedObjects")):
                _add_hex_object_id(embedded.get("id"), derived_ids)
                _add_hex_object_id(embedded.get("objectId"), derived_ids)
        derived_ids.discard(_normalize_object_id(dataset_id))
        for derived_id in derived_ids:
            if derived_id in owners and owners[derived_id] != dataset_id:
                shared.add(derived_id)
            else:
                owners[derived_id] = dataset_id
    for derived_id in shared:
        owners.pop(derived_id, None)
    return owners


def bind_visualizations_by_derived_objects(
    dashboard: DashboardDefinition,
    owner_by_derived_id: Dict[str, str],
) -> int:
    """Set ``visualization.datasets`` from dataset-scoped derived-object ids.

    Ids that also exist in any dashboard dataset's shared object catalog are
    ignored: a shared object cannot discriminate between datasets even when
    only one dataset's derived expressions happen to reference it. Returns the
    number of visualizations bound.
    """
    dataset_ids = {dataset.id for dataset in dashboard.datasets}
    shared_catalog_ids = {
        _normalize_object_id(object_id)
        for dataset in dashboard.datasets
        for object_id in dataset.object_ids
    }
    bound = 0
    for visualization in dashboard.visualizations:
        if visualization.datasets or not visualization.object_ids:
            continue
        owners = {
            owner_by_derived_id[normalized]
            for object_id in visualization.object_ids
            if (normalized := _normalize_object_id(object_id)) in owner_by_derived_id
            and normalized not in shared_catalog_ids
        } & dataset_ids
        if owners:
            visualization.datasets = sorted(owners)
            bound += 1
    return bound


_HEX_OBJECT_ID = re.compile(r"[0-9A-Fa-f]{32}")


def _add_hex_object_id(value: object, out: Set[str]) -> None:
    if isinstance(value, str) and _HEX_OBJECT_ID.fullmatch(value):
        out.add(_normalize_object_id(value))


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

# Keywords and function names excluded when extracting column identifiers from
# model expression text (e.g. `SUM(QTY_SOLD * UNIT_PRICE)` must yield only the
# column names, never the function tokens).
_SQL_KEYWORDS = {
    "abs",
    "and",
    "as",
    "avg",
    "between",
    "case",
    "cast",
    "coalesce",
    "count",
    "cross",
    "delete",
    "distinct",
    "else",
    "end",
    "first",
    "full",
    "group",
    "having",
    "if",
    "in",
    "inner",
    "into",
    "is",
    "join",
    "last",
    "left",
    "max",
    "median",
    "min",
    "not",
    "null",
    "nullif",
    "on",
    "or",
    "order",
    "outer",
    "right",
    "round",
    "select",
    "set",
    "stdev",
    "sum",
    "then",
    "trunc",
    "update",
    "var",
    "when",
    "where",
    "with",
}
