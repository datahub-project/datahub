import logging
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
from datahub.ingestion.source.microstrategy.config import (
    ConnectionPlatformConfig,
    MicroStrategyConfig,
)
from datahub.ingestion.source.microstrategy.constants import (
    MSTR_CREATE_STATEMENT_RE,
    MSTR_HEX_OBJECT_ID_RE,
    MSTR_LINEAGE_IRRELEVANT_STATEMENT_RE,
    MSTR_LINEAGE_STOP_WORDS,
    MSTR_NAME_TOKEN_RE,
    MSTR_NON_ALNUM_RE,
    MSTR_SQL_IDENTIFIER_RE,
    MSTR_WHITESPACE_RE,
)
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
    convert_urns_to_lowercase: bool = True


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
        # upstream dataset urn -> {lower(fieldPath): fieldPath}, or None when the
        # warehouse dataset has no schema in the graph. Cached so each upstream is
        # fetched at most once per run.
        self._warehouse_field_cache: Dict[str, Optional[Dict[str, str]]] = {}

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
        # MicroStrategy SQL views are multi-pass: statements (often CREATE VOLATILE/TEMP
        # TABLE ... AS select) concatenated without semicolons. Parse per statement and
        # subtract CREATE targets so volatile intermediates never appear as upstreams. Only
        # CREATE targets are subtracted: a table read and written by the same DML
        # (self-referencing INSERT/MERGE) is a genuine upstream.
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
            if MSTR_CREATE_STATEMENT_RE.match(statement):
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
        graph: Optional["DataHubGraph"] = None,
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
                field_urns = self._upstream_field_urns(
                    _model_expression_field_names(fact),
                    upstream_dataset_urn,
                    context.convert_urns_to_lowercase,
                    graph,
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
                    field_urns = self._upstream_field_urns(
                        _model_expression_field_names(form),
                        upstream_dataset_urn,
                        context.convert_urns_to_lowercase,
                        graph,
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

    def warehouse_dataset_urn(
        self,
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
            name=qualified_name.lower()
            if context.convert_urns_to_lowercase
            else qualified_name,
        )

    def _upstream_field_urns(
        self,
        field_names: List[str],
        upstream_dataset_urn: str,
        convert_to_lowercase: bool,
        graph: Optional["DataHubGraph"],
    ) -> List[str]:
        # Prefer the warehouse dataset's real column casing from the graph so
        # schemaField urns byte-match regardless of the case MicroStrategy reports;
        # fall back to the casing heuristic when the schema is unavailable.
        schema_fields = self._warehouse_field_map(upstream_dataset_urn, graph)
        urns: List[str] = []
        for field_name in field_names:
            resolved = None
            if schema_fields is not None:
                resolved = schema_fields.get(field_name.lower())
            if resolved is None:
                resolved = field_name.lower() if convert_to_lowercase else field_name
            urns.append(builder.make_schema_field_urn(upstream_dataset_urn, resolved))
        return urns

    def _warehouse_field_map(
        self,
        upstream_dataset_urn: str,
        graph: Optional["DataHubGraph"],
    ) -> Optional[Dict[str, str]]:
        if graph is None:
            return None
        if upstream_dataset_urn in self._warehouse_field_cache:
            return self._warehouse_field_cache[upstream_dataset_urn]

        field_map: Optional[Dict[str, str]] = None
        try:
            schema = graph.get_schema_metadata(upstream_dataset_urn)
        except Exception as error:
            logger.debug(
                "Failed to fetch schema for %s from graph: %s",
                upstream_dataset_urn,
                error,
            )
            schema = None
        if schema is not None:
            # Later fields win on case-insensitive collisions, which is rare and
            # harmless since either casing anchors to the same column.
            field_map = {
                field.fieldPath.lower(): field.fieldPath for field in schema.fields
            }
        self._warehouse_field_cache[upstream_dataset_urn] = field_map
        return field_map

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
        # Dashboards built from many cubes sharing one object catalog (e.g. one cube per
        # time period) make every dataset "match" every visualization. An inference that
        # excludes nothing carries no signal, so treat it as unresolved rather than emit an
        # all-to-all fan-out. Two-dataset dashboards are exempt (a combined viz is plausible);
        # dashboard-level fallback remains via `emit_dashboard_dataset_edges`.
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


DatasourcePlatformMapping = Optional[Dict[str, ConnectionPlatformConfig]]


def _detail_for_names(
    mapping: DatasourcePlatformMapping,
    *names: Optional[str],
) -> Optional[ConnectionPlatformConfig]:
    if not mapping:
        return None
    for name in names:
        if name and name in mapping:
            return mapping[name]
    return None


def warehouse_context_from_datasources(
    datasources: List[Datasource],
    env: str,
    mapping: DatasourcePlatformMapping = None,
) -> Optional[WarehouseLineageContext]:
    resolved = (
        warehouse_context_from_datasource(datasource, env, mapping)
        for datasource in datasources
    )
    contexts = {context for context in resolved if context is not None}
    if len(contexts) == 1:
        return next(iter(contexts))
    return None


def warehouse_context_from_datasource(
    datasource: DatasourceReference,
    env: str,
    mapping: DatasourcePlatformMapping = None,
) -> Optional[WarehouseLineageContext]:
    detail = _detail_for_names(mapping, datasource.connection_name, datasource.name)
    platform = (
        detail.platform if detail and detail.platform else None
    ) or datahub_platform_for_datasource(datasource)
    if not platform:
        return None
    return WarehouseLineageContext(
        platform=platform,
        env=(detail.env if detail and detail.env else env),
        platform_instance=detail.platform_instance if detail else None,
        database=datasource.database_name,
        schema=datasource.schema_name,
        convert_urns_to_lowercase=(
            detail.convert_urns_to_lowercase if detail else True
        ),
    )


def matching_datasource_for_context(
    datasources: List[Datasource],
    context: WarehouseLineageContext,
    mapping: DatasourcePlatformMapping = None,
) -> Optional[Datasource]:
    for datasource in datasources:
        if (
            warehouse_context_from_datasource(datasource, context.env, mapping)
            == context
        ):
            return datasource
    return None


def warehouse_context_with_connection(
    context: WarehouseLineageContext,
    connection: DatasourceConnection,
    mapping: DatasourcePlatformMapping = None,
) -> WarehouseLineageContext:
    detail = _detail_for_names(mapping, connection.name)
    platform = (
        (detail.platform if detail and detail.platform else None)
        or datahub_platform_for_connection(connection)
        or context.platform
    )
    return WarehouseLineageContext(
        platform=platform,
        env=(detail.env if detail and detail.env else context.env),
        platform_instance=(
            detail.platform_instance if detail else context.platform_instance
        ),
        database=connection.database_name or context.database,
        schema=connection.schema_name or context.schema,
        convert_urns_to_lowercase=(
            detail.convert_urns_to_lowercase
            if detail
            else context.convert_urns_to_lowercase
        ),
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


def _is_lineage_irrelevant_statement(statement: str) -> bool:
    return bool(MSTR_LINEAGE_IRRELEVANT_STATEMENT_RE.match(statement))


def _lineage_name_tokens(name: str) -> Set[str]:
    return {
        token
        for token in MSTR_NAME_TOKEN_RE.findall(name.upper())
        if len(token) > 1 and token not in MSTR_LINEAGE_STOP_WORDS
    }


def _normalize_source_type(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    return MSTR_NON_ALNUM_RE.sub("_", value.strip().lower()).strip("_")


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
    # The warehouse context carries the real database (from the JDBC
    # connection); MicroStrategy's table "namespace" is a logical namespace
    # (e.g. "SALES_DM_1" with a dedup suffix), so it is only a last resort.
    return qualify_table_name(
        table_name,
        database=context.database
        or _clean_identifier_part(physical_table.get("namespace")),
        schema=_clean_identifier_part(physical_table.get("tablePrefix"))
        or context.schema,
    )


def _model_expression_field_names(item: Dict[str, object]) -> List[str]:
    expression = item.get("expression")
    text = ""
    if isinstance(expression, dict):
        text = str(expression.get("text") or "")
    return extract_field_names_from_expression(text)


def extract_field_names_from_expression(expression: str) -> List[str]:
    if not expression.strip():
        return []
    identifiers = [
        identifier
        for identifier in MSTR_SQL_IDENTIFIER_RE.findall(expression)
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
    """Map each derived-object id to the single dataset that defines it. Only an entry's
    own id and its ``embeddedObjects`` ids count — ids merely referenced from expression
    trees may be shared-catalog objects and would mis-attribute ownership. Ids seen in more
    than one dataset carry no signal and are dropped. Keys are upper-cased object ids."""
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
    """Set ``visualization.datasets`` from dataset-scoped derived-object ids, ignoring ids
    in any dataset's shared object catalog (they cannot discriminate). Returns the count bound."""
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
        owners: Set[str] = set()
        for object_id in visualization.object_ids:
            normalized = _normalize_object_id(object_id)
            if (
                normalized in owner_by_derived_id
                and normalized not in shared_catalog_ids
            ):
                owners.add(owner_by_derived_id[normalized])
        owners &= dataset_ids
        if owners:
            visualization.datasets = sorted(owners)
            bound += 1
    return bound


def _add_hex_object_id(value: object, out: Set[str]) -> None:
    if isinstance(value, str) and MSTR_HEX_OBJECT_ID_RE.fullmatch(value):
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
    return MSTR_WHITESPACE_RE.sub(" ", value).strip().lower()


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
