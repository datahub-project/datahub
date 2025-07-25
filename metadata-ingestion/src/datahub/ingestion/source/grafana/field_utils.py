import logging
from typing import Any, Dict, List, Optional, Union

from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.source.grafana.models import Panel
from datahub.metadata.schema_classes import (
    NumberTypeClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    StringTypeClass,
    TimeTypeClass,
)
from datahub.sql_parsing.sqlglot_lineage import (
    create_lineage_sql_parsed_result,
    infer_output_schema,
)

logger = logging.getLogger(__name__)


def extract_sql_column_fields(target: Dict[str, Any]) -> List[SchemaFieldClass]:
    """Extract fields from SQL-style columns."""
    fields = []
    for col in target.get("sql", {}).get("columns", []):
        for param in col.get("parameters", []):
            if param.get("type") == "column" and param.get("name"):
                field_type: Union[NumberTypeClass, StringTypeClass, TimeTypeClass] = (
                    TimeTypeClass()
                    if col["type"] == "time"
                    else NumberTypeClass()
                    if col["type"] == "number"
                    else StringTypeClass()
                )
                fields.append(
                    SchemaFieldClass(
                        fieldPath=param["name"],
                        type=SchemaFieldDataTypeClass(type=field_type),
                        nativeDataType=col["type"],
                    )
                )
    return fields


def extract_prometheus_fields(target: Dict[str, Any]) -> List[SchemaFieldClass]:
    """Extract fields from Prometheus expressions."""
    expr = target.get("expr")
    if expr:
        legend = target.get("legendFormat", expr)
        return [
            SchemaFieldClass(
                fieldPath=legend,
                type=SchemaFieldDataTypeClass(type=NumberTypeClass()),
                nativeDataType="prometheus_metric",
            )
        ]
    return []


def extract_raw_sql_fields(
    target: Dict[str, Any],
    panel: Optional[Panel] = None,
    connection_to_platform_map: Optional[Dict[str, Any]] = None,
    graph: Optional[DataHubGraph] = None,
    report: Optional[Any] = None,
) -> List[SchemaFieldClass]:
    """Extract fields from raw SQL queries using DataHub's SQL parsing."""
    raw_sql = target.get("rawSql", "")
    if not raw_sql:
        return []

    # Determine upstream platform and environment from datasource mapping
    platform = "unknown"
    env = "PROD"
    default_db = None
    default_schema = None
    platform_instance = None
    schema_aware = False

    if panel and panel.datasource_ref and connection_to_platform_map:
        ds_type = panel.datasource_ref.type or "unknown"
        ds_uid = panel.datasource_ref.uid or "unknown"

        # Try to find mapping by datasource UID first, then by type
        platform_config = connection_to_platform_map.get(
            ds_uid
        ) or connection_to_platform_map.get(ds_type)

        if platform_config:
            platform = platform_config.platform
            env = getattr(platform_config, "env", env)
            default_db = getattr(platform_config, "database", None)
            default_schema = getattr(platform_config, "database_schema", None)
            platform_instance = getattr(platform_config, "platform_instance", None)

            # Enable schema-aware parsing if we have platform mapping and graph access
            if graph and platform != "unknown":
                schema_aware = True

    # Track SQL parsing attempt
    if report:
        report.report_sql_parsing_attempt()

    try:
        # Use DataHub's standard SQL parsing approach
        sql_parsing_result = create_lineage_sql_parsed_result(
            query=raw_sql,
            default_db=default_db,
            default_schema=default_schema,
            platform=platform,
            platform_instance=platform_instance,
            env=env,
            schema_aware=schema_aware,
            graph=graph,
        )

        # Extract the output schema from the parsing result
        output_schema = infer_output_schema(sql_parsing_result)

        if output_schema:
            if report:
                report.report_sql_parsing_success()
            return output_schema
        else:
            # If sqlglot parsing succeeds but no schema is inferred,
            # fall back to basic parsing
            logger.debug(f"No schema inferred from SQL: {raw_sql}")
            fallback_result = _extract_raw_sql_fields_fallback(target)
            if fallback_result and report:
                report.report_sql_parsing_success()
            elif report:
                report.report_sql_parsing_failure()
            return fallback_result

    except Exception as e:
        logger.debug(f"Failed to parse SQL with DataHub parser: {raw_sql}, error: {e}")
        if report:
            report.report_sql_parsing_failure()
        # Fallback to basic parsing for backwards compatibility
        return _extract_raw_sql_fields_fallback(target)


def _extract_raw_sql_fields_fallback(target: Dict[str, Any]) -> List[SchemaFieldClass]:
    """Fallback basic SQL parsing for when sqlglot fails."""
    raw_sql = target.get("rawSql", "").lower()
    if not raw_sql:
        return []

    try:
        sql = raw_sql.lower()
        select_start = sql.index("select") + 6  # len("select")
        from_start = sql.index("from")
        select_part = sql[select_start:from_start].strip()

        # Split by comma, handling nested parentheses
        columns = []
        current_column = ""
        paren_count = 0

        for char in select_part:
            if char == "," and paren_count == 0:
                if current_column.strip():
                    columns.append(current_column.strip())
                current_column = ""
            else:
                if char == "(":
                    paren_count += 1
                elif char == ")":
                    paren_count -= 1
                current_column += char

        if current_column.strip():
            columns.append(current_column.strip())

        # For each column, extract the alias if it exists
        fields = []
        for col in columns:
            # Check for alias with 'AS' keyword
            if " as " in col:
                field_name = col.split(" as ")[-1].strip()
            else:
                # If no alias, use the last part after last space
                # This handles both simple columns and function calls without alias
                field_name = col.split()[-1].strip()

            # Clean up any remaining quotes or parentheses
            field_name = field_name.strip("\"'()")

            fields.append(
                SchemaFieldClass(
                    fieldPath=field_name,
                    type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                    nativeDataType="sql_column",
                )
            )

        return fields

    except (IndexError, ValueError, StopIteration) as e:
        logger.warning(f"Failed to parse SQL: {target.get('rawSql')}", e)
        return []


def extract_fields_from_panel(
    panel: Panel,
    connection_to_platform_map: Optional[Dict[str, Any]] = None,
    graph: Optional[DataHubGraph] = None,
    report: Optional[Any] = None,
) -> List[SchemaFieldClass]:
    """Extract all fields from a panel."""
    fields = []
    fields.extend(
        extract_fields_from_targets(
            panel.query_targets, panel, connection_to_platform_map, graph, report
        )
    )
    fields.extend(get_fields_from_field_config(panel.field_config))
    fields.extend(get_fields_from_transformations(panel.transformations))

    # Track schema field extraction
    if report:
        if fields:
            report.report_schema_fields_extracted()
        else:
            report.report_no_schema_fields()

    return fields


def extract_fields_from_targets(
    targets: List[Dict[str, Any]],
    panel: Optional[Panel] = None,
    connection_to_platform_map: Optional[Dict[str, Any]] = None,
    graph: Optional[DataHubGraph] = None,
    report: Optional[Any] = None,
) -> List[SchemaFieldClass]:
    """Extract fields from panel targets."""
    fields = []
    for target in targets:
        fields.extend(extract_sql_column_fields(target))
        fields.extend(extract_prometheus_fields(target))
        fields.extend(
            extract_raw_sql_fields(
                target, panel, connection_to_platform_map, graph, report
            )
        )
        fields.extend(extract_time_format_fields(target))
    return fields


def extract_time_format_fields(target: Dict[str, Any]) -> List[SchemaFieldClass]:
    """Extract fields from time series and table formats."""
    if target.get("format") in {"time_series", "table"}:
        return [
            SchemaFieldClass(
                fieldPath="time",
                type=SchemaFieldDataTypeClass(type=TimeTypeClass()),
                nativeDataType="timestamp",
            )
        ]
    return []


def get_fields_from_field_config(
    field_config: Dict[str, Any],
) -> List[SchemaFieldClass]:
    """Extract fields from field configuration."""
    fields = []
    defaults = field_config.get("defaults", {})
    unit = defaults.get("unit")
    if unit:
        fields.append(
            SchemaFieldClass(
                fieldPath=f"value_{unit}",
                type=SchemaFieldDataTypeClass(type=NumberTypeClass()),
                nativeDataType="value",
            )
        )
    for override in field_config.get("overrides", []):
        if override.get("matcher", {}).get("id") == "byName":
            field_name = override.get("matcher", {}).get("options")
            if field_name:
                fields.append(
                    SchemaFieldClass(
                        fieldPath=field_name,
                        type=SchemaFieldDataTypeClass(type=NumberTypeClass()),
                        nativeDataType="metric",
                    )
                )
    return fields


def get_fields_from_transformations(
    transformations: List[Dict[str, Any]],
) -> List[SchemaFieldClass]:
    """Extract fields from transformations."""
    fields = []
    for transform in transformations:
        if transform.get("type") == "organize":
            for field_name in transform.get("options", {}).get("indexByName", {}):
                fields.append(
                    SchemaFieldClass(
                        fieldPath=field_name,
                        type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                        nativeDataType="transformed",
                    )
                )
    return fields
