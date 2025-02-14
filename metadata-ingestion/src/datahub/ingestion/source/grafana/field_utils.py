import logging
from typing import Any, Dict, List, Union

from datahub.ingestion.source.grafana.models import Panel
from datahub.metadata.schema_classes import (
    NumberTypeClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    StringTypeClass,
    TimeTypeClass,
)

logger = logging.getLogger(__name__)


def _deduplicate_fields(fields: List[SchemaFieldClass]) -> List[SchemaFieldClass]:
    """Remove duplicate fields based on fieldPath while preserving order."""
    unique_fields = {field.fieldPath: field for field in fields}
    return list(unique_fields.values())


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


def extract_raw_sql_fields(target: Dict[str, Any]) -> List[SchemaFieldClass]:
    """Extract fields from raw SQL queries using SQL parsing."""
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


def extract_fields_from_panel(panel: Panel) -> List[SchemaFieldClass]:
    """Extract all fields from a panel."""
    fields = []
    fields.extend(extract_fields_from_targets(panel.targets))
    fields.extend(get_fields_from_field_config(panel.field_config))
    fields.extend(get_fields_from_transformations(panel.transformations))
    return _deduplicate_fields(fields)


def extract_fields_from_targets(
    targets: List[Dict[str, Any]],
) -> List[SchemaFieldClass]:
    """Extract fields from panel targets."""
    fields = []
    for target in targets:
        fields.extend(extract_sql_column_fields(target))
        fields.extend(extract_prometheus_fields(target))
        fields.extend(extract_raw_sql_fields(target))
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
