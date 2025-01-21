"""Field extraction and processing utilities"""

from typing import Dict, List, Union

from datahub.ingestion.source.grafana.models import Panel
from datahub.metadata.schema_classes import (
    NumberTypeClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    StringTypeClass,
    TimeTypeClass,
)


def _deduplicate_fields(fields: List[SchemaFieldClass]) -> List[SchemaFieldClass]:
    """Remove duplicate fields based on fieldPath"""
    seen = set()
    unique_fields = []

    for field in fields:
        if field.fieldPath not in seen:
            seen.add(field.fieldPath)
            unique_fields.append(field)

    return unique_fields


def extract_sql_column_fields(target: Dict) -> List[SchemaFieldClass]:
    """Extract fields from SQL-style columns"""
    fields = []
    columns = target.get("sql", {}).get("columns", [])

    for col in columns:
        if not col.get("parameters"):
            continue

        for param in col["parameters"]:
            if not (param.get("name") and param.get("type") == "column"):
                continue

            field_type: Union[TimeTypeClass, NumberTypeClass, StringTypeClass]
            if col["type"] == "time":
                field_type = TimeTypeClass()
            elif col["type"] == "number":
                field_type = NumberTypeClass()
            else:
                field_type = StringTypeClass()

            fields.append(
                SchemaFieldClass(
                    fieldPath=param["name"],
                    type=SchemaFieldDataTypeClass(type=field_type),
                    nativeDataType=col["type"],
                )
            )

    return fields


def extract_prometheus_fields(target: Dict) -> List[SchemaFieldClass]:
    """Extract fields from Prometheus expressions"""
    if not target.get("expr"):
        return []

    return [
        SchemaFieldClass(
            fieldPath=target.get("legendFormat", target["expr"]),
            type=SchemaFieldDataTypeClass(type=NumberTypeClass()),
            nativeDataType="prometheus_metric",
        )
    ]


def extract_raw_sql_fields(target: Dict) -> List[SchemaFieldClass]:
    """Extract fields from raw SQL queries"""
    fields: List[SchemaFieldClass] = []
    raw_sql = target.get("rawSql")
    if not raw_sql:
        return fields

    try:
        sql = raw_sql.lower()
        select_start = sql.index("select") + 6
        from_start = sql.index("from")
        select_part = sql[select_start:from_start].strip()

        columns = [col.strip().split()[-1].strip() for col in select_part.split(",")]

        for col in columns:
            clean_col = col.split(" as ")[-1].strip('"').strip("'")
            fields.append(
                SchemaFieldClass(
                    fieldPath=clean_col,
                    type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                    nativeDataType="sql_column",
                )
            )
    except ValueError:
        pass

    return fields


def extract_time_format_fields(target: Dict) -> List[SchemaFieldClass]:
    """Extract fields from time series and table formats"""
    if target.get("format") not in ["time_series", "table"]:
        return []

    return [
        SchemaFieldClass(
            fieldPath="time",
            type=SchemaFieldDataTypeClass(type=TimeTypeClass()),
            nativeDataType="timestamp",
        )
    ]


def extract_fields_from_panel(panel: Panel) -> List[SchemaFieldClass]:
    """Extract all fields from a panel"""
    fields = []

    # Extract from targets
    for target in panel.targets:
        fields.extend(extract_sql_column_fields(target))
        fields.extend(extract_prometheus_fields(target))
        fields.extend(extract_raw_sql_fields(target))
        fields.extend(extract_time_format_fields(target))

    # Extract from field config
    fields.extend(get_fields_from_field_config(panel.field_config))

    # Extract from transformations
    fields.extend(get_fields_from_transformations(panel.transformations))

    return _deduplicate_fields(fields)


def get_fields_from_field_config(field_config: Dict) -> List[SchemaFieldClass]:
    """Extract fields from field configuration"""
    fields = []

    # Process defaults
    defaults = field_config.get("defaults", {})
    if "unit" in defaults:
        fields.append(
            SchemaFieldClass(
                fieldPath=f"value_{defaults.get('unit', 'unknown')}",
                type=SchemaFieldDataTypeClass(type=NumberTypeClass()),
                nativeDataType="value",
            )
        )

    # Process overrides
    for override in field_config.get("overrides", []):
        matcher = override.get("matcher", {})
        if matcher.get("id") == "byName":
            field_name = matcher.get("options")
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
    transformations: List[Dict],
) -> List[SchemaFieldClass]:
    """Extract fields from transformations"""
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
