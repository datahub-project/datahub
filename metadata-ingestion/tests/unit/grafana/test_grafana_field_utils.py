from datahub.ingestion.source.grafana.field_utils import (
    extract_prometheus_fields,
    extract_raw_sql_fields,
    extract_sql_column_fields,
    extract_time_format_fields,
    get_fields_from_field_config,
    get_fields_from_transformations,
)
from datahub.metadata.schema_classes import (
    NumberTypeClass,
    StringTypeClass,
    TimeTypeClass,
)


def test_extract_sql_column_fields():
    target = {
        "sql": {
            "columns": [
                {
                    "type": "time",
                    "parameters": [{"type": "column", "name": "timestamp"}],
                },
                {"type": "number", "parameters": [{"type": "column", "name": "value"}]},
                {"type": "string", "parameters": [{"type": "column", "name": "name"}]},
            ]
        }
    }

    fields = extract_sql_column_fields(target)

    assert len(fields) == 3
    assert fields[0].fieldPath == "timestamp"
    assert isinstance(fields[0].type.type, TimeTypeClass)
    assert fields[1].fieldPath == "value"
    assert isinstance(fields[1].type.type, NumberTypeClass)
    assert fields[2].fieldPath == "name"
    assert isinstance(fields[2].type.type, StringTypeClass)


def test_extract_prometheus_fields():
    target = {
        "expr": "sum(rate(http_requests_total[5m]))",
        "legendFormat": "HTTP Requests",
    }

    fields = extract_prometheus_fields(target)

    assert len(fields) == 1
    assert fields[0].fieldPath == "HTTP Requests"
    assert isinstance(fields[0].type.type, NumberTypeClass)
    assert fields[0].nativeDataType == "prometheus_metric"


def test_extract_raw_sql_fields():
    target = {
        "rawSql": "SELECT name as user_name, count as request_count FROM requests"
    }

    fields = extract_raw_sql_fields(target)
    assert len(fields) == 2
    assert fields[0].fieldPath == "user_name"
    assert fields[1].fieldPath == "request_count"


def test_extract_raw_sql_fields_invalid():
    target = {"rawSql": "INVALID SQL"}

    fields = extract_raw_sql_fields(target)
    assert len(fields) == 0


def test_extract_time_format_fields():
    target = {"format": "time_series"}
    fields = extract_time_format_fields(target)

    assert len(fields) == 1
    assert fields[0].fieldPath == "time"
    assert isinstance(fields[0].type.type, TimeTypeClass)
    assert fields[0].nativeDataType == "timestamp"


def test_get_fields_from_field_config():
    field_config = {
        "defaults": {"unit": "bytes"},
        "overrides": [{"matcher": {"id": "byName", "options": "memory_usage"}}],
    }

    fields = get_fields_from_field_config(field_config)

    assert len(fields) == 2
    assert fields[0].fieldPath == "value_bytes"
    assert fields[1].fieldPath == "memory_usage"


def test_get_fields_from_transformations():
    transformations = [
        {
            "type": "organize",
            "options": {"indexByName": {"user": "user", "value": "value"}},
        }
    ]

    fields = get_fields_from_transformations(transformations)
    assert len(fields) == 2
    field_paths = {f.fieldPath for f in fields}
    assert field_paths == {"user", "value"}
