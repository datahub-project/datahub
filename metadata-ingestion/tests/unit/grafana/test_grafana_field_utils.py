import logging

import pytest

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


def test_extract_raw_sql_fields_invalid(caplog):
    # Test with completely invalid SQL
    target = {"rawSql": "INVALID SQL"}
    caplog.set_level(logging.WARNING)

    with pytest.raises(
        TypeError, match="not all arguments converted during string formatting"
    ):
        extract_raw_sql_fields(target)

    # The initial ValueError occurred but was caught
    assert "Failed to parse SQL: INVALID SQL" in str(caplog.records)


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


def test_get_fields_from_field_config_empty():
    """Test that get_fields_from_field_config handles empty dict input."""
    fields = get_fields_from_field_config({})
    assert fields == []


def test_get_fields_from_field_config_none():
    """Test that get_fields_from_field_config handles empty dict input (via safe property)."""
    # The safe_field_config property ensures we never pass None to this function
    fields = get_fields_from_field_config({})
    assert fields == []


def test_panel_safe_field_config_property():
    """Test that Panel.safe_field_config always returns a dict, never None."""
    from datahub.ingestion.source.grafana.models import Panel

    # Test with explicit None field_config (should be converted by model validator)
    panel_data = {
        "id": "test1",
        "type": "text",
        "fieldConfig": None,  # This should be converted to {} by the model validator
    }

    panel = Panel.model_validate(panel_data)

    # The safe_field_config property should always return a dict
    safe_config = panel.safe_field_config
    assert isinstance(safe_config, dict)
    assert safe_config == {}  # Should be empty dict, not None

    # The actual field_config should also be a dict after validation
    assert isinstance(panel.field_config, dict)
    assert panel.field_config == {}


def test_extract_fields_from_panel_with_empty_fields():
    """Test that extract_fields_from_panel handles panels with empty fields efficiently."""
    from datahub.ingestion.source.grafana.field_utils import extract_fields_from_panel
    from datahub.ingestion.source.grafana.models import Panel

    # Create a text panel with no targets, fieldConfig, or transformations
    panel_data = {
        "id": 1,
        "type": "text",
        "title": "Test Panel",
    }

    panel = Panel.model_validate(panel_data)

    # Verify safe properties work
    assert panel.safe_field_config == {}
    assert panel.safe_query_targets == []
    assert panel.safe_transformations == []

    # Should extract no fields but not crash
    fields = extract_fields_from_panel(panel)
    assert isinstance(fields, list)
    assert len(fields) == 0  # No fields to extract from a text panel


def test_datasource_ref_field_access():
    """Test that datasource_ref fields are accessed correctly without getattr."""
    from datahub.ingestion.source.grafana.models import Panel

    # Test panel with datasource_ref having both type and uid
    panel_with_datasource = Panel.model_validate(
        {
            "id": 1,
            "type": "graph",
            "datasource": {"type": "prometheus", "uid": "prometheus-uid-123"},
        }
    )

    # Verify we can access fields directly
    assert panel_with_datasource.datasource_ref is not None
    assert panel_with_datasource.datasource_ref.type == "prometheus"
    assert panel_with_datasource.datasource_ref.uid == "prometheus-uid-123"

    # Test panel with partial datasource_ref (only type)
    panel_with_partial_datasource = Panel.model_validate(
        {
            "id": 2,
            "type": "graph",
            "datasource": {
                "type": "mysql"
                # No uid field
            },
        }
    )

    assert panel_with_partial_datasource.datasource_ref is not None
    assert panel_with_partial_datasource.datasource_ref.type == "mysql"
    assert panel_with_partial_datasource.datasource_ref.uid is None

    # Test panel with no datasource_ref
    panel_without_datasource = Panel.model_validate(
        {
            "id": 3,
            "type": "text",
            # No datasource field
        }
    )

    assert panel_without_datasource.datasource_ref is None


def test_extract_raw_sql_fields_with_text_panel():
    """Test that extract_raw_sql_fields handles text panels (no datasource) correctly."""
    from datahub.ingestion.source.grafana.field_utils import extract_raw_sql_fields
    from datahub.ingestion.source.grafana.models import Panel

    # Create a text panel with no datasource (like in real Grafana)
    text_panel = Panel.model_validate(
        {
            "id": 1,
            "type": "text",
            "title": "Text Panel",
            # No datasource field - this is normal for text panels
        }
    )

    # Should not crash when processing a target with rawSql but no datasource
    target_with_sql = {"rawSql": "SELECT * FROM test_table"}

    # This should work without error, even though panel.datasource_ref is None
    fields = extract_raw_sql_fields(
        target=target_with_sql,
        panel=text_panel,
        connection_to_platform_map={"postgres": "some_config"},
        graph=None,
        report=None,
    )

    # Should return some fields (fallback parsing) or empty list, but not crash
    assert isinstance(fields, list)
