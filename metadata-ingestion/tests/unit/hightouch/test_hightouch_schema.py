from datetime import datetime
from unittest.mock import Mock, patch

import pytest

from datahub.ingestion.source.hightouch.config import (
    HightouchSourceReport,
    PlatformDetail,
)
from datahub.ingestion.source.hightouch.hightouch_schema import HightouchSchemaHandler
from datahub.ingestion.source.hightouch.models import (
    HightouchModel,
    HightouchSchemaField,
    HightouchSourceConnection,
)
from datahub.ingestion.source.hightouch.urn_builder import HightouchUrnBuilder


@pytest.fixture
def report():
    return HightouchSourceReport()


@pytest.fixture
def mock_graph():
    graph = Mock()
    graph.get_schema_metadata = Mock(return_value=None)
    return graph


@pytest.fixture
def mock_urn_builder():
    return Mock(spec=HightouchUrnBuilder)


def test_mark_primary_key_in_schema(report, mock_graph, mock_urn_builder):
    handler = HightouchSchemaHandler(
        report=report, graph=mock_graph, urn_builder=mock_urn_builder
    )

    schema_fields = [
        HightouchSchemaField(name="id", type="INTEGER", description="User ID"),
        HightouchSchemaField(name="email", type="STRING"),
        HightouchSchemaField(name="name", type="STRING"),
    ]

    result = handler._mark_primary_key_in_schema(schema_fields, "id")

    assert len(result) == 3
    assert result[0].is_primary_key is True
    assert result[0].description == "User ID (Primary Key)"
    assert result[1].is_primary_key is False
    assert result[2].is_primary_key is False


def test_mark_primary_key_in_schema_no_description(
    report, mock_graph, mock_urn_builder
):
    handler = HightouchSchemaHandler(
        report=report, graph=mock_graph, urn_builder=mock_urn_builder
    )

    schema_fields = [
        HightouchSchemaField(name="id", type="INTEGER"),
        HightouchSchemaField(name="email", type="STRING"),
    ]

    result = handler._mark_primary_key_in_schema(schema_fields, "id")

    assert result[0].is_primary_key is True
    assert result[0].description == "Primary Key"


def test_mark_primary_key_case_insensitive(report, mock_graph, mock_urn_builder):
    handler = HightouchSchemaHandler(
        report=report, graph=mock_graph, urn_builder=mock_urn_builder
    )

    schema_fields = [
        HightouchSchemaField(name="USER_ID", type="INTEGER"),
        HightouchSchemaField(name="email", type="STRING"),
    ]

    result = handler._mark_primary_key_in_schema(schema_fields, "user_id")

    assert result[0].is_primary_key is True
    assert result[0].description == "Primary Key"


def test_schema_from_referenced_columns(report, mock_graph, mock_urn_builder):
    handler = HightouchSchemaHandler(
        report=report, graph=mock_graph, urn_builder=mock_urn_builder
    )

    referenced_columns = ["user_id", "email", "created_at"]

    result = handler._schema_from_referenced_columns(referenced_columns)

    assert result is not None
    assert len(result) == 3
    assert result[0].name == "user_id"
    assert result[0].type == "STRING"
    assert result[0].is_primary_key is False


def test_schema_from_referenced_columns_with_primary_key(
    report, mock_graph, mock_urn_builder
):
    handler = HightouchSchemaHandler(
        report=report, graph=mock_graph, urn_builder=mock_urn_builder
    )

    referenced_columns = ["user_id", "email", "created_at"]

    result = handler._schema_from_referenced_columns(
        referenced_columns, primary_key="user_id"
    )

    assert result is not None
    assert len(result) == 3
    assert result[0].name == "user_id"
    assert result[0].is_primary_key is True
    assert result[0].description == "Primary Key"
    assert result[1].is_primary_key is False


def test_schema_from_referenced_columns_empty(report, mock_graph, mock_urn_builder):
    handler = HightouchSchemaHandler(
        report=report, graph=mock_graph, urn_builder=mock_urn_builder
    )

    result = handler._schema_from_referenced_columns([])

    assert result is None


@patch("datahub.ingestion.source.hightouch.hightouch.HightouchAPIClient")
def test_fetch_schema_from_datahub_table_model(
    mock_api_client_class, report, mock_urn_builder
):
    mock_graph = Mock()
    mock_schema_metadata = Mock()
    mock_schema_metadata.fields = [
        Mock(fieldPath="id", nativeDataType="INTEGER", description="User ID"),
        Mock(fieldPath="email", nativeDataType="VARCHAR", description=None),
        Mock(fieldPath="created_at", nativeDataType="TIMESTAMP", description=None),
    ]
    mock_graph.get_schema_metadata.return_value = mock_schema_metadata

    mock_urn_builder._get_cached_source_details.return_value = PlatformDetail(
        platform="snowflake",
        platform_instance="prod",
        env="PROD",
        database="raw_data",
        include_schema_in_urn=False,
    )
    mock_urn_builder.make_upstream_table_urn.return_value = (
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,raw_data.customers,PROD)"
    )

    handler = HightouchSchemaHandler(
        report=report, graph=mock_graph, urn_builder=mock_urn_builder
    )

    model = HightouchModel(
        id="model_1",
        name="customers",
        slug="customers",
        workspace_id="workspace_1",
        source_id="source_1",
        query_type="table",
        created_at=datetime(2023, 1, 1),
        updated_at=datetime(2023, 1, 2),
    )

    source = HightouchSourceConnection(
        id="source_1",
        name="Snowflake Source",
        slug="snowflake-source",
        type="snowflake",
        workspace_id="workspace_1",
        created_at=datetime(2023, 1, 1),
        updated_at=datetime(2023, 1, 2),
        configuration={"database": "raw_data"},
    )

    result = handler._fetch_schema_from_datahub(model, source)

    assert result is not None
    assert len(result) == 3
    assert result[0].name == "id"
    assert result[0].type == "INTEGER"
    assert result[0].description == "User ID"
    assert result[1].name == "email"
    assert result[1].type == "VARCHAR"
    assert result[1].description is None


@patch("datahub.ingestion.source.hightouch.hightouch.HightouchAPIClient")
def test_fetch_schema_from_datahub_not_found(
    mock_api_client_class, report, mock_urn_builder
):
    mock_graph = Mock()
    mock_graph.get_schema_metadata.return_value = None

    mock_urn_builder._get_cached_source_details.return_value = PlatformDetail(
        platform="snowflake",
        platform_instance="prod",
        env="PROD",
        database="raw_data",
    )
    mock_urn_builder.make_upstream_table_urn.return_value = (
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,raw_data.customers,PROD)"
    )

    handler = HightouchSchemaHandler(
        report=report, graph=mock_graph, urn_builder=mock_urn_builder
    )

    model = HightouchModel(
        id="model_1",
        name="customers",
        slug="customers",
        workspace_id="workspace_1",
        source_id="source_1",
        query_type="table",
        created_at=datetime(2023, 1, 1),
        updated_at=datetime(2023, 1, 2),
    )

    source = HightouchSourceConnection(
        id="source_1",
        name="Snowflake Source",
        slug="snowflake-source",
        type="snowflake",
        workspace_id="workspace_1",
        created_at=datetime(2023, 1, 1),
        updated_at=datetime(2023, 1, 2),
        configuration={"database": "raw_data"},
    )

    result = handler._fetch_schema_from_datahub(model, source)

    assert result is None
    assert report.model_schemas_datahub_not_found


def test_resolve_schema_from_query_schema(report, mock_urn_builder):
    handler = HightouchSchemaHandler(
        report=report, graph=None, urn_builder=mock_urn_builder
    )

    model = HightouchModel(
        id="model_1",
        name="Test Model",
        slug="test-model",
        workspace_id="workspace_1",
        source_id="source_1",
        query_type="raw_sql",
        created_at=datetime(2023, 1, 1),
        updated_at=datetime(2023, 1, 2),
        query_schema=[
            {"name": "id", "type": "INTEGER"},
            {"name": "email", "type": "STRING"},
        ],
    )

    result = handler.resolve_schema(model, source=None)

    assert result is not None
    assert len(result) == 2
    assert result[0].name == "id"
    assert result[0].type == "INTEGER"


def test_resolve_schema_fallback_to_referenced_columns(report, mock_urn_builder):
    handler = HightouchSchemaHandler(
        report=report, graph=None, urn_builder=mock_urn_builder
    )

    model = HightouchModel(
        id="model_1",
        name="Test Model",
        slug="test-model",
        workspace_id="workspace_1",
        source_id="source_1",
        query_type="raw_sql",
        created_at=datetime(2023, 1, 1),
        updated_at=datetime(2023, 1, 2),
    )

    referenced_columns = ["user_id", "email", "name"]

    result = handler.resolve_schema(
        model, source=None, referenced_columns=referenced_columns
    )

    assert result is not None
    assert len(result) == 3
    assert result[0].name == "user_id"
    assert result[0].type == "STRING"


def test_resolve_schema_with_primary_key_marking(report, mock_urn_builder):
    handler = HightouchSchemaHandler(
        report=report, graph=None, urn_builder=mock_urn_builder
    )

    model = HightouchModel(
        id="model_1",
        name="Test Model",
        slug="test-model",
        workspace_id="workspace_1",
        source_id="source_1",
        query_type="raw_sql",
        created_at=datetime(2023, 1, 1),
        updated_at=datetime(2023, 1, 2),
        primary_key="id",
        query_schema=[
            {"name": "id", "type": "INTEGER"},
            {"name": "email", "type": "STRING"},
        ],
    )

    result = handler.resolve_schema(model, source=None)

    assert result is not None
    assert len(result) == 2
    assert result[0].is_primary_key is True
    assert result[0].description == "Primary Key"
    assert result[1].is_primary_key is False


def test_resolve_schema_returns_none_when_no_schema_available(report, mock_urn_builder):
    handler = HightouchSchemaHandler(
        report=report, graph=None, urn_builder=mock_urn_builder
    )

    model = HightouchModel(
        id="model_1",
        name="Test Model",
        slug="test-model",
        workspace_id="workspace_1",
        source_id="source_1",
        query_type="raw_sql",
        created_at=datetime(2023, 1, 1),
        updated_at=datetime(2023, 1, 2),
    )

    result = handler.resolve_schema(model, source=None)

    assert result is None


def test_get_first_value_helper(report, mock_graph, mock_urn_builder):
    handler = HightouchSchemaHandler(
        report=report, graph=mock_graph, urn_builder=mock_urn_builder
    )

    data = {
        "fieldName": "user_id",
        "name": "id",
        "column_name": "col_id",
    }

    result = handler._get_first_value(data, ["name", "fieldName", "column_name"])
    assert result == "id"

    result = handler._get_first_value(data, ["missing", "fieldName"])
    assert result == "user_id"

    result = handler._get_first_value(data, ["missing1", "missing2"])
    assert result is None
