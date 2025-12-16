from unittest.mock import MagicMock, patch

import pydantic
import pytest
from requests.exceptions import HTTPError

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.metabase import (
    DATASOURCE_URN_RECURSION_LIMIT,
    MetabaseConfig,
    MetabaseReport,
    MetabaseSource,
)
from datahub.metadata.com.linkedin.pegasus2avro.common import AuditStamp
from datahub.metadata.schema_classes import (
    GlobalTagsClass,
    UpstreamLineageClass,
)


class TestMetabaseSource(MetabaseSource):
    def __init__(self, ctx: PipelineContext, config: MetabaseConfig):
        self.config = config
        self.report = MetabaseReport()


def test_get_platform_instance():
    ctx = PipelineContext(run_id="test-metabase")
    config = MetabaseConfig()
    config.connect_uri = "http://localhost:3000"
    # config.database_id_to_instance_map = {"42": "my_main_clickhouse"}
    # config.platform_instance_map = {"clickhouse": "my_only_clickhouse"}
    metabase = TestMetabaseSource(ctx, config)

    # no mappings defined
    assert metabase.get_platform_instance("clickhouse", 42) is None

    # database_id_to_instance_map is defined, key is present
    metabase.config.database_id_to_instance_map = {"42": "my_main_clickhouse"}
    assert metabase.get_platform_instance(None, 42) == "my_main_clickhouse"

    # database_id_to_instance_map is defined, key is missing
    assert metabase.get_platform_instance(None, 999) is None

    # database_id_to_instance_map is defined, key is missing, platform_instance_map is defined and key present
    metabase.config.platform_instance_map = {"clickhouse": "my_only_clickhouse"}
    assert metabase.get_platform_instance("clickhouse", 999) == "my_only_clickhouse"

    # database_id_to_instance_map is defined, key is missing, platform_instance_map is defined and key missing
    assert metabase.get_platform_instance("missing-platform", 999) is None

    # database_id_to_instance_map is missing, platform_instance_map is defined and key present
    metabase.config.database_id_to_instance_map = None
    assert metabase.get_platform_instance("clickhouse", 999) == "my_only_clickhouse"

    # database_id_to_instance_map is missing, platform_instance_map is defined and key missing
    assert metabase.get_platform_instance("missing-platform", 999) is None


def test_set_display_uri():
    display_uri = "some_host:1234"

    config = MetabaseConfig.model_validate({"display_uri": display_uri})

    assert config.connect_uri == "localhost:3000"
    assert config.display_uri == display_uri


@patch("requests.session")
def test_connection_uses_api_key_if_in_config(mock_session):
    metabase_config = MetabaseConfig(
        connect_uri="localhost:3000", api_key=pydantic.SecretStr("key")
    )
    ctx = PipelineContext(run_id="metabase-test-apikey")

    mock_session_instance = MagicMock()
    mock_session_instance.headers = {}
    mock_session.return_value = mock_session_instance

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_session_instance.get.return_value = mock_response

    metabase_source = MetabaseSource(ctx, metabase_config)
    metabase_source.close()

    mock_session_instance.get.assert_called_once_with("localhost:3000/api/user/current")
    request_headers = mock_session_instance.headers
    assert request_headers["x-api-key"] == "key"


@patch("requests.delete")
@patch("requests.Session.get")
@patch("requests.post")
def test_create_session_from_config_username_password(mock_post, mock_get, mock_delete):
    metabase_config = MetabaseConfig(
        connect_uri="localhost:3000", username="un", password=pydantic.SecretStr("pwd")
    )
    ctx = PipelineContext(run_id="metabase-test")

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_get.return_value = mock_response
    mock_post.return_value = mock_response
    mock_delete.return_value = mock_response

    metabase_source = MetabaseSource(ctx, metabase_config)
    metabase_source.close()

    kwargs_post = mock_post.call_args
    assert kwargs_post[0][0] == "localhost:3000/api/session"
    assert kwargs_post[0][2]["password"] == "pwd"
    assert kwargs_post[0][2]["username"] == "un"

    kwargs_get = mock_get.call_args
    assert kwargs_get[0][0] == "localhost:3000/api/user/current"

    mock_delete.assert_called_once()


@patch("requests.delete")
@patch("requests.Session.get")
@patch("requests.post")
def test_fail_session_delete(mock_post, mock_get, mock_delete):
    metabase_config = MetabaseConfig(
        connect_uri="localhost:3000", username="un", password=pydantic.SecretStr("pwd")
    )
    ctx = PipelineContext(run_id="metabase-test")

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_get.return_value = mock_response
    mock_post.return_value = mock_response

    mock_response_delete = MagicMock()
    mock_response_delete.status_code = 400
    mock_delete.return_value = mock_response_delete

    mock_report = MagicMock()

    metabase_source = MetabaseSource(ctx, metabase_config)
    metabase_source.report = mock_report
    metabase_source.close()

    mock_report.report_failure.assert_called_once()


@patch("requests.delete")
@patch("requests.Session.get")
@patch("requests.post")
def test_get_table_urns_from_native_query(mock_post, mock_get, mock_delete):
    """Test extraction of table URNs from native SQL queries"""
    metabase_config = MetabaseConfig(
        connect_uri="http://localhost:3000",
        username="test",
        password=pydantic.SecretStr("pwd"),
    )
    ctx = PipelineContext(run_id="metabase-test")
    ctx.graph = None
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"id": "session-token"}
    mock_get.return_value = mock_response
    mock_post.return_value = mock_response
    mock_delete.return_value = mock_response

    metabase_source = MetabaseSource(ctx, metabase_config)
    metabase_source.get_datasource_from_id = MagicMock(
        return_value=("postgres", "mydb", "public", None)
    )
    card_details = {
        "database_id": "1",
        "dataset_query": {
            "native": {
                "query": "SELECT * FROM users JOIN orders ON users.id = orders.user_id"
            }
        },
    }

    table_urns = metabase_source._get_table_urns_from_native_query(card_details)
    expected_urns = {
        "urn:li:dataset:(urn:li:dataPlatform:postgres,mydb.public.users,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:postgres,mydb.public.orders,PROD)",
    }
    assert set(table_urns) == expected_urns

    metabase_source.close()


@patch("requests.delete")
@patch("requests.Session.get")
@patch("requests.post")
def test_get_table_urns_from_query_builder(mock_post, mock_get, mock_delete):
    """Test extraction of table URNs from query builder queries"""
    metabase_config = MetabaseConfig(
        connect_uri="http://localhost:3000",
        username="test",
        password=pydantic.SecretStr("pwd"),
    )
    ctx = PipelineContext(run_id="metabase-test")
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"id": "session-token"}
    mock_get.return_value = mock_response
    mock_post.return_value = mock_response
    mock_delete.return_value = mock_response

    metabase_source = MetabaseSource(ctx, metabase_config)
    metabase_source.get_datasource_from_id = MagicMock(
        return_value=("postgres", "mydb", "public", None)
    )
    metabase_source.get_source_table_from_id = MagicMock(
        return_value=("public", "products")
    )
    card_details = {
        "database_id": "1",
        "dataset_query": {"query": {"source-table": "42"}},
    }

    table_urns = metabase_source._get_table_urns_from_query_builder(card_details)
    expected_urn = (
        "urn:li:dataset:(urn:li:dataPlatform:postgres,mydb.public.products,PROD)"
    )
    assert table_urns == [expected_urn]

    metabase_source.close()


@patch("requests.delete")
@patch("requests.Session.get")
@patch("requests.post")
def test_get_table_urns_from_nested_query(mock_post, mock_get, mock_delete):
    """Test extraction of table URNs from nested queries (card referencing another card)"""
    metabase_config = MetabaseConfig(
        connect_uri="http://localhost:3000",
        username="test",
        password=pydantic.SecretStr("pwd"),
    )
    ctx = PipelineContext(run_id="metabase-test")
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"id": "session-token"}
    mock_get.return_value = mock_response
    mock_post.return_value = mock_response
    mock_delete.return_value = mock_response

    metabase_source = MetabaseSource(ctx, metabase_config)
    metabase_source.get_datasource_from_id = MagicMock(
        return_value=("postgres", "mydb", "public", None)
    )
    metabase_source.get_source_table_from_id = MagicMock(
        return_value=("public", "products")
    )
    referenced_card = {
        "database_id": "1",
        "dataset_query": {"type": "query", "query": {"source-table": "42"}},
    }

    metabase_source.get_card_details_by_id = MagicMock(return_value=referenced_card)  # type: ignore[method-assign]
    card_details = {
        "database_id": "1",
        "dataset_query": {"query": {"source-table": "card__123"}},
    }

    table_urns = metabase_source._get_table_urns_from_query_builder(card_details)
    expected_urn = (
        "urn:li:dataset:(urn:li:dataPlatform:postgres,mydb.public.products,PROD)"
    )
    assert table_urns == [expected_urn]
    metabase_source.get_card_details_by_id.assert_called_with("123")

    metabase_source.close()


@patch("requests.delete")
@patch("requests.Session.get")
@patch("requests.post")
def test_construct_dashboard_lineage(mock_post, mock_get, mock_delete):
    """Test construction of dashboard-level lineage"""
    metabase_config = MetabaseConfig(
        connect_uri="http://localhost:3000",
        username="test",
        password=pydantic.SecretStr("pwd"),
    )
    ctx = PipelineContext(run_id="metabase-test")
    ctx.graph = None
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"id": "session-token"}
    mock_get.return_value = mock_response
    mock_post.return_value = mock_response
    mock_delete.return_value = mock_response

    metabase_source = MetabaseSource(ctx, metabase_config)
    metabase_source.get_datasource_from_id = MagicMock(
        return_value=("postgres", "mydb", "public", None)
    )
    metabase_source.get_source_table_from_id = MagicMock(
        return_value=("public", "products")
    )
    card1 = {
        "database_id": "1",
        "dataset_query": {
            "type": "native",
            "native": {"query": "SELECT * FROM users"},
        },
    }

    card2 = {
        "database_id": "1",
        "dataset_query": {"type": "query", "query": {"source-table": "42"}},
    }

    def mock_get_card_details(card_id):
        if card_id == "1":
            return card1
        elif card_id == "2":
            return card2
        return None

    metabase_source.get_card_details_by_id = MagicMock(  # type: ignore[method-assign]
        side_effect=mock_get_card_details
    )

    dashboard_details = {
        "id": "100",
        "name": "Test Dashboard",
        "dashcards": [{"card": {"id": "1"}}, {"card": {"id": "2"}}],
    }
    last_modified = AuditStamp(time=0, actor="urn:li:corpuser:test")

    dataset_edges = metabase_source.construct_dashboard_lineage(
        dashboard_details, last_modified
    )
    assert dataset_edges is not None
    assert isinstance(dataset_edges, list)
    assert len(dataset_edges) == 2

    destination_urns = {edge.destinationUrn for edge in dataset_edges}
    expected_urns = {
        "urn:li:dataset:(urn:li:dataPlatform:postgres,mydb.public.users,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:postgres,mydb.public.products,PROD)",
    }
    assert destination_urns == expected_urns
    for edge in dataset_edges:
        assert edge.lastModified == last_modified

    metabase_source.close()


@patch("requests.delete")
@patch("requests.Session.get")
@patch("requests.post")
def test_construct_dashboard_lineage_empty_dashcards(mock_post, mock_get, mock_delete):
    """Test dashboard lineage construction with no cards"""
    metabase_config = MetabaseConfig(
        connect_uri="http://localhost:3000",
        username="test",
        password=pydantic.SecretStr("pwd"),
    )
    ctx = PipelineContext(run_id="metabase-test")
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"id": "session-token"}
    mock_get.return_value = mock_response
    mock_post.return_value = mock_response
    mock_delete.return_value = mock_response

    metabase_source = MetabaseSource(ctx, metabase_config)

    dashboard_details = {
        "id": "100",
        "name": "Empty Dashboard",
        "dashcards": [],
    }
    last_modified = AuditStamp(time=0, actor="urn:li:corpuser:test")

    dataset_edges = metabase_source.construct_dashboard_lineage(
        dashboard_details, last_modified
    )
    assert dataset_edges is None

    metabase_source.close()


@patch("requests.delete")
@patch("requests.Session.get")
@patch("requests.post")
def test_construct_dashboard_lineage_deduplication(mock_post, mock_get, mock_delete):
    """Test that dashboard lineage deduplicates table URNs"""
    metabase_config = MetabaseConfig(
        connect_uri="http://localhost:3000",
        username="test",
        password=pydantic.SecretStr("pwd"),
    )
    ctx = PipelineContext(run_id="metabase-test")
    ctx.graph = None
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"id": "session-token"}
    mock_get.return_value = mock_response
    mock_post.return_value = mock_response
    mock_delete.return_value = mock_response

    metabase_source = MetabaseSource(ctx, metabase_config)
    metabase_source.get_datasource_from_id = MagicMock(
        return_value=("postgres", "mydb", "public", None)
    )
    metabase_source.get_source_table_from_id = MagicMock(
        return_value=("public", "users")
    )
    card1 = {
        "database_id": "1",
        "dataset_query": {
            "type": "native",
            "native": {"query": "SELECT count(*) FROM users"},
        },
    }

    card2 = {
        "database_id": "1",
        "dataset_query": {
            "type": "native",
            "native": {"query": "SELECT * FROM users WHERE active = true"},
        },
    }

    def mock_get_card_details(card_id):
        if card_id == "1":
            return card1
        elif card_id == "2":
            return card2
        return None

    metabase_source.get_card_details_by_id = MagicMock(  # type: ignore[method-assign]
        side_effect=mock_get_card_details
    )
    dashboard_details = {
        "id": "100",
        "name": "Test Dashboard",
        "dashcards": [{"card": {"id": "1"}}, {"card": {"id": "2"}}],
    }
    last_modified = AuditStamp(time=0, actor="urn:li:corpuser:test")

    dataset_edges = metabase_source.construct_dashboard_lineage(
        dashboard_details, last_modified
    )
    assert dataset_edges is not None
    assert len(dataset_edges) == 1
    expected_urn = (
        "urn:li:dataset:(urn:li:dataPlatform:postgres,mydb.public.users,PROD)"
    )
    assert dataset_edges[0].destinationUrn == expected_urn

    metabase_source.close()


@patch("requests.delete")
@patch("requests.Session.get")
@patch("requests.post")
def test_get_table_urns_handles_missing_database_id(mock_post, mock_get, mock_delete):
    """Test that missing database_id is handled gracefully"""
    metabase_config = MetabaseConfig(
        connect_uri="http://localhost:3000",
        username="test",
        password=pydantic.SecretStr("pwd"),
    )
    ctx = PipelineContext(run_id="metabase-test")
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"id": "session-token"}
    mock_get.return_value = mock_response
    mock_post.return_value = mock_response
    mock_delete.return_value = mock_response

    metabase_source = MetabaseSource(ctx, metabase_config)
    card_details = {
        "dataset_query": {
            "type": "native",
            "native": {"query": "SELECT * FROM users"},
        }
    }

    table_urns = metabase_source._get_table_urns_from_native_query(card_details)
    assert table_urns == []

    metabase_source.close()


@patch("requests.delete")
@patch("requests.Session.get")
@patch("requests.post")
def test_get_table_urns_handles_missing_query(mock_post, mock_get, mock_delete):
    """Test that missing query is handled gracefully"""
    metabase_config = MetabaseConfig(
        connect_uri="http://localhost:3000",
        username="test",
        password=pydantic.SecretStr("pwd"),
    )
    ctx = PipelineContext(run_id="metabase-test")
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"id": "session-token"}
    mock_get.return_value = mock_response
    mock_post.return_value = mock_response
    mock_delete.return_value = mock_response

    metabase_source = MetabaseSource(ctx, metabase_config)

    metabase_source.get_datasource_from_id = MagicMock(
        return_value=("postgres", "mydb", "public", None)
    )
    card_details = {
        "database_id": "1",
        "dataset_query": {"native": {}},
    }

    table_urns = metabase_source._get_table_urns_from_native_query(card_details)
    assert table_urns == []

    metabase_source.close()


@patch("requests.delete")
@patch("requests.Session.get")
@patch("requests.post")
def test_extract_tags_from_collection(mock_post, mock_get, mock_delete):
    """Test that tags are extracted from collections"""
    metabase_config = MetabaseConfig(
        connect_uri="http://localhost:3000",
        username="test",
        password=pydantic.SecretStr("pwd"),
        extract_collections_as_tags=True,
    )
    ctx = PipelineContext(run_id="metabase-test")

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"id": "session-token"}
    mock_get.return_value = mock_response
    mock_post.return_value = mock_response
    mock_delete.return_value = mock_response

    metabase_source = MetabaseSource(ctx, metabase_config)

    collections_response = MagicMock()
    collections_response.status_code = 200
    collections_response.json.return_value = [
        {"id": "1", "name": "Sales Dashboard"},
    ]

    def mock_get_collections(url):
        if "/api/collection/" in url:
            return collections_response
        return mock_response

    metabase_source.session.get = MagicMock(side_effect=mock_get_collections)  # type: ignore[method-assign]
    tags = metabase_source._get_tags_from_collection("1")
    assert tags is not None
    assert isinstance(tags, GlobalTagsClass)
    assert len(tags.tags) == 1
    assert tags.tags[0].tag == "urn:li:tag:metabase_collection_sales_dashboard"
    metabase_source.close()


@patch("requests.delete")
@patch("requests.Session.get")
@patch("requests.post")
def test_is_metabase_model(mock_post, mock_get, mock_delete):
    """Test model detection"""
    metabase_config = MetabaseConfig(
        connect_uri="http://localhost:3000",
        username="test",
        password=pydantic.SecretStr("pwd"),
    )
    ctx = PipelineContext(run_id="metabase-test")

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"id": "session-token"}
    mock_get.return_value = mock_response
    mock_post.return_value = mock_response
    mock_delete.return_value = mock_response

    metabase_source = MetabaseSource(ctx, metabase_config)

    model_card = {"id": "123", "type": "model", "name": "Sales Model"}
    assert metabase_source._is_metabase_model(model_card) is True

    question_card = {"id": "456", "type": "question", "name": "Sales Query"}
    assert metabase_source._is_metabase_model(question_card) is False

    metabase_source.close()


@patch("requests.delete")
@patch("requests.Session.get")
@patch("requests.post")
def test_construct_model_from_api_data(mock_post, mock_get, mock_delete):
    """Test construction of DatasetSnapshot for Metabase Models"""

    metabase_config = MetabaseConfig(
        connect_uri="http://localhost:3000",
        username="test",
        password=pydantic.SecretStr("pwd"),
    )
    ctx = PipelineContext(run_id="metabase-test")
    ctx.graph = None

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"id": "session-token"}
    mock_get.return_value = mock_response
    mock_post.return_value = mock_response
    mock_delete.return_value = mock_response

    metabase_source = MetabaseSource(ctx, metabase_config)
    metabase_source.get_datasource_from_id = MagicMock(
        return_value=("postgres", "mydb", "public", None)
    )
    collections_response = MagicMock()
    collections_response.status_code = 200
    collections_response.json.return_value = [
        {"id": "42", "name": "Analytics"},
    ]

    def mock_get_collections(url):
        if "/api/collection/" in url:
            return collections_response
        return mock_response

    metabase_source.session.get = MagicMock(side_effect=mock_get_collections)  # type: ignore[method-assign]

    model_card = {
        "id": 123,
        "name": "Customer Revenue Model",
        "description": "A model for customer revenue analysis",
        "type": "model",
        "database_id": 1,
        "query_type": "native",
        "dataset_query": {
            "type": "native",
            "native": {
                "query": "SELECT customer_id, SUM(revenue) FROM orders GROUP BY customer_id"
            },
        },
        "result_metadata": [
            {
                "name": "customer_id",
                "display_name": "Customer ID",
                "base_type": "type/Integer",
                "effective_type": "type/Integer",
            },
            {
                "name": "revenue_sum",
                "display_name": "Total Revenue",
                "base_type": "type/Decimal",
                "effective_type": "type/Decimal",
            },
        ],
        "collection_id": 42,
        "creator_id": 1,
        "created_at": "2024-01-15T10:00:00Z",
    }

    workunits = list(metabase_source._emit_model_workunits(model_card))

    assert len(workunits) > 0

    expected_urn = "urn:li:dataset:(urn:li:dataPlatform:metabase,model.123,PROD)"

    # Verify all work units are for the correct entity
    for wu in workunits:
        mcp = wu.metadata
        assert mcp.entityUrn == expected_urn  # type: ignore[union-attr]

    # Check for SchemaMetadata aspect
    from datahub.metadata.com.linkedin.pegasus2avro.schema import SchemaMetadata

    schema_wu = next(
        (wu for wu in workunits if isinstance(wu.metadata.aspect, SchemaMetadata)),  # type: ignore[union-attr]
        None,
    )
    assert schema_wu is not None, "Models must have schema metadata"
    schema_aspect = schema_wu.metadata.aspect  # type: ignore[union-attr]
    assert len(schema_aspect.fields) == 2  # type: ignore[arg-type,union-attr]
    assert schema_aspect.fields[0].fieldPath == "customer_id"  # type: ignore[index,union-attr]
    assert schema_aspect.fields[1].fieldPath == "revenue_sum"  # type: ignore[index,union-attr]

    metabase_source.close()


@patch("requests.delete")
@patch("requests.Session.get")
@patch("requests.post")
def test_construct_model_with_lineage(mock_post, mock_get, mock_delete):
    """Test that models include lineage to source tables"""
    metabase_config = MetabaseConfig(
        connect_uri="http://localhost:3000",
        username="test",
        password=pydantic.SecretStr("pwd"),
    )
    ctx = PipelineContext(run_id="metabase-test")
    ctx.graph = None

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"id": "session-token"}
    mock_get.return_value = mock_response
    mock_post.return_value = mock_response
    mock_delete.return_value = mock_response

    metabase_source = MetabaseSource(ctx, metabase_config)
    metabase_source.get_datasource_from_id = MagicMock(
        return_value=("postgres", "mydb", "public", None)
    )

    model_card = {
        "id": 456,
        "name": "Sales Model",
        "type": "model",
        "database_id": 1,
        "dataset_query": {
            "type": "native",
            "native": {"query": "SELECT * FROM sales WHERE amount > 100"},
        },
        "collection_id": None,
        "creator_id": 1,
    }

    workunits = list(metabase_source._emit_model_workunits(model_card))
    assert len(workunits) > 0

    lineage_wu = next(
        (
            wu
            for wu in workunits
            if isinstance(wu.metadata.aspect, UpstreamLineageClass)  # type: ignore[union-attr]
        ),
        None,
    )
    assert lineage_wu is not None
    upstream_lineage = lineage_wu.metadata.aspect  # type: ignore[union-attr]
    assert len(upstream_lineage.upstreams) == 1  # type: ignore[arg-type,union-attr]
    expected_urn = (
        "urn:li:dataset:(urn:li:dataPlatform:postgres,mydb.public.sales,PROD)"
    )
    assert upstream_lineage.upstreams[0].dataset == expected_urn  # type: ignore[index,union-attr]

    metabase_source.close()


@patch("requests.delete")
@patch("requests.Session.get")
@patch("requests.post")
def test_extract_models_config_disabled(mock_post, mock_get, mock_delete):
    """Test that models are not extracted when config is disabled"""
    metabase_config = MetabaseConfig(
        connect_uri="http://localhost:3000",
        username="test",
        password=pydantic.SecretStr("pwd"),
        extract_models=False,
    )
    ctx = PipelineContext(run_id="metabase-test")

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"id": "session-token"}
    mock_get.return_value = mock_response
    mock_post.return_value = mock_response
    mock_delete.return_value = mock_response

    metabase_source = MetabaseSource(ctx, metabase_config)

    card_response = MagicMock()
    card_response.status_code = 200
    card_response.json.return_value = [{"id": 1, "name": "Test Model", "type": "model"}]
    metabase_source.session.get = MagicMock(return_value=card_response)  # type: ignore[method-assign]

    workunits = list(metabase_source.emit_model_mces())
    assert len(workunits) == 0

    metabase_source.close()


@patch("requests.delete")
@patch("requests.Session.get")
@patch("requests.post")
def test_extract_collections_as_tags_config_disabled(mock_post, mock_get, mock_delete):
    """Test that tags are not extracted when config is disabled"""
    metabase_config = MetabaseConfig(
        connect_uri="http://localhost:3000",
        username="test",
        password=pydantic.SecretStr("pwd"),
        extract_collections_as_tags=False,
    )
    ctx = PipelineContext(run_id="metabase-test")

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"id": "session-token"}
    mock_get.return_value = mock_response
    mock_post.return_value = mock_response
    mock_delete.return_value = mock_response

    metabase_source = MetabaseSource(ctx, metabase_config)

    tags = metabase_source._get_tags_from_collection("42")
    assert tags is None

    metabase_source.close()


@patch("requests.delete")
@patch("requests.Session.get")
@patch("requests.post")
def test_recursion_depth_limit_prevents_stack_overflow(
    mock_post, mock_get, mock_delete
):
    """Test that recursion depth limit prevents infinite loops from circular card references"""
    metabase_config = MetabaseConfig(
        connect_uri="http://localhost:3000",
        username="test",
        password=pydantic.SecretStr("pwd"),
    )
    ctx = PipelineContext(run_id="metabase-test")
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"id": "session-token"}
    mock_get.return_value = mock_response
    mock_post.return_value = mock_response
    mock_delete.return_value = mock_response

    metabase_source = MetabaseSource(ctx, metabase_config)
    card_a = {
        "id": "A",
        "database_id": "1",
        "dataset_query": {"type": "query", "query": {"source-table": "card__B"}},
    }

    card_b = {
        "id": "B",
        "database_id": "1",
        "dataset_query": {"type": "query", "query": {"source-table": "card__A"}},
    }

    def mock_get_card_details(card_id):
        if card_id == "A":
            return card_a
        elif card_id == "B":
            return card_b
        return None

    metabase_source.get_card_details_by_id = MagicMock(  # type: ignore[method-assign]
        side_effect=mock_get_card_details
    )
    table_urns = metabase_source._get_table_urns_from_card(card_a)

    assert table_urns == []
    assert len(metabase_source.report.warnings) > 0

    metabase_source.close()


@patch("requests.delete")
@patch("requests.Session.get")
@patch("requests.post")
def test_recursion_depth_tracking_through_nested_cards(
    mock_post, mock_get, mock_delete
):
    """Test that recursion depth is properly tracked through nested cards"""
    metabase_config = MetabaseConfig(
        connect_uri="http://localhost:3000",
        username="test",
        password=pydantic.SecretStr("pwd"),
    )
    ctx = PipelineContext(run_id="metabase-test")

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"id": "session-token"}
    mock_get.return_value = mock_response
    mock_post.return_value = mock_response
    mock_delete.return_value = mock_response

    metabase_source = MetabaseSource(ctx, metabase_config)
    metabase_source.get_datasource_from_id = MagicMock(
        return_value=("postgres", "mydb", "public", None)
    )
    metabase_source.get_source_table_from_id = MagicMock(
        return_value=("public", "base_table")
    )

    def mock_get_card_details(card_id):
        nested_cards = {
            "A": {
                "id": "A",
                "database_id": "1",
                "dataset_query": {
                    "type": "query",
                    "query": {"source-table": "card__B"},
                },
            },
            "B": {
                "id": "B",
                "database_id": "1",
                "dataset_query": {
                    "type": "query",
                    "query": {"source-table": "card__C"},
                },
            },
            "C": {
                "id": "C",
                "database_id": "1",
                "dataset_query": {
                    "type": "query",
                    "query": {"source-table": "card__D"},
                },
            },
            "D": {
                "id": "D",
                "database_id": "1",
                "dataset_query": {
                    "type": "query",
                    "query": {"source-table": "card__E"},
                },
            },
            "E": {
                "id": "E",
                "database_id": "1",
                "dataset_query": {
                    "type": "query",
                    "query": {"source-table": "card__F"},
                },
            },
            "F": {
                "id": "F",
                "database_id": "1",
                "dataset_query": {
                    "type": "query",
                    "query": {"source-table": "card__G"},
                },
            },
            "G": {
                "id": "G",
                "database_id": "1",
                "dataset_query": {"type": "query", "query": {"source-table": "42"}},
            },
        }
        return nested_cards.get(card_id)

    metabase_source.get_card_details_by_id = MagicMock(  # type: ignore[method-assign]
        side_effect=mock_get_card_details
    )

    # 7 levels of nesting (A→B→C→D→E→F→G) should exceed the recursion limit
    table_urns = metabase_source._get_table_urns_from_card(
        mock_get_card_details("A"), recursion_depth=0
    )
    assert table_urns == []
    assert len(metabase_source.report.warnings) > 0

    metabase_source.close()


@pytest.mark.parametrize(
    "input_name,expected_output",
    [
        ("Sales & Marketing", "sales_marketing"),
        ("Q1/Q2 Reports", "q1q2_reports"),
        ("Data: Analytics", "data_analytics"),
        ("Team #1 Dashboard", "team_1_dashboard"),
        ("Reports (2024)", "reports_2024"),
        ("Multi   Spaces", "multi_spaces"),
        ("__Leading__", "leading"),
        ("Trailing__", "trailing"),
        ("___consecutive___", "consecutive"),
    ],
)
@patch("requests.delete")
@patch("requests.Session.get")
@patch("requests.post")
def test_collection_name_sanitization_special_characters(
    mock_post, mock_get, mock_delete, input_name, expected_output
):
    """Test that collection names with special characters are properly sanitized"""
    metabase_config = MetabaseConfig(
        connect_uri="http://localhost:3000",
        username="test",
        password=pydantic.SecretStr("pwd"),
        extract_collections_as_tags=True,
    )
    ctx = PipelineContext(run_id="metabase-test")

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"id": "session-token"}
    mock_get.return_value = mock_response
    mock_post.return_value = mock_response
    mock_delete.return_value = mock_response

    metabase_source = MetabaseSource(ctx, metabase_config)

    sanitized = metabase_source._sanitize_collection_name(input_name)
    assert sanitized == expected_output

    metabase_source.close()


@patch("requests.delete")
@patch("requests.Session.get")
@patch("requests.post")
def test_collection_name_empty_after_sanitization(mock_post, mock_get, mock_delete):
    """Test that empty collection names after sanitization return None"""
    metabase_config = MetabaseConfig(
        connect_uri="http://localhost:3000",
        username="test",
        password=pydantic.SecretStr("pwd"),
        extract_collections_as_tags=True,
    )
    ctx = PipelineContext(run_id="metabase-test")

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"id": "session-token"}
    mock_get.return_value = mock_response
    mock_post.return_value = mock_response
    mock_delete.return_value = mock_response

    metabase_source = MetabaseSource(ctx, metabase_config)

    collections_response = MagicMock()
    collections_response.status_code = 200
    collections_response.json.return_value = [
        {
            "id": "1",
            "name": "!!!",
        },  # Only special chars, will be empty after sanitization
    ]

    def mock_get_collections(url):
        if "/api/collection/" in url:
            return collections_response
        return mock_response

    metabase_source.session.get = MagicMock(side_effect=mock_get_collections)  # type: ignore[method-assign]
    tags = metabase_source._get_tags_from_collection("1")
    assert tags is None

    metabase_source.close()


@patch("requests.delete")
@patch("requests.Session.get")
@patch("requests.post")
def test_collection_api_caching(mock_post, mock_get, mock_delete):
    """Test that _get_collections_map() uses caching to avoid N+1 API calls"""
    metabase_config = MetabaseConfig(
        connect_uri="http://localhost:3000",
        username="test",
        password=pydantic.SecretStr("pwd"),
        extract_collections_as_tags=True,
    )
    ctx = PipelineContext(run_id="metabase-test")

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"id": "session-token"}
    mock_get.return_value = mock_response
    mock_post.return_value = mock_response
    mock_delete.return_value = mock_response

    metabase_source = MetabaseSource(ctx, metabase_config)

    collections_response = MagicMock()
    collections_response.status_code = 200
    collections_response.json.return_value = [
        {"id": "1", "name": "Collection A"},
        {"id": "2", "name": "Collection B"},
    ]

    api_call_count = 0

    def mock_get_collections(url):
        nonlocal api_call_count
        if "/api/collection/" in url:
            api_call_count += 1
            return collections_response
        return mock_response

    metabase_source.session.get = MagicMock(side_effect=mock_get_collections)  # type: ignore[method-assign]
    metabase_source._get_collections_map.cache_clear()
    result1 = metabase_source._get_collections_map()
    api_calls_after_first = api_call_count
    assert api_calls_after_first >= 1  # At least one API call
    assert "1" in result1
    assert result1["1"]["name"] == "Collection A"
    result2 = metabase_source._get_collections_map()
    assert api_call_count == api_calls_after_first  # No new API calls
    assert result2 is result1  # Same object reference proves cache hit
    result3 = metabase_source._get_collections_map()
    assert api_call_count == api_calls_after_first  # Still no new API calls
    assert result3 is result1
    cache_info = metabase_source._get_collections_map.cache_info()
    assert cache_info.hits >= 2  # At least 2 cache hits
    assert cache_info.misses == 1  # Exactly 1 cache miss

    metabase_source.close()


@patch("requests.delete")
@patch("requests.Session.get")
@patch("requests.post")
def test_collection_map_returns_dict_keyed_by_id(mock_post, mock_get, mock_delete):
    """Test that _get_collections_map() returns a dict for O(1) lookup instead of list"""
    metabase_config = MetabaseConfig(
        connect_uri="http://localhost:3000",
        username="test",
        password=pydantic.SecretStr("pwd"),
        extract_collections_as_tags=True,
    )
    ctx = PipelineContext(run_id="metabase-test")

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"id": "session-token"}
    mock_get.return_value = mock_response
    mock_post.return_value = mock_response
    mock_delete.return_value = mock_response

    metabase_source = MetabaseSource(ctx, metabase_config)

    collections_response = MagicMock()
    collections_response.status_code = 200
    collections_response.json.return_value = [
        {"id": "1", "name": "Collection A"},
        {"id": "2", "name": "Collection B"},
        {"id": "3", "name": "Collection C"},
    ]

    def mock_get_collections(url):
        if "/api/collection/" in url:
            return collections_response
        return mock_response

    metabase_source.session.get = MagicMock(side_effect=mock_get_collections)  # type: ignore[method-assign]

    collections_map = metabase_source._get_collections_map()
    assert isinstance(collections_map, dict)
    assert "1" in collections_map
    assert "2" in collections_map
    assert "3" in collections_map
    assert collections_map["1"]["name"] == "Collection A"
    assert collections_map["2"]["name"] == "Collection B"
    assert collections_map["3"]["name"] == "Collection C"

    metabase_source.close()


@patch("requests.delete")
@patch("requests.Session.get")
@patch("requests.post")
def test_collection_404_error_handling(mock_post, mock_get, mock_delete):
    """Test that 404 errors for collections are handled silently with debug logging"""
    metabase_config = MetabaseConfig(
        connect_uri="http://localhost:3000",
        username="test",
        password=pydantic.SecretStr("pwd"),
        extract_collections_as_tags=True,
    )
    ctx = PipelineContext(run_id="metabase-test")

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"id": "session-token"}
    mock_get.return_value = mock_response
    mock_post.return_value = mock_response
    mock_delete.return_value = mock_response

    metabase_source = MetabaseSource(ctx, metabase_config)
    error_response = MagicMock()
    error_response.status_code = 404
    http_error = HTTPError()
    http_error.response = error_response

    def mock_get_collections(url):
        if "/api/collection/" in url:
            raise http_error
        return mock_response

    metabase_source.session.get = MagicMock(side_effect=mock_get_collections)  # type: ignore[method-assign]
    collections = metabase_source._get_collections_map()
    assert collections == {}
    assert len(metabase_source.report.warnings) == 0

    metabase_source.close()


@pytest.mark.parametrize("status_code", [401, 403, 500])
@patch("requests.delete")
@patch("requests.Session.get")
@patch("requests.post")
def test_collection_non_404_error_handling(
    mock_post, mock_get, mock_delete, status_code
):
    """Test that non-404 errors (401/403/500) for collections are reported as warnings"""
    metabase_config = MetabaseConfig(
        connect_uri="http://localhost:3000",
        username="test",
        password=pydantic.SecretStr("pwd"),
        extract_collections_as_tags=True,
    )
    ctx = PipelineContext(run_id="metabase-test")

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"id": "session-token"}
    mock_get.return_value = mock_response
    mock_post.return_value = mock_response
    mock_delete.return_value = mock_response

    metabase_source = MetabaseSource(ctx, metabase_config)

    error_response = MagicMock()
    error_response.status_code = status_code
    http_error = HTTPError()
    http_error.response = error_response

    def mock_get_collections(url, error=http_error):
        if "/api/collection/" in url:
            raise error
        return mock_response

    metabase_source.session.get = MagicMock(side_effect=mock_get_collections)  # type: ignore[method-assign]

    collections = metabase_source._get_collections_map()
    assert collections == {}

    assert len(metabase_source.report.warnings) == 1
    warning_message = str(metabase_source.report.warnings[0])
    assert "Check API credentials and permissions" in warning_message

    metabase_source.close()


@patch("requests.delete")
@patch("requests.Session.get")
@patch("requests.post")
def test_get_datasource_urn_delegates_to_get_table_urns(
    mock_post, mock_get, mock_delete
):
    """Test that get_datasource_urn() properly delegates to _get_table_urns_from_card()"""
    metabase_config = MetabaseConfig(
        connect_uri="http://localhost:3000",
        username="test",
        password=pydantic.SecretStr("pwd"),
    )
    ctx = PipelineContext(run_id="metabase-test")
    ctx.graph = None

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"id": "session-token"}
    mock_get.return_value = mock_response
    mock_post.return_value = mock_response
    mock_delete.return_value = mock_response

    metabase_source = MetabaseSource(ctx, metabase_config)

    expected_urns = [
        "urn:li:dataset:(urn:li:dataPlatform:postgres,mydb.public.users,PROD)"
    ]
    metabase_source._get_table_urns_from_card = MagicMock(  # type: ignore[method-assign]
        return_value=expected_urns
    )

    card_details = {
        "database_id": "1",
        "dataset_query": {
            "type": "native",
            "native": {"query": "SELECT * FROM users"},
        },
    }
    result = metabase_source.get_datasource_urn(card_details)
    metabase_source._get_table_urns_from_card.assert_called_once()
    assert result == expected_urns

    metabase_source.close()


@patch("requests.delete")
@patch("requests.Session.get")
@patch("requests.post")
def test_get_datasource_urn_returns_none_for_empty_list(
    mock_post, mock_get, mock_delete
):
    """Test that get_datasource_urn() returns None instead of empty list for backward compatibility"""
    metabase_config = MetabaseConfig(
        connect_uri="http://localhost:3000",
        username="test",
        password=pydantic.SecretStr("pwd"),
    )
    ctx = PipelineContext(run_id="metabase-test")

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"id": "session-token"}
    mock_get.return_value = mock_response
    mock_post.return_value = mock_response
    mock_delete.return_value = mock_response

    metabase_source = MetabaseSource(ctx, metabase_config)

    metabase_source._get_table_urns_from_card = MagicMock(return_value=[])  # type: ignore[method-assign]

    card_details = {"database_id": "1", "dataset_query": {}}
    result = metabase_source.get_datasource_urn(card_details)
    assert result is None

    metabase_source.close()


@patch("requests.delete")
@patch("requests.Session.get")
@patch("requests.post")
def test_get_datasource_urn_respects_recursion_limit(mock_post, mock_get, mock_delete):
    """Test that get_datasource_urn() respects recursion limit parameter"""
    metabase_config = MetabaseConfig(
        connect_uri="http://localhost:3000",
        username="test",
        password=pydantic.SecretStr("pwd"),
    )
    ctx = PipelineContext(run_id="metabase-test")

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"id": "session-token"}
    mock_get.return_value = mock_response
    mock_post.return_value = mock_response
    mock_delete.return_value = mock_response

    metabase_source = MetabaseSource(ctx, metabase_config)

    card_details = {"database_id": "1", "dataset_query": {}}
    result = metabase_source.get_datasource_urn(
        card_details, recursion_depth=DATASOURCE_URN_RECURSION_LIMIT + 1
    )
    assert result is None
    assert len(metabase_source.report.warnings) > 0

    metabase_source.close()


@patch("requests.delete")
@patch("requests.Session.get")
@patch("requests.post")
def test_emit_card_mces_skips_models_when_extraction_enabled(
    mock_post, mock_get, mock_delete
):
    """Test that emit_card_mces() skips model cards when extract_models is True"""
    metabase_config = MetabaseConfig(
        connect_uri="http://localhost:3000",
        username="test",
        password=pydantic.SecretStr("pwd"),
        extract_models=True,
    )
    ctx = PipelineContext(run_id="metabase-test")

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"id": "session-token"}
    mock_get.return_value = mock_response
    mock_post.return_value = mock_response
    mock_delete.return_value = mock_response

    metabase_source = MetabaseSource(ctx, metabase_config)
    card_response = MagicMock()
    card_response.status_code = 200
    card_response.json.return_value = [
        {"id": 1, "name": "Regular Card", "type": "question"},
        {"id": 2, "name": "Model Card", "type": "model"},
        {"id": 3, "name": "Another Card", "type": "question"},
    ]

    def mock_session_get(url):
        if "/api/card" in url:
            return card_response
        return mock_response

    metabase_source.session.get = MagicMock(side_effect=mock_session_get)  # type: ignore[method-assign]

    called_card_ids = []

    def mock_emit_chart(card_info):
        called_card_ids.append(card_info["id"])
        return []  # Return empty list of workunits

    metabase_source._emit_chart_workunits = MagicMock(  # type: ignore[method-assign,attr-defined]
        side_effect=mock_emit_chart
    )

    list(metabase_source.emit_card_mces())
    assert 1 in called_card_ids
    assert 3 in called_card_ids
    assert 2 not in called_card_ids  # Model should be skipped

    metabase_source.close()


@patch("requests.delete")
@patch("requests.Session.get")
@patch("requests.post")
def test_malformed_sql_parsing_failure(mock_post, mock_get, mock_delete):
    """Test that malformed SQL that parser can't handle returns empty list gracefully"""
    metabase_config = MetabaseConfig(
        connect_uri="http://localhost:3000",
        username="test",
        password=pydantic.SecretStr("pwd"),
    )
    ctx = PipelineContext(run_id="metabase-test")
    ctx.graph = None

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"id": "session-token"}
    mock_get.return_value = mock_response
    mock_post.return_value = mock_response
    mock_delete.return_value = mock_response

    metabase_source = MetabaseSource(ctx, metabase_config)

    metabase_source.get_datasource_from_id = MagicMock(
        return_value=("postgres", "mydb", "public", None)
    )
    card_details = {
        "database_id": "1",
        "dataset_query": {
            "native": {
                "query": "SELECT * FROM {{invalid syntax}} WHERE [[broken clause"
            }
        },
    }

    table_urns = metabase_source._get_table_urns_from_native_query(card_details)
    assert table_urns == []

    metabase_source.close()


@patch("requests.delete")
@patch("requests.Session.get")
@patch("requests.post")
def test_sql_with_cte_and_subqueries(mock_post, mock_get, mock_delete):
    """Test SQL parsing with CTEs and complex subqueries"""
    metabase_config = MetabaseConfig(
        connect_uri="http://localhost:3000",
        username="test",
        password=pydantic.SecretStr("pwd"),
    )
    ctx = PipelineContext(run_id="metabase-test")
    ctx.graph = None

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"id": "session-token"}
    mock_get.return_value = mock_response
    mock_post.return_value = mock_response
    mock_delete.return_value = mock_response

    metabase_source = MetabaseSource(ctx, metabase_config)

    metabase_source.get_datasource_from_id = MagicMock(
        return_value=("postgres", "mydb", "public", None)
    )
    card_details = {
        "database_id": "1",
        "dataset_query": {
            "native": {
                "query": """
                    WITH active_users AS (
                        SELECT id, name FROM users WHERE active = true
                    ),
                    recent_orders AS (
                        SELECT user_id, total FROM orders WHERE date > '2024-01-01'
                    )
                    SELECT au.name, ro.total
                    FROM active_users au
                    JOIN recent_orders ro ON au.id = ro.user_id
                    UNION
                    SELECT name, 0 as total FROM customers
                """
            }
        },
    }

    table_urns = metabase_source._get_table_urns_from_native_query(card_details)
    expected_urns = {
        "urn:li:dataset:(urn:li:dataPlatform:postgres,mydb.public.users,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:postgres,mydb.public.orders,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:postgres,mydb.public.customers,PROD)",
    }
    assert set(table_urns) == expected_urns

    metabase_source.close()


@patch("requests.delete")
@patch("requests.Session.get")
@patch("requests.post")
def test_api_500_error_handling(mock_post, mock_get, mock_delete):
    """Test that 500 errors are reported without crashing"""
    metabase_config = MetabaseConfig(
        connect_uri="http://localhost:3000",
        username="test",
        password=pydantic.SecretStr("pwd"),
    )
    ctx = PipelineContext(run_id="metabase-test")

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"id": "session-token"}
    mock_get.return_value = mock_response
    mock_post.return_value = mock_response
    mock_delete.return_value = mock_response

    metabase_source = MetabaseSource(ctx, metabase_config)
    error_response = MagicMock()
    error_response.status_code = 500
    error_response.raise_for_status.side_effect = HTTPError()

    def mock_get_cards(url):
        if "/api/card" in url:
            return error_response
        return mock_response

    metabase_source.session.get = MagicMock(side_effect=mock_get_cards)  # type: ignore[method-assign]
    workunits = list(metabase_source.emit_card_mces())
    assert len(workunits) == 0
    assert len(metabase_source.report.failures) > 0

    metabase_source.close()


@patch("requests.delete")
@patch("requests.Session.get")
@patch("requests.post")
def test_empty_query_returns_empty_list(mock_post, mock_get, mock_delete):
    """Test that cards with empty/null queries return empty lineage gracefully"""
    metabase_config = MetabaseConfig(
        connect_uri="http://localhost:3000",
        username="test",
        password=pydantic.SecretStr("pwd"),
    )
    ctx = PipelineContext(run_id="metabase-test")

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"id": "session-token"}
    mock_get.return_value = mock_response
    mock_post.return_value = mock_response
    mock_delete.return_value = mock_response

    metabase_source = MetabaseSource(ctx, metabase_config)

    metabase_source.get_datasource_from_id = MagicMock(
        return_value=("postgres", "mydb", "public", None)
    )

    test_cases = [
        {"database_id": "1", "dataset_query": {"native": {"query": ""}}},
        {"database_id": "1", "dataset_query": {"native": {"query": None}}},
        {"database_id": "1", "dataset_query": {"native": {}}},
        {"database_id": "1", "dataset_query": {}},
    ]

    for card_details in test_cases:
        table_urns = metabase_source._get_table_urns_from_native_query(card_details)
        assert table_urns == [], f"Failed for: {card_details}"

    metabase_source.close()


@patch("requests.delete")
@patch("requests.Session.get")
@patch("requests.post")
def test_model_referencing_another_model(mock_post, mock_get, mock_delete):
    """Test that models can reference other models (nested model resolution)"""
    metabase_config = MetabaseConfig(
        connect_uri="http://localhost:3000",
        username="test",
        password=pydantic.SecretStr("pwd"),
    )
    ctx = PipelineContext(run_id="metabase-test")

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"id": "session-token"}
    mock_get.return_value = mock_response
    mock_post.return_value = mock_response
    mock_delete.return_value = mock_response

    metabase_source = MetabaseSource(ctx, metabase_config)

    metabase_source.get_datasource_from_id = MagicMock(
        return_value=("postgres", "mydb", "public", None)
    )
    metabase_source.get_source_table_from_id = MagicMock(
        return_value=("public", "base_table")
    )
    model_b = {
        "id": "model_b",
        "type": "model",
        "database_id": "1",
        "dataset_query": {"type": "query", "query": {"source-table": "42"}},
    }
    model_a_details = {
        "id": "model_a",
        "type": "model",
        "database_id": "1",
        "dataset_query": {"type": "query", "query": {"source-table": "card__model_b"}},
    }

    metabase_source.get_card_details_by_id = MagicMock(return_value=model_b)  # type: ignore[method-assign]
    table_urns = metabase_source._get_table_urns_from_query_builder(
        model_a_details, recursion_depth=0
    )

    expected_urn = (
        "urn:li:dataset:(urn:li:dataPlatform:postgres,mydb.public.base_table,PROD)"
    )
    assert table_urns == [expected_urn]

    metabase_source.close()


@patch("requests.delete")
@patch("requests.Session.get")
@patch("requests.post")
def test_collection_hierarchy_parent_child(mock_post, mock_get, mock_delete):
    """Test that collection tags work with parent-child hierarchy"""
    metabase_config = MetabaseConfig(
        connect_uri="http://localhost:3000",
        username="test",
        password=pydantic.SecretStr("pwd"),
        extract_collections_as_tags=True,
    )
    ctx = PipelineContext(run_id="metabase-test")

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"id": "session-token"}
    mock_get.return_value = mock_response
    mock_post.return_value = mock_response
    mock_delete.return_value = mock_response

    metabase_source = MetabaseSource(ctx, metabase_config)
    collections_response = MagicMock()
    collections_response.status_code = 200
    collections_response.json.return_value = [
        {"id": "1", "name": "Marketing", "location": "/"},
        {"id": "2", "name": "Campaigns", "location": "/1/"},  # Child of Marketing
    ]

    def mock_get_collections(url):
        if "/api/collection/" in url:
            return collections_response
        return mock_response

    metabase_source.session.get = MagicMock(side_effect=mock_get_collections)  # type: ignore[method-assign]

    parent_tags = metabase_source._get_tags_from_collection("1")
    assert parent_tags is not None
    assert parent_tags.tags[0].tag == "urn:li:tag:metabase_collection_marketing"

    child_tags = metabase_source._get_tags_from_collection("2")  # type: ignore[attr-defined]
    assert child_tags is not None
    assert child_tags.tags[0].tag == "urn:li:tag:metabase_collection_campaigns"

    metabase_source.close()


@patch("requests.delete")
@patch("requests.Session.get")
@patch("requests.post")
def test_realistic_card_data_with_all_fields(mock_post, mock_get, mock_delete):
    """Test with realistic Metabase card containing 15+ fields like production data"""
    metabase_config = MetabaseConfig(
        connect_uri="http://localhost:3000",
        username="test",
        password=pydantic.SecretStr("pwd"),
    )
    ctx = PipelineContext(run_id="metabase-test")
    ctx.graph = None

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"id": "session-token"}
    mock_get.return_value = mock_response
    mock_post.return_value = mock_response
    mock_delete.return_value = mock_response

    metabase_source = MetabaseSource(ctx, metabase_config)
    metabase_source.get_datasource_from_id = MagicMock(
        return_value=("postgres", "mydb", "public", None)
    )
    realistic_card = {
        "id": 456,
        "name": "Revenue Analysis",
        "description": "Quarterly revenue breakdown by region",
        "display": "table",
        "visualization_settings": {"table.columns": []},
        "dataset_query": {
            "type": "native",
            "native": {
                "query": "SELECT region, SUM(revenue) FROM sales GROUP BY region",
                "template-tags": {},
            },
            "database": 1,
        },
        "database_id": 1,
        "table_id": None,
        "query_type": "native",
        "creator_id": 5,
        "created_at": "2024-01-15T10:30:00.000Z",
        "updated_at": "2024-01-20T14:45:00.000Z",
        "made_public_by_id": None,
        "public_uuid": None,
        "cache_ttl": None,
        "enable_embedding": False,
        "embedding_params": None,
        "collection_id": 12,
        "collection_position": 1,
        "result_metadata": [],
        "last-edit-info": {
            "id": 5,
            "email": "analyst@company.com",
            "first_name": "Data",
            "last_name": "Analyst",
            "timestamp": "2024-01-20T14:45:00.000Z",
        },
        "type": "question",
        "archived": False,
    }

    table_urns = metabase_source._get_table_urns_from_native_query(realistic_card)

    expected_urn = (
        "urn:li:dataset:(urn:li:dataPlatform:postgres,mydb.public.sales,PROD)"
    )
    assert table_urns == [expected_urn]

    metabase_source.close()
