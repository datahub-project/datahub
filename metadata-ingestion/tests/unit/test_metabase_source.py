from unittest.mock import MagicMock, patch

import pydantic

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.metabase import (
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

    # Mock successful authentication
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"id": "session-token"}
    mock_get.return_value = mock_response
    mock_post.return_value = mock_response
    mock_delete.return_value = mock_response

    metabase_source = MetabaseSource(ctx, metabase_config)

    # Mock get_datasource_from_id
    metabase_source.get_datasource_from_id = MagicMock(
        return_value=("postgres", "mydb", "public", None)
    )

    # Test with a simple SQL query
    card_details = {
        "database_id": "1",
        "dataset_query": {
            "native": {
                "query": "SELECT * FROM users JOIN orders ON users.id = orders.user_id"
            }
        },
    }

    table_urns = metabase_source._get_table_urns_from_native_query(card_details)

    # Should extract both tables
    assert len(table_urns) == 2
    assert any("users" in urn for urn in table_urns)
    assert any("orders" in urn for urn in table_urns)

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

    # Mock successful authentication
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"id": "session-token"}
    mock_get.return_value = mock_response
    mock_post.return_value = mock_response
    mock_delete.return_value = mock_response

    metabase_source = MetabaseSource(ctx, metabase_config)

    # Mock dependencies
    metabase_source.get_datasource_from_id = MagicMock(
        return_value=("postgres", "mydb", "public", None)
    )
    metabase_source.get_source_table_from_id = MagicMock(
        return_value=("public", "products")
    )

    # Test with query builder query
    card_details = {
        "database_id": "1",
        "dataset_query": {"query": {"source-table": "42"}},
    }

    table_urns = metabase_source._get_table_urns_from_query_builder(card_details)

    # Should extract one table
    assert len(table_urns) == 1
    assert "products" in table_urns[0]
    assert "postgres" in table_urns[0]

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

    # Mock successful authentication
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"id": "session-token"}
    mock_get.return_value = mock_response
    mock_post.return_value = mock_response
    mock_delete.return_value = mock_response

    metabase_source = MetabaseSource(ctx, metabase_config)

    # Mock dependencies
    metabase_source.get_datasource_from_id = MagicMock(
        return_value=("postgres", "mydb", "public", None)
    )
    metabase_source.get_source_table_from_id = MagicMock(
        return_value=("public", "products")
    )

    # The referenced card
    referenced_card = {
        "database_id": "1",
        "dataset_query": {"type": "query", "query": {"source-table": "42"}},
    }

    # Mock get_card_details_by_id to return the referenced card
    metabase_source.get_card_details_by_id = MagicMock(return_value=referenced_card)  # type: ignore[method-assign]

    # Test with nested query (card referencing another card)
    card_details = {
        "database_id": "1",
        "dataset_query": {"query": {"source-table": "card__123"}},
    }

    table_urns = metabase_source._get_table_urns_from_query_builder(card_details)

    # Should recursively resolve to the underlying table
    assert len(table_urns) == 1
    assert "products" in table_urns[0]

    # Verify that get_card_details_by_id was called with the correct ID
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

    # Mock successful authentication
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"id": "session-token"}
    mock_get.return_value = mock_response
    mock_post.return_value = mock_response
    mock_delete.return_value = mock_response

    metabase_source = MetabaseSource(ctx, metabase_config)

    # Mock dependencies
    metabase_source.get_datasource_from_id = MagicMock(
        return_value=("postgres", "mydb", "public", None)
    )
    metabase_source.get_source_table_from_id = MagicMock(
        return_value=("public", "products")
    )

    # Define cards that will be returned
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

    # Test dashboard with multiple cards
    dashboard_details = {
        "id": "100",
        "name": "Test Dashboard",
        "dashcards": [{"card": {"id": "1"}}, {"card": {"id": "2"}}],
    }

    # Create a mock AuditStamp for last_modified
    last_modified = AuditStamp(time=0, actor="urn:li:corpuser:test")

    dataset_edges = metabase_source.construct_dashboard_lineage(
        dashboard_details, last_modified
    )

    # Should create dataset edges
    assert dataset_edges is not None
    assert isinstance(dataset_edges, list)
    assert len(dataset_edges) >= 1  # At least one dataset edge

    # All edges should have destination URNs
    for edge in dataset_edges:
        assert edge.destinationUrn is not None
        assert edge.lastModified is not None

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

    # Mock successful authentication
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"id": "session-token"}
    mock_get.return_value = mock_response
    mock_post.return_value = mock_response
    mock_delete.return_value = mock_response

    metabase_source = MetabaseSource(ctx, metabase_config)

    # Test dashboard with no cards
    dashboard_details = {
        "id": "100",
        "name": "Empty Dashboard",
        "dashcards": [],
    }

    # Create a mock AuditStamp for last_modified
    last_modified = AuditStamp(time=0, actor="urn:li:corpuser:test")

    dataset_edges = metabase_source.construct_dashboard_lineage(
        dashboard_details, last_modified
    )

    # Should return None for empty dashboard
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

    # Mock successful authentication
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"id": "session-token"}
    mock_get.return_value = mock_response
    mock_post.return_value = mock_response
    mock_delete.return_value = mock_response

    metabase_source = MetabaseSource(ctx, metabase_config)

    # Mock dependencies
    metabase_source.get_datasource_from_id = MagicMock(
        return_value=("postgres", "mydb", "public", None)
    )
    metabase_source.get_source_table_from_id = MagicMock(
        return_value=("public", "users")
    )

    # Both cards reference the same table
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

    # Dashboard with two cards referencing the same table
    dashboard_details = {
        "id": "100",
        "name": "Test Dashboard",
        "dashcards": [{"card": {"id": "1"}}, {"card": {"id": "2"}}],
    }

    # Create a mock AuditStamp for last_modified
    last_modified = AuditStamp(time=0, actor="urn:li:corpuser:test")

    dataset_edges = metabase_source.construct_dashboard_lineage(
        dashboard_details, last_modified
    )

    # Should deduplicate to only one dataset edge
    assert dataset_edges is not None
    assert len(dataset_edges) == 1
    assert "users" in dataset_edges[0].destinationUrn

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

    # Mock successful authentication
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"id": "session-token"}
    mock_get.return_value = mock_response
    mock_post.return_value = mock_response
    mock_delete.return_value = mock_response

    metabase_source = MetabaseSource(ctx, metabase_config)

    # Test with missing database_id
    card_details = {
        "dataset_query": {
            "type": "native",
            "native": {"query": "SELECT * FROM users"},
        }
    }

    table_urns = metabase_source._get_table_urns_from_native_query(card_details)

    # Should return empty list
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

    # Mock successful authentication
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

    # Test with missing query
    card_details = {
        "database_id": "1",
        "dataset_query": {"native": {}},
    }

    table_urns = metabase_source._get_table_urns_from_native_query(card_details)

    # Should return empty list
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
    from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import (
        DatasetSnapshot,
    )

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

    # Mock collections API response
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
        "query_type": "native",  # This triggers ViewPropertiesClass
        "dataset_query": {
            "type": "native",
            "native": {
                "query": "SELECT customer_id, SUM(revenue) FROM orders GROUP BY customer_id"
            },
        },
        "collection_id": 42,
        "creator_id": 1,
        "created_at": "2024-01-15T10:00:00Z",
    }

    dataset_snapshot = metabase_source.construct_model_from_api_data(model_card)

    assert dataset_snapshot is not None
    assert isinstance(dataset_snapshot, DatasetSnapshot)
    assert "metabase" in dataset_snapshot.urn
    assert "123" in dataset_snapshot.urn

    aspect_types = [type(aspect).__name__ for aspect in dataset_snapshot.aspects]
    assert "DatasetPropertiesClass" in aspect_types
    assert "SubTypesClass" in aspect_types
    assert "ViewPropertiesClass" in aspect_types
    assert "OwnershipClass" in aspect_types
    assert "GlobalTagsClass" in aspect_types  # Tags from collection

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

    dataset_snapshot = metabase_source.construct_model_from_api_data(model_card)

    assert dataset_snapshot is not None
    upstream_lineage = None
    for aspect in dataset_snapshot.aspects:
        if isinstance(aspect, UpstreamLineageClass):
            upstream_lineage = aspect
            break

    assert upstream_lineage is not None
    assert len(upstream_lineage.upstreams) > 0
    assert any("sales" in upstream.dataset for upstream in upstream_lineage.upstreams)

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
