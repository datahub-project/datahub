from unittest.mock import MagicMock, patch

import pytest
from pydantic import SecretStr, ValidationError
from requests.exceptions import HTTPError

import datahub.emitter.mce_builder as builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.common.subtypes import DatasetSubTypes
from datahub.ingestion.source.metabase.config import MetabaseConfig
from datahub.ingestion.source.metabase.constants import (
    DATASOURCE_URN_RECURSION_LIMIT,
)
from datahub.ingestion.source.metabase.models import (
    DatasourceInfo,
    MetabaseCard,
    MetabaseCardListItem,
    MetabaseCollection,
    MetabaseDashboard,
    MetabaseDashboardListItem,
    MetabaseDatabaseDetails,
    MetabaseDatasetQuery,
    MetabaseField,
    MetabaseResultMetadata,
)
from datahub.ingestion.source.metabase.report import MetabaseReport
from datahub.ingestion.source.metabase.source import MetabaseSource
from datahub.metadata.schema_classes import (
    BytesTypeClass,
    ContainerClass,
    DashboardInfoClass,
    DatasetLineageTypeClass,
    GlobalTagsClass,
    SchemaMetadataClass,
    StringTypeClass,
    UpstreamLineageClass,
)


class FakeMetabaseSource(MetabaseSource):
    def __init__(self, ctx: PipelineContext, config: MetabaseConfig):
        self.config = config
        self.report = MetabaseReport()


def test_connect_uri_default_includes_scheme():
    config = MetabaseConfig(username="un", password=SecretStr("pwd"))
    assert config.connect_uri == "http://localhost:3000"


def test_connect_uri_adds_scheme_when_missing():
    config = MetabaseConfig(
        connect_uri="localhost:3000", username="un", password=SecretStr("pwd")
    )
    assert config.connect_uri == "http://localhost:3000"


@pytest.mark.parametrize(
    "kwargs",
    [
        {"api_key": SecretStr("  ")},
        {"username": "  ", "password": SecretStr("pwd")},
        {"username": "un", "password": SecretStr("  ")},
    ],
)
def test_blank_credentials_rejected(kwargs):
    with pytest.raises(ValidationError):
        MetabaseConfig(**kwargs)


def test_request_timeout_must_be_positive():
    with pytest.raises(ValidationError):
        MetabaseConfig(username="un", password=SecretStr("pwd"), request_timeout_sec=0)
    with pytest.raises(ValidationError):
        MetabaseConfig(username="un", password=SecretStr("pwd"), request_timeout_sec=-1)


def test_get_platform_instance():
    ctx = PipelineContext(run_id="test-metabase")
    config = MetabaseConfig(username="un", password=SecretStr("pwd"))
    config.connect_uri = "http://localhost:3000"
    metabase = FakeMetabaseSource(ctx, config)

    # no mappings defined
    assert metabase.get_platform_instance("clickhouse", 42) is None

    # database_id_to_instance_map is defined, key is present
    metabase.config.database_id_to_instance_map = {"42": "my_main_clickhouse"}
    metabase.get_platform_instance.cache_clear()
    assert metabase.get_platform_instance(None, 42) == "my_main_clickhouse"

    # database_id_to_instance_map is defined, key is missing
    metabase.get_platform_instance.cache_clear()
    assert metabase.get_platform_instance(None, 999) is None

    # database_id_to_instance_map is defined, key is missing, platform_instance_map is defined and key present
    metabase.config.platform_instance_map = {"clickhouse": "my_only_clickhouse"}
    metabase.get_platform_instance.cache_clear()
    assert metabase.get_platform_instance("clickhouse", 999) == "my_only_clickhouse"

    # database_id_to_instance_map is defined, key is missing, platform_instance_map is defined and key missing
    metabase.get_platform_instance.cache_clear()
    assert metabase.get_platform_instance("missing-platform", 999) is None

    # database_id_to_instance_map is missing, platform_instance_map is defined and key present
    metabase.config.database_id_to_instance_map = None
    metabase.get_platform_instance.cache_clear()
    assert metabase.get_platform_instance("clickhouse", 999) == "my_only_clickhouse"

    # database_id_to_instance_map is missing, platform_instance_map is defined and key missing
    metabase.get_platform_instance.cache_clear()
    assert metabase.get_platform_instance("missing-platform", 999) is None


@patch("requests.session")
def test_connection_uses_api_key_if_in_config(mock_session):
    metabase_config = MetabaseConfig(
        connect_uri="localhost:3000", api_key=SecretStr("key")
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

    mock_session_instance.get.assert_called_once_with(
        "http://localhost:3000/api/user/current",
        timeout=metabase_config.request_timeout_sec,
    )
    request_headers = mock_session_instance.headers
    assert request_headers["x-api-key"] == "key"


@patch("requests.delete")
@patch("requests.Session.get")
@patch("requests.post")
def test_create_session_from_config_username_password(mock_post, mock_get, mock_delete):
    metabase_config = MetabaseConfig(
        connect_uri="localhost:3000", username="un", password=SecretStr("pwd")
    )
    ctx = PipelineContext(run_id="metabase-test")

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"id": "test-session-token"}
    mock_get.return_value = mock_response
    mock_post.return_value = mock_response
    mock_delete.return_value = mock_response

    metabase_source = MetabaseSource(ctx, metabase_config)
    metabase_source.close()

    kwargs_post = mock_post.call_args
    assert kwargs_post[0][0] == "http://localhost:3000/api/session"
    assert kwargs_post[0][2]["password"] == "pwd"
    assert kwargs_post[0][2]["username"] == "un"

    kwargs_get = mock_get.call_args
    assert kwargs_get[0][0] == "http://localhost:3000/api/user/current"

    mock_delete.assert_called_once()


@patch("requests.delete")
@patch("requests.Session.get")
@patch("requests.post")
def test_warn_on_failed_session_delete(mock_post, mock_get, mock_delete):
    metabase_config = MetabaseConfig(
        connect_uri="localhost:3000", username="un", password=SecretStr("pwd")
    )
    ctx = PipelineContext(run_id="metabase-test")

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"id": "test-session-token"}
    mock_get.return_value = mock_response
    mock_post.return_value = mock_response

    mock_response_delete = MagicMock()
    mock_response_delete.status_code = 400
    mock_delete.return_value = mock_response_delete

    mock_report = MagicMock()

    metabase_source = MetabaseSource(ctx, metabase_config)
    metabase_source.report = mock_report
    metabase_source.close()

    # A non-2xx logout during teardown must not fail an otherwise-successful run.
    mock_report.failure.assert_not_called()
    mock_report.warning.assert_called_once()


@patch("requests.delete")
@patch("requests.Session.get")
@patch("requests.post")
def test_get_table_urns_from_native_query(mock_post, mock_get, mock_delete):
    """Test extraction of table URNs from native SQL queries"""
    metabase_config = MetabaseConfig(
        connect_uri="http://localhost:3000",
        username="test",
        password=SecretStr("pwd"),
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
    metabase_source.get_datasource_from_id = MagicMock(  # type: ignore[method-assign]
        return_value=DatasourceInfo(
            platform="postgres",
            database_name="mydb",
            schema="public",
            platform_instance=None,
        )
    )

    card = MetabaseCard(
        id=1,
        name="Test Card",
        database_id=1,
        dataset_query=MetabaseDatasetQuery(
            type="native",
            native={
                "query": "SELECT * FROM users JOIN orders ON users.id = orders.user_id"
            },
        ),
    )

    table_urns = metabase_source._get_table_urns_from_native_query(card)
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
        password=SecretStr("pwd"),
    )
    ctx = PipelineContext(run_id="metabase-test")
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"id": "session-token"}
    mock_get.return_value = mock_response
    mock_post.return_value = mock_response
    mock_delete.return_value = mock_response

    metabase_source = MetabaseSource(ctx, metabase_config)
    metabase_source.get_datasource_from_id = MagicMock(  # type: ignore[method-assign]
        return_value=DatasourceInfo(
            platform="postgres",
            database_name="mydb",
            schema="public",
            platform_instance=None,
        )
    )
    metabase_source.get_source_table_from_id = MagicMock(  # type: ignore[method-assign]
        return_value=("public", "products")
    )

    card = MetabaseCard(
        id=1,
        name="Test Card",
        database_id=1,
        dataset_query=MetabaseDatasetQuery(type="query", query={"source-table": 42}),
    )

    table_urns = metabase_source._get_table_urns_from_query_builder(card)
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
        password=SecretStr("pwd"),
    )
    ctx = PipelineContext(run_id="metabase-test")
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"id": "session-token"}
    mock_get.return_value = mock_response
    mock_post.return_value = mock_response
    mock_delete.return_value = mock_response

    metabase_source = MetabaseSource(ctx, metabase_config)
    metabase_source.get_datasource_from_id = MagicMock(  # type: ignore[method-assign]
        return_value=DatasourceInfo(
            platform="postgres",
            database_name="mydb",
            schema="public",
            platform_instance=None,
        )
    )
    metabase_source.get_source_table_from_id = MagicMock(  # type: ignore[method-assign]
        return_value=("public", "products")
    )

    referenced_card = MetabaseCard(
        id=123,
        name="Referenced Card",
        database_id=1,
        dataset_query=MetabaseDatasetQuery(type="query", query={"source-table": 42}),
    )

    metabase_source.get_card_details_by_id = MagicMock(return_value=referenced_card)  # type: ignore[method-assign]

    card = MetabaseCard(
        id=1,
        name="Test Card",
        database_id=1,
        dataset_query=MetabaseDatasetQuery(
            type="query", query={"source-table": "card__123"}
        ),
    )

    table_urns = metabase_source._get_table_urns_from_query_builder(card)
    expected_urn = (
        "urn:li:dataset:(urn:li:dataPlatform:postgres,mydb.public.products,PROD)"
    )
    assert table_urns == [expected_urn]
    metabase_source.get_card_details_by_id.assert_called_with("123")

    metabase_source.close()


@patch("requests.delete")
@patch("requests.Session.get")
@patch("requests.post")
def test_get_table_urns_handles_missing_database_id(mock_post, mock_get, mock_delete):
    """Test that missing database_id is handled gracefully"""
    metabase_config = MetabaseConfig(
        connect_uri="http://localhost:3000",
        username="test",
        password=SecretStr("pwd"),
    )
    ctx = PipelineContext(run_id="metabase-test")
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"id": "session-token"}
    mock_get.return_value = mock_response
    mock_post.return_value = mock_response
    mock_delete.return_value = mock_response

    metabase_source = MetabaseSource(ctx, metabase_config)

    card = MetabaseCard(
        id=1,
        name="Test Card",
        dataset_query=MetabaseDatasetQuery(
            type="native", native={"query": "SELECT * FROM users"}
        ),
    )

    table_urns = metabase_source._get_table_urns_from_native_query(card)
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
        password=SecretStr("pwd"),
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

    def mock_get_collections(url, **kwargs):
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


def test_clean_db_strips_jdbc_credentials():
    """JDBC connection strings with embedded credentials are sanitized for all db-field engines."""
    # H2: full JDBC file path with credentials
    assert (
        MetabaseDatabaseDetails(
            db="file:/plugins/sample-database.db;USER=GUEST;PASSWORD=guest"
        ).get_database_name("h2")
        == "sample-database"
    )
    # Defensive: semicolon params on a plain name (could occur on any engine)
    assert (
        MetabaseDatabaseDetails(db="mydb;sslmode=require").get_database_name("redshift")
        == "mydb"
    )
    # Snowflake / SQL Server: clean names are returned unchanged
    assert MetabaseDatabaseDetails(db="MYDB").get_database_name("snowflake") == "MYDB"
    assert (
        MetabaseDatabaseDetails(db="MyDatabase").get_database_name("sqlserver")
        == "MyDatabase"
    )
    assert MetabaseDatabaseDetails(db=None).get_database_name("h2") is None


def test_clean_db_h2_variants():
    """Common H2 db string formats are handled correctly."""
    assert MetabaseDatabaseDetails(db="mem:testdb").get_database_name("h2") == "testdb"
    assert (
        MetabaseDatabaseDetails(db="/data/myapp.db").get_database_name("h2") == "myapp"
    )


@patch("requests.delete")
@patch("requests.Session.get")
@patch("requests.post")
def test_construct_model_from_api_data(mock_post, mock_get, mock_delete):
    """Test construction of DatasetSnapshot for Metabase Models"""

    metabase_config = MetabaseConfig(
        connect_uri="http://localhost:3000",
        username="test",
        password=SecretStr("pwd"),
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
    metabase_source.get_datasource_from_id = MagicMock(  # type: ignore[method-assign]
        return_value=DatasourceInfo(
            platform="postgres",
            database_name="mydb",
            schema="public",
            platform_instance=None,
        )
    )
    collections_response = MagicMock()
    collections_response.status_code = 200
    collections_response.json.return_value = [
        {"id": "42", "name": "Analytics"},
    ]

    def mock_get_collections(url, **kwargs):
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

    # Set up mock to return full card details
    metabase_source.get_card_details_by_id = MagicMock(  # type: ignore[method-assign]
        return_value=MetabaseCard.model_validate(model_card)
    )

    # Call _emit_model_workunits() with MetabaseCardListItem
    model_card_list_item = MetabaseCardListItem(
        id=model_card["id"], type="model", name=model_card.get("name")
    )
    workunits = list(metabase_source._emit_model_workunits(model_card_list_item))

    assert len(workunits) > 0

    expected_urn = "urn:li:dataset:(urn:li:dataPlatform:metabase,model.123,PROD)"

    # Verify all work units are for the correct entity
    for wu in workunits:
        mcp = wu.metadata
        assert mcp.entityUrn == expected_urn  # type: ignore[union-attr]

    # Check for SchemaMetadata aspect

    schema_wu = next(
        (wu for wu in workunits if isinstance(wu.metadata.aspect, SchemaMetadataClass)),  # type: ignore[union-attr]
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
        password=SecretStr("pwd"),
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
    metabase_source.get_datasource_from_id = MagicMock(  # type: ignore[method-assign]
        return_value=DatasourceInfo(
            platform="postgres",
            database_name="mydb",
            schema="public",
            platform_instance=None,
        )
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

    # Set up mock to return full card details
    metabase_source.get_card_details_by_id = MagicMock(  # type: ignore[method-assign]
        return_value=MetabaseCard.model_validate(model_card)
    )

    # Call _emit_model_workunits() with MetabaseCardListItem
    model_card_list_item = MetabaseCardListItem(
        id=model_card["id"], type="model", name=model_card.get("name")
    )
    workunits = list(metabase_source._emit_model_workunits(model_card_list_item))
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
        password=SecretStr("pwd"),
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

    workunits = list(metabase_source.emit_model_workunits())
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
        password=SecretStr("pwd"),
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
        password=SecretStr("pwd"),
    )
    ctx = PipelineContext(run_id="metabase-test")
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"id": "session-token"}
    mock_get.return_value = mock_response
    mock_post.return_value = mock_response
    mock_delete.return_value = mock_response

    metabase_source = MetabaseSource(ctx, metabase_config)

    card_a = MetabaseCard(
        id=100,
        name="Card A",
        database_id=1,
        dataset_query=MetabaseDatasetQuery(
            type="query", query={"source-table": "card__200"}
        ),
    )

    card_b = MetabaseCard(
        id=200,
        name="Card B",
        database_id=1,
        dataset_query=MetabaseDatasetQuery(
            type="query", query={"source-table": "card__100"}
        ),
    )

    def mock_get_card_details(card_id):
        if card_id == "100" or card_id == 100:
            return card_a
        elif card_id == "200" or card_id == 200:
            return card_b
        return None

    metabase_source.get_card_details_by_id = MagicMock(  # type: ignore[method-assign]
        side_effect=mock_get_card_details
    )
    table_urns = metabase_source._get_table_urns_from_card(card_a)

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
def test_collection_name_sanitization_special_characters(input_name, expected_output):
    """Test that collection names with special characters are properly sanitized"""
    assert MetabaseCollection(id=1, name=input_name).tag_slug == expected_output


@patch("requests.delete")
@patch("requests.Session.get")
@patch("requests.post")
def test_collection_name_empty_after_sanitization(mock_post, mock_get, mock_delete):
    """Test that empty collection names after sanitization return None"""
    metabase_config = MetabaseConfig(
        connect_uri="http://localhost:3000",
        username="test",
        password=SecretStr("pwd"),
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

    def mock_get_collections(url, **kwargs):
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
def test_collection_404_error_handling(mock_post, mock_get, mock_delete):
    """Test that 404 errors for collections are handled silently with debug logging"""
    metabase_config = MetabaseConfig(
        connect_uri="http://localhost:3000",
        username="test",
        password=SecretStr("pwd"),
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

    def mock_get_collections(url, **kwargs):
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
    """Non-404 collection errors (401/403/500) are fatal: dashboards are discovered
    only by iterating collections, so a silent skip would drop every dashboard."""
    metabase_config = MetabaseConfig(
        connect_uri="http://localhost:3000",
        username="test",
        password=SecretStr("pwd"),
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

    def mock_get_collections(url, error=http_error, **kwargs):
        if "/api/collection/" in url:
            raise error
        return mock_response

    metabase_source.session.get = MagicMock(side_effect=mock_get_collections)  # type: ignore[method-assign]

    collections = metabase_source._get_collections_map()
    assert collections == {}

    assert len(metabase_source.report.failures) == 1
    failure_message = str(metabase_source.report.failures[0])
    assert "Check API credentials and permissions" in failure_message

    metabase_source.close()


@patch("requests.delete")
@patch("requests.Session.get")
@patch("requests.post")
def test_get_datasource_urn_respects_recursion_limit(mock_post, mock_get, mock_delete):
    """Test that get_datasource_urn() respects recursion limit parameter"""
    metabase_config = MetabaseConfig(
        connect_uri="http://localhost:3000",
        username="test",
        password=SecretStr("pwd"),
    )
    ctx = PipelineContext(run_id="metabase-test")

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"id": "session-token"}
    mock_get.return_value = mock_response
    mock_post.return_value = mock_response
    mock_delete.return_value = mock_response

    metabase_source = MetabaseSource(ctx, metabase_config)

    card = MetabaseCard(
        id=1,
        name="Test Card",
        database_id=1,
        dataset_query=MetabaseDatasetQuery(type="native"),
    )
    result = metabase_source.get_datasource_urn(
        card, recursion_depth=DATASOURCE_URN_RECURSION_LIMIT + 1
    )
    assert result == []
    assert len(metabase_source.report.warnings) > 0

    metabase_source.close()


@patch("requests.delete")
@patch("requests.Session.get")
@patch("requests.post")
def test_emit_chart_workunits_skips_models_when_extraction_enabled(
    mock_post, mock_get, mock_delete
):
    """Test that emit_chart_workunits() skips model cards when extract_models is True"""
    metabase_config = MetabaseConfig(
        connect_uri="http://localhost:3000",
        username="test",
        password=SecretStr("pwd"),
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

    def mock_session_get(url, **kwargs):
        if "/api/card" in url:
            return card_response
        return mock_response

    metabase_source.session.get = MagicMock(side_effect=mock_session_get)  # type: ignore[method-assign]

    called_card_ids = []

    def mock_emit_chart(card_info):
        called_card_ids.append(card_info.id)
        return []  # Return empty list of workunits

    metabase_source._emit_chart_workunits = MagicMock(  # type: ignore[method-assign,attr-defined]
        side_effect=mock_emit_chart
    )

    list(metabase_source.emit_chart_workunits())
    assert 1 in called_card_ids
    assert 3 in called_card_ids
    assert 2 not in called_card_ids  # Model should be skipped

    metabase_source.close()


@patch("requests.delete")
@patch("requests.Session.get")
@patch("requests.post")
def test_dashboard_edges_route_extracted_models_to_datasets(
    mock_post, mock_get, mock_delete
):
    metabase_config = MetabaseConfig(
        connect_uri="http://localhost:3000",
        username="test",
        password=SecretStr("pwd"),
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
    metabase_source._list_card_items = MagicMock(  # type: ignore[method-assign]
        return_value=[
            MetabaseCardListItem(id=10, name="Question", type="question"),
            MetabaseCardListItem(id=20, name="Model", type="model"),
        ]
    )
    metabase_source._get_json = MagicMock(  # type: ignore[method-assign]
        return_value={
            "id": 1,
            "name": "Sales",
            "dashcards": [
                {"id": 1, "dashboard_id": 1, "card": {"id": 10, "name": "Question"}},
                {"id": 2, "dashboard_id": 1, "card": {"id": 20, "name": "Model"}},
            ],
        }
    )

    workunits = list(
        metabase_source._emit_dashboard_workunits(
            MetabaseDashboardListItem(id=1, name="Sales", model="dashboard")
        )
    )
    dashboard_aspect = next(
        wu.metadata.aspect
        for wu in workunits
        if isinstance(wu.metadata, MetadataChangeProposalWrapper)
        and isinstance(wu.metadata.aspect, DashboardInfoClass)
    )
    assert [edge.destinationUrn for edge in dashboard_aspect.chartEdges or []] == [
        "urn:li:chart:(metabase,10)"
    ]
    assert [edge.destinationUrn for edge in dashboard_aspect.datasetEdges or []] == [
        "urn:li:dataset:(urn:li:dataPlatform:metabase,model.20,PROD)"
    ]

    metabase_source.close()


@patch("requests.delete")
@patch("requests.Session.get")
@patch("requests.post")
def test_malformed_sql_parsing_failure(mock_post, mock_get, mock_delete):
    """Test that malformed SQL that parser can't handle returns empty list gracefully"""
    metabase_config = MetabaseConfig(
        connect_uri="http://localhost:3000",
        username="test",
        password=SecretStr("pwd"),
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

    metabase_source.get_datasource_from_id = MagicMock(  # type: ignore[method-assign]
        return_value=DatasourceInfo(
            platform="postgres",
            database_name="mydb",
            schema="public",
            platform_instance=None,
        )
    )

    card = MetabaseCard(
        id=1,
        name="Test Card",
        database_id=1,
        dataset_query=MetabaseDatasetQuery(
            type="native",
            native={"query": "SELECT * FROM {{invalid syntax}} WHERE [[broken clause"},
        ),
    )

    table_urns = metabase_source._get_table_urns_from_native_query(card)
    assert table_urns == []
    assert metabase_source.report.native_sql_parse_failures == 1

    metabase_source.close()


@patch("requests.delete")
@patch("requests.Session.get")
@patch("requests.post")
def test_sql_with_cte_and_subqueries(mock_post, mock_get, mock_delete):
    """Test SQL parsing with CTEs and complex subqueries"""
    metabase_config = MetabaseConfig(
        connect_uri="http://localhost:3000",
        username="test",
        password=SecretStr("pwd"),
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

    metabase_source.get_datasource_from_id = MagicMock(  # type: ignore[method-assign]
        return_value=DatasourceInfo(
            platform="postgres",
            database_name="mydb",
            schema="public",
            platform_instance=None,
        )
    )

    card = MetabaseCard(
        id=1,
        name="Test Card",
        database_id=1,
        dataset_query=MetabaseDatasetQuery(
            type="native",
            native={
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
            },
        ),
    )

    table_urns = metabase_source._get_table_urns_from_native_query(card)
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
        password=SecretStr("pwd"),
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

    def mock_get_cards(url, **kwargs):
        if "/api/card" in url:
            return error_response
        return mock_response

    metabase_source.session.get = MagicMock(side_effect=mock_get_cards)  # type: ignore[method-assign]
    workunits = list(metabase_source.emit_chart_workunits())
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
        password=SecretStr("pwd"),
    )
    ctx = PipelineContext(run_id="metabase-test")

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"id": "session-token"}
    mock_get.return_value = mock_response
    mock_post.return_value = mock_response
    mock_delete.return_value = mock_response

    metabase_source = MetabaseSource(ctx, metabase_config)

    metabase_source.get_datasource_from_id = MagicMock(  # type: ignore[method-assign]
        return_value=DatasourceInfo(
            platform="postgres",
            database_name="mydb",
            schema="public",
            platform_instance=None,
        )
    )

    test_cases = [
        MetabaseCard(
            id=1,
            name="Test1",
            database_id=1,
            dataset_query=MetabaseDatasetQuery(type="native", native={"query": ""}),
        ),
        MetabaseCard(
            id=2,
            name="Test2",
            database_id=1,
            dataset_query=MetabaseDatasetQuery(type="native", native={"query": None}),
        ),
        MetabaseCard(
            id=3,
            name="Test3",
            database_id=1,
            dataset_query=MetabaseDatasetQuery(type="native", native={}),
        ),
        MetabaseCard(
            id=4,
            name="Test4",
            database_id=1,
            dataset_query=MetabaseDatasetQuery(type="native"),
        ),
    ]

    for card in test_cases:
        table_urns = metabase_source._get_table_urns_from_native_query(card)
        assert table_urns == [], f"Failed for: {card}"

    metabase_source.close()


@patch("requests.delete")
@patch("requests.Session.get")
@patch("requests.post")
def test_root_collection_kept_for_dashboard_discovery(mock_post, mock_get, mock_delete):
    """Root stays in the collections map for dashboard discovery, but is not a container."""
    metabase_config = MetabaseConfig(
        connect_uri="http://localhost:3000",
        username="test",
        password=SecretStr("pwd"),
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
            "authority_level": None,
            "can_write": True,
            "name": "Our analytics",
            "id": "root",
            "is_personal": False,
        },
        {"id": 1, "name": "Analytics"},
    ]

    metabase_source.session.get = MagicMock(return_value=collections_response)  # type: ignore[method-assign]
    collections = metabase_source._get_collections_map()

    assert "root" in collections
    assert collections["root"].is_root
    assert "1" in collections
    assert len(metabase_source.report.warnings) == 0

    container_urns = {
        wu.metadata.entityUrn
        for wu in metabase_source.emit_collection_containers()
        if isinstance(wu.metadata, MetadataChangeProposalWrapper)
    }
    assert metabase_source._gen_collection_key(1).as_urn() in container_urns
    assert all("root" not in (urn or "") for urn in container_urns)

    metabase_source.close()


def test_passthrough_cll_emits_copy_1to1_lineage():
    """A query-builder card that only selects from a single source table (no
    filter/aggregation/join) must produce COPY lineage with a 1:1 column mapping."""
    ctx = PipelineContext(run_id="metabase-test")
    config = MetabaseConfig(username="un", password=SecretStr("pwd"))
    metabase = FakeMetabaseSource(ctx, config)

    metabase.get_datasource_from_id = MagicMock(  # type: ignore[method-assign]
        return_value=DatasourceInfo(
            platform="postgres",
            database_name="mydb",
            schema="public",
            platform_instance=None,
        )
    )
    metabase.get_source_table_from_id = MagicMock(  # type: ignore[method-assign]
        return_value=("public", "orders")
    )

    card = MetabaseCard(
        id=1,
        name="Orders Model",
        database_id=1,
        dataset_query=MetabaseDatasetQuery(type="query", query={"source-table": 42}),
        result_metadata=[
            {"name": "order_id", "base_type": "type/Integer"},
            {"name": "amount", "base_type": "type/Decimal"},
        ],
    )

    model_urn = metabase._model_urn(card.id)
    lineage = metabase._get_passthrough_cll(card, model_urn)

    assert lineage is not None
    source_urn = "urn:li:dataset:(urn:li:dataPlatform:postgres,mydb.public.orders,PROD)"
    assert [u.dataset for u in lineage.upstreams] == [source_urn]
    assert lineage.upstreams[0].type == DatasetLineageTypeClass.COPY

    assert lineage.fineGrainedLineages is not None
    downstream_cols = {
        (fgl.downstreams or [""])[0].rsplit(",", 1)[-1].rstrip(")")
        for fgl in lineage.fineGrainedLineages
    }
    assert downstream_cols == {"order_id", "amount"}


def test_passthrough_over_card_flattened_to_physical_does_not_invent_columns():
    """A pass-through model wrapping a question (card__N) that flattens to a physical
    table exposes the nested question's computed outputs in result_metadata. Copying
    those 1:1 would invent physical columns (e.g. orders.count), so only coarse
    table-level COPY must be emitted."""
    ctx = PipelineContext(run_id="metabase-test")
    config = MetabaseConfig(
        username="un", password=SecretStr("pwd"), extract_models=False
    )
    metabase = FakeMetabaseSource(ctx, config)

    metabase.get_datasource_from_id = MagicMock(  # type: ignore[method-assign]
        return_value=DatasourceInfo(
            platform="postgres",
            database_name="mydb",
            schema="public",
            platform_instance=None,
        )
    )
    metabase.get_source_table_from_id = MagicMock(  # type: ignore[method-assign]
        return_value=("public", "orders")
    )
    # The wrapped question aggregates orders into a `count`; it flattens to the
    # physical orders table (not a model dataset) since it is not a model.
    referenced = MetabaseCard(
        id=2,
        name="Orders Count",
        type="question",
        database_id=1,
        dataset_query=MetabaseDatasetQuery(type="query", query={"source-table": 42}),
    )
    metabase.get_card_details_by_id = MagicMock(return_value=referenced)  # type: ignore[method-assign]

    card = MetabaseCard(
        id=1,
        name="Passthrough of Count",
        database_id=1,
        dataset_query=MetabaseDatasetQuery(
            type="query", query={"source-table": "card__2"}
        ),
        result_metadata=[{"name": "count", "field_ref": ["field", "count", None]}],
    )

    model_urn = metabase._model_urn(card.id)
    lineage = metabase._get_passthrough_cll(card, model_urn)

    assert lineage is not None
    source_urn = "urn:li:dataset:(urn:li:dataPlatform:postgres,mydb.public.orders,PROD)"
    assert [u.dataset for u in lineage.upstreams] == [source_urn]
    assert lineage.upstreams[0].type == DatasetLineageTypeClass.COPY
    assert not lineage.fineGrainedLineages
    assert metabase.report.query_builder_cll_dropped == 1


def test_passthrough_over_model_card_maps_columns_1to1():
    """A pass-through wrapping a model (card__N, extract_models on) resolves to that
    model's dataset URN, whose columns share the result_metadata names, so the 1:1
    column COPY is sound."""
    ctx = PipelineContext(run_id="metabase-test")
    config = MetabaseConfig(
        username="un", password=SecretStr("pwd"), extract_models=True
    )
    metabase = FakeMetabaseSource(ctx, config)

    referenced = MetabaseCard(
        id=2,
        name="Upstream Model",
        type="model",
        database_id=1,
        dataset_query=MetabaseDatasetQuery(type="query", query={"source-table": 42}),
    )
    metabase.get_card_details_by_id = MagicMock(return_value=referenced)  # type: ignore[method-assign]

    card = MetabaseCard(
        id=1,
        name="Passthrough of Model",
        database_id=1,
        dataset_query=MetabaseDatasetQuery(
            type="query", query={"source-table": "card__2"}
        ),
        result_metadata=[{"name": "revenue", "field_ref": ["field", "revenue", None]}],
    )

    model_urn = metabase._model_urn(card.id)
    lineage = metabase._get_passthrough_cll(card, model_urn)

    assert lineage is not None
    assert [u.dataset for u in lineage.upstreams] == [metabase._model_urn(2)]
    assert lineage.upstreams[0].type == DatasetLineageTypeClass.COPY
    assert lineage.fineGrainedLineages is not None
    assert len(lineage.fineGrainedLineages) == 1


def test_model_lineage_passthrough_prefers_copy_over_transformed():
    """A pass-through model whose result metadata carries id-based field refs would
    resolve via the query-builder path as TRANSFORMED; _get_model_lineage must route
    it through the pass-through COPY path instead (1:1 copy of the source)."""
    ctx = PipelineContext(run_id="metabase-test")
    config = MetabaseConfig(username="un", password=SecretStr("pwd"))
    metabase = FakeMetabaseSource(ctx, config)

    metabase.get_datasource_from_id = MagicMock(  # type: ignore[method-assign]
        return_value=DatasourceInfo(
            platform="postgres",
            database_name="mydb",
            schema="public",
            platform_instance=None,
        )
    )
    metabase.get_source_table_from_id = MagicMock(  # type: ignore[method-assign]
        return_value=("public", "orders")
    )
    # Field ids resolve, so the query-builder path could produce fine-grained
    # TRANSFORMED lineage if it were tried first.
    metabase.get_field_from_id = MagicMock(  # type: ignore[method-assign]
        return_value=MetabaseField(id=101, name="order_id", table_id=42)
    )

    card = MetabaseCard(
        id=1,
        name="Orders Model",
        database_id=1,
        dataset_query=MetabaseDatasetQuery(type="query", query={"source-table": 42}),
        result_metadata=[
            {"name": "order_id", "field_ref": ["field", 101, None]},
            {"name": "amount", "field_ref": ["field", 102, None]},
        ],
    )

    model_urn = metabase._model_urn(card.id)
    lineage = metabase._get_model_lineage(card, model_urn)

    assert lineage is not None
    assert lineage.upstreams[0].type == DatasetLineageTypeClass.COPY


def test_map_type_jsonb_prefers_bytes_over_json_substring():
    """`type/JSONB` must map to Bytes, not match the `JSON` substring -> String."""
    ctx = PipelineContext(run_id="metabase-test")
    config = MetabaseConfig(username="un", password=SecretStr("pwd"))
    metabase = FakeMetabaseSource(ctx, config)

    assert isinstance(
        metabase._map_metabase_type_to_datahub_type("type/JSONB"), BytesTypeClass
    )
    assert isinstance(
        metabase._map_metabase_type_to_datahub_type("type/JSON"), StringTypeClass
    )


def test_normalize_honors_standard_lowercase_mixin_flag():
    """The standard convert_urns_to_lowercase mixin flag (shown in recipe docs) must
    lowercase lineage table URNs, not just the connector-specific flag."""
    ctx = PipelineContext(run_id="metabase-test")

    default_src = FakeMetabaseSource(
        ctx, MetabaseConfig(username="un", password=SecretStr("pwd"))
    )
    assert default_src._normalize("MyTable") == "MyTable"

    mixin_src = FakeMetabaseSource(
        ctx,
        MetabaseConfig(
            username="un", password=SecretStr("pwd"), convert_urns_to_lowercase=True
        ),
    )
    assert mixin_src._normalize("MyTable") == "mytable"


def test_virtual_dashcard_null_card_does_not_drop_dashboard():
    """A virtual dashcard (card: null, e.g. text/heading) must not fail validation
    for the whole dashboard; the emitter skips cards with no `card`."""
    dashboard = MetabaseDashboard.model_validate(
        {
            "id": 7,
            "name": "Mixed",
            "dashcards": [
                {"id": 1, "card": None},
                {"id": 2, "card": {"id": 42, "name": "Q"}, "dashboard_id": 7},
            ],
        }
    )
    assert len(dashboard.dashcards) == 2
    assert dashboard.dashcards[0].card is None
    assert dashboard.dashcards[1].card is not None


def test_model_subtypes_passthrough_vs_transformed():
    """A pure pass-through model is just a METABASE_MODEL; anything that
    transforms (filter/aggregation/join) is additionally a VIEW."""
    ctx = PipelineContext(run_id="metabase-test")
    config = MetabaseConfig(username="un", password=SecretStr("pwd"))
    metabase = FakeMetabaseSource(ctx, config)

    passthrough = MetabaseCard(
        id=1,
        name="Passthrough",
        database_id=1,
        dataset_query=MetabaseDatasetQuery(type="query", query={"source-table": 42}),
    )
    assert metabase._get_model_subtypes(passthrough) == [DatasetSubTypes.METABASE_MODEL]

    transformed = MetabaseCard(
        id=2,
        name="Transformed",
        database_id=1,
        dataset_query=MetabaseDatasetQuery(
            type="query",
            query={
                "source-table": 42,
                "filter": ["=", ["field", 1, None], 5],
            },
        ),
    )
    assert metabase._get_model_subtypes(transformed) == [
        DatasetSubTypes.METABASE_MODEL,
        DatasetSubTypes.VIEW,
    ]


def test_query_builder_cll_recovers_named_ref_on_single_source():
    """A single-source query-builder card that selects one column by numeric id and one
    by name emits CLL for both: the id column resolves directly, and the name-only column
    is attributed to the sole upstream table by name (not dropped)."""
    ctx = PipelineContext(run_id="metabase-test")
    config = MetabaseConfig(username="un", password=SecretStr("pwd"))
    metabase = FakeMetabaseSource(ctx, config)

    metabase.get_datasource_from_id = MagicMock(  # type: ignore[method-assign]
        return_value=DatasourceInfo(
            platform="postgres",
            database_name="mydb",
            schema="public",
            platform_instance=None,
        )
    )
    metabase.get_source_table_from_id = MagicMock(  # type: ignore[method-assign]
        return_value=("public", "orders")
    )
    metabase.get_field_from_id = MagicMock(  # type: ignore[method-assign]
        return_value=MetabaseField(id=100, name="col_a", table_id=42)
    )

    card = MetabaseCard(
        id=1,
        name="Mixed Refs",
        database_id=1,
        dataset_query=MetabaseDatasetQuery(
            type="query",
            query={
                "source-table": 42,
                "fields": [
                    ["field", 100, None],
                    ["field", "computed_col", None],
                ],
            },
        ),
        result_metadata=[
            {
                "name": "col_a",
                "base_type": "type/Integer",
                "field_ref": ["field", 100, None],
            },
            {
                "name": "computed_col",
                "base_type": "type/Text",
                "field_ref": ["field", "computed_col", None],
            },
        ],
    )

    model_urn = metabase._model_urn(card.id)
    lineage = metabase._get_cll_from_query_builder(card, model_urn)

    assert lineage is not None
    assert lineage.fineGrainedLineages is not None
    # Both columns map upstream: col_a by field id, computed_col by name to the single
    # source table. Downstream field path -> upstream field path.
    downstreams = {
        (fg.downstreams or [""])[0].rsplit(",", 1)[-1].rstrip(")"): (
            fg.upstreams or [""]
        )[0]
        .rsplit(",", 1)[-1]
        .rstrip(")")
        for fg in lineage.fineGrainedLineages
    }
    assert downstreams == {"col_a": "col_a", "computed_col": "computed_col"}

    # Single-source cards recover name-only columns, so nothing is genuinely dropped.
    assert metabase.report.mbql_field_refs_by_name_dropped == 0
    assert metabase.report.query_builder_cll_dropped == 0


@patch("requests.delete")
@patch("requests.Session.get")
@patch("requests.post")
def test_nested_collection_sets_parent_container(mock_post, mock_get, mock_delete):
    metabase_config = MetabaseConfig(
        connect_uri="http://localhost:3000",
        username="test",
        password=SecretStr("pwd"),
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
        {"id": "root", "name": "Our analytics"},
        {"id": 200, "name": "Engineering", "parent_id": None, "location": "/"},
        {"id": 201, "name": "Product", "parent_id": 200, "location": "/200/"},
    ]
    metabase_source.session.get = MagicMock(return_value=collections_response)  # type: ignore[method-assign]

    parent_aspects = [
        wu.metadata
        for wu in metabase_source.emit_collection_containers()
        if isinstance(wu.metadata, MetadataChangeProposalWrapper)
        and isinstance(wu.metadata.aspect, ContainerClass)
    ]
    assert len(parent_aspects) == 1
    assert (
        parent_aspects[0].entityUrn == metabase_source._gen_collection_key(201).as_urn()
    )
    parent_container = parent_aspects[0].aspect
    assert isinstance(parent_container, ContainerClass)
    assert (
        parent_container.container == metabase_source._gen_collection_key(200).as_urn()
    )

    metabase_source.close()


@patch("requests.session")
def test_setup_session_rejects_html_current_user(mock_session):
    metabase_config = MetabaseConfig(
        connect_uri="http://localhost:3000", api_key=SecretStr("key")
    )
    ctx = PipelineContext(run_id="metabase-test-sso-html")

    mock_session_instance = MagicMock()
    mock_session_instance.headers = {}
    mock_session.return_value = mock_session_instance

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.side_effect = ValueError("Expecting value")
    mock_session_instance.get.return_value = mock_response

    metabase_source = MetabaseSource(ctx, metabase_config)
    assert len(metabase_source.report.failures) == 1
    metabase_source.close()


def test_create_input_field_uses_upstream_column_path():
    ctx = PipelineContext(run_id="metabase-test")
    config = MetabaseConfig(username="un", password=SecretStr("pwd"))
    metabase = FakeMetabaseSource(ctx, config)
    upstream = builder.make_schema_field_urn(
        "urn:li:dataset:(urn:li:dataPlatform:postgres,db.public.film,PROD)",
        "rating",
    )
    meta = MetabaseResultMetadata(
        name="count", display_name="Count", base_type="type/Integer"
    )
    field = metabase._create_input_field(upstream, meta)
    assert field.schemaFieldUrn == upstream
    assert field.schemaField is not None
    assert field.schemaField.fieldPath == "rating"


@patch("requests.delete")
@patch("requests.Session.get")
@patch("requests.post")
def test_native_sql_input_fields_resolve_per_table(mock_post, mock_get, mock_delete):
    metabase_config = MetabaseConfig(
        connect_uri="http://localhost:3000",
        username="test",
        password=SecretStr("pwd"),
    )
    ctx = PipelineContext(run_id="metabase-test")
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"id": "session-token"}
    mock_get.return_value = mock_response
    mock_post.return_value = mock_response
    mock_delete.return_value = mock_response

    metabase_source = MetabaseSource(ctx, metabase_config)
    users_urn = "urn:li:dataset:(urn:li:dataPlatform:postgres,mydb.public.users,PROD)"
    orders_urn = "urn:li:dataset:(urn:li:dataPlatform:postgres,mydb.public.orders,PROD)"
    users_id_urn = builder.make_schema_field_urn(users_urn, "id")
    orders_amount_urn = builder.make_schema_field_urn(orders_urn, "amount")

    user_col = MagicMock()
    user_col.downstream.column = "user_id"
    user_col.upstream_schema_field_urns.return_value = [users_id_urn]
    order_col = MagicMock()
    order_col.downstream.column = "amount"
    order_col.upstream_schema_field_urns.return_value = [orders_amount_urn]

    parsed = MagicMock()
    parsed.debug_info.table_error = None
    parsed.column_lineage = [user_col, order_col]
    metabase_source._parse_native_sql = MagicMock(  # type: ignore[method-assign]
        return_value=(parsed, DatasourceInfo(platform="postgres"))
    )

    card = MetabaseCard(
        id=9,
        name="Join SQL",
        database_id=1,
        dataset_query=MetabaseDatasetQuery(
            type="native",
            native={
                "query": "SELECT users.id AS user_id, orders.amount FROM users JOIN orders ON users.id = orders.user_id"
            },
        ),
        result_metadata=[
            {"name": "user_id", "base_type": "type/Integer"},
            {"name": "amount", "base_type": "type/Decimal"},
        ],
    )

    fields = metabase_source._get_input_fields_from_card(card)
    assert {f.schemaFieldUrn for f in fields} == {users_id_urn, orders_amount_urn}
    assert {f.schemaField.fieldPath for f in fields if f.schemaField} == {
        "id",
        "amount",
    }
    metabase_source.close()


@patch("requests.delete")
@patch("requests.Session.get")
@patch("requests.post")
def test_native_sql_input_fields_table_error_returns_empty(
    mock_post, mock_get, mock_delete
):
    metabase_config = MetabaseConfig(
        connect_uri="http://localhost:3000",
        username="test",
        password=SecretStr("pwd"),
    )
    ctx = PipelineContext(run_id="metabase-test")
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"id": "session-token"}
    mock_get.return_value = mock_response
    mock_post.return_value = mock_response
    mock_delete.return_value = mock_response

    metabase_source = MetabaseSource(ctx, metabase_config)
    parsed = MagicMock()
    parsed.debug_info.table_error = "no tables"
    parsed.column_lineage = []
    metabase_source._parse_native_sql = MagicMock(  # type: ignore[method-assign]
        return_value=(parsed, DatasourceInfo(platform="postgres"))
    )
    card = MetabaseCard(
        id=10,
        name="Broken SQL",
        database_id=1,
        dataset_query=MetabaseDatasetQuery(type="native", native={"query": "SELECT 1"}),
        result_metadata=[{"name": "user_id", "base_type": "type/Integer"}],
    )

    fields = metabase_source._get_input_fields_from_native_sql(card)
    assert fields == []
    assert any(
        "Native SQL Lineage Parse Failure" in str(warning)
        for warning in metabase_source.report.warnings
    )
    metabase_source.close()


@patch("requests.delete")
@patch("requests.Session.get")
@patch("requests.post")
def test_native_sql_input_fields_unresolved_column_warns(
    mock_post, mock_get, mock_delete
):
    metabase_config = MetabaseConfig(
        connect_uri="http://localhost:3000",
        username="test",
        password=SecretStr("pwd"),
    )
    ctx = PipelineContext(run_id="metabase-test")
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"id": "session-token"}
    mock_get.return_value = mock_response
    mock_post.return_value = mock_response
    mock_delete.return_value = mock_response

    metabase_source = MetabaseSource(ctx, metabase_config)
    parsed = MagicMock()
    parsed.debug_info.table_error = None
    parsed.column_lineage = []
    metabase_source._parse_native_sql = MagicMock(  # type: ignore[method-assign]
        return_value=(parsed, DatasourceInfo(platform="postgres"))
    )
    card = MetabaseCard(
        id=11,
        name="Unresolved SQL",
        database_id=1,
        dataset_query=MetabaseDatasetQuery(
            type="native", native={"query": "SELECT 1 AS user_id"}
        ),
        result_metadata=[{"name": "user_id", "base_type": "type/Integer"}],
    )

    fields = metabase_source._get_input_fields_from_native_sql(card)
    assert fields == []
    assert any(
        "Native SQL Input Field Unresolved" in str(warning)
        for warning in metabase_source.report.warnings
    )
    metabase_source.close()


@patch("requests.delete")
@patch("requests.Session.get")
@patch("requests.post")
def test_native_sql_input_fields_accumulate_duplicate_output_columns(
    mock_post, mock_get, mock_delete
):
    metabase_config = MetabaseConfig(
        connect_uri="http://localhost:3000",
        username="test",
        password=SecretStr("pwd"),
    )
    ctx = PipelineContext(run_id="metabase-test")
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"id": "session-token"}
    mock_get.return_value = mock_response
    mock_post.return_value = mock_response
    mock_delete.return_value = mock_response

    metabase_source = MetabaseSource(ctx, metabase_config)
    users_urn = "urn:li:dataset:(urn:li:dataPlatform:postgres,mydb.public.users,PROD)"
    orders_urn = "urn:li:dataset:(urn:li:dataPlatform:postgres,mydb.public.orders,PROD)"
    users_id_urn = builder.make_schema_field_urn(users_urn, "id")
    orders_user_id_urn = builder.make_schema_field_urn(orders_urn, "user_id")

    first = MagicMock()
    first.downstream.column = "user_id"
    first.upstream_schema_field_urns.return_value = [users_id_urn]
    second = MagicMock()
    second.downstream.column = "user_id"
    second.upstream_schema_field_urns.return_value = [orders_user_id_urn]

    parsed = MagicMock()
    parsed.debug_info.table_error = None
    parsed.column_lineage = [first, second]
    metabase_source._parse_native_sql = MagicMock(  # type: ignore[method-assign]
        return_value=(parsed, DatasourceInfo(platform="postgres"))
    )
    card = MetabaseCard(
        id=12,
        name="Union SQL",
        database_id=1,
        dataset_query=MetabaseDatasetQuery(
            type="native", native={"query": "SELECT id AS user_id FROM users"}
        ),
        result_metadata=[{"name": "user_id", "base_type": "type/Integer"}],
    )

    fields = metabase_source._get_input_fields_from_native_sql(card)
    assert {f.schemaFieldUrn for f in fields} == {users_id_urn, orders_user_id_urn}
    metabase_source.close()


def test_card_null_result_metadata_coerced_to_empty_list():
    # Metabase returns result_metadata: null for cards that have never been run;
    # the model must accept it rather than failing validation and dropping the card.
    card = MetabaseCard.model_validate(
        {
            "id": 1,
            "name": "Never Run",
            "database_id": 1,
            "result_metadata": None,
        }
    )
    assert card.result_metadata == []
