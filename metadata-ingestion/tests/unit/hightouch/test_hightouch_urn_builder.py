from datetime import datetime
from unittest.mock import patch

import pytest

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.hightouch.config import (
    HightouchAPIConfig,
    HightouchSourceConfig,
    PlatformDetail,
)
from datahub.ingestion.source.hightouch.hightouch import (
    HightouchSource as HightouchIngestionSource,
)
from datahub.ingestion.source.hightouch.models import (
    HightouchDestination,
    HightouchModel,
    HightouchSourceConnection,
)
from datahub.ingestion.source.hightouch.urn_builder import HightouchUrnBuilder
from datahub.metadata.urns import DatasetUrn


@pytest.fixture
def hightouch_config():
    return HightouchSourceConfig(
        api_config=HightouchAPIConfig(
            api_key="test_api_key",
            base_url="https://api.hightouch.com/api/v1",
        ),
        env="PROD",
        platform_instance="prod-instance",
        include_sibling_relationships=True,
    )


@pytest.fixture
def pipeline_context():
    return PipelineContext(run_id="test_run")


@pytest.fixture
def mock_source():
    return HightouchIngestionSource(
        HightouchSourceConfig(
            api_config=HightouchAPIConfig(api_key="test"),
            env="PROD",
        ),
        PipelineContext(run_id="test"),
    )


@patch("datahub.ingestion.source.hightouch.hightouch.HightouchAPIClient")
def test_urn_builder_make_model_urn_on_hightouch_platform(
    mock_api_client_class, pipeline_context
):
    config = HightouchSourceConfig(
        api_config=HightouchAPIConfig(api_key="test"),
        env="PROD",
        platform_instance="prod-instance",
        include_sibling_relationships=False,
    )

    source = HightouchIngestionSource(config, pipeline_context)
    urn_builder = HightouchUrnBuilder(
        config=source.config,
        get_platform_for_source=source._get_platform_for_source,
        get_platform_for_destination=source._get_platform_for_destination,
    )

    model = HightouchModel(
        id="model_123",
        name="Customer Model",
        slug="customer-model",
        workspace_id="workspace_1",
        source_id="source_1",
        query_type="raw_sql",
        created_at=datetime(2023, 1, 1),
        updated_at=datetime(2023, 1, 2),
    )

    source_connection = HightouchSourceConnection(
        id="source_1",
        name="Snowflake Source",
        slug="snowflake-source",
        type="snowflake",
        workspace_id="workspace_1",
        created_at=datetime(2023, 1, 1),
        updated_at=datetime(2023, 1, 2),
    )

    urn = urn_builder.make_model_urn(model, source_connection)

    assert isinstance(urn, (str, DatasetUrn))
    urn_str = str(urn)
    assert "hightouch" in urn_str
    assert "customer-model" in urn_str
    assert "PROD" in urn_str


@patch("datahub.ingestion.source.hightouch.hightouch.HightouchAPIClient")
def test_urn_builder_make_model_urn_on_source_platform(
    mock_api_client_class, pipeline_context
):
    config = HightouchSourceConfig(
        api_config=HightouchAPIConfig(api_key="test"),
        env="PROD",
        platform_instance="prod-instance",
        include_sibling_relationships=True,
        sources_to_platform_instance={
            "source_1": PlatformDetail(
                platform="snowflake",
                platform_instance="snowflake-prod",
                env="PROD",
                database="analytics",
            )
        },
    )

    source = HightouchIngestionSource(config, pipeline_context)
    urn_builder = HightouchUrnBuilder(
        config=source.config,
        get_platform_for_source=source._get_platform_for_source,
        get_platform_for_destination=source._get_platform_for_destination,
    )

    model = HightouchModel(
        id="model_123",
        name="Customer Model",
        slug="customer-model",
        workspace_id="workspace_1",
        source_id="source_1",
        query_type="raw_sql",
        created_at=datetime(2023, 1, 1),
        updated_at=datetime(2023, 1, 2),
    )

    source_connection = HightouchSourceConnection(
        id="source_1",
        name="Snowflake Source",
        slug="snowflake-source",
        type="snowflake",
        workspace_id="workspace_1",
        created_at=datetime(2023, 1, 1),
        updated_at=datetime(2023, 1, 2),
    )

    urn = urn_builder.make_model_urn(model, source_connection)

    assert isinstance(urn, (str, DatasetUrn))
    urn_str = str(urn)
    assert "hightouch" in urn_str
    assert "customer-model" in urn_str
    assert "PROD" in urn_str


@patch("datahub.ingestion.source.hightouch.hightouch.HightouchAPIClient")
def test_urn_builder_make_upstream_table_urn(mock_api_client_class, pipeline_context):
    config = HightouchSourceConfig(
        api_config=HightouchAPIConfig(api_key="test"),
        env="PROD",
        sources_to_platform_instance={
            "source_1": PlatformDetail(
                platform="snowflake",
                platform_instance="snowflake-prod",
                env="PROD",
                database="raw_data",
            )
        },
    )

    source = HightouchIngestionSource(config, pipeline_context)
    urn_builder = HightouchUrnBuilder(
        config=source.config,
        get_platform_for_source=source._get_platform_for_source,
        get_platform_for_destination=source._get_platform_for_destination,
    )

    source_connection = HightouchSourceConnection(
        id="source_1",
        name="Snowflake Source",
        slug="snowflake-source",
        type="snowflake",
        workspace_id="workspace_1",
        created_at=datetime(2023, 1, 1),
        updated_at=datetime(2023, 1, 2),
    )

    urn = urn_builder.make_upstream_table_urn("customers", source_connection)

    assert isinstance(urn, (str, DatasetUrn))
    urn_str = str(urn)
    assert "snowflake" in urn_str
    assert "raw_data.customers" in urn_str
    assert "PROD" in urn_str


@patch("datahub.ingestion.source.hightouch.hightouch.HightouchAPIClient")
def test_urn_builder_make_upstream_table_urn_with_qualified_name(
    mock_api_client_class, pipeline_context
):
    config = HightouchSourceConfig(
        api_config=HightouchAPIConfig(api_key="test"),
        env="PROD",
        sources_to_platform_instance={
            "source_1": PlatformDetail(
                platform="snowflake",
                platform_instance="snowflake-prod",
                env="PROD",
                database="raw_data",
            )
        },
    )

    source = HightouchIngestionSource(config, pipeline_context)
    urn_builder = HightouchUrnBuilder(
        config=source.config,
        get_platform_for_source=source._get_platform_for_source,
        get_platform_for_destination=source._get_platform_for_destination,
    )

    source_connection = HightouchSourceConnection(
        id="source_1",
        name="Snowflake Source",
        slug="snowflake-source",
        type="snowflake",
        workspace_id="workspace_1",
        created_at=datetime(2023, 1, 1),
        updated_at=datetime(2023, 1, 2),
    )

    urn = urn_builder.make_upstream_table_urn("schema.customers", source_connection)

    assert isinstance(urn, (str, DatasetUrn))
    urn_str = str(urn)
    assert "snowflake" in urn_str
    assert "schema.customers" in urn_str
    assert "raw_data" not in urn_str


@patch("datahub.ingestion.source.hightouch.hightouch.HightouchAPIClient")
def test_urn_builder_make_destination_urn(mock_api_client_class, pipeline_context):
    config = HightouchSourceConfig(
        api_config=HightouchAPIConfig(api_key="test"),
        env="PROD",
        destination_to_platform_instance={
            "dest_1": PlatformDetail(
                platform="salesforce",
                platform_instance="salesforce-prod",
                env="PROD",
            )
        },
    )

    source = HightouchIngestionSource(config, pipeline_context)
    urn_builder = HightouchUrnBuilder(
        config=source.config,
        get_platform_for_source=source._get_platform_for_source,
        get_platform_for_destination=source._get_platform_for_destination,
    )

    destination = HightouchDestination(
        id="dest_1",
        name="Salesforce Destination",
        slug="salesforce-dest",
        type="salesforce",
        workspace_id="workspace_1",
        created_at=datetime(2023, 1, 1),
        updated_at=datetime(2023, 1, 2),
    )

    urn = urn_builder.make_destination_urn("Contact", destination)

    assert isinstance(urn, (str, DatasetUrn))
    urn_str = str(urn)
    assert "salesforce" in urn_str
    assert "Contact" in urn_str
    assert "PROD" in urn_str


@patch("datahub.ingestion.source.hightouch.hightouch.HightouchAPIClient")
def test_urn_builder_make_destination_urn_with_database(
    mock_api_client_class, pipeline_context
):
    config = HightouchSourceConfig(
        api_config=HightouchAPIConfig(api_key="test"),
        env="PROD",
        destination_to_platform_instance={
            "dest_1": PlatformDetail(
                platform="snowflake",
                platform_instance="snowflake-prod",
                env="PROD",
                database="analytics",
            )
        },
    )

    source = HightouchIngestionSource(config, pipeline_context)
    urn_builder = HightouchUrnBuilder(
        config=source.config,
        get_platform_for_source=source._get_platform_for_source,
        get_platform_for_destination=source._get_platform_for_destination,
    )

    destination = HightouchDestination(
        id="dest_1",
        name="Snowflake Destination",
        slug="snowflake-dest",
        type="snowflake",
        workspace_id="workspace_1",
        created_at=datetime(2023, 1, 1),
        updated_at=datetime(2023, 1, 2),
    )

    urn = urn_builder.make_destination_urn("customers_enriched", destination)

    assert isinstance(urn, (str, DatasetUrn))
    urn_str = str(urn)
    assert "snowflake" in urn_str
    assert "analytics.customers_enriched" in urn_str
    assert "PROD" in urn_str


@patch("datahub.ingestion.source.hightouch.hightouch.HightouchAPIClient")
def test_urn_builder_caching_source_details(mock_api_client_class, pipeline_context):
    config = HightouchSourceConfig(
        api_config=HightouchAPIConfig(api_key="test"),
        env="PROD",
        sources_to_platform_instance={
            "source_1": PlatformDetail(
                platform="snowflake",
                platform_instance="snowflake-prod",
                env="PROD",
                database="raw_data",
            )
        },
    )

    mock_source_instance = HightouchIngestionSource(config, pipeline_context)
    urn_builder = HightouchUrnBuilder(
        config=mock_source_instance.config,
        get_platform_for_source=mock_source_instance._get_platform_for_source,
        get_platform_for_destination=mock_source_instance._get_platform_for_destination,
    )

    source_connection = HightouchSourceConnection(
        id="source_1",
        name="Snowflake Source",
        slug="snowflake-source",
        type="snowflake",
        workspace_id="workspace_1",
        created_at=datetime(2023, 1, 1),
        updated_at=datetime(2023, 1, 2),
    )

    _ = urn_builder.make_upstream_table_urn("table1", source_connection)
    _ = urn_builder.make_upstream_table_urn("table2", source_connection)

    assert "source_source_1" in urn_builder._platform_detail_cache
    assert len(urn_builder._platform_detail_cache) == 1


@patch("datahub.ingestion.source.hightouch.hightouch.HightouchAPIClient")
def test_urn_builder_fallback_to_source_type(mock_api_client_class, pipeline_context):
    config = HightouchSourceConfig(
        api_config=HightouchAPIConfig(api_key="test"),
        env="PROD",
        sources_to_platform_instance={},
    )

    source = HightouchIngestionSource(config, pipeline_context)
    urn_builder = HightouchUrnBuilder(
        config=source.config,
        get_platform_for_source=source._get_platform_for_source,
        get_platform_for_destination=source._get_platform_for_destination,
    )

    source_connection = HightouchSourceConnection(
        id="source_unknown",
        name="Unknown Source",
        slug="unknown-source",
        type="bigquery",
        workspace_id="workspace_1",
        created_at=datetime(2023, 1, 1),
        updated_at=datetime(2023, 1, 2),
    )

    urn = urn_builder.make_upstream_table_urn("table1", source_connection)

    assert isinstance(urn, (str, DatasetUrn))
    urn_str = str(urn)
    assert "bigquery" in urn_str


def _urn_builder_for_schema_flag(pipeline_context, include_schema_in_urn):
    config = HightouchSourceConfig(
        api_config=HightouchAPIConfig(api_key="test"),
        env="PROD",
        sources_to_platform_instance={
            "source_1": PlatformDetail(
                platform="snowflake",
                platform_instance="snowflake-prod",
                env="PROD",
                database="raw_data",
                include_schema_in_urn=include_schema_in_urn,
            )
        },
    )
    source = HightouchIngestionSource(config, pipeline_context)
    urn_builder = HightouchUrnBuilder(
        config=source.config,
        get_platform_for_source=source._get_platform_for_source,
        get_platform_for_destination=source._get_platform_for_destination,
    )
    source_connection = HightouchSourceConnection(
        id="source_1",
        name="Snowflake Source",
        slug="snowflake-source",
        type="snowflake",
        workspace_id="workspace_1",
        created_at=datetime(2023, 1, 1),
        updated_at=datetime(2023, 1, 2),
    )
    return urn_builder, source_connection


def _model(table_name: str) -> HightouchModel:
    return HightouchModel(
        id="model_1",
        name="Display Name",
        slug="table-model",
        workspace_id="workspace_1",
        source_id="source_1",
        query_type="table",
        table={"name": table_name},
        created_at=datetime(2023, 1, 1),
        updated_at=datetime(2023, 1, 2),
    )


@patch("datahub.ingestion.source.hightouch.hightouch.HightouchAPIClient")
def test_qualified_table_name_does_not_double_prefix_qualified_names(
    mock_api_client_class, pipeline_context
):
    """With schema qualification requested but no schema configured, a bare name is
    prefixed with the configured database, an already db-prefixed name is left alone,
    and a fully three-part name (even for a different database) is not turned into an
    invalid four-part name."""
    urn_builder, source_connection = _urn_builder_for_schema_flag(
        pipeline_context, include_schema_in_urn=True
    )

    assert (
        urn_builder.qualified_table_name(_model("customers"), source_connection)
        == "raw_data.customers"
    )
    assert (
        urn_builder.qualified_table_name(
            _model("raw_data.customers"), source_connection
        )
        == "raw_data.customers"
    )
    # A fully qualified name for a different database must be preserved as-is.
    assert (
        urn_builder.qualified_table_name(
            _model("other_db.public.customers"), source_connection
        )
        == "other_db.public.customers"
    )


@patch("datahub.ingestion.source.hightouch.hightouch.HightouchAPIClient")
def test_normalize_parsed_upstream_urn_keeps_schema_when_flag_true(
    mock_api_client_class, pipeline_context
):
    urn_builder, source_connection = _urn_builder_for_schema_flag(
        pipeline_context, include_schema_in_urn=True
    )
    # The parser puts the platform instance in front of the name, lowercased.
    parsed = DatasetUrn(
        platform="snowflake",
        name="snowflake-prod.raw_data.public.customers",
        env="PROD",
    )

    result = urn_builder.normalize_parsed_upstream_urn(parsed, source_connection)

    # Flag is on: the schema stays, and the URN is rebuilt with the configured
    # instance, as table-model upstreams are.
    assert result == str(
        DatasetUrn.create_from_ids(
            platform_id="snowflake",
            table_name="raw_data.public.customers",
            env="PROD",
            platform_instance="snowflake-prod",
        )
    )


@patch("datahub.ingestion.source.hightouch.hightouch.HightouchAPIClient")
def test_normalize_parsed_upstream_urn_strips_schema_when_flag_false(
    mock_api_client_class, pipeline_context
):
    urn_builder, source_connection = _urn_builder_for_schema_flag(
        pipeline_context, include_schema_in_urn=False
    )
    parsed = DatasetUrn(
        platform="snowflake", name="raw_data.public.customers", env="PROD"
    )

    result = urn_builder.normalize_parsed_upstream_urn(parsed, source_connection)

    # Flag is off: schema segment is dropped and the bare table is re-qualified with
    # the configured database, matching the connector's own source URNs.
    assert "public" not in result
    assert "raw_data.customers" in result


@pytest.mark.parametrize(
    "name, database, schema, expected",
    [
        pytest.param("orders", "db", "sch", "db.sch.orders", id="bare-name"),
        pytest.param(
            "sch.orders", "db", "other", "db.sch.orders", id="schema-qualified"
        ),
        pytest.param(
            "db2.sch.orders", "db", "sch", "db2.sch.orders", id="fully-qualified"
        ),
        pytest.param("DB.orders", "db", "", "DB.orders", id="database-qualified"),
        pytest.param("orders", "", "", "orders", id="nothing-configured"),
    ],
)
def test_qualify_adds_only_missing_parts(
    name: str, database: str, schema: str, expected: str
) -> None:
    assert HightouchUrnBuilder._qualify(name, database, schema) == expected


def _warehouse_source(platform: str, configuration: dict) -> HightouchSourceConnection:
    return HightouchSourceConnection(
        id="source_1",
        name="Warehouse",
        slug="warehouse",
        type=platform,
        workspace_id="workspace_1",
        created_at=datetime(2023, 1, 1),
        updated_at=datetime(2023, 1, 2),
        configuration=configuration,
    )


@pytest.mark.parametrize(
    "platform, convert_urns_to_lowercase, expected_name",
    [
        pytest.param(
            "snowflake", None, "analytics.public.product_events", id="snowflake-default"
        ),
        pytest.param(
            "snowflake", False, "ANALYTICS.PUBLIC.PRODUCT_EVENTS", id="snowflake-off"
        ),
        pytest.param(
            "bigquery", None, "ANALYTICS.PUBLIC.PRODUCT_EVENTS", id="other-default"
        ),
        pytest.param(
            "bigquery", True, "analytics.public.product_events", id="other-on"
        ),
    ],
)
@patch("datahub.ingestion.source.hightouch.hightouch.HightouchAPIClient")
def test_table_model_upstream_uses_table_name_not_display_name(
    mock_api_client_class,
    pipeline_context,
    platform,
    convert_urns_to_lowercase,
    expected_name,
):
    config = HightouchSourceConfig(
        api_config=HightouchAPIConfig(api_key="test_api_key"),
        sources_to_platform_instance={
            "source_1": PlatformDetail(
                platform=platform, convert_urns_to_lowercase=convert_urns_to_lowercase
            )
        },
    )
    source_instance = HightouchIngestionSource(config, pipeline_context)
    model = HightouchModel.model_validate(
        {
            "id": 1,
            "name": "Product Events",
            "slug": "product-events",
            "workspaceId": 1,
            "sourceId": 1,
            "queryType": "table",
            "table": {"name": "PRODUCT_EVENTS"},
            "createdAt": "2023-01-01T00:00:00Z",
            "updatedAt": "2023-01-02T00:00:00Z",
        }
    )

    urn = source_instance._urn_builder.source_table_urn(
        model,
        _warehouse_source(platform, {"database": "ANALYTICS", "schema": "PUBLIC"}),
    )

    assert urn is not None
    assert DatasetUrn.from_string(urn).name == expected_name


@patch("datahub.ingestion.source.hightouch.hightouch.HightouchAPIClient")
def test_dbt_model_upstream_is_its_generated_table(
    mock_api_client_class, pipeline_context
):
    source_instance = HightouchIngestionSource(
        HightouchSourceConfig(api_config=HightouchAPIConfig(api_key="test_api_key")),
        pipeline_context,
    )
    model = HightouchModel.model_validate(
        {
            "id": 2,
            "name": "Orders (dbt)",
            "slug": "orders-dbt",
            "workspaceId": 1,
            "sourceId": 1,
            "queryType": "dbt",
            "dbt": {
                "dbtUniqueId": "model.shop.orders",
                "compiledSql": "select 1",
                "rawSql": "select 1",
                "database": "ANALYTICS",
                "schema": "MARTS",
                "name": "ORDERS",
                "modelId": 7,
            },
            "createdAt": "2023-01-01T00:00:00Z",
            "updatedAt": "2023-01-02T00:00:00Z",
        }
    )

    urn = source_instance._urn_builder.source_table_urn(
        model, _warehouse_source("snowflake", {"database": "OTHER_DB"})
    )

    assert urn is not None
    assert DatasetUrn.from_string(urn).name == "analytics.marts.orders"


@pytest.mark.parametrize(
    "parsed_name, expected_name",
    [
        pytest.param("orders", "raw_data.orders", id="bare-table"),
        pytest.param("otherdb.orders", "otherdb.orders", id="cross-database"),
        pytest.param("otherdb.sch.orders", "otherdb.orders", id="three-part"),
    ],
)
@patch("datahub.ingestion.source.hightouch.hightouch.HightouchAPIClient")
def test_normalize_parsed_upstream_urn_keeps_the_written_database(
    mock_api_client_class, pipeline_context, parsed_name, expected_name
):
    urn_builder, source_connection = _urn_builder_for_schema_flag(
        pipeline_context, include_schema_in_urn=False
    )
    parsed = DatasetUrn(
        platform="snowflake", name=f"snowflake-prod.{parsed_name}", env="PROD"
    )

    result = urn_builder.normalize_parsed_upstream_urn(parsed, source_connection)

    # A table in another database is not filed under the source's own database.
    assert DatasetUrn.from_string(result).name == f"snowflake-prod.{expected_name}"


@patch("datahub.ingestion.source.hightouch.hightouch.HightouchAPIClient")
def test_snowflake_destination_urn_is_lowercased(
    mock_api_client_class, pipeline_context
):
    source_instance = HightouchIngestionSource(
        HightouchSourceConfig(
            api_config=HightouchAPIConfig(api_key="test_api_key"),
            destination_to_platform_instance={
                "dest_1": PlatformDetail(platform="snowflake", database="ANALYTICS")
            },
        ),
        pipeline_context,
    )
    destination = HightouchDestination(
        id="dest_1",
        name="Warehouse",
        slug="warehouse",
        type="snowflake",
        workspace_id="workspace_1",
        created_at=datetime(2023, 1, 1),
        updated_at=datetime(2023, 1, 2),
    )

    urn = source_instance._urn_builder.make_destination_urn("PUBLIC.USERS", destination)

    assert DatasetUrn.from_string(str(urn)).name == "analytics.public.users"
