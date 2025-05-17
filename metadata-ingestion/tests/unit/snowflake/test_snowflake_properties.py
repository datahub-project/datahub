from datetime import datetime
from unittest.mock import MagicMock

from datahub.ingestion.source.snowflake.snowflake_schema import (
    SnowflakeTable,
    SnowflakeView,
)
from datahub.ingestion.source.snowflake.snowflake_schema_gen import (
    SnowflakeSchemaGenerator,
)
from datahub.ingestion.source.snowflake.snowflake_utils import SnowsightUrlBuilder


def test_dynamic_table_properties_external_url():
    """Test that dynamic tables get the correct external URL in dataset properties."""

    # Create mock objects
    config = MagicMock()
    report = MagicMock()
    connection = MagicMock()
    filters = MagicMock()
    identifiers = MagicMock()
    domain_registry = None
    profiler = None
    aggregator = None

    # Create the snowsight URL builder
    snowsight_url_builder = SnowsightUrlBuilder(
        account_locator="abc123",
        region="us-west-2",
    )

    # Create a schema generator
    schema_generator = SnowflakeSchemaGenerator(
        config=config,
        report=report,
        connection=connection,
        filters=filters,
        identifiers=identifiers,
        domain_registry=domain_registry,
        profiler=profiler,
        aggregator=aggregator,
        snowsight_url_builder=snowsight_url_builder,
    )

    # Create a regular table
    regular_table = SnowflakeTable(
        name="REGULAR_TABLE",
        created=datetime.now(),
        last_altered=datetime.now(),
        comment="Regular table",
        is_dynamic=False,
        size_in_bytes=100,
        rows_count=10,
    )

    # Create a dynamic table
    dynamic_table = SnowflakeTable(
        name="DYNAMIC_TABLE",
        created=datetime.now(),
        last_altered=datetime.now(),
        comment="Dynamic table",
        is_dynamic=True,
        size_in_bytes=50,
        rows_count=5,
    )

    # Create a view
    view = SnowflakeView(
        name="TEST_VIEW",
        created=datetime.now(),
        last_altered=datetime.now(),
        materialized=False,
        comment="Test view",
        view_definition="SELECT * FROM test_table",
    )

    # Get the dataset properties for each entity
    regular_table_props = schema_generator.get_dataset_properties(
        regular_table, "TEST_SCHEMA", "TEST_DB"
    )
    dynamic_table_props = schema_generator.get_dataset_properties(
        dynamic_table, "TEST_SCHEMA", "TEST_DB"
    )
    view_props = schema_generator.get_dataset_properties(view, "TEST_SCHEMA", "TEST_DB")

    # Verify the properties have the correct URLs
    assert (
        regular_table_props.externalUrl
        == "https://app.snowflake.com/us-west-2/abc123/#/data/databases/TEST_DB/schemas/TEST_SCHEMA/table/REGULAR_TABLE/"
    )
    assert (
        dynamic_table_props.externalUrl
        == "https://app.snowflake.com/us-west-2/abc123/#/data/databases/TEST_DB/schemas/TEST_SCHEMA/dynamic-table/DYNAMIC_TABLE/"
    )
    assert (
        view_props.externalUrl
        == "https://app.snowflake.com/us-west-2/abc123/#/data/databases/TEST_DB/schemas/TEST_SCHEMA/view/TEST_VIEW/"
    )

    # Verify the dynamic table has the IS_DYNAMIC property set
    assert "IS_DYNAMIC" in dynamic_table_props.customProperties
    assert dynamic_table_props.customProperties["IS_DYNAMIC"] == "true"
    assert "IS_DYNAMIC" not in regular_table_props.customProperties
