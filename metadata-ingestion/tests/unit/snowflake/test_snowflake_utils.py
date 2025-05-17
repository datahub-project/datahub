from datahub.ingestion.source.snowflake.constants import SnowflakeObjectDomain
from datahub.ingestion.source.snowflake.snowflake_utils import SnowsightUrlBuilder


def test_snowsight_url_builder_dynamic_tables():
    """Test that SnowsightUrlBuilder generates the correct URLs for different table types"""

    url_builder = SnowsightUrlBuilder("abc12345", "us-west-2")

    # Test regular table URL
    table_url = url_builder.get_external_url_for_table(
        "test_table", "test_schema", "test_db", SnowflakeObjectDomain.TABLE
    )
    assert (
        table_url
        == "https://app.snowflake.com/us-west-2/abc12345/#/data/databases/test_db/schemas/test_schema/table/test_table/"
    )

    # Test dynamic table URL
    dynamic_table_url = url_builder.get_external_url_for_table(
        "dynamic_test_table",
        "test_schema",
        "test_db",
        SnowflakeObjectDomain.DYNAMIC_TABLE,
    )
    assert (
        dynamic_table_url
        == "https://app.snowflake.com/us-west-2/abc12345/#/data/databases/test_db/schemas/test_schema/dynamic-table/dynamic_test_table/"
    )

    # Test view URL
    view_url = url_builder.get_external_url_for_table(
        "test_view", "test_schema", "test_db", SnowflakeObjectDomain.VIEW
    )
    assert (
        view_url
        == "https://app.snowflake.com/us-west-2/abc12345/#/data/databases/test_db/schemas/test_schema/view/test_view/"
    )

    # Verify the explicit comparison to make sure our code is handling the condition correctly
    assert SnowflakeObjectDomain.DYNAMIC_TABLE == "dynamic table"
    assert SnowflakeObjectDomain.TABLE == "table"
    assert SnowflakeObjectDomain.VIEW == "view"
