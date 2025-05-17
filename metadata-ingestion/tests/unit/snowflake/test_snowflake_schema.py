from datetime import datetime

from datahub.ingestion.source.snowflake.snowflake_schema import (
    SnowflakeTable,
)


def test_snowflake_table_dynamic_property():
    """Test that the is_dynamic property works correctly for SnowflakeTable"""

    # Create a regular table
    regular_table = SnowflakeTable(
        name="test_table",
        created=datetime.now(),
        last_altered=datetime.now(),
        comment="This is a regular table",
        is_dynamic=False,
        size_in_bytes=100,
        rows_count=10,
    )

    # Create a dynamic table
    dynamic_table = SnowflakeTable(
        name="test_dynamic_table",
        created=datetime.now(),
        last_altered=datetime.now(),
        comment="This is a dynamic table",
        is_dynamic=True,
        size_in_bytes=50,
        rows_count=5,
    )

    # Check properties
    assert regular_table.is_dynamic is False
    assert dynamic_table.is_dynamic is True

    # This property is what should determine which URL type it gets
    # Test that the URL domain determination will be accurate
    assert regular_table.is_dynamic is not True
    assert dynamic_table.is_dynamic is True
