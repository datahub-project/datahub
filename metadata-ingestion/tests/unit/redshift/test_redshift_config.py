import pytest
from pydantic import ValidationError

from datahub.ingestion.source.redshift.config import RedshiftConfig


def test_incremental_lineage_default_to_false():
    config = RedshiftConfig(host_port="localhost:5439", database="test")
    assert config.incremental_lineage is False


def test_alternative_system_tables_schema_default_none():
    """Test that alternative_system_tables_schema defaults to None."""
    config = RedshiftConfig(host_port="localhost:5439", database="test")
    assert config.alternative_system_tables_schema is None


@pytest.mark.parametrize(
    "schema_value,should_pass",
    [
        # Valid cases
        ("db.schema", True),
        ("my_db.my_schema", True),
        ("db123.schema456", True),
        ("database_name.schema_name", True),
        ('"quoted_db"."quoted_schema"', True),
        ("db$name.schema$name", True),
        ("db-name.schema-name", True),
        (None, True),
        # Invalid cases
        ("db", False),  # No schema part
        ("db.schema.extra", False),  # Too many dots
        ("db.", False),  # Empty schema
        (".schema", False),  # Empty database
        ("", False),  # Empty string
        ("db. schema", False),  # Space after dot
        ("db .schema", False),  # Space before dot
        (" db.schema", False),  # Leading space
        ("db.schema ", False),  # Trailing space
        ("db..schema", False),  # Double dot
        ("db.sch ema", False),  # Space in schema
        ("d b.schema", False),  # Space in database
    ],
)
def test_alternative_system_tables_schema_validation(schema_value, should_pass):
    """Test validation of alternative_system_tables_schema field."""
    if should_pass:
        config = RedshiftConfig(
            host_port="localhost:5439",
            database="test",
            alternative_system_tables_schema=schema_value,
        )
        assert config.alternative_system_tables_schema == schema_value
    else:
        with pytest.raises(ValidationError) as exc_info:
            RedshiftConfig(
                host_port="localhost:5439",
                database="test",
                alternative_system_tables_schema=schema_value,
            )

        # Check that the error message mentions the format requirement
        error_message = str(exc_info.value)
        assert (
            "alternative_system_tables_schema must be in the format 'database_name.schema_name'"
            in error_message
        )
