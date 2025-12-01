import pytest
from pydantic import ValidationError

from datahub.ingestion.source.fivetran.config import (
    MAX_COLUMN_LINEAGE_PER_CONNECTOR_DEFAULT,
    MAX_JOBS_PER_CONNECTOR_DEFAULT,
    MAX_TABLE_LINEAGE_PER_CONNECTOR_DEFAULT,
    FivetranLogConfig,
    FivetranSourceConfig,
    SnowflakeDestinationConfig,
)


class TestFivetranConfig:
    """Test cases for Fivetran configuration with configurable limits."""

    def test_fivetran_log_config_default_limits(self):
        """Test that FivetranLogConfig uses default limits when not specified."""
        config = FivetranLogConfig(
            destination_platform="snowflake",
            snowflake_destination_config=SnowflakeDestinationConfig(
                account_id="test_account",
                warehouse="test_warehouse",
                username="test_user",
                password="test_password",
                database="test_database",
                log_schema="test_schema",
                role="test_role",
            ),
        )

        assert config.max_jobs_per_connector == MAX_JOBS_PER_CONNECTOR_DEFAULT
        assert (
            config.max_table_lineage_per_connector
            == MAX_TABLE_LINEAGE_PER_CONNECTOR_DEFAULT
        )
        assert (
            config.max_column_lineage_per_connector
            == MAX_COLUMN_LINEAGE_PER_CONNECTOR_DEFAULT
        )

    def test_fivetran_log_config_custom_limits(self):
        """Test that FivetranLogConfig accepts custom limits."""
        config = FivetranLogConfig(
            destination_platform="snowflake",
            snowflake_destination_config=SnowflakeDestinationConfig(
                account_id="test_account",
                warehouse="test_warehouse",
                username="test_user",
                password="test_password",
                database="test_database",
                log_schema="test_schema",
                role="test_role",
            ),
            max_jobs_per_connector=1000,
            max_table_lineage_per_connector=200,
            max_column_lineage_per_connector=2000,
        )

        assert config.max_jobs_per_connector == 1000
        assert config.max_table_lineage_per_connector == 200
        assert config.max_column_lineage_per_connector == 2000

    def test_fivetran_log_config_invalid_limits(self):
        """Test that FivetranLogConfig rejects invalid (non-positive) limits."""
        with pytest.raises(ValidationError) as excinfo:
            FivetranLogConfig(
                destination_platform="snowflake",
                snowflake_destination_config=SnowflakeDestinationConfig(
                    account_id="test_account",
                    warehouse="test_warehouse",
                    username="test_user",
                    password="test_password",
                    database="test_database",
                    log_schema="test_schema",
                    role="test_role",
                ),
                max_jobs_per_connector=0,
            )
        assert "greater than 0" in str(excinfo.value)

        with pytest.raises(ValidationError) as excinfo:
            FivetranLogConfig(
                destination_platform="snowflake",
                snowflake_destination_config=SnowflakeDestinationConfig(
                    account_id="test_account",
                    warehouse="test_warehouse",
                    username="test_user",
                    password="test_password",
                    database="test_database",
                    log_schema="test_schema",
                    role="test_role",
                ),
                max_table_lineage_per_connector=-1,
            )
        assert "greater than 0" in str(excinfo.value)

        with pytest.raises(ValidationError) as excinfo:
            FivetranLogConfig(
                destination_platform="snowflake",
                snowflake_destination_config=SnowflakeDestinationConfig(
                    account_id="test_account",
                    warehouse="test_warehouse",
                    username="test_user",
                    password="test_password",
                    database="test_database",
                    log_schema="test_schema",
                    role="test_role",
                ),
                max_column_lineage_per_connector=-100,
            )
        assert "greater than 0" in str(excinfo.value)

    def test_fivetran_source_config_with_log_limits(self):
        """Test that FivetranSourceConfig properly contains FivetranLogConfig with limits."""
        config = FivetranSourceConfig(
            fivetran_log_config=FivetranLogConfig(
                destination_platform="snowflake",
                snowflake_destination_config=SnowflakeDestinationConfig(
                    account_id="test_account",
                    warehouse="test_warehouse",
                    username="test_user",
                    password="test_password",
                    database="test_database",
                    log_schema="test_schema",
                    role="test_role",
                ),
                max_jobs_per_connector=750,
                max_table_lineage_per_connector=150,
                max_column_lineage_per_connector=1500,
            )
        )

        assert config.fivetran_log_config.max_jobs_per_connector == 750
        assert config.fivetran_log_config.max_table_lineage_per_connector == 150
        assert config.fivetran_log_config.max_column_lineage_per_connector == 1500
