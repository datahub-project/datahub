import pytest
from pydantic import ValidationError

from datahub.ingestion.source.powerbi.config import PowerBiDashboardSourceConfig


class TestPowerBiConfig:
    """Test PowerBI configuration validation."""

    base_config = {"tenant_id": "fake", "client_id": "foo", "client_secret": "bar"}

    @pytest.mark.parametrize(
        "config_dict,expected_config_or_exception",
        [
            # Valid configurations
            pytest.param(
                {
                    **base_config,
                    "dsn_to_database_schema": {
                        "dsn1": "database1",
                        "dsn2": "database2",
                    },
                },
                PowerBiDashboardSourceConfig(
                    tenant_id="fake",
                    client_id="foo",
                    client_secret="bar",
                    dsn_to_database_schema={
                        "dsn1": "database1",
                        "dsn2": "database2",
                    },
                ),
                id="dsn_database_only_mapping_valid",
            ),
            pytest.param(
                {
                    **base_config,
                    "dsn_to_database_schema": {
                        "dsn1": "database1.schema1",
                        "dsn2": "database2.schema2",
                    },
                },
                PowerBiDashboardSourceConfig(
                    tenant_id="fake",
                    client_id="foo",
                    client_secret="bar",
                    dsn_to_database_schema={
                        "dsn1": "database1.schema1",
                        "dsn2": "database2.schema2",
                    },
                ),
                id="dsn_database_schema_mapping_valid",
            ),
            pytest.param(
                {
                    **base_config,
                    "dsn_to_database_schema": {
                        "dsn1": "database1",
                        "dsn2": "database2.schema2",
                    },
                },
                PowerBiDashboardSourceConfig(
                    tenant_id="fake",
                    client_id="foo",
                    client_secret="bar",
                    dsn_to_database_schema={
                        "dsn1": "database1",
                        "dsn2": "database2.schema2",
                    },
                ),
                id="dsn_mixed_mapping_valid",
            ),
            pytest.param(
                {**base_config, "dsn_to_database_schema": {}},
                PowerBiDashboardSourceConfig(
                    tenant_id="fake",
                    client_id="foo",
                    client_secret="bar",
                    dsn_to_database_schema={},
                ),
                id="dsn_empty_dict_valid",
            ),
            pytest.param(
                base_config,
                PowerBiDashboardSourceConfig(
                    tenant_id="fake",
                    client_id="foo",
                    client_secret="bar",
                ),
                id="dsn_omitted_field_defaults_to_empty_valid",
            ),
            # Invalid configurations
            pytest.param(
                {
                    **base_config,
                    "dsn_to_database_schema": {"dsn1": "database.schema.extra"},
                },
                ValueError,
                id="dsn_too_many_dots_invalid",
            ),
            pytest.param(
                {**base_config, "dsn_to_database_schema": ["not", "a", "dict"]},
                ValidationError,
                id="dsn_non_dict_type_invalid",
            ),
        ],
    )
    def test_dsn_to_database_schema_config(
        self, config_dict, expected_config_or_exception
    ):
        """Test dsn_to_database_schema configuration validation."""
        if isinstance(expected_config_or_exception, type) and issubclass(
            expected_config_or_exception, Exception
        ):
            with pytest.raises(expected_config_or_exception):
                PowerBiDashboardSourceConfig.parse_obj(config_dict)
        else:
            config = PowerBiDashboardSourceConfig.parse_obj(config_dict)
            assert config == expected_config_or_exception
