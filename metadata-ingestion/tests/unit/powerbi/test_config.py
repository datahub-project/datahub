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
                PowerBiDashboardSourceConfig.model_validate(config_dict)
        else:
            config = PowerBiDashboardSourceConfig.model_validate(config_dict)
            assert config == expected_config_or_exception

    def test_certificate_path_auth_valid(self):
        config = PowerBiDashboardSourceConfig.model_validate(
            {
                "tenant_id": "fake",
                "client_id": "foo",
                "certificate_path": "/path/to/cert.pem",
            }
        )
        assert config.client_secret is None
        assert config.certificate_path == "/path/to/cert.pem"

    def test_certificate_data_auth_valid(self):
        config = PowerBiDashboardSourceConfig.model_validate(
            {
                "tenant_id": "fake",
                "client_id": "foo",
                # Escaped newlines (as stored in single-line secrets) must be
                # normalized to real newlines at parse time.
                "certificate_data": r"-----BEGIN PRIVATE KEY-----\nabc\n-----END PRIVATE KEY-----",
                "certificate_password": "passphrase",
            }
        )
        assert config.certificate_data is not None
        assert (
            config.certificate_data.get_secret_value()
            == "-----BEGIN PRIVATE KEY-----\nabc\n-----END PRIVATE KEY-----"
        )

    def test_no_auth_method_invalid(self):
        with pytest.raises(ValueError, match="No authentication method configured"):
            PowerBiDashboardSourceConfig.model_validate(
                {"tenant_id": "fake", "client_id": "foo"}
            )

    def test_secret_and_certificate_invalid(self):
        with pytest.raises(ValueError, match="only one authentication method"):
            PowerBiDashboardSourceConfig.model_validate(
                {
                    **self.base_config,
                    "certificate_path": "/path/to/cert.pem",
                }
            )

    def test_certificate_path_and_data_invalid(self):
        with pytest.raises(
            ValueError, match="only one of `certificate_path` or `certificate_data`"
        ):
            PowerBiDashboardSourceConfig.model_validate(
                {
                    "tenant_id": "fake",
                    "client_id": "foo",
                    "certificate_path": "/path/to/cert.pem",
                    "certificate_data": "-----BEGIN PRIVATE KEY-----\n...",
                }
            )

    def test_empty_client_secret_treated_as_unset(self):
        # An unset secret variable resolves to "" — must give the actionable
        # config error, not an opaque MSAL failure at runtime.
        with pytest.raises(ValueError, match="No authentication method configured"):
            PowerBiDashboardSourceConfig.model_validate(
                {"tenant_id": "fake", "client_id": "foo", "client_secret": ""}
            )

    def test_empty_client_secret_with_certificate_valid(self):
        config = PowerBiDashboardSourceConfig.model_validate(
            {
                "tenant_id": "fake",
                "client_id": "foo",
                "client_secret": "",
                "certificate_path": "/path/to/cert.pem",
            }
        )
        assert config.client_secret is None

    def test_certificate_password_without_certificate_invalid(self):
        with pytest.raises(
            ValueError, match="`certificate_password` is set but no certificate"
        ):
            PowerBiDashboardSourceConfig.model_validate(
                {
                    **self.base_config,
                    "certificate_password": "passphrase",
                }
            )

    def test_map_data_platform_postgresql_conversion(self):
        """Test that PostgreSql is converted to PostgreSQL."""
        config_dict = {
            **self.base_config,
            "dataset_type_mapping": {"PostgreSql": "postgres"},
        }
        config = PowerBiDashboardSourceConfig.model_validate(config_dict)
        assert "PostgreSQL" in config.dataset_type_mapping
        assert "PostgreSql" not in config.dataset_type_mapping

    def test_map_data_platform_no_postgresql(self):
        """Test map_data_platform when PostgreSql is not in value."""
        config_dict = {
            **self.base_config,
            "dataset_type_mapping": {"Snowflake": "snowflake"},
        }
        config = PowerBiDashboardSourceConfig.model_validate(config_dict)
        assert config.dataset_type_mapping == {"Snowflake": "snowflake"}

    def test_workspace_id_backward_compatibility(self):
        """Test workspace_id backward compatibility."""
        config_dict = {
            **self.base_config,
            "workspace_id": "test-id",
        }
        config = PowerBiDashboardSourceConfig.model_validate(config_dict)
        assert config.workspace_id_pattern.allow == ["^test-id$"]

    def test_workspace_id_ignored_when_pattern_set(self):
        """Test workspace_id is ignored when pattern is set."""
        config_dict = {
            **self.base_config,
            "workspace_id": "old-id",
            "workspace_id_pattern": {"allow": ["^new-id$"]},
        }
        config = PowerBiDashboardSourceConfig.model_validate(config_dict)
        assert config.workspace_id is None

    def test_raise_error_for_dataset_type_mapping(self):
        """Test error when both dataset_type_mapping and server_to_platform_instance are set."""
        config_dict = {
            **self.base_config,
            "dataset_type_mapping": {"PostgreSQL": "postgres"},
            "server_to_platform_instance": {"localhost": {"platform_instance": "test"}},
        }
        with pytest.raises(ValueError, match="dataset_type_mapping is deprecated"):
            PowerBiDashboardSourceConfig.model_validate(config_dict)

    def test_validate_extract_dataset_schema_false(self):
        """Test validate_extract_dataset_schema when extract_dataset_schema is False."""
        config_dict = {
            **self.base_config,
            "extract_dataset_schema": False,
            "extract_column_level_lineage": False,
        }
        config = PowerBiDashboardSourceConfig.model_validate(config_dict)
        assert config.extract_dataset_schema is False

    def test_validate_extract_column_level_lineage_false(self):
        """Test validate_extract_column_level_lineage early return when False."""
        config_dict = {
            **self.base_config,
            "extract_column_level_lineage": False,
        }
        config = PowerBiDashboardSourceConfig.model_validate(config_dict)
        assert config.extract_column_level_lineage is False

    def test_validate_extract_column_level_lineage_missing_flags(self):
        """Test validate_extract_column_level_lineage when required flags are missing."""
        config_dict = {
            **self.base_config,
            "extract_column_level_lineage": True,
            "native_query_parsing": False,
        }
        with pytest.raises(ValueError, match="Enable all these flags"):
            PowerBiDashboardSourceConfig.model_validate(config_dict)

    def test_validate_athena_table_platform_override_empty(self):
        """Test validate_athena_table_platform_override early return when empty."""
        config_dict = {
            **self.base_config,
            "athena_table_platform_override": [],
        }
        config = PowerBiDashboardSourceConfig.model_validate(config_dict)
        assert config.athena_table_platform_override == []

    def test_validate_athena_table_platform_override_invalid(self):
        """Test validate_athena_table_platform_override with invalid platform."""
        config_dict = {
            **self.base_config,
            "athena_table_platform_override": [
                {"database": "db", "table": "tbl", "platform": "invalid"}
            ],
        }
        with pytest.raises(ValueError, match="is not a recognized DataHub platform"):
            PowerBiDashboardSourceConfig.model_validate(config_dict)

    def test_get_from_dataset_type_mapping_exact_match(self):
        """Test get_from_dataset_type_mapping with exact match."""
        config = PowerBiDashboardSourceConfig(
            tenant_id="fake",
            client_id="foo",
            client_secret="bar",
            dataset_type_mapping={"PostgreSQL": "postgres"},
        )
        assert config.get_from_dataset_type_mapping("PostgreSQL") == "postgres"

    def test_get_from_dataset_type_mapping_normalized_match(self):
        """Test get_from_dataset_type_mapping with normalized match."""
        config = PowerBiDashboardSourceConfig(
            tenant_id="fake",
            client_id="foo",
            client_secret="bar",
            dataset_type_mapping={"AmazonRedshift": "redshift"},
        )
        assert config.get_from_dataset_type_mapping("Amazon Redshift") == "redshift"

    def test_get_from_dataset_type_mapping_normalized_not_found(self):
        """Test get_from_dataset_type_mapping when normalized name not in mapping."""
        config = PowerBiDashboardSourceConfig(
            tenant_id="fake",
            client_id="foo",
            client_secret="bar",
            dataset_type_mapping={"PostgreSQL": "postgres"},
        )
        assert config.get_from_dataset_type_mapping("Amazon Redshift") is None

    def test_get_from_dataset_type_mapping_no_spaces(self):
        """Test get_from_dataset_type_mapping when platform has no spaces."""
        config = PowerBiDashboardSourceConfig(
            tenant_id="fake",
            client_id="foo",
            client_secret="bar",
            dataset_type_mapping={"PostgreSQL": "postgres"},
        )
        assert config.get_from_dataset_type_mapping("Unknown") is None

    def test_is_platform_in_dataset_type_mapping(self):
        """Test is_platform_in_dataset_type_mapping."""
        config = PowerBiDashboardSourceConfig(
            tenant_id="fake",
            client_id="foo",
            client_secret="bar",
            dataset_type_mapping={"PostgreSQL": "postgres"},
        )
        assert config.is_platform_in_dataset_type_mapping("PostgreSQL") is True
        assert config.is_platform_in_dataset_type_mapping("Unknown") is False
