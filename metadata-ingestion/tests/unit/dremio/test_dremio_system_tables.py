from unittest.mock import Mock

from datahub.ingestion.source.dremio.dremio_api import (
    DREMIO_SYSTEM_TABLES_PATTERN,
    DremioFilter,
)
from datahub.ingestion.source.dremio.dremio_config import DremioSourceConfig
from datahub.ingestion.source.dremio.dremio_reporting import DremioSourceReport


class TestDremioSystemTableFiltering:
    """Test system table filtering functionality in Dremio connector."""

    def test_system_table_patterns(self):
        """Test that system table patterns are defined correctly."""
        expected_patterns = [
            r"^information_schema$",  # Exact INFORMATION_SCHEMA schema match
            r"^sys$",  # Exact SYS schema match
            r"^information_schema\..*",  # Tables in INFORMATION_SCHEMA schema
            r"^sys\..*",  # Tables in SYS schema
        ]
        assert expected_patterns == DREMIO_SYSTEM_TABLES_PATTERN

    def test_system_tables_excluded_by_default(self):
        """Test that system tables are excluded by default."""
        config = DremioSourceConfig()
        report = Mock(spec=DremioSourceReport)
        filter_obj = DremioFilter(config, report)

        # Test various system table patterns - only exact matches should be filtered
        system_tables = [
            ("columns", ["information_schema"], "table"),  # information_schema.columns
            ("tables", ["sys"], "table"),  # sys.tables
            ("views", ["information_schema"], "table"),  # information_schema.views
            ("users", ["sys"], "table"),  # sys.users
        ]

        for table_name, schema_path, table_type in system_tables:
            assert not filter_obj.is_dataset_allowed(
                table_name, schema_path, table_type
            ), f"System table {schema_path}.{table_name} should be excluded by default"

    def test_system_schemas_excluded_by_default(self):
        """Test that system schemas are excluded by default."""
        config = DremioSourceConfig()
        report = Mock(spec=DremioSourceReport)
        filter_obj = DremioFilter(config, report)

        # Test exact system schema matches only
        system_schemas = [
            ([], "information_schema"),  # Direct information_schema schema
            ([], "sys"),  # Direct sys schema
        ]

        for schema_path, container_name in system_schemas:
            assert not filter_obj.is_schema_allowed(schema_path, container_name), (
                f"System schema {schema_path}.{container_name} should be excluded by default"
            )

    def test_regular_tables_included_by_default(self):
        """Test that regular tables are included by default."""
        config = DremioSourceConfig()
        report = Mock(spec=DremioSourceReport)
        filter_obj = DremioFilter(config, report)

        # Test regular tables
        regular_tables = [
            ("users", ["source1", "public"], "table"),
            ("orders", ["source1", "sales"], "table"),
            ("customer_info", ["source1", "analytics"], "view"),
        ]

        for table_name, schema_path, table_type in regular_tables:
            assert filter_obj.is_dataset_allowed(table_name, schema_path, table_type), (
                f"Regular table {schema_path}.{table_name} should be included by default"
            )

    def test_regular_schemas_included_by_default(self):
        """Test that regular schemas are included by default."""
        config = DremioSourceConfig()
        report = Mock(spec=DremioSourceReport)
        filter_obj = DremioFilter(config, report)

        # Test regular schemas
        regular_schemas = [
            (["source1"], "public"),
            (["source1"], "sales"),
            (["source1", "analytics"], ""),
        ]

        for schema_path, container_name in regular_schemas:
            assert filter_obj.is_schema_allowed(schema_path, container_name), (
                f"Regular schema {schema_path}.{container_name} should be included by default"
            )

    def test_system_tables_included_when_enabled(self):
        """Test that system tables are included when include_system_tables=True."""
        config = DremioSourceConfig(include_system_tables=True)
        report = Mock(spec=DremioSourceReport)
        filter_obj = DremioFilter(config, report)

        # Test system tables are now included
        system_tables = [
            ("INFORMATION_SCHEMA", ["source1"], "table"),
            ("COLUMNS", ["source1", "INFORMATION_SCHEMA"], "table"),
            ("TABLES", ["source1", "SYS"], "table"),
        ]

        for table_name, schema_path, table_type in system_tables:
            assert filter_obj.is_dataset_allowed(table_name, schema_path, table_type), (
                f"System table {schema_path}.{table_name} should be included when include_system_tables=True"
            )

    def test_system_schemas_included_when_enabled(self):
        """Test that system schemas are included when include_system_tables=True."""
        config = DremioSourceConfig(include_system_tables=True)
        report = Mock(spec=DremioSourceReport)
        filter_obj = DremioFilter(config, report)

        # Test system schemas are now included
        system_schemas = [
            (["source1"], "INFORMATION_SCHEMA"),
            (["source1"], "SYS"),
        ]

        for schema_path, container_name in system_schemas:
            assert filter_obj.is_schema_allowed(schema_path, container_name), (
                f"System schema {schema_path}.{container_name} should be included when include_system_tables=True"
            )

    def test_config_default_value(self):
        """Test that include_system_tables defaults to False."""
        config = DremioSourceConfig()
        assert config.include_system_tables is False

    def test_config_can_be_set_to_true(self):
        """Test that include_system_tables can be set to True."""
        config = DremioSourceConfig(include_system_tables=True)
        assert config.include_system_tables is True

    def test_schemas_with_system_substrings_included(self):
        """Test that schemas containing 'sys' or 'information_schema' as substrings are NOT filtered."""
        config = DremioSourceConfig()
        report = Mock(spec=DremioSourceReport)
        filter_obj = DremioFilter(config, report)

        # These should NOT be filtered because they're not exact matches
        non_system_tables = [
            ("users", ["my_sys_schema"], "table"),  # my_sys_schema.users
            (
                "data",
                ["user_information_schema"],
                "table",
            ),  # user_information_schema.data
            ("orders", ["system_logs"], "table"),  # system_logs.orders
            ("metrics", ["sys_monitoring"], "table"),  # sys_monitoring.metrics
        ]

        for table_name, schema_path, table_type in non_system_tables:
            assert filter_obj.is_dataset_allowed(table_name, schema_path, table_type), (
                f"Table {schema_path}.{table_name} should NOT be filtered (contains system substring but not exact match)"
            )

        # These schema names should also NOT be filtered
        non_system_schemas = [
            ([], "my_sys_schema"),
            ([], "user_information_schema"),
            ([], "system_logs"),
            ([], "sys_monitoring"),
        ]

        for schema_path, container_name in non_system_schemas:
            assert filter_obj.is_schema_allowed(schema_path, container_name), (
                f"Schema {schema_path}.{container_name} should NOT be filtered (contains system substring but not exact match)"
            )
