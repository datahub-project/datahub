from typing import List, Tuple
from unittest.mock import Mock

from datahub.ingestion.source.dremio.dremio_api import DremioFilter
from datahub.ingestion.source.dremio.dremio_config import DremioSourceConfig
from datahub.ingestion.source.dremio.dremio_reporting import DremioSourceReport


class TestDremioSystemTableFiltering:
    def test_system_tables_included_by_default(self):
        config = DremioSourceConfig()
        report = Mock(spec=DremioSourceReport)
        filter_obj = DremioFilter(config, report)

        system_tables = [
            ("columns", ["information_schema"], "table"),
            ("tables", ["sys"], "table"),
        ]
        for table_name, schema_path, table_type in system_tables:
            assert filter_obj.is_dataset_allowed(table_name, schema_path, table_type)

    def test_system_schemas_included_by_default(self):
        config = DremioSourceConfig()
        report = Mock(spec=DremioSourceReport)
        filter_obj = DremioFilter(config, report)

        system_schemas: List[Tuple[List[str], str]] = [
            ([], "information_schema"),
            ([], "sys"),
        ]
        for schema_path, container_name in system_schemas:
            assert filter_obj.is_schema_allowed(schema_path, container_name)

    def test_schemas_with_system_substrings_not_filtered(self):
        """Schemas/tables that contain 'sys' or 'information_schema' as a substring (not exact match) must not be excluded."""
        config = DremioSourceConfig()
        report = Mock(spec=DremioSourceReport)
        filter_obj = DremioFilter(config, report)

        non_system_tables = [
            ("users", ["my_sys_schema"], "table"),
            ("data", ["user_information_schema"], "table"),
            ("orders", ["system_logs"], "table"),
            ("metrics", ["sys_monitoring"], "table"),
        ]
        for table_name, schema_path, table_type in non_system_tables:
            assert filter_obj.is_dataset_allowed(table_name, schema_path, table_type)

        non_system_schemas: List[Tuple[List[str], str]] = [
            ([], "my_sys_schema"),
            ([], "user_information_schema"),
            ([], "system_logs"),
            ([], "sys_monitoring"),
        ]
        for schema_path, container_name in non_system_schemas:
            assert filter_obj.is_schema_allowed(schema_path, container_name)
