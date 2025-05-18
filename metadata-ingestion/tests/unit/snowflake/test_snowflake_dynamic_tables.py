import unittest
from datetime import datetime
from unittest import mock
from unittest.mock import MagicMock

from datahub.ingestion.source.snowflake.constants import SnowflakeObjectDomain
from datahub.ingestion.source.snowflake.snowflake_lineage_v2 import (
    SnowflakeLineageExtractor,
    UpstreamTableNode,
)
from datahub.ingestion.source.snowflake.snowflake_schema import (
    SnowflakeDynamicTable,
    SnowflakeTable,
)
from datahub.ingestion.source.snowflake.snowflake_schema_gen import (
    SnowflakeSchemaGenerator,
)
from datahub.ingestion.source.snowflake.snowflake_utils import SnowflakeFilter


class TestSnowflakeDynamicTables(unittest.TestCase):
    def test_snowflake_dynamic_table_subtype(self):
        """Test that SnowflakeDynamicTable returns the correct subtype"""
        dynamic_table = SnowflakeDynamicTable(
            name="TEST_DYNAMIC_TABLE",
            type="DYNAMIC_TABLE",
            created=datetime.now(),
            comment="Dynamic table comment",
            last_altered=datetime.now(),
            size_in_bytes=1000,
            rows_count=100,
            is_dynamic=True,
        )

        regular_table = SnowflakeTable(
            name="TEST_TABLE",
            type="BASE TABLE",
            created=datetime.now(),
            comment="Regular table comment",
            last_altered=datetime.now(),
            size_in_bytes=1000,
            rows_count=100,
            is_dynamic=False,
        )

        # Verify that subtypes are returned correctly
        from datahub.ingestion.source.common.subtypes import DatasetSubTypes

        self.assertEqual(dynamic_table.get_subtype(), DatasetSubTypes.DYNAMIC_TABLE)
        self.assertEqual(regular_table.get_subtype(), DatasetSubTypes.TABLE)

    @mock.patch(
        "datahub.ingestion.source.snowflake.snowflake_schema_gen.SnowflakeSchemaGenerator.get_connection"
    )
    def test_dynamic_table_metadata_extraction(self, mock_get_connection):
        """Test that dynamic table metadata is extracted correctly"""
        # Set up mocks
        connection = mock.MagicMock()
        connection.query.return_value = iter(
            [
                {
                    "TARGET_LAG": "60 minutes",
                    "TARGET_LAG_TYPE": "DOWNSTREAM",
                    "WAREHOUSE": "COMPUTE_WH",
                    "SCHEDULE": "USING CRON * * * * * UTC",
                    "CONDITION": "TRUE",
                    "CREATED_ON": datetime.now(),
                }
            ]
        )
        mock_get_connection.return_value = connection

        schema_gen = SnowflakeSchemaGenerator(
            config=MagicMock(),
            report=MagicMock(),
            connection=connection,
            filters=MagicMock(),
            identifiers=MagicMock(),
            domain_registry=None,
            profiler=None,
            aggregator=None,
            snowsight_url_builder=None,
        )

        dynamic_table = SnowflakeDynamicTable(
            name="TEST_DYNAMIC_TABLE",
            type="DYNAMIC TABLE",
            created=datetime.now(),
            comment="Dynamic table comment",
            last_altered=datetime.now(),
            size_in_bytes=1000,
            rows_count=100,
            is_dynamic=True,
        )

        # Get dataset properties
        props = schema_gen.get_dataset_properties(
            table=dynamic_table, schema_name="TEST_SCHEMA", db_name="TEST_DB"
        )

        # Verify that custom properties include dynamic table specific fields
        self.assertIn("IS_DYNAMIC", props.customProperties)
        self.assertEqual(props.customProperties["IS_DYNAMIC"], "true")
        self.assertIn("DYNAMIC_TABLE_TARGET_LAG", props.customProperties)
        self.assertIn("DYNAMIC_TABLE_WAREHOUSE", props.customProperties)
        self.assertIn("DYNAMIC_TABLE_SCHEDULE", props.customProperties)

    def test_dynamic_table_lineage_extraction(self):
        """Test that dynamic table lineage is extracted correctly"""
        # Mock the query results for dependency info
        connection = mock.MagicMock()
        connection.query.return_value = iter(
            [
                {
                    "TARGET_LAG": "60 minutes",
                    "TARGET_LAG_TYPE": "DOWNSTREAM",
                    "WAREHOUSE": "COMPUTE_WH",
                    "SCHEDULE": "USING CRON * * * * * UTC",
                    "QUERY_TEXT": "SELECT * FROM source_table",
                }
            ]
        )

        # Create a mock upstream table node for dynamic table
        upstream_node = UpstreamTableNode(
            upstream_object_domain=SnowflakeObjectDomain.DYNAMIC_TABLE,
            upstream_object_name="TEST_DB.TEST_SCHEMA.DYNAMIC_TABLE",
            query_id="12345",
        )

        # Create properly configured mocks
        identifiers = MagicMock()
        identifiers.get_dataset_identifier_from_qualified_name.return_value = (
            "TEST_DB.TEST_SCHEMA.DYNAMIC_TABLE"
        )
        identifiers.gen_dataset_urn.return_value = "urn:li:dataset:(urn:li:dataPlatform:snowflake,TEST_DB.TEST_SCHEMA.DYNAMIC_TABLE,PROD)"

        sql_aggregator = MagicMock()

        # Create a mock extractor with our properly configured mocks
        extractor = SnowflakeLineageExtractor(
            config=MagicMock(),
            report=MagicMock(),
            connection=connection,
            filters=MagicMock(),
            identifiers=identifiers,
            redundant_run_skip_handler=None,
            sql_aggregator=sql_aggregator,
        )

        # Test the dynamic table lineage extraction
        upstream_urns = extractor.map_query_result_upstreams([upstream_node], "12345")

        # Verify that the upstream URN was created for the dynamic table
        self.assertEqual(len(upstream_urns), 1)
        self.assertEqual(
            upstream_urns[0],
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,TEST_DB.TEST_SCHEMA.DYNAMIC_TABLE,PROD)",
        )

        # Verify that the view definition was added to sql_aggregator
        sql_aggregator.add_view_definition.assert_called_once()

    def test_is_dataset_pattern_allowed_for_dynamic_tables(self):
        """Test that dynamic tables are properly filtered by SnowflakeFilter.is_dataset_pattern_allowed"""

        # Create mock source report
        mock_report = MagicMock()

        # Mock the filter patterns directly
        mock_db_pattern = MagicMock()
        mock_db_pattern.allowed.return_value = True

        mock_schema_pattern = MagicMock()
        mock_schema_pattern.allowed.return_value = True

        mock_table_pattern = MagicMock()

        # Create filter configuration with mock patterns
        filter_config = MagicMock()
        filter_config.database_pattern = mock_db_pattern
        filter_config.schema_pattern = mock_schema_pattern
        filter_config.table_pattern = mock_table_pattern
        filter_config.match_fully_qualified_names = False

        # Create a patch for _cleanup_qualified_name to return expected values
        with mock.patch(
            "datahub.ingestion.source.snowflake.snowflake_utils._cleanup_qualified_name",
            autospec=True,
        ) as mock_cleanup:
            # Return the same value as input for testing
            mock_cleanup.side_effect = lambda name, _: name

            snowflake_filter = SnowflakeFilter(
                filter_config=filter_config, structured_reporter=mock_report
            )

            # Test that a dynamic table matching the pattern is allowed
            mock_table_pattern.allowed.return_value = True
            self.assertTrue(
                snowflake_filter.is_dataset_pattern_allowed(
                    dataset_name="test_db.test_schema.dynamic_table1",
                    dataset_type="dynamic table",
                )
            )

            # Test that a dynamic table not matching the pattern is not allowed
            mock_table_pattern.allowed.return_value = False
            self.assertFalse(
                snowflake_filter.is_dataset_pattern_allowed(
                    dataset_name="test_db.test_schema.table1",
                    dataset_type="dynamic table",
                )
            )

            # Test that a regular table matching the same pattern is allowed
            mock_table_pattern.allowed.return_value = True
            self.assertTrue(
                snowflake_filter.is_dataset_pattern_allowed(
                    dataset_name="test_db.test_schema.dynamic_table1",
                    dataset_type="table",
                )
            )


if __name__ == "__main__":
    unittest.main()
