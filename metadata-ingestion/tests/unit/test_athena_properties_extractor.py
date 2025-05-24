"""
Pytest tests for AthenaPropertiesExtractor.

Tests the extraction of properties, partitioning information,
and row format details from various Athena CREATE TABLE SQL statements.
"""

import pytest

from datahub.ingestion.source.sql.athena_properties_extractor import (
    AthenaPropertiesExtractionError,
    AthenaPropertiesExtractor,
    AthenaTableInfo,
    ColumnInfo,
    PartitionInfo,
    RowFormatInfo,
    TableProperties,
    TransformInfo,
)


class TestAthenaPropertiesExtractor:
    """Test class for AthenaPropertiesExtractor."""

    def test_iceberg_table_with_complex_partitioning(self):
        """Test extraction from Iceberg table with complex partitioning."""
        sql = """
              CREATE TABLE iceberg_table (ts timestamp, id bigint, data string, category string)
                  PARTITIONED BY (category, bucket(16, id), year(ts), month(ts), day(ts), hour(ts), truncate(10, ts))
          LOCATION 's3://amzn-s3-demo-bucket/your-folder/'
          TBLPROPERTIES ( 'table_type' = 'ICEBERG' ) \
              """

        result = AthenaPropertiesExtractor.get_table_properties(sql)

        # Test basic structure
        assert isinstance(result, AthenaTableInfo)
        assert isinstance(result.partition_info, PartitionInfo)
        assert isinstance(result.table_properties, TableProperties)
        assert isinstance(result.row_format, RowFormatInfo)

        # Test partition info
        partition_info = result.partition_info

        # Should have multiple simple columns
        assert len(partition_info.simple_columns) > 0

        # Check for category column (simple partition)
        category_cols = [
            col for col in partition_info.simple_columns if col.name == "category"
        ]
        assert len(category_cols) == 1
        assert category_cols[0].type == "TEXT"

        # Check for id column (used in bucket transform)
        id_cols = [col for col in partition_info.simple_columns if col.name == "id"]
        assert len(id_cols) == 1
        assert id_cols[0].type == "BIGINT"

        # Check for ts column (used in time transforms)
        ts_cols = [col for col in partition_info.simple_columns if col.name == "ts"]
        assert len(ts_cols) == 1
        assert ts_cols[0].type == "TIMESTAMP"

        # Test transforms
        transforms = partition_info.transforms
        assert len(transforms) >= 6  # bucket, year, month, day, hour, truncate

        # Check bucket transform
        bucket_transforms = [t for t in transforms if t.type == "bucket"]
        assert len(bucket_transforms) == 1
        bucket_transform = bucket_transforms[0]
        assert bucket_transform.column.name == "id"
        assert bucket_transform.bucket_count == 16

        # Check time transforms
        time_transform_types = {
            t.type for t in transforms if t.type in ["year", "month", "day", "hour"]
        }
        assert "year" in time_transform_types
        assert "month" in time_transform_types
        assert "day" in time_transform_types
        assert "hour" in time_transform_types

        # Check truncate transform
        truncate_transforms = [t for t in transforms if t.type == "truncate"]
        assert len(truncate_transforms) == 1
        truncate_transform = truncate_transforms[0]
        assert truncate_transform.column.name == "ts"
        assert truncate_transform.length == 10

        # Test table properties
        table_props = result.table_properties
        assert table_props.location == "s3://amzn-s3-demo-bucket/your-folder/"
        assert table_props.additional_properties is not None
        assert table_props.additional_properties.get("table_type") == "ICEBERG"

    def test_trino_table_with_array_partitioning(self):
        """Test extraction from Trino table with ARRAY partitioning."""
        sql = """
              create table trino.db_collection (
                                                   col1 varchar,
                                                   col2 varchar,
                                                   col3 varchar
              )with (
                   external_location = 's3a://bucket/trino/db_collection/*',
                   format = 'PARQUET',
                   partitioned_by = ARRAY['col1','col2']
                   ) \
              """

        result = AthenaPropertiesExtractor.get_table_properties(sql)

        # Test table properties
        table_props = result.table_properties
        assert table_props.location == "s3a://bucket/trino/db_collection/*"
        assert table_props.format == "PARQUET"

        # Note: ARRAY partitioning might not be parsed the same way as standard PARTITIONED BY
        # This tests that the extraction doesn't fail and extracts what it can

    def test_simple_orc_table(self):
        """Test extraction from simple ORC table."""
        sql = """
              CREATE TABLE orders (
                                      orderkey bigint,
                                      orderstatus varchar,
                                      totalprice double,
                                      orderdate date
              )
                  WITH (format = 'ORC')
              """

        result = AthenaPropertiesExtractor.get_table_properties(sql)

        # Test basic structure
        assert isinstance(result, AthenaTableInfo)

        # Should have no partitions
        assert len(result.partition_info.simple_columns) == 0
        assert len(result.partition_info.transforms) == 0

        # Test table properties
        table_props = result.table_properties
        assert table_props.format == "ORC"
        assert table_props.location is None
        assert table_props.comment is None

    def test_table_with_comments(self):
        """Test extraction from table with table and column comments."""
        sql = """
              CREATE TABLE IF NOT EXISTS orders (
                                                    orderkey bigint,
                                                    orderstatus varchar,
                                                    totalprice double COMMENT 'Price in cents.',
                                                    orderdate date
              )
                  COMMENT 'A table to keep track of orders.' \
              """

        result = AthenaPropertiesExtractor.get_table_properties(sql)

        # Test table comment
        table_props = result.table_properties
        assert table_props.comment == "A table to keep track of orders."

        # No partitions expected
        assert len(result.partition_info.simple_columns) == 0
        assert len(result.partition_info.transforms) == 0

    def test_table_with_row_format_and_serde(self):
        """Test extraction from table with row format and SERDE properties."""
        sql = """
              CREATE TABLE IF NOT EXISTS orders (
                                                    orderkey bigint,
                                                    orderstatus varchar,
                                                    totalprice double,
                                                    orderdate date
              )
                  ROW FORMAT DELIMITED COLLECTION ITEMS TERMINATED BY ','
                  STORED AS PARQUET
                  WITH SERDEPROPERTIES (
                      'serialization.format' = '1'
                      ) \
              """

        result = AthenaPropertiesExtractor.get_table_properties(sql)

        # Test table properties
        table_props = result.table_properties

        # Test SERDE properties
        assert table_props.serde_properties is not None
        assert table_props.serde_properties.get("serialization.format") == "1"

        # Test row format
        row_format = result.row_format
        assert isinstance(row_format, RowFormatInfo)
        assert isinstance(row_format.properties, dict)
        assert "No RowFormatDelimitedProperty found" not in row_format.json_formatted

    def test_empty_sql_raises_error(self):
        """Test that empty SQL raises appropriate error."""
        with pytest.raises(
            AthenaPropertiesExtractionError, match="SQL statement cannot be empty"
        ):
            AthenaPropertiesExtractor.get_table_properties("")

        with pytest.raises(
            AthenaPropertiesExtractionError, match="SQL statement cannot be empty"
        ):
            AthenaPropertiesExtractor.get_table_properties("   ")

    def test_minimal_create_table(self):
        """Test extraction from minimal CREATE TABLE statement."""
        sql = "CREATE TABLE test (id int)"

        result = AthenaPropertiesExtractor.get_table_properties(sql)

        # Should not fail and return basic structure
        assert isinstance(result, AthenaTableInfo)
        assert len(result.partition_info.simple_columns) == 0
        assert len(result.partition_info.transforms) == 0
        assert result.table_properties.location is None

    def test_column_info_dataclass(self):
        """Test ColumnInfo dataclass properties."""
        sql = """
              CREATE TABLE test (id bigint, name varchar)
                  PARTITIONED BY (id) \
              """

        result = AthenaPropertiesExtractor.get_table_properties(sql)

        # Test that we get ColumnInfo objects
        assert len(result.partition_info.simple_columns) == 1
        column = result.partition_info.simple_columns[0]

        assert isinstance(column, ColumnInfo)
        assert column.name == "id"
        assert column.type == "BIGINT"

    def test_transform_info_dataclass(self):
        """Test TransformInfo dataclass properties."""
        sql = """
              CREATE TABLE test (ts timestamp, id bigint)
                  PARTITIONED BY (year(ts), bucket(8, id)) \
              """

        result = AthenaPropertiesExtractor.get_table_properties(sql)

        transforms = result.partition_info.transforms
        assert len(transforms) >= 2

        # Find year transform
        year_transforms = [t for t in transforms if t.type == "year"]
        assert len(year_transforms) == 1
        year_transform = year_transforms[0]

        assert isinstance(year_transform, TransformInfo)
        assert year_transform.type == "year"
        assert isinstance(year_transform.column, ColumnInfo)
        assert year_transform.column.name == "ts"
        assert year_transform.bucket_count is None
        assert year_transform.length is None

        # Find bucket transform
        bucket_transforms = [t for t in transforms if t.type == "bucket"]
        assert len(bucket_transforms) == 1
        bucket_transform = bucket_transforms[0]

        assert isinstance(bucket_transform, TransformInfo)
        assert bucket_transform.type == "bucket"
        assert bucket_transform.column.name == "id"
        assert bucket_transform.bucket_count == 8
        assert bucket_transform.length is None

    def test_multiple_sql_statements_stateless(self):
        """Test that the extractor is stateless and works with multiple SQL statements."""
        sql1 = "CREATE TABLE test1 (id int) WITH (format = 'PARQUET')"
        sql2 = "CREATE TABLE test2 (name varchar) WITH (format = 'ORC')"

        # Call multiple times to ensure no state interference
        result1 = AthenaPropertiesExtractor.get_table_properties(sql1)
        result2 = AthenaPropertiesExtractor.get_table_properties(sql2)
        result1_again = AthenaPropertiesExtractor.get_table_properties(sql1)

        # Results should be consistent
        assert result1.table_properties.format == "PARQUET"
        assert result2.table_properties.format == "ORC"
        assert result1_again.table_properties.format == "PARQUET"

        # Results should be independent
        assert result1.table_properties.format != result2.table_properties.format

    @pytest.mark.parametrize(
        "sql,expected_location",
        [
            (
                "CREATE TABLE test (id int) LOCATION 's3://bucket/path/'",
                "s3://bucket/path/",
            ),
            ("CREATE TABLE test (id int)", None),
        ],
    )
    def test_location_extraction_parametrized(self, sql, expected_location):
        """Test location extraction with parametrized inputs."""
        result = AthenaPropertiesExtractor.get_table_properties(sql)
        assert result.table_properties.location == expected_location


# Integration test that could be run with actual SQL files
class TestAthenaPropertiesExtractorIntegration:
    """Integration tests for AthenaPropertiesExtractor."""

    def test_complex_real_world_example(self):
        """Test with a complex real-world-like example."""
        sql = """
              CREATE TABLE analytics.user_events (
                                                     user_id bigint COMMENT 'Unique user identifier',
                                                     event_time timestamp COMMENT 'When the event occurred',
                                                     event_type varchar COMMENT 'Type of event',
                                                     session_id varchar,
                                                     properties map<varchar, varchar> COMMENT 'Event properties',
                                                     created_date date
              )
                  COMMENT 'User event tracking table'
        PARTITIONED BY (
            created_date,
            bucket(100, user_id),
            hour(event_time)
        )
        LOCATION 's3://analytics-bucket/user-events/'
        STORED AS PARQUET
        TBLPROPERTIES (
            'table_type' = 'ICEBERG',
            'write.target-file-size-bytes' = '134217728',
            'write.delete.mode' = 'copy-on-write'
        ) \
              """

        result = AthenaPropertiesExtractor.get_table_properties(sql)

        # Comprehensive validation
        assert isinstance(result, AthenaTableInfo)

        # Check table properties
        props = result.table_properties
        assert props.location == "s3://analytics-bucket/user-events/"
        assert props.comment == "User event tracking table"
        assert props.additional_properties is not None
        assert props.additional_properties.get("table_type") == "ICEBERG"

        # Check partitioning
        partition_info = result.partition_info

        # Should have created_date as simple partition
        date_cols = [
            col for col in partition_info.simple_columns if col.name == "user_id"
        ]
        assert len(date_cols) == 1
        assert date_cols[0].type == "BIGINT"

        date_cols = [
            col for col in partition_info.simple_columns if col.name == "created_date"
        ]
        assert len(date_cols) == 1
        assert date_cols[0].type == "DATE"

        # Should have transforms
        transforms = partition_info.transforms
        transform_types = {t.type for t in transforms}
        assert "bucket" in transform_types
        assert "hour" in transform_types

        # Validate bucket transform
        bucket_transforms = [t for t in transforms if t.type == "bucket"]
        assert len(bucket_transforms) == 1
        assert bucket_transforms[0].bucket_count == 100
        assert bucket_transforms[0].column.name == "user_id"

    def test_external_table_with_row_format_delimited(self):
        """Test extraction from external table with detailed row format."""
        sql = """
        CREATE EXTERNAL TABLE `my_table`(
          `itcf id` string, 
          `itcf control name` string, 
          `itcf control description` string, 
          `itcf process` string, 
          `standard` string, 
          `controlid` string, 
          `threshold` string, 
          `status` string, 
          `date reported` string, 
          `remediation (accs specific)` string, 
          `aws account id` string, 
          `aws resource id` string, 
          `aws account owner` string)
        ROW FORMAT DELIMITED 
          FIELDS TERMINATED BY ',' 
          ESCAPED BY '\\\\'
          LINES TERMINATED BY '\\n'
        LOCATION
          's3://myfolder/'
        TBLPROPERTIES (  
          'skip.header.line.count'='1');
        """

        result = AthenaPropertiesExtractor.get_table_properties(sql)

        # Test basic structure
        assert isinstance(result, AthenaTableInfo)

        # Test table properties
        table_props = result.table_properties
        assert table_props.location == "s3://myfolder/"

        # Test TBLPROPERTIES
        assert table_props.additional_properties is not None
        assert table_props.additional_properties.get("skip.header.line.count") == "1"

        # Test row format
        row_format = result.row_format
        assert isinstance(row_format, RowFormatInfo)

        # The row format should contain delimited properties
        # Note: The exact keys depend on how sqlglot parses ROW FORMAT DELIMITED
        assert isinstance(row_format.properties, dict)

        # Should have structured JSON output
        assert row_format.json_formatted != "No RowFormatDelimitedProperty found"

        # Should not have partitions (no PARTITIONED BY clause)
        assert len(result.partition_info.simple_columns) == 0
        assert len(result.partition_info.transforms) == 0

    def test_database_qualified_table_with_iceberg_properties(self):
        """Test extraction from database-qualified table with Iceberg properties."""
        sql = """
              CREATE TABLE mydatabase.my_table (
                                                   id string,
                                                   name string,
                                                   type string,
                                                   industry string,
                                                   annual_revenue double,
                                                   website string,
                                                   phone string,
                                                   billing_street string,
                                                   billing_city string,
                                                   billing_state string,
                                                   billing_postal_code string,
                                                   billing_country string,
                                                   shipping_street string,
                                                   shipping_city string,
                                                   shipping_state string,
                                                   shipping_postal_code string,
                                                   shipping_country string,
                                                   number_of_employees int,
                                                   description string,
                                                   owner_id string,
                                                   created_date timestamp,
                                                   last_modified_date timestamp,
                                                   is_deleted boolean)
                  LOCATION 's3://mybucket/myfolder/'
        TBLPROPERTIES (
          'table_type'='iceberg',
          'write_compression'='snappy',
          'format'='parquet',
          'optimize_rewrite_delete_file_threshold'='10'
        ); \
              """

        result = AthenaPropertiesExtractor.get_table_properties(sql)

        # Test basic structure
        assert isinstance(result, AthenaTableInfo)

        # Test table properties
        table_props = result.table_properties
        assert table_props.location == "s3://mybucket/myfolder/"

        # Test multiple TBLPROPERTIES
        assert table_props.additional_properties is not None
        expected_props = {
            "table_type": "iceberg",
            "write_compression": "snappy",
            "format": "parquet",
            "optimize_rewrite_delete_file_threshold": "10",
        }

        for key, expected_value in expected_props.items():
            assert table_props.additional_properties.get(key) == expected_value, (
                f"Expected {key}={expected_value}, got {table_props.additional_properties.get(key)}"
            )

        # Should not have partitions (no PARTITIONED BY clause)
        assert len(result.partition_info.simple_columns) == 0
        assert len(result.partition_info.transforms) == 0

        # Row format should be empty/default
        row_format = result.row_format
        assert isinstance(row_format, RowFormatInfo)
        # Should either be empty dict or indicate no row format found
        assert (
            len(row_format.properties) == 0
            or "No RowFormatDelimitedProperty found" in row_format.json_formatted
        )

    def test_iceberg_table_with_backtick_partitioning(self):
        """Test extraction from Iceberg table with backtick-quoted partition functions."""
        sql = """
              CREATE TABLE datalake_agg.ml_outdoor_master (
                                                              event_uuid string,
                                                              uuid string,
                                                              _pk string)
                  PARTITIONED BY (
          `day(event_timestamp)`,
          `month(event_timestamp)`
        )
        LOCATION 's3://bucket/folder/table'
        TBLPROPERTIES (
          'table_type'='iceberg',
          'vacuum_max_snapshot_age_seconds'='60',
          'format'='PARQUET',
          'write_compression'='GZIP',
          'optimize_rewrite_delete_file_threshold'='2',
          'optimize_rewrite_data_file_threshold'='5',
          'vacuum_min_snapshots_to_keep'='6'
        ) \
              """

        result = AthenaPropertiesExtractor.get_table_properties(sql)

        # Test basic structure
        assert isinstance(result, AthenaTableInfo)

        # Test table properties
        table_props = result.table_properties
        assert table_props.location == "s3://bucket/folder/table"

        # Test comprehensive TBLPROPERTIES for Iceberg
        assert table_props.additional_properties is not None
        expected_props = {
            "table_type": "iceberg",
            "vacuum_max_snapshot_age_seconds": "60",
            "format": "PARQUET",
            "write_compression": "GZIP",
            "optimize_rewrite_delete_file_threshold": "2",
            "optimize_rewrite_data_file_threshold": "5",
            "vacuum_min_snapshots_to_keep": "6",
        }

        for key, expected_value in expected_props.items():
            actual_value = table_props.additional_properties.get(key)
            assert actual_value == expected_value, (
                f"Expected {key}={expected_value}, got {actual_value}"
            )

        # Test partition info - this is the interesting part with backtick-quoted functions
        partition_info = result.partition_info

        # Should have transforms for day() and month() functions
        transforms = partition_info.transforms
        assert len(transforms) >= 2, (
            f"Expected at least 2 transforms, got {len(transforms)}"
        )

        # Check for day transform
        day_transforms = [t for t in transforms if t.type == "day"]
        assert len(day_transforms) >= 1, (
            f"Expected day transform, transforms: {[t.type for t in transforms]}"
        )

        if day_transforms:
            day_transform = day_transforms[0]
            assert isinstance(day_transform, TransformInfo)
            assert day_transform.type == "day"
            assert isinstance(day_transform.column, ColumnInfo)
            # The column should be event_timestamp (extracted from day(event_timestamp))
            assert day_transform.column.name == "event_timestamp"

        # Check for month transform
        month_transforms = [t for t in transforms if t.type == "month"]
        assert len(month_transforms) >= 1, (
            f"Expected month transform, transforms: {[t.type for t in transforms]}"
        )

        if month_transforms:
            month_transform = month_transforms[0]
            assert isinstance(month_transform, TransformInfo)
            assert month_transform.type == "month"
            assert isinstance(month_transform.column, ColumnInfo)
            # The column should be event_timestamp (extracted from month(event_timestamp))
            assert month_transform.column.name == "event_timestamp"

        # Test simple columns - should include event_timestamp from the transforms
        simple_columns = partition_info.simple_columns
        event_timestamp_cols = [
            col for col in simple_columns if col.name == "event_timestamp"
        ]
        assert len(event_timestamp_cols) >= 1, (
            f"Expected event_timestamp column, columns: {[col.name for col in simple_columns]}"
        )

        # The event_timestamp column type might be "unknown" since it's not in the table definition
        # but referenced in partitioning - this tests our defensive handling
        if event_timestamp_cols:
            event_timestamp_col = event_timestamp_cols[0]
            assert isinstance(event_timestamp_col, ColumnInfo)
            assert event_timestamp_col.name == "event_timestamp"
            # Type should be "unknown" since event_timestamp is not in the table columns
            assert event_timestamp_col.type == "unknown"

    def test_partition_function_extraction_edge_cases(self):
        """Test edge cases in partition function extraction with various formats."""
        sql = """
              CREATE TABLE test_partitions (
                                               ts timestamp,
                                               id bigint,
                                               data string
              )
                  PARTITIONED BY (
          `day(ts)`,
          `bucket(5, id)`,
          `truncate(100, data)`
        ) \
              """

        result = AthenaPropertiesExtractor.get_table_properties(sql)

        partition_info = result.partition_info
        transforms = partition_info.transforms

        # Should have 3 transforms
        assert len(transforms) == 3

        # Verify each transform type exists
        transform_types = {t.type for t in transforms}
        assert "day" in transform_types
        assert "bucket" in transform_types
        assert "truncate" in transform_types

        # Test bucket transform parameters
        bucket_transforms = [t for t in transforms if t.type == "bucket"]
        if bucket_transforms:
            bucket_transform = bucket_transforms[0]
            assert bucket_transform.bucket_count == 5
            assert bucket_transform.column.name == "id"
            assert bucket_transform.column.type == "BIGINT"

        # Test truncate transform parameters
        truncate_transforms = [t for t in transforms if t.type == "truncate"]
        if truncate_transforms:
            truncate_transform = truncate_transforms[0]
            assert truncate_transform.length == 100
            assert truncate_transform.column.name == "data"
            assert truncate_transform.column.type == "TEXT"

        # Test day transform
        day_transforms = [t for t in transforms if t.type == "day"]
        if day_transforms:
            day_transform = day_transforms[0]
            assert day_transform.column.name == "ts"
            assert day_transform.column.type == "TIMESTAMP"
