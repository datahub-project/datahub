from datetime import datetime
from unittest import mock

import pytest
from freezegun import freeze_time
from sqlalchemy import types
from sqlalchemy_bigquery import STRUCT

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.aws.s3_util import make_s3_urn
from datahub.ingestion.source.sql.athena import CustomAthenaRestDialect
from datahub.utilities.sqlalchemy_type_converter import MapType

FROZEN_TIME = "2020-04-14 07:00:00"


def test_athena_config_query_location_old_plus_new_value_not_allowed():
    from datahub.ingestion.source.sql.athena import AthenaConfig

    with pytest.raises(ValueError):
        AthenaConfig.parse_obj(
            {
                "aws_region": "us-west-1",
                "s3_staging_dir": "s3://sample-staging-dir/",
                "query_result_location": "s3://query_result_location",
                "work_group": "test-workgroup",
            }
        )


def test_athena_config_staging_dir_is_set_as_query_result():
    from datahub.ingestion.source.sql.athena import AthenaConfig

    config = AthenaConfig.parse_obj(
        {
            "aws_region": "us-west-1",
            "s3_staging_dir": "s3://sample-staging-dir/",
            "work_group": "test-workgroup",
        }
    )

    expected_config = AthenaConfig.parse_obj(
        {
            "aws_region": "us-west-1",
            "query_result_location": "s3://sample-staging-dir/",
            "work_group": "test-workgroup",
        }
    )

    assert config.json() == expected_config.json()


def test_athena_uri():
    from datahub.ingestion.source.sql.athena import AthenaConfig

    config = AthenaConfig.parse_obj(
        {
            "aws_region": "us-west-1",
            "query_result_location": "s3://query-result-location/",
            "work_group": "test-workgroup",
        }
    )
    assert config.get_sql_alchemy_url() == (
        "awsathena+rest://@athena.us-west-1.amazonaws.com:443"
        "?catalog_name=awsdatacatalog"
        "&duration_seconds=3600"
        "&s3_staging_dir=s3%3A%2F%2Fquery-result-location%2F"
        "&work_group=test-workgroup"
    )


@pytest.mark.integration
@freeze_time(FROZEN_TIME)
def test_athena_get_table_properties():
    from pyathena.model import AthenaTableMetadata

    from datahub.ingestion.source.sql.athena import AthenaConfig, AthenaSource

    config = AthenaConfig.parse_obj(
        {
            "aws_region": "us-west-1",
            "s3_staging_dir": "s3://sample-staging-dir/",
            "work_group": "test-workgroup",
        }
    )
    schema: str = "test_schema"
    table: str = "test_table"

    table_metadata = {
        "TableMetadata": {
            "Name": "test",
            "TableType": "testType",
            "CreateTime": datetime.now(),
            "LastAccessTime": datetime.now(),
            "PartitionKeys": [
                {"Name": "year", "Type": "string", "Comment": "testComment"},
                {"Name": "month", "Type": "string", "Comment": "testComment"},
            ],
            "Parameters": {
                "comment": "testComment",
                "location": "s3://testLocation",
                "inputformat": "testInputFormat",
                "outputformat": "testOutputFormat",
                "serde.serialization.lib": "testSerde",
            },
        },
    }

    mock_cursor = mock.MagicMock()
    mock_inspector = mock.MagicMock()
    mock_inspector.engine.raw_connection().cursor.return_value = mock_cursor
    mock_cursor.get_table_metadata.return_value = AthenaTableMetadata(
        response=table_metadata
    )

    # Mock partition query results
    mock_cursor.execute.return_value.description = [
        ["year"],
        ["month"],
    ]
    mock_cursor.execute.return_value.__iter__.return_value = [["2023", "12"]]

    ctx = PipelineContext(run_id="test")
    source = AthenaSource(config=config, ctx=ctx)
    source.cursor = mock_cursor

    # Test table properties
    description, custom_properties, location = source.get_table_properties(
        inspector=mock_inspector, table=table, schema=schema
    )
    assert custom_properties == {
        "comment": "testComment",
        "create_time": "2020-04-14 07:00:00",
        "inputformat": "testInputFormat",
        "last_access_time": "2020-04-14 07:00:00",
        "location": "s3://testLocation",
        "outputformat": "testOutputFormat",
        "partition_keys": '[{"name": "year", "type": "string", "comment": "testComment"}, {"name": "month", "type": "string", "comment": "testComment"}]',
        "serde.serialization.lib": "testSerde",
        "table_type": "testType",
    }
    assert location == make_s3_urn("s3://testLocation", "PROD")

    # Test partition functionality
    partitions = source.get_partitions(
        inspector=mock_inspector, schema=schema, table=table
    )
    assert partitions == ["year", "month"]

    # Verify the correct SQL query was generated for partitions
    expected_query = """\
select year,month from "test_schema"."test_table$partitions" \
where CAST(year as VARCHAR) || '-' || CAST(month as VARCHAR) = \
(select max(CAST(year as VARCHAR) || '-' || CAST(month as VARCHAR)) \
from "test_schema"."test_table$partitions")"""
    mock_cursor.execute.assert_called_once()
    actual_query = mock_cursor.execute.call_args[0][0]
    assert actual_query == expected_query

    # Verify partition cache was populated correctly
    assert source.table_partition_cache[schema][table].partitions == partitions
    assert source.table_partition_cache[schema][table].max_partition == {
        "year": "2023",
        "month": "12",
    }


def test_get_column_type_simple_types():
    assert isinstance(
        CustomAthenaRestDialect()._get_column_type(type_="int"), types.Integer
    )
    assert isinstance(
        CustomAthenaRestDialect()._get_column_type(type_="string"), types.String
    )
    assert isinstance(
        CustomAthenaRestDialect()._get_column_type(type_="boolean"), types.BOOLEAN
    )
    assert isinstance(
        CustomAthenaRestDialect()._get_column_type(type_="long"), types.BIGINT
    )
    assert isinstance(
        CustomAthenaRestDialect()._get_column_type(type_="double"), types.FLOAT
    )


def test_get_column_type_array():
    result = CustomAthenaRestDialect()._get_column_type(type_="array<string>")

    assert isinstance(result, types.ARRAY)
    assert isinstance(result.item_type, types.String)


def test_get_column_type_map():
    result = CustomAthenaRestDialect()._get_column_type(type_="map<string,int>")

    assert isinstance(result, MapType)
    assert isinstance(result.types[0], types.String)
    assert isinstance(result.types[1], types.Integer)


def test_column_type_struct():
    result = CustomAthenaRestDialect()._get_column_type(type_="struct<test:string>")

    assert isinstance(result, STRUCT)
    assert isinstance(result._STRUCT_fields[0], tuple)
    assert result._STRUCT_fields[0][0] == "test"
    assert isinstance(result._STRUCT_fields[0][1], types.String)


def test_column_type_decimal():
    result = CustomAthenaRestDialect()._get_column_type(type_="decimal(10,2)")

    assert isinstance(result, types.DECIMAL)
    assert 10 == result.precision
    assert 2 == result.scale


def test_column_type_complex_combination():
    result = CustomAthenaRestDialect()._get_column_type(
        type_="struct<id:string,name:string,choices:array<struct<id:string,label:string>>>"
    )

    assert isinstance(result, STRUCT)

    assert isinstance(result._STRUCT_fields[0], tuple)
    assert result._STRUCT_fields[0][0] == "id"
    assert isinstance(result._STRUCT_fields[0][1], types.String)

    assert isinstance(result._STRUCT_fields[1], tuple)
    assert result._STRUCT_fields[1][0] == "name"
    assert isinstance(result._STRUCT_fields[1][1], types.String)

    assert isinstance(result._STRUCT_fields[2], tuple)
    assert result._STRUCT_fields[2][0] == "choices"
    assert isinstance(result._STRUCT_fields[2][1], types.ARRAY)

    assert isinstance(result._STRUCT_fields[2][1].item_type, STRUCT)

    assert isinstance(result._STRUCT_fields[2][1].item_type._STRUCT_fields[0], tuple)
    assert result._STRUCT_fields[2][1].item_type._STRUCT_fields[0][0] == "id"
    assert isinstance(
        result._STRUCT_fields[2][1].item_type._STRUCT_fields[0][1], types.String
    )

    assert isinstance(result._STRUCT_fields[2][1].item_type._STRUCT_fields[1], tuple)
    assert result._STRUCT_fields[2][1].item_type._STRUCT_fields[1][0] == "label"
    assert isinstance(
        result._STRUCT_fields[2][1].item_type._STRUCT_fields[1][1], types.String
    )


def test_casted_partition_key():
    from datahub.ingestion.source.sql.athena import AthenaSource

    assert AthenaSource._casted_partition_key("test_col") == "CAST(test_col as VARCHAR)"
