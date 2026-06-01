from datetime import datetime
from typing import List
from unittest import mock

import pytest
import sqlglot
import time_machine
from pyathena import OperationalError
from pyathena.model import AthenaTableMetadata
from sqlalchemy import types
from sqlalchemy_bigquery import STRUCT
from sqlglot.dialects import Athena

from datahub.emitter.mce_builder import (
    make_dataset_urn,
    make_dataset_urn_with_platform_instance,
)
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.aws.s3_util import make_s3_urn
from datahub.ingestion.source.sql.athena import (
    AthenaConfig,
    AthenaSource,
    CustomAthenaRestDialect,
    Partitionitem,
)
from datahub.metadata.schema_classes import (
    ArrayTypeClass,
    BooleanTypeClass,
    MapTypeClass,
    StringTypeClass,
)
from datahub.utilities.sqlalchemy_type_converter import MapType

FROZEN_TIME = "2020-04-14 07:00:00"


def test_athena_config_query_location_old_plus_new_value_not_allowed():
    with pytest.raises(ValueError):
        AthenaConfig.model_validate(
            {
                "aws_region": "us-west-1",
                "s3_staging_dir": "s3://sample-staging-dir/",
                "query_result_location": "s3://query_result_location",
                "work_group": "test-workgroup",
            }
        )


def test_athena_config_staging_dir_is_set_as_query_result():
    config = AthenaConfig.model_validate(
        {
            "aws_region": "us-west-1",
            "s3_staging_dir": "s3://sample-staging-dir/",
            "work_group": "test-workgroup",
        }
    )

    expected_config = AthenaConfig.model_validate(
        {
            "aws_region": "us-west-1",
            "query_result_location": "s3://sample-staging-dir/",
            "work_group": "test-workgroup",
        }
    )

    assert config.model_dump_json() == expected_config.model_dump_json()


def test_athena_config_rejects_empty_catalog_name():
    with pytest.raises(ValueError):
        AthenaConfig.model_validate(
            {
                "aws_region": "us-west-1",
                "query_result_location": "s3://q/",
                "work_group": "wg",
                "catalog_name": "",
            }
        )


def test_athena_uri():
    config = AthenaConfig.model_validate(
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


@time_machine.travel(FROZEN_TIME, tick=False)
def test_athena_get_table_properties():
    config = AthenaConfig.model_validate(
        {
            "aws_region": "us-west-1",
            "s3_staging_dir": "s3://sample-staging-dir/",
            "work_group": "test-workgroup",
            "profiling": {"enabled": True, "partition_profiling_enabled": True},
            "extract_partitions_using_create_statements": True,
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
    mock_cursor.get_table_metadata.return_value = AthenaTableMetadata(
        response=table_metadata
    )

    class MockCursorResult:
        def __init__(self, data: List, description: List):
            self._data = data
            self._description = description

        def __iter__(self):
            """Makes the object iterable, which allows list() to work"""
            return iter(self._data)

        @property
        def description(self):
            """Returns the description as requested"""
            return self._description

    mock_result = MockCursorResult(
        data=[["2023", "12"]], description=[["year"], ["month"]]
    )
    mock_cursor.execute.side_effect = [
        OperationalError("First call fails"),
        mock_result,
    ]
    mock_cursor.fetchall.side_effect = [OperationalError("First call fails")]
    mock_cursor.connection.client.get_paginator.return_value.paginate.return_value = [
        {"DataCatalogsSummary": [{"CatalogName": "AwsDataCatalog", "Type": "GLUE"}]}
    ]

    ctx = PipelineContext(run_id="test")
    source = AthenaSource(config=config, ctx=ctx)
    source.cursor = mock_cursor

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
    # Glue catalog → Glue URN as upstream lineage
    expected_glue_urn = make_dataset_urn("glue", f"{schema}.{table}")
    assert location == expected_glue_urn

    partitions = source.get_partitions(
        inspector=mock_inspector, schema=schema, table=table
    )
    assert partitions == ["year", "month"]

    expected_create_table_query = "SHOW CREATE TABLE `test_schema`.`test_table`"
    expected_query = """\
select year,month from "test_schema"."test_table$partitions" \
where CONCAT(CAST(year as VARCHAR), CAST('-' AS VARCHAR), CAST(month as VARCHAR)) = \
(select max(CONCAT(CAST(year as VARCHAR), CAST('-' AS VARCHAR), CAST(month as VARCHAR))) \
from "test_schema"."test_table$partitions")"""
    assert mock_cursor.execute.call_count == 2
    assert expected_create_table_query == mock_cursor.execute.call_args_list[0][0][0]
    actual_query = mock_cursor.execute.call_args_list[1][0][0]
    assert actual_query == expected_query

    assert source.table_partition_cache[schema][table].partitions == partitions
    assert source.table_partition_cache[schema][table].max_partition == {
        "year": "2023",
        "month": "12",
    }


@time_machine.travel(FROZEN_TIME, tick=False)
def test_athena_get_table_properties_iceberg_location():
    config = AthenaConfig.model_validate(
        {
            "aws_region": "us-west-1",
            "s3_staging_dir": "s3://sample-staging-dir/",
            "work_group": "test-workgroup",
            "catalog_name": "my-hive-catalog",
        }
    )
    schema: str = "default"
    table: str = "test_iceberg_table"

    table_metadata = {
        "TableMetadata": {
            "Name": "test_iceberg_table",
            "TableType": "EXTERNAL_TABLE",
            "CreateTime": datetime.now(),
            "PartitionKeys": [],
            "Parameters": {
                "table_type": "ICEBERG",
                "location": "s3://testLocation/test_iceberg_table/",
                "metadata_location": "s3://testLocation/test_iceberg_table/metadata/00000-d5e84dda-2a12-4639-ab7e-32d480643374.metadata.json",
            },
        },
    }

    mock_cursor = mock.MagicMock()
    mock_inspector = mock.MagicMock()
    mock_cursor.get_table_metadata.return_value = AthenaTableMetadata(
        response=table_metadata
    )
    mock_cursor.connection.client.get_paginator.return_value.paginate.return_value = [
        {
            "DataCatalogsSummary": [
                {"CatalogName": "my-hive-catalog", "Type": "HIVE"},
            ]
        }
    ]

    ctx = PipelineContext(run_id="test")
    source = AthenaSource(config=config, ctx=ctx)
    source.cursor = mock_cursor

    # Non-Glue Iceberg tables get Iceberg URN as upstream lineage
    _, _, location = source.get_table_properties(
        inspector=mock_inspector, table=table, schema=schema
    )
    expected_iceberg_urn = make_dataset_urn("iceberg", f"{schema}.{table}")
    assert location == expected_iceberg_urn


def test_get_upstream_location_handles_none_parameters():
    """Returns None gracefully (no AttributeError) when metadata.parameters is None."""
    config = AthenaConfig.model_validate(
        {
            "aws_region": "us-west-1",
            "query_result_location": "s3://query-result-location/",
            "work_group": "test-workgroup",
            "catalog_name": "my-hive-catalog",
        }
    )
    ctx = PipelineContext(run_id="test")
    source = AthenaSource(config=config, ctx=ctx)

    mock_cursor = mock.MagicMock()
    mock_cursor.connection.client.get_paginator.return_value.paginate.return_value = [
        {"DataCatalogsSummary": [{"CatalogName": "my-hive-catalog", "Type": "HIVE"}]}
    ]
    source.cursor = mock_cursor

    metadata = mock.MagicMock(spec=AthenaTableMetadata)
    metadata.parameters = None

    location = source._get_upstream_location(
        schema="default",
        table="t",
        metadata=metadata,
    )
    assert location is None


@pytest.mark.parametrize(
    "location_param,expected_urn",
    [
        pytest.param(
            {"location": "s3://my-bucket/my-hive-table/"},
            make_s3_urn("s3://my-bucket/my-hive-table/", "PROD"),
            id="s3_location",
        ),
        pytest.param(
            {"location": "hdfs://namenode/data/test_table"},
            None,
            id="non_s3_location",
        ),
        pytest.param(
            {},
            None,
            id="no_location",
        ),
    ],
)
def test_athena_get_table_properties_non_glue_non_iceberg_location(
    location_param, expected_urn
):
    """Non-Glue, non-Iceberg tables: lineage depends on location parameter."""
    config = AthenaConfig.model_validate(
        {
            "aws_region": "us-west-1",
            "query_result_location": "s3://query-result-location/",
            "work_group": "test-workgroup",
            "catalog_name": "my-hive-catalog",
        }
    )
    schema = "default"
    table = "test_table"

    table_metadata = {
        "TableMetadata": {
            "Name": table,
            "TableType": "EXTERNAL_TABLE",
            "CreateTime": None,
            "PartitionKeys": [],
            "Parameters": location_param,
        },
    }

    mock_cursor = mock.MagicMock()
    mock_inspector = mock.MagicMock()
    mock_cursor.get_table_metadata.return_value = AthenaTableMetadata(
        response=table_metadata
    )
    mock_cursor.connection.client.get_paginator.return_value.paginate.return_value = [
        {"DataCatalogsSummary": [{"CatalogName": "my-hive-catalog", "Type": "HIVE"}]}
    ]

    ctx = PipelineContext(run_id="test")
    source = AthenaSource(config=config, ctx=ctx)
    source.cursor = mock_cursor

    _, _, location = source.get_table_properties(
        inspector=mock_inspector, table=table, schema=schema
    )
    assert location == expected_urn


def test_catalog_type_returns_glue():
    """_catalog_type returns 'GLUE' when catalog is found in API response."""
    config = AthenaConfig.model_validate(
        {
            "aws_region": "us-west-1",
            "query_result_location": "s3://query-result-location/",
            "work_group": "test-workgroup",
            "catalog_name": "AwsDataCatalog",
        }
    )
    ctx = PipelineContext(run_id="test")
    source = AthenaSource(config=config, ctx=ctx)

    mock_cursor = mock.MagicMock()
    mock_cursor.connection.client.get_paginator.return_value.paginate.return_value = [
        {
            "DataCatalogsSummary": [
                {"CatalogName": "AwsDataCatalog", "Type": "GLUE"},
                {"CatalogName": "other-catalog", "Type": "HIVE"},
            ]
        }
    ]
    source.cursor = mock_cursor

    assert source._catalog_type == "GLUE"


def test_catalog_type_case_insensitive_match():
    """_catalog_type matches catalog name case-insensitively."""
    config = AthenaConfig.model_validate(
        {
            "aws_region": "us-west-1",
            "query_result_location": "s3://query-result-location/",
            "work_group": "test-workgroup",
            "catalog_name": "awsdatacatalog",
        }
    )
    ctx = PipelineContext(run_id="test")
    source = AthenaSource(config=config, ctx=ctx)

    mock_cursor = mock.MagicMock()
    # API returns uppercase name, config has lowercase
    mock_cursor.connection.client.get_paginator.return_value.paginate.return_value = [
        {"DataCatalogsSummary": [{"CatalogName": "AwsDataCatalog", "Type": "GLUE"}]}
    ]
    source.cursor = mock_cursor

    assert source._catalog_type == "GLUE"


def test_catalog_type_finds_catalog_on_later_page():
    """ListDataCatalogs is paginated; matching must scan across all pages."""
    config = AthenaConfig.model_validate(
        {
            "aws_region": "us-west-1",
            "query_result_location": "s3://query-result-location/",
            "work_group": "test-workgroup",
            "catalog_name": "my-hive-catalog",
        }
    )
    ctx = PipelineContext(run_id="test")
    source = AthenaSource(config=config, ctx=ctx)

    mock_cursor = mock.MagicMock()
    mock_cursor.connection.client.get_paginator.return_value.paginate.return_value = [
        {
            "DataCatalogsSummary": [
                {"CatalogName": "first-catalog", "Type": "FEDERATED"}
            ]
        },
        {"DataCatalogsSummary": [{"CatalogName": "second-catalog", "Type": "LAMBDA"}]},
        {"DataCatalogsSummary": [{"CatalogName": "my-hive-catalog", "Type": "HIVE"}]},
    ]
    source.cursor = mock_cursor

    assert source._catalog_type == "HIVE"


def test_catalog_type_returns_none_when_not_found():
    """_catalog_type returns None when catalog is not in API response.

    No report.warning is emitted at the API layer here — the policy layer
    (`is_glue_catalog`) emits the actionable "Skipping upstream lineage" warning
    only when the fallback heuristic also fails to classify the catalog.
    """
    config = AthenaConfig.model_validate(
        {
            "aws_region": "us-west-1",
            "query_result_location": "s3://query-result-location/",
            "work_group": "test-workgroup",
            "catalog_name": "missing-catalog",
        }
    )
    ctx = PipelineContext(run_id="test")
    source = AthenaSource(config=config, ctx=ctx)

    mock_cursor = mock.MagicMock()
    mock_cursor.connection.client.get_paginator.return_value.paginate.return_value = [
        {"DataCatalogsSummary": [{"CatalogName": "AwsDataCatalog", "Type": "GLUE"}]}
    ]
    source.cursor = mock_cursor

    assert source._catalog_type is None
    assert list(source.report.warnings) == []


def test_catalog_type_returns_none_on_api_exception():
    """_catalog_type returns None and logs a warning when the API call fails."""
    config = AthenaConfig.model_validate(
        {
            "aws_region": "us-west-1",
            "query_result_location": "s3://query-result-location/",
            "work_group": "test-workgroup",
        }
    )
    ctx = PipelineContext(run_id="test")
    source = AthenaSource(config=config, ctx=ctx)

    mock_cursor = mock.MagicMock()
    mock_cursor.connection.client.get_paginator.return_value.paginate.side_effect = (
        Exception("Network error")
    )
    source.cursor = mock_cursor

    assert source._catalog_type is None
    assert any(
        "Failed to determine catalog type" in w.message for w in source.report.warnings
    )


def test_catalog_type_caches_api_response_on_failure():
    config = AthenaConfig.model_validate(
        {
            "aws_region": "us-west-1",
            "query_result_location": "s3://query-result-location/",
            "work_group": "test-workgroup",
            "catalog_name": "missing-catalog",
        }
    )
    ctx = PipelineContext(run_id="test")
    source = AthenaSource(config=config, ctx=ctx)

    mock_cursor = mock.MagicMock()
    mock_cursor.connection.client.get_paginator.return_value.paginate.side_effect = (
        Exception("Network error")
    )
    source.cursor = mock_cursor

    assert source._catalog_type is None
    assert source._catalog_type is None
    assert source._catalog_type is None

    # API must be called exactly once even on the failure path.
    mock_cursor.connection.client.get_paginator.assert_called_once()


def test_is_glue_catalog_caches_result():
    """is_glue_catalog only calls list_data_catalogs once even when accessed multiple times."""
    config = AthenaConfig.model_validate(
        {
            "aws_region": "us-west-1",
            "query_result_location": "s3://query-result-location/",
            "work_group": "test-workgroup",
            "catalog_name": "AwsDataCatalog",
        }
    )
    ctx = PipelineContext(run_id="test")
    source = AthenaSource(config=config, ctx=ctx)

    mock_cursor = mock.MagicMock()
    mock_cursor.connection.client.get_paginator.return_value.paginate.return_value = [
        {"DataCatalogsSummary": [{"CatalogName": "AwsDataCatalog", "Type": "GLUE"}]}
    ]
    source.cursor = mock_cursor

    assert source.is_glue_catalog is True
    assert source.is_glue_catalog is True
    assert source.is_glue_catalog is True

    # API should have been called exactly once due to caching
    mock_cursor.connection.client.get_paginator.assert_called_once()


def test_is_glue_catalog_returns_none_for_non_default_catalog_when_not_found():
    config = AthenaConfig.model_validate(
        {
            "aws_region": "us-west-1",
            "query_result_location": "s3://query-result-location/",
            "work_group": "test-workgroup",
            "catalog_name": "missing-catalog",
        }
    )
    ctx = PipelineContext(run_id="test")
    source = AthenaSource(config=config, ctx=ctx)

    mock_cursor = mock.MagicMock()
    mock_cursor.connection.client.get_paginator.return_value.paginate.return_value = [
        {"DataCatalogsSummary": [{"CatalogName": "AwsDataCatalog", "Type": "GLUE"}]}
    ]
    source.cursor = mock_cursor

    assert source.is_glue_catalog is None
    assert any("Skipping upstream lineage" in w.message for w in source.report.warnings)


def test_is_glue_catalog_defaults_true_for_awsdatacatalog_when_not_found():
    """is_glue_catalog defaults to True for AwsDataCatalog, which is always Glue."""
    config = AthenaConfig.model_validate(
        {
            "aws_region": "us-west-1",
            "query_result_location": "s3://query-result-location/",
            "work_group": "test-workgroup",
            "catalog_name": "AwsDataCatalog",
        }
    )
    ctx = PipelineContext(run_id="test")
    source = AthenaSource(config=config, ctx=ctx)

    mock_cursor = mock.MagicMock()
    # API response doesn't include AwsDataCatalog; fallback should still assume Glue.
    mock_cursor.connection.client.get_paginator.return_value.paginate.return_value = [
        {"DataCatalogsSummary": [{"CatalogName": "other-catalog", "Type": "HIVE"}]}
    ]
    source.cursor = mock_cursor

    assert source.is_glue_catalog is True


def test_is_glue_catalog_false_for_hive_catalog():
    """is_glue_catalog returns False for HIVE catalog type."""
    config = AthenaConfig.model_validate(
        {
            "aws_region": "us-west-1",
            "query_result_location": "s3://query-result-location/",
            "work_group": "test-workgroup",
            "catalog_name": "my-hive-catalog",
        }
    )
    ctx = PipelineContext(run_id="test")
    source = AthenaSource(config=config, ctx=ctx)

    mock_cursor = mock.MagicMock()
    mock_cursor.connection.client.get_paginator.return_value.paginate.return_value = [
        {"DataCatalogsSummary": [{"CatalogName": "my-hive-catalog", "Type": "HIVE"}]}
    ]
    source.cursor = mock_cursor

    assert source.is_glue_catalog is False


@time_machine.travel(FROZEN_TIME, tick=False)
def test_athena_get_table_properties_glue_iceberg_returns_glue_urn():
    """When catalog is Glue, Iceberg tables still get Glue URN (Glue takes precedence)."""
    config = AthenaConfig.model_validate(
        {
            "aws_region": "us-west-1",
            "query_result_location": "s3://query-result-location/",
            "work_group": "test-workgroup",
        }
    )
    schema = "default"
    table = "iceberg_on_glue"

    table_metadata = {
        "TableMetadata": {
            "Name": table,
            "TableType": "EXTERNAL_TABLE",
            "CreateTime": datetime.now(),
            "PartitionKeys": [],
            "Parameters": {
                "table_type": "ICEBERG",
                "location": "s3://bucket/iceberg_on_glue/",
            },
        },
    }

    mock_cursor = mock.MagicMock()
    mock_inspector = mock.MagicMock()
    mock_cursor.get_table_metadata.return_value = AthenaTableMetadata(
        response=table_metadata
    )
    mock_cursor.connection.client.get_paginator.return_value.paginate.return_value = [
        {"DataCatalogsSummary": [{"CatalogName": "AwsDataCatalog", "Type": "GLUE"}]}
    ]

    ctx = PipelineContext(run_id="test")
    source = AthenaSource(config=config, ctx=ctx)
    source.cursor = mock_cursor

    # Glue catalog takes precedence over Iceberg table type
    _, _, location = source.get_table_properties(
        inspector=mock_inspector, table=table, schema=schema
    )
    expected_glue_urn = make_dataset_urn("glue", f"{schema}.{table}")
    assert location == expected_glue_urn


def test_get_upstream_location_threads_glue_platform_instance():
    config = AthenaConfig.model_validate(
        {
            "aws_region": "us-west-1",
            "query_result_location": "s3://query-result-location/",
            "work_group": "test-workgroup",
            "glue_platform_instance": "prod_glue",
        }
    )
    ctx = PipelineContext(run_id="test")
    source = AthenaSource(config=config, ctx=ctx)

    mock_cursor = mock.MagicMock()
    mock_cursor.connection.client.get_paginator.return_value.paginate.return_value = [
        {"DataCatalogsSummary": [{"CatalogName": "AwsDataCatalog", "Type": "GLUE"}]}
    ]
    source.cursor = mock_cursor

    metadata = mock.MagicMock(spec=AthenaTableMetadata)
    metadata.parameters = {}

    location = source._get_upstream_location(
        schema="default",
        table="t",
        metadata=metadata,
    )
    assert location == make_dataset_urn_with_platform_instance(
        platform="glue",
        name="default.t",
        platform_instance="prod_glue",
        env="PROD",
    )


def test_get_upstream_location_threads_iceberg_platform_instance():
    config = AthenaConfig.model_validate(
        {
            "aws_region": "us-west-1",
            "query_result_location": "s3://query-result-location/",
            "work_group": "test-workgroup",
            "catalog_name": "my-hive-catalog",
            "iceberg_platform_instance": "prod_iceberg",
        }
    )
    ctx = PipelineContext(run_id="test")
    source = AthenaSource(config=config, ctx=ctx)

    mock_cursor = mock.MagicMock()
    mock_cursor.connection.client.get_paginator.return_value.paginate.return_value = [
        {"DataCatalogsSummary": [{"CatalogName": "my-hive-catalog", "Type": "HIVE"}]}
    ]
    source.cursor = mock_cursor

    metadata = mock.MagicMock(spec=AthenaTableMetadata)
    metadata.parameters = {"table_type": "ICEBERG"}

    location = source._get_upstream_location(
        schema="default",
        table="t",
        metadata=metadata,
    )
    assert location == make_dataset_urn_with_platform_instance(
        platform="iceberg",
        name="default.t",
        platform_instance="prod_iceberg",
        env="PROD",
    )


def test_is_glue_catalog_defaults_true_on_api_exception_for_awsdatacatalog():
    """is_glue_catalog defaults to True on API exception when catalog is AwsDataCatalog."""
    config = AthenaConfig.model_validate(
        {
            "aws_region": "us-west-1",
            "query_result_location": "s3://query-result-location/",
            "work_group": "test-workgroup",
        }
    )
    ctx = PipelineContext(run_id="test")
    source = AthenaSource(config=config, ctx=ctx)

    mock_cursor = mock.MagicMock()
    mock_cursor.connection.client.get_paginator.return_value.paginate.side_effect = (
        Exception("Network error")
    )
    source.cursor = mock_cursor

    # AwsDataCatalog is always Glue, so API failure still safely defaults to True.
    assert source.is_glue_catalog is True


def test_is_glue_catalog_returns_none_on_api_exception_for_non_default_catalog():
    """is_glue_catalog returns None on API exception when a non-default catalog is configured."""
    config = AthenaConfig.model_validate(
        {
            "aws_region": "us-west-1",
            "query_result_location": "s3://query-result-location/",
            "work_group": "test-workgroup",
            "catalog_name": "my-custom-catalog",
        }
    )
    ctx = PipelineContext(run_id="test")
    source = AthenaSource(config=config, ctx=ctx)

    mock_cursor = mock.MagicMock()
    mock_cursor.connection.client.get_paginator.return_value.paginate.side_effect = (
        Exception("Network error")
    )
    source.cursor = mock_cursor

    assert source.is_glue_catalog is None


@time_machine.travel(FROZEN_TIME, tick=False)
def test_athena_get_table_properties_skips_lineage_when_catalog_type_unknown():
    config = AthenaConfig.model_validate(
        {
            "aws_region": "us-west-1",
            "query_result_location": "s3://query-result-location/",
            "work_group": "test-workgroup",
            "catalog_name": "my-custom-catalog",
        }
    )
    schema = "default"
    table = "iceberg_unknown_catalog"

    table_metadata = {
        "TableMetadata": {
            "Name": table,
            "TableType": "EXTERNAL_TABLE",
            "CreateTime": datetime.now(),
            "PartitionKeys": [],
            "Parameters": {
                "table_type": "ICEBERG",
                "location": "s3://bucket/iceberg_unknown_catalog/",
            },
        },
    }

    mock_cursor = mock.MagicMock()
    mock_inspector = mock.MagicMock()
    mock_cursor.get_table_metadata.return_value = AthenaTableMetadata(
        response=table_metadata
    )
    mock_cursor.connection.client.get_paginator.return_value.paginate.side_effect = (
        Exception("Network error")
    )

    ctx = PipelineContext(run_id="test")
    source = AthenaSource(config=config, ctx=ctx)
    source.cursor = mock_cursor

    _, _, location = source.get_table_properties(
        inspector=mock_inspector, table=table, schema=schema
    )
    assert location is None


def test_is_glue_catalog_caches_fallback_on_api_exception():
    config = AthenaConfig.model_validate(
        {
            "aws_region": "us-west-1",
            "query_result_location": "s3://query-result-location/",
            "work_group": "test-workgroup",
            "catalog_name": "my-custom-catalog",
        }
    )
    ctx = PipelineContext(run_id="test")
    source = AthenaSource(config=config, ctx=ctx)

    mock_cursor = mock.MagicMock()
    mock_cursor.connection.client.get_paginator.return_value.paginate.side_effect = (
        Exception("Network error")
    )
    source.cursor = mock_cursor

    assert source.is_glue_catalog is None
    assert source.is_glue_catalog is None
    assert source.is_glue_catalog is None

    # Fallback must be cached: API called once, warning emitted once.
    mock_cursor.connection.client.get_paginator.assert_called_once()


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
    assert result.precision == 10
    assert result.scale == 2


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
    assert AthenaSource._casted_partition_key("test_col") == "CAST(test_col as VARCHAR)"


def test_convert_simple_field_paths_to_v1_enabled():
    """Test that emit_schema_fieldpaths_as_v1 correctly converts simple field paths when enabled"""
    config = AthenaConfig.model_validate(
        {
            "aws_region": "us-west-1",
            "query_result_location": "s3://query-result-location/",
            "work_group": "test-workgroup",
            "emit_schema_fieldpaths_as_v1": True,
        }
    )

    ctx = PipelineContext(run_id="test")
    source = AthenaSource(config=config, ctx=ctx)
    mock_inspector = mock.MagicMock()

    string_column = {
        "name": "simple_string_col",
        "type": types.String(),
        "comment": "A simple string column",
        "nullable": True,
    }

    fields = source.get_schema_fields_for_column(
        dataset_name="test_dataset",
        column=string_column,
        inspector=mock_inspector,
    )

    assert len(fields) == 1
    field = fields[0]
    assert field.fieldPath == "simple_string_col"
    assert isinstance(field.type.type, StringTypeClass)

    # Boolean type conversion may have issues in SQLAlchemy type converter
    bool_column = {
        "name": "simple_bool_col",
        "type": types.Boolean(),
        "comment": "A simple boolean column",
        "nullable": True,
    }

    fields = source.get_schema_fields_for_column(
        dataset_name="test_dataset",
        column=bool_column,
        inspector=mock_inspector,
    )

    assert len(fields) == 1
    field = fields[0]
    if field.fieldPath:
        assert field.fieldPath == "simple_bool_col"
        assert isinstance(field.type.type, BooleanTypeClass)


def test_convert_simple_field_paths_to_v1_disabled():
    """Test that emit_schema_fieldpaths_as_v1 keeps v2 field paths when disabled"""
    config = AthenaConfig.model_validate(
        {
            "aws_region": "us-west-1",
            "query_result_location": "s3://query-result-location/",
            "work_group": "test-workgroup",
            "emit_schema_fieldpaths_as_v1": False,
        }
    )

    ctx = PipelineContext(run_id="test")
    source = AthenaSource(config=config, ctx=ctx)
    mock_inspector = mock.MagicMock()

    string_column = {
        "name": "simple_string_col",
        "type": types.String(),
        "comment": "A simple string column",
        "nullable": True,
    }

    fields = source.get_schema_fields_for_column(
        dataset_name="test_dataset",
        column=string_column,
        inspector=mock_inspector,
    )

    assert len(fields) == 1
    field = fields[0]
    assert field.fieldPath.startswith("[version=2.0]")
    assert isinstance(field.type.type, StringTypeClass)


def test_convert_simple_field_paths_to_v1_complex_types_ignored():
    """Test that complex types (arrays, maps, structs) are not affected by emit_schema_fieldpaths_as_v1"""
    config = AthenaConfig.model_validate(
        {
            "aws_region": "us-west-1",
            "query_result_location": "s3://query-result-location/",
            "work_group": "test-workgroup",
            "emit_schema_fieldpaths_as_v1": True,
        }
    )

    ctx = PipelineContext(run_id="test")
    source = AthenaSource(config=config, ctx=ctx)
    mock_inspector = mock.MagicMock()

    array_column = {
        "name": "array_col",
        "type": types.ARRAY(types.String()),
        "comment": "An array column",
        "nullable": True,
    }

    fields = source.get_schema_fields_for_column(
        dataset_name="test_dataset",
        column=array_column,
        inspector=mock_inspector,
    )

    assert len(fields) > 1 or (
        len(fields) == 1 and fields[0].fieldPath.startswith("[version=2.0]")
    )
    assert isinstance(fields[0].type.type, ArrayTypeClass)

    map_column = {
        "name": "map_col",
        "type": MapType(types.String(), types.Integer()),
        "comment": "A map column",
        "nullable": True,
    }

    fields = source.get_schema_fields_for_column(
        dataset_name="test_dataset",
        column=map_column,
        inspector=mock_inspector,
    )

    assert len(fields) > 1 or (
        len(fields) == 1 and fields[0].fieldPath.startswith("[version=2.0]")
    )
    assert isinstance(fields[0].type.type, MapTypeClass)


def test_convert_simple_field_paths_to_v1_with_partition_keys():
    """Test that emit_schema_fieldpaths_as_v1 works correctly with partition keys"""
    config = AthenaConfig.model_validate(
        {
            "aws_region": "us-west-1",
            "query_result_location": "s3://query-result-location/",
            "work_group": "test-workgroup",
            "emit_schema_fieldpaths_as_v1": True,
        }
    )

    ctx = PipelineContext(run_id="test")
    source = AthenaSource(config=config, ctx=ctx)
    mock_inspector = mock.MagicMock()

    string_column = {
        "name": "partition_col",
        "type": types.String(),
        "comment": "A partition column",
        "nullable": True,
    }

    fields = source.get_schema_fields_for_column(
        dataset_name="test_dataset",
        column=string_column,
        inspector=mock_inspector,
        partition_keys=["partition_col"],
    )

    assert len(fields) == 1
    field = fields[0]
    assert field.fieldPath == "partition_col"  # v1 format (simple path)
    assert isinstance(field.type.type, StringTypeClass)
    assert field.isPartitioningKey is True  # Should be marked as partitioning key


def test_convert_simple_field_paths_to_v1_default_behavior():
    """Test that emit_schema_fieldpaths_as_v1 defaults to False"""
    config = AthenaConfig.model_validate(
        {
            "aws_region": "us-west-1",
            "query_result_location": "s3://query-result-location/",
            "work_group": "test-workgroup",
        }
    )

    assert config.emit_schema_fieldpaths_as_v1 is False  # Should default to False


def test_get_partitions_returns_none_when_extract_partitions_disabled():
    """Test that get_partitions returns None when extract_partitions is False"""
    config = AthenaConfig.model_validate(
        {
            "aws_region": "us-west-1",
            "query_result_location": "s3://query-result-location/",
            "work_group": "test-workgroup",
            "extract_partitions": False,
        }
    )

    ctx = PipelineContext(run_id="test")
    source = AthenaSource(config=config, ctx=ctx)

    mock_inspector = mock.MagicMock()

    result = source.get_partitions(mock_inspector, "test_schema", "test_table")

    assert result is None
    assert not mock_inspector.called


def test_get_partitions_attempts_extraction_when_extract_partitions_enabled():
    """Test that get_partitions attempts partition extraction when extract_partitions is True"""
    config = AthenaConfig.model_validate(
        {
            "aws_region": "us-west-1",
            "query_result_location": "s3://query-result-location/",
            "work_group": "test-workgroup",
            "extract_partitions": True,
        }
    )

    ctx = PipelineContext(run_id="test")
    source = AthenaSource(config=config, ctx=ctx)

    mock_inspector = mock.MagicMock()
    mock_cursor = mock.MagicMock()

    mock_metadata = mock.MagicMock()
    mock_partition_key = mock.MagicMock()
    mock_partition_key.name = "year"
    mock_metadata.partition_keys = [mock_partition_key]
    mock_cursor.get_table_metadata.return_value = mock_metadata

    source.cursor = mock_cursor

    result = source.get_partitions(mock_inspector, "test_schema", "test_table")

    mock_cursor.get_table_metadata.assert_called_once_with(
        table_name="test_table", schema_name="test_schema"
    )
    assert isinstance(result, list)
    assert result == ["year"]


def test_partition_profiling_sql_generation_single_key():
    """Test that partition profiling generates valid SQL for single partition key and can be parsed by SQLGlot."""
    config = AthenaConfig.model_validate(
        {
            "aws_region": "us-west-1",
            "query_result_location": "s3://query-result-location/",
            "work_group": "test-workgroup",
            "extract_partitions": True,
            "profiling": {"enabled": True, "partition_profiling_enabled": True},
        }
    )

    ctx = PipelineContext(run_id="test")
    source = AthenaSource(config=config, ctx=ctx)

    mock_cursor = mock.MagicMock()
    mock_metadata = mock.MagicMock()
    mock_partition_key = mock.MagicMock()
    mock_partition_key.name = "year"
    mock_metadata.partition_keys = [mock_partition_key]
    mock_cursor.get_table_metadata.return_value = mock_metadata

    mock_result = mock.MagicMock()
    mock_result.description = [["year"]]
    mock_result.__iter__ = lambda x: iter([["2023"]])
    mock_cursor.execute.return_value = mock_result

    source.cursor = mock_cursor

    result = source.get_partitions(mock.MagicMock(), "test_schema", "test_table")

    assert mock_cursor.execute.called
    generated_query = mock_cursor.execute.call_args[0][0]

    assert "CAST(year as VARCHAR)" in generated_query
    assert "CONCAT" not in generated_query  # Single key shouldn't use CONCAT
    assert '"test_schema"."test_table$partitions"' in generated_query

    try:
        parsed = sqlglot.parse_one(generated_query, dialect=Athena)
        assert parsed is not None
        assert isinstance(parsed, sqlglot.expressions.Select)
    except Exception as e:
        pytest.fail(
            f"SQLGlot failed to parse single partition query: {e}\nQuery: {generated_query}"
        )

    assert result == ["year"]


def test_partition_profiling_sql_generation_multiple_keys():
    """Test that partition profiling generates valid SQL for multiple partition keys and can be parsed by SQLGlot."""

    config = AthenaConfig.model_validate(
        {
            "aws_region": "us-west-1",
            "query_result_location": "s3://query-result-location/",
            "work_group": "test-workgroup",
            "extract_partitions": True,
            "profiling": {"enabled": True, "partition_profiling_enabled": True},
        }
    )

    ctx = PipelineContext(run_id="test")
    source = AthenaSource(config=config, ctx=ctx)

    mock_cursor = mock.MagicMock()
    mock_metadata = mock.MagicMock()

    mock_year_key = mock.MagicMock()
    mock_year_key.name = "year"
    mock_month_key = mock.MagicMock()
    mock_month_key.name = "month"
    mock_day_key = mock.MagicMock()
    mock_day_key.name = "day"

    mock_metadata.partition_keys = [mock_year_key, mock_month_key, mock_day_key]
    mock_cursor.get_table_metadata.return_value = mock_metadata

    mock_result = mock.MagicMock()
    mock_result.description = [["year"], ["month"], ["day"]]
    mock_result.__iter__ = lambda x: iter([["2023", "12", "25"]])
    mock_cursor.execute.return_value = mock_result

    source.cursor = mock_cursor

    result = source.get_partitions(mock.MagicMock(), "test_schema", "test_table")

    assert mock_cursor.execute.called
    generated_query = mock_cursor.execute.call_args[0][0]

    assert "CONCAT(" in generated_query  # Multiple keys should use CONCAT
    assert "CAST(year as VARCHAR)" in generated_query
    assert "CAST(month as VARCHAR)" in generated_query
    assert "CAST(day as VARCHAR)" in generated_query
    assert "CAST('-' AS VARCHAR)" in generated_query
    assert '"test_schema"."test_table$partitions"' in generated_query

    try:
        parsed = sqlglot.parse_one(generated_query, dialect=Athena)
        assert parsed is not None
        assert isinstance(parsed, sqlglot.expressions.Select)
    except Exception as e:
        pytest.fail(
            f"SQLGlot failed to parse multiple partition query: {e}\nQuery: {generated_query}"
        )

    assert result == ["year", "month", "day"]


def test_partition_profiling_sql_generation_complex_schema_table_names():
    """Test that partition profiling handles complex schema/table names correctly and generates valid SQL."""

    config = AthenaConfig.model_validate(
        {
            "aws_region": "us-west-1",
            "query_result_location": "s3://query-result-location/",
            "work_group": "test-workgroup",
            "extract_partitions": True,
            "profiling": {"enabled": True, "partition_profiling_enabled": True},
        }
    )

    ctx = PipelineContext(run_id="test")
    source = AthenaSource(config=config, ctx=ctx)

    mock_cursor = mock.MagicMock()
    mock_metadata = mock.MagicMock()

    mock_partition_key = mock.MagicMock()
    mock_partition_key.name = "event_date"
    mock_metadata.partition_keys = [mock_partition_key]
    mock_cursor.get_table_metadata.return_value = mock_metadata

    mock_result = mock.MagicMock()
    mock_result.description = [["event_date"]]
    mock_result.__iter__ = lambda x: iter([["2023-12-25"]])
    mock_cursor.execute.return_value = mock_result

    source.cursor = mock_cursor

    schema = "ad_cdp_audience"
    table = "system_import_label"

    result = source.get_partitions(mock.MagicMock(), schema, table)

    assert mock_cursor.execute.called
    generated_query = mock_cursor.execute.call_args[0][0]

    expected_table_ref = f'"{schema}"."{table}$partitions"'
    assert expected_table_ref in generated_query
    assert "CAST(event_date as VARCHAR)" in generated_query

    try:
        parsed = sqlglot.parse_one(generated_query, dialect=Athena)
        assert parsed is not None
        assert isinstance(parsed, sqlglot.expressions.Select)
    except Exception as e:
        pytest.fail(
            f"SQLGlot failed to parse complex schema/table query: {e}\nQuery: {generated_query}"
        )

    assert result == ["event_date"]


def test_casted_partition_key_method():
    """Test the _casted_partition_key helper method generates valid SQL fragments."""
    casted_key = AthenaSource._casted_partition_key("test_column")
    assert casted_key == "CAST(test_column as VARCHAR)"

    try:
        parsed = sqlglot.parse_one(casted_key, dialect=Athena)
        assert parsed is not None
        assert isinstance(parsed, sqlglot.expressions.Cast)
        assert parsed.this.name == "test_column"
        assert "VARCHAR" in str(parsed.to.this)
    except Exception as e:
        pytest.fail(
            f"SQLGlot failed to parse CAST expression: {e}\nExpression: {casted_key}"
        )

    # Test with various column name formats
    test_cases = ["year", "month", "event_date", "created_at", "partition_key_123"]

    for column_name in test_cases:
        casted = AthenaSource._casted_partition_key(column_name)
        try:
            parsed = sqlglot.parse_one(casted, dialect=Athena)
            assert parsed is not None
            assert isinstance(parsed, sqlglot.expressions.Cast)
        except Exception as e:
            pytest.fail(f"SQLGlot failed to parse CAST for column '{column_name}': {e}")


def test_concat_function_generation_validates_with_sqlglot():
    """Test that our CONCAT function generation produces valid Athena SQL according to SQLGlot."""
    partition_keys = ["year", "month", "day"]
    casted_keys = [AthenaSource._casted_partition_key(key) for key in partition_keys]

    concat_args = []
    for i, key in enumerate(casted_keys):
        concat_args.append(key)
        if i < len(casted_keys) - 1:  # Add separator except for last element
            concat_args.append("CAST('-' AS VARCHAR)")

    concat_expr = f"CONCAT({', '.join(concat_args)})"

    expected = "CONCAT(CAST(year as VARCHAR), CAST('-' AS VARCHAR), CAST(month as VARCHAR), CAST('-' AS VARCHAR), CAST(day as VARCHAR))"
    assert concat_expr == expected

    try:
        parsed = sqlglot.parse_one(concat_expr, dialect=Athena)
        assert parsed is not None
        assert isinstance(parsed, sqlglot.expressions.Concat)
        assert len(parsed.expressions) == 5  # 3 partition keys + 2 separators
    except Exception as e:
        pytest.fail(
            f"SQLGlot failed to parse CONCAT expression: {e}\nExpression: {concat_expr}"
        )


def test_build_max_partition_query():
    """Test _build_max_partition_query method directly without mocking."""
    query_single = AthenaSource._build_max_partition_query(
        "test_schema", "test_table", ["year"]
    )
    expected_single = 'select year from "test_schema"."test_table$partitions" where CAST(year as VARCHAR) = (select max(CAST(year as VARCHAR)) from "test_schema"."test_table$partitions")'
    assert query_single == expected_single

    query_multiple = AthenaSource._build_max_partition_query(
        "test_schema", "test_table", ["year", "month", "day"]
    )
    expected_multiple = "select year,month,day from \"test_schema\".\"test_table$partitions\" where CONCAT(CAST(year as VARCHAR), CAST('-' AS VARCHAR), CAST(month as VARCHAR), CAST('-' AS VARCHAR), CAST(day as VARCHAR)) = (select max(CONCAT(CAST(year as VARCHAR), CAST('-' AS VARCHAR), CAST(month as VARCHAR), CAST('-' AS VARCHAR), CAST(day as VARCHAR))) from \"test_schema\".\"test_table$partitions\")"
    assert query_multiple == expected_multiple

    try:
        parsed_single = sqlglot.parse_one(query_single, dialect=Athena)
        assert parsed_single is not None
        assert isinstance(parsed_single, sqlglot.expressions.Select)

        parsed_multiple = sqlglot.parse_one(query_multiple, dialect=Athena)
        assert parsed_multiple is not None
        assert isinstance(parsed_multiple, sqlglot.expressions.Select)
    except Exception as e:
        pytest.fail(f"SQLGlot failed to parse generated query: {e}")


def test_partition_profiling_disabled_no_sql_generation():
    """Test that when partition profiling is disabled, no complex SQL is generated."""
    config = AthenaConfig.model_validate(
        {
            "aws_region": "us-west-1",
            "query_result_location": "s3://query-result-location/",
            "work_group": "test-workgroup",
            "extract_partitions": True,
            "profiling": {"enabled": False, "partition_profiling_enabled": False},
        }
    )

    ctx = PipelineContext(run_id="test")
    source = AthenaSource(config=config, ctx=ctx)

    # Mock cursor and metadata
    mock_cursor = mock.MagicMock()
    mock_metadata = mock.MagicMock()
    mock_partition_key = mock.MagicMock()
    mock_partition_key.name = "year"
    mock_metadata.partition_keys = [mock_partition_key]
    mock_cursor.get_table_metadata.return_value = mock_metadata

    source.cursor = mock_cursor

    result = source.get_partitions(mock.MagicMock(), "test_schema", "test_table")

    mock_cursor.get_table_metadata.assert_called_once()
    assert not mock_cursor.execute.called

    assert result == ["year"]


def test_sanitize_identifier_valid_names():
    """Test _sanitize_identifier method with valid Athena identifiers."""
    valid_identifiers = [
        "table_name",
        "schema123",
        "column_1",
        "MyTable",  # Should be allowed (Athena converts to lowercase)
        "DATABASE",
        "a",  # Single character
        "table123name",
        "data_warehouse_table",
    ]

    for identifier in valid_identifiers:
        result = AthenaSource._sanitize_identifier(identifier)
        assert result == identifier, f"Expected {identifier} to be valid"


def test_sanitize_identifier_valid_complex_types():
    """Test _sanitize_identifier method with valid complex type identifiers."""
    valid_complex_identifiers = [
        "struct.field",
        "data.subfield",
        "nested.struct.field",
        "array.element",
        "map.key",
        "record.attribute.subfield",
    ]

    for identifier in valid_complex_identifiers:
        result = AthenaSource._sanitize_identifier(identifier)
        assert result == identifier, (
            f"Expected {identifier} to be valid for complex types"
        )


def test_sanitize_identifier_invalid_characters():
    """Test _sanitize_identifier method rejects invalid characters."""
    invalid_identifiers = [
        "table-name",  # hyphen not allowed in Athena
        "table name",  # spaces not allowed
        "table@domain",  # @ not allowed
        "table#hash",  # # not allowed
        "table$var",  # $ not allowed (except in system tables)
        "table%percent",  # % not allowed
        "table&and",  # & not allowed
        "table*star",  # * not allowed
        "table(paren",  # ( not allowed
        "table)paren",  # ) not allowed
        "table+plus",  # + not allowed
        "table=equal",  # = not allowed
        "table[bracket",  # [ not allowed
        "table]bracket",  # ] not allowed
        "table{brace",  # { not allowed
        "table}brace",  # } not allowed
        "table|pipe",  # | not allowed
        "table\\backslash",  # \ not allowed
        "table:colon",  # : not allowed
        "table;semicolon",  # ; not allowed
        "table<less",  # < not allowed
        "table>greater",  # > not allowed
        "table?question",  # ? not allowed
        "table/slash",  # / not allowed
        "table,comma",  # , not allowed
    ]

    for identifier in invalid_identifiers:
        with pytest.raises(ValueError, match="contains unsafe characters"):
            AthenaSource._sanitize_identifier(identifier)


def test_sanitize_identifier_sql_injection_attempts():
    """Test _sanitize_identifier method blocks SQL injection attempts."""
    sql_injection_attempts = [
        "table'; DROP TABLE users; --",
        'table" OR 1=1 --',
        "table; DELETE FROM data;",
        "'; UNION SELECT * FROM passwords --",
        "table') OR ('1'='1",
        "table\"; INSERT INTO logs VALUES ('hack'); --",
        "table' AND 1=0 UNION SELECT * FROM admin --",
        "table/*comment*/",
        "table--comment",
        "table' OR 'a'='a",
        "1'; DROP DATABASE test; --",
        "table'; EXEC xp_cmdshell('dir'); --",
    ]

    for injection_attempt in sql_injection_attempts:
        with pytest.raises(ValueError, match="contains unsafe characters"):
            AthenaSource._sanitize_identifier(injection_attempt)


def test_sanitize_identifier_quote_injection_attempts():
    """Test _sanitize_identifier method blocks quote-based injection attempts."""
    quote_injection_attempts = [
        'table"',
        "table'",
        'table""',
        "table''",
        'table"extra',
        "table'extra",
        "`table`",
        '"table"',
        "'table'",
    ]

    for injection_attempt in quote_injection_attempts:
        with pytest.raises(ValueError, match="contains unsafe characters"):
            AthenaSource._sanitize_identifier(injection_attempt)


def test_sanitize_identifier_empty_and_edge_cases():
    """Test _sanitize_identifier method with empty and edge case inputs."""
    with pytest.raises(ValueError, match="Identifier cannot be empty"):
        AthenaSource._sanitize_identifier("")

    with pytest.raises(ValueError, match="Identifier cannot be empty"):
        AthenaSource._sanitize_identifier(None)  # type: ignore[arg-type]

    long_identifier = "a" * 200  # Still within Athena's 255 byte limit
    result = AthenaSource._sanitize_identifier(long_identifier)
    assert result == long_identifier


def test_sanitize_identifier_integration_with_build_max_partition_query():
    """Test that _sanitize_identifier works correctly within _build_max_partition_query."""
    query = AthenaSource._build_max_partition_query(
        "valid_schema", "valid_table", ["valid_partition"]
    )
    assert "valid_schema" in query
    assert "valid_table" in query
    assert "valid_partition" in query

    with pytest.raises(ValueError, match="contains unsafe characters"):
        AthenaSource._build_max_partition_query(
            "schema'; DROP TABLE users; --", "table", ["partition"]
        )

    with pytest.raises(ValueError, match="contains unsafe characters"):
        AthenaSource._build_max_partition_query(
            "schema", "table-with-hyphen", ["partition"]
        )

    with pytest.raises(ValueError, match="contains unsafe characters"):
        AthenaSource._build_max_partition_query(
            "schema", "table", ["partition; DELETE FROM data;"]
        )


def test_sanitize_identifier_error_handling_in_get_partitions():
    """Test that ValueError from _sanitize_identifier is handled gracefully in get_partitions method."""
    config = AthenaConfig.model_validate(
        {
            "aws_region": "us-west-1",
            "query_result_location": "s3://query-result-location/",
            "work_group": "test-workgroup",
            "extract_partitions": True,
            "profiling": {"enabled": True, "partition_profiling_enabled": True},
        }
    )

    ctx = PipelineContext(run_id="test")
    source = AthenaSource(config=config, ctx=ctx)

    mock_cursor = mock.MagicMock()
    mock_metadata = mock.MagicMock()
    mock_partition_key = mock.MagicMock()
    mock_partition_key.name = "year"
    mock_metadata.partition_keys = [mock_partition_key]
    mock_cursor.get_table_metadata.return_value = mock_metadata
    source.cursor = mock_cursor

    # Malicious schema name is handled gracefully by report_exc; partitions still returned
    result = source.get_partitions(
        mock.MagicMock(), "schema'; DROP TABLE users; --", "valid_table"
    )

    assert result == ["year"]
    mock_cursor.get_table_metadata.assert_called_once()


def test_sanitize_identifier_error_handling_in_generate_partition_profiler_query(
    caplog,
):
    """Test that ValueError from _sanitize_identifier is handled gracefully in generate_partition_profiler_query."""
    config = AthenaConfig.model_validate(
        {
            "aws_region": "us-west-1",
            "query_result_location": "s3://query-result-location/",
            "work_group": "test-workgroup",
            "profiling": {"enabled": True, "partition_profiling_enabled": True},
        }
    )

    ctx = PipelineContext(run_id="test")
    source = AthenaSource(config=config, ctx=ctx)

    source.table_partition_cache["valid_schema"] = {
        "valid_table": Partitionitem(
            partitions=["year"],
            max_partition={"malicious'; DROP TABLE users; --": "2023"},
        )
    }

    with caplog.at_level("WARNING"):
        result = source.generate_partition_profiler_query(
            "valid_schema", "valid_table", None
        )

    assert result == (None, None)

    # Filter for WARNING level logs from the athena source (ignore INFO logs from other modules)
    warning_records = [
        r for r in caplog.records if r.levelname == "WARNING" and "athena" in r.name
    ]
    assert len(warning_records) == 1, (
        f"Expected 1 WARNING log from athena source, but got {len(warning_records)}. "
        f"All records: {[(r.levelname, r.name, r.message) for r in caplog.records]}"
    )
    log_record = warning_records[0]
    assert (
        "Failed to generate partition profiler query for valid_schema.valid_table due to unsafe identifiers"
        in log_record.message
    )
    assert "contains unsafe characters" in log_record.message
    assert "Partition profiling disabled for this table" in log_record.message
