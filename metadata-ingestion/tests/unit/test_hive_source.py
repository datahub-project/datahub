import deepdiff

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.sql.hive import (
    HiveConfig,
    HiveSource,
    HiveStorageLineage,
    HiveStorageLineageConfig,
    StoragePathParser,
    StoragePlatform,
)
from datahub.utilities.hive_schema_to_avro import get_avro_schema_for_hive_column


def test_hive_configuration_get_identifier_with_database():
    test_db_name = "test_database"
    # test_table_name = "test_table"
    config_dict = {
        "username": "test",
        "password": "test",
        "host_port": "test:80",
        "database": test_db_name,
        "scheme": "hive+https",
    }
    hive_config = HiveConfig.parse_obj(config_dict)
    expected_output = f"{test_db_name}"
    ctx = PipelineContext(run_id="test")
    hive_source = HiveSource(hive_config, ctx)
    output = HiveSource.get_schema_names(hive_source, hive_config)
    assert output == [expected_output]


def test_hive_configuration_get_avro_schema_from_native_data_type():
    # Test 3  - struct of struct
    datatype_string = "struct<type:string,provider:array<int>,abc:struct<t1:string>>"
    output = get_avro_schema_for_hive_column("service", datatype_string)
    diff = deepdiff.DeepDiff(
        (
            {
                "type": "record",
                "native_data_type": "struct<type:string,provider:array<int>,abc:struct<t1:string>>",
                "name": "__struct_fa089c000053479b8d73496a2d95af64",
                "fields": [
                    {
                        "name": "type",
                        "type": {
                            "type": "string",
                            "native_data_type": "string",
                            "_nullable": True,
                        },
                    },
                    {
                        "name": "provider",
                        "type": {
                            "type": "array",
                            "native_data_type": "array<int>",
                            "items": {
                                "type": "int",
                                "native_data_type": "int",
                                "_nullable": True,
                            },
                        },
                    },
                    {
                        "name": "abc",
                        "type": {
                            "type": "record",
                            "native_data_type": "struct<t1:string>",
                            "name": "__struct_0a5925decc1743a09f9a7f7fc7a7efe6",
                            "fields": [
                                {
                                    "name": "t1",
                                    "type": {
                                        "type": "string",
                                        "native_data_type": "string",
                                        "_nullable": True,
                                    },
                                }
                            ],
                        },
                    },
                ],
            }
        ),
        output["fields"][0]["type"],  # type: ignore
        exclude_regex_paths=[
            r"root\['name'\]",
            r"root\['fields'\]\[2\]\['type'\]\['name'\]",
        ],
    )

    assert diff == {}


def test_storage_path_parser():
    # Test S3 path
    s3_path = "s3://my-bucket/path/to/file"
    result = StoragePathParser.parse_storage_location(s3_path)
    assert result is not None
    platform, path = result
    assert platform == StoragePlatform.S3
    assert path == "/my-bucket/path/to/file"

    # Test Azure path
    azure_path = "abfss://container@account.dfs.core.windows.net/path/to/file"
    result = StoragePathParser.parse_storage_location(azure_path)
    assert result is not None
    platform, path = result
    assert platform == StoragePlatform.AZURE
    assert path == "/container/path/to/file"

    # Test local path
    local_path = "/path/to/local/file"
    result = StoragePathParser.parse_storage_location(local_path)
    assert result is not None
    platform, path = result
    assert platform == StoragePlatform.LOCAL
    assert path == "/path/to/local/file"

    # Test invalid path
    invalid_path = "invalid://path"
    result = StoragePathParser.parse_storage_location(invalid_path)
    assert result is None


def test_storage_lineage_config():
    # Test valid configuration
    config = HiveStorageLineageConfig(
        emit_storage_lineage=True,
        hive_storage_lineage_direction="upstream",
        include_column_lineage=True,
        storage_platform_instance="test_instance",
    )
    assert config.emit_storage_lineage is True
    assert config.hive_storage_lineage_direction == "upstream"
    assert config.include_column_lineage is True
    assert config.storage_platform_instance == "test_instance"

    # Test invalid direction
    try:
        HiveStorageLineageConfig(
            emit_storage_lineage=True,
            hive_storage_lineage_direction="invalid",
            include_column_lineage=True,
            storage_platform_instance=None,
        )
        raise AssertionError("Should have raised ValueError")
    except ValueError:
        pass


def test_hive_config_storage_lineage():
    # Test valid configuration
    config_dict = {
        "username": "test",
        "password": "test",
        "host_port": "test:80",
        "emit_storage_lineage": True,
        "hive_storage_lineage_direction": "upstream",
        "include_column_lineage": True,
        "storage_platform_instance": "test_instance",
    }
    config = HiveConfig.parse_obj(config_dict)
    assert config.emit_storage_lineage is True
    assert config.hive_storage_lineage_direction == "upstream"
    assert config.include_column_lineage is True
    assert config.storage_platform_instance == "test_instance"

    # Test invalid direction
    try:
        HiveConfig.parse_obj(
            {
                "username": "test",
                "password": "test",
                "host_port": "test:80",
                "emit_storage_lineage": True,
                "hive_storage_lineage_direction": "invalid",
                "include_column_lineage": True,
            }
        )
        raise AssertionError("Should have raised ValueError")
    except ValueError:
        pass


def test_storage_lineage_case_preservation():
    """Test that platform names and instances preserve their case."""
    # Test with mixed case platform instance
    config = HiveStorageLineageConfig(
        emit_storage_lineage=True,
        hive_storage_lineage_direction="upstream",
        include_column_lineage=True,
        storage_platform_instance="TestInstance",
    )

    # Create storage lineage handler with convert_urns_to_lowercase=False
    storage_lineage = HiveStorageLineage(
        config=config,
        env="PROD",
        convert_urns_to_lowercase=False,
    )

    # Test S3 path with mixed case
    s3_path = "s3://My-Bucket/path/to/file"
    storage_info = storage_lineage._make_storage_dataset_urn(s3_path)
    assert storage_info is not None
    storage_urn, platform_name = storage_info

    # Verify platform instance is preserved in URN
    assert "TestInstance" in storage_urn

    # Test with convert_urns_to_lowercase=True
    storage_lineage_lowercase = HiveStorageLineage(
        config=config,
        env="PROD",
        convert_urns_to_lowercase=True,
    )

    storage_info = storage_lineage_lowercase._make_storage_dataset_urn(s3_path)
    assert storage_info is not None
    storage_urn, platform_name = storage_info

    # Verify platform instance is not converted to lowercase when convert_urns_to_lowercase=True
    assert "TestInstance" in storage_urn
