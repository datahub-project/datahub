import deepdiff
import pytest

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.sql.hive.hive_source import HiveConfig, HiveSource
from datahub.ingestion.source.sql.hive.storage_lineage import (
    HiveStorageLineageConfigMixin,
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
    hive_config = HiveConfig.model_validate(config_dict)
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


def test_hive_source_storage_lineage_config_default():
    """Test HiveConfig storage lineage configuration defaults"""
    config_dict = {
        "username": "test_user",
        "password": "test_password",
        "host_port": "localhost:10000",
    }
    config = HiveConfig.model_validate(config_dict)

    assert config.emit_storage_lineage is False
    assert config.hive_storage_lineage_direction == "upstream"
    assert config.include_column_lineage is True


def test_hive_source_storage_lineage_config_enabled():
    """Test HiveConfig with storage lineage enabled"""
    config_dict = {
        "username": "test_user",
        "password": "test_password",
        "host_port": "localhost:10000",
        "emit_storage_lineage": True,
        "hive_storage_lineage_direction": "downstream",
        "include_column_lineage": False,
        "storage_platform_instance": "prod-hdfs",
    }
    config = HiveConfig.model_validate(config_dict)

    assert config.emit_storage_lineage is True
    assert config.hive_storage_lineage_direction == "downstream"
    assert config.include_column_lineage is False
    assert config.storage_platform_instance == "prod-hdfs"

    # Config inherits from HiveStorageLineageConfigMixin
    assert isinstance(config, HiveStorageLineageConfigMixin)


def test_hive_source_storage_lineage_direction_validation():
    """Test that invalid storage lineage direction raises ValueError"""
    config_dict = {
        "username": "test_user",
        "password": "test_password",
        "host_port": "localhost:10000",
        "emit_storage_lineage": True,
        "hive_storage_lineage_direction": "sideways",
    }

    with pytest.raises(ValueError) as exc_info:
        HiveConfig.model_validate(config_dict)

    # Check for key parts of error message (may have single or double quotes)
    assert "upstream" in str(exc_info.value) and "downstream" in str(exc_info.value)


def test_hive_source_initialization_with_storage_lineage():
    """Test HiveSource initialization with storage lineage"""
    config_dict = {
        "username": "test_user",
        "password": "test_password",
        "host_port": "localhost:10000",
        "emit_storage_lineage": True,
        "hive_storage_lineage_direction": "upstream",
        "include_column_lineage": True,
    }
    config = HiveConfig.model_validate(config_dict)
    ctx = PipelineContext(run_id="test-run")

    source = HiveSource(config, ctx)

    assert source.storage_lineage is not None
    assert source.storage_lineage.config.emit_storage_lineage is True
    assert source.storage_lineage.config.hive_storage_lineage_direction == "upstream"
    assert source.storage_lineage.config.include_column_lineage is True


def test_hive_source_view_lineage_config():
    """Test that HiveSource inherits view lineage support from base class"""
    config_dict = {
        "username": "test_user",
        "password": "test_password",
        "host_port": "localhost:10000",
        "include_view_lineage": True,
    }
    config = HiveConfig.model_validate(config_dict)
    ctx = PipelineContext(run_id="test-run")

    source = HiveSource(config, ctx)

    # Verify aggregator is initialized (inherited from SQLAlchemySource)
    assert hasattr(source, "aggregator")
    assert source.aggregator is not None
    assert source.config.include_view_lineage is True


def test_hive_source_view_lineage_disabled():
    """Test HiveSource with view lineage disabled"""
    config_dict = {
        "username": "test_user",
        "password": "test_password",
        "host_port": "localhost:10000",
        "include_view_lineage": False,
    }
    config = HiveConfig.model_validate(config_dict)
    ctx = PipelineContext(run_id="test-run")

    source = HiveSource(config, ctx)

    assert source.config.include_view_lineage is False


def test_hive_source_get_db_schema():
    """Test HiveSource get_db_schema method"""
    config_dict = {
        "username": "test_user",
        "password": "test_password",
        "host_port": "localhost:10000",
    }
    config = HiveConfig.model_validate(config_dict)
    ctx = PipelineContext(run_id="test-run")

    source = HiveSource(config, ctx)

    # Test two-tier format: schema.table
    default_db, default_schema = source.get_db_schema("my_schema.my_view")

    assert default_db is None
    assert default_schema == "my_schema"


def test_hive_source_combined_lineage_config():
    """Test HiveSource with both storage and view lineage enabled"""
    config_dict = {
        "username": "test_user",
        "password": "test_password",
        "host_port": "localhost:10000",
        "emit_storage_lineage": True,
        "hive_storage_lineage_direction": "upstream",
        "include_column_lineage": True,
        "include_view_lineage": True,
        "include_view_column_lineage": True,
        "storage_platform_instance": "prod-s3",
        "env": "PROD",
    }
    config = HiveConfig.model_validate(config_dict)
    ctx = PipelineContext(run_id="test-run")

    source = HiveSource(config, ctx)

    # Verify both storage lineage and view lineage are configured
    assert source.storage_lineage is not None
    assert source.storage_lineage.config.emit_storage_lineage is True
    assert source.storage_lineage.config.include_column_lineage is True

    assert source.aggregator is not None
    assert source.config.include_view_lineage is True
    assert source.config.include_view_column_lineage is True


def test_hive_source_all_storage_platforms():
    """Test HiveConfig with different storage platform types"""
    platforms = ["s3", "hdfs", "adls", "gcs", "dbfs"]

    for platform in platforms:
        config_dict = {
            "username": "test_user",
            "password": "test_password",
            "host_port": "localhost:10000",
            "emit_storage_lineage": True,
            "storage_platform_instance": f"{platform}-prod",
        }
        config = HiveConfig.model_validate(config_dict)

        # Config inherits from HiveStorageLineageConfigMixin
        assert config.storage_platform_instance == f"{platform}-prod"
