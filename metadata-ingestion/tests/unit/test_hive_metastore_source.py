from unittest.mock import patch

import pytest

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.sql.hive.hive_metastore_source import (
    HiveMetastore,
    HiveMetastoreSource,
)
from datahub.ingestion.source.sql.hive.storage_lineage import HiveStorageLineageConfig


def test_hive_metastore_configuration_basic():
    """Test basic HiveMetastore configuration"""
    config_dict = {
        "username": "test_user",
        "password": "test_password",
        "host_port": "localhost:3306",
        "database": "test_db",
    }
    config = HiveMetastore.model_validate(config_dict)

    assert config.username == "test_user"
    assert config.host_port == "localhost:3306"
    assert config.database == "test_db"
    assert config.emit_storage_lineage is False  # default value
    assert config.hive_storage_lineage_direction == "upstream"  # default value
    assert config.include_column_lineage is True  # default value


def test_hive_metastore_storage_lineage_config():
    """Test HiveMetastore storage lineage configuration"""
    config_dict = {
        "username": "test_user",
        "password": "test_password",
        "host_port": "localhost:3306",
        "database": "test_db",
        "emit_storage_lineage": True,
        "hive_storage_lineage_direction": "downstream",
        "include_column_lineage": False,
        "storage_platform_instance": "prod-cluster",
    }
    config = HiveMetastore.model_validate(config_dict)

    assert config.emit_storage_lineage is True
    assert config.hive_storage_lineage_direction == "downstream"
    assert config.include_column_lineage is False
    assert config.storage_platform_instance == "prod-cluster"

    # Test get_storage_lineage_config method
    storage_config = config.get_storage_lineage_config()
    assert isinstance(storage_config, HiveStorageLineageConfig)
    assert storage_config.emit_storage_lineage is True
    assert storage_config.hive_storage_lineage_direction == "downstream"
    assert storage_config.include_column_lineage is False
    assert storage_config.storage_platform_instance == "prod-cluster"


def test_hive_metastore_storage_lineage_direction_validation():
    """Test that invalid storage lineage direction raises ValueError"""
    config_dict = {
        "username": "test_user",
        "password": "test_password",
        "host_port": "localhost:3306",
        "database": "test_db",
        "emit_storage_lineage": True,
        "hive_storage_lineage_direction": "invalid_direction",
    }

    # HiveMetastore doesn't validate this, but HiveStorageLineageConfig should
    config = HiveMetastore.model_validate(config_dict)

    with pytest.raises(ValueError) as exc_info:
        config.get_storage_lineage_config()

    assert "must be either upstream or downstream" in str(exc_info.value)


def test_hive_metastore_source_initialization():
    """Test HiveMetastoreSource initialization with storage lineage"""
    config_dict = {
        "username": "test_user",
        "password": "test_password",
        "host_port": "localhost:3306",
        "database": "test_db",
        "emit_storage_lineage": True,
    }
    config = HiveMetastore.model_validate(config_dict)
    ctx = PipelineContext(run_id="test-run")

    with patch(
        "datahub.ingestion.source.sql.hive.hive_metastore_source.SQLAlchemyClient"
    ):
        source = HiveMetastoreSource(config, ctx)

        assert source.config == config
        assert source.storage_lineage is not None
        assert source.storage_lineage.config.emit_storage_lineage is True


@patch("datahub.ingestion.source.sql.hive.hive_metastore_source.SQLAlchemyClient")
def test_hive_metastore_source_with_storage_lineage_disabled(mock_client):
    """Test that storage lineage is not emitted when disabled"""
    config_dict = {
        "username": "test_user",
        "password": "test_password",
        "host_port": "localhost:3306",
        "database": "test_db",
        "emit_storage_lineage": False,
    }
    config = HiveMetastore.model_validate(config_dict)
    ctx = PipelineContext(run_id="test-run")

    source = HiveMetastoreSource(config, ctx)

    # Mock table with location
    table_dict = {"StorageDescriptor": {"Location": "s3://my-bucket/path/to/table"}}

    # When emit_storage_lineage is False, get_lineage_mcp should return nothing
    lineage_mcps = list(
        source.storage_lineage.get_lineage_mcp(
            dataset_urn="urn:li:dataset:(urn:li:dataPlatform:hive,test_table,PROD)",
            table=table_dict,
        )
    )

    assert len(lineage_mcps) == 0


def test_storage_lineage_config_upstream_direction():
    """Test HiveStorageLineageConfig with upstream direction"""
    config = HiveStorageLineageConfig(
        emit_storage_lineage=True,
        hive_storage_lineage_direction="upstream",
        include_column_lineage=True,
        storage_platform_instance=None,
    )

    assert config.emit_storage_lineage is True
    assert config.hive_storage_lineage_direction == "upstream"
    assert config.include_column_lineage is True


def test_storage_lineage_config_downstream_direction():
    """Test HiveStorageLineageConfig with downstream direction"""
    config = HiveStorageLineageConfig(
        emit_storage_lineage=True,
        hive_storage_lineage_direction="downstream",
        include_column_lineage=False,
        storage_platform_instance="prod",
    )

    assert config.emit_storage_lineage is True
    assert config.hive_storage_lineage_direction == "downstream"
    assert config.include_column_lineage is False
    assert config.storage_platform_instance == "prod"


def test_storage_lineage_config_invalid_direction():
    """Test that HiveStorageLineageConfig raises ValueError for invalid direction"""
    with pytest.raises(ValueError) as exc_info:
        HiveStorageLineageConfig(
            emit_storage_lineage=True,
            hive_storage_lineage_direction="sideways",
            include_column_lineage=True,
            storage_platform_instance=None,
        )

    assert "must be either upstream or downstream" in str(exc_info.value)


def test_storage_lineage_config_case_insensitive():
    """Test that direction is case insensitive"""
    config1 = HiveStorageLineageConfig(
        emit_storage_lineage=True,
        hive_storage_lineage_direction="UPSTREAM",
        include_column_lineage=True,
        storage_platform_instance=None,
    )

    config2 = HiveStorageLineageConfig(
        emit_storage_lineage=True,
        hive_storage_lineage_direction="Downstream",
        include_column_lineage=True,
        storage_platform_instance=None,
    )

    assert config1.hive_storage_lineage_direction == "upstream"
    assert config2.hive_storage_lineage_direction == "downstream"


def test_hive_metastore_mode_configuration():
    """Test different mode configurations"""
    config_dict = {
        "username": "test_user",
        "password": "test_password",
        "host_port": "localhost:3306",
        "database": "test_db",
        "mode": "presto-on-hive",
    }
    config = HiveMetastore.model_validate(config_dict)

    assert config.mode.value == "presto-on-hive"


def test_hive_metastore_with_catalog_name_in_ids():
    """Test include_catalog_name_in_ids configuration"""
    config_dict = {
        "username": "test_user",
        "password": "test_password",
        "host_port": "localhost:3306",
        "database": "test_db",
        "include_catalog_name_in_ids": True,
    }
    config = HiveMetastore.model_validate(config_dict)

    assert config.include_catalog_name_in_ids is True


def test_hive_metastore_simplify_nested_field_paths():
    """Test simplify_nested_field_paths configuration"""
    config_dict = {
        "username": "test_user",
        "password": "test_password",
        "host_port": "localhost:3306",
        "database": "test_db",
        "simplify_nested_field_paths": True,
    }
    config = HiveMetastore.model_validate(config_dict)

    assert config.simplify_nested_field_paths is True


@patch("datahub.ingestion.source.sql.hive.hive_metastore_source.SQLAlchemyClient")
def test_hive_metastore_source_storage_lineage_integration(mock_client):
    """Test that storage lineage is properly initialized in source"""
    config_dict = {
        "username": "test_user",
        "password": "test_password",
        "host_port": "localhost:3306",
        "database": "test_db",
        "emit_storage_lineage": True,
        "hive_storage_lineage_direction": "upstream",
        "include_column_lineage": True,
        "storage_platform_instance": "prod",
        "env": "PROD",
        "convert_urns_to_lowercase": True,
    }
    config = HiveMetastore.model_validate(config_dict)
    ctx = PipelineContext(run_id="test-run")

    source = HiveMetastoreSource(config, ctx)

    # Verify storage_lineage is properly initialized
    assert source.storage_lineage is not None
    assert source.storage_lineage.config.emit_storage_lineage is True
    assert source.storage_lineage.config.hive_storage_lineage_direction == "upstream"
    assert source.storage_lineage.config.include_column_lineage is True
    assert source.storage_lineage.env == "PROD"
    assert source.storage_lineage.convert_urns_to_lowercase is True


def test_hive_metastore_all_storage_platforms():
    """Test configuration with all storage platform types"""
    platforms = ["s3", "adls", "gcs", "dbfs", "hdfs"]

    for platform in platforms:
        config_dict = {
            "username": "test_user",
            "password": "test_password",
            "host_port": "localhost:3306",
            "database": "test_db",
            "emit_storage_lineage": True,
            "storage_platform_instance": f"{platform}-prod",
        }
        config = HiveMetastore.model_validate(config_dict)

        storage_config = config.get_storage_lineage_config()
        assert storage_config.storage_platform_instance == f"{platform}-prod"


def test_hive_metastore_view_lineage_config_default():
    """Test that include_view_lineage is enabled by default"""
    config_dict = {
        "username": "test_user",
        "password": "test_password",
        "host_port": "localhost:3306",
        "database": "test_db",
    }
    config = HiveMetastore.model_validate(config_dict)

    assert config.include_view_lineage is True


def test_hive_metastore_view_lineage_config_disabled():
    """Test that include_view_lineage can be disabled"""
    config_dict = {
        "username": "test_user",
        "password": "test_password",
        "host_port": "localhost:3306",
        "database": "test_db",
        "include_view_lineage": False,
    }
    config = HiveMetastore.model_validate(config_dict)

    assert config.include_view_lineage is False


@patch("datahub.ingestion.source.sql.hive.hive_metastore_source.SQLAlchemyClient")
def test_hive_metastore_get_db_schema_without_catalog(mock_client):
    """Test get_db_schema parsing for schema.table format"""
    config_dict = {
        "username": "test_user",
        "password": "test_password",
        "host_port": "localhost:3306",
        "database": "test_db",
        "include_catalog_name_in_ids": False,
    }
    config = HiveMetastore.model_validate(config_dict)
    ctx = PipelineContext(run_id="test-run")

    source = HiveMetastoreSource(config, ctx)

    default_db, default_schema = source.get_db_schema("my_schema.my_view")

    assert default_db is None
    assert default_schema == "my_schema"


@patch("datahub.ingestion.source.sql.hive.hive_metastore_source.SQLAlchemyClient")
def test_hive_metastore_get_db_schema_with_catalog(mock_client):
    """Test get_db_schema parsing for catalog.schema.table format"""
    config_dict = {
        "username": "test_user",
        "password": "test_password",
        "host_port": "localhost:3306",
        "database": "test_db",
        "include_catalog_name_in_ids": True,
    }
    config = HiveMetastore.model_validate(config_dict)
    ctx = PipelineContext(run_id="test-run")

    source = HiveMetastoreSource(config, ctx)

    default_db, default_schema = source.get_db_schema("my_catalog.my_schema.my_view")

    assert default_db == "my_catalog"
    assert default_schema == "my_schema"


@patch("datahub.ingestion.source.sql.hive.hive_metastore_source.SQLAlchemyClient")
def test_hive_metastore_get_db_schema_single_part(mock_client):
    """Test get_db_schema parsing for single-part identifiers"""
    config_dict = {
        "username": "test_user",
        "password": "test_password",
        "host_port": "localhost:3306",
        "database": "test_db",
    }
    config = HiveMetastore.model_validate(config_dict)
    ctx = PipelineContext(run_id="test-run")

    source = HiveMetastoreSource(config, ctx)

    default_db, default_schema = source.get_db_schema("my_view")

    assert default_db is None
    assert default_schema == "my_view"


@patch("datahub.ingestion.source.sql.hive.hive_metastore_source.SQLAlchemyClient")
def test_hive_metastore_aggregator_initialization(mock_client):
    """Test that SQL aggregator is initialized for view lineage"""
    config_dict = {
        "username": "test_user",
        "password": "test_password",
        "host_port": "localhost:3306",
        "database": "test_db",
        "include_view_lineage": True,
    }
    config = HiveMetastore.model_validate(config_dict)
    ctx = PipelineContext(run_id="test-run")

    source = HiveMetastoreSource(config, ctx)

    # Verify aggregator is initialized (inherited from SQLAlchemySource)
    assert hasattr(source, "aggregator")
    assert source.aggregator is not None
    assert source.config.include_view_lineage is True
