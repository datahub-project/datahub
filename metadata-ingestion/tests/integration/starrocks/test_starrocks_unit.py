import pytest

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.sql.mysql import MySQLConfig
from datahub.ingestion.source.sql.starrocks import StarRocksSource


@pytest.mark.integration
def test_starrocks_source_config():
    """Test that StarRocks source can be configured properly"""
    config_dict = {
        "host_port": "localhost:59030",
        "database": "test_db",
        "username": "root",
        "password": "example",
    }

    # Test that config can be validated
    config = MySQLConfig.model_validate(config_dict)
    assert config is not None
    assert config.host_port == "localhost:59030"
    assert config.username == "root"
    assert config.database == "test_db"

    # Test that source can be instantiated with config
    ctx = PipelineContext(run_id="test-run")
    source = StarRocksSource(config, ctx)
    assert source is not None
    assert source.get_platform() == "starrocks"


@pytest.mark.integration
def test_starrocks_source_inherits_mysql():
    """Test that StarRocks source properly inherits from MySQL source"""
    from datahub.ingestion.source.sql.mysql import MySQLSource

    # Verify inheritance
    assert issubclass(StarRocksSource, MySQLSource)

    # Test platform override
    config_dict = {
        "host_port": "localhost:59030",
        "database": "test_db",
        "username": "root",
        "password": "example",
    }

    config = MySQLConfig.model_validate(config_dict)
    ctx = PipelineContext(run_id="test-run")
    source = StarRocksSource(config, ctx)
    assert source.get_platform() == "starrocks"  # Not "mysql"


@pytest.mark.integration
def test_starrocks_source_platform_name():
    """Test that StarRocks source has correct platform name decorator"""
    # Check platform name is correctly set via decorator
    assert hasattr(StarRocksSource, "__qualname__")

    config_dict = {
        "host_port": "localhost:59030",
        "database": "test_db",
        "username": "root",
        "password": "example",
    }

    config = MySQLConfig.model_validate(config_dict)
    ctx = PipelineContext(run_id="test-run")
    source = StarRocksSource(config, ctx)

    # Most important test: platform should be "starrocks"
    assert source.get_platform() == "starrocks"
