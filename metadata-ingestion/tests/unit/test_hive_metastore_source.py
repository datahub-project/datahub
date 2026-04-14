from unittest.mock import patch

import pytest

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.common.subtypes import DatasetSubTypes
from datahub.ingestion.source.sql.hive.exceptions import InvalidDatasetIdentifierError
from datahub.ingestion.source.sql.hive.hive_metastore_source import (
    HiveMetastore,
    HiveMetastoreSource,
)
from datahub.ingestion.source.sql.hive.storage_lineage import (
    HiveStorageLineageConfigMixin,
    LineageDirection,
)


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
    assert config.emit_storage_lineage is False
    assert config.hive_storage_lineage_direction == "upstream"
    assert config.include_column_lineage is True


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

    # Config inherits from HiveStorageLineageConfigMixin, so it has all storage lineage fields
    assert isinstance(config, HiveStorageLineageConfigMixin)


def test_hive_metastore_storage_lineage_direction_validation():
    """Test that invalid storage lineage direction raises ValueError at config level"""
    config_dict = {
        "username": "test_user",
        "password": "test_password",
        "host_port": "localhost:3306",
        "database": "test_db",
        "emit_storage_lineage": True,
        "hive_storage_lineage_direction": "invalid_direction",
    }

    with pytest.raises(ValueError) as exc_info:
        HiveMetastore.model_validate(config_dict)

    assert "upstream" in str(exc_info.value) and "downstream" in str(exc_info.value)


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

    with patch("datahub.ingestion.source.sql.hive.hive_sql_fetcher.SQLAlchemyClient"):
        source = HiveMetastoreSource(config, ctx)

        assert source.config == config
        # Storage lineage config is on the source config
        assert source.config.emit_storage_lineage is True


@patch("datahub.ingestion.source.sql.hive.hive_sql_fetcher.SQLAlchemyClient")
def test_hive_metastore_source_with_storage_lineage_disabled(mock_client):
    """Test that storage lineage is not emitted when disabled"""
    from datahub.ingestion.source.sql.hive.storage_lineage import HiveStorageLineage

    config_dict = {
        "username": "test_user",
        "password": "test_password",
        "host_port": "localhost:3306",
        "database": "test_db",
        "emit_storage_lineage": False,
    }
    config = HiveMetastore.model_validate(config_dict)

    # Test storage lineage directly
    storage_lineage = HiveStorageLineage(config=config, env=config.env)

    table_dict = {"StorageDescriptor": {"Location": "s3://my-bucket/path/to/table"}}

    lineage_mcps = list(
        storage_lineage.get_lineage_mcp(
            dataset_urn="urn:li:dataset:(urn:li:dataPlatform:hive,test_table,PROD)",
            table=table_dict,
        )
    )

    assert len(lineage_mcps) == 0


def test_storage_lineage_config_upstream_direction():
    """Test HiveStorageLineageConfigMixin with upstream direction"""
    config = HiveStorageLineageConfigMixin(
        emit_storage_lineage=True,
        hive_storage_lineage_direction="upstream",
        include_column_lineage=True,
        storage_platform_instance=None,
    )

    assert config.emit_storage_lineage is True
    assert config.hive_storage_lineage_direction == "upstream"
    assert config.include_column_lineage is True


def test_storage_lineage_config_downstream_direction():
    """Test HiveStorageLineageConfigMixin with downstream direction"""
    config = HiveStorageLineageConfigMixin(
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
    """Test that HiveStorageLineageConfigMixin raises ValueError for invalid direction"""
    with pytest.raises(ValueError) as exc_info:
        HiveStorageLineageConfigMixin(
            emit_storage_lineage=True,
            hive_storage_lineage_direction="sideways",
            include_column_lineage=True,
            storage_platform_instance=None,
        )

    assert "upstream" in str(exc_info.value) and "downstream" in str(exc_info.value)


@patch("datahub.ingestion.source.sql.hive.hive_sql_fetcher.SQLAlchemyClient")
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


@patch("datahub.ingestion.source.sql.hive.hive_sql_fetcher.SQLAlchemyClient")
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


@patch("datahub.ingestion.source.sql.hive.hive_sql_fetcher.SQLAlchemyClient")
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


@patch("datahub.ingestion.source.sql.hive.hive_sql_fetcher.SQLAlchemyClient")
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

    # Aggregator is now private attribute _aggregator
    assert hasattr(source, "_aggregator")
    assert source._aggregator is not None
    assert source.config.include_view_lineage is True


@patch("datahub.ingestion.source.sql.hive.hive_sql_fetcher.SQLAlchemyClient")
def test_get_db_schema_empty_identifier_raises_error(mock_client):
    """Test that get_db_schema raises ValueError for empty identifier"""
    config_dict = {
        "username": "test_user",
        "password": "test_password",
        "host_port": "localhost:3306",
        "database": "test_db",
    }
    config = HiveMetastore.model_validate(config_dict)
    ctx = PipelineContext(run_id="test-run")

    source = HiveMetastoreSource(config, ctx)

    with pytest.raises(InvalidDatasetIdentifierError) as exc_info:
        source.get_db_schema("")

    assert "cannot be empty" in str(exc_info.value)


@patch("datahub.ingestion.source.sql.hive.hive_sql_fetcher.SQLAlchemyClient")
def test_get_db_schema_whitespace_only_raises_error(mock_client):
    """Test that get_db_schema raises ValueError for whitespace-only identifier"""
    config_dict = {
        "username": "test_user",
        "password": "test_password",
        "host_port": "localhost:3306",
        "database": "test_db",
    }
    config = HiveMetastore.model_validate(config_dict)
    ctx = PipelineContext(run_id="test-run")

    source = HiveMetastoreSource(config, ctx)

    with pytest.raises(InvalidDatasetIdentifierError) as exc_info:
        source.get_db_schema("   ")

    assert "cannot be empty" in str(exc_info.value)


@patch("datahub.ingestion.source.sql.hive.hive_sql_fetcher.SQLAlchemyClient")
def test_get_db_schema_double_dots_raises_error(mock_client):
    """Test that get_db_schema raises ValueError for malformed identifiers with double dots"""
    config_dict = {
        "username": "test_user",
        "password": "test_password",
        "host_port": "localhost:3306",
        "database": "test_db",
    }
    config = HiveMetastore.model_validate(config_dict)
    ctx = PipelineContext(run_id="test-run")

    source = HiveMetastoreSource(config, ctx)

    with pytest.raises(InvalidDatasetIdentifierError) as exc_info:
        source.get_db_schema("..")

    assert "Invalid dataset identifier" in str(exc_info.value)


@patch("datahub.ingestion.source.sql.hive.hive_sql_fetcher.SQLAlchemyClient")
def test_get_db_schema_catalog_with_two_parts(mock_client):
    """Test get_db_schema with catalog mode and two-part identifier"""
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

    default_db, default_schema = source.get_db_schema("schema.table")

    assert default_db is None
    assert default_schema == "schema"


@patch("datahub.ingestion.source.sql.hive.hive_sql_fetcher.SQLAlchemyClient")
def test_get_db_schema_four_parts_with_catalog(mock_client):
    """Test get_db_schema handles identifiers with more than 3 parts"""
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

    default_db, default_schema = source.get_db_schema("catalog.schema.table.extra_part")

    assert default_db == "catalog"
    assert default_schema == "schema"


@patch("datahub.ingestion.source.sql.hive.hive_sql_fetcher.SQLAlchemyClient")
def test_presto_view_column_metadata_parsing(mock_client):
    """Test HiveMetadataProcessor._get_presto_view_column_metadata parses encoded view correctly"""
    import base64
    import json

    from datahub.ingestion.source.sql.hive.hive_metadata_processor import (
        HiveMetadataProcessor,
    )

    config_dict = {
        "username": "test_user",
        "password": "test_password",
        "host_port": "localhost:3306",
        "database": "test_db",
        "mode": "presto-on-hive",
    }
    config = HiveMetastore.model_validate(config_dict)
    ctx = PipelineContext(run_id="test-run")

    source = HiveMetastoreSource(config, ctx)

    # Create processor to test the method
    processor = HiveMetadataProcessor(
        config=config,
        fetcher=source._fetcher,
        report=source.report,
        platform=source.platform,
    )

    view_data = {
        "originalSql": "SELECT id, name FROM users WHERE active = true",
        "columns": [
            {"name": "id", "type": "bigint"},
            {"name": "name", "type": "varchar"},
        ],
    }
    encoded_data = base64.b64encode(json.dumps(view_data).encode()).decode()
    view_original_text = f"/* Presto View: {encoded_data} */"

    columns, view_definition = processor._get_presto_view_column_metadata(
        view_original_text
    )

    assert view_definition == "SELECT id, name FROM users WHERE active = true"
    assert len(columns) == 2
    assert columns[0]["col_name"] == "id"
    assert columns[0]["col_type"] == "bigint"
    assert columns[1]["col_name"] == "name"
    assert columns[1]["col_type"] == "varchar"


@patch("datahub.ingestion.source.sql.hive.hive_sql_fetcher.SQLAlchemyClient")
def test_presto_view_complex_sql(mock_client):
    """Test HiveMetadataProcessor._get_presto_view_column_metadata handles complex SQL definitions"""
    import base64
    import json

    from datahub.ingestion.source.sql.hive.hive_metadata_processor import (
        HiveMetadataProcessor,
    )

    config_dict = {
        "username": "test_user",
        "password": "test_password",
        "host_port": "localhost:3306",
        "database": "test_db",
        "mode": "presto-on-hive",
    }
    config = HiveMetastore.model_validate(config_dict)
    ctx = PipelineContext(run_id="test-run")

    source = HiveMetastoreSource(config, ctx)

    # Create processor to test the method
    processor = HiveMetadataProcessor(
        config=config,
        fetcher=source._fetcher,
        report=source.report,
        platform=source.platform,
    )

    complex_sql = """
    SELECT
        u.id,
        u.name,
        COUNT(o.id) as order_count
    FROM users u
    LEFT JOIN orders o ON u.id = o.user_id
    WHERE u.active = true
    GROUP BY u.id, u.name
    """

    view_data = {
        "originalSql": complex_sql,
        "columns": [
            {"name": "id", "type": "bigint"},
            {"name": "name", "type": "varchar"},
            {"name": "order_count", "type": "bigint"},
        ],
    }
    encoded_data = base64.b64encode(json.dumps(view_data).encode()).decode()
    view_original_text = f"/* Presto View: {encoded_data} */"

    columns, view_definition = processor._get_presto_view_column_metadata(
        view_original_text
    )

    assert "LEFT JOIN orders" in view_definition
    assert "GROUP BY" in view_definition
    assert len(columns) == 3


@patch("datahub.ingestion.source.sql.hive.hive_sql_fetcher.SQLAlchemyClient")
def test_view_lineage_adds_to_aggregator(mock_client):
    """Test that view definitions are added to the aggregator when view lineage is enabled"""
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

    assert source.config.include_view_lineage is True
    assert hasattr(source, "_aggregator")
    assert source._aggregator is not None


def test_hive_metastore_storage_lineage_config_from_mixin():
    """Test that storage lineage config fields come from mixin"""
    config_dict = {
        "username": "test_user",
        "password": "test_password",
        "host_port": "localhost:3306",
        "database": "test_db",
        "emit_storage_lineage": True,
        "hive_storage_lineage_direction": "downstream",
        "include_column_lineage": False,
        "storage_platform_instance": "prod-s3",
    }
    config = HiveMetastore.model_validate(config_dict)

    assert config.emit_storage_lineage is True
    assert config.hive_storage_lineage_direction == LineageDirection.DOWNSTREAM
    assert config.include_column_lineage is False
    assert config.storage_platform_instance == "prod-s3"

    # Config inherits from HiveStorageLineageConfigMixin
    assert config.emit_storage_lineage is True
    assert config.hive_storage_lineage_direction == LineageDirection.DOWNSTREAM


@patch("datahub.ingestion.source.sql.hive.hive_sql_fetcher.SQLAlchemyClient")
def test_hive_metastore_subtype_config(mock_client):
    """Test that HiveMetastoreSource correctly initializes table and view subtypes"""
    config_dict = {
        "username": "test_user",
        "password": "test_password",
        "host_port": "localhost:3306",
        "database": "test_db",
    }
    config = HiveMetastore.model_validate(config_dict)
    ctx = PipelineContext(run_id="test-run")

    source = HiveMetastoreSource(config, ctx)
    # Verify that the source uses DatasetSubTypes enum values
    assert source.table_subtype == DatasetSubTypes.TABLE
    assert source.view_subtype == DatasetSubTypes.VIEW


@patch("datahub.ingestion.source.sql.hive.hive_sql_fetcher.SQLAlchemyClient")
def test_hive_metastore_subtype_config_pascalcase(mock_client):
    """Test that HiveMetastoreSource correctly handles PascalCase subtype config"""
    config_dict = {
        "username": "test_user",
        "password": "test_password",
        "host_port": "localhost:3306",
        "database": "test_db",
        "use_dataset_pascalcase_subtype": True,
    }
    config = HiveMetastore.model_validate(config_dict)
    ctx = PipelineContext(run_id="test-run")

    source = HiveMetastoreSource(config, ctx)
    # When PascalCase is enabled, verify the title() transformation
    assert source.table_subtype == DatasetSubTypes.TABLE.title()
    assert source.view_subtype == DatasetSubTypes.VIEW.title()


# =============================================================================
# Storage Lineage Business Logic Tests
# =============================================================================


def test_storage_lineage_s3_location_generates_upstream_lineage():
    """Test that S3 storage locations generate correct upstream lineage MCPs"""
    from datahub.ingestion.source.sql.hive.storage_lineage import HiveStorageLineage

    config = HiveStorageLineageConfigMixin(
        emit_storage_lineage=True,
        hive_storage_lineage_direction="upstream",
        include_column_lineage=True,
        storage_platform_instance=None,
    )

    storage_lineage = HiveStorageLineage(config=config, env="PROD")

    table_dict = {
        "StorageDescriptor": {"Location": "s3://my-bucket/warehouse/db/table"}
    }

    lineage_mcps = list(
        storage_lineage.get_lineage_mcp(
            dataset_urn="urn:li:dataset:(urn:li:dataPlatform:hive,db.my_table,PROD)",
            table=table_dict,
        )
    )

    # Should generate upstream lineage for S3 location
    assert len(lineage_mcps) > 0
    # The upstream should be an S3 dataset
    mcp = lineage_mcps[0]
    assert "s3" in str(mcp)


def test_storage_lineage_downstream_direction_reverses_lineage():
    """Test that downstream direction generates lineage from storage TO hive"""
    from datahub.ingestion.source.sql.hive.storage_lineage import HiveStorageLineage

    config = HiveStorageLineageConfigMixin(
        emit_storage_lineage=True,
        hive_storage_lineage_direction="downstream",
        include_column_lineage=False,
        storage_platform_instance=None,
    )

    storage_lineage = HiveStorageLineage(config=config, env="PROD")

    table_dict = {"StorageDescriptor": {"Location": "s3://bucket/path"}}

    lineage_mcps = list(
        storage_lineage.get_lineage_mcp(
            dataset_urn="urn:li:dataset:(urn:li:dataPlatform:hive,db.table,PROD)",
            table=table_dict,
        )
    )

    # Should generate downstream lineage (storage -> hive)
    assert len(lineage_mcps) > 0


def test_storage_lineage_missing_location_skips_gracefully():
    """Test that missing storage location doesn't crash - just skips lineage"""
    from datahub.ingestion.source.sql.hive.storage_lineage import HiveStorageLineage

    config = HiveStorageLineageConfigMixin(
        emit_storage_lineage=True,
        hive_storage_lineage_direction="upstream",
        include_column_lineage=True,
        storage_platform_instance=None,
    )

    storage_lineage = HiveStorageLineage(config=config, env="PROD")

    # Table with no StorageDescriptor
    table_dict: dict = {}

    lineage_mcps = list(
        storage_lineage.get_lineage_mcp(
            dataset_urn="urn:li:dataset:(urn:li:dataPlatform:hive,db.table,PROD)",
            table=table_dict,
        )
    )

    # Should not crash, just return no lineage
    assert len(lineage_mcps) == 0


def test_storage_lineage_empty_location_skips_gracefully():
    """Test that empty storage location doesn't crash"""
    from datahub.ingestion.source.sql.hive.storage_lineage import HiveStorageLineage

    config = HiveStorageLineageConfigMixin(
        emit_storage_lineage=True,
        hive_storage_lineage_direction="upstream",
        include_column_lineage=True,
        storage_platform_instance=None,
    )

    storage_lineage = HiveStorageLineage(config=config, env="PROD")

    table_dict = {"StorageDescriptor": {"Location": ""}}

    lineage_mcps = list(
        storage_lineage.get_lineage_mcp(
            dataset_urn="urn:li:dataset:(urn:li:dataPlatform:hive,db.table,PROD)",
            table=table_dict,
        )
    )

    # Should not crash, just return no lineage
    assert len(lineage_mcps) == 0


# =============================================================================
# Thrift Connection Type Validation Tests
# =============================================================================


def test_thrift_connection_rejects_presto_mode():
    """Test that Thrift connection type rejects presto/trino modes"""
    config_dict = {
        "connection_type": "thrift",
        "host_port": "localhost:9083",
        "mode": "presto-on-hive",
    }

    with pytest.raises(ValueError) as exc_info:
        HiveMetastore.model_validate(config_dict)

    assert "not supported with 'connection_type: thrift'" in str(exc_info.value)


def test_thrift_connection_where_clause_also_deprecated():
    """Test that WHERE clause suffix is deprecated regardless of connection type"""
    # Note: where_clause_suffix options are now deprecated for ALL connection types,
    # so the deprecation error is raised before any thrift-specific validation
    config_dict = {
        "connection_type": "thrift",
        "host_port": "localhost:9083",
        "tables_where_clause_suffix": "AND d.NAME = 'mydb'",
    }

    with pytest.raises(ValueError) as exc_info:
        HiveMetastore.model_validate(config_dict)

    # Deprecation error is raised first (before thrift validation)
    assert "DEPRECATED" in str(exc_info.value)


def test_thrift_connection_allows_database_pattern():
    """Test that Thrift connection type allows pattern-based filtering"""

    config_dict = {
        "connection_type": "thrift",
        "host_port": "localhost:9083",
        "database_pattern": {"allow": ["^prod_.*"], "deny": ["^test_.*"]},
    }

    config = HiveMetastore.model_validate(config_dict)

    # Pattern-based filtering should work
    assert config.database_pattern.allowed("prod_warehouse")
    assert not config.database_pattern.allowed("test_db")


# =============================================================================
# Edge Cases in Dataset Identifier Parsing
# =============================================================================


@patch("datahub.ingestion.source.sql.hive.hive_sql_fetcher.SQLAlchemyClient")
def test_get_db_schema_handles_leading_dots(mock_client):
    """Test that get_db_schema handles identifiers with leading dots"""
    config_dict = {
        "username": "test_user",
        "password": "test_password",
        "host_port": "localhost:3306",
        "database": "test_db",
    }
    config = HiveMetastore.model_validate(config_dict)
    ctx = PipelineContext(run_id="test-run")

    source = HiveMetastoreSource(config, ctx)

    # Leading dot should be filtered out during split
    default_db, default_schema = source.get_db_schema(".schema.table")

    assert default_db is None
    assert default_schema == "schema"


@patch("datahub.ingestion.source.sql.hive.hive_sql_fetcher.SQLAlchemyClient")
def test_get_db_schema_none_raises_error(mock_client):
    """Test that get_db_schema raises error for None input"""
    config_dict = {
        "username": "test_user",
        "password": "test_password",
        "host_port": "localhost:3306",
        "database": "test_db",
    }
    config = HiveMetastore.model_validate(config_dict)
    ctx = PipelineContext(run_id="test-run")

    source = HiveMetastoreSource(config, ctx)

    with pytest.raises(InvalidDatasetIdentifierError) as exc_info:
        source.get_db_schema(None)  # type: ignore

    assert "cannot be None" in str(exc_info.value)


# =============================================================================
# Security: WHERE Clause Suffix Deprecation Tests
# =============================================================================


def test_where_clause_suffix_empty_string_accepted():
    """Test that empty WHERE clause suffix (default) is accepted"""
    config_dict = {
        "host_port": "localhost:3306",
        "tables_where_clause_suffix": "",
    }
    config = HiveMetastore.model_validate(config_dict)
    assert config.tables_where_clause_suffix == ""


def test_where_clause_suffix_tables_deprecated():
    """Test that tables_where_clause_suffix raises deprecation error"""
    config_dict = {
        "host_port": "localhost:3306",
        "tables_where_clause_suffix": "AND d.NAME = 'test_db'",
    }

    with pytest.raises(ValueError) as exc_info:
        HiveMetastore.model_validate(config_dict)

    error_msg = str(exc_info.value)
    assert "DEPRECATED" in error_msg
    assert "tables_where_clause_suffix" in error_msg
    assert "database_pattern" in error_msg


def test_where_clause_suffix_views_deprecated():
    """Test that views_where_clause_suffix raises deprecation error"""
    config_dict = {
        "host_port": "localhost:3306",
        "views_where_clause_suffix": "AND d.NAME = 'test_db'",
    }

    with pytest.raises(ValueError) as exc_info:
        HiveMetastore.model_validate(config_dict)

    assert "DEPRECATED" in str(exc_info.value)
    assert "views_where_clause_suffix" in str(exc_info.value)


def test_where_clause_suffix_schemas_deprecated():
    """Test that schemas_where_clause_suffix raises deprecation error"""
    config_dict = {
        "host_port": "localhost:3306",
        "schemas_where_clause_suffix": "AND d.NAME LIKE 'prod_%'",
    }

    with pytest.raises(ValueError) as exc_info:
        HiveMetastore.model_validate(config_dict)

    assert "DEPRECATED" in str(exc_info.value)
    assert "schemas_where_clause_suffix" in str(exc_info.value)


def test_where_clause_suffix_multiple_deprecated():
    """Test that multiple deprecated options are reported together"""
    config_dict = {
        "host_port": "localhost:3306",
        "tables_where_clause_suffix": "AND 1=1",
        "views_where_clause_suffix": "AND 2=2",
    }

    with pytest.raises(ValueError) as exc_info:
        HiveMetastore.model_validate(config_dict)

    error_msg = str(exc_info.value)
    assert "tables_where_clause_suffix" in error_msg
    assert "views_where_clause_suffix" in error_msg


def test_where_clause_suffix_deprecation_suggests_database_pattern():
    """Test that deprecation error suggests using database_pattern"""
    config_dict = {
        "host_port": "localhost:3306",
        "tables_where_clause_suffix": "AND d.NAME = 'mydb'",
    }

    with pytest.raises(ValueError) as exc_info:
        HiveMetastore.model_validate(config_dict)

    error_msg = str(exc_info.value)
    assert "database_pattern" in error_msg


# =============================================================================
# Security: Parameterized Database Filter Tests
# =============================================================================


def test_sql_fetcher_db_filter_enabled_when_configured():
    """Test that database filter is enabled when both metastore_db_name and database are set"""
    from datahub.ingestion.source.sql.hive.hive_sql_fetcher import SQLAlchemyDataFetcher

    config_dict = {
        "host_port": "localhost:3306",
        "metastore_db_name": "metastore",
        "database": "test_db",
    }
    config = HiveMetastore.model_validate(config_dict)
    fetcher = SQLAlchemyDataFetcher(config)

    # Should enable filtering
    assert fetcher._should_filter_by_database() is True
    # Should return bind params with db_name
    assert fetcher._get_db_filter_params() == {"db_name": "test_db"}


def test_sql_fetcher_db_filter_disabled_when_database_not_set():
    """Test that no database filter is applied when database is not set"""
    from datahub.ingestion.source.sql.hive.hive_sql_fetcher import SQLAlchemyDataFetcher

    config_dict = {
        "host_port": "localhost:3306",
    }
    config = HiveMetastore.model_validate(config_dict)
    fetcher = SQLAlchemyDataFetcher(config)

    assert fetcher._should_filter_by_database() is False
    assert fetcher._get_db_filter_params() is None


def test_sql_fetcher_db_filter_disabled_without_metastore_db_name():
    """Test that database filter requires metastore_db_name to be set"""
    from datahub.ingestion.source.sql.hive.hive_sql_fetcher import SQLAlchemyDataFetcher

    config_dict = {
        "host_port": "localhost:3306",
        "database": "test_db",
        # metastore_db_name is not set
    }
    config = HiveMetastore.model_validate(config_dict)
    fetcher = SQLAlchemyDataFetcher(config)

    # Without metastore_db_name, no filter is applied
    assert fetcher._should_filter_by_database() is False
    assert fetcher._get_db_filter_params() is None


def test_sql_fetcher_special_chars_in_database_name_are_safe():
    """Test that special characters in database name are safely parameterized"""
    from datahub.ingestion.source.sql.hive.hive_sql_fetcher import (
        SQLAlchemyDataFetcher,
        _get_tables_query,
    )

    # Database name with characters that would be dangerous if not parameterized
    config_dict = {
        "host_port": "localhost:3306",
        "metastore_db_name": "metastore",
        "database": "test'; DROP TABLE users; --",
    }
    config = HiveMetastore.model_validate(config_dict)
    fetcher = SQLAlchemyDataFetcher(config)

    # The dangerous string should be in bind_params only
    bind_params = fetcher._get_db_filter_params()
    assert bind_params is not None
    assert bind_params["db_name"] == "test'; DROP TABLE users; --"

    # The SQL statement should use :db_name placeholder, not the actual value
    query_with_filter = _get_tables_query(with_db_filter=True, is_postgresql=False)
    assert ":db_name" in query_with_filter
    assert "DROP" not in query_with_filter


def test_sql_fetcher_uses_correct_query_variant():
    """Test that fetcher selects correct query based on filtering"""
    from datahub.ingestion.source.sql.hive.hive_sql_fetcher import SQLAlchemyDataFetcher

    # With filtering
    config_with_filter = HiveMetastore.model_validate(
        {
            "host_port": "localhost:3306",
            "metastore_db_name": "metastore",
            "database": "prod_db",
        }
    )
    fetcher_with_filter = SQLAlchemyDataFetcher(config_with_filter)
    assert fetcher_with_filter._should_filter_by_database() is True

    # Without filtering
    config_no_filter = HiveMetastore.model_validate(
        {
            "host_port": "localhost:3306",
        }
    )
    fetcher_no_filter = SQLAlchemyDataFetcher(config_no_filter)
    assert fetcher_no_filter._should_filter_by_database() is False
