from typing import Any, Dict
from unittest.mock import Mock

from datahub.ingestion.source.sql.hive.storage_lineage import (
    HiveStorageLineage,
    HiveStorageLineageConfigMixin,
    HiveStorageSourceReport,
    LineageDirection,
    StoragePathParser,
    StoragePlatform,
)
from datahub.metadata.schema_classes import (
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StringTypeClass,
)


def test_storage_path_parser_s3():
    """Test S3 path parsing"""
    result = StoragePathParser.parse_storage_location("s3://bucket/path/to/data")
    assert result == (StoragePlatform.S3, "bucket/path/to/data")


def test_storage_path_parser_azure_abfss():
    """Test Azure ABFSS path parsing"""
    result = StoragePathParser.parse_storage_location(
        "abfss://container@account.dfs.core.windows.net/path/data"
    )
    assert result == (StoragePlatform.AZURE, "container/path/data")


def test_storage_path_parser_gcs():
    """Test GCS path parsing"""
    result = StoragePathParser.parse_storage_location("gs://bucket/path/to/data")
    assert result == (StoragePlatform.GCS, "bucket/path/to/data")


def test_storage_path_parser_hdfs():
    """Test HDFS path parsing"""
    result = StoragePathParser.parse_storage_location("hdfs://namenode:8020/path/data")
    assert result == (StoragePlatform.HDFS, "namenode:8020/path/data")


def test_storage_path_parser_dbfs():
    """Test DBFS path parsing"""
    result = StoragePathParser.parse_storage_location("dbfs:/path/to/data")
    assert result == (StoragePlatform.DBFS, "/path/to/data")


def test_storage_path_parser_local():
    """Test local file path parsing"""
    result = StoragePathParser.parse_storage_location("/local/path/to/data")
    assert result == (StoragePlatform.LOCAL, "/local/path/to/data")


def test_storage_path_parser_invalid_scheme():
    """Test invalid scheme returns None"""
    result = StoragePathParser.parse_storage_location("invalid://path")
    assert result is None


def test_storage_path_parser_empty():
    """Test empty string returns None"""
    result = StoragePathParser.parse_storage_location("")
    assert result is None


def test_storage_path_parser_normalize_slashes():
    """Test path normalization removes trailing and multiple slashes"""
    result = StoragePathParser.parse_storage_location("s3://bucket//path///data/")
    assert result == (StoragePlatform.S3, "bucket/path/data")


def test_get_platform_name():
    """Test platform name mapping"""
    assert StoragePathParser.get_platform_name(StoragePlatform.S3) == "s3"
    assert StoragePathParser.get_platform_name(StoragePlatform.AZURE) == "adls"
    assert StoragePathParser.get_platform_name(StoragePlatform.GCS) == "gcs"
    assert StoragePathParser.get_platform_name(StoragePlatform.HDFS) == "hdfs"
    assert StoragePathParser.get_platform_name(StoragePlatform.DBFS) == "dbfs"
    assert StoragePathParser.get_platform_name(StoragePlatform.LOCAL) == "file"


def test_hive_storage_lineage_config_defaults():
    """Test default configuration values"""
    config = HiveStorageLineageConfigMixin()

    assert config.emit_storage_lineage is False
    assert config.hive_storage_lineage_direction == LineageDirection.UPSTREAM
    assert config.include_column_lineage is True
    assert config.storage_platform_instance is None


def test_hive_storage_lineage_config_custom():
    """Test custom configuration"""
    config = HiveStorageLineageConfigMixin(
        emit_storage_lineage=True,
        hive_storage_lineage_direction=LineageDirection.DOWNSTREAM,
        include_column_lineage=False,
        storage_platform_instance="prod",
    )

    assert config.emit_storage_lineage is True
    assert config.hive_storage_lineage_direction == LineageDirection.DOWNSTREAM
    assert config.include_column_lineage is False
    assert config.storage_platform_instance == "prod"


def test_hive_storage_source_report():
    """Test storage report tracking"""
    report = HiveStorageSourceReport()

    assert report.storage_locations_scanned == 0
    assert report.filtered_locations == []
    assert report.failed_locations == []

    report.report_location_scanned()
    assert report.storage_locations_scanned == 1

    report.report_location_filtered("s3://bucket/filtered")
    assert "s3://bucket/filtered" in report.filtered_locations

    report.report_location_failed("s3://bucket/failed")
    assert "s3://bucket/failed" in report.failed_locations


def test_hive_storage_lineage_disabled():
    """Test that no lineage is emitted when disabled"""
    config = HiveStorageLineageConfigMixin(emit_storage_lineage=False)
    lineage = HiveStorageLineage(config=config, env="PROD")

    table = {"StorageDescriptor": {"Location": "s3://bucket/path"}}
    result = list(lineage.get_lineage_mcp(dataset_urn="test_urn", table=table))

    assert len(result) == 0


def test_hive_storage_lineage_no_location():
    """Test that no lineage is emitted when table has no location"""
    config = HiveStorageLineageConfigMixin(emit_storage_lineage=True)
    lineage = HiveStorageLineage(config=config, env="PROD")

    table: Dict[str, Any] = {"StorageDescriptor": {}}
    result = list(lineage.get_lineage_mcp(dataset_urn="test_urn", table=table))

    assert len(result) == 0


def test_hive_storage_lineage_invalid_location():
    """Test that invalid storage location is tracked as failed"""
    config = HiveStorageLineageConfigMixin(emit_storage_lineage=True)
    lineage = HiveStorageLineage(config=config, env="PROD")

    table = {"StorageDescriptor": {"Location": "invalid://bad-scheme/path"}}
    result = list(lineage.get_lineage_mcp(dataset_urn="test_urn", table=table))

    assert len(result) == 0
    assert len(lineage.report.failed_locations) == 1


def test_hive_storage_lineage_upstream_direction():
    """Test upstream lineage generation (storage -> hive)"""
    config = HiveStorageLineageConfigMixin(
        emit_storage_lineage=True,
        hive_storage_lineage_direction=LineageDirection.UPSTREAM,
        include_column_lineage=False,
    )
    lineage = HiveStorageLineage(config=config, env="PROD")

    table = {"StorageDescriptor": {"Location": "s3://my-bucket/data/table"}}
    dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:hive,db.table,PROD)"

    result = list(lineage.get_lineage_mcp(dataset_urn=dataset_urn, table=table))

    assert len(result) > 0
    assert lineage.report.storage_locations_scanned == 1

    lineage_wu = [wu for wu in result if "lineage" in wu.id]
    assert len(lineage_wu) == 1


def test_hive_storage_lineage_downstream_direction():
    """Test downstream lineage generation (hive -> storage)"""
    config = HiveStorageLineageConfigMixin(
        emit_storage_lineage=True,
        hive_storage_lineage_direction=LineageDirection.DOWNSTREAM,
        include_column_lineage=False,
    )
    lineage = HiveStorageLineage(config=config, env="PROD")

    table = {"StorageDescriptor": {"Location": "s3://my-bucket/data/table"}}
    dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:hive,db.table,PROD)"

    result = list(lineage.get_lineage_mcp(dataset_urn=dataset_urn, table=table))

    assert len(result) > 0
    lineage_wu = [wu for wu in result if "lineage" in wu.id]
    assert len(lineage_wu) == 1


def test_hive_storage_lineage_with_column_lineage():
    """Test column-level lineage generation"""
    config = HiveStorageLineageConfigMixin(
        emit_storage_lineage=True,
        hive_storage_lineage_direction=LineageDirection.UPSTREAM,
        include_column_lineage=True,
    )
    lineage = HiveStorageLineage(config=config, env="PROD")

    schema = SchemaMetadataClass(
        schemaName="test_schema",
        platform="urn:li:dataPlatform:hive",
        version=0,
        fields=[
            SchemaFieldClass(
                fieldPath="id",
                type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                nativeDataType="string",
            ),
            SchemaFieldClass(
                fieldPath="name",
                type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                nativeDataType="string",
            ),
        ],
        hash="",
        platformSchema=Mock(),
    )

    table = {"StorageDescriptor": {"Location": "s3://bucket/data"}}
    dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:hive,db.table,PROD)"

    result = list(
        lineage.get_lineage_mcp(
            dataset_urn=dataset_urn, table=table, dataset_schema=schema
        )
    )

    assert len(result) > 0
    lineage_wu = [wu for wu in result if "lineage" in wu.id]
    assert len(lineage_wu) == 1


def test_hive_storage_lineage_lowercase_urns():
    """Test URN lowercasing"""
    config = HiveStorageLineageConfigMixin(
        emit_storage_lineage=True,
        storage_platform_instance="PROD-CLUSTER",
    )
    lineage = HiveStorageLineage(config=config, env="PROD")

    table = {"StorageDescriptor": {"Location": "S3://BUCKET/PATH"}}
    dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:hive,db.table,PROD)"

    result = list(lineage.get_lineage_mcp(dataset_urn=dataset_urn, table=table))
    assert len(result) > 0


def test_hive_storage_lineage_platform_instance():
    """Test platform instance in storage URNs"""
    config = HiveStorageLineageConfigMixin(
        emit_storage_lineage=True, storage_platform_instance="my-s3-prod"
    )
    lineage = HiveStorageLineage(config=config, env="PROD")

    table = {"StorageDescriptor": {"Location": "s3://bucket/data"}}
    dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:hive,db.table,PROD)"

    result = list(lineage.get_lineage_mcp(dataset_urn=dataset_urn, table=table))

    assert len(result) > 0


def test_make_storage_dataset_urn_success():
    """Test successful storage URN creation"""
    from datahub.ingestion.source.sql.hive.storage_lineage import StoragePlatform

    config = HiveStorageLineageConfigMixin(emit_storage_lineage=True)
    lineage = HiveStorageLineage(config=config, env="PROD")

    result = lineage._make_storage_dataset_urn(StoragePlatform.S3, "bucket/path/data")

    assert result is not None
    urn, platform = result
    assert "s3" in urn
    assert platform == "s3"


def test_make_storage_dataset_urn_invalid():
    """Test invalid URN parameters returns None"""
    from datahub.ingestion.source.sql.hive.storage_lineage import StoragePlatform

    config = HiveStorageLineageConfigMixin(emit_storage_lineage=True)
    lineage = HiveStorageLineage(config=config, env="PROD")

    # Test with empty path (should fail validation)
    result = lineage._make_storage_dataset_urn(StoragePlatform.S3, "")
    assert result is None


def test_storage_lineage_all_platforms():
    """Test lineage generation for all storage platforms"""
    config = HiveStorageLineageConfigMixin(emit_storage_lineage=True)
    lineage = HiveStorageLineage(config=config, env="PROD")

    test_locations = [
        "s3://bucket/data",
        "gs://bucket/data",
        "abfss://container@account.dfs.core.windows.net/data",
        "hdfs://namenode:8020/data",
        "dbfs:/data",
        "/local/path/data",
    ]

    dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:hive,db.table,PROD)"

    for location in test_locations:
        table = {"StorageDescriptor": {"Location": location}}
        result = list(lineage.get_lineage_mcp(dataset_urn=dataset_urn, table=table))
        assert len(result) > 0, f"Failed for {location}"


def test_get_storage_schema():
    """Test storage schema generation"""
    config = HiveStorageLineageConfigMixin(emit_storage_lineage=True)
    lineage = HiveStorageLineage(config=config, env="PROD")

    table_schema = SchemaMetadataClass(
        schemaName="test",
        platform="urn:li:dataPlatform:hive",
        version=0,
        fields=[
            SchemaFieldClass(
                fieldPath="id",
                type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                nativeDataType="string",
            )
        ],
        hash="",
        platformSchema=Mock(),
    )

    storage_schema = lineage._get_storage_schema("s3://bucket/data", table_schema)

    assert storage_schema is not None
    assert len(storage_schema.fields) == 1
    assert storage_schema.fields[0].fieldPath == "id"


def test_get_storage_schema_no_table_schema():
    """Test storage schema returns None when no table schema provided"""
    config = HiveStorageLineageConfigMixin(emit_storage_lineage=True)
    lineage = HiveStorageLineage(config=config, env="PROD")

    storage_schema = lineage._get_storage_schema("s3://bucket/data", None)
    assert storage_schema is None


def test_get_storage_schema_invalid_location():
    """Test storage schema returns None for invalid location"""
    config = HiveStorageLineageConfigMixin(emit_storage_lineage=True)
    lineage = HiveStorageLineage(config=config, env="PROD")

    table_schema = SchemaMetadataClass(
        schemaName="test",
        platform="urn:li:dataPlatform:hive",
        version=0,
        fields=[],
        hash="",
        platformSchema=Mock(),
    )

    storage_schema = lineage._get_storage_schema("invalid://bad", table_schema)
    assert storage_schema is None


def test_fine_grained_lineage_upstream():
    """Test column lineage generation in upstream direction"""
    config = HiveStorageLineageConfigMixin(
        emit_storage_lineage=True,
        hive_storage_lineage_direction=LineageDirection.UPSTREAM,
        include_column_lineage=True,
    )
    lineage = HiveStorageLineage(config=config, env="PROD")

    dataset_schema = SchemaMetadataClass(
        schemaName="hive_schema",
        platform="urn:li:dataPlatform:hive",
        version=0,
        fields=[
            SchemaFieldClass(
                fieldPath="id",
                type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                nativeDataType="bigint",
            ),
            SchemaFieldClass(
                fieldPath="name",
                type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                nativeDataType="string",
            ),
        ],
        hash="",
        platformSchema=Mock(),
    )

    storage_schema = SchemaMetadataClass(
        schemaName="s3_schema",
        platform="urn:li:dataPlatform:s3",
        version=0,
        fields=[
            SchemaFieldClass(
                fieldPath="id",
                type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                nativeDataType="bigint",
            ),
            SchemaFieldClass(
                fieldPath="name",
                type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                nativeDataType="string",
            ),
        ],
        hash="",
        platformSchema=Mock(),
    )

    dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:hive,db.table,PROD)"
    storage_urn = "urn:li:dataset:(urn:li:dataPlatform:s3,bucket/path,PROD)"

    fine_grained = list(
        lineage._get_fine_grained_lineages(
            dataset_urn, storage_urn, dataset_schema, storage_schema
        )
    )

    assert len(fine_grained) == 2
    assert all(fg.upstreams for fg in fine_grained)
    assert all(fg.downstreams for fg in fine_grained)


def test_fine_grained_lineage_downstream():
    """Test column lineage generation in downstream direction"""
    config = HiveStorageLineageConfigMixin(
        emit_storage_lineage=True,
        hive_storage_lineage_direction=LineageDirection.DOWNSTREAM,
        include_column_lineage=True,
    )
    lineage = HiveStorageLineage(config=config, env="PROD")

    dataset_schema = SchemaMetadataClass(
        schemaName="hive_schema",
        platform="urn:li:dataPlatform:hive",
        version=0,
        fields=[
            SchemaFieldClass(
                fieldPath="id",
                type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                nativeDataType="bigint",
            )
        ],
        hash="",
        platformSchema=Mock(),
    )

    storage_schema = SchemaMetadataClass(
        schemaName="s3_schema",
        platform="urn:li:dataPlatform:s3",
        version=0,
        fields=[
            SchemaFieldClass(
                fieldPath="id",
                type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                nativeDataType="bigint",
            )
        ],
        hash="",
        platformSchema=Mock(),
    )

    dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:hive,db.table,PROD)"
    storage_urn = "urn:li:dataset:(urn:li:dataPlatform:s3,bucket/path,PROD)"

    fine_grained = list(
        lineage._get_fine_grained_lineages(
            dataset_urn, storage_urn, dataset_schema, storage_schema
        )
    )

    assert len(fine_grained) == 1


def test_fine_grained_lineage_disabled():
    """Test no column lineage when disabled"""
    config = HiveStorageLineageConfigMixin(
        emit_storage_lineage=True, include_column_lineage=False
    )
    lineage = HiveStorageLineage(config=config, env="PROD")

    dataset_schema = SchemaMetadataClass(
        schemaName="hive_schema",
        platform="urn:li:dataPlatform:hive",
        version=0,
        fields=[
            SchemaFieldClass(
                fieldPath="id",
                type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                nativeDataType="bigint",
            )
        ],
        hash="",
        platformSchema=Mock(),
    )

    storage_schema = SchemaMetadataClass(
        schemaName="s3_schema",
        platform="urn:li:dataPlatform:s3",
        version=0,
        fields=[
            SchemaFieldClass(
                fieldPath="id",
                type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                nativeDataType="bigint",
            )
        ],
        hash="",
        platformSchema=Mock(),
    )

    dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:hive,db.table,PROD)"
    storage_urn = "urn:li:dataset:(urn:li:dataPlatform:s3,bucket/path,PROD)"

    fine_grained = list(
        lineage._get_fine_grained_lineages(
            dataset_urn, storage_urn, dataset_schema, storage_schema
        )
    )

    assert len(fine_grained) == 0


def test_fine_grained_lineage_partial_match():
    """Test column lineage with only some matching fields"""
    config = HiveStorageLineageConfigMixin(
        emit_storage_lineage=True,
        hive_storage_lineage_direction=LineageDirection.UPSTREAM,
        include_column_lineage=True,
    )
    lineage = HiveStorageLineage(config=config, env="PROD")

    dataset_schema = SchemaMetadataClass(
        schemaName="hive_schema",
        platform="urn:li:dataPlatform:hive",
        version=0,
        fields=[
            SchemaFieldClass(
                fieldPath="id",
                type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                nativeDataType="bigint",
            ),
            SchemaFieldClass(
                fieldPath="name",
                type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                nativeDataType="string",
            ),
            SchemaFieldClass(
                fieldPath="extra",
                type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                nativeDataType="string",
            ),
        ],
        hash="",
        platformSchema=Mock(),
    )

    storage_schema = SchemaMetadataClass(
        schemaName="s3_schema",
        platform="urn:li:dataPlatform:s3",
        version=0,
        fields=[
            SchemaFieldClass(
                fieldPath="id",
                type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                nativeDataType="bigint",
            ),
            SchemaFieldClass(
                fieldPath="name",
                type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                nativeDataType="string",
            ),
        ],
        hash="",
        platformSchema=Mock(),
    )

    dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:hive,db.table,PROD)"
    storage_urn = "urn:li:dataset:(urn:li:dataPlatform:s3,bucket/path,PROD)"

    fine_grained = list(
        lineage._get_fine_grained_lineages(
            dataset_urn, storage_urn, dataset_schema, storage_schema
        )
    )

    assert len(fine_grained) == 2


def test_storage_dataset_mcp_generation():
    """Test storage dataset MCP generation includes all aspects"""
    config = HiveStorageLineageConfigMixin(emit_storage_lineage=True)
    lineage = HiveStorageLineage(config=config, env="PROD")

    schema = SchemaMetadataClass(
        schemaName="test",
        platform="urn:li:dataPlatform:hive",
        version=0,
        fields=[
            SchemaFieldClass(
                fieldPath="id",
                type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                nativeDataType="string",
            )
        ],
        hash="",
        platformSchema=Mock(),
    )

    result = list(
        lineage.get_storage_dataset_mcp(
            storage_location="s3://bucket/path", schema_metadata=schema
        )
    )

    assert len(result) == 3
    ids = [wu.id for wu in result]
    assert any("props" in id for id in ids)
    assert any("platform" in id for id in ids)
    assert any("schema" in id for id in ids)


def test_storage_dataset_mcp_without_schema():
    """Test storage dataset MCP generation without schema"""
    config = HiveStorageLineageConfigMixin(emit_storage_lineage=True)
    lineage = HiveStorageLineage(config=config, env="PROD")

    result = list(lineage.get_storage_dataset_mcp(storage_location="s3://bucket/path"))

    assert len(result) == 2


def test_lineage_direction_enum_values():
    """Test LineageDirection enum values"""
    assert LineageDirection.UPSTREAM == "upstream"
    assert LineageDirection.DOWNSTREAM == "downstream"
    assert str(LineageDirection.UPSTREAM) == "upstream"
    assert str(LineageDirection.DOWNSTREAM) == "downstream"


def test_storage_platform_enum_values():
    """Test StoragePlatform enum values"""
    assert StoragePlatform.S3 == "s3"
    assert StoragePlatform.AZURE == "abs"
    assert StoragePlatform.GCS == "gcs"
    assert StoragePlatform.HDFS == "hdfs"
    assert StoragePlatform.DBFS == "dbfs"
    assert StoragePlatform.LOCAL == "file"
