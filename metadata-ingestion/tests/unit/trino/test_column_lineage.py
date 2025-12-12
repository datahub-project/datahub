from unittest.mock import MagicMock

from datahub.ingestion.source.sql.trino import ConnectorDetail, TrinoConfig, TrinoSource
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    MySqlDDLClass,
    SchemaField,
    SchemaFieldDataType,
    SchemaMetadata,
    StringTypeClass,
)


def test_column_lineage_generation():
    config = TrinoConfig(
        host_port="localhost:8080",
        database="hive_catalog",
        username="test",
        ingest_lineage_to_connectors=True,
        include_column_lineage=True,
        catalog_to_connector_details={
            "hive_catalog": ConnectorDetail(
                connector_platform="hive",
                connector_database=None,
            )
        },
    )

    mock_ctx = MagicMock()
    mock_ctx.graph = None
    source = TrinoSource(config, mock_ctx)

    schema_metadata = SchemaMetadata(
        schemaName="test_table",
        platform="urn:li:dataPlatform:trino",
        version=0,
        hash="",
        platformSchema=MySqlDDLClass(tableSchema=""),
        fields=[
            SchemaField(
                fieldPath="id",
                type=SchemaFieldDataType(type=StringTypeClass()),
                nativeDataType="VARCHAR",
                nullable=False,
            ),
            SchemaField(
                fieldPath="name",
                type=SchemaFieldDataType(type=StringTypeClass()),
                nativeDataType="VARCHAR",
                nullable=True,
            ),
            SchemaField(
                fieldPath="value",
                type=SchemaFieldDataType(type=StringTypeClass()),
                nativeDataType="BIGINT",
                nullable=True,
            ),
        ],
    )

    dataset_urn = (
        "urn:li:dataset:(urn:li:dataPlatform:trino,hive_catalog.schema1.table1,PROD)"
    )
    source_dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:hive,schema1.table1,PROD)"

    workunits = list(
        source.gen_lineage_workunit(dataset_urn, source_dataset_urn, schema_metadata)
    )

    assert len(workunits) == 1

    workunit = workunits[0]
    assert hasattr(workunit.metadata, "aspect")
    upstream_lineage = workunit.metadata.aspect
    assert upstream_lineage is not None

    assert hasattr(upstream_lineage, "upstreams")
    assert len(upstream_lineage.upstreams) == 1
    assert upstream_lineage.upstreams[0].dataset == source_dataset_urn

    assert hasattr(upstream_lineage, "fineGrainedLineages")
    assert upstream_lineage.fineGrainedLineages is not None
    assert len(upstream_lineage.fineGrainedLineages) == 3

    for i, field in enumerate(schema_metadata.fields):
        fine_grained = upstream_lineage.fineGrainedLineages[i]

        assert len(fine_grained.downstreams) == 1
        expected_downstream = f"urn:li:schemaField:({dataset_urn},{field.fieldPath})"
        assert fine_grained.downstreams[0] == expected_downstream

        assert len(fine_grained.upstreams) == 1
        expected_upstream = (
            f"urn:li:schemaField:({source_dataset_urn},{field.fieldPath})"
        )
        assert fine_grained.upstreams[0] == expected_upstream


def test_column_lineage_disabled():
    config = TrinoConfig(
        host_port="localhost:8080",
        database="hive_catalog",
        username="test",
        ingest_lineage_to_connectors=True,
        include_column_lineage=False,
        catalog_to_connector_details={
            "hive_catalog": ConnectorDetail(
                connector_platform="hive",
                connector_database=None,
            )
        },
    )

    mock_ctx = MagicMock()
    mock_ctx.graph = None
    source = TrinoSource(config, mock_ctx)

    schema_metadata = SchemaMetadata(
        schemaName="test_table",
        platform="urn:li:dataPlatform:trino",
        version=0,
        hash="",
        platformSchema=MySqlDDLClass(tableSchema=""),
        fields=[
            SchemaField(
                fieldPath="id",
                type=SchemaFieldDataType(type=StringTypeClass()),
                nativeDataType="VARCHAR",
                nullable=False,
            ),
        ],
    )

    dataset_urn = (
        "urn:li:dataset:(urn:li:dataPlatform:trino,hive_catalog.schema1.table1,PROD)"
    )
    source_dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:hive,schema1.table1,PROD)"

    workunits = list(
        source.gen_lineage_workunit(dataset_urn, source_dataset_urn, schema_metadata)
    )

    assert len(workunits) == 1

    workunit = workunits[0]
    assert hasattr(workunit.metadata, "aspect")
    upstream_lineage = workunit.metadata.aspect
    assert upstream_lineage is not None

    assert hasattr(upstream_lineage, "upstreams")
    assert len(upstream_lineage.upstreams) == 1

    assert hasattr(upstream_lineage, "fineGrainedLineages")
    assert upstream_lineage.fineGrainedLineages is None
