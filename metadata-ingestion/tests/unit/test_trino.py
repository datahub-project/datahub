from unittest import mock

from datahub.emitter.mce_builder import make_schema_field_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.source.sql.sql_common import PipelineContext, SQLAlchemySource
from datahub.ingestion.source.sql.trino import TrinoConfig, TrinoSource
from datahub.metadata.schema_classes import (
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemalessClass,
    SchemaMetadataClass,
    StringTypeClass,
    UpstreamLineageClass,
)


def get_test_trino_source(include_column_lineage: bool = True) -> TrinoSource:
    config = TrinoConfig(
        host_port="localhost:8080",
        database="iceberg_catalog",
        username="test",
        include_column_lineage=include_column_lineage,
        ingest_lineage_to_connectors=True,
    )
    return TrinoSource(
        config=config, ctx=PipelineContext(run_id="test"), platform="trino"
    )


def get_test_trino_schema_metadata(
    field_paths: list[str],
) -> SchemaMetadataClass:
    return SchemaMetadataClass(
        schemaName="iceberg_catalog.contextad.accountcontact",
        platform="urn:li:dataPlatform:trino",
        version=0,
        hash="",
        platformSchema=SchemalessClass(),
        fields=[
            SchemaFieldClass(
                fieldPath=path,
                nativeDataType="varchar",
                type=SchemaFieldDataTypeClass(type=StringTypeClass()),
            )
            for path in field_paths
        ],
    )


def test_trino_gen_lineage_workunit_includes_fine_grained_lineage_when_schema_provided():
    source = get_test_trino_source(include_column_lineage=True)
    dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:trino,iceberg_catalog.contextad.accountcontact,PROD)"
    source_dataset_urn = (
        "urn:li:dataset:(urn:li:dataPlatform:iceberg,contextad.accountcontact,PROD)"
    )
    schema_metadata = get_test_trino_schema_metadata(
        ["accountid", "accountmanagerid", "businessdevid", "accountservicetype"]
    )

    workunits = list(
        source.gen_lineage_workunit(dataset_urn, source_dataset_urn, schema_metadata)
    )
    assert len(workunits) == 1
    upstream_lineage = workunits[0].get_aspect_of_type(UpstreamLineageClass)
    assert isinstance(upstream_lineage, UpstreamLineageClass)
    fgl = upstream_lineage.fineGrainedLineages
    assert fgl is not None
    assert len(fgl) == 4
    for fg in fgl:
        assert fg.upstreams is not None
        assert fg.downstreams is not None
        assert len(fg.upstreams) == 1
        assert len(fg.downstreams) == 1
        assert "iceberg" in fg.upstreams[0]
        assert "trino" in fg.downstreams[0]
    assert make_schema_field_urn(source_dataset_urn, "accountid") in [
        fg.upstreams[0] for fg in fgl if fg.upstreams
    ]
    assert make_schema_field_urn(dataset_urn, "accountid") in [
        fg.downstreams[0] for fg in fgl if fg.downstreams
    ]


def test_trino_gen_lineage_workunit_no_fine_grained_lineage_when_disabled():
    source = get_test_trino_source(include_column_lineage=False)
    dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:trino,iceberg_catalog.contextad.accountcontact,PROD)"
    source_dataset_urn = (
        "urn:li:dataset:(urn:li:dataPlatform:iceberg,contextad.accountcontact,PROD)"
    )
    schema_metadata = get_test_trino_schema_metadata(["accountid"])

    workunits = list(
        source.gen_lineage_workunit(dataset_urn, source_dataset_urn, schema_metadata)
    )
    assert len(workunits) == 1
    upstream_lineage = workunits[0].get_aspect_of_type(UpstreamLineageClass)
    assert isinstance(upstream_lineage, UpstreamLineageClass)
    assert upstream_lineage.fineGrainedLineages is None


def test_trino_gen_lineage_workunit_no_fine_grained_lineage_when_schema_none():
    source = get_test_trino_source(include_column_lineage=True)
    dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:trino,iceberg_catalog.contextad.accountcontact,PROD)"
    source_dataset_urn = (
        "urn:li:dataset:(urn:li:dataPlatform:iceberg,contextad.accountcontact,PROD)"
    )

    workunits = list(source.gen_lineage_workunit(dataset_urn, source_dataset_urn, None))
    assert len(workunits) == 1
    upstream_lineage = workunits[0].get_aspect_of_type(UpstreamLineageClass)
    assert isinstance(upstream_lineage, UpstreamLineageClass)
    assert upstream_lineage.fineGrainedLineages is None


def test_trino_gen_lineage_workunit_no_fine_grained_lineage_when_schema_empty_fields():
    source = get_test_trino_source(include_column_lineage=True)
    dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:trino,iceberg_catalog.contextad.accountcontact,PROD)"
    source_dataset_urn = (
        "urn:li:dataset:(urn:li:dataPlatform:iceberg,contextad.accountcontact,PROD)"
    )
    schema_metadata = get_test_trino_schema_metadata([])

    workunits = list(
        source.gen_lineage_workunit(dataset_urn, source_dataset_urn, schema_metadata)
    )
    assert len(workunits) == 1
    upstream_lineage = workunits[0].get_aspect_of_type(UpstreamLineageClass)
    assert isinstance(upstream_lineage, UpstreamLineageClass)
    assert upstream_lineage.fineGrainedLineages is None


def test_trino_gen_lineage_workunit_upstreams_present_with_or_without_cll():
    source = get_test_trino_source(include_column_lineage=True)
    dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:trino,catalog.schema.table,PROD)"
    source_dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:hive,schema.table,PROD)"

    workunits = list(source.gen_lineage_workunit(dataset_urn, source_dataset_urn, None))
    assert len(workunits) == 1
    upstream_lineage = workunits[0].get_aspect_of_type(UpstreamLineageClass)
    assert upstream_lineage is not None
    assert len(upstream_lineage.upstreams) == 1
    assert upstream_lineage.upstreams[0].dataset == source_dataset_urn


def test_trino_process_table_emits_connector_lineage_with_schema():
    """Covers _process_table: extracts schema from parent workunits for CLL."""
    source = get_test_trino_source(include_column_lineage=True)
    dataset_name = "iceberg_catalog.ctx.t1"
    dataset_urn = (
        "urn:li:dataset:(urn:li:dataPlatform:trino,iceberg_catalog.ctx.t1,PROD)"
    )
    schema, table = "ctx", "t1"
    source_urn = "urn:li:dataset:(urn:li:dataPlatform:iceberg,ctx.t1,PROD)"
    mock_inspector = mock.Mock()
    sql_config = mock.Mock(spec=["view_pattern", "table_pattern"])

    schema_metadata = get_test_trino_schema_metadata(["col1"])
    schema_wu = MetadataChangeProposalWrapper(
        entityUrn=dataset_urn,
        aspect=schema_metadata,
    ).as_workunit()

    with (
        mock.patch.object(
            SQLAlchemySource, "_process_table", return_value=iter([schema_wu])
        ),
        mock.patch.object(source, "_get_source_dataset_urn", return_value=source_urn),
    ):
        workunits = list(
            source._process_table(
                dataset_name,
                mock_inspector,
                schema,
                table,
                sql_config,
                data_reader=None,
            )
        )

    lineage_wus = [w for w in workunits if w.get_aspect_of_type(UpstreamLineageClass)]
    assert len(lineage_wus) == 1
    upstream_lineage = lineage_wus[0].get_aspect_of_type(UpstreamLineageClass)
    assert isinstance(upstream_lineage, UpstreamLineageClass)
    assert upstream_lineage.fineGrainedLineages is not None
    assert len(upstream_lineage.fineGrainedLineages) == 1
    assert upstream_lineage.upstreams[0].dataset == source_urn


def test_trino_process_view_emits_connector_lineage_with_schema():
    """Covers _process_view: extracts schema from parent workunits for CLL."""
    source = get_test_trino_source(include_column_lineage=True)
    dataset_name = "iceberg_catalog.ctx.v1"
    dataset_urn = (
        "urn:li:dataset:(urn:li:dataPlatform:trino,iceberg_catalog.ctx.v1,PROD)"
    )
    schema, view = "ctx", "v1"
    source_urn = "urn:li:dataset:(urn:li:dataPlatform:iceberg,ctx.v1,PROD)"
    mock_inspector = mock.Mock()
    sql_config = mock.Mock(spec=["view_pattern", "table_pattern"])

    schema_metadata = get_test_trino_schema_metadata(["col1"])
    schema_wu = MetadataChangeProposalWrapper(
        entityUrn=dataset_urn,
        aspect=schema_metadata,
    ).as_workunit()

    with (
        mock.patch.object(
            SQLAlchemySource, "_process_view", return_value=iter([schema_wu])
        ),
        mock.patch.object(source, "_get_source_dataset_urn", return_value=source_urn),
    ):
        workunits = list(
            source._process_view(
                dataset_name,
                mock_inspector,
                schema,
                view,
                sql_config,
            )
        )

    lineage_wus = [w for w in workunits if w.get_aspect_of_type(UpstreamLineageClass)]
    assert len(lineage_wus) == 1
    upstream_lineage = lineage_wus[0].get_aspect_of_type(UpstreamLineageClass)
    assert isinstance(upstream_lineage, UpstreamLineageClass)
    assert upstream_lineage.fineGrainedLineages is not None
    assert len(upstream_lineage.fineGrainedLineages) == 1
    assert upstream_lineage.upstreams[0].dataset == source_urn


def test_trino_process_table_emits_lineage_without_cll_when_no_schema_emitted():
    """Table-level lineage still emitted when parent doesn't emit SchemaMetadata."""
    source = get_test_trino_source(include_column_lineage=True)
    dataset_name = "iceberg_catalog.ctx.t1"
    schema, table = "ctx", "t1"
    source_urn = "urn:li:dataset:(urn:li:dataPlatform:iceberg,ctx.t1,PROD)"
    mock_inspector = mock.Mock()
    sql_config = mock.Mock(spec=["view_pattern", "table_pattern"])

    with (
        mock.patch.object(SQLAlchemySource, "_process_table", return_value=iter([])),
        mock.patch.object(source, "_get_source_dataset_urn", return_value=source_urn),
    ):
        workunits = list(
            source._process_table(
                dataset_name,
                mock_inspector,
                schema,
                table,
                sql_config,
                data_reader=None,
            )
        )

    lineage_wus = [w for w in workunits if w.get_aspect_of_type(UpstreamLineageClass)]
    assert len(lineage_wus) == 1
    upstream_lineage = lineage_wus[0].get_aspect_of_type(UpstreamLineageClass)
    assert isinstance(upstream_lineage, UpstreamLineageClass)
    assert upstream_lineage.fineGrainedLineages is None
    assert len(upstream_lineage.upstreams) == 1
