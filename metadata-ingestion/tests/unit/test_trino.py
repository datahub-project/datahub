from typing import List, Optional
from unittest import mock

from datahub.emitter.mce_builder import make_schema_field_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DataHubGraph
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


def get_test_trino_source(
    include_column_lineage: bool = True,
    graph: Optional[DataHubGraph] = None,
) -> TrinoSource:
    config = TrinoConfig(
        host_port="localhost:8080",
        database="iceberg_catalog",
        username="test",
        include_column_lineage=include_column_lineage,
        ingest_lineage_to_connectors=True,
    )
    return TrinoSource(
        config=config,
        ctx=PipelineContext(run_id="test", graph=graph),
        platform="trino",
    )


def make_source_schema(field_paths: List[str]) -> SchemaMetadataClass:
    """Schema as the *connector source* (e.g. Iceberg) ingested it — real casing."""
    return SchemaMetadataClass(
        schemaName="contextad.accountcontact",
        platform="urn:li:dataPlatform:iceberg",
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


def test_trino_gen_lineage_workunit_resolves_upstream_column_case_from_source_schema():
    """Upstream column URNs must use the source's real (mixed-case) column names,
    looked up from the source dataset's schema in the graph, not Trino's lowercase
    names. The downstream (Trino) side stays lowercase."""
    source_schema = make_source_schema(
        ["AccountId", "AccountManagerId", "BusinessDevId", "AccountServiceType"]
    )
    graph = mock.Mock(spec=DataHubGraph)
    graph.get_aspect.return_value = source_schema
    source = get_test_trino_source(include_column_lineage=True, graph=graph)

    dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:trino,iceberg_catalog.contextad.accountcontact,PROD)"
    source_dataset_urn = (
        "urn:li:dataset:(urn:li:dataPlatform:iceberg,contextad.accountcontact,PROD)"
    )
    # Trino reports columns lowercase.
    trino_schema = get_test_trino_schema_metadata(
        ["accountid", "accountmanagerid", "businessdevid", "accountservicetype"]
    )

    workunits = list(
        source.gen_lineage_workunit(dataset_urn, source_dataset_urn, trino_schema)
    )
    assert len(workunits) == 1
    upstream_lineage = workunits[0].get_aspect_of_type(UpstreamLineageClass)
    assert isinstance(upstream_lineage, UpstreamLineageClass)
    fgl = upstream_lineage.fineGrainedLineages
    assert fgl is not None
    assert len(fgl) == 4

    upstreams = [fg.upstreams[0] for fg in fgl if fg.upstreams]
    downstreams = [fg.downstreams[0] for fg in fgl if fg.downstreams]

    # Upstream uses the source's mixed-case column name.
    assert make_schema_field_urn(source_dataset_urn, "AccountId") in upstreams
    assert make_schema_field_urn(source_dataset_urn, "AccountServiceType") in upstreams
    # The lowercase upstream URN must NOT be emitted.
    assert make_schema_field_urn(source_dataset_urn, "accountid") not in upstreams
    # Downstream (Trino) stays lowercase.
    assert make_schema_field_urn(dataset_urn, "accountid") in downstreams


def test_trino_gen_lineage_workunit_falls_back_to_trino_path_when_source_schema_missing():
    """With a graph but no source schema (e.g. source not ingested yet), fall back to
    Trino's own column path so behaviour is unchanged for already-matching sources."""
    graph = mock.Mock(spec=DataHubGraph)
    graph.get_aspect.return_value = None  # source schema not available
    source = get_test_trino_source(include_column_lineage=True, graph=graph)

    dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:trino,iceberg_catalog.contextad.accountcontact,PROD)"
    source_dataset_urn = (
        "urn:li:dataset:(urn:li:dataPlatform:iceberg,contextad.accountcontact,PROD)"
    )
    trino_schema = get_test_trino_schema_metadata(["accountid", "accountservicetype"])

    workunits = list(
        source.gen_lineage_workunit(dataset_urn, source_dataset_urn, trino_schema)
    )
    upstream_lineage = workunits[0].get_aspect_of_type(UpstreamLineageClass)
    assert isinstance(upstream_lineage, UpstreamLineageClass)
    fgl = upstream_lineage.fineGrainedLineages
    assert fgl is not None
    assert len(fgl) == 2
    upstreams = [fg.upstreams[0] for fg in fgl if fg.upstreams]
    # Falls back to the Trino (lowercase) path on the source urn.
    assert make_schema_field_urn(source_dataset_urn, "accountid") in upstreams


def test_trino_gen_lineage_workunit_skips_columns_without_source_match():
    """When the source schema is known but a Trino column has no case-insensitive
    match, that column's lineage is skipped (no invalid upstream URN emitted)."""
    source_schema = make_source_schema(["AccountId"])  # source only has AccountId
    graph = mock.Mock(spec=DataHubGraph)
    graph.get_aspect.return_value = source_schema
    source = get_test_trino_source(include_column_lineage=True, graph=graph)

    dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:trino,iceberg_catalog.contextad.accountcontact,PROD)"
    source_dataset_urn = (
        "urn:li:dataset:(urn:li:dataPlatform:iceberg,contextad.accountcontact,PROD)"
    )
    # "trinoonly" has no counterpart in the source schema.
    trino_schema = get_test_trino_schema_metadata(["accountid", "trinoonly"])

    workunits = list(
        source.gen_lineage_workunit(dataset_urn, source_dataset_urn, trino_schema)
    )
    upstream_lineage = workunits[0].get_aspect_of_type(UpstreamLineageClass)
    assert isinstance(upstream_lineage, UpstreamLineageClass)
    fgl = upstream_lineage.fineGrainedLineages
    assert fgl is not None
    assert len(fgl) == 1  # only the matched column
    assert make_schema_field_urn(source_dataset_urn, "AccountId") in [
        fg.upstreams[0] for fg in fgl if fg.upstreams
    ]
    # The dataset-level upstream is still present regardless.
    assert len(upstream_lineage.upstreams) == 1
    assert upstream_lineage.upstreams[0].dataset == source_dataset_urn


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
