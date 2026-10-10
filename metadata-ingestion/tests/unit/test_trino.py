from typing import List, Optional
from unittest import mock

from datahub.emitter.mce_builder import make_schema_field_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.source.sql.sql_common import PipelineContext, SQLAlchemySource
from datahub.ingestion.source.sql.trino import (
    ConnectorDetail,
    TrinoConfig,
    TrinoSource,
)
from datahub.metadata.schema_classes import (
    ChangeTypeClass,
    MetadataChangeProposalClass,
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


def test_trino_gen_lineage_workunit_falls_back_when_graph_lookup_raises():
    """A graph failure degrades to the Trino path instead of aborting the lineage.

    ``get_aspect`` raises on any non-404 response, and the call happens mid-iteration
    after the sibling workunits were already emitted, so an unguarded failure would
    leave the dataset half-linked.
    """
    graph = mock.Mock(spec=DataHubGraph)
    graph.get_aspect.side_effect = Exception("GMS unavailable")
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
    assert make_schema_field_urn(source_dataset_urn, "accountid") in upstreams
    assert source.report.warnings

    # The failure is cached, so a second table on the same source does not re-query GMS.
    list(source.gen_lineage_workunit(dataset_urn, source_dataset_urn, trino_schema))
    assert graph.get_aspect.call_count == 1


def test_trino_gen_lineage_workunit_skips_case_ambiguous_source_columns():
    """A source that distinguishes two columns only by case cannot be matched.

    Trino reports one lowercased name for both, so binding it to either would be a
    coin flip. The ambiguous column is skipped; unambiguous ones still resolve.
    """
    source_schema = make_source_schema(["AccountId", "accountid", "ServiceType"])
    graph = mock.Mock(spec=DataHubGraph)
    graph.get_aspect.return_value = source_schema
    source = get_test_trino_source(include_column_lineage=True, graph=graph)

    dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:trino,iceberg_catalog.contextad.accountcontact,PROD)"
    source_dataset_urn = (
        "urn:li:dataset:(urn:li:dataPlatform:iceberg,contextad.accountcontact,PROD)"
    )
    trino_schema = get_test_trino_schema_metadata(["accountid", "servicetype"])

    workunits = list(
        source.gen_lineage_workunit(dataset_urn, source_dataset_urn, trino_schema)
    )
    upstream_lineage = workunits[0].get_aspect_of_type(UpstreamLineageClass)
    assert isinstance(upstream_lineage, UpstreamLineageClass)
    fgl = upstream_lineage.fineGrainedLineages
    assert fgl is not None

    upstreams = [fg.upstreams[0] for fg in fgl if fg.upstreams]
    # Only the unambiguous column resolves; neither casing of accountid is guessed at.
    assert upstreams == [make_schema_field_urn(source_dataset_urn, "ServiceType")]
    assert make_schema_field_urn(source_dataset_urn, "AccountId") not in upstreams
    assert make_schema_field_urn(source_dataset_urn, "accountid") not in upstreams
    assert source.report.warnings
    # The dataset-level upstream is unaffected.
    assert len(upstream_lineage.upstreams) == 1


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


def _make_source_with_connector_details(
    catalog_to_connector_details: dict[str, ConnectorDetail],
) -> TrinoSource:
    config = TrinoConfig(
        host_port="localhost:8080",
        database="db",
        username="test",
        catalog_to_connector_details=catalog_to_connector_details,
    )
    return TrinoSource(
        config=config, ctx=PipelineContext(run_id="test"), platform="trino"
    )


def test_get_source_dataset_urn_oracle_is_two_tier():
    """Oracle maps to a two-tier (schema.table) native URN with no extra config."""
    source = _make_source_with_connector_details({})
    with mock.patch(
        "datahub.ingestion.source.sql.trino.get_catalog_connector_name",
        return_value="oracle",
    ):
        urn = source._get_source_dataset_urn(
            "oracle_catalog.hr.employees", mock.Mock(), "hr", "employees"
        )
    assert urn == "urn:li:dataset:(urn:li:dataPlatform:oracle,hr.employees,PROD)"


def test_get_source_dataset_urn_starrocks_is_three_tier():
    """StarRocks is three-tier; connector_database supplies the catalog tier."""
    source = _make_source_with_connector_details(
        {"sr_catalog": ConnectorDetail(connector_database="default_catalog")}
    )
    with mock.patch(
        "datahub.ingestion.source.sql.trino.get_catalog_connector_name",
        return_value="starrocks",
    ):
        urn = source._get_source_dataset_urn(
            "sr_catalog.web.clicks", mock.Mock(), "web", "clicks"
        )
    assert (
        urn
        == "urn:li:dataset:(urn:li:dataPlatform:starrocks,default_catalog.web.clicks,PROD)"
    )


def test_get_source_dataset_urn_starrocks_without_connector_database_returns_none():
    """Three-tier connector without connector_database cannot build a URN."""
    source = _make_source_with_connector_details({})
    with mock.patch(
        "datahub.ingestion.source.sql.trino.get_catalog_connector_name",
        return_value="starrocks",
    ):
        urn = source._get_source_dataset_urn(
            "sr_catalog.web.clicks", mock.Mock(), "web", "clicks"
        )
    assert urn is None


def test_get_source_dataset_urn_starrocks_via_mysql_connector_override():
    """StarRocks is reached via Trino's mysql connector; connector_platform retargets it.

    Trino reports connector_name="mysql", so connector_platform="starrocks" overrides
    the platform lookup and connector_database supplies the three-tier catalog tier.
    """
    source = _make_source_with_connector_details(
        {
            "sr_catalog": ConnectorDetail(
                connector_platform="starrocks",
                connector_database="default_catalog",
            )
        }
    )
    with mock.patch(
        "datahub.ingestion.source.sql.trino.get_catalog_connector_name",
        return_value="mysql",
    ):
        urn = source._get_source_dataset_urn(
            "sr_catalog.web.clicks", mock.Mock(), "web", "clicks"
        )
    assert (
        urn
        == "urn:li:dataset:(urn:li:dataPlatform:starrocks,default_catalog.web.clicks,PROD)"
    )


def test_get_source_dataset_urn_oracle_three_tier_when_connector_database_set():
    """connector_database forces a three-tier URN even for a two-tier connector.

    Covers Oracle ingested with add_database_name_to_urn=true, where the native URN
    is database.schema.table.
    """
    source = _make_source_with_connector_details(
        {"oracle_catalog": ConnectorDetail(connector_database="orclpdb1")}
    )
    with mock.patch(
        "datahub.ingestion.source.sql.trino.get_catalog_connector_name",
        return_value="oracle",
    ):
        urn = source._get_source_dataset_urn(
            "oracle_catalog.hr.employees", mock.Mock(), "hr", "employees"
        )
    assert (
        urn == "urn:li:dataset:(urn:li:dataPlatform:oracle,orclpdb1.hr.employees,PROD)"
    )


def test_get_source_dataset_urn_oracle_uppercase_connector_database_is_lowercased():
    """An all-uppercase Oracle connector_database matches the native oracle URN.

    Oracle stores unquoted identifiers uppercase and the `oracle` source lowercases them,
    so a connector_database copied verbatim from Oracle must be folded the same way or the
    generated URN never matches the native one.
    """
    source = _make_source_with_connector_details(
        {"oracle_catalog": ConnectorDetail(connector_database="ORCLPDB1")}
    )
    with mock.patch(
        "datahub.ingestion.source.sql.trino.get_catalog_connector_name",
        return_value="oracle",
    ):
        urn = source._get_source_dataset_urn(
            "oracle_catalog.hr.employees", mock.Mock(), "hr", "employees"
        )
    assert (
        urn == "urn:li:dataset:(urn:li:dataPlatform:oracle,orclpdb1.hr.employees,PROD)"
    )


def test_get_source_dataset_urn_oracle_mixed_case_connector_database_preserved():
    """Mixed-case means a quoted Oracle identifier, which the oracle source preserves."""
    source = _make_source_with_connector_details(
        {"oracle_catalog": ConnectorDetail(connector_database="OrclPdb1")}
    )
    with mock.patch(
        "datahub.ingestion.source.sql.trino.get_catalog_connector_name",
        return_value="oracle",
    ):
        urn = source._get_source_dataset_urn(
            "oracle_catalog.hr.employees", mock.Mock(), "hr", "employees"
        )
    assert (
        urn == "urn:li:dataset:(urn:li:dataPlatform:oracle,OrclPdb1.hr.employees,PROD)"
    )


def test_get_source_dataset_urn_non_oracle_connector_database_case_untouched():
    """Oracle's uppercase-folding rule must not leak to other platforms."""
    source = _make_source_with_connector_details(
        {"sr_catalog": ConnectorDetail(connector_database="DEFAULT_CATALOG")}
    )
    with mock.patch(
        "datahub.ingestion.source.sql.trino.get_catalog_connector_name",
        return_value="starrocks",
    ):
        urn = source._get_source_dataset_urn(
            "sr_catalog.web.clicks", mock.Mock(), "web", "clicks"
        )
    assert (
        urn
        == "urn:li:dataset:(urn:li:dataPlatform:starrocks,DEFAULT_CATALOG.web.clicks,PROD)"
    )


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


def test_trino_gen_siblings_workunit_connector_side_is_not_primary():
    """Trino must not claim ownership of the connector's native dataset.

    The connector-side workunit is a patch marked non-primary, so stateful ingestion
    records it in Trino's skip list rather than its checkpoint. Were it primary, Trino
    would soft-delete a dataset owned by the native source once that URN stopped being
    emitted -- which the connector_database precedence change makes reachable on the
    first run after upgrading. The Trino-side aspect stays primary; Trino owns that.
    """
    source = get_test_trino_source()
    dataset_urn = (
        "urn:li:dataset:(urn:li:dataPlatform:trino,oracle_catalog.hr.employees,PROD)"
    )
    source_dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:oracle,hr.employees,PROD)"

    workunits = list(source.gen_siblings_workunit(dataset_urn, source_dataset_urn))

    by_urn = {wu.get_urn(): wu for wu in workunits}
    assert by_urn[dataset_urn].is_primary_source
    assert not by_urn[source_dataset_urn].is_primary_source


def test_trino_gen_siblings_workunit_connector_side_patches_rather_than_upserts():
    """The connector side must patch, so an existing pairing (e.g. dbt) survives."""
    source = get_test_trino_source()
    dataset_urn = (
        "urn:li:dataset:(urn:li:dataPlatform:trino,oracle_catalog.hr.employees,PROD)"
    )
    source_dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:oracle,hr.employees,PROD)"

    workunits = list(source.gen_siblings_workunit(dataset_urn, source_dataset_urn))
    connector_wu = next(wu for wu in workunits if wu.get_urn() == source_dataset_urn)

    mcp = connector_wu.metadata
    assert isinstance(mcp, MetadataChangeProposalClass)
    assert mcp.changeType == ChangeTypeClass.PATCH
    assert mcp.aspectName == "siblings"
