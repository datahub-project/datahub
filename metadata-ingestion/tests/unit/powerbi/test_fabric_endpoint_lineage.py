"""Sql.Database queries over a Fabric SQL analytics endpoint host resolve to
the OneLake table behind the endpoint (Mapper.extract_lineage)."""

from typing import Any, Dict, List, Optional

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.powerbi.config import (
    PowerBiDashboardSourceConfig,
    PowerBiDashboardSourceReport,
)
from datahub.ingestion.source.powerbi.dataplatform_instance_resolver import (
    create_dataplatform_instance_resolver,
)
from datahub.ingestion.source.powerbi.m_query.pattern_handler import (
    FABRIC_SQL_ENDPOINT_HOST_SUFFIX,
)
from datahub.ingestion.source.powerbi.powerbi import Mapper
from datahub.ingestion.source.powerbi.rest_api_wrapper.data_classes import (
    Column,
    Dataflow,
    DataflowQuery,
    FabricArtifact,
    PowerBIDataset,
    Table,
    Workspace,
)
from datahub.metadata.schema_classes import StringTypeClass, UpstreamLineageClass

WORKSPACE_ID = "ff23fbe3-7418-42f8-a675-9f10eb2b78cb"
WAREHOUSE_ID = "9a8b7c6d-5e4f-4a3b-8c2d-1e0f9a8b7c6d"
LAKEHOUSE_ID = "3c4d5e6f-7a8b-4c9d-8e0f-1a2b3c4d5e6f"
SQL_ENDPOINT_ID = "6f2e7d8c-9b0a-4c1d-8e2f-3a4b5c6d7e8f"
ENDPOINT_HOST = f"my-fabric-endpoint{FABRIC_SQL_ENDPOINT_HOST_SUFFIX}"
DS_URN = (
    "urn:li:dataset:(urn:li:dataPlatform:powerbi,TestWorkspace.TestDataset.claims,PROD)"
)
NAVIGATION = 'Source{[Schema="dbo",Item="claims"]}[Data]'


def _m_query(
    server: str,
    database: str,
    navigation: str = NAVIGATION,
    record: str = "",
) -> str:
    return (
        "let\n"
        f'    Source = Sql.Database("{server}", "{database}"{record}),\n'
        f"    dbo_claims = {navigation}\n"
        "in\n"
        "    dbo_claims"
    )


def _mapper(**config_overrides: Any) -> Mapper:
    config = PowerBiDashboardSourceConfig(
        tenant_id="test-tenant-id",
        client_id="test-client-id",
        client_secret="test-client-secret",
        extract_lineage=True,
        extract_column_level_lineage=True,
        **config_overrides,
    )
    return Mapper(
        ctx=PipelineContext(run_id="test-run-id"),
        config=config,
        reporter=PowerBiDashboardSourceReport(),
        dataplatform_instance_resolver=create_dataplatform_instance_resolver(config),
    )


def _report(mapper: Mapper) -> PowerBiDashboardSourceReport:
    return mapper._Mapper__reporter  # type: ignore[attr-defined]


def _workspace(artifacts: Optional[List[FabricArtifact]] = None) -> Workspace:
    return Workspace(
        id=WORKSPACE_ID,
        name="TestWorkspace",
        type="Workspace",
        webUrl=None,
        datasets={},
        dashboards={},
        reports={},
        report_endorsements={},
        dashboard_endorsements={},
        scan_result={},
        independent_datasets={},
        app=None,
        fabric_artifacts={a.id: a for a in artifacts or []},
    )


def _warehouse(
    item_id: str = WAREHOUSE_ID, name: str = "Claims_Warehouse"
) -> FabricArtifact:
    return FabricArtifact(
        id=item_id,
        name=name,
        artifact_type="Warehouse",
        workspace_id=WORKSPACE_ID,
    )


def _lakehouse_with_endpoint(name: str = "Claims_Lakehouse") -> List[FabricArtifact]:
    return [
        FabricArtifact(
            id=LAKEHOUSE_ID,
            name=name,
            artifact_type="Lakehouse",
            workspace_id=WORKSPACE_ID,
        ),
        FabricArtifact(
            id=SQL_ENDPOINT_ID,
            name=name,
            artifact_type="SQLAnalyticsEndpoint",
            workspace_id=WORKSPACE_ID,
            physical_item_ids=[LAKEHOUSE_ID],
        ),
    ]


def _table(expression: str) -> Table:
    dataset = PowerBIDataset(
        id="ds-123",
        name="TestDataset",
        description="Test dataset",
        webUrl=None,
        workspace_id=WORKSPACE_ID,
        workspace_name="TestWorkspace",
        parameters={},
        tables=[],
        tags=[],
        configuredBy=None,
    )
    table = Table(
        name="claims",
        full_name="TestWorkspace.TestDataset.claims",
        expression=expression,
        storage_mode="Import",
        columns=[
            Column(
                name="claim_id",
                dataType="String",
                isHidden=False,
                datahubDataType=StringTypeClass(),
            )
        ],
    )
    table.dataset = dataset
    return table


def _lineage(mapper: Mapper, workspace: Workspace, expression: str) -> Dict[str, Any]:
    mcps = mapper.extract_lineage(_table(expression), DS_URN, workspace)
    if not mcps:
        return {"upstreams": [], "column_upstreams": []}
    assert len(mcps) == 1
    aspect = mcps[0].aspect
    assert isinstance(aspect, UpstreamLineageClass)
    return {
        "upstreams": [u.dataset for u in aspect.upstreams],
        "column_upstreams": [
            u for f in aspect.fineGrainedLineages or [] for u in f.upstreams or []
        ],
    }


def _onelake_urn(
    item_id: str,
    platform_instance: str = "",
    env: str = "PROD",
    table: str = "dbo.claims",
) -> str:
    prefix = f"{platform_instance}." if platform_instance else ""
    return f"urn:li:dataset:(urn:li:dataPlatform:fabric-onelake,{prefix}{WORKSPACE_ID}.{item_id}.{table},{env})"


MSSQL_OTHER = (
    "urn:li:dataset:(urn:li:dataPlatform:mssql,other_lakehouse.dbo.claims,PROD)"
)


def test_warehouse_endpoint_by_id_resolves_to_onelake_table() -> None:
    mapper = _mapper()
    lineage = _lineage(
        mapper, _workspace([_warehouse()]), _m_query(ENDPOINT_HOST, WAREHOUSE_ID)
    )

    assert lineage["upstreams"] == [_onelake_urn(WAREHOUSE_ID)]
    assert lineage["column_upstreams"] == [
        f"urn:li:schemaField:({_onelake_urn(WAREHOUSE_ID)},claim_id)"
    ]
    report = _report(mapper)
    assert report.fabric_endpoint_references_resolved == 1
    assert report.fabric_endpoint_references_unresolved == 0
    assert report.warnings == []


def test_lakehouse_endpoint_by_display_name_resolves_to_lakehouse_table() -> None:
    mapper = _mapper()
    # Power BI Desktop writes the item's display name as the database; the
    # endpoint artifact maps it back to the lakehouse the OneLake URN needs.
    lineage = _lineage(
        mapper,
        _workspace(_lakehouse_with_endpoint()),
        _m_query(ENDPOINT_HOST, "claims_lakehouse"),
    )

    assert lineage["upstreams"] == [_onelake_urn(LAKEHOUSE_ID)]
    report = _report(mapper)
    assert report.fabric_endpoint_references_resolved == 1
    assert report.fabric_endpoint_references_unresolved == 0
    assert report.fabric_endpoint_references_ambiguous == 0
    assert report.fabric_endpoint_m_shapes_unsupported == 0
    assert report.warnings == []


def test_display_name_with_spaces_and_mixed_case_matches() -> None:
    mapper = _mapper()
    lineage = _lineage(
        mapper,
        _workspace([_warehouse(name="Sales Warehouse 2024")]),
        _m_query(ENDPOINT_HOST, "sales WAREHOUSE 2024"),
    )

    assert lineage["upstreams"] == [_onelake_urn(WAREHOUSE_ID)]


def test_region_prefixed_endpoint_host_is_recognised() -> None:
    mapper = _mapper()
    host = f"abc123-def456.z01{FABRIC_SQL_ENDPOINT_HOST_SUFFIX}"
    lineage = _lineage(mapper, _workspace([_warehouse()]), _m_query(host, WAREHOUSE_ID))

    assert lineage["upstreams"] == [_onelake_urn(WAREHOUSE_ID)]


def test_missing_schema_defaults_to_dbo() -> None:
    mapper = _mapper()
    lineage = _lineage(
        mapper,
        _workspace([_warehouse()]),
        _m_query(
            ENDPOINT_HOST, WAREHOUSE_ID, navigation='Source{[Item="claims"]}[Data]'
        ),
    )

    assert lineage["upstreams"] == [_onelake_urn(WAREHOUSE_ID)]


def test_missing_item_keeps_mssql_edge_and_reports_shape() -> None:
    mapper = _mapper()
    lineage = _lineage(
        mapper,
        _workspace([_warehouse()]),
        _m_query(
            ENDPOINT_HOST, WAREHOUSE_ID, navigation='Source{[Schema="dbo"]}[Data]'
        ),
    )

    assert not any("fabric-onelake" in urn for urn in lineage["upstreams"])
    assert all("mssql" in urn for urn in lineage["upstreams"])
    report = _report(mapper)
    assert report.fabric_endpoint_m_shapes_unsupported == 1
    assert report.fabric_endpoint_references_resolved == 0
    assert "Fabric Endpoint M Shape Unsupported" in {w.title for w in report.warnings}


def test_native_query_tables_are_swapped() -> None:
    mapper = _mapper()
    lineage = _lineage(
        mapper,
        _workspace([_warehouse()]),
        _m_query(
            ENDPOINT_HOST,
            "Claims_Warehouse",
            navigation="Source",
            record=', [Query="SELECT claim_id FROM dbo.claims"]',
        ),
    )

    assert lineage["upstreams"] == [_onelake_urn(WAREHOUSE_ID)]
    assert lineage["column_upstreams"] == [
        f"urn:li:schemaField:({_onelake_urn(WAREHOUSE_ID)},claim_id)"
    ]
    report = _report(mapper)
    assert report.fabric_endpoint_references_resolved == 1
    assert report.warnings == []


def test_sql_databases_navigation_keeps_mssql_edge_and_reports_shape() -> None:
    mapper = _mapper()
    expression = (
        "let\n"
        f'    Source = Sql.Databases("{ENDPOINT_HOST}"),\n'
        '    db = Source{[Name="Claims_Warehouse"]}[Data],\n'
        '    dbo_claims = db{[Schema="dbo",Item="claims"]}[Data]\n'
        "in\n"
        "    dbo_claims"
    )
    lineage = _lineage(mapper, _workspace([_warehouse()]), expression)

    assert lineage["upstreams"] == [
        "urn:li:dataset:(urn:li:dataPlatform:mssql,claims_warehouse.dbo.claims,PROD)"
    ]
    report = _report(mapper)
    assert report.fabric_endpoint_m_shapes_unsupported == 1
    assert "Fabric Endpoint M Shape Unsupported" in {w.title for w in report.warnings}


def test_endpoint_uses_server_to_platform_instance_keyed_by_workspace() -> None:
    mapper = _mapper(
        server_to_platform_instance={
            WORKSPACE_ID: {"platform_instance": "fabric_prod", "env": "DEV"}
        }
    )
    lineage = _lineage(
        mapper, _workspace([_warehouse()]), _m_query(ENDPOINT_HOST, WAREHOUSE_ID)
    )

    assert lineage["upstreams"] == [_onelake_urn(WAREHOUSE_ID, "fabric_prod", "DEV")]


def test_swapped_edge_survives_a_narrowed_dataset_type_mapping() -> None:
    # The admin-scan fixtures narrow dataset_type_mapping to Sql only: the
    # mssql edge used to pass that filter, so its OneLake replacement must
    # too, or an existing edge silently becomes none.
    mapper = _mapper(dataset_type_mapping={"Sql": "mssql"})
    lineage = _lineage(
        mapper, _workspace([_warehouse()]), _m_query(ENDPOINT_HOST, WAREHOUSE_ID)
    )

    assert lineage["upstreams"] == [_onelake_urn(WAREHOUSE_ID)]
    assert lineage["column_upstreams"] == [
        f"urn:li:schemaField:({_onelake_urn(WAREHOUSE_ID)},claim_id)"
    ]


def test_endpoint_outside_workspace_patterns_keeps_mssql_edge_and_warns() -> None:
    # A lakehouse in a workspace that workspace_id_pattern excluded is not
    # among this workspace's Fabric items, so only its name is known.
    mapper = _mapper()
    lineage = _lineage(
        mapper, _workspace([_warehouse()]), _m_query(ENDPOINT_HOST, "Other_Lakehouse")
    )

    assert lineage["upstreams"] == [MSSQL_OTHER]
    report = _report(mapper)
    assert report.fabric_endpoint_references_unresolved == 1
    assert report.fabric_endpoint_references_resolved == 0
    assert "Fabric Endpoint Not Resolved" in {w.title for w in report.warnings}

    # The same model mapped again (report + tile) is counted once.
    _lineage(
        mapper, _workspace([_warehouse()]), _m_query(ENDPOINT_HOST, "Other_Lakehouse")
    )
    assert report.fabric_endpoint_references_unresolved == 1


def test_display_name_shared_by_lakehouse_and_warehouse_is_ambiguous() -> None:
    mapper = _mapper()
    artifacts = _lakehouse_with_endpoint(name="Sales") + [_warehouse(name="Sales")]
    lineage = _lineage(mapper, _workspace(artifacts), _m_query(ENDPOINT_HOST, "Sales"))

    assert lineage["upstreams"] == [
        "urn:li:dataset:(urn:li:dataPlatform:mssql,sales.dbo.claims,PROD)"
    ]
    report = _report(mapper)
    assert report.fabric_endpoint_references_ambiguous == 1
    assert report.fabric_endpoint_references_resolved == 0
    assert "Fabric Endpoint Name Ambiguous" in {w.title for w in report.warnings}

    # Naming the warehouse by id is still unambiguous.
    lineage = _lineage(
        mapper, _workspace(artifacts), _m_query(ENDPOINT_HOST, WAREHOUSE_ID)
    )
    assert lineage["upstreams"] == [_onelake_urn(WAREHOUSE_ID)]


def test_non_fabric_sql_server_is_untouched() -> None:
    mapper = _mapper()
    lineage = _lineage(
        mapper, _workspace([_warehouse()]), _m_query("sql.example.com", "claims_db")
    )

    assert lineage["upstreams"] == [
        "urn:li:dataset:(urn:li:dataPlatform:mssql,claims_db.dbo.claims,PROD)"
    ]
    report = _report(mapper)
    assert report.fabric_endpoint_references_unresolved == 0
    assert report.fabric_endpoint_references_resolved == 0
    assert report.fabric_endpoint_m_shapes_unsupported == 0


def test_dataflow_entity_over_endpoint_gets_onelake_edge() -> None:
    # A Dataflow Gen2 entity reading the warehouse endpoint is mapped the
    # same way as a semantic-model table, not left with an mssql edge.
    mapper = _mapper()
    workspace = _workspace([_warehouse()])
    mapper.workspace_key = mapper.make_container_key_for_workspace(workspace)
    dataflow = Dataflow(
        id="8b9c0d1e-2f3a-4b4c-8d5e-6f7a8b9c0d1e",
        name="Claims Dataflow",
        description=None,
        workspace_name=workspace.name,
        queries=[
            DataflowQuery(
                name="claims",
                expression=_m_query(ENDPOINT_HOST, WAREHOUSE_ID),
                is_shared=True,
                load_enabled=True,
            )
        ],
    )

    lineage_aspects = [
        mcp.aspect
        for mcp in mapper.to_datahub_dataflow(dataflow, workspace)
        if isinstance(mcp.aspect, UpstreamLineageClass)
    ]

    assert len(lineage_aspects) == 1
    assert [u.dataset for u in lineage_aspects[0].upstreams] == [
        _onelake_urn(WAREHOUSE_ID)
    ]
    report = _report(mapper)
    assert report.fabric_endpoint_references_resolved == 1
    assert report.warnings == []


def test_swapped_endpoint_keeps_source_column_lineage_of_a_renamed_field() -> None:
    # A field the model renamed keeps its TMDL sourceColumn on the upstream
    # side; the endpoint swap must carry that edge onto the OneLake table
    # rather than leaving it on the mssql URN the coarse edge no longer has.
    mapper = _mapper()
    table = _table(_m_query(ENDPOINT_HOST, WAREHOUSE_ID))
    table.columns = [
        Column(
            name="Claim Number",
            dataType="String",
            isHidden=False,
            datahubDataType=StringTypeClass(),
            source_column="CLAIM_ID",
        )
    ]
    mcps = mapper.extract_lineage(table, DS_URN, _workspace([_warehouse()]))
    assert len(mcps) == 1
    aspect = mcps[0].aspect
    assert isinstance(aspect, UpstreamLineageClass)
    assert [u.dataset for u in aspect.upstreams] == [_onelake_urn(WAREHOUSE_ID)]
    assert [
        (edge.downstreams, edge.upstreams) for edge in aspect.fineGrainedLineages or []
    ] == [
        (
            [f"urn:li:schemaField:({DS_URN},Claim Number)"],
            [f"urn:li:schemaField:({_onelake_urn(WAREHOUSE_ID)},CLAIM_ID)"],
        )
    ]
