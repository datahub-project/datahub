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
from datahub.ingestion.source.powerbi.powerbi import Mapper
from datahub.ingestion.source.powerbi.rest_api_wrapper.data_classes import (
    Column,
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
ENDPOINT_HOST = "my-fabric-endpoint.datawarehouse.fabric.microsoft.com"
DS_URN = (
    "urn:li:dataset:(urn:li:dataPlatform:powerbi,TestWorkspace.TestDataset.claims,PROD)"
)


def _m_query(server: str, database: str) -> str:
    return (
        "let\n"
        f'    Source = Sql.Database("{server}", "{database}"),\n'
        '    dbo_claims = Source{[Schema="dbo",Item="claims"]}[Data]\n'
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


def _warehouse() -> FabricArtifact:
    return FabricArtifact(
        id=WAREHOUSE_ID,
        name="Claims_Warehouse",
        artifact_type="Warehouse",
        workspace_id=WORKSPACE_ID,
    )


def _lakehouse_with_endpoint() -> List[FabricArtifact]:
    return [
        FabricArtifact(
            id=LAKEHOUSE_ID,
            name="Claims_Lakehouse",
            artifact_type="Lakehouse",
            workspace_id=WORKSPACE_ID,
        ),
        FabricArtifact(
            id=SQL_ENDPOINT_ID,
            name="Claims_Lakehouse",
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
    assert len(mcps) == 1
    aspect = mcps[0].aspect
    assert isinstance(aspect, UpstreamLineageClass)
    return {
        "upstreams": [u.dataset for u in aspect.upstreams],
        "column_upstreams": [
            u for f in aspect.fineGrainedLineages or [] for u in f.upstreams or []
        ],
    }


def _onelake_urn(item_id: str, platform_instance: str = "", env: str = "PROD") -> str:
    prefix = f"{platform_instance}." if platform_instance else ""
    return f"urn:li:dataset:(urn:li:dataPlatform:fabric-onelake,{prefix}{WORKSPACE_ID}.{item_id}.dbo.claims,{env})"


def test_warehouse_endpoint_by_id_resolves_to_onelake_table() -> None:
    mapper = _mapper()
    lineage = _lineage(
        mapper, _workspace([_warehouse()]), _m_query(ENDPOINT_HOST, WAREHOUSE_ID)
    )

    assert lineage["upstreams"] == [_onelake_urn(WAREHOUSE_ID)]
    assert lineage["column_upstreams"] == [
        f"urn:li:schemaField:({_onelake_urn(WAREHOUSE_ID)},claim_id)"
    ]
    report = mapper._Mapper__reporter  # type: ignore[attr-defined]
    assert report.fabric_endpoint_references_resolved == 1
    assert report.fabric_endpoints_unresolved == 0


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


def test_unknown_endpoint_keeps_mssql_edge_and_warns() -> None:
    mapper = _mapper()
    lineage = _lineage(
        mapper, _workspace([_warehouse()]), _m_query(ENDPOINT_HOST, "Other_Lakehouse")
    )

    assert lineage["upstreams"] == [
        "urn:li:dataset:(urn:li:dataPlatform:mssql,other_lakehouse.dbo.claims,PROD)"
    ]
    report = mapper._Mapper__reporter  # type: ignore[attr-defined]
    assert report.fabric_endpoints_unresolved == 1
    assert report.fabric_endpoint_references_resolved == 0
    assert "Fabric Endpoint Not Resolved" in {w.title for w in report.warnings}

    # The same model mapped again (report + tile) is counted once.
    _lineage(
        mapper, _workspace([_warehouse()]), _m_query(ENDPOINT_HOST, "Other_Lakehouse")
    )
    assert report.fabric_endpoints_unresolved == 1


def test_non_fabric_sql_server_is_untouched() -> None:
    mapper = _mapper()
    lineage = _lineage(
        mapper, _workspace([_warehouse()]), _m_query("sql.example.com", "claims_db")
    )

    assert lineage["upstreams"] == [
        "urn:li:dataset:(urn:li:dataPlatform:mssql,claims_db.dbo.claims,PROD)"
    ]
    report = mapper._Mapper__reporter  # type: ignore[attr-defined]
    assert report.fabric_endpoints_unresolved == 0
    assert report.fabric_endpoint_references_resolved == 0
