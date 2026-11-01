"""Dataflow Gen2 ingestion (``extract_dataflows`` in workspace_apis mode):
definition fetch and mashup parsing in PowerBiAPI, entity emission and
lineage in the Mapper."""

import base64
import json
from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock

import pytest
import requests
import requests_mock

from datahub.ingestion.source.powerbi.config import (
    PowerBiDashboardSourceConfig,
    PowerBiDashboardSourceReport,
)
from datahub.ingestion.source.powerbi.dataplatform_instance_resolver import (
    create_dataplatform_instance_resolver,
)
from datahub.ingestion.source.powerbi.powerbi import Mapper
from datahub.ingestion.source.powerbi.rest_api_wrapper.data_classes import (
    Dataflow,
    PowerBIDataset,
    Table,
    Workspace,
)
from datahub.ingestion.source.powerbi.rest_api_wrapper.dataflow_definition_parser import (
    compose_let_expression,
    parse_section_document,
)
from datahub.ingestion.source.powerbi.rest_api_wrapper.fabric_resolver import (
    FABRIC_BASE_URL,
    FabricDefinitionResolver,
)
from datahub.ingestion.source.powerbi.rest_api_wrapper.powerbi_api import PowerBiAPI
from datahub.metadata.schema_classes import (
    ContainerClass,
    ContainerPropertiesClass,
    SubTypesClass,
    UpstreamLineageClass,
)

WORKSPACE_ID = "64ed5cad-7c10-4684-8180-826122881108"
OTHER_WORKSPACE_ID = "11111111-2222-4333-8444-555555555555"
DATAFLOW_ID = "8b9c0d1e-2f3a-4b4c-8d5e-6f7a8b9c0d1e"
OTHER_DATAFLOW_ID = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"
MODEL_ID = "05169cd2-e713-41e6-9600-1d8066d95445"

ITEMS_URL = f"{FABRIC_BASE_URL}/workspaces/{WORKSPACE_ID}/items"
DEFINITION_URL = (
    f"{FABRIC_BASE_URL}/workspaces/{WORKSPACE_ID}/dataflows/{DATAFLOW_ID}/getDefinition"
)

SNOWFLAKE_POLICIES = (
    "urn:li:dataset:(urn:li:dataPlatform:snowflake,sales_db.public.policies,PROD)"
)
MSSQL_AGENTS = "urn:li:dataset:(urn:li:dataPlatform:mssql,agency.dbo.agents,PROD)"
POLICIES_ENTITY_URN = f"urn:li:dataset:(urn:li:dataPlatform:powerbi,dataflows.{DATAFLOW_ID}.Policies,PROD)"
ACTIVE_AGENTS_ENTITY_URN = f"urn:li:dataset:(urn:li:dataPlatform:powerbi,dataflows.{DATAFLOW_ID}.Active_Agents,PROD)"

MASHUP = """section Section1;
shared Policies = let
    Source = Snowflake.Databases("my-account.snowflakecomputing.com","COMPUTE_WH"),
    Db = Source{[Name="SALES_DB",Kind="Database"]}[Data],
    Sch = Db{[Name="PUBLIC",Kind="Schema"]}[Data],
    Tbl = Sch{[Name="POLICIES",Kind="Table"]}[Data]
in
    Tbl;
// A helper query feeding the loaded one below; not an entity.
Agents_Staging = let
    Source = Sql.Database("sql-host.example.com", "Agency"),
    Agents = Source{[Schema="dbo",Item="Agents"]}[Data]
in
    Agents;
[StagingDefinition = [Kind = "FastCopy"]]
shared #"Active Agents" = let
    Source = Agents_Staging,
    Active = Table.SelectRows(Source, each [Status] = "Active; Licensed")
in
    Active;
"""

QUERY_METADATA = {
    "formatVersion": "202502",
    "queriesMetadata": {
        "Policies": {"queryName": "Policies", "loadEnabled": True},
        "Agents_Staging": {"queryName": "Agents_Staging", "loadEnabled": False},
        "Active Agents": {"queryName": "Active Agents", "loadEnabled": True},
    },
}


def dataflow_table_expression(workspace_id: str, dataflow_id: str) -> str:
    return f"""let
    Source = PowerPlatform.Dataflows(null),
    Workspaces = Source{{[Id="Workspaces"]}}[Data],
    Workspace = Workspaces{{[workspaceId="{workspace_id}"]}}[Data],
    Dataflow = Workspace{{[dataflowId="{dataflow_id}"]}}[Data],
    Policies = Dataflow{{[entity="Policies",version=""]}}[Data]
in
    Policies"""


class FakeMsal:
    def acquire_token_for_client(self, scopes: List[str]) -> Dict[str, Any]:
        return {"access_token": "fabric-token", "expires_in": 3600}


def _config(**overrides: Any) -> PowerBiDashboardSourceConfig:
    base: Dict[str, Any] = {
        "tenant_id": "fake",
        "client_id": "foo",
        "client_secret": "bar",
        "metadata_api_mode": "workspace_apis",
        "extract_dataflows": True,
        "extract_column_level_lineage": False,
        "convert_lineage_urns_to_lowercase": True,
    }
    base.update(overrides)
    return PowerBiDashboardSourceConfig.model_validate(base)


def _workspace() -> Workspace:
    return Workspace(
        id=WORKSPACE_ID,
        name="demo-workspace",
        type="Workspace",
        webUrl=f"https://app.powerbi.com/groups/{WORKSPACE_ID}",
        datasets={},
        dashboards={},
        reports={},
        report_endorsements={},
        dashboard_endorsements={},
        scan_result={},
        independent_datasets={},
        app=None,
    )


def _definition_body(
    mashup: Optional[str] = MASHUP, query_metadata: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    parts: List[Dict[str, str]] = []
    if mashup is not None:
        parts.append(
            {
                "path": "mashup.pq",
                "payload": base64.b64encode(mashup.encode()).decode(),
                "payloadType": "InlineBase64",
            }
        )
    metadata = QUERY_METADATA if query_metadata is None else query_metadata
    parts.append(
        {
            "path": "queryMetadata.json",
            "payload": base64.b64encode(json.dumps(metadata).encode()).decode(),
            "payloadType": "InlineBase64",
        }
    )
    return {"definition": {"parts": parts}}


def _api(config: Optional[PowerBiDashboardSourceConfig] = None) -> PowerBiAPI:
    """A PowerBiAPI whose Fabric resolver is real (so the HTTP layer is
    exercised through requests_mock) and whose Power BI resolver is mocked."""
    api = PowerBiAPI.__new__(PowerBiAPI)
    api._PowerBiAPI__config = config or _config()  # type: ignore[attr-defined]
    api.reporter = PowerBiDashboardSourceReport()
    api.dataset_registry = {}
    api.dataflow_registry = {}
    api.log_http_error = MagicMock()  # type: ignore[method-assign]
    api._PowerBiAPI__workspace_users = {}  # type: ignore[attr-defined]

    regular = MagicMock()
    regular.get_datasets.return_value = []
    api._PowerBiAPI__regular_api_resolver = regular  # type: ignore[attr-defined]
    api._PowerBiAPI__fabric_resolver = FabricDefinitionResolver(  # type: ignore[attr-defined]
        msal_client=FakeMsal(),  # type: ignore[arg-type]
        request_session=requests.Session(),
        lro_timeout_seconds=10,
    )
    return api


def _register_items(m: requests_mock.Mocker) -> None:
    m.get(
        ITEMS_URL,
        json={
            "value": [
                {
                    "id": DATAFLOW_ID,
                    "type": "Dataflow",
                    "displayName": "Policies Dataflow",
                    "description": "Curated policies",
                    "workspaceId": WORKSPACE_ID,
                },
                {
                    "id": MODEL_ID,
                    "type": "SemanticModel",
                    "displayName": "Insurance Model",
                    "workspaceId": WORKSPACE_ID,
                },
            ]
        },
    )


def _mapper(
    api: PowerBiAPI, config: Optional[PowerBiDashboardSourceConfig] = None
) -> Mapper:
    config = config or _config()
    mapper = Mapper(
        ctx=MagicMock(),
        config=config,
        reporter=api.reporter,
        dataplatform_instance_resolver=create_dataplatform_instance_resolver(config),
        dataflow_registry=api.dataflow_registry,
    )
    mapper.workspace_key = mapper.make_container_key_for_workspace(_workspace())
    return mapper


def _fetched_dataflow(api: PowerBiAPI, workspace: Workspace) -> Dataflow:
    with requests_mock.Mocker() as m:
        _register_items(m)
        m.post(DEFINITION_URL, json=_definition_body())
        api.fill_metadata_from_definitions([workspace])
        assert m.last_request is not None
        assert m.last_request.headers["Authorization"] == "Bearer fabric-token"
    return workspace.dataflows[DATAFLOW_ID]


def _lineage_by_urn(mcps: List[Any]) -> Dict[str, List[str]]:
    return {
        mcp.entityUrn: sorted(u.dataset for u in mcp.aspect.upstreams)
        for mcp in mcps
        if isinstance(mcp.aspect, UpstreamLineageClass)
    }


def _model_table(expression: str) -> Table:
    dataset = PowerBIDataset(
        id=MODEL_ID,
        name="Insurance Model",
        description="",
        webUrl=None,
        workspace_id=WORKSPACE_ID,
        workspace_name="demo-workspace",
        parameters={},
        tables=[],
        tags=[],
    )
    table = Table(
        name="Dataflow Policies",
        full_name="Insurance_Model.Dataflow_Policies",
        expression=expression,
        dataset=dataset,
    )
    dataset.tables.append(table)
    return table


def test_dataflow_entities_are_emitted_with_upstream_lineage() -> None:
    api = _api()
    workspace = _workspace()
    dataflow = _fetched_dataflow(api, workspace)

    assert api.reporter.dataflows_fetched == 1
    assert api.reporter.dataflow_entities_parsed == 2
    assert [q.name for q in dataflow.entities()] == ["Policies", "Active Agents"]
    assert [q.name for q in dataflow.queries if not q.is_entity] == ["Agents_Staging"]
    assert api.dataflow_registry[DATAFLOW_ID] is dataflow
    assert dataflow.webUrl == f"{workspace.webUrl}/dataflows/{DATAFLOW_ID}"

    mcps = _mapper(api).to_datahub_dataflow(dataflow, workspace)

    container_urn = next(
        mcp.entityUrn
        for mcp in mcps
        if isinstance(mcp.aspect, ContainerPropertiesClass)
    )
    subtypes = {
        mcp.entityUrn: mcp.aspect.typeNames
        for mcp in mcps
        if isinstance(mcp.aspect, SubTypesClass)
    }
    assert subtypes[container_urn] == ["Dataflow"]
    assert subtypes[POLICIES_ENTITY_URN] == ["Dataflow"]
    assert subtypes[ACTIVE_AGENTS_ENTITY_URN] == ["Dataflow"]
    assert all(
        mcp.aspect.container == container_urn
        for mcp in mcps
        if isinstance(mcp.aspect, ContainerClass) and mcp.entityUrn != container_urn
    )
    # The helper query is not an entity, but the entity built on it still
    # resolves to the helper's source.
    assert _lineage_by_urn(mcps) == {
        POLICIES_ENTITY_URN: [SNOWFLAKE_POLICIES],
        ACTIVE_AGENTS_ENTITY_URN: [MSSQL_AGENTS],
    }
    assert api.reporter.dataflow_queries_unparseable == 0
    assert not api.reporter.warnings


def test_unparseable_query_is_a_warning_and_does_not_block_the_others() -> None:
    api = _api()
    workspace = _workspace()
    broken = MASHUP.replace("Agents = Source{[Schema", "Agents = Source{{[Schema")
    with requests_mock.Mocker() as m:
        _register_items(m)
        m.post(DEFINITION_URL, json=_definition_body(mashup=broken))
        api.fill_metadata_from_definitions([workspace])
    dataflow = workspace.dataflows[DATAFLOW_ID]

    mcps = _mapper(api).to_datahub_dataflow(dataflow, workspace)

    assert _lineage_by_urn(mcps) == {POLICIES_ENTITY_URN: [SNOWFLAKE_POLICIES]}
    assert api.reporter.dataflow_queries_unparseable == 1
    assert [w.title for w in api.reporter.warnings] == ["Dataflow Query Not Parsed"]
    # Both entities are still emitted; only the lineage through the broken
    # helper is missing.
    assert ACTIVE_AGENTS_ENTITY_URN in {mcp.entityUrn for mcp in mcps}


def test_semantic_model_table_links_to_the_dataflow_entity() -> None:
    api = _api()
    workspace = _workspace()
    _fetched_dataflow(api, workspace)
    table = _model_table(dataflow_table_expression(WORKSPACE_ID, DATAFLOW_ID))

    mapper = _mapper(api)
    mcps = mapper.extract_lineage(
        table, mapper.make_dataset_urn_for_table(table), workspace
    )

    assert _lineage_by_urn(mcps) == {
        "urn:li:dataset:(urn:li:dataPlatform:powerbi,Insurance_Model.Dataflow_Policies,PROD)": [
            POLICIES_ENTITY_URN
        ]
    }
    assert api.reporter.dataflow_references_resolved == 1
    assert not api.reporter.warnings


def test_powerbi_dataflows_function_is_also_recognized() -> None:
    api = _api()
    workspace = _workspace()
    _fetched_dataflow(api, workspace)
    table = _model_table(
        f"""let
    Source = PowerBI.Dataflows(null),
    Ws = Source{{[workspaceId="{WORKSPACE_ID}"]}}[Data],
    Df = Ws{{[dataflowId="{DATAFLOW_ID}"]}}[Data],
    Policies = Df{{[entity="Policies"]}}[Data]
in
    Policies"""
    )

    mapper = _mapper(api)
    mcps = mapper.extract_lineage(table, "urn:li:dataset:(x,y,PROD)", workspace)

    assert _lineage_by_urn(mcps) == {"urn:li:dataset:(x,y,PROD)": [POLICIES_ENTITY_URN]}


def test_reference_to_dataflow_outside_the_run_warns_without_an_edge() -> None:
    api = _api()
    workspace = _workspace()
    _fetched_dataflow(api, workspace)
    # Another workspace, not in the allow pattern, so its dataflow was never
    # fetched and is absent from the registry.
    table = _model_table(
        dataflow_table_expression(OTHER_WORKSPACE_ID, OTHER_DATAFLOW_ID)
    )

    mapper = _mapper(api)
    mcps = mapper.extract_lineage(table, "urn:li:dataset:(x,y,PROD)", workspace)

    assert mcps == []
    assert api.reporter.dataflow_references_not_ingested == 1
    assert api.reporter.dataflow_references_resolved == 0
    warning = api.reporter.warnings[0]
    assert warning.title == "Dataflow Not Ingested"
    assert OTHER_DATAFLOW_ID in str(warning.context)


def test_reference_to_an_entity_the_dataflow_does_not_load_warns() -> None:
    api = _api()
    workspace = _workspace()
    _fetched_dataflow(api, workspace)
    table = _model_table(
        dataflow_table_expression(WORKSPACE_ID, DATAFLOW_ID).replace(
            'entity="Policies"', 'entity="Agents_Staging"'
        )
    )

    mapper = _mapper(api)
    mcps = mapper.extract_lineage(table, "urn:li:dataset:(x,y,PROD)", workspace)

    assert mcps == []
    assert api.reporter.dataflow_references_not_ingested == 1
    assert [w.title for w in api.reporter.warnings] == ["Dataflow Not Ingested"]


@pytest.mark.parametrize(
    "status,title",
    [
        (403, "Fabric API Permission Denied"),
        (500, "Dataflow Definition Not Fetched"),
    ],
)
def test_definition_fetch_failure_is_a_failure(status: int, title: str) -> None:
    api = _api()
    workspace = _workspace()
    with requests_mock.Mocker() as m:
        _register_items(m)
        m.post(DEFINITION_URL, status_code=status, json={"errorCode": "x"})
        api.fill_metadata_from_definitions([workspace])

    assert workspace.dataflows == {}
    assert api.dataflow_registry == {}
    assert api.reporter.fabric_definitions_failed == 1
    assert [f.title for f in api.reporter.failures] == [title]


def test_definition_without_mashup_is_a_failure() -> None:
    api = _api()
    workspace = _workspace()
    with requests_mock.Mocker() as m:
        _register_items(m)
        m.post(DEFINITION_URL, json=_definition_body(mashup=None))
        api.fill_metadata_from_definitions([workspace])

    assert workspace.dataflows == {}
    assert [f.title for f in api.reporter.failures] == ["Dataflow Mashup Not Parsed"]


def test_broken_query_metadata_falls_back_to_shared_keyword() -> None:
    api = _api()
    workspace = _workspace()
    with requests_mock.Mocker() as m:
        _register_items(m)
        body = _definition_body()
        body["definition"]["parts"][1]["payload"] = base64.b64encode(
            b"{not json"
        ).decode()
        m.post(DEFINITION_URL, json=body)
        api.fill_metadata_from_definitions([workspace])

    dataflow = workspace.dataflows[DATAFLOW_ID]
    assert [q.name for q in dataflow.entities()] == ["Policies", "Active Agents"]
    assert [w.title for w in api.reporter.warnings] == ["Dataflow Part Not Parsed"]


def test_dataflows_are_not_fetched_unless_enabled() -> None:
    api = _api(_config(extract_dataflows=False))
    workspace = _workspace()
    with requests_mock.Mocker() as m:
        _register_items(m)
        api.fill_metadata_from_definitions([workspace])
        assert not any(r.url.startswith(DEFINITION_URL) for r in m.request_history)
    assert workspace.dataflows == {}


def test_section_document_parsing_and_composition() -> None:
    queries = parse_section_document(
        'section Section1;\n/* block; comment */\nshared #"Order ""Lines""" = 1;'
        '\n[Attr = [Nested = "a;b"]] shared B = let x = "s;t" in x; // tail; comment\nC = B;'
    )
    assert [(q.name, q.is_shared, q.expression) for q in queries] == [
        ('Order "Lines"', True, "1"),
        ("B", True, 'let x = "s;t" in x'),
        ("C", False, "B"),
    ]
    assert compose_let_expression({"A": "1", "B": "A"}, "B") == (
        'let\n    #"A" = 1,\n    #"B" = A\nin\n    #"B"'
    )
    with pytest.raises(ValueError):
        parse_section_document("section S;\nnot a member;")
