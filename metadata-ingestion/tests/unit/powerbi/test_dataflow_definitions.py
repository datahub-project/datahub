"""Dataflow Gen2 ingestion (``extract_dataflows`` in workspace_apis mode):
definition fetch and mashup parsing in PowerBiAPI, entity emission and
lineage in the Mapper, and the Phase 1 / Phase 2 split in the source."""

import base64
import json
from typing import Any, Dict, List, Optional
from unittest import mock
from unittest.mock import MagicMock

import pytest
import requests
import requests_mock

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.powerbi.config import (
    PowerBiDashboardSourceConfig,
    PowerBiDashboardSourceReport,
)
from datahub.ingestion.source.powerbi.dataplatform_instance_resolver import (
    create_dataplatform_instance_resolver,
)
from datahub.ingestion.source.powerbi.powerbi import (
    Mapper,
    PowerBiDashboardSource,
)
from datahub.ingestion.source.powerbi.rest_api_wrapper.data_classes import (
    Dataflow,
    DataflowQuery,
    PowerBIDataset,
    Table,
    Workspace,
)
from datahub.ingestion.source.powerbi.rest_api_wrapper.dataflow_definition_parser import (
    compose_let_expression,
    parse_dataflow_definition,
    parse_section_document,
)
from datahub.ingestion.source.powerbi.rest_api_wrapper.fabric_resolver import (
    FABRIC_BASE_URL,
    DefinitionPart,
    FabricDefinitionResolver,
)
from datahub.ingestion.source.powerbi.rest_api_wrapper.powerbi_api import PowerBiAPI
from datahub.metadata.schema_classes import (
    ContainerClass,
    ContainerPropertiesClass,
    SubTypesClass,
    UpstreamLineageClass,
)
from tests.unit.powerbi.powerbi_test_helpers import (
    WORKSPACE_ID,
    FakeMsal,
    make_api,
    make_config,
    make_workspace,
)

OTHER_WORKSPACE_ID = "11111111-2222-4333-8444-555555555555"
DATAFLOW_ID = "8b9c0d1e-2f3a-4b4c-8d5e-6f7a8b9c0d1e"
SECOND_DATAFLOW_ID = "9c0d1e2f-3a4b-4c5d-8e6f-7a8b9c0d1e2f"
OTHER_DATAFLOW_ID = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"
MODEL_ID = "05169cd2-e713-41e6-9600-1d8066d95445"

ITEMS_URL = f"{FABRIC_BASE_URL}/workspaces/{WORKSPACE_ID}/items"


def definition_url(dataflow_id: str) -> str:
    return f"{FABRIC_BASE_URL}/workspaces/{WORKSPACE_ID}/dataflows/{dataflow_id}/getDefinition"


DEFINITION_URL = definition_url(DATAFLOW_ID)

SNOWFLAKE_POLICIES = (
    "urn:li:dataset:(urn:li:dataPlatform:snowflake,sales_db.public.policies,PROD)"
)
MSSQL_AGENTS = "urn:li:dataset:(urn:li:dataPlatform:mssql,agency.dbo.agents,PROD)"
MODEL_TABLE_URN = "urn:li:dataset:(urn:li:dataPlatform:powerbi,Insurance_Model.Dataflow_Policies,PROD)"


def entity_urn(dataflow_id: str, entity: str) -> str:
    return f"urn:li:dataset:(urn:li:dataPlatform:powerbi,dataflows.{dataflow_id}.{entity},PROD)"


POLICIES_ENTITY_URN = entity_urn(DATAFLOW_ID, "Policies")
ACTIVE_AGENTS_ENTITY_URN = entity_urn(DATAFLOW_ID, "Active_Agents")

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


def dataflow_table_expression(
    workspace_id: str, dataflow_id: str, entity: str = "Policies"
) -> str:
    return f"""let
    Source = PowerPlatform.Dataflows(null),
    Workspaces = Source{{[Id="Workspaces"]}}[Data],
    Workspace = Workspaces{{[workspaceId="{workspace_id}"]}}[Data],
    Dataflow = Workspace{{[dataflowId="{dataflow_id}"]}}[Data],
    Entity = Dataflow{{[entity="{entity}",version=""]}}[Data]
in
    Entity"""


def _config(**overrides: Any) -> PowerBiDashboardSourceConfig:
    defaults: Dict[str, Any] = {
        "metadata_api_mode": "workspace_apis",
        "extract_dataflows": True,
        "extract_column_level_lineage": False,
        "convert_lineage_urns_to_lowercase": True,
    }
    return make_config(**{**defaults, **overrides})


def _workspace() -> Workspace:
    return make_workspace(web_url=f"https://app.powerbi.com/groups/{WORKSPACE_ID}")


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
    """Real Fabric resolver (HTTP through requests_mock), mocked Power BI
    resolver."""
    regular = MagicMock()
    regular.get_datasets.return_value = []
    return make_api(
        config or _config(),
        regular_resolver=regular,
        fabric_resolver=FabricDefinitionResolver(
            msal_client=FakeMsal(),  # type: ignore[arg-type]
            request_session=requests.Session(),
            lro_timeout_seconds=10,
        ),
    )


def _item(dataflow_id: str, name: str) -> Dict[str, str]:
    return {
        "id": dataflow_id,
        "type": "Dataflow",
        "displayName": name,
        "description": "Curated policies",
        "workspaceId": WORKSPACE_ID,
    }


def _register_items(
    m: requests_mock.Mocker, items: Optional[List[Dict[str, str]]] = None
) -> None:
    rows = items if items is not None else [_item(DATAFLOW_ID, "Policies Dataflow")]
    rows.append(
        {
            "id": MODEL_ID,
            "type": "SemanticModel",
            "displayName": "Insurance Model",
            "workspaceId": WORKSPACE_ID,
        }
    )
    m.get(ITEMS_URL, json={"value": rows})


def _mapper(
    api: PowerBiAPI, config: Optional[PowerBiDashboardSourceConfig] = None
) -> Mapper:
    config = config or _config()
    mapper = Mapper(
        ctx=MagicMock(),
        config=config,
        reporter=api.reporter,
        dataplatform_instance_resolver=create_dataplatform_instance_resolver(config),
        powerbi_client=api,
    )
    mapper.workspace_key = mapper.make_container_key_for_workspace(_workspace())
    return mapper


def _fetched_dataflow(api: PowerBiAPI, workspace: Workspace) -> Dataflow:
    with requests_mock.Mocker() as m:
        _register_items(m)
        m.post(DEFINITION_URL, json=_definition_body())
        api.fill_metadata_from_definitions([workspace])
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


# --- PowerBiAPI: fetch and parse ------------------------------------------


def test_dataflow_entities_are_emitted_with_upstream_lineage() -> None:
    api = _api()
    workspace = _workspace()
    dataflow = _fetched_dataflow(api, workspace)

    assert api.reporter.dataflows_fetched == 1
    assert api.reporter.dataflow_definitions_fetched == 1
    assert api.reporter.fabric_definitions_fetched == 0
    assert api.reporter.dataflow_entities_parsed == 2
    assert [q.name for q in dataflow.entities()] == ["Policies", "Active Agents"]
    assert [q.name for q in dataflow.queries if not q.is_entity] == ["Agents_Staging"]
    assert api.get_ingested_dataflow(DATAFLOW_ID.upper()) is dataflow
    assert dataflow.webUrl is not None and dataflow.webUrl.endswith(
        f"/dataflows/{DATAFLOW_ID}"
    )

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


def test_two_dataflows_with_the_same_entity_name_get_distinct_urns() -> None:
    api = _api()
    workspace = _workspace()
    with requests_mock.Mocker() as m:
        _register_items(
            m,
            [_item(DATAFLOW_ID, "Policies"), _item(SECOND_DATAFLOW_ID, "Policies")],
        )
        m.post(DEFINITION_URL, json=_definition_body())
        m.post(definition_url(SECOND_DATAFLOW_ID), json=_definition_body())
        api.fill_metadata_from_definitions([workspace])

    mapper = _mapper(api)
    urns = {
        mcp.entityUrn
        for dataflow in workspace.dataflows.values()
        for mcp in mapper.to_datahub_dataflow(dataflow, workspace)
    }
    assert entity_urn(DATAFLOW_ID, "Policies") in urns
    assert entity_urn(SECOND_DATAFLOW_ID, "Policies") in urns


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


def test_unrecognised_member_is_a_warning_unless_nothing_parses() -> None:
    api = _api()
    workspace = _workspace()
    with requests_mock.Mocker() as m:
        _register_items(m)
        m.post(
            DEFINITION_URL,
            json=_definition_body(mashup=MASHUP + "this is not a member;\n"),
        )
        api.fill_metadata_from_definitions([workspace])
    assert [q.name for q in workspace.dataflows[DATAFLOW_ID].entities()] == [
        "Policies",
        "Active Agents",
    ]
    assert api.reporter.dataflow_queries_unparseable == 1
    assert [w.title for w in api.reporter.warnings] == ["Dataflow Query Not Parsed"]
    assert not api.reporter.failures

    api = _api()
    workspace = _workspace()
    with requests_mock.Mocker() as m:
        _register_items(m)
        m.post(
            DEFINITION_URL,
            json=_definition_body(mashup="section Section1;\nnot a member;"),
        )
        api.fill_metadata_from_definitions([workspace])
    assert workspace.dataflows == {}
    assert [f.title for f in api.reporter.failures] == ["Dataflow Mashup Not Parsed"]


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
    assert api.get_ingested_dataflow(DATAFLOW_ID) is None
    assert api.was_dataflow_listed(DATAFLOW_ID)
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


def test_item_listing_failure_is_a_failure_when_dataflows_are_on() -> None:
    api = _api()
    workspace = _workspace()
    with requests_mock.Mocker() as m:
        m.get(ITEMS_URL, status_code=500, json={})
        api.fill_metadata_from_definitions([workspace])
    assert "Fabric Items Not Listed" in [f.title for f in api.reporter.failures]

    api = _api(_config(extract_dataflows=False))
    with requests_mock.Mocker() as m:
        m.get(ITEMS_URL, status_code=500, json={})
        api.fill_metadata_from_definitions([_workspace()])
    assert not api.reporter.failures
    assert "Fabric Items Not Listed" in [w.title for w in api.reporter.warnings]


# --- Mapper: semantic-model table -> dataflow entity -----------------------


def test_semantic_model_table_links_to_the_dataflow_entity() -> None:
    api = _api()
    workspace = _workspace()
    _fetched_dataflow(api, workspace)
    # Upper-cased id in the M reference: GUIDs are case-insensitive.
    table = _model_table(dataflow_table_expression(WORKSPACE_ID, DATAFLOW_ID.upper()))

    mapper = _mapper(api)
    mcps = mapper.extract_lineage(table, MODEL_TABLE_URN, workspace)
    # Mapped a second time (report + tile): one edge each, counted once.
    mapper.extract_lineage(table, MODEL_TABLE_URN, workspace)

    assert _lineage_by_urn(mcps) == {MODEL_TABLE_URN: [POLICIES_ENTITY_URN]}
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

    mcps = _mapper(api).extract_lineage(table, MODEL_TABLE_URN, workspace)

    assert _lineage_by_urn(mcps) == {MODEL_TABLE_URN: [POLICIES_ENTITY_URN]}


def test_navigation_without_dataflow_id_warns() -> None:
    api = _api()
    workspace = _workspace()
    table = _model_table(
        """let
    Source = PowerPlatform.Dataflows(null),
    Workspaces = Source{[Id="Workspaces"]}[Data],
    Entity = Workspaces{[entity=SomeParameter]}[Data]
in
    Entity"""
    )

    mcps = _mapper(api).extract_lineage(table, MODEL_TABLE_URN, workspace)

    assert mcps == []
    assert api.reporter.dataflow_references_unresolved == 1
    assert [w.title for w in api.reporter.warnings] == [
        "Dataflow Reference Not Resolved"
    ]


@pytest.mark.parametrize(
    "extract_dataflows,listed,advice",
    [
        pytest.param(False, False, "Enable extract_dataflows", id="flag_off"),
        pytest.param(True, False, "workspace_id_pattern", id="other_workspace"),
        pytest.param(True, True, "could not be fetched", id="fetch_failed"),
    ],
)
def test_reference_to_dataflow_outside_the_run_warns_without_an_edge(
    extract_dataflows: bool, listed: bool, advice: str
) -> None:
    api = _api(_config(extract_dataflows=extract_dataflows))
    workspace = _workspace()
    if listed:
        with requests_mock.Mocker() as m:
            _register_items(m, [_item(OTHER_DATAFLOW_ID, "Broken")])
            m.post(definition_url(OTHER_DATAFLOW_ID), status_code=500, json={})
            api.fill_metadata_from_definitions([workspace])
    table = _model_table(
        dataflow_table_expression(OTHER_WORKSPACE_ID, OTHER_DATAFLOW_ID)
    )

    mapper = _mapper(api, api._PowerBiAPI__config)  # type: ignore[attr-defined]
    mcps = mapper.extract_lineage(table, MODEL_TABLE_URN, workspace)
    mapper.extract_lineage(table, MODEL_TABLE_URN, workspace)

    assert mcps == []
    assert api.reporter.dataflow_references_not_ingested == 1
    assert api.reporter.dataflow_references_resolved == 0
    warning = next(
        w for w in api.reporter.warnings if w.title == "Dataflow Not Ingested"
    )
    assert advice in str(warning.message)
    assert OTHER_DATAFLOW_ID in str(warning.context)


def test_reference_to_an_entity_the_dataflow_does_not_load_warns() -> None:
    api = _api()
    workspace = _workspace()
    _fetched_dataflow(api, workspace)
    table = _model_table(
        dataflow_table_expression(WORKSPACE_ID, DATAFLOW_ID, entity="Agents_Staging")
    )

    mcps = _mapper(api).extract_lineage(table, MODEL_TABLE_URN, workspace)

    assert mcps == []
    assert api.reporter.dataflow_references_not_ingested == 1
    assert [w.title for w in api.reporter.warnings] == ["Dataflow Not Ingested"]


# --- Source: dataflows survive a Phase 2 failure ---------------------------


def test_dataflows_are_emitted_even_when_their_workspace_fails_phase_2() -> None:
    """The registry is filled in Phase 1; if Phase 2 then fails for the
    dataflow's workspace, the dataflow entities must still be emitted so
    that the edge a model in another workspace gets is never dangling."""
    dataflow_workspace = make_workspace(workspace_id=WORKSPACE_ID, name="dataflows")
    model_workspace = make_workspace(workspace_id=OTHER_WORKSPACE_ID, name="models")
    dataflow = Dataflow(
        id=DATAFLOW_ID,
        name="Policies Dataflow",
        description=None,
        workspace_name=dataflow_workspace.name,
        queries=[
            DataflowQuery(
                name="Policies",
                expression='let Source = Snowflake.Databases("my-account.snowflakecomputing.com","WH") in Source',
                is_shared=True,
                load_enabled=True,
            )
        ],
    )
    table = _model_table(dataflow_table_expression(WORKSPACE_ID, DATAFLOW_ID))
    model = table.dataset
    assert model is not None

    def fill_phase1(workspaces: List[Workspace]) -> set:
        for ws in workspaces:
            if ws.id == WORKSPACE_ID:
                ws.dataflows = {dataflow.id: dataflow}
                source.powerbi_client.dataflow_registry[dataflow.id] = dataflow
                source.powerbi_client.dataflows_listed.add(dataflow.id)
            else:
                ws.independent_datasets = {MODEL_ID: model}
        return set()

    def fill_phase2(workspace: Workspace) -> None:
        if workspace.id == WORKSPACE_ID:
            raise requests.HTTPError("boom")

    with mock.patch("msal.ConfidentialClientApplication", return_value=FakeMsal()):
        source = PowerBiDashboardSource(
            config=_config(
                extract_independent_datasets=True,
                extract_lineage=True,
                extract_workspaces_to_containers=True,
            ),
            ctx=PipelineContext(run_id="dataflow-test"),
        )
    source.validate_dataset_type_mapping = MagicMock()  # type: ignore[method-assign]
    source.get_allowed_workspaces = MagicMock(  # type: ignore[method-assign]
        return_value=[dataflow_workspace, model_workspace]
    )
    source.powerbi_client.fill_metadata_from_definitions = MagicMock(  # type: ignore[method-assign]
        side_effect=fill_phase1
    )
    source.powerbi_client.fill_regular_metadata_detail = MagicMock(  # type: ignore[method-assign]
        side_effect=fill_phase2
    )

    workunits = list(source.get_workunits_internal())

    mcps = [
        wu.metadata
        for wu in workunits
        if isinstance(wu.metadata, MetadataChangeProposalWrapper)
    ]
    assert POLICIES_ENTITY_URN in {mcp.entityUrn for mcp in mcps}
    lineage = _lineage_by_urn(mcps)
    assert lineage[MODEL_TABLE_URN] == [POLICIES_ENTITY_URN]
    report = source.get_report()
    assert isinstance(report, PowerBiDashboardSourceReport)
    assert [f.title for f in report.failures] == ["Failed to Fetch Workspace Metadata"]
    assert report.dataflow_references_resolved == 1


# --- Pure functions ---------------------------------------------------------


@pytest.mark.parametrize(
    "text,expected",
    [
        pytest.param("", [], id="empty"),
        pytest.param("section S;", [], id="section_only"),
        pytest.param(
            'section Section1;\n/* block; comment */\nshared #"Order ""Lines""" = 1;',
            [('Order "Lines"', True, "1")],
            id="quoted_identifier_and_block_comment",
        ),
        pytest.param(
            '[Attr = [Nested = "a;b"]] shared B = let x = "s;t" in x; // tail; comment\nC = B;',
            [("B", True, 'let x = "s;t" in x'), ("C", False, "B")],
            id="attribute_record_strings_and_line_comment",
        ),
        pytest.param(
            "sectionData = 1;\nsharedX = 2;",
            [("sectionData", False, "1"), ("sharedX", False, "2")],
            id="keyword_prefixed_names",
        ),
    ],
)
def test_parse_section_document(text: str, expected: List[Any]) -> None:
    document = parse_section_document(text)
    assert [(m.name, m.is_shared, m.expression) for m in document.members] == expected
    assert document.failed_members == []


def test_parse_section_document_collects_unrecognised_members() -> None:
    document = parse_section_document("section S;\nnot a member;\nA = 1;")
    assert [m.name for m in document.members] == ["A"]
    assert document.failed_members == ["not a member"]
    with pytest.raises(ValueError):
        parse_section_document('A = "unterminated;')


def test_compose_let_expression() -> None:
    assert compose_let_expression({"A": "1", "B": "A"}, "B") == (
        'let\n    #"A" = 1,\n    #"B" = A\nin\n    #"B"'
    )


@pytest.mark.parametrize(
    "metadata",
    [
        pytest.param({"formatVersion": "202502"}, id="no_queriesMetadata"),
        pytest.param({"queriesMetadata": {"A": {"queryName": "A"}}}, id="absent"),
        pytest.param({"queriesMetadata": {"A": {"loadEnabled": "yes"}}}, id="non_bool"),
    ],
)
def test_query_metadata_without_usable_load_flag_falls_back_to_shared(
    metadata: Dict[str, Any],
) -> None:
    parsed = parse_dataflow_definition(
        [
            DefinitionPart("mashup.pq", "section S;\nshared A = 1;\nB = 2;"),
            DefinitionPart("queryMetadata.json", json.dumps(metadata)),
        ]
    )
    assert parsed.has_mashup
    assert [(q.name, q.is_entity) for q in parsed.queries] == [
        ("A", True),
        ("B", False),
    ]


@pytest.mark.parametrize(
    "mashup",
    [
        pytest.param("section S;\nshared A = B;\nshared B = A;", id="circular"),
        pytest.param("section S;\nshared A = Missing;", id="missing_binding"),
    ],
)
def test_self_referencing_or_dangling_queries_yield_no_lineage(mashup: str) -> None:
    api = _api()
    workspace = _workspace()
    with requests_mock.Mocker() as m:
        _register_items(m)
        m.post(DEFINITION_URL, json=_definition_body(mashup=mashup, query_metadata={}))
        api.fill_metadata_from_definitions([workspace])

    mcps = _mapper(api).to_datahub_dataflow(workspace.dataflows[DATAFLOW_ID], workspace)

    assert _lineage_by_urn(mcps) == {}
    assert entity_urn(DATAFLOW_ID, "A") in {mcp.entityUrn for mcp in mcps}
