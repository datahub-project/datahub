"""Fabric OneLake probe: verdicts that match what ingestion filters on, and
listings that say when they could not look."""

from typing import Callable, Dict, Iterator, List, Optional, Set, Tuple, cast
from unittest.mock import MagicMock, patch

import pytest
import requests
from azure.core.exceptions import ClientAuthenticationError

from datahub.ingestion.agent.filter_check import check_filters
from datahub.ingestion.agent.filter_input import listing_from_run
from datahub.ingestion.agent.probe_methods import ProbeMethodResult, run_probe_method
from datahub.ingestion.agent.verdicts import ProbeArgumentError, ProbeReadFailed
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.common.subtypes import (
    DatasetContainerSubTypes,
    DatasetSubTypes,
    GenericContainerSubTypes,
)
from datahub.ingestion.source.fabric.common.models import FabricWorkspace
from datahub.ingestion.source.fabric.onelake.client import OneLakeClient
from datahub.ingestion.source.fabric.onelake.config import FabricOneLakeSourceConfig
from datahub.ingestion.source.fabric.onelake.models import (
    FabricColumn,
    FabricItem,
    FabricLakehouse,
    FabricTable,
    FabricView,
    FabricWarehouse,
)
from datahub.ingestion.source.fabric.onelake.onelake_probe import (
    FabricOneLakeMetadataProbe,
    FabricReadError,
)
from datahub.ingestion.source.fabric.onelake.schema_client import (
    SchemaExtractionClient,
)
from datahub.ingestion.source.fabric.onelake.source import (
    FabricOneLakeSource,
    LakehouseKey,
)
from datahub.sdk.dataset import Dataset

SOURCE = "fabric-onelake"
WS = FabricWorkspace(id="00000000-0000-0000-0000-00000000000a", name="sales-ws")
LH_ID = "00000000-0000-0000-0000-00000000000b"


def _tables() -> List[FabricTable]:
    # "" is a schemas-disabled lakehouse table; ingestion files it under dbo.
    return [
        FabricTable(name="orders", schema_name="", item_id=LH_ID, workspace_id=WS.id),
        FabricTable(
            name="refunds", schema_name="finance", item_id=LH_ID, workspace_id=WS.id
        ),
        FabricTable(
            name="tmp_load", schema_name="staging", item_id=LH_ID, workspace_id=WS.id
        ),
    ]


def _ingested(config_dict: Dict[str, object]) -> Set[str]:
    source = FabricOneLakeSource.create(
        config_dict, PipelineContext(run_id="probe-equivalence")
    )
    source.client = MagicMock()
    source.client.list_lakehouse_tables.return_value = _tables()
    emitted = list(
        source._process_item_tables(
            workspace=WS,
            item_id=LH_ID,
            item_type="Lakehouse",
            item_container_key=LakehouseKey(workspace_id=WS.id, lakehouse_id=LH_ID),
            item_display_name="lh_main",
            schema_map={},
            emitted_schemas=set(),
        )
    )
    # _process_item_tables swallows its own errors into a warning; an empty
    # set from a crash would make the equivalence below pass vacuously.
    assert not source.report.warnings
    return {
        e.display_name
        for e in emitted
        if isinstance(e, Dataset) and e.display_name is not None
    }


def _probed(config_dict: Dict[str, object]) -> Set[str]:
    included: Set[str] = set()
    for table in _tables():
        schema = table.schema_name or "dbo"
        result = check_filters(
            source_type=SOURCE,
            config_dict=config_dict,
            kind=str(DatasetSubTypes.TABLE),
            parent_path=[WS.name, "lh_main", schema],
            names=[table.name],
        )
        if result.results[0].included:
            included.add(table.name)
    return included


def test_table_verdicts_match_ingestion_across_schema_and_table_patterns() -> None:
    configs: List[Dict[str, object]] = [
        {"table_pattern": {"allow": ["^dbo\\.orders$"]}},
        {"table_pattern": {"deny": ["^staging\\..*"]}},
        {"schema_pattern": {"deny": ["^finance$"]}},
        # A bare table name never matches: ingestion filters on schema.table.
        {"table_pattern": {"allow": ["^orders$"]}},
    ]
    for config_dict in configs:
        assert _probed(config_dict) == _ingested(config_dict), config_dict


def test_schemaless_lakehouse_table_is_judged_as_dbo() -> None:
    result = check_filters(
        source_type=SOURCE,
        config_dict={},
        kind=str(DatasetSubTypes.TABLE),
        parent_path=[WS.name, "lh_main", "dbo"],
        names=["orders"],
    )
    assert result.results[0].target == "dbo.orders"


def test_containers_are_judged_on_their_display_names() -> None:
    config: Dict[str, object] = {
        "workspace_pattern": {"allow": ["^sales-.*"]},
        "lakehouse_pattern": {"deny": ["^lh_scratch$"]},
    }
    ws = check_filters(
        source_type=SOURCE,
        config_dict=config,
        kind=str(GenericContainerSubTypes.FABRIC_WORKSPACE),
        parent_path=[],
        names=["sales-ws", "hr-ws"],
    )
    assert [(v.target, v.included) for v in ws.results] == [
        ("sales-ws", True),
        ("hr-ws", False),
    ]
    lh = check_filters(
        source_type=SOURCE,
        config_dict=config,
        kind=str(DatasetContainerSubTypes.FABRIC_LAKEHOUSE),
        parent_path=["hr-ws"],
        names=["lh_main"],
    )
    # Judged on the bare name, and excluded by the denied workspace above it.
    assert lh.results[0].target == "lh_main"
    assert lh.results[0].excluded_by == "workspace_pattern"


def test_table_verdict_says_the_item_level_was_not_judged() -> None:
    result = check_filters(
        source_type=SOURCE,
        config_dict={"schema_pattern": {"deny": ["^staging$"]}},
        kind=str(DatasetSubTypes.TABLE),
        parent_path=[WS.name, "lh_main", "staging"],
        names=["tmp_load"],
    )
    assert result.results[0].excluded_by == "schema_pattern"
    assert any("Fabric Schema" in w for w in result.warnings)


def test_a_table_without_its_schema_is_judged_bare_and_says_so() -> None:
    result = check_filters(
        source_type=SOURCE,
        config_dict={},
        kind=str(DatasetSubTypes.TABLE),
        parent_path=[],
        names=["orders"],
    )
    assert result.results[0].target == "orders"
    assert any("no parent given" in w for w in result.warnings)


@pytest.mark.parametrize(
    "kind",
    [
        GenericContainerSubTypes.FABRIC_WORKSPACE,
        DatasetContainerSubTypes.FABRIC_LAKEHOUSE,
        DatasetContainerSubTypes.FABRIC_WAREHOUSE,
        DatasetContainerSubTypes.FABRIC_SCHEMA,
    ],
)
def test_containers_are_matched_bare_without_asking_for_a_parent(kind: str) -> None:
    result = check_filters(
        source_type=SOURCE,
        config_dict={},
        kind=str(kind),
        parent_path=[],
        names=["finance"],
    )
    assert result.results[0].target == "finance"
    assert result.warnings == []


WH_ID = "00000000-0000-0000-0000-00000000000c"


class _FakeClient:
    """Answers only what the probe calls, and records the calls so resolution
    cost is visible."""

    def __init__(self) -> None:
        self.calls: List[str] = []
        self.closed = False
        self.auth_helper = MagicMock()
        self.lakehouse_tables: List[FabricTable] = _tables()
        self.degrade_message: str = ""
        self.workspaces_error: Optional[Exception] = None
        self.lakehouses_error: Optional[Exception] = None
        # The item GET the SQL endpoint lookup makes: a response body, or an
        # error to raise.
        self.item_body: Dict[str, object] = {}
        self.item_error: Optional[Exception] = None

    def list_workspaces(self) -> Iterator[FabricWorkspace]:
        self.calls.append("workspaces")
        if self.workspaces_error is not None:
            raise self.workspaces_error
        yield WS
        yield FabricWorkspace(id="00000000-0000-0000-0000-0000000000ff", name="hr-ws")

    def list_lakehouses(self, workspace_id: str) -> Iterator[FabricLakehouse]:
        self.calls.append("lakehouses")
        if self.lakehouses_error is not None:
            raise self.lakehouses_error
        yield FabricLakehouse(
            id=LH_ID, name="lh_main", type="Lakehouse", workspace_id=workspace_id
        )
        yield FabricLakehouse(
            id="lh-dup", name="shared_name", type="Lakehouse", workspace_id=workspace_id
        )

    def list_warehouses(self, workspace_id: str) -> Iterator[FabricWarehouse]:
        self.calls.append("warehouses")
        yield FabricWarehouse(
            id=WH_ID, name="wh_main", type="Warehouse", workspace_id=workspace_id
        )
        yield FabricWarehouse(
            id="wh-dup", name="shared_name", type="Warehouse", workspace_id=workspace_id
        )

    def list_lakehouse_tables(
        self,
        workspace_id: str,
        lakehouse_id: str,
        *,
        on_degraded: Optional[Callable[[str], None]] = None,
    ) -> Iterator[FabricTable]:
        self.calls.append("lakehouse_tables")
        if self.degrade_message and on_degraded is not None:
            on_degraded(self.degrade_message)
            return iter([])
        return iter(self.lakehouse_tables)

    def list_warehouse_tables(
        self,
        workspace_id: str,
        warehouse_id: str,
        *,
        on_degraded: Optional[Callable[[str], None]] = None,
    ) -> Iterator[FabricTable]:
        self.calls.append("warehouse_tables")
        return iter(
            [
                FabricTable(
                    name="facts",
                    schema_name="dbo",
                    item_id=warehouse_id,
                    workspace_id=workspace_id,
                )
            ]
        )

    def get(self, endpoint: str) -> MagicMock:
        self.calls.append(f"get {endpoint}")
        if self.item_error is not None:
            raise self.item_error
        response = MagicMock()
        response.json.return_value = self.item_body
        return response

    def close(self) -> None:
        self.closed = True


def _http_error(status: int) -> requests.HTTPError:
    response = requests.Response()
    response.status_code = status
    response.url = "https://api.fabric.microsoft.com/v1/placeholder?token=secret-ish"
    return requests.HTTPError(f"{status} for url ...", response=response)


def _probe(**config: object) -> Tuple[FabricOneLakeMetadataProbe, _FakeClient]:
    client = _FakeClient()
    return (
        FabricOneLakeMetadataProbe(
            # A structural fake of the parts of OneLakeClient the probe calls.
            cast(OneLakeClient, client),
            FabricOneLakeSourceConfig.model_validate(config),
        ),
        client,
    )


def test_workspaces_lists_names_and_ids_including_denied_ones() -> None:
    probe, _ = _probe(workspace_pattern={"deny": ["^hr-ws$"]})
    assert [w["name"] for w in probe.workspaces(limit=10)] == ["sales-ws", "hr-ws"]


def test_workspace_and_item_resolve_by_guid_as_well_as_name() -> None:
    probe, _ = _probe()
    by_name = probe.lakehouses(workspace="sales-ws")
    by_guid = probe.lakehouses(workspace=WS.id)
    assert (
        by_name
        == by_guid
        == [
            {"name": "lh_main", "id": LH_ID},
            {"name": "shared_name", "id": "lh-dup"},
        ]
    )


def test_an_unknown_workspace_is_the_callers_error() -> None:
    probe, _ = _probe()
    with pytest.raises(ValueError, match="no-such-ws"):
        probe.lakehouses(workspace="no-such-ws")
    assert probe.failures == []


def test_a_name_shared_by_a_lakehouse_and_a_warehouse_asks_for_item_type() -> None:
    probe, _ = _probe()
    with pytest.raises(ValueError, match="--item-type Lakehouse"):
        probe.tables(workspace="sales-ws", item="shared_name", schema="dbo")
    assert probe.tables(
        workspace="sales-ws", item="shared_name", schema="dbo", item_type="Warehouse"
    ) == ["facts"]


def test_an_unknown_item_type_is_the_callers_error() -> None:
    probe, _ = _probe()
    ws = probe._workspace("sales-ws")
    with pytest.raises(ValueError, match="--item-type"):
        probe._item(ws, "lh_main", "Notebook")


def test_a_refused_rest_read_is_recorded_without_the_response_text() -> None:
    probe, client = _probe()
    response = requests.Response()
    response.status_code = 403
    response.url = "https://api.fabric.microsoft.com/v1/workspaces?token=secret-ish"
    client.workspaces_error = requests.HTTPError("403 for url ...", response=response)

    with pytest.raises(FabricReadError) as excinfo:
        probe.workspaces()

    # Recorded, so run_probe_method reports a read failure (exit 3), and
    # scrubbed to status + operation: the URL and body never reach the output.
    assert probe.failures == ["listing workspaces failed: HTTP 403"]
    assert "secret-ish" not in str(excinfo.value)
    assert not isinstance(excinfo.value, ValueError)


def test_a_credential_failure_says_so_without_the_sdk_text() -> None:
    probe, client = _probe()
    client.workspaces_error = ClientAuthenticationError(
        "authority https://login.example/placeholder-tenant rejected secret-ish"
    )

    with pytest.raises(FabricReadError):
        probe.workspaces()

    assert len(probe.failures) == 1
    assert "credential" in probe.failures[0]
    assert "secret-ish" not in probe.failures[0]
    assert "placeholder-tenant" not in probe.failures[0]


def test_a_failed_lakehouse_listing_still_resolves_a_warehouse() -> None:
    probe, client = _probe()
    client.lakehouses_error = _http_error(403)

    assert probe.tables(workspace="sales-ws", item="wh_main", schema="dbo") == ["facts"]
    # Answered, so not a read failure; but a same-named lakehouse could not be
    # ruled out, and the caller is told so.
    assert probe.failures == []
    assert any(
        "listing lakehouses" in w and "HTTP 403" in w and "wh_main" in w
        for w in probe.warnings
    )
    assert not any("secret-ish" in w for w in probe.warnings)


def test_an_item_not_found_because_a_listing_failed_is_a_read_failure() -> None:
    probe, client = _probe()
    client.lakehouses_error = _http_error(403)

    with pytest.raises(FabricReadError):
        probe.tables(workspace="sales-ws", item="lh_main", schema="dbo")
    assert probe.failures == [
        "listing lakehouses in workspace 'sales-ws' failed: HTTP 403"
    ]


def test_sql_endpoint_reports_a_failed_item_read_as_a_failure() -> None:
    probe, client = _probe(sql_endpoint={"enabled": True})
    client.item_error = _http_error(403)

    with pytest.raises(FabricReadError) as excinfo:
        probe.sql_endpoint(workspace="sales-ws", item="lh_main")
    assert len(probe.failures) == 1
    assert "HTTP 403" in probe.failures[0]
    assert "secret-ish" not in str(excinfo.value)


def test_sql_endpoint_is_null_only_when_the_item_has_none() -> None:
    probe, client = _probe(sql_endpoint={"enabled": True})
    client.item_body = {"properties": {}}
    assert probe.sql_endpoint(workspace="sales-ws", item="lh_main")["host"] is None
    assert probe.failures == []
    assert any("not provisioned" in w for w in probe.warnings)

    client.item_body = {
        "properties": {
            "sqlEndpointProperties": {
                "provisioningStatus": "Success",
                "connectionString": "placeholder.datawarehouse.fabric.microsoft.com",
            }
        }
    }
    assert (
        probe.sql_endpoint(workspace="sales-ws", item="lh_main")["host"]
        == "placeholder.datawarehouse.fabric.microsoft.com"
    )


def test_exit_closes_the_client() -> None:
    probe, client = _probe()
    with probe:
        pass
    assert client.closed


def test_tables_are_filtered_to_the_schema_and_schemaless_ones_live_in_dbo() -> None:
    probe, _ = _probe()
    assert probe.tables(workspace="sales-ws", item="lh_main", schema="dbo") == [
        "orders"
    ]
    assert probe.tables(workspace="sales-ws", item="lh_main", schema="finance") == [
        "refunds"
    ]


def test_schemas_are_derived_from_tables_as_ingestion_does() -> None:
    probe, _ = _probe(extract_views=False)
    assert probe.schemas(workspace="sales-ws", item="lh_main") == [
        "dbo",
        "finance",
        "staging",
    ]


def test_an_unreadable_table_listing_is_empty_with_a_warning() -> None:
    probe, client = _probe()
    client.degrade_message = "OneLake table API returned HTTP 403"
    assert probe.tables(workspace="sales-ws", item="lh_main", schema="dbo") == []
    assert probe.warnings == ["OneLake table API returned HTTP 403"]


def test_a_tables_command_resolves_the_workspace_once() -> None:
    probe, client = _probe()
    probe.tables(
        workspace="sales-ws", item="lh_main", schema="dbo", item_type="Lakehouse"
    )
    assert client.calls.count("workspaces") == 1


class _FakeSchemaClient:
    def __init__(self) -> None:
        self.closed = False
        self.views_error: Optional[Exception] = None
        self.close_error: Optional[Exception] = None

    def get_all_views(self, workspace_id: str, item_id: str) -> List[FabricView]:
        if self.views_error is not None:
            raise self.views_error
        return [
            FabricView(
                name="v_orders",
                schema_name="reporting",
                item_id=item_id,
                workspace_id=workspace_id,
                view_definition="CREATE VIEW reporting.v_orders AS SELECT 1 AS a",
            ),
            FabricView(
                name="v_hidden",
                schema_name="reporting",
                item_id=item_id,
                workspace_id=workspace_id,
                view_definition=None,
            ),
        ]

    def get_all_table_columns(
        self, workspace_id: str, item_id: str
    ) -> Dict[Tuple[str, str], List[FabricColumn]]:
        return {
            ("dbo", "orders"): [
                FabricColumn(
                    name="id", data_type="INT", is_nullable=False, ordinal_position=1
                )
            ]
        }

    def close(self) -> None:
        if self.close_error is not None:
            raise self.close_error
        self.closed = True


def _sql_probe(
    **config: object,
) -> Tuple[FabricOneLakeMetadataProbe, _FakeSchemaClient]:
    schema_client = _FakeSchemaClient()

    def _factory(ws: FabricWorkspace, item: FabricItem) -> SchemaExtractionClient:
        # Structural fake: implements only what the probe calls.
        return cast(SchemaExtractionClient, schema_client)

    probe = FabricOneLakeMetadataProbe(
        cast(OneLakeClient, _FakeClient()),
        FabricOneLakeSourceConfig.model_validate(config),
        schema_client_factory=_factory,
    )
    return probe, schema_client


def test_views_columns_and_definition_come_from_the_sql_endpoint() -> None:
    probe, schema_client = _sql_probe()
    with probe:
        assert probe.views(
            workspace="sales-ws", item="lh_main", schema="reporting"
        ) == ["v_orders", "v_hidden"]
        assert probe.columns(
            workspace="sales-ws", item="lh_main", schema="dbo", table="orders"
        ) == [{"name": "id", "type": "INT", "nullable": False}]
        definition = probe.view_definition(
            workspace="sales-ws", item="lh_main", schema="reporting", view="v_orders"
        )
        assert definition is not None and definition.startswith("CREATE VIEW")
        assert "reporting" in probe.schemas(workspace="sales-ws", item="lh_main")
    assert schema_client.closed
    assert probe.failures == []


def test_an_unknown_table_is_the_callers_error() -> None:
    probe, _ = _sql_probe()
    with pytest.raises(ValueError, match="dbo.nope"):
        probe.columns(workspace="sales-ws", item="lh_main", schema="dbo", table="nope")


def test_an_unreadable_view_definition_is_null_with_a_warning() -> None:
    probe, _ = _sql_probe()
    assert (
        probe.view_definition(
            workspace="sales-ws", item="lh_main", schema="reporting", view="v_hidden"
        )
        is None
    )
    assert any("VIEW DEFINITION" in w for w in probe.warnings)


def test_an_unresolvable_endpoint_is_a_read_failure_not_a_bad_argument() -> None:
    def _no_endpoint(ws: FabricWorkspace, item: FabricItem) -> SchemaExtractionClient:
        raise ValueError("SQL Analytics Endpoint URL is required for Lakehouse x")

    probe = FabricOneLakeMetadataProbe(
        cast(OneLakeClient, _FakeClient()),
        FabricOneLakeSourceConfig.model_validate({}),
        schema_client_factory=_no_endpoint,
    )
    with pytest.raises(FabricReadError):
        probe.views(workspace="sales-ws", item="lh_main", schema="dbo")
    # Recorded, so run_probe_method raises ProbeReadFailed (exit 3), not exit 2.
    assert len(probe.failures) == 1 and "lh_main" in probe.failures[0]
    assert "not provisioned" in probe.failures[0]


def test_a_failed_catalog_query_is_recorded_without_the_driver_text() -> None:
    probe, schema_client = _sql_probe()
    schema_client.views_error = RuntimeError("driver said: Server=host;secret-ish")
    with pytest.raises(FabricReadError) as excinfo:
        probe.views(workspace="sales-ws", item="lh_main", schema="reporting")
    assert probe.failures and "secret-ish" not in probe.failures[0]
    assert "secret-ish" not in str(excinfo.value)


def test_schemas_degrade_to_tables_only_when_views_cannot_be_read() -> None:
    probe, schema_client = _sql_probe()
    schema_client.views_error = RuntimeError("driver said: secret-ish")
    assert probe.schemas(workspace="sales-ws", item="lh_main") == [
        "dbo",
        "finance",
        "staging",
    ]
    # Partial, not failed: the tables answered.
    assert probe.failures == []
    assert len(probe.warnings) == 1 and "secret-ish" not in probe.warnings[0]


def test_a_recipe_without_an_enabled_sql_endpoint_is_told_why() -> None:
    probe, _ = _sql_probe(
        sql_endpoint={"enabled": False},
        extract_views=False,
        extract_schema={"enabled": False},
        usage={"include_usage_statistics": False},
    )
    with pytest.raises(ValueError, match="sql_endpoint"):
        probe.columns(
            workspace="sales-ws", item="lh_main", schema="dbo", table="orders"
        )


def test_a_kind_switched_off_by_the_recipe_is_excluded_by_that_switch() -> None:
    result = check_filters(
        source_type=SOURCE,
        config_dict={"extract_views": False},
        kind=str(DatasetSubTypes.VIEW),
        parent_path=[WS.name, "lh_main", "reporting"],
        names=["v_orders"],
    )
    assert (result.results[0].included, result.results[0].excluded_by) == (
        False,
        "extract_views",
    )
    warehouse = check_filters(
        source_type=SOURCE,
        config_dict={"extract_warehouses": False},
        kind=str(DatasetContainerSubTypes.FABRIC_WAREHOUSE),
        parent_path=[WS.name],
        names=["wh_main"],
    )
    assert warehouse.results[0].excluded_by == "extract_warehouses"


def test_a_workspace_given_by_id_warns_that_the_parent_path_carries_the_id() -> None:
    probe, _ = _probe()
    probe.lakehouses(workspace=WS.id)
    assert len(probe.warnings) == 1
    assert "resolved by id to workspace 'sales-ws'" in probe.warnings[0]
    assert "--workspace 'sales-ws'" in probe.warnings[0]


def test_an_item_given_by_id_warns_too() -> None:
    probe, _ = _probe()
    probe.tables(workspace="sales-ws", item=LH_ID, schema="dbo")
    assert len(probe.warnings) == 1
    assert "resolved by id to Lakehouse 'lh_main'" in probe.warnings[0]
    assert "--item 'lh_main'" in probe.warnings[0]


def test_display_names_resolve_without_a_warning() -> None:
    probe, _ = _probe()
    probe.lakehouses(workspace="sales-ws")
    probe.tables(workspace="sales-ws", item="lh_main", schema="dbo")
    assert probe.warnings == []


def test_a_run_by_display_name_judges_as_ingestion_does_from_run() -> None:
    config: Dict[str, object] = {
        "workspace_pattern": {"allow": ["^sales-.*"]},
        "table_pattern": {"deny": ["^staging\\..*"]},
    }
    client = _FakeClient()

    def _for_config(
        cls: object, cfg: FabricOneLakeSourceConfig
    ) -> FabricOneLakeMetadataProbe:
        return FabricOneLakeMetadataProbe(cast(OneLakeClient, client), cfg)

    def _judged(command: str, kwargs: Dict[str, object]) -> Dict[str, bool]:
        with patch.object(
            FabricOneLakeMetadataProbe, "for_config", classmethod(_for_config)
        ):
            run = run_probe_method(SOURCE, config, command, kwargs)
        assert run.warnings == []
        listing = listing_from_run(run.to_dict())
        assert listing.kind is not None
        verdicts = check_filters(
            source_type=SOURCE,
            config_dict=config,
            kind=listing.kind,
            parent_path=listing.parent_path,
            names=listing.names,
            attributes=listing.attributes,
        )
        return {v.name: v.included for v in verdicts.results}

    # The lakehouse sits in an allowed workspace, so ingestion reaches it.
    assert _judged("lakehouses", {"workspace": "sales-ws"}) == {
        "lh_main": True,
        "shared_name": True,
    }
    assert _judged(
        "tables", {"workspace": "sales-ws", "item": "lh_main", "schema": "staging"}
    ) == {"tmp_load": False}


def test_exit_closes_the_rest_session_even_if_a_sql_client_close_fails() -> None:
    probe, schema_client = _sql_probe()
    probe.views(workspace="sales-ws", item="lh_main", schema="reporting")
    schema_client.close_error = RuntimeError("engine dispose failed")
    rest = cast(_FakeClient, probe._client)
    with pytest.raises(RuntimeError):
        probe.__exit__(None, None, None)
    assert rest.closed


def test_a_case_only_workspace_miss_names_the_listed_spelling() -> None:
    probe, _ = _probe()
    with pytest.raises(ValueError, match="did you mean 'sales-ws'"):
        probe.lakehouses(workspace="SALES-WS")
    assert probe.failures == []


def _run(
    config: Dict[str, object],
    command: str,
    kwargs: Dict[str, object],
    client: Optional[_FakeClient] = None,
) -> ProbeMethodResult:
    fake = client or _FakeClient()

    def _factory(ws: FabricWorkspace, item: FabricItem) -> SchemaExtractionClient:
        return cast(SchemaExtractionClient, _FakeSchemaClient())

    def _for_config(
        cls: object, cfg: FabricOneLakeSourceConfig
    ) -> FabricOneLakeMetadataProbe:
        return FabricOneLakeMetadataProbe(
            cast(OneLakeClient, fake), cfg, schema_client_factory=_factory
        )

    with patch.object(
        FabricOneLakeMetadataProbe, "for_config", classmethod(_for_config)
    ):
        return run_probe_method(SOURCE, config, command, kwargs)


_NO_SQL_ENDPOINT: Dict[str, object] = {
    "sql_endpoint": {"enabled": False},
    "extract_views": False,
    "extract_schema": {"enabled": False},
    "usage": {"include_usage_statistics": False},
}
_ORDERS = {"workspace": "sales-ws", "item": "lh_main", "schema": "dbo"}


@pytest.mark.parametrize(
    "config, command, kwargs, shown",
    [
        ({}, "columns", {**_ORDERS, "table": "nope"}, "dbo.nope"),
        ({}, "view_definition", {**_ORDERS, "view": "nope"}, "no view 'dbo.nope'"),
        ({}, "tables", {**_ORDERS, "item_type": "Notebook"}, "--item-type"),
        (_NO_SQL_ENDPOINT, "columns", {**_ORDERS, "table": "orders"}, "sql_endpoint"),
    ],
)
def test_a_refusal_reaches_the_caller_with_its_message(
    config: Dict[str, object], command: str, kwargs: Dict[str, object], shown: str
) -> None:
    with pytest.raises(ProbeArgumentError, match=shown):
        _run(config, command, kwargs)


def test_a_failed_read_reaches_the_caller_as_a_read_failure() -> None:
    client = _FakeClient()
    client.workspaces_error = _http_error(403)
    with pytest.raises(ProbeReadFailed) as excinfo:
        _run({}, "workspaces", {}, client=client)
    assert "listing workspaces failed: HTTP 403" in str(excinfo.value)
    assert "secret-ish" not in str(excinfo.value)
