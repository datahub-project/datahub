"""Fabric OneLake probe: verdicts that match what ingestion filters on, and
listings that say when they could not look."""

from typing import Callable, Dict, Iterator, List, Optional, Set, Tuple, cast
from unittest.mock import MagicMock

import pytest
import requests

from datahub.ingestion.agent.filter_check import check_filters
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
    FabricLakehouse,
    FabricTable,
    FabricWarehouse,
)
from datahub.ingestion.source.fabric.onelake.onelake_probe import (
    FabricOneLakeMetadataProbe,
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


WH_ID = "00000000-0000-0000-0000-00000000000c"


class _FakeClient:
    """Answers only what the probe calls; records calls so resolution cost is visible."""

    def __init__(self) -> None:
        self.calls: List[str] = []
        self.closed = False
        self.auth_helper = MagicMock()
        self.lakehouse_tables: List[FabricTable] = _tables()
        self.degrade_message: str = ""
        self.workspaces_error: Optional[Exception] = None

    def list_workspaces(self) -> Iterator[FabricWorkspace]:
        self.calls.append("workspaces")
        if self.workspaces_error is not None:
            raise self.workspaces_error
        yield WS
        yield FabricWorkspace(id="00000000-0000-0000-0000-0000000000ff", name="hr-ws")

    def list_lakehouses(self, workspace_id: str) -> Iterator[FabricLakehouse]:
        self.calls.append("lakehouses")
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

    def close(self) -> None:
        self.closed = True


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
    with pytest.raises(ValueError, match="item_type"):
        probe.tables(workspace="sales-ws", item="shared_name", schema="dbo")
    assert probe.tables(
        workspace="sales-ws", item="shared_name", schema="dbo", item_type="Warehouse"
    ) == ["facts"]


def test_an_unknown_item_type_is_the_callers_error() -> None:
    probe, _ = _probe()
    ws = probe._workspace("sales-ws")
    with pytest.raises(ValueError, match="item_type"):
        probe._item(ws, "lh_main", "Notebook")


def test_a_refused_rest_read_is_recorded_without_the_response_text() -> None:
    probe, client = _probe()
    response = requests.Response()
    response.status_code = 403
    response.url = "https://api.fabric.microsoft.com/v1/workspaces?token=secret-ish"
    client.workspaces_error = requests.HTTPError("403 for url ...", response=response)

    with pytest.raises(Exception) as excinfo:
        probe.workspaces()

    # Recorded, so run_probe_method reports a read failure (exit 3), and
    # scrubbed to status + operation: the URL and body never reach the output.
    assert probe.failures == ["listing workspaces failed: HTTP 403"]
    assert "secret-ish" not in str(excinfo.value)
    assert not isinstance(excinfo.value, ValueError)


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
