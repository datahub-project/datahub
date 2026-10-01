"""Fabric OneLake probe: verdicts that match what ingestion filters on, and
listings that say when they could not look."""

from typing import Dict, List, Set
from unittest.mock import MagicMock

from datahub.ingestion.agent.filter_check import check_filters
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.common.subtypes import (
    DatasetContainerSubTypes,
    DatasetSubTypes,
    GenericContainerSubTypes,
)
from datahub.ingestion.source.fabric.common.models import FabricWorkspace
from datahub.ingestion.source.fabric.onelake.models import FabricTable
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
