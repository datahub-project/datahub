"""Eventstream ingestion: flows, topology jobs, and Lakehouse outlets."""

from typing import Iterable, Optional
from unittest.mock import MagicMock

from datahub.ingestion.source.azure.azure_auth import AzureCredentialConfig
from datahub.ingestion.source.fabric.common.models import FabricWorkspace
from datahub.ingestion.source.fabric.onelake.config import (
    EventstreamConfig,
    FabricOneLakeSourceConfig,
)
from datahub.ingestion.source.fabric.onelake.eventstreams import process_eventstreams
from datahub.ingestion.source.fabric.onelake.models import FabricEventstream
from datahub.ingestion.source.fabric.onelake.report import FabricOneLakeSourceReport
from datahub.metadata.schema_classes import (
    DataJobInfoClass,
    DataJobInputOutputClass,
)
from datahub.metadata.urns import DataFlowUrn
from datahub.sdk.dataflow import DataFlow
from datahub.sdk.datajob import DataJob
from datahub.sdk.entity import Entity

WS = "11111111-1111-1111-1111-111111111111"
ES = "22222222-2222-2222-2222-222222222222"
LH = "33333333-3333-3333-3333-333333333333"
HUB = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
STREAM = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
DEST = "cccccccc-cccc-cccc-cccc-cccccccccccc"

_TOPOLOGY = {
    "compatibilityLevel": "1.1",
    "sources": [
        {
            "id": HUB,
            "name": "Hub",
            "type": "AzureEventHub",
            "properties": {"dataConnectionId": "secret-conn"},
            "status": "Running",
        }
    ],
    "streams": [
        {
            "id": STREAM,
            "name": "Orders",
            "type": "DefaultStream",
            "inputNodes": [{"name": "Hub"}],
            "status": "Running",
        }
    ],
    "operators": [],
    "destinations": [
        {
            "id": DEST,
            "name": "ToLakehouse",
            "type": "Lakehouse",
            "properties": {
                "workspaceId": WS,
                "itemId": LH,
                "schema": "dbo",
                "deltaTable": "orders",
                "connectionString": "secret",
            },
            "inputNodes": [{"name": "Orders"}],
            "status": "Running",
        }
    ],
}


def _config(**eventstream: object) -> FabricOneLakeSourceConfig:
    return FabricOneLakeSourceConfig(
        credential=AzureCredentialConfig(),
        extract_eventstream=EventstreamConfig(enabled=True, **eventstream),  # type: ignore[arg-type]
    )


def _item(name: str = "orders-live") -> FabricEventstream:
    return FabricEventstream(
        id=ES,
        display_name=name,
        workspace_id=WS,
        description="orders topic",
    )


def _emit(
    *,
    topology: Optional[dict] = None,
    items: Optional[list[FabricEventstream]] = None,
    fail_list: bool = False,
    fail_topology: bool = False,
    eventstream: Optional[dict] = None,
) -> tuple[list[Entity], FabricOneLakeSourceReport, MagicMock]:
    client = MagicMock()
    if fail_list:
        client.list_eventstreams.side_effect = RuntimeError("list failed")
    else:
        client.list_eventstreams.return_value = (
            items if items is not None else [_item()]
        )
    if fail_topology:
        client.get_eventstream_topology.side_effect = RuntimeError("topology failed")
    else:
        client.get_eventstream_topology.return_value = (
            _TOPOLOGY if topology is None else topology
        )
    report = FabricOneLakeSourceReport()
    entities = list(
        process_eventstreams(
            client=client,
            config=_config(**(eventstream or {})),
            report=report,
            workspace=FabricWorkspace(id=WS, name="analytics"),
            owners_fn=lambda principal: None,
            normalize_name=lambda name: name.lower(),
            attach_sensitivity=lambda entity, label_id: [],
        )
    )
    return entities, report, client


def _jobs(entities: Iterable[Entity]) -> dict[str, DataJob]:
    return {
        entity.display_name: entity
        for entity in entities
        if isinstance(entity, DataJob)
    }


def test_disabled_does_not_list() -> None:
    client = MagicMock()
    report = FabricOneLakeSourceReport()
    entities = list(
        process_eventstreams(
            client=client,
            config=FabricOneLakeSourceConfig(credential=AzureCredentialConfig()),
            report=report,
            workspace=FabricWorkspace(id=WS, name="analytics"),
            owners_fn=lambda principal: None,
            normalize_name=lambda name: name,
            attach_sensitivity=lambda entity, label_id: [],
        )
    )
    assert entities == []
    client.list_eventstreams.assert_not_called()


def test_topology_becomes_flow_and_jobs() -> None:
    entities, report, _client = _emit()
    assert report.eventstreams_scanned == 1

    flows = [entity for entity in entities if isinstance(entity, DataFlow)]
    assert len(flows) == 1
    flow = flows[0]
    assert flow.display_name == "orders-live"
    assert str(flow.urn) == str(
        DataFlowUrn.create_from_ids(
            orchestrator="fabric-onelake",
            flow_id=f"{WS}.{ES}",
            env="PROD",
        )
    )

    jobs = _jobs(entities)
    assert set(jobs) == {"Hub", "Orders", "ToLakehouse"}

    hub_props = jobs["Hub"]._get_aspect(DataJobInfoClass).customProperties
    assert hub_props["node_type"] == "AzureEventHub"
    assert hub_props["node_role"] == "source"
    assert "properties" not in hub_props
    assert "dataConnectionId" not in hub_props

    stream_io = jobs["Orders"]._get_aspect(DataJobInputOutputClass)
    assert stream_io.inputDatajobs == [str(jobs["Hub"].urn)]
    assert stream_io.outputDatasets == []

    dest_props = jobs["ToLakehouse"]._get_aspect(DataJobInfoClass).customProperties
    assert dest_props["deltaTable"] == "orders"
    assert "properties" not in dest_props
    assert "connectionString" not in dest_props
    dest_io = jobs["ToLakehouse"]._get_aspect(DataJobInputOutputClass)
    assert dest_io.inputDatajobs == [str(jobs["Orders"].urn)]
    assert len(dest_io.outputDatasets) == 1
    assert dest_io.outputDatasets[0].endswith(f"{WS}.{LH}.dbo.orders,PROD)")


def test_connection_properties_are_stored_when_requested() -> None:
    entities, _report, _client = _emit(
        eventstream={"include_connection_properties": True}
    )
    jobs = _jobs(entities)
    hub_props = jobs["Hub"]._get_aspect(DataJobInfoClass).customProperties
    assert "secret-conn" in hub_props["properties"]


def test_type_pattern_drops_sources() -> None:
    entities, report, _client = _emit(
        eventstream={"source_type_pattern": {"allow": ["^SampleData$"]}}
    )
    jobs = _jobs(entities)
    assert "Hub" not in jobs
    assert "Orders" in jobs
    assert list(report.filtered_eventstream_nodes) == ["Hub (AzureEventHub)"]
    stream_io = jobs["Orders"]._get_aspect(DataJobInputOutputClass)
    assert stream_io is None or not stream_io.inputDatajobs


def test_name_pattern_skips_the_item() -> None:
    _entities, report, client = _emit(
        eventstream={"eventstream_pattern": {"allow": ["^clicks-"]}}
    )
    assert report.eventstreams_scanned == 0
    assert list(report.filtered_eventstreams) == ["orders-live"]
    client.get_eventstream_topology.assert_not_called()


def test_topology_failure_still_emits_the_flow() -> None:
    entities, report, _client = _emit(fail_topology=True)
    assert report.eventstreams_scanned == 1
    assert [entity for entity in entities if isinstance(entity, DataFlow)]
    assert not [entity for entity in entities if isinstance(entity, DataJob)]


def test_list_failure_is_a_warning() -> None:
    entities, report, _client = _emit(fail_list=True)
    assert entities == []
    assert report.eventstreams_scanned == 0
    assert report.warnings
