"""Emit Fabric Eventstream items as pipelines and their topology as jobs.

Each eventstream is a DataFlow. Topology nodes (sources, destinations,
operators, and streams) are DataJobs under that flow. ``inputNodes`` name
references become job-to-job lineage. A Lakehouse destination that names a
workspace, item, and delta table is also an outlet to that OneLake table.
Connection properties are omitted unless ``include_connection_properties``.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Callable, Iterable
from typing import Optional

from datahub.ingestion.source.common.subtypes import (
    DataFlowSubTypes,
    DataJobSubTypes,
)
from datahub.ingestion.source.fabric.common.constants import FABRIC_APP_BASE_URL
from datahub.ingestion.source.fabric.common.models import (
    FabricWorkspace,
    WorkspaceKey,
)
from datahub.ingestion.source.fabric.common.urn_generator import (
    FABRIC_ONELAKE_PLATFORM,
    make_eventstream_flow_id,
    make_onelake_urn,
)
from datahub.ingestion.source.fabric.common.utils import make_workspace_key
from datahub.ingestion.source.fabric.onelake.client import OneLakeClient
from datahub.ingestion.source.fabric.onelake.config import FabricOneLakeSourceConfig
from datahub.ingestion.source.fabric.onelake.models import FabricEventstream
from datahub.ingestion.source.fabric.onelake.report import FabricOneLakeSourceReport
from datahub.metadata.schema_classes import DataJobInputOutputClass
from datahub.metadata.urns import CorpUserUrn, DataJobUrn
from datahub.sdk.dataflow import DataFlow
from datahub.sdk.datajob import DataJob
from datahub.sdk.entity import Entity

logger = logging.getLogger(__name__)

OwnersFn = Callable[[Optional[str]], Optional[list[tuple[CorpUserUrn, str]]]]
SensitivityFn = Callable[[Entity, Optional[str]], Iterable[Entity]]

# Identifiers of a Lakehouse (or similar) target. These are not connection
# secrets; the rest of ``properties`` stays behind include_connection_properties.
_TARGET_PROPERTY_KEYS = ("workspaceId", "itemId", "schema", "deltaTable")

_NODE_SECTIONS: tuple[tuple[str, str, str], ...] = (
    ("sources", "source", DataJobSubTypes.FABRIC_EVENTSTREAM_SOURCE),
    ("streams", "stream", DataJobSubTypes.FABRIC_EVENTSTREAM_STREAM),
    ("operators", "operator", DataJobSubTypes.FABRIC_EVENTSTREAM_OPERATOR),
    ("destinations", "destination", DataJobSubTypes.FABRIC_EVENTSTREAM_DESTINATION),
)


def process_eventstreams(
    *,
    client: OneLakeClient,
    config: FabricOneLakeSourceConfig,
    report: FabricOneLakeSourceReport,
    workspace: FabricWorkspace,
    owners_fn: OwnersFn,
    normalize_name: Callable[[str], str],
    attach_sensitivity: SensitivityFn,
) -> Iterable[Entity]:
    """List eventstreams in one workspace and emit each one that passes the pattern."""
    eventstream_config = config.extract_eventstream
    if not eventstream_config.enabled:
        return

    try:
        items = list(client.list_eventstreams(workspace.id))
    except Exception as error:
        report.warning(
            title="Failed to List Eventstreams",
            message="Unable to retrieve eventstreams from workspace.",
            context=f"workspace={workspace.name}",
            exc=error,
            log=False,
        )
        return

    for item in items:
        if not eventstream_config.eventstream_pattern.allowed(item.display_name):
            report.report_eventstream_filtered(item.display_name)
            continue

        report.report_eventstream_scanned()
        topology: Optional[dict[str, object]] = None
        if eventstream_config.include_topology:
            try:
                topology = client.get_eventstream_topology(workspace.id, item.id)
            except Exception as error:
                report.warning(
                    title="Failed to Read Eventstream Topology",
                    message=(
                        "The eventstream was ingested without its sources and destinations."
                    ),
                    context=(
                        f"workspace={workspace.name}, eventstream={item.display_name}"
                    ),
                    exc=error,
                    log=False,
                )
        logger.info(f"Processing eventstream: {item.display_name} ({item.id})")
        yield from _emit_eventstream(
            config=config,
            report=report,
            workspace=workspace,
            item=item,
            topology=topology,
            owners_fn=owners_fn,
            normalize_name=normalize_name,
            attach_sensitivity=attach_sensitivity,
        )


def _emit_eventstream(
    *,
    config: FabricOneLakeSourceConfig,
    report: FabricOneLakeSourceReport,
    workspace: FabricWorkspace,
    item: FabricEventstream,
    topology: Optional[dict[str, object]],
    owners_fn: OwnersFn,
    normalize_name: Callable[[str], str],
    attach_sensitivity: SensitivityFn,
) -> Iterable[Entity]:
    workspace_key = make_workspace_key(
        workspace.id, config.platform_instance, config.env
    )
    external_url = f"{FABRIC_APP_BASE_URL}/groups/{workspace.id}/eventstreams/{item.id}"
    flow_properties = {
        "eventstream_id": item.id,
        "workspace_id": workspace.id,
    }
    compatibility_level = (topology or {}).get("compatibilityLevel")
    if compatibility_level:
        flow_properties["compatibility_level"] = str(compatibility_level)

    dataflow = DataFlow(
        platform=FABRIC_ONELAKE_PLATFORM,
        name=make_eventstream_flow_id(workspace.id, item.id),
        platform_instance=config.platform_instance,
        env=config.env,
        display_name=item.display_name,
        description=item.description,
        subtype=DataFlowSubTypes.FABRIC_EVENTSTREAM,
        parent_container=workspace_key,
        external_url=external_url,
        custom_properties=flow_properties,
        owners=owners_fn(item.owner_user_principal_name),
    )
    yield dataflow
    yield from attach_sensitivity(dataflow, item.sensitivity_label_id)

    if topology is None:
        return

    nodes = _kept_nodes(topology, config, report)
    urn_by_name = {
        str(node["name"]): str(
            DataJobUrn.create_from_ids(
                job_id=str(node["job_id"]), data_flow_urn=str(dataflow.urn)
            )
        )
        for node in nodes
    }
    for node in nodes:
        yield _emit_node(
            config=config,
            dataflow=dataflow,
            workspace_key=workspace_key,
            external_url=external_url,
            node=node,
            urn_by_name=urn_by_name,
            normalize_name=normalize_name,
            owners=owners_fn(item.owner_user_principal_name),
        )


def _kept_nodes(
    topology: dict[str, object],
    config: FabricOneLakeSourceConfig,
    report: FabricOneLakeSourceReport,
) -> list[dict[str, object]]:
    eventstream_config = config.extract_eventstream
    kept: list[dict[str, object]] = []
    for section, role, subtype in _NODE_SECTIONS:
        raw_nodes = topology.get(section) or []
        if not isinstance(raw_nodes, list):
            continue
        for raw in raw_nodes:
            if not isinstance(raw, dict):
                continue
            name = raw.get("name")
            if not isinstance(name, str) or not name:
                continue
            raw_type = raw.get("type")
            node_type = raw_type if isinstance(raw_type, str) else ""
            if role == "source" and not eventstream_config.source_type_pattern.allowed(
                node_type
            ):
                report.report_eventstream_node_filtered(f"{name} ({node_type})")
                continue
            if (
                role == "destination"
                and not eventstream_config.destination_type_pattern.allowed(node_type)
            ):
                report.report_eventstream_node_filtered(f"{name} ({node_type})")
                continue
            raw_id = raw.get("id")
            job_id = raw_id if isinstance(raw_id, str) and raw_id else name
            kept.append(
                {
                    "job_id": job_id,
                    "name": name,
                    "role": role,
                    "subtype": subtype,
                    "node_type": node_type,
                    "raw": raw,
                }
            )
    return kept


def _emit_node(
    *,
    config: FabricOneLakeSourceConfig,
    dataflow: DataFlow,
    workspace_key: WorkspaceKey,
    external_url: str,
    node: dict[str, object],
    urn_by_name: dict[str, str],
    normalize_name: Callable[[str], str],
    owners: Optional[list[tuple[CorpUserUrn, str]]],
) -> DataJob:
    raw = node["raw"]
    assert isinstance(raw, dict)
    job_id = str(node["job_id"])
    name = str(node["name"])
    this_urn = urn_by_name[name]
    input_job_urns = sorted(
        {
            urn
            for input_name in _input_names(raw)
            if (urn := urn_by_name.get(input_name)) is not None and urn != this_urn
        }
    )
    outlet = _lakehouse_outlet(raw, config, normalize_name)
    extra_aspects = None
    if input_job_urns or outlet:
        extra_aspects = [
            DataJobInputOutputClass(
                inputDatasets=[],
                outputDatasets=[outlet] if outlet else [],
                inputDatajobs=input_job_urns or None,
            )
        ]

    datajob = DataJob(
        name=job_id,
        flow=dataflow,
        display_name=name,
        subtype=str(node["subtype"]),
        external_url=external_url,
        custom_properties=_custom_properties(
            raw,
            role=str(node["role"]),
            node_type=str(node["node_type"]),
            include_connection_properties=(
                config.extract_eventstream.include_connection_properties
            ),
        ),
        owners=owners,
        extra_aspects=extra_aspects,
    )
    datajob._set_container(workspace_key)
    return datajob


def _input_names(node: dict[str, object]) -> list[str]:
    references = node.get("inputNodes")
    if not isinstance(references, list):
        return []
    names: list[str] = []
    for reference in references:
        if not isinstance(reference, dict):
            continue
        name = reference.get("name")
        if isinstance(name, str) and name:
            names.append(name)
    return names


def _lakehouse_outlet(
    node: dict[str, object],
    config: FabricOneLakeSourceConfig,
    normalize_name: Callable[[str], str],
) -> Optional[str]:
    if node.get("type") != "Lakehouse":
        return None
    properties = node.get("properties")
    if not isinstance(properties, dict):
        return None
    workspace_id = properties.get("workspaceId")
    item_id = properties.get("itemId")
    table_name = properties.get("deltaTable")
    if not (
        isinstance(workspace_id, str)
        and workspace_id
        and isinstance(item_id, str)
        and item_id
        and isinstance(table_name, str)
        and table_name
    ):
        return None
    schema_name = properties.get("schema")
    return make_onelake_urn(
        workspace_id=workspace_id,
        item_id=item_id,
        table_name=normalize_name(table_name),
        schema_name=(
            normalize_name(schema_name) if isinstance(schema_name, str) else None
        ),
        env=config.env,
        platform_instance=config.platform_instance,
    )


def _custom_properties(
    node: dict[str, object],
    *,
    role: str,
    node_type: str,
    include_connection_properties: bool,
) -> dict[str, str]:
    properties: dict[str, str] = {"node_role": role}
    node_id = node.get("id")
    if isinstance(node_id, str) and node_id:
        properties["node_id"] = node_id
    if node_type:
        properties["node_type"] = node_type
    status = node.get("status")
    if isinstance(status, str) and status:
        properties["status"] = status
    error = _error_text(node.get("error"))
    if error:
        properties["error"] = error

    raw_properties = node.get("properties")
    if isinstance(raw_properties, dict):
        for key in _TARGET_PROPERTY_KEYS:
            value = raw_properties.get(key)
            if isinstance(value, str) and value:
                properties[key] = value
        if include_connection_properties and raw_properties:
            properties["properties"] = json.dumps(
                raw_properties, default=str, sort_keys=True
            )
    return properties


def _error_text(error: object) -> Optional[str]:
    if not isinstance(error, dict):
        return None
    parts = [
        str(error[key])
        for key in ("errorCode", "message")
        if isinstance(error.get(key), str) and error.get(key)
    ]
    return ": ".join(parts) or None
