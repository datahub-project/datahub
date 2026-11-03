"""Emit Fabric Eventstream items as pipelines and their topology as jobs.

Each eventstream is a DataFlow. Sources and streams are datasets, because
that is the entity with a Columns tab. Their columns come from the node's own
``inputSchemas`` and from downstream nodes that name them. A stream with no
schema of its own keeps the columns of the sources that feed it, and a source
with no schema of its own keeps the columns of the stream it feeds. Operators
and destinations stay data jobs. A Lakehouse destination that names a
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
    DatasetSubTypes,
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
from datahub.metadata.schema_classes import (
    ArrayTypeClass,
    BooleanTypeClass,
    DataJobInputOutputClass,
    FineGrainedLineageClass,
    NullTypeClass,
    NumberTypeClass,
    RecordTypeClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    StringTypeClass,
    TimeTypeClass,
    UpstreamClass,
)
from datahub.metadata.urns import CorpUserUrn, DataJobUrn, DatasetUrn
from datahub.sdk.dataflow import DataFlow
from datahub.sdk.datajob import DataJob
from datahub.sdk.dataset import Dataset
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
            else:
                _enrich_topology_schemas(
                    client=client,
                    report=report,
                    workspace=workspace,
                    item=item,
                    topology=topology,
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
    owners = owners_fn(item.owner_user_principal_name)
    # Sources and streams are datasets. A data job has no Columns tab.
    dataset_nodes = [node for node in nodes if node["role"] in ("source", "stream")]
    job_nodes = [node for node in nodes if node["role"] not in ("source", "stream")]
    urn_by_name = {
        str(node["name"]): str(
            DataJobUrn.create_from_ids(
                job_id=str(node["job_id"]), data_flow_urn=str(dataflow.urn)
            )
        )
        for node in job_nodes
    }
    columns_by_name = _columns_for_sources_and_streams(nodes, topology)
    schema_urn_by_name = {
        str(node["name"]): str(
            DatasetUrn.create_from_ids(
                platform_id=FABRIC_ONELAKE_PLATFORM,
                table_name=f"{workspace.id}.{item.id}.{node['job_id']}",
                platform_instance=config.platform_instance,
                env=config.env,
            )
        )
        for node in dataset_nodes
    }
    for node in dataset_nodes:
        name = str(node["name"])
        dataset = _schema_dataset(
            config=config,
            workspace=workspace,
            item=item,
            workspace_key=workspace_key,
            external_url=external_url,
            node=node,
            columns=columns_by_name.get(name) or [],
            upstreams=[
                upstream
                for input_name in _input_names(_raw(node))
                if (upstream := schema_urn_by_name.get(input_name)) is not None
            ],
            normalize_name=normalize_name,
            owners=owners,
        )
        yield dataset
        yield from attach_sensitivity(dataset, item.sensitivity_label_id)
    for node in job_nodes:
        yield _emit_node(
            config=config,
            dataflow=dataflow,
            workspace_key=workspace_key,
            external_url=external_url,
            node=node,
            urn_by_name=urn_by_name,
            schema_urn_by_name=schema_urn_by_name,
            normalize_name=normalize_name,
            owners=owners,
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
    schema_urn_by_name: dict[str, str],
    normalize_name: Callable[[str], str],
    owners: Optional[list[tuple[CorpUserUrn, str]]],
) -> DataJob:
    raw = node["raw"]
    assert isinstance(raw, dict)
    job_id = str(node["job_id"])
    name = str(node["name"])
    this_urn = urn_by_name[name]
    input_names = _input_names(raw)
    input_job_urns = sorted(
        {
            urn
            for input_name in input_names
            if (urn := urn_by_name.get(input_name)) is not None and urn != this_urn
        }
    )
    inlet_urns = sorted(
        {
            dataset_urn
            for input_name in input_names
            if (dataset_urn := schema_urn_by_name.get(input_name)) is not None
        }
    )
    outlet_urns: list[str] = []
    if schema_urn := schema_urn_by_name.get(name):
        outlet_urns.append(schema_urn)
    lakehouse_outlet = _lakehouse_outlet(raw, config, normalize_name)
    if lakehouse_outlet:
        outlet_urns.append(lakehouse_outlet)
    extra_aspects = None
    if input_job_urns or inlet_urns or outlet_urns:
        extra_aspects = [
            DataJobInputOutputClass(
                inputDatasets=inlet_urns,
                outputDatasets=outlet_urns,
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


def _raw(node: dict[str, object]) -> dict[str, object]:
    raw = node["raw"]
    assert isinstance(raw, dict)
    return raw


# The topology summary often leaves inputSchemas off sources and destinations.
# The per-node GET returns them. Operators already carry schemas on the topology.
_SCHEMA_NODE_COLLECTIONS = ("sources", "destinations")


def _enrich_topology_schemas(
    *,
    client: OneLakeClient,
    report: FabricOneLakeSourceReport,
    workspace: FabricWorkspace,
    item: FabricEventstream,
    topology: dict[str, object],
) -> None:
    for collection in _SCHEMA_NODE_COLLECTIONS:
        raw_nodes = topology.get(collection)
        if not isinstance(raw_nodes, list):
            continue
        for raw in raw_nodes:
            if not isinstance(raw, dict) or _own_columns(raw):
                continue
            node_id = raw.get("id")
            if not isinstance(node_id, str) or not node_id:
                continue
            try:
                detail = client.get_eventstream_node(
                    workspace.id, item.id, collection, node_id
                )
            except Exception as error:
                report.warning(
                    title="Failed to Read Eventstream Node Schema",
                    message="Eventstream columns were ingested without this node.",
                    context=(
                        f"workspace={workspace.name}, eventstream={item.display_name}, "
                        f"{collection}={raw.get('name') or node_id}"
                    ),
                    exc=error,
                    log=False,
                )
                continue
            if isinstance(detail, dict) and _own_columns(detail):
                raw["inputSchemas"] = detail.get("inputSchemas")


def _columns_for_sources_and_streams(
    nodes: list[dict[str, object]], topology: dict[str, object]
) -> dict[str, list[object]]:
    """Columns to show on each source and stream.

    ``inputSchemas[].name`` is often the inferred schema name (for example
    ``Bicycles``), not the stream or source name. A single upstream node still
    receives those columns. A stream with no schema keeps its inputs' columns,
    and a source with no schema keeps the columns of a stream it feeds.
    """
    columns: dict[str, list[object]] = {
        str(node["name"]): [] for node in nodes if node["role"] in ("source", "stream")
    }
    for section, role, _subtype in _NODE_SECTIONS:
        raw_nodes = topology.get(section) or []
        if not isinstance(raw_nodes, list):
            continue
        for raw in raw_nodes:
            if not isinstance(raw, dict):
                continue
            input_names = [name for name in _input_names(raw) if name in columns]
            node_name = raw.get("name")
            for entry_columns in _schema_column_lists(raw):
                schema_name = _schema_entry_name(raw, entry_columns)
                if schema_name in columns:
                    _add_columns(columns, schema_name, entry_columns)
                elif len(input_names) == 1:
                    _add_columns(columns, input_names[0], entry_columns)
                elif role in ("source", "stream") and isinstance(node_name, str):
                    _add_columns(columns, node_name, entry_columns)
                else:
                    for input_name in input_names:
                        _add_columns(columns, input_name, entry_columns)

    for node in nodes:
        name = str(node["name"])
        if node["role"] != "stream" or columns.get(name):
            continue
        for input_name in _input_names(_raw(node)):
            _add_columns(columns, name, columns.get(input_name) or [])

    for node in nodes:
        name = str(node["name"])
        if node["role"] != "source" or columns.get(name):
            continue
        for other in nodes:
            if other["role"] != "stream" or name not in _input_names(_raw(other)):
                continue
            if columns.get(str(other["name"])):
                _add_columns(columns, name, columns[str(other["name"])])
                break
    return columns


def _schema_entry_name(raw: dict[str, object], entry_columns: list[object]) -> str:
    """The inputSchemas entry name for this column list, when it has one."""
    entries = raw.get("inputSchemas")
    if not isinstance(entries, list):
        return ""
    for entry in entries:
        if isinstance(entry, dict) and _entry_columns(entry) is entry_columns:
            name = entry.get("name")
            return name if isinstance(name, str) else ""
    return ""


def _add_columns(
    columns: dict[str, list[object]], name: str, extra: list[object]
) -> None:
    if name not in columns:
        return
    seen = {
        col.get("name") or col.get("columnName")
        for col in columns[name]
        if isinstance(col, dict)
    }
    for col in extra:
        if not isinstance(col, dict):
            continue
        col_name = col.get("name") or col.get("columnName")
        if col_name in seen:
            continue
        columns[name].append(col)
        seen.add(col_name)


def _own_columns(node: dict[str, object]) -> list[object]:
    columns: list[object] = []
    for entry_columns in _schema_column_lists(node):
        columns.extend(entry_columns)
    return columns


def _schema_column_lists(node: dict[str, object]) -> list[list[object]]:
    entries = node.get("inputSchemas")
    if not isinstance(entries, list):
        return []
    lists: list[list[object]] = []
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        columns = _entry_columns(entry)
        if columns:
            lists.append(columns)
    return lists


def _entry_columns(entry: dict[str, object]) -> list[object]:
    schema = entry.get("schema")
    if isinstance(schema, list):
        return schema
    if not isinstance(schema, dict):
        return []
    for key in ("columns", "fields"):
        columns = schema.get(key)
        if isinstance(columns, list) and columns:
            return columns
    return []


def _schema_dataset(
    *,
    config: FabricOneLakeSourceConfig,
    workspace: FabricWorkspace,
    item: FabricEventstream,
    workspace_key: WorkspaceKey,
    external_url: str,
    node: dict[str, object],
    columns: list[object],
    upstreams: list[str],
    normalize_name: Callable[[str], str],
    owners: Optional[list[tuple[CorpUserUrn, str]]],
) -> Dataset:
    fields = _schema_fields(columns, normalize_name)
    # list is invariant, so the URN list has to be typed as Dataset's upstream union.
    schema_upstreams: (
        list[str | DatasetUrn | UpstreamClass | FineGrainedLineageClass] | None
    ) = list(upstreams) if upstreams else None
    subtype = (
        DatasetSubTypes.FABRIC_EVENTSTREAM_SOURCE
        if node["role"] == "source"
        else DatasetSubTypes.FABRIC_EVENTSTREAM_STREAM
    )
    return Dataset(
        platform=FABRIC_ONELAKE_PLATFORM,
        name=f"{workspace.id}.{item.id}.{node['job_id']}",
        platform_instance=config.platform_instance,
        env=config.env,
        display_name=str(node["name"]),
        parent_container=workspace_key,
        subtype=subtype,
        external_url=external_url,
        custom_properties=_custom_properties(
            _raw(node),
            role=str(node["role"]),
            node_type=str(node["node_type"]),
            include_connection_properties=(
                config.extract_eventstream.include_connection_properties
            ),
        ),
        owners=owners,
        schema=fields or None,
        upstreams=schema_upstreams,
    )


def _schema_fields(
    columns: list[object],
    normalize_name: Callable[[str], str],
    parent: str = "",
) -> list[SchemaFieldClass]:
    fields: list[SchemaFieldClass] = []
    for column in columns:
        if not isinstance(column, dict):
            continue
        raw_name = column.get("name") or column.get("columnName")
        if not isinstance(raw_name, str) or not raw_name:
            continue
        raw_type = column.get("type")
        if not isinstance(raw_type, str):
            raw_type = column.get("dataType")
        native = raw_type if isinstance(raw_type, str) else "Any"
        field_type = _column_type(native)[1]
        name = normalize_name(raw_name)
        path = f"{parent}.{name}" if parent else name
        fields.append(
            SchemaFieldClass(
                fieldPath=path,
                type=field_type,
                nativeDataType=native,
            )
        )
        nested = column.get("fields")
        if native == "Record" and isinstance(nested, list):
            fields.extend(_schema_fields(nested, normalize_name, path))
        items = column.get("items")
        if native == "Array" and isinstance(items, dict):
            item_fields = items.get("fields")
            if isinstance(item_fields, list):
                fields.extend(_schema_fields(item_fields, normalize_name, path))
    return fields


def _column_type(native: str) -> tuple[str, SchemaFieldDataTypeClass]:
    if native == "BigInt":
        return "long", SchemaFieldDataTypeClass(type=NumberTypeClass())
    if native == "Float":
        return "float", SchemaFieldDataTypeClass(type=NumberTypeClass())
    if native == "Nvarchar(max)":
        return "string", SchemaFieldDataTypeClass(type=StringTypeClass())
    if native == "DateTime":
        return "timestamp", SchemaFieldDataTypeClass(type=TimeTypeClass())
    if native == "Bit":
        return "boolean", SchemaFieldDataTypeClass(type=BooleanTypeClass())
    if native == "Record":
        return "struct", SchemaFieldDataTypeClass(type=RecordTypeClass())
    if native == "Array":
        return "array", SchemaFieldDataTypeClass(type=ArrayTypeClass())
    return "null", SchemaFieldDataTypeClass(type=NullTypeClass())


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
