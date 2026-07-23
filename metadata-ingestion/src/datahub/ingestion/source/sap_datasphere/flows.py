from typing import Dict, List, Optional

from datahub.ingestion.source.common.subtypes import DataJobSubTypes
from datahub.ingestion.source.sap_datasphere.constants import (
    FLOW_ATTR_MAP_EXPRESSION,
    FLOW_ATTR_MAP_TARGET,
    FLOW_COLUMN_EXPR_RE,
    FLOW_COMPONENT_CONSUMER_SUFFIX,
    FLOW_COMPONENT_PRODUCER_SUFFIX,
    FLOW_CONFIG_ATTR_MAPPINGS,
    FLOW_CONFIG_CONNECTION_ID,
    FLOW_CONFIG_DWC_ENTITY,
    FLOW_CONFIG_HANA_CONNECTION,
    FLOW_CONFIG_QUALIFIED_NAME,
    FLOW_KEY_COMPONENT,
    FLOW_KEY_CONFIG,
    FLOW_KEY_CONTENTS,
    FLOW_KEY_METADATA,
    FLOW_KEY_PROCESSES,
    FLOW_LOCAL_CONNECTION_ID,
    OBJECT_TYPE_DATA_FLOWS,
    OBJECT_TYPE_REPLICATION_FLOWS,
    OBJECT_TYPE_TASK_CHAINS,
    OBJECT_TYPE_TRANSFORMATION_FLOWS,
    RF_KEY_SOURCE_SYSTEM,
    RF_KEY_TARGET_SYSTEM,
    RF_KEY_TASKS,
    RF_OBJECT_NAME,
    RF_SYSTEM_CONNECTION_ID,
    RF_SYSTEM_CONNECTION_TYPE,
    RF_TASK_SOURCE_OBJECT,
    RF_TASK_TARGET_OBJECT,
    RF_TASK_TRANSFORM,
)
from datahub.ingestion.source.sap_datasphere.models import (
    AttrMapping,
    FlowColumnMapping,
    FlowEndpoint,
    ParsedFlow,
    ProducerColumns,
    SystemIdentity,
)

_SUBTYPE_BY_OBJECT_TYPE: Dict[str, str] = {
    OBJECT_TYPE_DATA_FLOWS: DataJobSubTypes.SAP_DATA_FLOW,
    OBJECT_TYPE_REPLICATION_FLOWS: DataJobSubTypes.SAP_REPLICATION_FLOW,
    OBJECT_TYPE_TRANSFORMATION_FLOWS: DataJobSubTypes.SAP_TRANSFORMATION_FLOW,
    OBJECT_TYPE_TASK_CHAINS: DataJobSubTypes.SAP_TASK_CHAIN,
}


def _flow_body(payload: Dict, object_type: str, technical_name: str) -> Optional[Dict]:
    # The payload wraps the single object under a top key equal to its object
    # type segment, e.g. {"dataflows": {<name>: {...}}}. Prefer the exact name;
    # fall back to the sole entry so a naming mismatch doesn't lose the flow.
    top = payload.get(object_type)
    if not isinstance(top, dict) or not top:
        return None
    body = top.get(technical_name)
    if body is None and len(top) == 1:
        body = next(iter(top.values()))
    return body if isinstance(body, dict) else None


def _column_from_expression(expression: object) -> Optional[str]:
    if not isinstance(expression, str):
        return None
    match = FLOW_COLUMN_EXPR_RE.match(expression.strip())
    return match.group(1) if match else None


def _dedup_endpoints(endpoints: List[FlowEndpoint]) -> List[FlowEndpoint]:
    seen: set = set()
    out: List[FlowEndpoint] = []
    for ep in endpoints:
        key = (ep.object_name, ep.connection)
        if key not in seen:
            seen.add(key)
            out.append(ep)
    return out


def _process_endpoint(config: Dict) -> Optional[FlowEndpoint]:
    entity = config.get(FLOW_CONFIG_DWC_ENTITY) or config.get(
        FLOW_CONFIG_QUALIFIED_NAME
    )
    if not isinstance(entity, str) or not entity:
        return None
    hana = config.get(FLOW_CONFIG_HANA_CONNECTION)
    connection_id = (
        hana.get(FLOW_CONFIG_CONNECTION_ID) if isinstance(hana, dict) else None
    )
    is_local = connection_id in (None, "", FLOW_LOCAL_CONNECTION_ID)
    return FlowEndpoint(
        object_name=entity,
        is_local=is_local,
        connection=None if is_local else connection_id,
    )


def _attribute_mappings(config: Dict) -> List[AttrMapping]:
    # (downstream_col, upstream_col) pairs for bare column expressions.
    mappings = config.get(FLOW_CONFIG_ATTR_MAPPINGS)
    if not isinstance(mappings, list):
        return []
    pairs: List[AttrMapping] = []
    for m in mappings:
        if not isinstance(m, dict):
            continue
        target = m.get(FLOW_ATTR_MAP_TARGET)
        upstream_col = _column_from_expression(m.get(FLOW_ATTR_MAP_EXPRESSION))
        if isinstance(target, str) and target and upstream_col:
            pairs.append(AttrMapping(downstream_col=target, upstream_col=upstream_col))
    return pairs


def _parse_process_flow(
    payload: Dict, object_type: str, technical_name: str
) -> Optional[ParsedFlow]:
    """Parse a data flow / transformation flow: a process graph whose
    ``*.consumer`` nodes are inputs and ``*.producer`` nodes are outputs, with
    the producer's attributeMappings giving column-level lineage."""
    body = _flow_body(payload, object_type, technical_name)
    if body is None:
        return None
    contents = body.get(FLOW_KEY_CONTENTS)
    processes = contents.get(FLOW_KEY_PROCESSES) if isinstance(contents, dict) else None
    if not isinstance(processes, dict):
        return None

    inputs: List[FlowEndpoint] = []
    outputs: List[FlowEndpoint] = []
    producer_mappings: List[ProducerColumns] = []
    for process in processes.values():
        if not isinstance(process, dict):
            continue
        component = process.get(FLOW_KEY_COMPONENT)
        metadata = process.get(FLOW_KEY_METADATA)
        config = metadata.get(FLOW_KEY_CONFIG) if isinstance(metadata, dict) else None
        if not isinstance(component, str) or not isinstance(config, dict):
            continue
        endpoint = _process_endpoint(config)
        if endpoint is None:
            continue
        if component.endswith(FLOW_COMPONENT_CONSUMER_SUFFIX):
            inputs.append(endpoint)
        elif component.endswith(FLOW_COMPONENT_PRODUCER_SUFFIX):
            outputs.append(endpoint)
            producer_mappings.append(
                ProducerColumns(
                    object_name=endpoint.object_name,
                    mappings=_attribute_mappings(config),
                )
            )

    inputs_t = _dedup_endpoints(inputs)
    outputs_t = _dedup_endpoints(outputs)

    # Column lineage is only unambiguous when there is a single input to attribute
    # the producer's source columns to; multi-input flows keep table-level only.
    column_mappings: List[FlowColumnMapping] = []
    if len(inputs_t) == 1:
        sole_input = inputs_t[0].object_name
        for producer in producer_mappings:
            for pair in producer.mappings:
                column_mappings.append(
                    FlowColumnMapping(
                        downstream_object=producer.object_name,
                        downstream_col=pair.downstream_col,
                        upstream_object=sole_input,
                        upstream_col=pair.upstream_col,
                    )
                )

    if not inputs_t and not outputs_t:
        return None
    return ParsedFlow(
        technical_name=technical_name,
        subtype=_SUBTYPE_BY_OBJECT_TYPE[object_type],
        inputs=inputs_t,
        outputs=outputs_t,
        column_mappings=column_mappings,
    )


def _parse_replication_flow(payload: Dict, technical_name: str) -> Optional[ParsedFlow]:
    """Parse a replication flow: one or more (sourceObject -> targetObject) tasks
    piping data between two external systems, with per-task column mappings."""
    body = _flow_body(payload, OBJECT_TYPE_REPLICATION_FLOWS, technical_name)
    if body is None:
        return None
    contents = body.get(FLOW_KEY_CONTENTS)
    if not isinstance(contents, dict):
        return None

    source_system = _system_identity(contents.get(RF_KEY_SOURCE_SYSTEM))
    target_system = _system_identity(contents.get(RF_KEY_TARGET_SYSTEM))
    tasks = contents.get(RF_KEY_TASKS)
    if not isinstance(tasks, list):
        return None

    inputs: List[FlowEndpoint] = []
    outputs: List[FlowEndpoint] = []
    column_mappings: List[FlowColumnMapping] = []
    for task in tasks:
        if not isinstance(task, dict):
            continue
        source_name = _object_name(task.get(RF_TASK_SOURCE_OBJECT))
        target_name = _object_name(task.get(RF_TASK_TARGET_OBJECT))
        if not source_name or not target_name:
            continue
        inputs.append(
            FlowEndpoint(
                object_name=source_name,
                is_local=False,
                connection=source_system.connection,
                connection_type=source_system.connection_type,
            )
        )
        outputs.append(
            FlowEndpoint(
                object_name=target_name,
                is_local=False,
                connection=target_system.connection,
                connection_type=target_system.connection_type,
            )
        )
        transform = task.get(RF_TASK_TRANSFORM)
        if isinstance(transform, dict):
            for pair in _attribute_mappings(transform):
                column_mappings.append(
                    FlowColumnMapping(
                        downstream_object=target_name,
                        downstream_col=pair.downstream_col,
                        upstream_object=source_name,
                        upstream_col=pair.upstream_col,
                    )
                )

    inputs_t = _dedup_endpoints(inputs)
    outputs_t = _dedup_endpoints(outputs)
    if not inputs_t and not outputs_t:
        return None
    return ParsedFlow(
        technical_name=technical_name,
        subtype=DataJobSubTypes.SAP_REPLICATION_FLOW,
        inputs=inputs_t,
        outputs=outputs_t,
        column_mappings=column_mappings,
    )


def _system_identity(system: object) -> SystemIdentity:
    # sourceSystem/targetSystem are single-element lists carrying the external
    # connection id + type used to route the objects to a DataHub platform.
    if isinstance(system, list) and system and isinstance(system[0], dict):
        entry = system[0]
        return SystemIdentity(
            connection=entry.get(RF_SYSTEM_CONNECTION_ID),
            connection_type=entry.get(RF_SYSTEM_CONNECTION_TYPE),
        )
    return SystemIdentity()


def _object_name(obj: object) -> Optional[str]:
    if isinstance(obj, dict):
        name = obj.get(RF_OBJECT_NAME)
        return name if isinstance(name, str) and name else None
    return None


def _parse_task_chain(payload: Dict, technical_name: str) -> Optional[ParsedFlow]:
    # EXPERIMENTAL: no live task-chain payload was available to reverse-engineer
    # the member/reference grammar, so the chain is surfaced as an IO-less job
    # (its subtype + presence) rather than guessing lineage edges. Extend once a
    # real payload is captured.
    body = _flow_body(payload, OBJECT_TYPE_TASK_CHAINS, technical_name)
    if body is None:
        return None
    return ParsedFlow(
        technical_name=technical_name,
        subtype=DataJobSubTypes.SAP_TASK_CHAIN,
    )


def parse_flow(
    payload: Dict, object_type: str, technical_name: str
) -> Optional[ParsedFlow]:
    """Reduce a flow definition to its IO datasets + column mappings.

    ``transformationflows`` and ``taskchains`` are EXPERIMENTAL: no live payload
    was available to verify their grammar. Transformation flows are parsed with
    the data-flow process-graph reader (they share the ``sap.dis`` process
    model); task chains are surfaced as IO-less jobs.
    """
    if object_type in (OBJECT_TYPE_DATA_FLOWS, OBJECT_TYPE_TRANSFORMATION_FLOWS):
        return _parse_process_flow(payload, object_type, technical_name)
    if object_type == OBJECT_TYPE_REPLICATION_FLOWS:
        return _parse_replication_flow(payload, technical_name)
    if object_type == OBJECT_TYPE_TASK_CHAINS:
        return _parse_task_chain(payload, technical_name)
    return None
