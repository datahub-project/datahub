from typing import Dict, Optional

from datahub.ingestion.source.common.subtypes import DataJobSubTypes
from datahub.ingestion.source.sap_datasphere.constants import (
    OBJECT_TYPE_DATA_FLOWS,
    OBJECT_TYPE_REPLICATION_FLOWS,
    OBJECT_TYPE_TASK_CHAINS,
    OBJECT_TYPE_TRANSFORMATION_FLOWS,
)
from datahub.ingestion.source.sap_datasphere.flows import parse_flow

# Anonymized payloads that mirror the shape of live dwaas-core flow definitions
# (generic table/column names — no customer identifiers).


def _process(component: str, config: Dict) -> Dict:
    return {"component": component, "metadata": {"config": config}}


def _data_flow_payload(
    name: str = "MY_DATA_FLOW",
    source_connection: str = "$DWC",
) -> Dict:
    return {
        OBJECT_TYPE_DATA_FLOWS: {
            name: {
                "contents": {
                    "processes": {
                        "p1": _process(
                            "com.sap.dataflow.table.consumer",
                            {
                                "dwcEntity": "SRC_TABLE",
                                "hanaConnection": {"connectionID": source_connection},
                            },
                        ),
                        "p2": _process(
                            "com.sap.dataflow.table.producer",
                            {
                                "dwcEntity": "TGT_TABLE",
                                "hanaConnection": {"connectionID": "$DWC"},
                                "attributeMappings": [
                                    {"target": "OUT_COL", "expression": '"IN_COL"'},
                                    # Compound expression stays table-level only.
                                    {
                                        "target": "COMPUTED",
                                        "expression": '"A" + "B"',
                                    },
                                ],
                            },
                        ),
                    }
                }
            }
        }
    }


def test_parse_data_flow_table_and_column_lineage():
    parsed = parse_flow(_data_flow_payload(), OBJECT_TYPE_DATA_FLOWS, "MY_DATA_FLOW")
    assert parsed is not None
    assert parsed.subtype == DataJobSubTypes.SAP_DATA_FLOW
    assert [e.object_name for e in parsed.inputs] == ["SRC_TABLE"]
    assert [e.object_name for e in parsed.outputs] == ["TGT_TABLE"]
    assert parsed.inputs[0].is_local is True
    # Only the bare-quoted-identifier mapping yields a column edge.
    assert len(parsed.column_mappings) == 1
    mapping = parsed.column_mappings[0]
    assert (mapping.upstream_object, mapping.upstream_col) == ("SRC_TABLE", "IN_COL")
    assert (mapping.downstream_object, mapping.downstream_col) == (
        "TGT_TABLE",
        "OUT_COL",
    )


def test_parse_data_flow_external_source_endpoint():
    parsed = parse_flow(
        _data_flow_payload(source_connection="MY_REMOTE_CONN"),
        OBJECT_TYPE_DATA_FLOWS,
        "MY_DATA_FLOW",
    )
    assert parsed is not None
    src = parsed.inputs[0]
    assert src.is_local is False
    assert src.connection == "MY_REMOTE_CONN"


def test_parse_data_flow_multiple_inputs_suppresses_column_lineage():
    """With more than one input the producer's source columns can't be
    unambiguously attributed, so only table-level edges are kept."""
    payload = _data_flow_payload()
    payload[OBJECT_TYPE_DATA_FLOWS]["MY_DATA_FLOW"]["contents"]["processes"]["p3"] = (
        _process(
            "com.sap.dataflow.table.consumer",
            {
                "dwcEntity": "SECOND_SRC",
                "hanaConnection": {"connectionID": "$DWC"},
            },
        )
    )
    parsed = parse_flow(payload, OBJECT_TYPE_DATA_FLOWS, "MY_DATA_FLOW")
    assert parsed is not None
    assert {e.object_name for e in parsed.inputs} == {"SRC_TABLE", "SECOND_SRC"}
    assert parsed.column_mappings == []


def test_parse_data_flow_body_falls_back_to_sole_entry_on_name_mismatch():
    payload = _data_flow_payload(name="ACTUAL_NAME")
    parsed = parse_flow(payload, OBJECT_TYPE_DATA_FLOWS, "REQUESTED_NAME")
    assert parsed is not None
    assert parsed.technical_name == "REQUESTED_NAME"
    assert [e.object_name for e in parsed.inputs] == ["SRC_TABLE"]


def test_parse_transformation_flow_uses_process_reader():
    payload = _data_flow_payload(name="MY_TF")
    payload[OBJECT_TYPE_TRANSFORMATION_FLOWS] = payload.pop(OBJECT_TYPE_DATA_FLOWS)
    parsed = parse_flow(payload, OBJECT_TYPE_TRANSFORMATION_FLOWS, "MY_TF")
    assert parsed is not None
    assert parsed.subtype == DataJobSubTypes.SAP_TRANSFORMATION_FLOW
    assert [e.object_name for e in parsed.outputs] == ["TGT_TABLE"]


def _replication_flow_payload(
    name: str = "MY_REPL_FLOW",
    source_system: Optional[Dict] = None,
    target_system: Optional[Dict] = None,
) -> Dict:
    source_system = source_system or {
        "connectionId": "SRC_CONN",
        "connectionType": "S3",
    }
    target_system = target_system or {
        "connectionId": "TGT_CONN",
        "connectionType": "HANA",
    }
    return {
        OBJECT_TYPE_REPLICATION_FLOWS: {
            name: {
                "contents": {
                    "sourceSystem": [source_system],
                    "targetSystem": [target_system],
                    "replicationTasks": [
                        {
                            "sourceObject": {"name": "SRC_OBJ"},
                            "targetObject": {"name": "TGT_OBJ"},
                            "transform": {
                                "attributeMappings": [
                                    {"target": "T_COL", "expression": '"S_COL"'},
                                ]
                            },
                        }
                    ],
                }
            }
        }
    }


def test_parse_replication_flow_endpoints_and_column_lineage():
    parsed = parse_flow(
        _replication_flow_payload(), OBJECT_TYPE_REPLICATION_FLOWS, "MY_REPL_FLOW"
    )
    assert parsed is not None
    assert parsed.subtype == DataJobSubTypes.SAP_REPLICATION_FLOW
    src = parsed.inputs[0]
    tgt = parsed.outputs[0]
    assert (src.object_name, src.connection, src.connection_type) == (
        "SRC_OBJ",
        "SRC_CONN",
        "S3",
    )
    assert (tgt.object_name, tgt.connection, tgt.connection_type) == (
        "TGT_OBJ",
        "TGT_CONN",
        "HANA",
    )
    assert src.is_local is False
    assert len(parsed.column_mappings) == 1
    mapping = parsed.column_mappings[0]
    assert (mapping.upstream_object, mapping.upstream_col) == ("SRC_OBJ", "S_COL")
    assert (mapping.downstream_object, mapping.downstream_col) == ("TGT_OBJ", "T_COL")


def test_parse_replication_flow_dwc_target_is_local():
    # A $DWC (managed) target system means the object lives in the tenant's own
    # HANA Cloud and must be emitted on the sap_datasphere platform, not treated
    # as an external HANA table.
    parsed = parse_flow(
        _replication_flow_payload(
            target_system={"connectionId": "$DWC", "connectionType": "HANA"},
        ),
        OBJECT_TYPE_REPLICATION_FLOWS,
        "MY_REPL_FLOW",
    )
    assert parsed is not None
    tgt = parsed.outputs[0]
    assert tgt.is_local is True
    assert tgt.connection is None
    assert tgt.container is None


def test_parse_replication_flow_carries_system_container():
    # The system container (schema/dataset path) is captured on each external
    # endpoint so the URN can be schema-qualified downstream.
    parsed = parse_flow(
        _replication_flow_payload(
            source_system={
                "connectionId": "SRC_CONN",
                "connectionType": "ABAP",
                "container": "/CDS_EXTRACTION",
            },
            target_system={
                "connectionId": "TGT_CONN",
                "connectionType": "BIGQUERY",
                "container": "/staging",
            },
        ),
        OBJECT_TYPE_REPLICATION_FLOWS,
        "MY_REPL_FLOW",
    )
    assert parsed is not None
    assert parsed.inputs[0].container == "/CDS_EXTRACTION"
    assert parsed.outputs[0].container == "/staging"


def test_parse_task_chain_is_io_less_job():
    payload = {OBJECT_TYPE_TASK_CHAINS: {"MY_CHAIN": {"kind": "sap.dis.taskchain"}}}
    parsed = parse_flow(payload, OBJECT_TYPE_TASK_CHAINS, "MY_CHAIN")
    assert parsed is not None
    assert parsed.subtype == DataJobSubTypes.SAP_TASK_CHAIN
    assert parsed.inputs == []
    assert parsed.outputs == []
    assert parsed.column_mappings == []


def test_parse_flow_unknown_object_type_returns_none():
    assert parse_flow({}, "views", "X") is None


def test_parse_flow_missing_body_returns_none():
    assert parse_flow({}, OBJECT_TYPE_DATA_FLOWS, "MISSING") is None
