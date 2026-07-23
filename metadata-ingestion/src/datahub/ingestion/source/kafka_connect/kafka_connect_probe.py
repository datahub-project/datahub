from typing import Any, List, Sequence

from datahub.ingestion.agent.models import ProbeNodeKind, ProbeResult
from datahub.ingestion.agent.probe import ClientProbe, ProbeLevel
from datahub.ingestion.source.common.subtypes import DataFlowSubTypes


def _connectors(client: Any, config: Any, parent_path: List[str]) -> Sequence[str]:
    response = client.get(f"{config.get_effective_connect_uri()}/connectors")
    response.raise_for_status()
    return sorted(response.json())


# Kafka Connect is a flat connector namespace filtered by the connector's own
# connector_patterns — one ProbeLevel, no bespoke code. Neither DataFlowSubTypes nor
# DataJobSubTypes has a generic "connector" member: each connector is emitted as a
# plain DataFlow with no subtype at all (see construct_flow_workunit). Of the existing
# DataFlowSubTypes members, DLT_PIPELINE is reused as the least-wrong fit — both
# represent one named, config-driven data-movement pipeline mapped 1:1 to a DataFlow
# entity — rather than inventing a new "Kafka Connect Connector" member.
KAFKA_CONNECT_PROBE = ClientProbe(
    client_factory=lambda config: config.get_connect_session(),
    close=lambda session: session.close(),
    levels=[
        ProbeLevel(DataFlowSubTypes.DLT_PIPELINE, "connector_patterns", _connectors)
    ],
)

KAFKA_CONNECT_PROBE_HIERARCHY: List[ProbeNodeKind] = KAFKA_CONNECT_PROBE.hierarchy()


def list_kafka_connect_children(
    config: Any, parent_path: List[str], limit: int
) -> ProbeResult:
    return KAFKA_CONNECT_PROBE.list_children(config, parent_path, limit)
