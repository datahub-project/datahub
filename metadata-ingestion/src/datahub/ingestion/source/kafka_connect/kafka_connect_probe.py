from typing import Any, List, Sequence

from datahub.ingestion.agent.models import ProbeNodeKind, ProbeResult
from datahub.ingestion.agent.probe import ClientProbe, ProbeLevel


def _connectors(client: Any, config: Any, parent_path: List[str]) -> Sequence[str]:
    response = client.get(f"{config.get_effective_connect_uri()}/connectors")
    response.raise_for_status()
    return sorted(response.json())


# Kafka Connect is a flat connector namespace filtered by the connector's own
# connector_patterns. A connector is emitted as a plain DataFlow with no subtype
# (see construct_flow_workunit), and no shared-subtype member names the concept —
# so the probe uses the plain, honest kind label "Connector" (ProbeNodeKind is open).
KAFKA_CONNECT_PROBE = ClientProbe(
    client_factory=lambda config: config.get_connect_session(),
    close=lambda session: session.close(),
    levels=[ProbeLevel("Connector", list_names=_connectors)],
)

KAFKA_CONNECT_PROBE_HIERARCHY: List[ProbeNodeKind] = KAFKA_CONNECT_PROBE.hierarchy()


def list_kafka_connect_children(
    config: Any, parent_path: List[str], limit: int
) -> ProbeResult:
    return KAFKA_CONNECT_PROBE.list_children(config, parent_path, limit)
