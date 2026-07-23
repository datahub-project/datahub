from typing import Any, List, Sequence

from datahub.ingestion.agent.models import ProbeNodeKind, ProbeResult
from datahub.ingestion.agent.probe import ClientProbe, ProbeLevel
from datahub.ingestion.source.common.subtypes import (
    DatasetContainerSubTypes,
    DatasetSubTypes,
)


def _namespaces(client: Any, config: Any, parent_path: List[str]) -> Sequence[str]:
    return [".".join(namespace) for namespace in client.list_namespaces()]


def _tables(client: Any, config: Any, parent_path: List[str]) -> Sequence[str]:
    # Catalog.list_tables accepts the dotted namespace string directly; it
    # splits it back into an Identifier tuple internally.
    return [identifier[-1] for identifier in client.list_tables(parent_path[0])]


# Iceberg is a 2-level catalog (namespace -> table) reached through the
# pyiceberg catalog the config already exposes via get_catalog().
ICEBERG_PROBE = ClientProbe(
    client_factory=lambda config: config.get_catalog(),
    levels=[
        ProbeLevel(
            DatasetContainerSubTypes.NAMESPACE, "namespace_pattern", _namespaces
        ),
        ProbeLevel(DatasetSubTypes.TABLE, "table_pattern", _tables),
    ],
)

ICEBERG_PROBE_HIERARCHY: List[ProbeNodeKind] = ICEBERG_PROBE.hierarchy()


def list_iceberg_children(
    config: Any, parent_path: List[str], limit: int
) -> ProbeResult:
    return ICEBERG_PROBE.list_children(config, parent_path, limit)
