from typing import Any, List, Sequence

from datahub.ingestion.agent.models import ProbeNodeKind, ProbeResult
from datahub.ingestion.agent.probe import ClientProbe, ProbeLevel
from datahub.ingestion.source.common.subtypes import (
    DatasetContainerSubTypes,
    DatasetSubTypes,
)
from datahub.ingestion.source.iceberg.iceberg import dataset_name


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
        ProbeLevel(DatasetContainerSubTypes.NAMESPACE, list_names=_namespaces),
        # iceberg.py's _process_dataset matches table_pattern against the dotted
        # "<namespace>.<table>" identifier, not the bare table name. ctx.name is
        # the leaf table name and ctx.parent_path[0] is already the dotted
        # namespace (see _namespaces above), so joining the two reproduces
        # ingestion's own dataset_name(Identifier) exactly.
        ProbeLevel(
            DatasetSubTypes.TABLE,
            list_names=_tables,
            parent=DatasetContainerSubTypes.NAMESPACE,
            filter_target=lambda ctx: dataset_name(list(ctx.parent_path) + [ctx.name]),
        ),
    ],
)

ICEBERG_PROBE_HIERARCHY: List[ProbeNodeKind] = ICEBERG_PROBE.hierarchy()


def list_iceberg_children(
    config: Any, parent_path: List[str], limit: int
) -> ProbeResult:
    return ICEBERG_PROBE.list_children(config, parent_path, limit)
