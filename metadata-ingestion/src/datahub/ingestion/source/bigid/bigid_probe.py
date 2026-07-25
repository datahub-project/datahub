from typing import Any, List, Sequence

from datahub.ingestion.agent.models import ProbeNodeKind, ProbeResult
from datahub.ingestion.agent.probe import ClientProbe, ProbeLevel
from datahub.ingestion.source.common.subtypes import DatasetSubTypes


def _connections(client: Any, config: Any, parent_path: List[str]) -> Sequence[str]:
    return [c.name for c in client.get_connections() if c.name]


def _catalog_objects(client: Any, config: Any, parent_path: List[str]) -> Sequence[str]:
    # BigID's catalog API has no per-connection filter, so this scans the whole catalog
    # and keeps only objects whose source matches the connection just descended into —
    # mirroring _process_catalog's own connection_pattern gate. fully_qualified_name is
    # "{connection}.{rest}"; the connection prefix is stripped here because ClientProbe's
    # level_nodes re-prefixes with the parent path, reassembling the full fqn — and
    # the level sets classify_on_fqn so dataset_pattern is matched against that full fqn,
    # exactly as _process_catalog does.
    connection = parent_path[0]
    prefix = f"{connection}."
    names: List[str] = []
    for obj in client.get_catalog_objects():
        if obj.source != connection or not obj.fully_qualified_name:
            continue
        fqn = obj.fully_qualified_name
        names.append(fqn[len(prefix) :] if fqn.startswith(prefix) else fqn)
    return names


# BigID is connection -> catalog object, reached through the connector's own REST client
# (config.get_client()). There is no dedicated container subtype for a BigID connection in
# the shared taxonomy, so DatasetSubTypes.CONNECTION (a generic "Connection" kind) is
# reused for want of a closer fit.
BIGID_PROBE = ClientProbe(
    client_factory=lambda config: config.get_client(),
    close=lambda client: client.close(),
    levels=[
        ProbeLevel(DatasetSubTypes.CONNECTION, list_names=_connections),
        # BigID's own noun ("dataset"); the kind is DataHub's generic Table subtype.
        ProbeLevel(
            DatasetSubTypes.TABLE,
            "dataset_pattern",
            _catalog_objects,
            classify_on_fqn=True,
            parent=DatasetSubTypes.CONNECTION,
        ),
    ],
)

BIGID_PROBE_HIERARCHY: List[ProbeNodeKind] = BIGID_PROBE.hierarchy()


def list_bigid_children(config: Any, parent_path: List[str], limit: int) -> ProbeResult:
    return BIGID_PROBE.list_children(config, parent_path, limit)
