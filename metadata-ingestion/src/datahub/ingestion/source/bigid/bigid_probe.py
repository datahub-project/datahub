from typing import Any, List, Sequence

from datahub.ingestion.agent.models import ProbeNodeKind, ProbeResult
from datahub.ingestion.agent.probe import ClientProbe, ProbeLevel
from datahub.ingestion.source.common.subtypes import DatasetSubTypes


def _connections(client: Any, config: Any, parent_path: List[str]) -> Sequence[str]:
    return [c.name for c in client.get_connections() if c.name]


def _catalog_objects(client: Any, config: Any, parent_path: List[str]) -> Sequence[str]:
    # BigID's catalog API has no per-connection filter, so this scans the whole catalog
    # and keeps only objects whose source matches the connection just descended into —
    # mirroring _process_catalog's own connection_pattern gate.
    #
    # fully_qualified_name is "{connection}.{rest}"; ClientProbe's container_nodes already
    # re-prefixes a level's names with the parent path, so the connection prefix is
    # stripped here to avoid doubling it in the reported fqn. One consequence: dataset_pattern
    # (which _process_catalog matches against the *full* fully_qualified_name) is instead
    # matched against this connection-relative remainder — a deliberate, documented
    # deviation from production filtering, since ClientProbe's classify always tests the
    # bare name a level returns, not the assembled fqn.
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
# the ProbeNodeKind union, so DatasetSubTypes.CONNECTION (a generic "Connection" kind) is
# reused for want of a closer fit.
BIGID_PROBE = ClientProbe(
    client_factory=lambda config: config.get_client(),
    close=lambda client: client.close(),
    levels=[
        ProbeLevel(DatasetSubTypes.CONNECTION, "connection_pattern", _connections),
        ProbeLevel(DatasetSubTypes.TABLE, "dataset_pattern", _catalog_objects),
    ],
)

BIGID_PROBE_HIERARCHY: List[ProbeNodeKind] = BIGID_PROBE.hierarchy()


def list_bigid_children(config: Any, parent_path: List[str], limit: int) -> ProbeResult:
    return BIGID_PROBE.list_children(config, parent_path, limit)
