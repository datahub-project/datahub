from typing import Any, Dict, List, Sequence, Set

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.source.airbyte.models import (
    AirbyteConfigStreamRef,
    NamespaceQueueResult,
    StreamNamespacesByName,
)


def clean_uri(uri: str) -> str:
    return uri.rstrip("/")


def apply_pattern(
    items: List[Dict[str, Any]],
    pattern: AllowDenyPattern,
    name_key: str = "name",
) -> List[Dict[str, Any]]:
    if not items:
        return []
    if pattern.allow_all():
        return items
    return [
        item
        for item in items
        if (name := item.get(name_key, "")) and pattern.allowed(str(name))
    ]


def namespace_queues_for_catalog(
    config_streams: Sequence[AirbyteConfigStreamRef],
    namespaces_by_name: StreamNamespacesByName,
) -> NamespaceQueueResult:
    """A name resolves only when exactly one discovered namespace is still
    unclaimed; anything else is reported as ambiguous and left alone.
    `configurations.streams` order is connection-config order while `/streams`
    order is source-discovery order, so pairing them positionally would emit
    confidently-wrong URNs that look correct downstream."""
    unnamed_counts: Dict[str, int] = {}
    claimed: Dict[str, Set[str]] = {}
    for stream in config_streams:
        name = stream.name or ""
        if not name:
            continue
        if stream.namespace:
            claimed.setdefault(name, set()).add(stream.namespace)
        else:
            unnamed_counts[name] = unnamed_counts.get(name, 0) + 1

    queues: StreamNamespacesByName = {}
    ambiguous: StreamNamespacesByName = {}
    for name, namespaces in namespaces_by_name.items():
        needed = unnamed_counts.get(name, 0)
        if not needed:
            continue
        unclaimed = [ns for ns in namespaces if ns not in claimed.get(name, set())]
        if not unclaimed:
            continue
        if len(unclaimed) == 1:
            queues[name] = [unclaimed[0]] * needed
        else:
            ambiguous[name] = unclaimed
    return NamespaceQueueResult(queues=queues, ambiguous=ambiguous)
