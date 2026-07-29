from typing import Any, Dict, List, Sequence

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
    unnamed_counts: Dict[str, int] = {}
    for stream in config_streams:
        name = stream.name or ""
        if not name:
            continue
        if not stream.namespace:
            unnamed_counts[name] = unnamed_counts.get(name, 0) + 1

    queues: StreamNamespacesByName = {}
    ambiguous: StreamNamespacesByName = {}
    positional: StreamNamespacesByName = {}
    for name, namespaces in namespaces_by_name.items():
        needed = unnamed_counts.get(name, 0)
        if not needed or not namespaces:
            continue
        if len(namespaces) == 1:
            queues[name] = [namespaces[0]] * needed
        elif needed == len(namespaces):
            queues[name] = list(namespaces)
            positional[name] = list(namespaces)
        else:
            ambiguous[name] = list(namespaces)
    return NamespaceQueueResult(
        queues=queues, ambiguous=ambiguous, positional=positional
    )
