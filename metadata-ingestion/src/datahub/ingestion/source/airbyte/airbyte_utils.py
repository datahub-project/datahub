import logging
from typing import Any, Dict, List, Tuple

from datahub.configuration.common import AllowDenyPattern

logger = logging.getLogger(__name__)


def clean_uri(uri: str) -> str:
    return uri.rstrip("/")


def apply_pattern(
    items: List[Dict[str, Any]], pattern: AllowDenyPattern, name_key: str = "name"
) -> List[Dict[str, Any]]:
    if not items:
        return []
    if pattern.allow_all():
        return items
    return [
        item
        for item in items
        if (name := item.get(name_key, "")) and pattern.allowed(name)
    ]


def coerce_str(value: object) -> str:
    if isinstance(value, str):
        return value
    return str(value) if value is not None else ""


def stream_namespace_from_api(stream: Dict[str, Any]) -> str:
    namespace = (
        stream.get("namespace")
        or stream.get("streamnamespace")
        or stream.get("streamNamespace")
        or ""
    )
    return namespace if isinstance(namespace, str) else ""


def namespace_queues_for_catalog(
    config_streams: List[Dict[str, Any]],
    namespaces_by_name: Dict[str, List[str]],
) -> Tuple[Dict[str, List[str]], Dict[str, List[str]]]:
    # Unnamed count vs full /streams list; does not subtract explicit siblings.
    unnamed_counts: Dict[str, int] = {}
    for stream in config_streams:
        name = coerce_str(stream.get("name"))
        if not name:
            continue
        namespace = coerce_str(stream.get("namespace"))
        if not namespace:
            unnamed_counts[name] = unnamed_counts.get(name, 0) + 1

    queues: Dict[str, List[str]] = {}
    ambiguous: Dict[str, List[str]] = {}
    for name, namespaces in namespaces_by_name.items():
        needed = unnamed_counts.get(name, 0)
        if not needed or not namespaces:
            continue
        if len(namespaces) == 1:
            queues[name] = [namespaces[0]] * needed
        elif needed == len(namespaces):
            queues[name] = list(namespaces)
        else:
            ambiguous[name] = list(namespaces)
            logger.warning(
                "Skipping namespace backfill for stream %r: %s unnamed config "
                "stream(s) vs %s /streams namespace(s) %s",
                name,
                needed,
                len(namespaces),
                namespaces,
            )
    return queues, ambiguous
