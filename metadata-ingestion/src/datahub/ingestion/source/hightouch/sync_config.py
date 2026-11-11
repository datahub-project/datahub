from typing import Any, Dict, List, Optional

from datahub.ingestion.source.hightouch.config import HightouchSourceReport
from datahub.ingestion.source.hightouch.models import (
    HightouchFieldMapping,
    HightouchSync,
)

# Keys that may name the destination object or table, in priority order. The
# configuration schema depends on the destination type.
DESTINATION_OBJECT_KEYS = ["object", "tableName", "table", "destinationTable"]


def destination_object(sync: HightouchSync) -> Optional[str]:
    config = sync.configuration
    if config.get("type") == "event":
        return _name_of(config.get("eventName"))
    for key in DESTINATION_OBJECT_KEYS:
        name = _name_of(config.get(key))
        if name:
            return name
    return None


def _name_of(value: Any) -> Optional[str]:
    if isinstance(value, str):
        return value or None
    if isinstance(value, dict):
        name = value.get("from") or value.get("name")
        return name if isinstance(name, str) and name else None
    return None


def field_mappings(
    sync: HightouchSync, report: HightouchSourceReport
) -> List[HightouchFieldMapping]:
    """Model column -> destination field pairs. Only standard mappings copy a model
    column; static values and templates have no source column."""
    config = sync.configuration
    raw = _listed_mappings(sync, config, report)
    # The field Hightouch matches records on is mapped separately from the other
    # columns, as a single {from, to} object.
    external_id = config.get("externalIdMapping")
    if isinstance(external_id, dict):
        raw.append(
            {
                "source": external_id.get("from"),
                "dest": external_id.get("to"),
                "type": external_id.get("type"),
            }
        )

    mappings = []
    for item in raw:
        if item.get("type") not in (None, "standard"):
            continue
        source, dest = item.get("source"), item.get("dest")
        if not isinstance(source, str) or not isinstance(dest, str):
            _report_dropped(sync, report, f"unusable mapping {source!r} -> {dest!r}")
            continue
        mappings.append(
            HightouchFieldMapping(source_field=source, destination_field=dest)
        )
    return mappings


def _listed_mappings(
    sync: HightouchSync, config: Dict[str, Any], report: HightouchSourceReport
) -> List[Dict[str, Any]]:
    # Hightouch encodes column mappings as a `mappings` list of {from, to}, a
    # `fieldMappings` list of {sourceField, destinationField}, or a `columnMappings`
    # dict of destination field -> source field, depending on the destination.
    for key, source_key, dest_key in (
        ("mappings", "from", "to"),
        ("fieldMappings", "sourceField", "destinationField"),
    ):
        listed = config.get(key)
        if listed is None:
            continue
        if not isinstance(listed, list):
            _report_dropped(sync, report, f"'{key}' is a {type(listed).__name__}")
            return []
        normalized = []
        for item in listed:
            if not isinstance(item, dict):
                _report_dropped(sync, report, f"'{key}' item is not an object")
                continue
            normalized.append(
                {
                    "source": item.get(source_key, item.get("from")),
                    "dest": item.get(dest_key, item.get("to")),
                    "type": item.get("type"),
                }
            )
        return normalized

    column_mappings = config.get("columnMappings")
    if column_mappings is None:
        return []
    if not isinstance(column_mappings, dict):
        _report_dropped(
            sync, report, f"'columnMappings' is a {type(column_mappings).__name__}"
        )
        return []
    return [{"source": src, "dest": dest} for dest, src in column_mappings.items()]


def _report_dropped(
    sync: HightouchSync, report: HightouchSourceReport, detail: str
) -> None:
    report.field_mappings_dropped += 1
    report.warning(
        title="Dropped sync field mappings",
        message="Some field mappings could not be interpreted and were skipped, so "
        "column-level lineage may be incomplete.",
        context=f"sync_slug: {sync.slug}, {detail}",
    )
