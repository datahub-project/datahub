"""Custom properties extraction utilities for Dataplex source."""

from typing import Any, Mapping

from google.cloud import dataplex_v1

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.source.dataplex.dataplex_helpers import serialize_field_value


def extract_aspects_to_custom_properties(
    aspects: Mapping[Any, Any],
    custom_properties: dict[str, str],
    aspect_pattern: AllowDenyPattern,
) -> None:
    """Extract aspects as custom properties, skipping aspect types denied by
    ``aspect_pattern`` (defaults to denying DataHub-authored ``datahub_*`` aspects).

    Args:
        aspects: Dictionary of aspects from entry
        custom_properties: Dictionary to update with aspect properties
        aspect_pattern: Allow/deny pattern matched against aspect type names
    """
    for aspect_key, aspect_value in aspects.items():
        # Dataplex aspect keys arrive as "<project>.<location>.<aspect_type>"
        # (and occasionally as a ".../aspectTypes/<aspect_type>" path); take the
        # final segment either way so the pattern matches the bare aspect type.
        aspect_type = aspect_key.replace("/", ".").split(".")[-1]
        if not aspect_pattern.allowed(aspect_type):
            continue
        custom_properties[f"dataplex_aspect_{aspect_type}"] = aspect_type

        if hasattr(aspect_value, "data") and aspect_value.data:
            for field_key, field_value in aspect_value.data.items():
                property_key = f"dataplex_{aspect_type}_{field_key}"
                custom_properties[property_key] = serialize_field_value(field_value)


def extract_entry_custom_properties(
    entry: dataplex_v1.Entry,
    entry_id: str,
    entry_group_id: str,
    aspect_pattern: AllowDenyPattern,
) -> dict[str, str]:
    """Extract custom properties from a Dataplex entry.

    Args:
        entry: Entry object from Catalog API
        entry_id: Entry ID
        entry_group_id: Entry group ID
        aspect_pattern: Allow/deny pattern matched against aspect type names

    Returns:
        Dictionary of custom properties
    """
    custom_properties = {
        "dataplex_ingested": "true",
        "dataplex_entry_id": entry_id,
        "dataplex_entry_group": entry_group_id,
        "dataplex_fully_qualified_name": entry.fully_qualified_name,
    }

    if entry.entry_type:
        custom_properties["dataplex_entry_type"] = entry.entry_type

    if hasattr(entry, "parent_entry") and entry.parent_entry:
        custom_properties["dataplex_parent_entry"] = entry.parent_entry

    if entry.entry_source:
        if hasattr(entry.entry_source, "resource") and entry.entry_source.resource:
            custom_properties["dataplex_source_resource"] = entry.entry_source.resource
        if hasattr(entry.entry_source, "system") and entry.entry_source.system:
            custom_properties["dataplex_source_system"] = entry.entry_source.system
        if hasattr(entry.entry_source, "platform") and entry.entry_source.platform:
            custom_properties["dataplex_source_platform"] = entry.entry_source.platform

    if entry.aspects:
        extract_aspects_to_custom_properties(
            entry.aspects, custom_properties, aspect_pattern
        )

    return custom_properties
