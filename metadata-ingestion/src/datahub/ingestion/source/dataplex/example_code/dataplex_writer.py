"""
Dataplex Writer Module

Handles formatting and writing extraction data to log files.
"""

import logging
from typing import Any, Dict

logger = logging.getLogger(__name__)

# Constants
SEPARATOR_WIDTH = 80
MAX_RULES_TO_DISPLAY = 5


def write_extraction_log(
    extraction_data: Dict[str, Any], output_file: str = "dataplex_extraction.log"
):
    """Write extraction data to log file with summary at top"""

    try:
        with open(output_file, "w") as f:
            # Write header
            f.write("=" * SEPARATOR_WIDTH + "\n")
            f.write("DATAPLEX DATA EXTRACTION LOG\n")
            f.write("=" * SEPARATOR_WIDTH + "\n\n")

            # Write summary
            f.write("EXTRACTION SUMMARY\n")
            f.write("-" * SEPARATOR_WIDTH + "\n")
            f.write(f"Extraction Time: {extraction_data['extraction_time']}\n")
            f.write(f"Project ID: {extraction_data['project_id']}\n")
            f.write(f"Location: {extraction_data['location']}\n\n")

            f.write("Object Counts:\n")
            f.write(f"  Lakes:                  {len(extraction_data['lakes'])}\n")
            f.write(f"  Zones:                  {len(extraction_data['zones'])}\n")
            f.write(f"  Assets:                 {len(extraction_data['assets'])}\n")
            f.write(f"  Entities:               {len(extraction_data['entities'])}\n")
            f.write(
                f"  Entry Groups:           {len(extraction_data['entry_groups'])}\n"
            )
            f.write(f"  Entries:                {len(extraction_data['entries'])}\n")
            f.write(f"  Data Scans:             {len(extraction_data['data_scans'])}\n")
            f.write(
                f"  Data Quality Results:   {len(extraction_data['data_quality_results'])}\n"
            )
            f.write(
                f"  Data Profile Results:   {len(extraction_data['data_profile_results'])}\n"
            )
            f.write(
                f"  TOTAL:                  {len(extraction_data['lakes']) + len(extraction_data['zones']) + len(extraction_data['assets']) + len(extraction_data['entities']) + len(extraction_data['entry_groups']) + len(extraction_data['entries']) + len(extraction_data['data_scans'])}\n\n"
            )

            # Platform breakdown for entities
            if extraction_data["entities"]:
                platform_counts = {}
                for entity in extraction_data["entities"]:
                    platform = entity["platform"]
                    platform_counts[platform] = platform_counts.get(platform, 0) + 1

                f.write("Entity Platform Breakdown:\n")
                for platform, count in sorted(platform_counts.items()):
                    f.write(f"  {platform}: {count}\n")
                f.write("\n")

            # Aspect statistics
            entities_with_aspects = sum(
                1 for e in extraction_data["entities"] if e.get("aspects")
            )
            entries_with_aspects = sum(
                1 for e in extraction_data["entries"] if e.get("aspects")
            )
            total_entity_aspects = sum(
                len(e.get("aspects", {})) for e in extraction_data["entities"]
            )
            total_entry_aspects = sum(
                len(e.get("aspects", {})) for e in extraction_data["entries"]
            )

            f.write("Aspect Statistics:\n")
            f.write(
                f"  Entities with aspects: {entities_with_aspects}/{len(extraction_data['entities'])}\n"
            )
            f.write(
                f"  Entries with aspects:  {entries_with_aspects}/{len(extraction_data['entries'])}\n"
            )
            f.write(f"  Total entity aspects:  {total_entity_aspects}\n")
            f.write(f"  Total entry aspects:   {total_entry_aspects}\n")

            if total_entity_aspects == 0 and total_entry_aspects == 0:
                f.write(
                    "\n  ⚠️  WARNING: No aspects found on any entities or entries.\n"
                )
                f.write("     This may indicate that:\n")
                f.write(
                    "     - No custom aspects have been attached to your Dataplex resources\n"
                )
                f.write("     - Aspect extraction is not working correctly\n")
                f.write(
                    "     - You may need to attach aspects to test extraction functionality\n"
                )
            f.write("\n")

            # Entity-Entry matching statistics
            if extraction_data["entities"]:
                entities_with_entries = sum(
                    1 for e in extraction_data["entities"] if e.get("catalog_entry")
                )
                f.write("Entity-Entry Association:\n")
                f.write(
                    f"  Entities with catalog entries: {entities_with_entries}/{len(extraction_data['entities'])}\n"
                )
                if entities_with_entries < len(extraction_data["entities"]):
                    f.write(
                        f"  Entities without entries:       {len(extraction_data['entities']) - entities_with_entries}\n"
                    )
                f.write("\n")

            f.write("=" * SEPARATOR_WIDTH + "\n\n")

            # Write detailed data
            f.write("DETAILED EXTRACTION DATA\n")
            f.write("=" * SEPARATOR_WIDTH + "\n\n")

            # Lakes
            if extraction_data["lakes"]:
                _write_lakes_section(f, extraction_data["lakes"])

            # Zones
            if extraction_data["zones"]:
                _write_zones_section(f, extraction_data["zones"])

            # Assets
            if extraction_data["assets"]:
                _write_assets_section(f, extraction_data["assets"])

            # Entities
            if extraction_data["entities"]:
                _write_entities_section(f, extraction_data["entities"])

            # Entry Groups
            if extraction_data["entry_groups"]:
                _write_entry_groups_section(f, extraction_data["entry_groups"])

            # Entries
            if extraction_data["entries"]:
                _write_entries_section(f, extraction_data["entries"])

            # Data Scans
            if extraction_data["data_scans"]:
                _write_data_scans_section(f, extraction_data["data_scans"])

            # Data Quality Results
            if extraction_data["data_quality_results"]:
                _write_quality_results_section(
                    f, extraction_data["data_quality_results"]
                )

            # Data Profile Results
            if extraction_data["data_profile_results"]:
                _write_profile_results_section(
                    f, extraction_data["data_profile_results"]
                )

            f.write("=" * SEPARATOR_WIDTH + "\n")
            f.write("END OF EXTRACTION LOG\n")
            f.write("=" * SEPARATOR_WIDTH + "\n")

        logger.info(f"\nExtraction log written to: {output_file}")
    except (IOError, OSError, PermissionError) as e:
        logger.error(f"\nERROR: Failed to write extraction log to {output_file}: {e}")
        raise


def _write_lakes_section(f, lakes: list) -> None:
    """Write lakes section to file"""
    f.write("LAKES\n")
    f.write("-" * SEPARATOR_WIDTH + "\n")
    for idx, lake in enumerate(lakes, 1):
        f.write(f"\n{idx}. {lake['display_name']}\n")
        f.write(f"   ID: {lake['id']}\n")
        f.write(f"   Name: {lake['name']}\n")
        f.write(f"   State: {lake['state']}\n")
        f.write(f"   Created: {lake['create_time']}\n")
        f.write(f"   Description: {lake['description']}\n")
        f.write(f"   DataHub URN: {lake['datahub_urn']}\n")
    f.write("\n")


def _write_zones_section(f, zones: list) -> None:
    """Write zones section to file"""
    f.write("\nZONES\n")
    f.write("-" * SEPARATOR_WIDTH + "\n")
    for idx, zone in enumerate(zones, 1):
        f.write(f"\n{idx}. {zone['display_name']}\n")
        f.write(f"   ID: {zone['id']}\n")
        f.write(f"   Lake: {zone['lake_id']}\n")
        f.write(f"   Name: {zone['name']}\n")
        f.write(f"   Type: {zone['type']}\n")
        f.write(f"   State: {zone['state']}\n")
        f.write(f"   Created: {zone['create_time']}\n")
        f.write(f"   Description: {zone['description']}\n")
        f.write(f"   DataHub URN: {zone['datahub_urn']}\n")
    f.write("\n")


def _write_assets_section(f, assets: list) -> None:
    """Write assets section to file"""
    f.write("\nASSETS\n")
    f.write("-" * SEPARATOR_WIDTH + "\n")
    for idx, asset in enumerate(assets, 1):
        f.write(f"\n{idx}. {asset['display_name']}\n")
        f.write(f"   ID: {asset['id']}\n")
        f.write(f"   Lake: {asset['lake_id']}\n")
        f.write(f"   Zone: {asset['zone_id']}\n")
        f.write(f"   Name: {asset['name']}\n")
        f.write(f"   State: {asset['state']}\n")
        f.write(f"   Resource Type: {asset['resource_type']}\n")
        f.write(f"   Resource Name: {asset['resource_name']}\n")
        f.write(f"   Created: {asset['create_time']}\n")
        f.write(f"   Description: {asset['description']}\n")
        f.write(f"   DataHub URN: {asset['datahub_urn']}\n")
    f.write("\n")


def _write_schema_fields(f, schema_fields) -> None:
    """Write schema fields to file"""
    if isinstance(schema_fields, list) and schema_fields:
        f.write(f"   Schema Fields ({len(schema_fields)}):\n")
        for field in schema_fields:
            f.write(f"     - {field['name']}: {field['type']} ({field['mode']})")
            if field.get("description"):
                f.write(f" - {field['description']}")
            f.write("\n")
    else:
        field_count = (
            len(schema_fields) if isinstance(schema_fields, list) else schema_fields
        )
        f.write(f"   Schema Fields: {field_count}\n")


def _write_catalog_entry(f, catalog_entry) -> None:
    """Write catalog entry information to file"""
    if not catalog_entry:
        f.write("\n   CATALOG ENTRY: Not found\n")
        return

    f.write("\n   CATALOG ENTRY:\n")
    f.write(f"   ├─ Entry ID: {catalog_entry['entry_id']}\n")
    f.write(f"   ├─ Entry Name: {catalog_entry['entry_name']}\n")
    f.write(f"   ├─ Entry Group: {catalog_entry['entry_group_id']}\n")
    f.write(f"   ├─ Entry Type: {catalog_entry['entry_type']}\n")
    f.write(f"   └─ Created: {catalog_entry['create_time']}\n")

    entry_aspects = catalog_entry.get("entry_aspects", {})
    if entry_aspects:
        f.write(f"   \n   CATALOG ENTRY ASPECTS ({len(entry_aspects)}):\n")
        for aspect_key, aspect_value in entry_aspects.items():
            _write_aspect_details(f, aspect_key, aspect_value)


def _write_aspect_details(f, aspect_key: str, aspect_value: dict) -> None:
    """Write aspect details to file"""
    f.write(f"     - {aspect_key}\n")
    if aspect_value.get("aspect_type"):
        f.write(f"       Type: {aspect_value['aspect_type']}\n")
    if aspect_value.get("path"):
        f.write(f"       Path: {aspect_value['path']}\n")
    if aspect_value.get("data"):
        f.write(f"       Data: {aspect_value['data']}\n")


def _write_entity_aspects(f, aspects: dict) -> None:
    """Write entity aspects to file"""
    if not aspects:
        return

    f.write(f"\n   ENTITY ASPECTS ({len(aspects)}):\n")
    for aspect_key, aspect_value in aspects.items():
        _write_aspect_details(f, aspect_key, aspect_value)


def _write_lineage_info(f, entity: dict) -> None:
    """Write lineage information to file"""
    if "lineage_upstream" in entity and entity["lineage_upstream"]:
        f.write(f"\n   LINEAGE UPSTREAM ({len(entity['lineage_upstream'])}):\n")
        for upstream in entity["lineage_upstream"]:
            f.write(f"     - {upstream}\n")
    elif "lineage_upstream" in entity:
        f.write("\n   LINEAGE UPSTREAM: (none)\n")

    if "lineage_downstream" in entity and entity["lineage_downstream"]:
        f.write(f"   LINEAGE DOWNSTREAM ({len(entity['lineage_downstream'])}):\n")
        for downstream in entity["lineage_downstream"]:
            f.write(f"     - {downstream}\n")
    elif "lineage_downstream" in entity:
        f.write("   LINEAGE DOWNSTREAM: (none)\n")


def _write_entities_section(f, entities: list) -> None:
    """Write entities section to file with associated catalog entry information"""
    f.write("\nENTITIES (DATASETS) WITH CATALOG ENTRIES\n")
    f.write("-" * SEPARATOR_WIDTH + "\n")
    for idx, entity in enumerate(entities, 1):
        f.write(f"\n{idx}. {entity['id']}\n")
        f.write(f"   Lake: {entity['lake_id']}\n")
        f.write(f"   Zone: {entity['zone_id']}\n")
        f.write(f"   Asset: {entity['asset']}\n")
        f.write(f"   Name: {entity['name']}\n")
        f.write(f"   Type: {entity['type']}\n")
        f.write(f"   Platform: {entity['platform']}\n")
        f.write(f"   Data Path: {entity['data_path']}\n")

        # Write schema fields
        _write_schema_fields(f, entity.get("schema_fields", []))

        f.write(f"   Created: {entity['create_time']}\n")
        f.write(f"   DataHub URN: {entity['datahub_urn']}\n")

        # Write catalog entry information
        _write_catalog_entry(f, entity.get("catalog_entry"))

        # Write entity aspect information
        _write_entity_aspects(f, entity.get("aspects", {}))

        # Write lineage information
        _write_lineage_info(f, entity)
    f.write("\n")


def _write_entry_groups_section(f, entry_groups: list) -> None:
    """Write entry groups section to file"""
    f.write("\nENTRY GROUPS\n")
    f.write("-" * SEPARATOR_WIDTH + "\n")
    for idx, group in enumerate(entry_groups, 1):
        f.write(f"\n{idx}. {group['display_name']}\n")
        f.write(f"   ID: {group['id']}\n")
        f.write(f"   Name: {group['name']}\n")
        f.write(f"   Created: {group['create_time']}\n")
        f.write(f"   Description: {group['description']}\n")
        f.write(f"   DataHub URN: {group['datahub_urn']}\n")
    f.write("\n")


def _write_entries_section(f, entries: list) -> None:
    """Write entries section to file"""
    f.write("\nENTRIES\n")
    f.write("-" * SEPARATOR_WIDTH + "\n")
    for idx, entry in enumerate(entries, 1):
        f.write(f"\n{idx}. {entry['id']}\n")
        f.write(f"   Entry Group: {entry['entry_group_id']}\n")
        f.write(f"   Name: {entry['name']}\n")
        f.write(f"   Entry Type: {entry['entry_type']}\n")
        f.write(f"   FQN: {entry['fully_qualified_name']}\n")
        f.write(f"   Created: {entry['create_time']}\n")
        f.write(f"   DataHub URN: {entry['datahub_urn']}\n")

        # Write aspect information if available
        aspects = entry.get("aspects", {})
        if aspects:
            f.write(f"   Aspects ({len(aspects)}):\n")
            for aspect_key, aspect_value in aspects.items():
                f.write(f"     - {aspect_key}\n")
                if aspect_value.get("aspect_type"):
                    f.write(f"       Type: {aspect_value['aspect_type']}\n")
                if aspect_value.get("path"):
                    f.write(f"       Path: {aspect_value['path']}\n")
                if aspect_value.get("data"):
                    f.write(f"       Data: {aspect_value['data']}\n")
    f.write("\n")


def _write_data_scans_section(f, data_scans: list) -> None:
    """Write data scans section to file"""
    f.write("\nDATA SCANS (QUALITY & PROFILING)\n")
    f.write("-" * SEPARATOR_WIDTH + "\n")
    for idx, scan in enumerate(data_scans, 1):
        f.write(f"\n{idx}. {scan['display_name']} ({scan['type']})\n")
        f.write(f"   ID: {scan['id']}\n")
        f.write(f"   Name: {scan['name']}\n")
        f.write(f"   State: {scan['state']}\n")
        f.write(f"   Type: {scan['type']}\n")
        f.write(f"   Data Resource: {scan['data_resource']}\n")
        if scan.get("data_entity"):
            f.write(f"   Data Entity: {scan['data_entity']}\n")
        f.write(f"   Created: {scan['create_time']}\n")
        f.write(f"   Description: {scan['description']}\n")

        if scan["type"] == "Data Quality" and "rules" in scan:
            f.write(f"   Quality Rules: {len(scan['rules'])}\n")
            if scan["rules"]:
                for rule_idx, rule in enumerate(
                    scan["rules"][:MAX_RULES_TO_DISPLAY], 1
                ):
                    f.write(
                        f"     {rule_idx}. Type: {rule['type']}, Dimension: {rule['dimension']}, Threshold: {rule['threshold']}\n"
                    )
                if len(scan["rules"]) > MAX_RULES_TO_DISPLAY:
                    f.write(
                        f"     ... and {len(scan['rules']) - MAX_RULES_TO_DISPLAY} more rules\n"
                    )

        if "sampling_percent" in scan:
            f.write(f"   Sampling: {scan['sampling_percent']}%\n")

        f.write(f"   DataHub URN: {scan['datahub_urn']}\n")
    f.write("\n")


def _write_quality_results_section(f, quality_results: list) -> None:
    """Write data quality results section to file"""
    f.write("\nDATA QUALITY RESULTS (LATEST)\n")
    f.write("-" * SEPARATOR_WIDTH + "\n")
    for idx, result in enumerate(quality_results, 1):
        f.write(f"\n{idx}. Scan: {result['scan_id']} - Job: {result['job_id']}\n")
        f.write(f"   Scan Name: {result['scan_name']}\n")
        f.write(f"   Job State: {result['state']}\n")
        f.write(f"   Passed: {result['passed']}\n")
        if result["score"] is not None:
            f.write(f"   Score: {result['score']:.2f}\n")
        f.write(f"   Rules Evaluated: {result['rules_evaluated']}\n")
        f.write(
            f"   Rules Passed: {result['rules_passed']}/{result['rules_evaluated']}\n"
        )
        f.write(f"   Dimensions Evaluated: {result['dimensions_count']}\n")
        f.write(f"   Start Time: {result['start_time']}\n")
        f.write(f"   End Time: {result['end_time']}\n")
    f.write("\n")


def _write_profile_results_section(f, profile_results: list) -> None:
    """Write data profile results section to file"""
    f.write("\nDATA PROFILE RESULTS (LATEST)\n")
    f.write("-" * SEPARATOR_WIDTH + "\n")
    for idx, result in enumerate(profile_results, 1):
        f.write(f"\n{idx}. Scan: {result['scan_id']} - Job: {result['job_id']}\n")
        f.write(f"   Scan Name: {result['scan_name']}\n")
        f.write(f"   Job State: {result['state']}\n")
        f.write(f"   Row Count: {result['row_count']}\n")
        f.write(f"   Fields Profiled: {result['fields_profiled']}\n")
        f.write(f"   Start Time: {result['start_time']}\n")
        f.write(f"   End Time: {result['end_time']}\n")
    f.write("\n")
