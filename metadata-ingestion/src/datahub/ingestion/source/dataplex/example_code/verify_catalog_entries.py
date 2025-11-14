#!/usr/bin/env python3
"""
Verify Catalog Entries for BigQuery Tables

This script checks if catalog entries already exist for BigQuery tables.
Dataplex automatically creates entries for BigQuery tables in the @bigquery entry group.

Requirements:
    pip install google-cloud-dataplex

Environment Variables:
    DATAPLEX_PROJECT_ID: GCP Project ID (required)
    DATAPLEX_LOCATION: GCP Location (required)

Usage:
    python verify_catalog_entries.py
"""

import logging
import os
import sys

from google.api_core import exceptions
from google.cloud import dataplex_v1

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


def verify_entries(project_id: str, location: str):
    """Verify catalog entries exist for BigQuery tables"""

    catalog_client = dataplex_v1.CatalogServiceClient()

    logger.info("=" * 80)
    logger.info("VERIFYING CATALOG ENTRIES")
    logger.info("=" * 80)
    logger.info(f"Project: {project_id}")
    logger.info(f"Location: {location}\n")

    # Check the @bigquery entry group (managed by Dataplex)
    entry_group_name = (
        f"projects/{project_id}/locations/{location}/entryGroups/@bigquery"
    )

    logger.info("Checking entry group: @bigquery")
    try:
        request = dataplex_v1.GetEntryGroupRequest(name=entry_group_name)
        entry_group = catalog_client.get_entry_group(request=request)
        logger.info(f"✓ Entry group exists: {entry_group.name}\n")
    except exceptions.NotFound:
        logger.warning("✗ @bigquery entry group not found")
        logger.warning(
            "  This is unusual - BigQuery tables should auto-create this group\n"
        )
        return
    except Exception as e:
        logger.error(f"✗ Error checking entry group: {e}\n")
        return

    # List all entries in the group
    logger.info("Listing entries in @bigquery group...")
    try:
        entries_request = dataplex_v1.ListEntriesRequest(parent=entry_group_name)
        entries = catalog_client.list_entries(request=entries_request)

        entry_count = 0
        table_entries = []

        for entry in entries:
            entry_count += 1
            entry_id = entry.name.split("/")[-1]

            # Check if it's a BigQuery table
            if entry.entry_source and entry.entry_source.resource:
                resource = entry.entry_source.resource
                if "/tables/" in resource:
                    table_entries.append(
                        {
                            "id": entry_id,
                            "name": entry.name,
                            "fqn": entry.fully_qualified_name,
                            "resource": resource,
                        }
                    )

        logger.info(f"\nFound {entry_count} total entries")
        logger.info(f"Found {len(table_entries)} BigQuery table entries\n")

        if table_entries:
            logger.info("BigQuery Table Entries:")
            logger.info("-" * 80)
            for idx, entry in enumerate(table_entries, 1):
                logger.info(f"\n{idx}. ID: {entry['id']}")
                logger.info(f"   FQN: {entry['fqn']}")
                logger.info(f"   Resource: {entry['resource']}")

        logger.info("\n" + "=" * 80)
        logger.info("VERIFICATION COMPLETE")
        logger.info("=" * 80)

        if table_entries:
            logger.info("\n✓ Catalog entries exist for your BigQuery tables!")
            logger.info("\nTo view lineage in the UI:")
            logger.info("  1. Go to BigQuery Console")
            logger.info(
                "  2. Navigate to your table (e.g., your-project.your-dataset.your-table)"
            )
            logger.info("  3. Click the 'Lineage' tab")
            logger.info("\nOR:")
            logger.info("  1. Go to Dataplex → Catalog")
            logger.info("  2. Search for your table name")
            logger.info("  3. Click on the entry → 'Lineage' tab")
        else:
            logger.info("\n⚠ No BigQuery table entries found")
            logger.info("  Tables may not be automatically synced yet")
            logger.info("  Wait a few minutes and try again")

    except Exception as e:
        logger.error(f"✗ Error listing entries: {e}")


def main():
    """Main function"""
    PROJECT_ID = os.getenv("DATAPLEX_PROJECT_ID")
    LOCATION = os.getenv("DATAPLEX_LOCATION")

    if not PROJECT_ID:
        logger.error("ERROR: DATAPLEX_PROJECT_ID environment variable is required")
        sys.exit(1)

    if not LOCATION:
        logger.error("ERROR: DATAPLEX_LOCATION environment variable is required")
        sys.exit(1)

    verify_entries(PROJECT_ID, LOCATION)


if __name__ == "__main__":
    main()
