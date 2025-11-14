#!/usr/bin/env python3
"""
Create Catalog Entries for BigQuery Tables

This script creates catalog entries for BigQuery tables discovered in Dataplex.
Uses a custom entry type to avoid permission issues with Google-managed types.

Requirements:
    pip install google-cloud-dataplex

Environment Variables:
    DATAPLEX_PROJECT_ID: GCP Project ID (required)
    DATAPLEX_LOCATION: GCP Location (required)

Usage:
    python create_table_entries.py
"""

import logging
import os
import sys

from google.api_core import exceptions
from google.cloud import dataplex_v1

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


class CatalogEntryCreator:
    """Creates catalog entries for Dataplex entities"""

    def __init__(self, project_id: str, location: str):
        """Initialize the catalog entry creator"""
        self.project_id = project_id
        self.location = location
        self.dataplex_client = dataplex_v1.DataplexServiceClient()
        self.metadata_client = dataplex_v1.MetadataServiceClient()
        self.catalog_client = dataplex_v1.CatalogServiceClient()

    def ensure_entry_type(self, entry_type_id: str = "bigquery-table-custom") -> str:
        """
        Ensure the entry type exists, create if it doesn't

        Args:
            entry_type_id: ID of the entry type

        Returns:
            Full resource name of the entry type
        """
        parent = f"projects/{self.project_id}/locations/{self.location}"
        entry_type_name = f"{parent}/entryTypes/{entry_type_id}"

        try:
            # Try to get the existing entry type
            request = dataplex_v1.GetEntryTypeRequest(name=entry_type_name)
            self.catalog_client.get_entry_type(request=request)
            logger.info(f"✓ Entry type already exists: {entry_type_id}")
            return entry_type_name
        except exceptions.NotFound:
            # Entry type doesn't exist, create it
            logger.info(f"Creating entry type: {entry_type_id}")
            try:
                # Define the entry type
                entry_type = dataplex_v1.EntryType(
                    description="Custom entry type for BigQuery tables",
                    type_aliases=["TABLE"],
                    platform="BigQuery",
                    system="bigquery",
                )

                request = dataplex_v1.CreateEntryTypeRequest(
                    parent=parent, entry_type=entry_type, entry_type_id=entry_type_id
                )
                operation = self.catalog_client.create_entry_type(request=request)
                # Wait for the operation to complete
                result = operation.result()
                logger.info(f"✓ Created entry type: {entry_type_id}")
                return result.name
            except Exception as e:
                logger.error(f"✗ Failed to create entry type: {e}")
                raise

    def ensure_entry_group(self, entry_group_id: str = "bigquery-tables") -> str:
        """
        Ensure the entry group exists, create if it doesn't

        Args:
            entry_group_id: ID of the entry group

        Returns:
            Full resource name of the entry group
        """
        parent = f"projects/{self.project_id}/locations/{self.location}"
        entry_group_name = f"{parent}/entryGroups/{entry_group_id}"

        try:
            # Try to get the existing entry group
            request = dataplex_v1.GetEntryGroupRequest(name=entry_group_name)
            self.catalog_client.get_entry_group(request=request)
            logger.info(f"✓ Entry group already exists: {entry_group_id}")
            return entry_group_name
        except exceptions.NotFound:
            # Entry group doesn't exist, create it
            logger.info(f"Creating entry group: {entry_group_id}")
            try:
                entry_group = dataplex_v1.EntryGroup(
                    description="BigQuery tables from Dataplex for aspect testing"
                )
                request = dataplex_v1.CreateEntryGroupRequest(
                    parent=parent,
                    entry_group=entry_group,
                    entry_group_id=entry_group_id,
                )
                operation = self.catalog_client.create_entry_group(request=request)
                # Wait for the operation to complete
                result = operation.result()
                logger.info(f"✓ Created entry group: {entry_group_id}")
                return result.name
            except Exception as e:
                logger.error(f"✗ Failed to create entry group: {e}")
                raise

    def create_bigquery_table_entry(
        self,
        entry_group_name: str,
        entry_type_name: str,
        table_id: str,
        fully_qualified_name: str,
        project: str,
        dataset: str,
        table: str,
    ):
        """
        Create a catalog entry for a BigQuery table

        Args:
            entry_group_name: Full resource name of the entry group
            entry_type_name: Full resource name of the entry type
            table_id: Unique ID for the entry
            fully_qualified_name: FQN for lineage matching
            project: BigQuery project ID
            dataset: BigQuery dataset name
            table: BigQuery table name
        """
        # Create entry ID using dataset and table name
        entry_id = f"{dataset}-{table}"
        entry_name = f"{entry_group_name}/entries/{entry_id}"

        try:
            # Check if entry already exists
            try:
                request = dataplex_v1.GetEntryRequest(name=entry_name)
                self.catalog_client.get_entry(request=request)
                logger.info(f"  ✓ Entry already exists: {table}")
                return
            except exceptions.NotFound:
                pass  # Entry doesn't exist, create it

            # Create the entry
            entry = dataplex_v1.Entry(
                name=entry_name,
                entry_type=entry_type_name,
                fully_qualified_name=fully_qualified_name,
                entry_source=dataplex_v1.EntrySource(
                    resource=f"//bigquery.googleapis.com/projects/{project}/datasets/{dataset}/tables/{table}",
                    system="BIGQUERY",
                ),
            )

            request = dataplex_v1.CreateEntryRequest(
                parent=entry_group_name, entry=entry, entry_id=entry_id
            )

            self.catalog_client.create_entry(request=request)
            logger.info(f"  ✓ Created entry: {table}")

        except Exception as e:
            logger.error(f"  ✗ Failed to create entry for {table}: {e}")

    def create_entries_for_dataplex_entities(self):
        """
        Discover entities from Dataplex and create catalog entries for them
        """
        logger.info("=" * 80)
        logger.info("CREATING CATALOG ENTRIES FOR BIGQUERY TABLES")
        logger.info("=" * 80)

        # Ensure entry type exists
        entry_type_name = self.ensure_entry_type("bigquery-table-custom")

        # Ensure entry group exists
        entry_group_name = self.ensure_entry_group("bigquery-tables")

        # Discover entities from Dataplex
        parent = f"projects/{self.project_id}/locations/{self.location}"

        logger.info("\nDiscovering Dataplex lakes...")
        lakes_request = dataplex_v1.ListLakesRequest(parent=parent)
        lakes = self.dataplex_client.list_lakes(request=lakes_request)

        entity_count = 0

        for lake in lakes:
            lake_id = lake.name.split("/")[-1]
            logger.info(f"\nProcessing lake: {lake_id}")

            # Get zones
            zones_parent = (
                f"projects/{self.project_id}/locations/{self.location}/lakes/{lake_id}"
            )
            zones_request = dataplex_v1.ListZonesRequest(parent=zones_parent)
            zones = self.dataplex_client.list_zones(request=zones_request)

            for zone in zones:
                zone_id = zone.name.split("/")[-1]
                logger.info(f"  Processing zone: {zone_id}")

                # Get entities
                entities_parent = f"projects/{self.project_id}/locations/{self.location}/lakes/{lake_id}/zones/{zone_id}"
                entities_request = dataplex_v1.ListEntitiesRequest(
                    parent=entities_parent
                )
                entities = self.metadata_client.list_entities(request=entities_request)

                for entity in entities:
                    # Only process BigQuery tables
                    if entity.type_.name != "TABLE":
                        continue

                    # Check if it's a BigQuery table by checking the asset
                    if entity.asset:
                        try:
                            asset_id = entity.asset
                            asset_name = f"projects/{self.project_id}/locations/{self.location}/lakes/{lake_id}/zones/{zone_id}/assets/{asset_id}"
                            asset_request = dataplex_v1.GetAssetRequest(name=asset_name)
                            asset = self.dataplex_client.get_asset(
                                request=asset_request
                            )

                            if (
                                asset.resource_spec
                                and asset.resource_spec.type_.name == "BIGQUERY_DATASET"
                            ):
                                # Parse the data path: projects/PROJECT/datasets/DATASET/tables/TABLE
                                if entity.data_path:
                                    parts = entity.data_path.split("/")
                                    if (
                                        len(parts) >= 6
                                        and parts[0] == "projects"
                                        and parts[2] == "datasets"
                                        and parts[4] == "tables"
                                    ):
                                        project = parts[1]
                                        dataset = parts[3]
                                        table = parts[5]

                                        # Create FQN in the format needed for lineage
                                        fqn = f"bigquery:{project}.{dataset}.{table}"

                                        logger.info(f"    Creating entry for: {table}")
                                        self.create_bigquery_table_entry(
                                            entry_group_name,
                                            entry_type_name,
                                            entity.id,
                                            fqn,
                                            project,
                                            dataset,
                                            table,
                                        )
                                        entity_count += 1

                        except Exception as e:
                            logger.warning(
                                f"    Could not process entity {entity.id}: {e}"
                            )

        logger.info("\n" + "=" * 80)
        logger.info("CATALOG ENTRY CREATION COMPLETE")
        logger.info("=" * 80)
        logger.info(f"Created/verified {entity_count} catalog entries")
        logger.info("\nNext steps:")
        logger.info("  1. Run: python add_test_aspects.py")
        logger.info("     (This will add test aspects to the new catalog entries)")
        logger.info("  2. Run: python dataplex_explorer.py")
        logger.info("     (This will extract and verify aspect data)")


def main():
    """Main function"""
    # Get configuration from environment variables
    PROJECT_ID = os.getenv("DATAPLEX_PROJECT_ID")
    LOCATION = os.getenv("DATAPLEX_LOCATION")

    if not PROJECT_ID:
        logger.error("ERROR: DATAPLEX_PROJECT_ID environment variable is required")
        sys.exit(1)

    if not LOCATION:
        logger.error("ERROR: DATAPLEX_LOCATION environment variable is required")
        sys.exit(1)

    logger.info(f"Project ID: {PROJECT_ID}")
    logger.info(f"Location: {LOCATION}\n")

    # Create catalog entries
    creator = CatalogEntryCreator(PROJECT_ID, LOCATION)
    creator.create_entries_for_dataplex_entities()


if __name__ == "__main__":
    main()
