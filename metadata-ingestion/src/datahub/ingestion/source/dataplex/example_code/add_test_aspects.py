#!/usr/bin/env python3
"""
Add Test Aspects to Dataplex Entities

This script adds simple test aspects to Dataplex entities to verify aspect extraction.
Creates a custom aspect type and attaches test aspects to each entity.

Requirements:
    pip install google-cloud-dataplex

Environment Variables:
    DATAPLEX_PROJECT_ID: GCP Project ID (required)
    DATAPLEX_LOCATION: GCP Location (required)

Usage:
    python add_test_aspects.py
"""

import logging
import os
import sys

from google.api_core import exceptions
from google.cloud import dataplex_v1
from google.protobuf import struct_pb2

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


class AspectAdder:
    """Adds test aspects to Dataplex entities"""

    def __init__(self, project_id: str, location: str):
        """Initialize the aspect adder"""
        self.project_id = project_id
        self.location = location
        self.dataplex_client = dataplex_v1.DataplexServiceClient()
        self.metadata_client = dataplex_v1.MetadataServiceClient()
        self.catalog_client = dataplex_v1.CatalogServiceClient()

    def ensure_aspect_type(self, aspect_type_id: str = "test-metadata") -> str:
        """
        Ensure the aspect type exists, create if it doesn't

        Args:
            aspect_type_id: ID of the aspect type

        Returns:
            Full resource name of the aspect type
        """
        parent = f"projects/{self.project_id}/locations/{self.location}"
        aspect_type_name = f"{parent}/aspectTypes/{aspect_type_id}"

        try:
            # Try to get the existing aspect type
            request = dataplex_v1.GetAspectTypeRequest(name=aspect_type_name)
            self.catalog_client.get_aspect_type(request=request)
            logger.info(f"✓ Aspect type already exists: {aspect_type_id}")
            return aspect_type_name
        except exceptions.NotFound:
            # Aspect type doesn't exist, create it
            logger.info(f"Creating aspect type: {aspect_type_id}")
            try:
                # Define the aspect type schema
                aspect_type = dataplex_v1.AspectType(
                    description="Test metadata for validating aspect extraction",
                    metadata_template=dataplex_v1.AspectType.MetadataTemplate(
                        name="test_metadata_template",
                        type_="record",
                        record_fields=[
                            dataplex_v1.AspectType.MetadataTemplate(
                                name="data_owner",
                                type_="string",
                                index=1,
                                constraints=dataplex_v1.AspectType.MetadataTemplate.Constraints(
                                    required=False
                                ),
                                annotations=dataplex_v1.AspectType.MetadataTemplate.Annotations(
                                    description="Owner of the data"
                                ),
                            ),
                            dataplex_v1.AspectType.MetadataTemplate(
                                name="data_classification",
                                type_="string",
                                index=2,
                                constraints=dataplex_v1.AspectType.MetadataTemplate.Constraints(
                                    required=False
                                ),
                                annotations=dataplex_v1.AspectType.MetadataTemplate.Annotations(
                                    description="Data classification level"
                                ),
                            ),
                            dataplex_v1.AspectType.MetadataTemplate(
                                name="retention_days",
                                type_="int",
                                index=3,
                                constraints=dataplex_v1.AspectType.MetadataTemplate.Constraints(
                                    required=False
                                ),
                                annotations=dataplex_v1.AspectType.MetadataTemplate.Annotations(
                                    description="Data retention period in days"
                                ),
                            ),
                        ],
                    ),
                )

                request = dataplex_v1.CreateAspectTypeRequest(
                    parent=parent,
                    aspect_type=aspect_type,
                    aspect_type_id=aspect_type_id,
                )
                operation = self.catalog_client.create_aspect_type(request=request)
                # Wait for the operation to complete
                result = operation.result()
                logger.info(f"✓ Created aspect type: {aspect_type_id}")
                return result.name
            except Exception as e:
                logger.error(f"✗ Failed to create aspect type: {e}")
                raise

    def add_aspect_to_entry(
        self, entry_name: str, aspect_type_name: str, entry_id: str
    ):
        """
        Add a test aspect to a catalog entry

        Args:
            entry_name: Full resource name of the entry
            aspect_type_name: Full resource name of the aspect type
            entry_id: Entry ID for display purposes
        """
        try:
            # Create aspect data
            aspect_data = struct_pb2.Struct()
            aspect_data["data_owner"] = (
                f"team-{entry_id.split('_')[0] if '_' in entry_id else 'data'}"
            )
            aspect_data["data_classification"] = "internal"
            aspect_data["retention_days"] = 90

            # Create the aspect
            aspect = dataplex_v1.Aspect(aspect_type=aspect_type_name, data=aspect_data)

            # Generate aspect key in the format: project.location.aspectType
            aspect_key = f"{self.project_id}.{self.location}.test-metadata"

            # Update the entry with the aspect
            request = dataplex_v1.UpdateEntryRequest(
                entry=dataplex_v1.Entry(name=entry_name, aspects={aspect_key: aspect}),
                update_mask={"paths": ["aspects"]},
            )

            self.catalog_client.update_entry(request=request)
            logger.info(f"  ✓ Added aspect to entry: {entry_id}")

        except Exception as e:
            logger.error(f"  ✗ Failed to add aspect to {entry_id}: {e}")

    def add_aspects_to_all_entries(self):
        """
        Discover all catalog entries and add test aspects to them
        """
        logger.info("=" * 80)
        logger.info("ADDING TEST ASPECTS TO DATAPLEX CATALOG ENTRIES")
        logger.info("=" * 80)

        # Ensure aspect type exists
        aspect_type_name = self.ensure_aspect_type("test-metadata")

        # Discover entry groups from Dataplex Catalog
        parent = f"projects/{self.project_id}/locations/{self.location}"

        logger.info("\nDiscovering entry groups...")
        entry_groups_request = dataplex_v1.ListEntryGroupsRequest(parent=parent)
        entry_groups = self.catalog_client.list_entry_groups(
            request=entry_groups_request
        )

        entry_count = 0

        for entry_group in entry_groups:
            entry_group_id = entry_group.name.split("/")[-1]
            logger.info(f"\nProcessing entry group: {entry_group_id}")

            # Get entries in this entry group
            entries_parent = f"projects/{self.project_id}/locations/{self.location}/entryGroups/{entry_group_id}"
            entries_request = dataplex_v1.ListEntriesRequest(parent=entries_parent)
            entries = self.catalog_client.list_entries(request=entries_request)

            for entry in entries:
                entry_id = entry.name.split("/")[-1]
                self.add_aspect_to_entry(entry.name, aspect_type_name, entry_id)
                entry_count += 1

        logger.info("\n" + "=" * 80)
        logger.info("ASPECT ADDITION COMPLETE")
        logger.info("=" * 80)
        logger.info(f"Added test aspects to {entry_count} catalog entries")
        logger.info("\nYou can now run the extraction to verify aspect extraction:")
        logger.info("  python dataplex_explorer.py")


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

    # Add aspects to catalog entries
    adder = AspectAdder(PROJECT_ID, LOCATION)
    adder.add_aspects_to_all_entries()


if __name__ == "__main__":
    main()
