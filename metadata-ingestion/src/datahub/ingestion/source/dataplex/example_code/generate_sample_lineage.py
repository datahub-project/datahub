#!/usr/bin/env python3
"""
Generate Sample Lineage Data for Dataplex Entities

This script creates sample lineage relationships between entities discovered in Dataplex.
It uses the Google Cloud Data Lineage API to write lineage events that create links
between datasets.

IMPORTANT: This is a helper script for testing purposes. The lineage relationships
created are examples and may not reflect actual data transformations.

Requirements:
    pip install google-cloud-datacatalog-lineage

Environment Variables:
    DATAPLEX_PROJECT_ID: GCP Project ID (required)
    DATAPLEX_LOCATION: GCP Location (required)

Usage:
    python generate_sample_lineage.py
"""

import logging
import os
import sys
from datetime import datetime

from google.cloud.datacatalog_lineage_v1 import (
    CreateLineageEventRequest,
    CreateProcessRequest,
    CreateRunRequest,
    EntityReference,
    EventLink,
    LineageClient,
    LineageEvent,
    Process,
    Run,
)
from google.protobuf.timestamp_pb2 import Timestamp

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


class LineageGenerator:
    """Generates sample lineage data for Dataplex entities"""

    def __init__(self, project_id: str, location: str):
        """Initialize the lineage generator"""
        self.project_id = project_id
        self.location = location
        self.lineage_client = LineageClient()
        self.parent = f"projects/{project_id}/locations/{location}"

    def create_process(self, process_id: str, display_name: str) -> str:
        """
        Create a process (represents a data transformation job)

        Args:
            process_id: Unique identifier for the process
            display_name: Human-readable name

        Returns:
            The full resource name of the created process
        """
        process_name = f"{self.parent}/processes/{process_id}"

        try:
            process = Process(
                name=process_name,
                display_name=display_name,
                attributes={
                    "job_type": "sample_transformation",
                    "description": "Sample lineage for testing",
                },
            )

            request = CreateProcessRequest(parent=self.parent, process=process)

            result = self.lineage_client.create_process(request=request)
            logger.info(f"✓ Created process: {display_name}")
            return result.name

        except Exception:
            # Process might already exist, return the name to use it
            return process_name

    def create_run(
        self, process_name: str, run_id: str, event_time: datetime = None
    ) -> str:
        """
        Create a run for a process

        Args:
            process_name: Full resource name of the process
            run_id: Unique identifier for this run
            event_time: When the run started (defaults to now)

        Returns:
            The full resource name of the created run
        """
        if event_time is None:
            event_time = datetime.now()

        run_name = f"{process_name}/runs/{run_id}"

        try:
            # Create timestamp
            timestamp = Timestamp()
            timestamp.FromDatetime(event_time)

            run = Run(
                name=run_name,
                start_time=timestamp,
                attributes={"run_type": "sample_run"},
            )

            request = CreateRunRequest(parent=process_name, run=run)

            result = self.lineage_client.create_run(request=request)
            return result.name

        except Exception:
            # Run might already exist, return the name to use it
            return run_name

    def create_lineage_event(
        self,
        process_name: str,
        run_id: str,
        source_fqn: str,
        target_fqn: str,
        event_time: datetime = None,
    ):
        """
        Create a lineage event linking source to target

        Args:
            process_name: Full resource name of the process
            run_id: Unique identifier for this run
            source_fqn: Fully qualified name of source entity (e.g., "bigquery:project.dataset.table")
            target_fqn: Fully qualified name of target entity
            event_time: When the event occurred (defaults to now)
        """
        if event_time is None:
            event_time = datetime.now()

        # First, ensure the run exists
        self.create_run(process_name, run_id, event_time)

        # Create EntityReferences
        source = EntityReference(fully_qualified_name=source_fqn)
        target = EntityReference(fully_qualified_name=target_fqn)

        # Create event links
        links = [EventLink(source=source, target=target)]

        # Create timestamp
        timestamp = Timestamp()
        timestamp.FromDatetime(event_time)

        # Create the lineage event
        lineage_event = LineageEvent(start_time=timestamp, links=links)

        request = CreateLineageEventRequest(
            parent=f"{process_name}/runs/{run_id}", lineage_event=lineage_event
        )

        try:
            self.lineage_client.create_lineage_event(request=request)
            logger.info(f"  ✓ Created lineage: {source_fqn} → {target_fqn}")
        except Exception as e:
            logger.error(f"  ✗ Failed to create lineage: {e}")

    def generate_sample_lineage_for_adoption_dataset(self):
        """
        Generate sample lineage for the adoption dataset entities

        Creates these relationships:
        - humans → human_profiles (transformation)
        - pets → pet_profiles (transformation)
        - human_profiles + pet_profiles → adoptions (join/aggregation)
        """
        logger.info("=" * 80)
        logger.info("GENERATING SAMPLE LINEAGE DATA")
        logger.info("=" * 80)

        # Entity fully qualified names from the extraction log
        # IMPORTANT: Must match the format that dataplex_client.py uses for searching
        # Format: bigquery:projects/PROJECT/datasets/DATASET/tables/TABLE
        # This matches entity.data_path from Dataplex
        # TODO: Replace with your actual project ID, dataset, and table names
        entities = {
            "humans": f"bigquery:projects/{self.project_id}/datasets/sample_dataset/tables/humans",
            "pets": f"bigquery:projects/{self.project_id}/datasets/sample_dataset/tables/pets",
            "human_profiles": f"bigquery:projects/{self.project_id}/datasets/sample_dataset/tables/human_profiles",
            "pet_profiles": f"bigquery:projects/{self.project_id}/datasets/sample_dataset/tables/pet_profiles",
            "adoptions": f"bigquery:projects/{self.project_id}/datasets/sample_dataset/tables/adoptions",
        }

        logger.info("\nUsing FQN format: bigquery:projects/.../datasets/.../tables/...")

        # Create processes for different transformation types
        logger.info("\nCreating processes...")
        process_human_profile = self.create_process(
            "generate_human_profiles", "Generate Human Profiles"
        )
        process_pet_profile = self.create_process(
            "generate_pet_profiles", "Generate Pet Profiles"
        )
        process_adoptions = self.create_process(
            "create_adoptions_report", "Create Adoptions Report"
        )

        # Generate lineage events with timestamps
        base_time = datetime.now()
        run_suffix = datetime.now().strftime("%Y%m%d_%H%M%S")

        logger.info("\nGenerating lineage events...")

        # Lineage 1: humans → human_profiles
        logger.info("\n1. Upstream: humans → Downstream: human_profiles")
        self.create_lineage_event(
            process_human_profile,
            f"run_{run_suffix}_1",
            entities["humans"],
            entities["human_profiles"],
            base_time,
        )

        # Lineage 2: pets → pet_profiles
        logger.info("\n2. Upstream: pets → Downstream: pet_profiles")
        self.create_lineage_event(
            process_pet_profile,
            f"run_{run_suffix}_2",
            entities["pets"],
            entities["pet_profiles"],
            base_time,
        )

        # Lineage 3: human_profiles → adoptions
        logger.info("\n3. Upstream: human_profiles → Downstream: adoptions")
        self.create_lineage_event(
            process_adoptions,
            f"run_{run_suffix}_3a",
            entities["human_profiles"],
            entities["adoptions"],
            base_time,
        )

        # Lineage 4: pet_profiles → adoptions
        logger.info("\n4. Upstream: pet_profiles → Downstream: adoptions")
        self.create_lineage_event(
            process_adoptions,
            f"run_{run_suffix}_3b",
            entities["pet_profiles"],
            entities["adoptions"],
            base_time,
        )

        logger.info("\n" + "=" * 80)
        logger.info("LINEAGE GENERATION COMPLETE")
        logger.info("=" * 80)
        logger.info("\nCreated lineage graph:")
        logger.info("  humans ────────────┐")
        logger.info("                     ↓")
        logger.info("              human_profiles ─┐")
        logger.info("                              ↓")
        logger.info("                         adoptions")
        logger.info("                              ↑")
        logger.info("               pet_profiles ──┘")
        logger.info("                     ↑")
        logger.info("  pets ─────────────┘")
        logger.info(
            "\nYou can now run the dataplex_explorer.py script to see the lineage!"
        )


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

    # Create generator and generate lineage
    generator = LineageGenerator(PROJECT_ID, LOCATION)
    generator.generate_sample_lineage_for_adoption_dataset()


if __name__ == "__main__":
    main()
