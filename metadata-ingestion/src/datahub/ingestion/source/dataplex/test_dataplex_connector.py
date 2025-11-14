#!/usr/bin/env python3
"""
Test script for Dataplex connector.

This script allows you to test the Dataplex connector without writing metadata to DataHub.
It validates authentication, API access, and metadata extraction.

Prerequisites:
    1. Install the package in development mode:
       cd metadata-ingestion && pip install -e .

    2. Or run via gradle:
       cd <repo-root> && ./gradlew :metadata-ingestion:installDev

Usage:
    python test_dataplex_connector.py --config config.yml
    python test_dataplex_connector.py --project my-project --location us-central1
    python test_dataplex_connector.py --help
"""

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Any, Dict, Optional

import yaml

try:
    from google.api_core import exceptions

    from datahub.ingestion.api.common import PipelineContext
    from datahub.ingestion.source.dataplex.dataplex import DataplexSource
    from datahub.ingestion.source.dataplex.dataplex_config import DataplexConfig
except ImportError as e:
    print("=" * 80)
    print("ERROR: Required packages not installed")
    print("=" * 80)
    print(f"\nImport error: {e}\n")
    print("Please install the package first:")
    print("\nOption 1 - Using pip (from metadata-ingestion directory):")
    print("  cd metadata-ingestion")
    print("  pip install -e .")
    print("\nOption 2 - Using gradle (from repository root):")
    print("  ./gradlew :metadata-ingestion:installDev")
    print("\nOption 3 - Install just the dependencies:")
    print("  pip install google-cloud-dataplex google-cloud-datacatalog-lineage pyyaml")
    print("=" * 80)
    sys.exit(1)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


class DataplexConnectorTester:
    """Test harness for Dataplex connector."""

    def __init__(self, config: Dict[str, Any]):
        """Initialize tester with configuration."""
        self.config = config
        self.source: Optional[DataplexSource] = None
        self.results: Dict[str, Any] = {
            "authentication": None,
            "api_access": None,
            "projects": [],
            "lakes": [],
            "zones": [],
            "entities": [],
            "workunits_generated": 0,
            "errors": [],
        }

    def run_all_tests(self) -> bool:
        """Run all tests and return success status."""
        logger.info("=" * 80)
        logger.info("Starting Dataplex Connector Tests")
        logger.info("=" * 80)

        tests = [
            ("Authentication", self.test_authentication),
            ("Configuration Validation", self.test_configuration),
            ("API Access", self.test_api_access),
            ("Project Listing", self.test_projects),
            ("Lake Extraction", self.test_lakes),
            ("Zone Extraction", self.test_zones),
            ("Entity Extraction", self.test_entities),
            ("Workunit Generation", self.test_workunit_generation),
        ]

        all_passed = True
        for test_name, test_func in tests:
            logger.info(f"\n{'=' * 80}")
            logger.info(f"Test: {test_name}")
            logger.info("=" * 80)

            try:
                result = test_func()
                status = "✅ PASSED" if result else "❌ FAILED"
                logger.info(f"{test_name}: {status}")
                if not result:
                    all_passed = False
            except Exception as e:
                logger.error(f"{test_name}: ❌ FAILED with exception: {e}")
                self.results["errors"].append({"test": test_name, "error": str(e)})
                all_passed = False

        self._print_summary()
        return all_passed

    def test_authentication(self) -> bool:
        """Test GCP authentication."""
        try:
            # Parse config
            dataplex_config = DataplexConfig.model_validate(self.config)

            # Check if credentials are configured
            creds = dataplex_config.get_credentials()
            if creds:
                logger.info("✓ Using service account credentials")
                self.results["authentication"] = "service_account"
            else:
                logger.info("✓ Using Application Default Credentials (ADC)")
                self.results["authentication"] = "adc"

            return True
        except Exception as e:
            logger.error(f"Authentication test failed: {e}")
            return False

    def test_configuration(self) -> bool:
        """Test configuration validation."""
        try:
            dataplex_config = DataplexConfig.model_validate(self.config)

            logger.info(f"✓ Project IDs: {dataplex_config.project_ids}")
            logger.info(f"✓ Location: {dataplex_config.location}")
            logger.info(f"✓ Extract Lakes: {dataplex_config.extract_lakes}")
            logger.info(f"✓ Extract Zones: {dataplex_config.extract_zones}")
            logger.info(f"✓ Extract Assets: {dataplex_config.extract_assets}")
            logger.info(f"✓ Extract Entities: {dataplex_config.extract_entities}")

            return True
        except Exception as e:
            logger.error(f"Configuration validation failed: {e}")
            return False

    def test_api_access(self) -> bool:
        """Test Dataplex API access."""
        try:
            # Initialize source
            ctx = PipelineContext(run_id="test-run")
            self.source = DataplexSource(
                ctx, DataplexConfig.model_validate(self.config)
            )

            logger.info("✓ Successfully initialized Dataplex clients")
            self.results["api_access"] = True
            return True
        except Exception as e:
            logger.error(f"API access test failed: {e}")
            self.results["api_access"] = False
            return False

    def test_projects(self) -> bool:
        """Test project access."""
        if not self.source:
            logger.error("Source not initialized")
            return False

        try:
            for project_id in self.source.config.project_ids:
                logger.info(f"✓ Project configured: {project_id}")
                self.results["projects"].append(
                    {"project_id": project_id, "accessible": True}
                )

            return True
        except Exception as e:
            logger.error(f"Project test failed: {e}")
            return False

    def test_lakes(self) -> bool:
        """Test lake extraction."""
        if not self.source:
            logger.error("Source not initialized")
            return False

        try:
            total_lakes = 0
            for project_id in self.source.config.project_ids:
                logger.info(f"\nScanning lakes in project: {project_id}")

                parent = (
                    f"projects/{project_id}/locations/{self.source.config.location}"
                )
                request = self.source.dataplex_client.list_lakes.__self__.list_lakes(
                    parent=parent
                )

                project_lakes = []
                for lake in request:
                    lake_id = lake.name.split("/")[-1]
                    filtered = (
                        not self.source.config.filter_config.lake_pattern.allowed(
                            lake_id
                        )
                    )

                    lake_info = {
                        "id": lake_id,
                        "display_name": lake.display_name,
                        "description": lake.description,
                        "state": str(lake.state),
                        "filtered": filtered,
                    }

                    status = "🚫 FILTERED" if filtered else "✅ INCLUDED"
                    logger.info(f"  {status} Lake: {lake_id} ({lake.display_name})")
                    project_lakes.append(lake_info)
                    total_lakes += 1

                self.results["lakes"].extend(project_lakes)

            logger.info(f"\n✓ Total lakes found: {total_lakes}")
            return True
        except exceptions.PermissionDenied as e:
            logger.error(f"Permission denied accessing lakes: {e}")
            logger.error(
                "Make sure your service account has 'dataplex.lakes.list' permission"
            )
            return False
        except Exception as e:
            logger.error(f"Lake extraction test failed: {e}")
            return False

    def test_zones(self) -> bool:
        """Test zone extraction."""
        if not self.source:
            logger.error("Source not initialized")
            return False

        try:
            total_zones = 0
            for project_id in self.source.config.project_ids:
                logger.info(f"\nScanning zones in project: {project_id}")

                parent = (
                    f"projects/{project_id}/locations/{self.source.config.location}"
                )
                lakes_request = self.source.dataplex_client.list_lakes(parent=parent)

                for lake in lakes_request:
                    lake_id = lake.name.split("/")[-1]

                    if not self.source.config.filter_config.lake_pattern.allowed(
                        lake_id
                    ):
                        continue

                    logger.info(f"\n  Lake: {lake_id}")

                    zones_parent = f"projects/{project_id}/locations/{self.source.config.location}/lakes/{lake_id}"
                    zones_request = self.source.dataplex_client.list_zones(
                        parent=zones_parent
                    )

                    for zone in zones_request:
                        zone_id = zone.name.split("/")[-1]
                        filtered = (
                            not self.source.config.filter_config.zone_pattern.allowed(
                                zone_id
                            )
                        )

                        zone_info = {
                            "id": zone_id,
                            "lake_id": lake_id,
                            "display_name": zone.display_name,
                            "type": zone.type_.name,
                            "state": str(zone.state),
                            "filtered": filtered,
                        }

                        status = "🚫 FILTERED" if filtered else "✅ INCLUDED"
                        logger.info(
                            f"    {status} Zone: {zone_id} ({zone.display_name}) - Type: {zone.type_.name}"
                        )
                        self.results["zones"].append(zone_info)
                        total_zones += 1

            logger.info(f"\n✓ Total zones found: {total_zones}")
            return True
        except exceptions.PermissionDenied as e:
            logger.error(f"Permission denied accessing zones: {e}")
            logger.error(
                "Make sure your service account has 'dataplex.zones.list' permission"
            )
            return False
        except Exception as e:
            logger.error(f"Zone extraction test failed: {e}")
            return False

    def test_entities(self) -> bool:
        """Test entity extraction."""
        if not self.source:
            logger.error("Source not initialized")
            return False

        try:
            total_entities = 0
            for project_id in self.source.config.project_ids:
                logger.info(f"\nScanning entities in project: {project_id}")

                parent = (
                    f"projects/{project_id}/locations/{self.source.config.location}"
                )
                lakes_request = self.source.dataplex_client.list_lakes(parent=parent)

                for lake in lakes_request:
                    lake_id = lake.name.split("/")[-1]

                    if not self.source.config.filter_config.lake_pattern.allowed(
                        lake_id
                    ):
                        continue

                    zones_parent = f"projects/{project_id}/locations/{self.source.config.location}/lakes/{lake_id}"
                    zones_request = self.source.dataplex_client.list_zones(
                        parent=zones_parent
                    )

                    for zone in zones_request:
                        zone_id = zone.name.split("/")[-1]

                        if not self.source.config.filter_config.zone_pattern.allowed(
                            zone_id
                        ):
                            continue

                        logger.info(f"\n  Lake: {lake_id} / Zone: {zone_id}")

                        entities_parent = f"projects/{project_id}/locations/{self.source.config.location}/lakes/{lake_id}/zones/{zone_id}"
                        entities_request = self.source.metadata_client.list_entities(
                            parent=entities_parent
                        )

                        for entity in entities_request:
                            entity_id = entity.id
                            filtered = not self.source.config.filter_config.entity_pattern.allowed(
                                entity_id
                            )

                            entity_info = {
                                "id": entity_id,
                                "lake_id": lake_id,
                                "zone_id": zone_id,
                                "type": entity.type_,
                                "system": entity.system,
                                "format": (
                                    entity.format.format_.name
                                    if entity.format
                                    else None
                                ),
                                "filtered": filtered,
                            }

                            status = "🚫 FILTERED" if filtered else "✅ INCLUDED"
                            logger.info(
                                f"    {status} Entity: {entity_id} ({entity.type_}) - System: {entity.system}"
                            )
                            self.results["entities"].append(entity_info)
                            total_entities += 1

            logger.info(f"\n✓ Total entities found: {total_entities}")
            return True
        except exceptions.PermissionDenied as e:
            logger.error(f"Permission denied accessing entities: {e}")
            logger.error(
                "Make sure your service account has 'dataplex.entities.list' permission"
            )
            return False
        except Exception as e:
            logger.error(f"Entity extraction test failed: {e}")
            return False

    def test_workunit_generation(self) -> bool:
        """Test workunit generation without writing to DataHub."""
        if not self.source:
            logger.error("Source not initialized")
            return False

        try:
            logger.info("Generating workunits (dry run - not writing to DataHub)...")

            workunit_count = 0
            workunit_types: Dict[str, int] = {}

            for workunit in self.source.get_workunits():
                workunit_count += 1

                # Count by aspect type
                if hasattr(workunit, "metadata") and hasattr(
                    workunit.metadata, "aspectName"
                ):
                    aspect_name = workunit.metadata.aspectName
                    workunit_types[aspect_name] = workunit_types.get(aspect_name, 0) + 1

            self.results["workunits_generated"] = workunit_count
            self.results["workunit_types"] = workunit_types

            logger.info(f"\n✓ Total workunits generated: {workunit_count}")
            logger.info("\nWorkunit breakdown by aspect type:")
            for aspect_type, count in sorted(workunit_types.items()):
                logger.info(f"  - {aspect_type}: {count}")

            return True
        except Exception as e:
            logger.error(f"Workunit generation test failed: {e}")
            return False

    def _print_summary(self) -> None:
        """Print test summary."""
        logger.info("\n" + "=" * 80)
        logger.info("TEST SUMMARY")
        logger.info("=" * 80)

        logger.info(f"\nAuthentication: {self.results['authentication']}")
        logger.info(f"API Access: {self.results['api_access']}")
        logger.info(f"Projects Scanned: {len(self.results['projects'])}")
        logger.info(f"Lakes Found: {len(self.results['lakes'])}")
        logger.info(f"Zones Found: {len(self.results['zones'])}")
        logger.info(f"Entities Found: {len(self.results['entities'])}")
        logger.info(f"Workunits Generated: {self.results['workunits_generated']}")

        if self.results["errors"]:
            logger.error(f"\nErrors Encountered: {len(self.results['errors'])}")
            for error in self.results["errors"]:
                logger.error(f"  - {error['test']}: {error['error']}")

    def export_results(self, output_file: str) -> None:
        """Export test results to JSON file."""
        output_path = Path(output_file)
        with open(output_path, "w") as f:
            json.dump(self.results, f, indent=2, default=str)
        logger.info(f"\n✓ Results exported to: {output_path}")


def load_config(config_path: Optional[str]) -> Dict[str, Any]:
    """Load configuration from YAML file."""
    if config_path:
        with open(config_path) as f:
            full_config = yaml.safe_load(f)
            # Extract just the source config
            return full_config.get("source", {}).get("config", {})
    return {}


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Test Dataplex connector without writing to DataHub"
    )

    # Config file option
    parser.add_argument(
        "--config", "-c", help="Path to DataHub recipe YAML config file"
    )

    # Quick test options
    parser.add_argument("--project", help="GCP Project ID (for quick testing)")
    parser.add_argument(
        "--location",
        default="us-central1",
        help="GCP location (default: us-central1)",
    )
    parser.add_argument(
        "--credential-file", help="Path to service account JSON credential file"
    )

    # Output options
    parser.add_argument("--output", "-o", help="Export results to JSON file (optional)")
    parser.add_argument(
        "--verbose", "-v", action="store_true", help="Enable verbose logging"
    )

    args = parser.parse_args()

    # Set logging level
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # Build config
    if args.config:
        config = load_config(args.config)
    elif args.project:
        # Quick test mode with minimal config
        config = {
            "project_ids": [args.project],
            "location": args.location,
        }

        if args.credential_file:
            # Load credential file
            with open(args.credential_file) as f:
                cred_data = json.load(f)
                config["credential"] = cred_data
    else:
        parser.error("Either --config or --project must be specified")
        return 1

    # Run tests
    tester = DataplexConnectorTester(config)
    success = tester.run_all_tests()

    # Export results if requested
    if args.output:
        tester.export_results(args.output)

    # Exit with appropriate code
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
