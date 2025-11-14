#!/usr/bin/env python3
"""
Dataplex Explorer - Google Dataplex Data Extraction Tool

Extracts all Dataplex objects including data quality, profiling, and lineage information.
Writes comprehensive data to a structured log file with summary statistics.

Requires: pip install google-cloud-dataplex google-cloud-datacatalog-lineage

Usage:
    python dataplex_explorer.py [output_file]

Environment Variables:
    DATAPLEX_PROJECT_ID: GCP Project ID (required)
    DATAPLEX_LOCATION: GCP Location (required)

Note: To test lineage extraction, use generate_sample_lineage.py to create sample
      lineage relationships between entities before running this script.
"""

import logging
import os
import sys

from dataplex_client import DataplexClient
from dataplex_writer import write_extraction_log

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(message)s")


def main():
    """Main function - Extract all Dataplex data and write to log file"""

    # Configuration - Read from environment variables (required)
    PROJECT_ID = os.getenv("DATAPLEX_PROJECT_ID")
    LOCATION = os.getenv("DATAPLEX_LOCATION")

    if not PROJECT_ID:
        print("ERROR: DATAPLEX_PROJECT_ID environment variable is required")
        sys.exit(1)

    if not LOCATION:
        print("ERROR: DATAPLEX_LOCATION environment variable is required")
        sys.exit(1)

    # Determine output file
    output_file = sys.argv[1] if len(sys.argv) > 1 else "dataplex_extraction.log"

    print("=" * 80)
    print("DATAPLEX EXPLORER")
    print("=" * 80)
    print(f"Project ID: {PROJECT_ID}")
    print(f"Location: {LOCATION}")
    print(f"Output File: {output_file}")
    print("=" * 80)
    print()

    # Initialize client
    print("Initializing Dataplex client...")
    client = DataplexClient(PROJECT_ID, LOCATION)
    print("Client initialized.\n")

    # Extract all data
    extraction_data = client.extract_all_data()

    # Write to log file
    print("\nWriting extraction data to log file...")
    write_extraction_log(extraction_data, output_file)

    # Print summary to console
    print("\n" + "=" * 80)
    print("EXTRACTION COMPLETE")
    print("=" * 80)
    total = (
        len(extraction_data["lakes"])
        + len(extraction_data["zones"])
        + len(extraction_data["assets"])
        + len(extraction_data["entities"])
        + len(extraction_data["entry_groups"])
        + len(extraction_data["entries"])
        + len(extraction_data["data_scans"])
    )
    print(f"Total objects extracted: {total}")
    print(f"  - Lakes: {len(extraction_data['lakes'])}")
    print(f"  - Zones: {len(extraction_data['zones'])}")
    print(f"  - Assets: {len(extraction_data['assets'])}")
    print(f"  - Entities: {len(extraction_data['entities'])}")
    print(f"  - Entry Groups: {len(extraction_data['entry_groups'])}")
    print(f"  - Entries: {len(extraction_data['entries'])}")
    print(f"  - Data Scans: {len(extraction_data['data_scans'])}")
    print(f"  - Data Quality Results: {len(extraction_data['data_quality_results'])}")
    print(f"  - Data Profile Results: {len(extraction_data['data_profile_results'])}")
    print("=" * 80)


if __name__ == "__main__":
    main()
