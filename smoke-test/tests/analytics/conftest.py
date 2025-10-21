"""
Pytest fixtures for analytics smoke tests.

Provides session-scoped fixture for loading analytics data with relative timestamps.
"""

import json
import logging
import os
import subprocess
import time
from pathlib import Path
from typing import Generator

import pytest
import requests

logger = logging.getLogger(__name__)


def _validate_analytics_data_searchable(elasticsearch_url: str) -> None:
    """
    Validate that analytics data is searchable in Elasticsearch.

    Forces index refresh and queries for guaranteed coverage events to ensure
    data is available before tests run.

    Args:
        elasticsearch_url: Elasticsearch base URL
    """
    logger.info("Validating that analytics data is searchable in Elasticsearch...")

    # Force Elasticsearch index refresh to make data immediately searchable
    try:
        refresh_response = requests.post(
            f"{elasticsearch_url}/datahub_usage_event/_refresh", timeout=10
        )
        if refresh_response.status_code == 200:
            logger.info("✓ Elasticsearch index refreshed successfully")
        else:
            logger.warning(
                f"Index refresh returned status {refresh_response.status_code}"
            )
    except Exception as e:
        logger.warning(f"Failed to refresh index: {e}")

    # Probe query to verify guaranteed coverage events are searchable
    # Query for EntitySectionViewEvent with entityType=DATASET (guaranteed to exist)
    probe_query = {
        "query": {
            "bool": {
                "must": [
                    {"term": {"type": "EntitySectionViewEvent"}},
                    {"term": {"entityType.keyword": "DATASET"}},
                ]
            }
        },
        "size": 1,
    }

    max_retries = 10
    retry_delay = 1  # seconds

    for attempt in range(1, max_retries + 1):
        try:
            response = requests.post(
                f"{elasticsearch_url}/datahub_usage_event/_search",
                json=probe_query,
                headers={"Content-Type": "application/json"},
                timeout=10,
            )

            if response.status_code == 200:
                result = response.json()
                hit_count = result.get("hits", {}).get("total", {}).get("value", 0)

                if hit_count > 0:
                    logger.info(
                        f"✓ Data is searchable! Found {hit_count} EntitySectionViewEvent events for DATASET"
                    )
                    break
                else:
                    logger.warning(
                        f"Attempt {attempt}/{max_retries}: No results yet, retrying in {retry_delay}s..."
                    )
                    if attempt < max_retries:
                        time.sleep(retry_delay)
            else:
                logger.warning(
                    f"Attempt {attempt}/{max_retries}: Query returned status {response.status_code}"
                )
                if attempt < max_retries:
                    time.sleep(retry_delay)

        except Exception as e:
            logger.warning(f"Attempt {attempt}/{max_retries}: Query failed: {e}")
            if attempt < max_retries:
                time.sleep(retry_delay)
    else:
        # If we exhausted all retries, log a warning but continue
        # (tests may still pass if data becomes searchable shortly after)
        logger.warning(
            "⚠️  Data searchability validation timed out, but continuing with tests"
        )

    logger.info("Analytics data is ready for testing")


@pytest.fixture(scope="session")
def analytics_events_loaded(auth_session) -> Generator[dict, None, None]:
    """
    Load analytics events with relative timestamps for smoke tests.

    This fixture:
    1. Generates synthetic activity events with timestamps relative to current execution time
    2. Loads events directly to Elasticsearch
    3. Ensures "Past Week" and "Past Month" analytics have fresh data
    4. Runs once per test session for efficiency

    Args:
        auth_session: Authenticated session fixture (ensures system is healthy)

    Returns:
        dict: Statistics about loaded events (event_count, event_types)
    """
    # auth_session ensures system is healthy
    logger.info("Loading analytics data...")

    # Get the path to the backfill script
    script_dir = (
        Path(__file__).parent.parent.parent / "test_resources" / "analytics_backfill"
    )
    backfill_script = script_dir / "backfill_activity_events.py"
    users_file = script_dir / "users.json"

    # Check if users file exists, create minimal one if not
    if not users_file.exists():
        logger.info(f"Creating minimal users file at {users_file}")
        minimal_users = [
            {"username": "admin", "email": "admin@test.com"},
            {"username": "datahub", "email": "datahub@test.com"},
        ]
        with open(users_file, "w") as f:
            json.dump(minimal_users, f, indent=2)

    # Get Elasticsearch URL from environment or use default
    elasticsearch_url = os.getenv("ELASTICSEARCH_URL", "http://localhost:9200")

    logger.info("Generating and loading analytics events with relative timestamps...")
    logger.info(f"  Elasticsearch URL: {elasticsearch_url}")
    logger.info("  Days of data: 30")
    logger.info("  Events per day: 200")

    # Run the backfill script with direct Elasticsearch loading
    cmd = [
        "python3",
        str(backfill_script),
        "--users-file",
        str(users_file),
        "--days",
        "30",
        "--events-per-day",
        "200",
        "--elasticsearch-url",
        elasticsearch_url,
        "--load-to-elasticsearch",
        "--seed",
        "42",  # Deterministic seed for reproducible tests
        "--ensure-test-coverage",  # Guarantee required entity types
    ]

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=True,
            timeout=300,  # 5 minute timeout
        )

        logger.info("Analytics data loading output:")
        for line in result.stdout.splitlines():
            logger.info(f"  {line}")

        if result.stderr:
            logger.warning("Warnings during data loading:")
            for line in result.stderr.splitlines():
                logger.warning(f"  {line}")

        # Parse output to extract statistics
        stats = {"event_count": 0, "event_types": {}}
        for line in result.stdout.splitlines():
            if "Generated" in line and "total events" in line:
                # Extract event count from "Generated 5432 total events"
                parts = line.split()
                if len(parts) >= 2:
                    try:
                        stats["event_count"] = int(parts[1])
                    except (ValueError, IndexError):
                        pass

        logger.info(f"✅ Successfully loaded {stats['event_count']} analytics events")

        # Validate that data is searchable before proceeding with tests
        _validate_analytics_data_searchable(elasticsearch_url)

        yield stats

    except subprocess.TimeoutExpired:
        logger.error("Analytics data loading timed out after 5 minutes")
        raise
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to load analytics data: {e}")
        logger.error(f"stdout: {e.stdout}")
        logger.error(f"stderr: {e.stderr}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error loading analytics data: {e}")
        raise

    # Cleanup happens automatically when fixture goes out of scope
    logger.info("Analytics test session complete")


@pytest.fixture(scope="session")
def analytics_cypress_entities_loaded(
    analytics_events_loaded, ingest_cleanup_data
) -> None:
    """
    Ensure Cypress test entities are ingested before running analytics tests.

    This fixture depends on:
    - analytics_events_loaded: Fresh analytics events
    - ingest_cleanup_data: Standard Cypress test data
    """
    # Both dependencies ensure data is loaded
    # This fixture just provides a convenient way to require both
    logger.info("Analytics test data and entities ready")
