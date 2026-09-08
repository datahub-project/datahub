"""
Pytest fixtures for analytics smoke tests.

Provides session-scoped fixture for loading analytics data with relative timestamps.
"""

import json
import logging
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Generator

import pytest
import requests

from tests.utilities import env_vars
from tests.utilities.usage_events_sot import (
    PRODUCT_USAGE_EVENT_TABLE,
    resolve_usage_events_implementation,
)

logger = logging.getLogger(__name__)


def _validate_analytics_data_searchable(elasticsearch_url: str) -> None:
    """
    Validate that analytics data is searchable in Elasticsearch.

    Forces index refresh and queries for guaranteed coverage events to ensure
    data is available before tests run.
    """
    logger.info("Validating that analytics data is searchable in Elasticsearch...")

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
    retry_delay = 1

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
        logger.warning(
            "⚠️  Data searchability validation timed out, but continuing with tests"
        )

    logger.info("Analytics data is ready for testing")


def _validate_analytics_data_in_postgres() -> None:
    """Validate synthetic usage events are present in pgAnalytics."""
    import psycopg2

    host_port = env_vars.get_postgres_url()
    host, _, port_s = host_port.partition(":")
    port = int(port_s or "5432")
    logger.info("Validating that analytics data is present in Postgres...")

    max_retries = 10
    retry_delay = 1
    for attempt in range(1, max_retries + 1):
        try:
            conn = psycopg2.connect(
                host=host,
                port=port,
                user=env_vars.get_postgres_username(),
                password=env_vars.get_postgres_password(),
                dbname="datahub",
            )
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        f"""
                        SELECT count(*) FROM {PRODUCT_USAGE_EVENT_TABLE}
                        WHERE metric_family = 'datahub_usage'
                          AND event_type = 'EntitySectionViewEvent'
                          AND upper(entity_type) = 'DATASET'
                          AND (usage_source IS NULL OR usage_source <> 'backend')
                        """
                    )
                    hit_count = cur.fetchone()[0]
            finally:
                conn.close()

            if hit_count > 0:
                logger.info(
                    "✓ Postgres has %s EntitySectionViewEvent events for DATASET",
                    hit_count,
                )
                break
            logger.warning(
                "Attempt %s/%s: No Postgres rows yet, retrying in %ss...",
                attempt,
                max_retries,
                retry_delay,
            )
            if attempt < max_retries:
                time.sleep(retry_delay)
        except Exception as e:
            logger.warning(
                "Attempt %s/%s: Postgres probe failed: %s", attempt, max_retries, e
            )
            if attempt < max_retries:
                time.sleep(retry_delay)
    else:
        logger.warning(
            "⚠️  Postgres analytics validation timed out, but continuing with tests"
        )

    logger.info("Analytics data is ready for testing")


@pytest.fixture(scope="session")
def analytics_events_loaded(auth_session) -> Generator[dict, None, None]:
    """
    Load analytics events with relative timestamps for smoke tests.

    Loads into Postgres when DATAHUB_USAGE_EVENTS_IMPLEMENTATION=postgres,
    otherwise into Elasticsearch (legacy SoT).
    """
    logger.info("Loading analytics data...")

    script_dir = (
        Path(__file__).parent.parent.parent / "test_resources" / "analytics_backfill"
    )
    backfill_script = script_dir / "backfill_activity_events.py"
    users_file = script_dir / "users.json"

    if not users_file.exists():
        logger.info(f"Creating minimal users file at {users_file}")
        minimal_users = [
            {"username": "admin", "email": "admin@test.com"},
            {"username": "datahub", "email": "datahub@test.com"},
        ]
        with open(users_file, "w") as f:
            json.dump(minimal_users, f, indent=2)

    elasticsearch_url = os.getenv("ELASTICSEARCH_URL", "http://localhost:9200")
    sot = resolve_usage_events_implementation(auth_session)
    use_postgres = sot == "postgres"
    days_to_generate = 45

    logger.info("Generating and loading analytics events with relative timestamps...")
    logger.info("  Usage SoT: %s", sot)
    logger.info(f"  Days of data: {days_to_generate}")
    logger.info("  Events per day: 200")

    cmd = [
        sys.executable,
        str(backfill_script),
        "--users-file",
        str(users_file),
        "--days",
        str(days_to_generate),
        "--events-per-day",
        "200",
        "--seed",
        "42",
        "--ensure-test-coverage",
    ]
    if use_postgres:
        cmd.extend(
            [
                "--load-to-postgres",
                "--postgres-url",
                env_vars.get_postgres_url(),
                "--postgres-username",
                env_vars.get_postgres_username(),
                "--postgres-password",
                env_vars.get_postgres_password(),
            ]
        )
    else:
        cmd.extend(
            [
                "--elasticsearch-url",
                elasticsearch_url,
                "--load-to-elasticsearch",
            ]
        )

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=True,
            timeout=300,
        )

        logger.info("Analytics data loading output:")
        for line in result.stdout.splitlines():
            logger.info(f"  {line}")

        if result.stderr:
            logger.warning("Warnings during data loading:")
            for line in result.stderr.splitlines():
                logger.warning(f"  {line}")

        stats = {"event_count": 0, "event_types": {}}
        for line in result.stdout.splitlines():
            if "Generated" in line and "total events" in line:
                parts = line.split()
                if len(parts) >= 2:
                    try:
                        stats["event_count"] = int(parts[1])
                    except (ValueError, IndexError):
                        pass

        logger.info(f"✅ Successfully loaded {stats['event_count']} analytics events")

        if use_postgres:
            _validate_analytics_data_in_postgres()
        else:
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
    logger.info("Analytics test data and entities ready")
