"""
Base class for DataHub Locust performance tests.

All domain perf tests subclass DataHubUser to get:
  - Consistent auth header injection (token from DATAHUB_GMS_TOKEN env var)
  - Structured GraphQL error detection (resp.failure on non-null errors array)
  - helpers for building common DataHub URNs
"""

import os
import random
from typing import Any

from locust import HttpUser, between

PLATFORMS = ["snowflake", "bigquery", "redshift", "mysql", "hive", "postgres"]
ENVS = ["PROD", "DEV"]
DATASET_ID_RANGE = (1, 100_000)


def random_dataset_urn(
    id_range: tuple[int, int] = DATASET_ID_RANGE,
    platforms: list[str] = PLATFORMS,
) -> str:
    platform = random.choice(platforms)
    idx = random.randint(*id_range)
    env = random.choice(ENVS)
    return f"urn:li:dataset:(urn:li:dataPlatform:{platform},test_dataset_{idx},{env})"


class DataHubUser(HttpUser):
    """
    Base Locust user for DataHub GMS load tests.
    Subclasses must define @task methods; they inherit auth headers and helpers.
    """

    abstract = True
    wait_time = between(1, 3)

    def on_start(self) -> None:
        token = os.environ.get("DATAHUB_GMS_TOKEN", "")
        self.headers: dict[str, str] = {"Content-Type": "application/json"}
        if token:
            self.headers["Authorization"] = f"Bearer {token}"
        else:
            # Local dev: actor spoofing header (no auth middleware)
            self.headers["X-DataHub-Actor"] = "urn:li:corpuser:datahub"

    def graphql(
        self,
        query: str,
        variables: dict[str, Any],
        name: str,
    ) -> None:
        """POST a GraphQL operation and mark failure on HTTP errors or GQL errors."""
        payload = {"query": query, "variables": variables}
        with self.client.post(
            "/api/graphql",
            json=payload,
            headers=self.headers,
            catch_response=True,
            name=name,
        ) as resp:
            if resp.status_code != 200:
                resp.failure(f"HTTP {resp.status_code}: {resp.text[:200]}")
                return
            body: dict[str, Any] = resp.json()
            errors = body.get("errors")
            if errors:
                msg = errors[0].get("message", str(errors[0]))
                resp.failure(f"GraphQL error: {msg}")
            else:
                resp.success()
