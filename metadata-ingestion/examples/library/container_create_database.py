# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

# metadata-ingestion/examples/library/container_create_database.py
from datahub.emitter.mcp_builder import DatabaseKey
from datahub.sdk import Container, DataHubClient

client = DataHubClient.from_env()

container = Container(
    container_key=DatabaseKey(
        platform="snowflake",
        instance="production",
        database="analytics_db",
    ),
    display_name="Analytics Database",
    description="Main analytics database containing reporting and metrics data",
    subtype="Database",
    external_url="https://app.snowflake.com/analytics_db",
    parent_container=None,
)

client.entities.upsert(container)

print(f"Created database container with URN: {container.urn}")
