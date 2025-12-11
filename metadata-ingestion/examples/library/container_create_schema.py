# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

# metadata-ingestion/examples/library/container_create_schema.py
from datahub.emitter.mcp_builder import DatabaseKey, SchemaKey
from datahub.sdk import Container, DataHubClient

client = DataHubClient.from_env()

# First, create the database container
database_key = DatabaseKey(
    platform="snowflake",
    instance="production",
    database="analytics_db",
)

database_container = Container(
    container_key=database_key,
    display_name="Analytics Database",
    description="Main analytics database",
    subtype="Database",
)

client.entities.upsert(database_container)
print(f"Created database container: {database_container.urn}")

# Create a schema container within the database
schema_key = SchemaKey(
    platform="snowflake",
    instance="production",
    database="analytics_db",
    schema="reporting",
)

schema_container = Container(
    container_key=schema_key,
    display_name="Reporting Schema",
    description="Schema containing all reporting tables and views",
    subtype="Schema",
)

client.entities.upsert(schema_container)
print(f"Created schema container: {schema_container.urn}")
print("Schema container is nested under database container")
