# metadata-ingestion/examples/library/version_set_query.py
"""
Query a version set to retrieve information about versions.

This example demonstrates how to fetch version set metadata and query
all versions using both REST API and GraphQL approaches.
"""

from urllib.parse import quote

import requests

from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
from datahub.metadata.schema_classes import (
    VersionPropertiesClass,
    VersionSetPropertiesClass,
)

# Initialize DataHub Graph client
config = DatahubClientConfig(server="http://localhost:8080")
graph = DataHubGraph(config)

# Define the version set URN to query
version_set_urn = "urn:li:versionSet:(abc123def456,mlModel)"

# Method 1: Query using the Python SDK
print("=== Querying Version Set via Python SDK ===\n")

# Get the version set properties
version_set_props = graph.get_aspect(
    entity_urn=version_set_urn,
    aspect_type=VersionSetPropertiesClass,
)

if version_set_props:
    print(f"Version Set: {version_set_urn}")
    print(f"Latest Version: {version_set_props.latest}")
    print(f"Versioning Scheme: {version_set_props.versioningScheme}")

    if version_set_props.customProperties:
        print("\nCustom Properties:")
        for key, value in version_set_props.customProperties.items():
            print(f"  {key}: {value}")

    # Get version properties for the latest version
    print("\n=== Latest Version Details ===\n")
    latest_version_props = graph.get_aspect(
        entity_urn=version_set_props.latest,
        aspect_type=VersionPropertiesClass,
    )

    if latest_version_props:
        print(f"Version: {latest_version_props.version.versionTag}")
        print(f"Sort ID: {latest_version_props.sortId}")
        if latest_version_props.comment:
            print(f"Comment: {latest_version_props.comment}")
        if latest_version_props.aliases:
            aliases = [
                alias.versionTag
                for alias in latest_version_props.aliases
                if alias.versionTag is not None
            ]
            print(f"Aliases: {', '.join(aliases)}")
else:
    print(f"Version set {version_set_urn} not found")

# Method 2: Query all versions using GraphQL
print("\n=== Querying All Versions via GraphQL ===\n")

graphql_query = """
query ($urn: String!) {
  versionSet(urn: $urn) {
    urn
    latestVersion {
      urn
      ... on MLModel {
        properties {
          name
          description
        }
      }
    }
    versionsSearch(input: {
      query: "*"
      start: 0
      count: 100
    }) {
      total
      searchResults {
        entity {
          urn
          ... on MLModel {
            versionProperties {
              version {
                versionTag
              }
              sortId
              comment
              isLatest
              aliases {
                versionTag
              }
            }
          }
        }
      }
    }
  }
}
"""

variables = {"urn": version_set_urn}

response = graph.execute_graphql(graphql_query, variables)

if "versionSet" in response and response["versionSet"]:
    version_set_data = response["versionSet"]
    versions = version_set_data.get("versionsSearch", {}).get("searchResults", [])

    print(f"Found {len(versions)} version(s)\n")

    for result in versions:
        entity = result.get("entity", {})
        version_props = entity.get("versionProperties", {})

        version_tag = version_props.get("version", {}).get("versionTag", "Unknown")
        is_latest = version_props.get("isLatest", False)
        comment = version_props.get("comment", "No comment")
        aliases = [
            alias.get("versionTag")
            for alias in version_props.get("aliases", [])
            if alias.get("versionTag")
        ]

        print(f"Version: {version_tag}")
        print(f"  URN: {entity.get('urn')}")
        print(f"  Latest: {is_latest}")
        print(f"  Comment: {comment}")
        if aliases:
            print(f"  Aliases: {', '.join(aliases)}")
        print()
else:
    print("No version set data found")

# Method 3: Query using REST API directly
print("\n=== Querying via REST API ===\n")

rest_url = f"http://localhost:8080/entities/{quote(version_set_urn, safe='')}"

try:
    rest_response = requests.get(rest_url)
    rest_response.raise_for_status()

    entity_data = rest_response.json()
    aspects = entity_data.get("aspects", {})

    if "versionSetProperties" in aspects:
        props = aspects["versionSetProperties"]["value"]
        print(f"Latest (from REST): {props.get('latest')}")
        print(f"Versioning Scheme (from REST): {props.get('versioningScheme')}")
    else:
        print("No versionSetProperties found in REST response")

except requests.exceptions.RequestException as e:
    print(f"REST API error: {e}")
