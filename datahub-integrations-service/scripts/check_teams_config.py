#!/usr/bin/env python3
"""
Check Teams Configuration

This script checks if Teams configuration exists in DataHub and shows its details.
"""

import json
import os
import sys

# Add the source directory to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from datahub.ingestion.graph.client import get_default_graph

_TEAMS_CONFIG_URN = "urn:li:dataHubConnection:__system_teams-0"


def main():
    print("🔍 Checking Teams Configuration...")
    print(f"Teams Config URN: {_TEAMS_CONFIG_URN}")
    print()

    # Get graph client with proper authentication
    graph = get_default_graph()

    # Check raw connection data
    print("📡 Raw connection data from GraphQL:")
    try:
        query = """
        query GetConnection($urn: String!) {
          connection(urn: $urn) {
            urn
            details {
              type
              name
              json {
                blob
              }
            }
          }
        }
        """

        result = graph.execute_graphql(query, {"urn": _TEAMS_CONFIG_URN})

        if result.get("connection"):
            connection_details = result["connection"]["details"]
            print(f"Connection Type: {connection_details['type']}")
            print(f"Connection Name: {connection_details['name']}")
            if connection_details.get("json", {}).get("blob"):
                blob_data = json.loads(connection_details["json"]["blob"])
                print("Configuration data:")
                print(json.dumps(blob_data, indent=2))
            else:
                print("❌ No JSON blob found")
        else:
            print("❌ No connection found")
    except Exception as e:
        print(f"❌ Error getting connection data: {e}")
        import traceback

        traceback.print_exc()

    # Check environment variables as fallback
    print()
    print("🌱 Environment variables:")
    env_vars = [
        "DATAHUB_TEAMS_APP_ID",
        "DATAHUB_TEAMS_APP_PASSWORD",
        "DATAHUB_TEAMS_TENANT_ID",
        "DATAHUB_TEAMS_WEBHOOK_URL",
        "DATAHUB_TEAMS_ENABLE_CONVERSATION_HISTORY",
    ]

    for var in env_vars:
        value = os.environ.get(var)
        if var == "DATAHUB_TEAMS_APP_PASSWORD" and value:
            value = "***"
        print(f"  {var}: {value}")


if __name__ == "__main__":
    main()
