#!/usr/bin/env python3
"""
Test script to verify connection to live DataHub instance.
"""

import os

import requests


def test_datahub_connection():
    """Test connection to DataHub instance."""

    # Configuration
    DATAHUB_URL = os.environ.get("DATAHUB_URL")
    API_TOKEN = os.environ.get("TOKEN")

    if not DATAHUB_URL:
        print("❌ Error: DATAHUB_URL environment variable not set")
        print(
            "Please set DATAHUB_URL environment variable with your DataHub instance URL"
        )
        return

    if not API_TOKEN:
        print("❌ Error: TOKEN environment variable not set")
        print("Please set TOKEN environment variable with your DataHub API token")
        return

    print("Testing DataHub Connection")
    print("=" * 40)
    print(f"URL: {DATAHUB_URL}")

    headers = {
        "Authorization": f"Bearer {API_TOKEN}",
        "Content-Type": "application/json",
    }

    try:
        # Test 1: Basic health check
        print("\n1. Testing basic connection...")
        health_url = f"{DATAHUB_URL}/health"
        response = requests.get(health_url, headers=headers, timeout=10)
        print(f"   Status: {response.status_code}")
        if response.status_code == 200:
            print("   ✅ Connection successful!")
        else:
            print(f"   ❌ Unexpected status: {response.status_code}")

        # Test 2: GraphQL endpoint
        print("\n2. Testing GraphQL endpoint...")
        graphql_url = f"{DATAHUB_URL}/api/graphql"

        # Simple query to test authentication
        query = """
        query {
          __schema {
            types {
              name
            }
          }
        }
        """

        response = requests.post(
            graphql_url, headers=headers, json={"query": query}, timeout=10
        )

        print(f"   Status: {response.status_code}")
        if response.status_code == 200:
            print("   ✅ GraphQL endpoint accessible!")
            data = response.json()
            if "errors" in data:
                print(f"   ⚠️  GraphQL errors: {data['errors']}")
            else:
                print("   ✅ GraphQL query successful!")
        else:
            print(f"   ❌ GraphQL failed: {response.status_code}")
            print(f"   Response: {response.text[:200]}...")

        # Test 3: Check existing glossary terms
        print("\n3. Checking existing glossary terms...")
        glossary_query = """
        query {
          glossaryTerms(first: 5) {
            total
            terms {
              urn
              name
              description
            }
          }
        }
        """

        response = requests.post(
            graphql_url, headers=headers, json={"query": glossary_query}, timeout=10
        )

        print(f"   Status: {response.status_code}")
        if response.status_code == 200:
            data = response.json()
            if "errors" in data:
                print(f"   ⚠️  GraphQL errors: {data['errors']}")
            else:
                total_terms = (
                    data.get("data", {}).get("glossaryTerms", {}).get("total", 0)
                )
                print(f"   ✅ Found {total_terms} existing glossary terms")
                if total_terms > 0:
                    terms = data["data"]["glossaryTerms"]["terms"]
                    print("   Sample terms:")
                    for term in terms[:3]:
                        print(f"     - {term['name']} ({term['urn']})")
        else:
            print(f"   ❌ Glossary query failed: {response.status_code}")

    except requests.exceptions.RequestException as e:
        print(f"❌ Connection error: {e}")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    test_datahub_connection()
