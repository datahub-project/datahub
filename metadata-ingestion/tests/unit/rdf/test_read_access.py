#!/usr/bin/env python3
"""
Test script to check read access to DataHub.
"""

import json
import os

import requests


def test_read_access():
    """Test if we can read data from DataHub."""

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

    headers = {
        "Authorization": f"Bearer {API_TOKEN}",
        "Content-Type": "application/json",
    }

    print("Testing DataHub Read Access")
    print("=" * 40)

    try:
        # Test 1: Try to get server config
        print("1. Testing server config endpoint...")
        config_url = f"{DATAHUB_URL}/config"
        response = requests.get(config_url, headers=headers, timeout=10)
        print(f"   Status: {response.status_code}")
        if response.status_code == 200:
            print("   ✅ Server config accessible")
        else:
            print(f"   ❌ Failed: {response.status_code}")

        # Test 2: Try to get existing entities
        print("\n2. Testing entities endpoint...")
        entities_url = f"{DATAHUB_URL}/entities"
        response = requests.get(entities_url, headers=headers, timeout=10)
        print(f"   Status: {response.status_code}")
        if response.status_code == 200:
            print("   ✅ Entities endpoint accessible")
            print(f"   Response length: {len(response.text)}")
            print(f"   Response preview: {response.text[:200]}...")
        else:
            print(f"   ❌ Failed: {response.status_code}")

        # Test 3: Try to get specific entity (the test term we created)
        print("\n3. Testing specific entity access...")
        test_urn = "urn:li:glossaryTerm:test_fibo_term"
        entity_url = f"{DATAHUB_URL}/entities?urn={test_urn}"
        response = requests.get(entity_url, headers=headers, timeout=10)
        print(f"   Status: {response.status_code}")
        if response.status_code == 200:
            print("   ✅ Entity access working")
            print(f"   Response length: {len(response.text)}")
            print(f"   Response preview: {response.text[:200]}...")

            # Try to parse JSON if possible
            try:
                data = response.json()
                print("   JSON parsed successfully")
                if isinstance(data, dict):
                    print(f"   Keys: {list(data.keys())}")
            except json.JSONDecodeError:
                print("   Response is not valid JSON")
        else:
            print(f"   ❌ Failed: {response.status_code}")

        # Test 4: Try to get glossary terms
        print("\n4. Testing glossary terms endpoint...")
        glossary_url = f"{DATAHUB_URL}/entities?entity=glossaryTerm"
        response = requests.get(glossary_url, headers=headers, timeout=10)
        print(f"   Status: {response.status_code}")
        if response.status_code == 200:
            print("   ✅ Glossary terms accessible")
            print(f"   Response length: {len(response.text)}")
            print(f"   Response preview: {response.text[:200]}...")

            # Try to parse JSON if possible
            try:
                data = response.json()
                print("   JSON parsed successfully")
                if isinstance(data, dict):
                    print(f"   Keys: {list(data.keys())}")
            except json.JSONDecodeError:
                print("   Response is not valid JSON")
        else:
            print(f"   ❌ Failed: {response.status_code}")

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    test_read_access()
