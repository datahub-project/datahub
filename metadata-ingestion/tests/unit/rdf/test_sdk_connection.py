#!/usr/bin/env python3
"""
Test script to verify DataHub SDK connection and test glossary operations.
"""

import os

from datahub.emitter.mce_builder import make_term_urn
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import (
    GlossaryTermInfoClass,
    GlossaryTermKeyClass,
    GlossaryTermSnapshotClass,
    MetadataChangeEventClass,
)


def test_sdk_connection():
    """Test DataHub SDK connection and basic glossary operations."""

    print("Testing DataHub SDK Connection")
    print("=" * 40)

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

    try:
        # Create emitter
        print(f"Connecting to: {DATAHUB_URL}")
        emitter = DatahubRestEmitter(DATAHUB_URL, API_TOKEN)

        # Test connection
        print("\n1. Testing connection...")
        config = emitter.get_server_config()
        print("   ✅ Connected successfully!")
        print(f"   Server config: {config}")

        # Test creating a simple glossary term
        print("\n2. Testing glossary term creation...")

        # Create a test term
        term_id = "test_fibo_term"
        term_urn = make_term_urn(term_id)

        term_info = GlossaryTermInfoClass(
            name="Test FIBO Term",
            definition="A test term to verify SDK functionality",
            termSource="EXTERNAL",
        )

        term_snapshot = GlossaryTermSnapshotClass(
            urn=term_urn, aspects=[GlossaryTermKeyClass(name=term_id), term_info]
        )

        mce = MetadataChangeEventClass(proposedSnapshot=term_snapshot)

        # Emit the term
        print(f"   Creating term: {term_urn}")
        emitter.emit_mce(mce)
        emitter.flush()
        print("   ✅ Term created successfully!")

        print("\n✅ DataHub SDK connection and glossary operations working!")
        print(f"   Test term URN: {term_urn}")

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    test_sdk_connection()
