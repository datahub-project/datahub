#!/usr/bin/env python3
"""
Setup script for Iglu Server integration test.

This script:
1. Initializes the Iglu Server database
2. Uploads test schemas to the Iglu registry

Usage:
    python setup_iglu.py
"""

import json
import logging
import sys
import time
from typing import Dict

import requests

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Configuration
IGLU_SERVER_URL = "http://localhost:8081"
SUPER_API_KEY = "12345678-1234-1234-1234-123456789012"
MAX_RETRIES = 30
RETRY_DELAY = 2


def wait_for_iglu() -> bool:
    """Wait for Iglu Server to be ready."""
    logger.info("Waiting for Iglu Server to be ready...")

    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(f"{IGLU_SERVER_URL}/api/meta/health", timeout=5)
            if response.status_code == 200:
                logger.info("Iglu Server is ready!")
                return True
        except requests.exceptions.RequestException:
            pass

        logger.info(
            f"Iglu Server not ready yet (attempt {attempt + 1}/{MAX_RETRIES})"
        )
        time.sleep(RETRY_DELAY)

    logger.error("Iglu Server did not become ready in time")
    return False


def create_test_schemas() -> Dict[str, Dict]:
    """Create test schemas for ingestion."""
    return {
        "com.test.event/page_view/jsonschema/1-0-0": {
            "$schema": "http://iglucentral.com/schemas/com.snowplowanalytics.self-desc/schema/jsonschema/1-0-0#",
            "self": {
                "vendor": "com.test.event",
                "name": "page_view",
                "format": "jsonschema",
                "version": "1-0-0",
            },
            "type": "object",
            "description": "Page view event for testing Iglu-only mode",
            "properties": {
                "page_url": {
                    "type": "string",
                    "description": "URL of the viewed page",
                    "maxLength": 4096,
                },
                "page_title": {
                    "type": "string",
                    "description": "Title of the page",
                    "maxLength": 2048,
                },
                "referrer": {
                    "type": ["string", "null"],
                    "description": "Referring URL",
                    "maxLength": 4096,
                },
            },
            "required": ["page_url"],
            "additionalProperties": False,
        },
        "com.test.event/checkout_started/jsonschema/1-0-0": {
            "$schema": "http://iglucentral.com/schemas/com.snowplowanalytics.self-desc/schema/jsonschema/1-0-0#",
            "self": {
                "vendor": "com.test.event",
                "name": "checkout_started",
                "format": "jsonschema",
                "version": "1-0-0",
            },
            "type": "object",
            "description": "Checkout started event",
            "properties": {
                "cart_id": {
                    "type": "string",
                    "description": "Shopping cart ID",
                },
                "total_amount": {
                    "type": "number",
                    "description": "Total cart value",
                    "minimum": 0,
                },
                "currency": {
                    "type": "string",
                    "description": "Currency code",
                    "pattern": "^[A-Z]{3}$",
                },
                "item_count": {
                    "type": "integer",
                    "description": "Number of items in cart",
                    "minimum": 1,
                },
            },
            "required": ["cart_id", "total_amount", "currency", "item_count"],
            "additionalProperties": False,
        },
        "com.test.context/user_context/jsonschema/1-0-0": {
            "$schema": "http://iglucentral.com/schemas/com.snowplowanalytics.self-desc/schema/jsonschema/1-0-0#",
            "self": {
                "vendor": "com.test.context",
                "name": "user_context",
                "format": "jsonschema",
                "version": "1-0-0",
            },
            "type": "object",
            "description": "User context entity",
            "properties": {
                "user_id": {
                    "type": "string",
                    "description": "User identifier",
                },
                "user_type": {
                    "type": "string",
                    "description": "User type (guest, registered, premium)",
                    "enum": ["guest", "registered", "premium"],
                },
                "session_id": {
                    "type": "string",
                    "description": "Session identifier",
                },
            },
            "required": ["user_id", "user_type"],
            "additionalProperties": False,
        },
    }


def upload_schema(schema_path: str, schema_data: Dict) -> bool:
    """Upload a schema to Iglu Server."""
    # Parse schema path: vendor/name/format/version
    parts = schema_path.split("/")
    if len(parts) != 4:
        logger.error(f"Invalid schema path: {schema_path}")
        return False

    vendor, name, format_type, version = parts

    # Iglu API endpoint for uploading schemas
    # PUT /api/schemas/{vendor}/{name}/{format}/{version}?isPublic=true
    url = f"{IGLU_SERVER_URL}/api/schemas/{vendor}/{name}/{format_type}/{version}"

    headers = {
        "Content-Type": "application/json",
        "apikey": SUPER_API_KEY,
    }

    params = {
        "isPublic": "true"  # Make schemas publicly readable for testing
    }

    try:
        logger.info(f"Uploading schema: {schema_path}")
        response = requests.put(
            url, json=schema_data, headers=headers, params=params, timeout=10
        )

        if response.status_code in (200, 201):
            logger.info(f"✓ Successfully uploaded: {schema_path}")
            return True
        else:
            logger.error(
                f"✗ Failed to upload {schema_path}: {response.status_code} - {response.text}"
            )
            return False

    except requests.exceptions.RequestException as e:
        logger.error(f"✗ Error uploading {schema_path}: {e}")
        return False


def verify_schema(schema_path: str) -> bool:
    """Verify a schema can be retrieved from Iglu Server."""
    parts = schema_path.split("/")
    vendor, name, format_type, version = parts

    url = f"{IGLU_SERVER_URL}/api/schemas/{vendor}/{name}/{format_type}/{version}"

    try:
        response = requests.get(url, timeout=5)
        if response.status_code == 200:
            logger.info(f"✓ Verified schema: {schema_path}")
            return True
        else:
            logger.error(
                f"✗ Failed to verify {schema_path}: {response.status_code}"
            )
            return False
    except requests.exceptions.RequestException as e:
        logger.error(f"✗ Error verifying {schema_path}: {e}")
        return False


def main():
    """Main setup function."""
    logger.info("=" * 60)
    logger.info("Iglu Server Setup for Integration Testing")
    logger.info("=" * 60)

    # Step 1: Wait for Iglu Server
    if not wait_for_iglu():
        logger.error("Failed to connect to Iglu Server")
        sys.exit(1)

    # Step 2: Create and upload test schemas
    schemas = create_test_schemas()
    logger.info(f"Uploading {len(schemas)} test schemas...")

    success_count = 0
    for schema_path, schema_data in schemas.items():
        if upload_schema(schema_path, schema_data):
            success_count += 1

    logger.info(f"Successfully uploaded {success_count}/{len(schemas)} schemas")

    # Step 3: Verify schemas
    logger.info("Verifying uploaded schemas...")
    verify_count = 0
    for schema_path in schemas.keys():
        if verify_schema(schema_path):
            verify_count += 1

    logger.info(f"Successfully verified {verify_count}/{len(schemas)} schemas")

    # Summary
    logger.info("=" * 60)
    if success_count == len(schemas) and verify_count == len(schemas):
        logger.info("✓ Iglu Server setup complete!")
        logger.info(f"Iglu Server URL: {IGLU_SERVER_URL}")
        logger.info(f"Uploaded schemas: {len(schemas)}")
        sys.exit(0)
    else:
        logger.error("✗ Iglu Server setup had errors")
        sys.exit(1)


if __name__ == "__main__":
    main()
