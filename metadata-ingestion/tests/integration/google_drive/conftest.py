"""Pytest fixtures for Google Drive integration tests.

These tests run against a real Google Drive folder using a service account,
so they are skipped at collection time unless the required environment
variables are present (see ``pytest_collection_modifyitems`` below).
"""

import os

import pytest


def pytest_collection_modifyitems(config, items):
    """Skip all Google Drive integration tests if required env vars are not set."""
    service_account_key_file = os.environ.get("GOOGLE_DRIVE_SERVICE_ACCOUNT_KEY_FILE")
    test_folder_id = os.environ.get("GOOGLE_DRIVE_TEST_FOLDER_ID")

    if not service_account_key_file or not test_folder_id:
        skip_marker = pytest.mark.skip(
            reason=(
                "GOOGLE_DRIVE_SERVICE_ACCOUNT_KEY_FILE and GOOGLE_DRIVE_TEST_FOLDER_ID "
                "environment variables must be set for Google Drive integration tests"
            )
        )
        for item in items:
            if "google_drive" in str(item.fspath):
                item.add_marker(skip_marker)
