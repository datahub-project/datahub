"""Integration tests for Notion connector.

These tests validate the Notion connector against real Notion pages,
specifically testing the monkeypatches for:
1. SyncBlock compatibility (synced_from field handling)
2. NumberedListItem new fields (list_start_index, list_format)
3. Embedding credential validation warnings

Requirements:
    NOTION_API_KEY: Notion integration API key (required)
    NOTION_TEST_PARENT_PAGE_ID: Parent page ID where test pages will be created (required)
    AWS_ACCESS_KEY_ID: AWS credentials for Bedrock embedding tests (optional)
    COHERE_API_KEY: Cohere API key for embedding tests (optional)

Setup:
    1. Create a Notion integration at https://www.notion.so/my-integrations
    2. Share a test page with the integration
    3. Export NOTION_API_KEY=<your-integration-token>
    4. Export NOTION_TEST_PARENT_PAGE_ID=<your-test-page-id>
"""

import logging
import os
from pathlib import Path
from typing import List

import pytest

from datahub.ingestion.run.pipeline import Pipeline

# Mark all tests in this module as integration tests
pytestmark = pytest.mark.integration


def test_notion_synced_blocks_ingestion(
    test_page_synced_blocks: str,
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Test that pages with synced blocks can be ingested without errors.

    This validates the SyncBlock monkeypatch that fixes the synced_from field handling.

    Expected behavior:
        - Ingestion succeeds without KeyError on synced blocks
        - Log shows "Applied monkeypatch to OriginalSyncedBlock"
        - Page is successfully processed
    """
    caplog.set_level(logging.INFO)

    api_key = os.environ.get("NOTION_API_KEY")
    assert api_key, "NOTION_API_KEY must be set"

    pipeline = Pipeline.create(
        {
            "run_id": "test-notion-synced-blocks",
            "source": {
                "type": "notion",
                "config": {
                    "api_key": api_key,
                    "page_ids": [test_page_synced_blocks],
                    "recursive": False,
                    "advanced": {
                        "continue_on_failure": False,
                        "raise_on_error": True,
                    },
                },
            },
            "sink": {
                "type": "file",
                "config": {
                    "filename": str(tmp_path / "notion_synced_blocks.json"),
                },
            },
        }
    )

    pipeline.run()
    pipeline.raise_from_status()

    # Verify monkeypatch was applied
    assert any(
        "Applied monkeypatch to OriginalSyncedBlock" in record.message
        for record in caplog.records
    ), "OriginalSyncedBlock monkeypatch was not applied"

    # Note: Warning about synced blocks may or may not appear depending on
    # whether synced blocks are actually encountered during processing

    # Verify ingestion succeeded
    output_file = tmp_path / "notion_synced_blocks.json"
    assert output_file.exists(), "Output file was not created"
    assert output_file.stat().st_size > 0, "Output file is empty"


def test_notion_numbered_lists_ingestion(
    test_page_numbered_lists: str,
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Test that the NumberedListItem monkeypatch is applied.

    This validates that the NumberedListItem monkeypatch is loaded, which handles
    new Notion API fields (list_start_index, list_format) when they appear.

    Expected behavior:
        - Log shows "Applied monkeypatch to NumberedListItem"
        - Page is successfully processed (even with simple content)
    """
    caplog.set_level(logging.INFO)

    api_key = os.environ.get("NOTION_API_KEY")
    assert api_key, "NOTION_API_KEY must be set"

    pipeline = Pipeline.create(
        {
            "run_id": "test-notion-numbered-lists",
            "source": {
                "type": "notion",
                "config": {
                    "api_key": api_key,
                    "page_ids": [test_page_numbered_lists],
                    "recursive": False,
                    "advanced": {
                        "continue_on_failure": True,  # Allow test to complete even if ingestion has issues
                        "raise_on_error": False,
                    },
                },
            },
            "sink": {
                "type": "file",
                "config": {
                    "filename": str(tmp_path / "notion_numbered_lists.json"),
                },
            },
        }
    )

    pipeline.run()

    # Verify monkeypatch was applied (this is the critical validation)
    assert any(
        "Applied monkeypatch to NumberedListItem" in record.message
        for record in caplog.records
    ), "NumberedListItem monkeypatch was not applied"

    # Verify some output was created
    output_file = tmp_path / "notion_numbered_lists.json"
    assert output_file.exists(), "Output file was not created"


def test_notion_full_ingestion(
    test_page_ids_for_ingestion: List[str],
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Test full ingestion with all test pages.

    This validates that all monkeypatches work together correctly.

    Expected behavior:
        - All monkeypatches are applied
        - Test pages are processed (some may succeed, some may have issues)
        - No fatal errors that prevent monkeypatch application
    """
    caplog.set_level(logging.INFO)

    api_key = os.environ.get("NOTION_API_KEY")
    assert api_key, "NOTION_API_KEY must be set"

    pipeline = Pipeline.create(
        {
            "run_id": "test-notion-full-ingestion",
            "source": {
                "type": "notion",
                "config": {
                    "api_key": api_key,
                    "page_ids": test_page_ids_for_ingestion,
                    "recursive": False,
                    "advanced": {
                        "continue_on_failure": True,  # Allow partial success
                        "raise_on_error": False,
                    },
                },
            },
            "sink": {
                "type": "file",
                "config": {
                    "filename": str(tmp_path / "notion_full_ingestion.json"),
                },
            },
        }
    )

    pipeline.run()

    # Verify all monkeypatches were applied (this is the critical validation)
    monkeypatches = [
        "Applied monkeypatch to OriginalSyncedBlock",
        "Applied monkeypatch to NumberedListItem",
        "Applied monkeypatch to unstructured-ingest Page class",
        "database property classes",  # Part of "Applied monkeypatch to 22 database property classes"
    ]

    for patch_message in monkeypatches:
        assert any(patch_message in record.message for record in caplog.records), (
            f"Expected monkeypatch not found: {patch_message}"
        )

    # Verify output file was created
    output_file = tmp_path / "notion_full_ingestion.json"
    assert output_file.exists(), "Output file was not created"


def test_notion_bedrock_credential_warning(
    test_page_complex_content: str,
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test that missing AWS Bedrock credentials produce clear warnings.

    This validates the early credential validation warning system.

    Expected behavior:
        - Warning about missing AWS credentials is logged
        - Ingestion still succeeds (non-blocking)
        - Documents are ingested without embeddings
    """
    caplog.set_level(logging.WARNING)

    # Clear any existing AWS credentials from environment
    monkeypatch.delenv("AWS_ACCESS_KEY_ID", raising=False)
    monkeypatch.delenv("AWS_SECRET_ACCESS_KEY", raising=False)
    monkeypatch.delenv("AWS_PROFILE", raising=False)

    api_key = os.environ.get("NOTION_API_KEY")
    assert api_key, "NOTION_API_KEY must be set"

    pipeline = Pipeline.create(
        {
            "run_id": "test-notion-bedrock-warning",
            "source": {
                "type": "notion",
                "config": {
                    "api_key": api_key,
                    "page_ids": [test_page_complex_content],
                    "recursive": False,
                    "embedding": {
                        "provider": "bedrock",
                        "model": "amazon.titan-embed-text-v1",
                    },
                    "advanced": {
                        "continue_on_failure": True,
                        "raise_on_error": False,
                    },
                },
            },
            "sink": {
                "type": "file",
                "config": {
                    "filename": str(tmp_path / "notion_bedrock_warning.json"),
                },
            },
        }
    )

    pipeline.run()

    # Verify warning was logged (should see the prominent warning banner)
    assert any(
        "WARNING: AWS Bedrock embeddings configured but credentials not detected"
        in record.message
        for record in caplog.records
    ), "Early credential warning was not logged"

    # Verify output file was created (documents may or may not be ingested depending on page content)
    output_file = tmp_path / "notion_bedrock_warning.json"
    assert output_file.exists(), "Output file was not created"


def test_notion_cohere_credential_warning(
    test_page_complex_content: str,
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test that invalid Cohere credentials produce clear error messages.

    This validates the per-document credential error detection.

    Expected behavior:
        - Credential errors are detected and logged prominently
        - Documents are still ingested without embeddings
        - Final report shows embedding failures
    """
    caplog.set_level(logging.ERROR)

    api_key = os.environ.get("NOTION_API_KEY")
    assert api_key, "NOTION_API_KEY must be set"

    pipeline = Pipeline.create(
        {
            "run_id": "test-notion-cohere-warning",
            "source": {
                "type": "notion",
                "config": {
                    "api_key": api_key,
                    "page_ids": [test_page_complex_content],
                    "recursive": False,
                    "embedding": {
                        "provider": "cohere",
                        "model": "embed-english-v3.0",
                        "api_key": "invalid-key-for-testing",
                    },
                    "advanced": {
                        "continue_on_failure": True,
                        "raise_on_error": False,
                    },
                },
            },
            "sink": {
                "type": "file",
                "config": {
                    "filename": str(tmp_path / "notion_cohere_warning.json"),
                },
            },
        }
    )

    pipeline.run()

    # Get the report to check for embedding failures
    from datahub.ingestion.source.notion.notion_report import NotionSourceReport

    source_report = pipeline.source.get_report()
    assert isinstance(source_report, NotionSourceReport)

    # Verify credential error detection
    # Should see either:
    # 1. "EMBEDDING CREDENTIAL ERROR" log message, OR
    # 2. "EMBEDDING GENERATION FAILED FOR ALL X DOCUMENTS" in logs, OR
    # 3. Report shows embedding failures with zero successes
    credential_error_logged = any(
        "EMBEDDING CREDENTIAL ERROR" in record.message for record in caplog.records
    )
    all_embeddings_failed = any(
        "EMBEDDING GENERATION FAILED FOR ALL" in record.message
        for record in caplog.records
    )
    report_shows_failures = (
        source_report.num_embedding_failures > 0
        and source_report.num_documents_with_embeddings == 0
    )

    assert credential_error_logged or all_embeddings_failed or report_shows_failures, (
        f"Credential error should be detected. "
        f"credential_error_logged={credential_error_logged}, "
        f"all_embeddings_failed={all_embeddings_failed}, "
        f"report_shows_failures={report_shows_failures} "
        f"(failures={source_report.num_embedding_failures}, "
        f"successes={source_report.num_documents_with_embeddings})"
    )

    # Verify output file was created (documents may or may not be ingested depending on page content)
    output_file = tmp_path / "notion_cohere_warning.json"
    assert output_file.exists(), "Output file was not created"


def test_notion_test_connection(test_page_complex_content: str) -> None:
    """Test the test_connection capability of the Notion source.

    This validates that test_connection works correctly with real credentials.

    Expected behavior:
        - test_connection succeeds with valid credentials
        - Can access the test page
        - Basic connectivity and page access capabilities are reported
    """
    from datahub.ingestion.source.notion.notion_source import NotionSource

    api_key = os.environ.get("NOTION_API_KEY")
    assert api_key, "NOTION_API_KEY must be set"

    config = {
        "api_key": api_key,
        "page_ids": [test_page_complex_content],
    }

    report = NotionSource.test_connection(config)

    # Verify test connection succeeded
    assert report.basic_connectivity, "Basic connectivity should succeed"
    assert report.basic_connectivity.capable, "Should be able to connect to Notion API"

    # Verify page access capability
    assert report.capability_report, "Capability report should be present"
    page_access = report.capability_report.get("Page/Database Access")
    assert page_access, "Page access capability should be tested"
    assert page_access.capable, "Should be able to access the test page"
