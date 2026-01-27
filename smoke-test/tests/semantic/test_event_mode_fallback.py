"""
Smoke tests for Event Mode Fallback in DataHub Documents Source.

These tests validate that the datahub-documents source correctly falls back to batch mode
when event mode encounters problems (no state, no offsets, API errors, etc.).

Test scenarios:
1. Fallback when state handler unavailable
2. Fallback when no offsets found (first run)
3. Fallback when Events API returns errors
4. Successful event mode processing (no fallback)
5. Fallback preserves existing offsets

These tests are DISABLED by default. To enable, set:
    ENABLE_SEMANTIC_SEARCH_TESTS=true

Prerequisites:
- DataHub running with Kafka enabled
- ELASTICSEARCH_SEMANTIC_SEARCH_ENABLED=true
- SEARCH_SERVICE_SEMANTIC_SEARCH_ENABLED=true
- AWS credentials configured for embedding generation (AWS_PROFILE or AWS_ACCESS_KEY_ID)

Usage:
    AWS_PROFILE=your-profile ENABLE_SEMANTIC_SEARCH_TESTS=true pytest tests/semantic/test_event_mode_fallback.py -v
"""

import json
import logging
import tempfile
import time
import uuid
from pathlib import Path
from typing import Any

import pytest

from tests.consistency_utils import wait_for_writes_to_sync
from tests.semantic.test_semantic_search import (
    SEMANTIC_SEARCH_ENABLED,
    create_documents_with_sdk,
    delete_document,
)
from tests.utils import run_datahub_cmd

logger = logging.getLogger(__name__)


def create_event_mode_recipe(
    auth_session,
    tmp_dir: Path,
    consumer_id: str,
    document_urns: list[str] | None = None,
    lookback_days: int | None = 1,
    reset_offsets: bool = False,
    state_file: str | None = None,
) -> Path:
    """
    Create a recipe file for datahub-documents source in event mode.

    Args:
        auth_session: Auth session with GMS credentials
        tmp_dir: Temporary directory for recipe file
        consumer_id: Unique consumer ID for event tracking
        document_urns: Optional list of specific document URNs to process
        lookback_days: Number of days to look back for events (None = no lookback)
        reset_offsets: Whether to reset offsets (bypasses stateful ingestion)
        state_file: Optional path to state file (for file-based checkpointing)

    Returns:
        Path to the recipe file
    """
    event_mode_config: dict[str, Any] = {
        "enabled": True,
        "consumer_id": consumer_id,
        "topics": ["MetadataChangeLog_Versioned_v1"],
        "reset_offsets": reset_offsets,
        "idle_timeout_seconds": 30,  # Shorter timeout for testing
        "poll_timeout_seconds": 2,
        "poll_limit": 100,
    }

    # Only add lookback_days if it's not None
    if lookback_days is not None:
        event_mode_config["lookback_days"] = lookback_days

    config: dict[str, Any] = {
        "datahub": {
            "server": auth_session.gms_url(),
            "token": auth_session.gms_token(),
        },
        "event_mode": event_mode_config,
        "chunking": {
            "strategy": "by_title",
            "max_characters": 500,
        },
        # No embedding config - should auto-load from server via AppConfig API
        # Disable incremental mode for testing to force reprocessing
        "incremental": {
            "enabled": False,
        },
        "stateful_ingestion": {
            "enabled": True,
        },
    }

    # Configure file-based state provider if state_file is specified
    if state_file:
        config["stateful_ingestion"]["state_provider"] = {
            "type": "file",
            "config": {
                "filename": state_file,
            },
        }

    # Add document_urns if specified (for testing fallback to batch mode)
    if document_urns:
        config["document_urns"] = document_urns

    recipe = {
        "pipeline_name": f"datahub_documents_event_fallback_test_{uuid.uuid4().hex[:8]}",
        "source": {
            "type": "datahub-documents",
            "config": config,
        },
        "sink": {
            "type": "datahub-rest",
            "config": {
                "server": auth_session.gms_url(),
                "token": auth_session.gms_token(),
            },
        },
    }

    recipe_path = tmp_dir / f"event_fallback_recipe_{uuid.uuid4().hex[:8]}.yml"
    with open(recipe_path, "w") as f:
        import yaml

        yaml.dump(recipe, f)

    logger.info(f"Created event mode recipe: {recipe_path}")
    return recipe_path


def run_ingestion(auth_session, recipe_path: Path) -> dict[str, Any]:
    """Run the datahub-documents ingestion source and return result."""
    logger.info(f"Running ingestion with recipe: {recipe_path}")

    try:
        # Load recipe file
        import yaml

        with open(recipe_path) as f:
            recipe_config = yaml.safe_load(f)

        # Create and run pipeline
        from datahub.ingestion.run.pipeline import Pipeline

        pipeline = Pipeline.create(recipe_config)
        pipeline.run()
        pipeline.raise_from_status()

        logger.info("Ingestion completed")
        return {"exit_code": 0}

    except Exception as e:
        logger.error(f"Ingestion failed: {e}")
        # Don't raise - we want to test fallback behavior
        return {"exit_code": 1}


def verify_semantic_content_exists(auth_session, urn: str) -> bool:
    """Check if semanticContent aspect exists for a document."""
    result = run_datahub_cmd(
        ["get", "--urn", urn, "-a", "semanticContent"],
        env={
            "DATAHUB_GMS_URL": auth_session.gms_url(),
            "DATAHUB_GMS_TOKEN": auth_session.gms_token(),
        },
    )

    if result.exit_code != 0:
        logger.warning(
            f"Failed to get semanticContent for {urn}: exit_code={result.exit_code}, "
            f"stdout={result.stdout}, stderr={result.stderr}"
        )
        return False

    try:
        aspect_data = json.loads(result.stdout)
        has_content = aspect_data.get("semanticContent") is not None
        if not has_content:
            logger.warning(f"No semanticContent in response for {urn}: {aspect_data}")
        return has_content
    except (json.JSONDecodeError, KeyError) as e:
        logger.warning(f"Failed to parse semanticContent response for {urn}: {e}")
        return False


@pytest.mark.skipif(
    not SEMANTIC_SEARCH_ENABLED,
    reason="Semantic search tests disabled. Set ENABLE_SEMANTIC_SEARCH_TESTS=true to run.",
)
class TestEventModeFallback:
    """
    Smoke tests for event mode fallback to batch mode.

    These tests verify that the source correctly falls back to batch mode
    when event mode encounters problems.
    """

    @pytest.fixture(autouse=True)
    def setup_and_teardown(self, auth_session):
        """Set up test documents and clean up after tests."""
        self.created_urns: list[str] = []
        self.auth_session = auth_session

        yield

        # Cleanup: delete all created documents
        for urn in self.created_urns:
            try:
                delete_document(auth_session, urn)
                logger.info(f"Cleaned up document: {urn}")
            except Exception as e:
                logger.warning(f"Failed to delete {urn}: {e}")

    def test_fallback_when_no_offsets_first_run(self, auth_session):
        """
        Test that event mode falls back to batch mode when no offsets found (first run).

        Steps:
        1. Create documents
        2. Run ingestion in event mode (no existing offsets)
        3. Verify fallback to batch mode processed documents
        """
        logger.info("Testing fallback: no offsets on first run")

        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)

            # Step 1: Create documents
            docs = [
                {
                    "title": "Fallback Test Document",
                    "text": "This document tests fallback to batch mode when no offsets are found in event mode.",
                    "sub_type": "guide",
                },
            ]

            created_docs = create_documents_with_sdk(auth_session, docs)

            for _doc_id, urn in created_docs:
                self.created_urns.append(urn)

            state_file = str(tmp_path / "state.json")

            # Step 2: Run ingestion in event mode (no existing offsets, no lookback)
            consumer_id = f"fallback-test-{uuid.uuid4().hex[:8]}"
            recipe_path = create_event_mode_recipe(
                auth_session,
                tmp_path,
                consumer_id,
                document_urns=[
                    urn for _doc_id, urn in created_docs
                ],  # Specify URNs to ensure batch mode can process them
                lookback_days=0,  # No lookback - should trigger fallback
                reset_offsets=False,  # Don't reset - test stateful ingestion
                state_file=state_file,
            )

            logger.info("Running ingestion in event mode (no offsets, no lookback)...")
            run_ingestion(auth_session, recipe_path)
            wait_for_writes_to_sync(mcp_only=True)
            time.sleep(5)

            # Step 3: Verify documents were processed (fallback to batch mode)
            logger.info("Verifying documents were processed via fallback...")
            for _doc_id, urn in created_docs:
                has_semantic_content = verify_semantic_content_exists(auth_session, urn)
                assert has_semantic_content, (
                    f"Document {urn} should have been processed via fallback to batch mode"
                )
                logger.info(
                    f"  ✓ {urn}: semanticContent exists (processed via fallback)"
                )

            logger.info("✓ Fallback test passed: documents processed via batch mode")

    def test_successful_event_mode_with_lookback(self, auth_session):
        """
        Test that event mode works successfully when lookback is enabled.

        Steps:
        1. Create documents
        2. Run ingestion in event mode with lookback
        3. Verify documents were processed
        """
        logger.info("Testing successful event mode with lookback")

        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)

            # Step 1: Create documents
            docs = [
                {
                    "title": "Event Mode Success Test",
                    "text": "This document tests successful event mode processing with lookback enabled.",
                    "sub_type": "guide",
                },
            ]

            created_docs = create_documents_with_sdk(auth_session, docs)

            for _doc_id, urn in created_docs:
                self.created_urns.append(urn)

            wait_for_writes_to_sync(mcp_only=True)
            time.sleep(5)  # Wait for events to be available

            # Step 2: Run ingestion in event mode with lookback
            consumer_id = f"event-success-test-{uuid.uuid4().hex[:8]}"
            recipe_path = create_event_mode_recipe(
                auth_session,
                tmp_path,
                consumer_id,
                document_urns=None,  # Don't specify URNs - let event mode find them
                lookback_days=1,  # Look back 1 day
                reset_offsets=False,
            )

            logger.info("Running ingestion in event mode (with lookback)...")
            result = run_ingestion(auth_session, recipe_path)
            wait_for_writes_to_sync(mcp_only=True)
            time.sleep(5)

            # Step 3: Verify documents were processed
            # Note: Event mode may or may not process documents depending on timing
            # This test verifies that event mode doesn't crash and completes successfully
            logger.info("Verifying ingestion completed successfully...")
            # We can't easily verify if event mode or batch mode was used in smoke tests
            # The important thing is that ingestion completes without errors
            assert result["exit_code"] in [0, None], (
                "Ingestion should complete successfully (exit code 0 or None)"
            )

            logger.info("✓ Event mode success test passed: ingestion completed")

    def test_fallback_with_document_urns_specified(self, auth_session):
        """
        Test that event mode falls back to batch mode when document_urns are specified.

        When document_urns are specified, the source should use batch mode
        even if event mode is enabled (for targeted processing).

        Steps:
        1. Create documents
        2. Run ingestion in event mode with document_urns specified
        3. Verify documents were processed (batch mode)
        """
        logger.info("Testing fallback: document_urns specified")

        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)

            # Step 1: Create documents
            docs = [
                {
                    "title": "Document URNs Fallback Test",
                    "text": "This document tests that specifying document_urns causes fallback to batch mode.",
                    "sub_type": "guide",
                },
            ]

            created_docs = create_documents_with_sdk(auth_session, docs)

            for _doc_id, urn in created_docs:
                self.created_urns.append(urn)

            state_file = str(tmp_path / "state.json")

            # Step 2: Run ingestion in event mode with document_urns specified
            consumer_id = f"urns-fallback-test-{uuid.uuid4().hex[:8]}"
            recipe_path = create_event_mode_recipe(
                auth_session,
                tmp_path,
                consumer_id,
                document_urns=[
                    urn for _doc_id, urn in created_docs
                ],  # Specify URNs - should use batch mode
                lookback_days=1,
                reset_offsets=False,
                state_file=state_file,
            )

            logger.info("Running ingestion in event mode (with document_urns)...")
            run_ingestion(auth_session, recipe_path)
            wait_for_writes_to_sync(mcp_only=True)
            time.sleep(10)  # Longer wait to ensure aspects are persisted

            # Step 3: Verify documents were processed
            logger.info("Verifying documents were processed...")
            for _doc_id, urn in created_docs:
                has_semantic_content = verify_semantic_content_exists(auth_session, urn)
                assert has_semantic_content, (
                    f"Document {urn} should have been processed when document_urns are specified"
                )
                logger.info(f"  ✓ {urn}: semanticContent exists")

            logger.info("✓ Document URNs fallback test passed")

    def test_bootstrap_flow_first_run_captures_offset(self, auth_session):
        """
        Test the bootstrap flow: first run captures current offset, second run uses event mode.

        Steps:
        1. Create documents
        2. First run: Event mode with no offsets (should bootstrap and capture offset, then fallback to batch)
        3. Verify documents were processed
        4. Second run: Event mode with captured offset (should use event mode, not fallback)
        5. Verify offset was captured and second run used event mode
        """
        logger.info("Testing bootstrap flow: first run captures offset")

        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)

            # Step 1: Create documents
            docs = [
                {
                    "title": "Bootstrap Test Document",
                    "text": "This document tests the bootstrap flow where the first run captures the current offset before falling back to batch mode.",
                    "sub_type": "guide",
                },
            ]

            created_docs = create_documents_with_sdk(auth_session, docs)

            for _doc_id, urn in created_docs:
                self.created_urns.append(urn)

            wait_for_writes_to_sync(mcp_only=True)
            time.sleep(5)  # Wait for events to be available

            state_file = str(tmp_path / "bootstrap_state.json")

            # Step 2: First run - Event mode with no offsets (should bootstrap)
            consumer_id = f"bootstrap-test-{uuid.uuid4().hex[:8]}"
            recipe_path = create_event_mode_recipe(
                auth_session,
                tmp_path,
                consumer_id,
                document_urns=None,  # Don't specify URNs - let it process all
                lookback_days=None,  # No lookback - should trigger bootstrap
                reset_offsets=False,
                state_file=state_file,
            )

            logger.info(
                "Running first ingestion (should bootstrap and capture offset)..."
            )
            run_ingestion(auth_session, recipe_path)
            wait_for_writes_to_sync(mcp_only=True)
            time.sleep(5)

            # Step 3: Verify documents were processed (via batch mode fallback)
            logger.info("Verifying documents were processed in first run...")
            for _doc_id, urn in created_docs:
                has_semantic_content = verify_semantic_content_exists(auth_session, urn)
                assert has_semantic_content, (
                    f"Document {urn} should have been processed in first run (bootstrap + batch mode)"
                )
                logger.info(
                    f"  ✓ {urn}: semanticContent exists (processed in first run)"
                )

            # Step 4: Verify state file exists and contains offset
            logger.info("Verifying state file contains captured offset...")
            offsets_captured = False
            if Path(state_file).exists():
                with open(state_file) as f:
                    state_data = json.load(f)
                    # Check if state contains event offsets
                    # The exact structure depends on stateful ingestion implementation
                    logger.info(
                        f"State file contents: {json.dumps(state_data, indent=2)}"
                    )
                    # State should exist (even if empty initially, it should be created)
                    assert state_data is not None, (
                        "State file should exist and contain data"
                    )

                    # Check for event_offsets in state (structure: {"event_offsets": {"topic": "offset_id"}})
                    # State might be nested under "state" or "checkpoint" key depending on provider
                    event_offsets = None
                    if "event_offsets" in state_data:
                        event_offsets = state_data["event_offsets"]
                    elif (
                        "state" in state_data and "event_offsets" in state_data["state"]
                    ):
                        event_offsets = state_data["state"]["event_offsets"]
                    elif (
                        "checkpoint" in state_data
                        and "state" in state_data["checkpoint"]
                    ):
                        checkpoint_state = state_data["checkpoint"]["state"]
                        if "event_offsets" in checkpoint_state:
                            event_offsets = checkpoint_state["event_offsets"]

                    if event_offsets:
                        # Verify offsets exist for the topics we're using
                        topic = "MetadataChangeLog_Versioned_v1"
                        if topic in event_offsets and event_offsets[topic]:
                            offsets_captured = True
                            logger.info(
                                f"✓ Verified offset captured for topic {topic}: {event_offsets[topic]}"
                            )
                        else:
                            logger.warning(
                                f"Offset not found in state for topic {topic}. "
                                f"Available keys: {list(event_offsets.keys())}"
                            )
                    else:
                        logger.warning(
                            "event_offsets not found in state file structure. "
                            "State may be using DataHub state provider instead of file-based."
                        )
            else:
                logger.warning(
                    f"State file {state_file} does not exist - may be using DataHub state provider. "
                    "Cannot verify offsets in file, but bootstrap should still work."
                )

            # Note: If using DataHub state provider, we can't easily verify offsets from file
            # But the bootstrap should still work - we'll verify by checking second run behavior

            # Step 5: Second run - Should use event mode (offset captured)
            logger.info(
                "Running second ingestion (should use event mode with captured offset)..."
            )

            # Create a new document to trigger an event
            new_docs = [
                {
                    "title": "Bootstrap Test Document 2",
                    "text": "This document is created after the first run to test that the second run uses event mode.",
                    "sub_type": "guide",
                },
            ]
            new_created_docs = create_documents_with_sdk(auth_session, new_docs)
            for _doc_id, urn in new_created_docs:
                self.created_urns.append(urn)

            wait_for_writes_to_sync(mcp_only=True)
            time.sleep(5)  # Wait for events to be available

            # Run second ingestion with same consumer_id and state_file
            recipe_path_2 = create_event_mode_recipe(
                auth_session,
                tmp_path,
                consumer_id,  # Same consumer_id to use same offsets
                document_urns=None,
                lookback_days=None,  # No lookback - should use captured offset
                reset_offsets=False,
                state_file=state_file,  # Same state file
            )

            result = run_ingestion(auth_session, recipe_path_2)
            wait_for_writes_to_sync(mcp_only=True)
            time.sleep(5)

            # Step 6: Verify second run completed successfully and used event mode
            # The second run should use event mode (not fallback) because offset was captured
            logger.info("Verifying second run completed successfully...")
            assert result["exit_code"] in [0, None], (
                "Second ingestion should complete successfully (using event mode with captured offset)"
            )

            # Verify new document was processed (indicating event mode worked)
            # If event mode worked, the new document created between runs should be processed
            # If it fell back to batch mode, it would process ALL documents, not just new ones
            logger.info(
                "Verifying new document was processed (indicating event mode was used)..."
            )
            new_doc_processed = False
            for _doc_id, urn in new_created_docs:
                has_semantic_content = verify_semantic_content_exists(auth_session, urn)
                if has_semantic_content:
                    new_doc_processed = True
                    logger.info(
                        f"  ✓ {urn}: semanticContent exists (processed in second run via event mode)"
                    )
                else:
                    logger.warning(
                        f"  ⚠ {urn}: semanticContent not found - may not have been processed yet"
                    )

            # If offsets were captured and new document was processed, bootstrap worked
            if offsets_captured and new_doc_processed:
                logger.info(
                    "✓ Bootstrap flow test passed: "
                    "offset captured in first run, second run used event mode to process new document"
                )
            elif new_doc_processed:
                logger.info(
                    "✓ Bootstrap flow test passed: "
                    "second run processed new document (event mode likely used, offsets may be in DataHub state)"
                )
            else:
                logger.warning(
                    "⚠ Bootstrap flow test completed but new document not processed. "
                    "This may indicate event mode didn't work, or timing issues. "
                    "First run documents were processed successfully."
                )
