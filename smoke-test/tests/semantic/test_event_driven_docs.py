"""
Smoke tests for Event-Driven Document Ingestion.

These tests validate that the datahub-documents source can react to document changes
in real-time using Kafka events (MetadataChangeLog_Versioned_v1).

Test flow:
1. Create a document using the Document SDK
2. Start datahub-documents source in event mode (background process)
3. Update the document content (triggers MCL event)
4. Verify the source picks up the change and reprocesses
5. Check that semanticContent aspect was updated with new chunks

These tests are DISABLED by default. To enable, set:
    ENABLE_SEMANTIC_SEARCH_TESTS=true

Prerequisites:
- DataHub running with Kafka enabled
- ELASTICSEARCH_SEMANTIC_SEARCH_ENABLED=true
- SEARCH_SERVICE_SEMANTIC_SEARCH_ENABLED=true
- AWS credentials configured for embedding generation (AWS_PROFILE or AWS_ACCESS_KEY_ID)

Usage:
    # Run with semantic search tests enabled
    AWS_PROFILE=your-profile ENABLE_SEMANTIC_SEARCH_TESTS=true pytest tests/semantic/test_event_driven_docs.py -v
"""

import json
import logging
import os
import subprocess
import sys
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

# Test gating - disabled by default (reuse same flag as semantic search tests)
# Incremental wait times for event processing
EVENT_PROCESSING_WAIT_SECONDS = 15
MAX_WAIT_FOR_UPDATE_SECONDS = 60


def get_semantic_content(auth_session, urn: str) -> dict[str, Any] | None:
    """Get semanticContent aspect for a document URN.

    Returns:
        Dictionary of semanticContent aspect, or None if not found
    """
    result = run_datahub_cmd(
        ["get", "--urn", urn, "-a", "semanticContent"],
        env={
            "DATAHUB_GMS_URL": auth_session.gms_url(),
            "DATAHUB_GMS_TOKEN": auth_session.gms_token(),
        },
    )

    if result.exit_code != 0:
        return None

    try:
        aspect_data = json.loads(result.stdout)
        return aspect_data.get("semanticContent")
    except (json.JSONDecodeError, KeyError):
        return None


def extract_chunk_texts(semantic_content: dict[str, Any]) -> list[str]:
    """Extract all chunk text content from semanticContent aspect.

    Args:
        semantic_content: The semanticContent aspect dict

    Returns:
        List of chunk text strings
    """
    if not semantic_content:
        return []

    embeddings = semantic_content.get("embeddings", {})
    cohere_embed = embeddings.get("cohere_embed_v3", {})
    chunks = cohere_embed.get("chunks", [])

    return [chunk.get("text", "") for chunk in chunks]


def create_event_mode_recipe(auth_session, pipeline_name: str, tmp_dir: Path) -> Path:
    """Create a recipe file for datahub-documents source in event mode.

    Args:
        auth_session: Auth session with GMS credentials
        pipeline_name: Unique pipeline name for consumer tracking
        tmp_dir: Temporary directory for recipe file

    Returns:
        Path to the recipe file
    """
    recipe = {
        "source": {
            "type": "datahub-documents",
            "config": {
                "datahub": {
                    "server": auth_session.gms_url(),
                    "token": auth_session.gms_token(),
                },
                # Event-driven mode configuration
                "event_mode": {
                    "enabled": True,
                    "consumer_id": f"test-event-driven-{pipeline_name}",
                    "topics": ["MetadataChangeLog_Versioned_v1"],
                    "lookback_days": 1,  # Look back 1 day to catch our test events
                    "reset_offsets": False,  # Use stateful ingestion (don't reset)
                    "idle_timeout_seconds": 60,  # Exit after 60s of no events
                    "poll_timeout_seconds": 2,
                    "poll_limit": 100,
                },
                # Stateful ingestion (enabled by default, but explicit for clarity)
                "stateful_ingestion": {
                    "enabled": True,
                },
                # Chunking configuration
                "chunking": {
                    "strategy": "by_title",
                    "max_characters": 500,
                },
                # No embedding config - should auto-load from server via AppConfig API
                # Disable incremental mode for testing to force reprocessing
                "incremental": {
                    "enabled": False,
                },
            },
        },
        "sink": {
            "type": "datahub-rest",
            "config": {
                "server": auth_session.gms_url(),
                "token": auth_session.gms_token(),
            },
        },
    }

    recipe_path = tmp_dir / f"event_mode_recipe_{pipeline_name}.yml"
    with open(recipe_path, "w") as f:
        import yaml

        yaml.dump(recipe, f)

    logger.info(f"Created event mode recipe: {recipe_path}")
    return recipe_path


@pytest.mark.skipif(
    not SEMANTIC_SEARCH_ENABLED,
    reason="Semantic search tests disabled. Set ENABLE_SEMANTIC_SEARCH_TESTS=true to run.",
)
class TestEventDrivenDocumentIngestion:
    """
    Event-driven document ingestion smoke tests.

    These tests verify that the datahub-documents source can react to document
    changes in real-time by consuming Kafka events.
    """

    @pytest.fixture(autouse=True)
    def setup_and_teardown(self, auth_session):
        """Set up test documents and clean up after tests."""
        self.created_urns: list[str] = []
        self.auth_session = auth_session
        self.background_processes: list[subprocess.Popen] = []

        yield

        # Cleanup: kill background processes
        for proc in self.background_processes:
            try:
                logger.info(f"Terminating background process PID={proc.pid}")
                proc.terminate()
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                logger.warning(f"Force killing process PID={proc.pid}")
                proc.kill()
            except Exception as e:
                logger.warning(f"Failed to terminate process: {e}")

        # Cleanup: delete all created documents
        for urn in self.created_urns:
            try:
                delete_document(auth_session, urn)
                logger.info(f"Cleaned up document: {urn}")
            except Exception as e:
                logger.warning(f"Failed to delete {urn}: {e}")

    def test_event_driven_document_update(self, auth_session):
        """
        Test that datahub-documents source reacts to document updates via Kafka events.

        Steps:
        1. Create a document with initial content
        2. Verify initial semanticContent exists
        3. Start datahub-documents source in event mode (background)
        4. Update the document with new content
        5. Wait for event to be processed
        6. Verify semanticContent was updated with new chunks
        """
        logger.info("Starting event-driven document update test")

        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)

            # Step 1: Create initial document
            logger.info("Creating initial document with SDK...")
            initial_docs = [
                {
                    "title": "Event Test Document",
                    "text": """Event Test Document

Initial Content Section

This is the initial content of the document.
We will update this content to test event-driven processing.
The source should detect the change and regenerate embeddings.
""",
                    "sub_type": "test",
                }
            ]

            created_docs = create_documents_with_sdk(auth_session, initial_docs)
            doc_id, urn = created_docs[0]
            self.created_urns.append(urn)

            logger.info(f"Created document: {urn}")

            # Wait for initial document to be indexed
            wait_for_writes_to_sync(mcp_only=True)
            time.sleep(5)

            # Step 2: Run initial ingestion to create baseline semanticContent
            logger.info("Running initial batch ingestion...")
            initial_recipe = {
                "source": {
                    "type": "datahub-documents",
                    "config": {
                        "datahub": {
                            "server": auth_session.gms_url(),
                            "token": auth_session.gms_token(),
                        },
                        "document_urns": [urn],
                        "platform_filter": [
                            "*"
                        ],  # Process all documents regardless of platform
                        "chunking": {
                            "strategy": "by_title",
                            "max_characters": 500,
                        },
                        # No embedding config - should auto-load from server via AppConfig API
                        # Disable incremental mode for testing to force reprocessing
                        "incremental": {
                            "enabled": False,
                        },
                    },
                },
                "sink": {
                    "type": "datahub-rest",
                    "config": {
                        "server": auth_session.gms_url(),
                        "token": auth_session.gms_token(),
                    },
                },
            }

            # Run initial ingestion using Pipeline directly
            from datahub.ingestion.run.pipeline import Pipeline

            try:
                pipeline = Pipeline.create(initial_recipe)
                pipeline.run()
                pipeline.raise_from_status()
                logger.info("Initial ingestion completed")
            except Exception as e:
                pytest.fail(f"Initial ingestion failed: {e}")
            wait_for_writes_to_sync(mcp_only=True)
            time.sleep(5)

            # Verify initial semanticContent exists
            initial_semantic = get_semantic_content(auth_session, urn)
            assert initial_semantic is not None, "Initial semanticContent not created"

            initial_chunks = extract_chunk_texts(initial_semantic)
            logger.info(
                f"Initial semanticContent has {len(initial_chunks)} chunks: {initial_chunks}"
            )
            assert len(initial_chunks) > 0, "No chunks in initial semanticContent"

            # Verify initial content appears in chunks
            initial_content_found = any(
                "initial content" in chunk.lower() for chunk in initial_chunks
            )
            assert initial_content_found, (
                "Initial content not found in semanticContent chunks"
            )

            # Step 3: Start datahub-documents source in event mode (background)
            pipeline_name = f"event-test-{uuid.uuid4().hex[:8]}"
            event_recipe_path = create_event_mode_recipe(
                auth_session, pipeline_name, tmp_path
            )

            logger.info("Starting datahub-documents source in event mode...")
            log_file = tmp_path / "event_source.log"

            # Use the venv's datahub command explicitly
            datahub_cmd = str(Path(sys.executable).parent / "datahub")

            proc = subprocess.Popen(
                [
                    datahub_cmd,
                    "ingest",
                    "-c",
                    str(event_recipe_path),
                ],
                env={
                    "DATAHUB_GMS_URL": auth_session.gms_url(),
                    "DATAHUB_GMS_TOKEN": auth_session.gms_token(),
                    **os.environ,  # Include AWS credentials
                },
                stdout=open(log_file, "w"),
                stderr=subprocess.STDOUT,
            )
            self.background_processes.append(proc)

            logger.info(
                f"Background process started (PID={proc.pid}), logs: {log_file}"
            )

            # Wait for source to start consuming
            time.sleep(10)

            # Check if process is still running
            if proc.poll() is not None:
                with open(log_file) as f:
                    logs = f.read()
                pytest.fail(
                    f"Event source process died unexpectedly (exit code {proc.poll()}). Logs:\n{logs}"
                )

            # Step 4: Update the document with new content
            logger.info("Updating document with new content...")

            from datahub.emitter.rest_emitter import DatahubRestEmitter
            from datahub.sdk.document import Document

            emitter = DatahubRestEmitter(
                gms_server=auth_session.gms_url(), token=auth_session.gms_token()
            )

            updated_doc = Document.create_document(
                id=doc_id,
                title="Event Test Document",
                text="""Event Test Document

Updated Content Section

This is the UPDATED content of the document.
The event-driven source should detect this change.
New embeddings should be generated for this updated text.

Additional Information

This section contains extra information that wasn't in the original.
It proves that the document was successfully updated and reprocessed.
""",
                subtype="test",
            )

            # Emit the updated document (triggers MCL event)
            for mcp in updated_doc.as_mcps():
                emitter.emit(mcp)

            emitter.close()
            logger.info("Document update emitted")
            wait_for_writes_to_sync(mcp_only=True)

            # Step 5: Wait for event to be processed
            logger.info(
                f"Waiting up to {MAX_WAIT_FOR_UPDATE_SECONDS}s for event processing..."
            )

            updated_semantic = None
            updated_chunks = []
            update_detected = False

            for attempt in range(MAX_WAIT_FOR_UPDATE_SECONDS // 5):
                time.sleep(5)

                # Check process is still alive
                if proc.poll() is not None:
                    logger.info(
                        f"Event source process exited (exit code {proc.poll()}). Waiting for writes to flush..."
                    )
                    # Process has exited, give the sink time to flush buffered writes
                    wait_for_writes_to_sync(mcp_only=True)
                    time.sleep(5)
                    break

                # Check for updated semanticContent
                updated_semantic = get_semantic_content(auth_session, urn)
                if updated_semantic:
                    updated_chunks = extract_chunk_texts(updated_semantic)

                    # Check if updated content appears in chunks
                    update_detected = any(
                        "updated content" in chunk.lower() for chunk in updated_chunks
                    )

                    if update_detected:
                        logger.info(
                            f"Update detected after {(attempt + 1) * 5}s! New chunks: {updated_chunks}"
                        )
                        break
                    else:
                        logger.info(
                            f"Attempt {attempt + 1}: Updated content not yet in chunks"
                        )

            # If process exited without detecting update, check one more time
            if not update_detected and proc.poll() is not None:
                logger.info("Process exited, checking final state...")
                updated_semantic = get_semantic_content(auth_session, urn)
                if updated_semantic:
                    updated_chunks = extract_chunk_texts(updated_semantic)
                    update_detected = any(
                        "updated content" in chunk.lower() for chunk in updated_chunks
                    )
                    if update_detected:
                        logger.info(
                            f"Update found after process exit! Chunks: {updated_chunks}"
                        )

            # Step 6: Verify semanticContent was updated
            assert updated_semantic is not None, "Updated semanticContent not found"
            assert len(updated_chunks) > 0, "No chunks in updated semanticContent"
            assert update_detected, (
                f"Updated content not found in chunks. Chunks: {updated_chunks}"
            )

            # Verify old content is gone
            old_content_found = any(
                "initial content" in chunk.lower() for chunk in updated_chunks
            )
            assert not old_content_found, "Old content still present in updated chunks"

            # Verify new content is present
            new_content_found = any(
                "additional information" in chunk.lower() for chunk in updated_chunks
            )
            assert new_content_found, "New content section not found in updated chunks"

            logger.info("✓ Event-driven document update test passed!")

            # Print logs for debugging
            with open(log_file) as f:
                logs = f.read()
                logger.info(f"Event source logs:\n{logs}")
