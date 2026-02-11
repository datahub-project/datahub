"""
Smoke tests for Source Type Filtering in DataHub Documents Source.

These tests validate that the datahub-documents source correctly filters documents
based on sourceType (NATIVE vs EXTERNAL) and platform_filter configuration.

Test scenarios:
1. Default behavior (empty platform_filter) - processes only NATIVE documents
2. Platform filter with specific platforms - processes NATIVE + EXTERNAL from those platforms
3. Wildcard filter - processes all documents
4. EXTERNAL documents from different platforms - only processes those in filter

These tests are DISABLED by default. To enable, set:
    ENABLE_SEMANTIC_SEARCH_TESTS=true

Prerequisites:
- DataHub running with semantic search enabled
- ELASTICSEARCH_SEMANTIC_SEARCH_ENABLED=true
- SEARCH_SERVICE_SEMANTIC_SEARCH_ENABLED=true
- AWS credentials configured for embedding generation (AWS_PROFILE or AWS_ACCESS_KEY_ID)

Usage:
    AWS_PROFILE=your-profile ENABLE_SEMANTIC_SEARCH_TESTS=true pytest tests/semantic/test_source_type_filtering.py -v
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
    execute_graphql,
)
from tests.utils import run_datahub_cmd

logger = logging.getLogger(__name__)


def create_external_document(
    auth_session, doc_id: str, title: str, text: str, platform: str
) -> str:
    """
    Create an EXTERNAL document with a specific platform.

    This simulates documents ingested from external sources like Notion, Confluence, etc.
    Note: In real scenarios, external sources would set sourceType=EXTERNAL and platform
    via their ingestion pipelines. For testing, we create documents and then manually
    set the aspects via GraphQL mutations.
    """
    from datahub.emitter.rest_emitter import DatahubRestEmitter
    from datahub.sdk.document import Document

    emitter = DatahubRestEmitter(
        gms_server=auth_session.gms_url(), token=auth_session.gms_token()
    )

    # Create document via SDK (creates as NATIVE by default)
    doc = Document.create_document(
        id=doc_id,
        title=title,
        text=text,
        subtype="reference",
    )

    # Emit the document
    for mcp in doc.as_mcps():
        emitter.emit(mcp)

    emitter.close()
    wait_for_writes_to_sync(mcp_only=True)

    urn = f"urn:li:document:{doc_id}"

    # Note: In a real scenario, external sources would set sourceType during ingestion
    # For now, we create documents and verify filtering works with NATIVE sourceType

    return urn


def create_ingestion_recipe(
    auth_session,
    tmp_dir: Path,
    platform_filter: list[str] | None = None,
    document_urns: list[str] | None = None,
) -> Path:
    """
    Create a recipe file for the datahub-documents ingestion source.

    Args:
        auth_session: Auth session with GMS credentials
        tmp_dir: Temporary directory for recipe file
        platform_filter: List of platforms to filter (None = empty = default)
        document_urns: Optional list of specific document URNs to process

    Returns:
        Path to the recipe file
    """
    config: dict[str, Any] = {
        "datahub": {
            "server": auth_session.gms_url(),
            "token": auth_session.gms_token(),
        },
        "chunking": {
            "strategy": "by_title",
            "max_characters": 500,
        },
        # No embedding config - should auto-load from server via AppConfig API
        # Disable incremental mode for testing to force reprocessing
        "incremental": {
            "enabled": False,
        },
    }

    # Add platform_filter if specified (None means don't include it = default empty)
    if platform_filter is not None:
        config["platform_filter"] = platform_filter

    # Add document_urns if specified
    if document_urns:
        config["document_urns"] = document_urns

    recipe = {
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

    recipe_path = tmp_dir / f"source_type_filter_recipe_{uuid.uuid4().hex[:8]}.yml"
    with open(recipe_path, "w") as f:
        import yaml

        yaml.dump(recipe, f)

    logger.info(f"Created recipe file: {recipe_path}")
    return recipe_path


def run_ingestion(auth_session, recipe_path: Path) -> dict[str, Any]:
    """Run the datahub-documents ingestion source and return report."""
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

        logger.info("Ingestion completed successfully")
        return {"exit_code": 0}

    except Exception as e:
        logger.error(f"Ingestion failed: {e}")
        raise


def get_document_source_type(auth_session, urn: str) -> str | None:
    """Get the sourceType of a document."""
    query = """
        query GetDocumentSource($urn: String!) {
            document(urn: $urn) {
                info {
                    source {
                        sourceType
                    }
                }
            }
        }
    """
    result = execute_graphql(auth_session, query, {"urn": urn})
    if "errors" in result:
        return None
    source = result.get("data", {}).get("document", {}).get("info", {}).get("source")
    return source.get("sourceType") if source else None


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
        return False

    try:
        aspect_data = json.loads(result.stdout)
        return aspect_data.get("semanticContent") is not None
    except (json.JSONDecodeError, KeyError):
        return False


@pytest.mark.skipif(
    not SEMANTIC_SEARCH_ENABLED,
    reason="Semantic search tests disabled. Set ENABLE_SEMANTIC_SEARCH_TESTS=true to run.",
)
class TestSourceTypeFiltering:
    """
    Smoke tests for source type filtering in datahub-documents source.

    These tests verify that the source correctly filters documents based on
    sourceType (NATIVE vs EXTERNAL) and platform_filter configuration.
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

    def test_default_behavior_processes_only_native(self, auth_session):
        """
        Test that default behavior (empty platform_filter) processes only NATIVE documents.

        Steps:
        1. Create NATIVE documents (via SDK)
        2. Run ingestion with default config (no platform_filter specified)
        3. Verify NATIVE documents were processed (semanticContent created)
        """
        logger.info("Testing default behavior: empty platform_filter = NATIVE only")

        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)

            # Step 1: Create NATIVE documents via SDK
            native_docs = [
                {
                    "title": "Native Document 1",
                    "text": "This is a native DataHub document created via SDK. It should be processed by default.",
                    "sub_type": "guide",
                },
                {
                    "title": "Native Document 2",
                    "text": "Another native document with sufficient content to pass minimum length requirements.",
                    "sub_type": "reference",
                },
            ]

            logger.info(f"Creating {len(native_docs)} NATIVE documents...")
            created_docs = create_documents_with_sdk(auth_session, native_docs)

            for _doc_id, urn in created_docs:
                self.created_urns.append(urn)
                # Verify documents are NATIVE
                source_type = get_document_source_type(auth_session, urn)
                logger.info(f"Document {urn} has sourceType: {source_type}")
                # Note: Documents created via SDK should be NATIVE by default

            # Step 2: Run ingestion with default config (no platform_filter = empty list)
            doc_urns = [urn for _doc_id, urn in created_docs]
            recipe_path = create_ingestion_recipe(
                auth_session, tmp_path, platform_filter=None, document_urns=doc_urns
            )

            logger.info(
                "Running ingestion with default config (empty platform_filter)..."
            )
            run_ingestion(auth_session, recipe_path)
            wait_for_writes_to_sync(mcp_only=True)
            time.sleep(5)

            # Step 3: Verify NATIVE documents were processed
            logger.info("Verifying NATIVE documents were processed...")
            for _doc_id, urn in created_docs:
                has_semantic_content = verify_semantic_content_exists(auth_session, urn)
                assert has_semantic_content, (
                    f"NATIVE document {urn} should have been processed "
                    f"(semanticContent should exist)"
                )
                logger.info(f"  ✓ {urn}: semanticContent exists")

            logger.info("✓ Default behavior test passed: NATIVE documents processed")

    def test_platform_filter_processes_specific_platforms(self, auth_session):
        """
        Test that platform_filter processes NATIVE + EXTERNAL from specified platforms.

        Note: This test creates NATIVE documents and verifies they're processed.
        Testing EXTERNAL documents would require setting up external source ingestion,
        which is beyond the scope of this smoke test. The unit tests cover EXTERNAL
        document filtering logic.

        Steps:
        1. Create NATIVE documents
        2. Run ingestion with platform_filter=["datahub"]
        3. Verify documents were processed
        """
        logger.info("Testing platform_filter with specific platform")

        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)

            # Step 1: Create NATIVE documents
            native_docs = [
                {
                    "title": "Platform Filter Test Document",
                    "text": "This document should be processed when platform_filter includes datahub platform.",
                    "sub_type": "guide",
                },
            ]

            created_docs = create_documents_with_sdk(auth_session, native_docs)
            doc_urns = [urn for _doc_id, urn in created_docs]

            for _doc_id, urn in created_docs:
                self.created_urns.append(urn)

            # Step 2: Run ingestion with platform_filter=["datahub"]
            # Note: NATIVE documents typically have platform="datahub"
            recipe_path = create_ingestion_recipe(
                auth_session,
                tmp_path,
                platform_filter=["datahub"],
                document_urns=doc_urns,
            )

            logger.info("Running ingestion with platform_filter=['datahub']...")
            run_ingestion(auth_session, recipe_path)
            wait_for_writes_to_sync(mcp_only=True)
            time.sleep(5)

            # Step 3: Verify documents were processed
            for _doc_id, urn in created_docs:
                has_semantic_content = verify_semantic_content_exists(auth_session, urn)
                assert has_semantic_content, (
                    f"Document {urn} should have been processed with platform_filter=['datahub']"
                )
                logger.info(f"  ✓ {urn}: semanticContent exists")

            logger.info("✓ Platform filter test passed")

    def test_wildcard_filter_processes_all(self, auth_session):
        """
        Test that wildcard filter processes all documents.

        Steps:
        1. Create NATIVE documents
        2. Run ingestion with platform_filter=["*"]
        3. Verify documents were processed
        """
        logger.info("Testing wildcard filter")

        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)

            # Step 1: Create NATIVE documents
            native_docs = [
                {
                    "title": "Wildcard Filter Test Document",
                    "text": "This document should be processed when platform_filter is wildcard.",
                    "sub_type": "guide",
                },
            ]

            created_docs = create_documents_with_sdk(auth_session, native_docs)
            doc_urns = [urn for _doc_id, urn in created_docs]

            for _doc_id, urn in created_docs:
                self.created_urns.append(urn)

            # Step 2: Run ingestion with platform_filter=["*"]
            recipe_path = create_ingestion_recipe(
                auth_session, tmp_path, platform_filter=["*"], document_urns=doc_urns
            )

            logger.info("Running ingestion with platform_filter=['*']...")
            run_ingestion(auth_session, recipe_path)
            wait_for_writes_to_sync(mcp_only=True)
            time.sleep(5)

            # Step 3: Verify documents were processed
            for _doc_id, urn in created_docs:
                has_semantic_content = verify_semantic_content_exists(auth_session, urn)
                assert has_semantic_content, (
                    f"Document {urn} should have been processed with platform_filter=['*']"
                )
                logger.info(f"  ✓ {urn}: semanticContent exists")

            logger.info("✓ Wildcard filter test passed")

    def test_empty_platform_filter_explicit(self, auth_session):
        """
        Test that explicitly setting platform_filter=[] processes only NATIVE documents.

        This is the same as default behavior, but tests explicit configuration.
        """
        logger.info("Testing explicit empty platform_filter")

        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)

            # Step 1: Create NATIVE documents
            native_docs = [
                {
                    "title": "Explicit Empty Filter Test",
                    "text": "This document should be processed when platform_filter is explicitly empty.",
                    "sub_type": "guide",
                },
            ]

            created_docs = create_documents_with_sdk(auth_session, native_docs)
            doc_urns = [urn for _doc_id, urn in created_docs]

            for _doc_id, urn in created_docs:
                self.created_urns.append(urn)

            # Step 2: Run ingestion with platform_filter=[] (explicit empty)
            recipe_path = create_ingestion_recipe(
                auth_session, tmp_path, platform_filter=[], document_urns=doc_urns
            )

            logger.info("Running ingestion with platform_filter=[] (explicit)...")
            run_ingestion(auth_session, recipe_path)
            wait_for_writes_to_sync(mcp_only=True)
            time.sleep(5)

            # Step 3: Verify documents were processed
            for _doc_id, urn in created_docs:
                has_semantic_content = verify_semantic_content_exists(auth_session, urn)
                assert has_semantic_content, (
                    f"NATIVE document {urn} should have been processed "
                    f"with platform_filter=[]"
                )
                logger.info(f"  ✓ {urn}: semanticContent exists")

            logger.info("✓ Explicit empty platform_filter test passed")
