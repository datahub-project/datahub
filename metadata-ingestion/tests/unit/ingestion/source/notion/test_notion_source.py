"""Tests for NotionSource."""

from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from pydantic import SecretStr

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.notion.notion_config import NotionSourceConfig
from datahub.ingestion.source.notion.notion_report import NotionSourceReport
from datahub.ingestion.source.notion.notion_source import NotionSource
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
)


@pytest.fixture
def config():
    return NotionSourceConfig(
        api_key=SecretStr("secret_test_key"),
        page_ids=["2bffc6a6-4277-8024-97c9-d0f26faa4480"],
        # Bypass server validation in tests with valid test config
        embedding={
            "provider": "bedrock",
            "model": "cohere.embed-english-v3",
            "aws_region": "us-west-2",
            "allow_local_embedding_config": True,
        },
    )


@pytest.fixture
def pipeline_context():
    return PipelineContext(run_id="test_run")


@pytest.fixture
def notion_source(config, pipeline_context):
    return NotionSource(config=config, ctx=pipeline_context)


def test_source_initialization(notion_source):
    assert notion_source.config is not None
    assert notion_source.report is not None
    assert notion_source.document_builder is not None


def test_extract_notion_url():
    config = NotionSourceConfig(
        api_key=SecretStr("secret_test_key"),
        page_ids=["2bffc6a6-4277-8024-97c9-d0f26faa4480"],
        embedding={
            "provider": "bedrock",
            "model": "cohere.embed-english-v3",
            "aws_region": "us-west-2",
            "allow_local_embedding_config": True,
        },
    )
    ctx = PipelineContext(run_id="test")
    source = NotionSource(config=config, ctx=ctx)

    # Setup parent metadata
    source.notion_parent_metadata = {
        "test-page-id": {"url": "https://www.notion.so/Test-Page-123"}
    }

    # Test metadata with page_id
    metadata = {"data_source": {"record_locator": {"page_id": "test-page-id"}}}

    url = source._extract_notion_url(metadata)
    assert url == "https://www.notion.so/Test-Page-123"


def test_extract_notion_url_missing_page_id():
    config = NotionSourceConfig(
        api_key=SecretStr("secret_test_key"),
        page_ids=["2bffc6a6-4277-8024-97c9-d0f26faa4480"],
        embedding={
            "provider": "bedrock",
            "model": "cohere.embed-english-v3",
            "aws_region": "us-west-2",
            "allow_local_embedding_config": True,
        },
    )
    ctx = PipelineContext(run_id="test")
    source = NotionSource(config=config, ctx=ctx)

    metadata: dict[str, Any] = {"data_source": {"record_locator": {}}}

    url = source._extract_notion_url(metadata)
    assert url is None


def test_extract_notion_url_not_in_map():
    config = NotionSourceConfig(
        api_key=SecretStr("secret_test_key"),
        page_ids=["2bffc6a6-4277-8024-97c9-d0f26faa4480"],
        embedding={
            "provider": "bedrock",
            "model": "cohere.embed-english-v3",
            "aws_region": "us-west-2",
            "allow_local_embedding_config": True,
        },
    )
    ctx = PipelineContext(run_id="test")
    source = NotionSource(config=config, ctx=ctx)

    source.notion_parent_metadata = {}

    metadata = {"data_source": {"record_locator": {"page_id": "unknown-page-id"}}}

    url = source._extract_notion_url(metadata)
    # URL is now constructed from page_id even if not in metadata map
    assert url == "https://www.notion.so/unknownpageid"


def test_extract_notion_parent_urn_page_parent():
    config = NotionSourceConfig(
        api_key=SecretStr("secret_test_key"),
        page_ids=["2bffc6a6-4277-8024-97c9-d0f26faa4480"],
        embedding={
            "provider": "bedrock",
            "model": "cohere.embed-english-v3",
            "aws_region": "us-west-2",
            "allow_local_embedding_config": True,
        },
    )
    ctx = PipelineContext(run_id="test")
    source = NotionSource(config=config, ctx=ctx)

    # Setup parent metadata
    source.notion_parent_metadata = {
        "child-page-id": {"parent": {"type": "page_id", "page_id": "parent-page-id"}}
    }

    metadata = {"data_source": {"record_locator": {"page_id": "child-page-id"}}}

    parent_urn = source._extract_notion_parent_urn([], metadata)
    assert parent_urn is not None
    assert "urn:li:document:" in parent_urn


def test_extract_notion_parent_urn_database_parent():
    config = NotionSourceConfig(
        api_key=SecretStr("secret_test_key"),
        page_ids=["2bffc6a6-4277-8024-97c9-d0f26faa4480"],
        embedding={
            "provider": "bedrock",
            "model": "cohere.embed-english-v3",
            "aws_region": "us-west-2",
            "allow_local_embedding_config": True,
        },
    )
    ctx = PipelineContext(run_id="test")
    source = NotionSource(config=config, ctx=ctx)

    # Setup parent metadata
    source.notion_parent_metadata = {
        "child-page-id": {
            "parent": {"type": "database_id", "database_id": "parent-db-id"}
        }
    }

    metadata = {"data_source": {"record_locator": {"page_id": "child-page-id"}}}

    parent_urn = source._extract_notion_parent_urn([], metadata)
    assert parent_urn is not None
    assert "urn:li:document:" in parent_urn


def test_extract_notion_parent_urn_workspace_parent():
    config = NotionSourceConfig(
        api_key=SecretStr("secret_test_key"),
        page_ids=["2bffc6a6-4277-8024-97c9-d0f26faa4480"],
        embedding={
            "provider": "bedrock",
            "model": "cohere.embed-english-v3",
            "aws_region": "us-west-2",
            "allow_local_embedding_config": True,
        },
    )
    ctx = PipelineContext(run_id="test")
    source = NotionSource(config=config, ctx=ctx)

    # Setup parent metadata
    source.notion_parent_metadata = {
        "child-page-id": {"parent": {"type": "workspace", "workspace": True}}
    }

    metadata = {"data_source": {"record_locator": {"page_id": "child-page-id"}}}

    parent_urn = source._extract_notion_parent_urn([], metadata)
    assert parent_urn is None


def test_extract_notion_parent_urn_no_parent():
    config = NotionSourceConfig(
        api_key=SecretStr("secret_test_key"),
        page_ids=["2bffc6a6-4277-8024-97c9-d0f26faa4480"],
        embedding={
            "provider": "bedrock",
            "model": "cohere.embed-english-v3",
            "aws_region": "us-west-2",
            "allow_local_embedding_config": True,
        },
    )
    ctx = PipelineContext(run_id="test")
    source = NotionSource(config=config, ctx=ctx)

    source.notion_parent_metadata = {}

    metadata = {"data_source": {"record_locator": {"page_id": "unknown-page-id"}}}

    parent_urn = source._extract_notion_parent_urn([], metadata)
    assert parent_urn is None


def test_should_skip_file_page_id_filter_match():
    config = NotionSourceConfig(
        api_key=SecretStr("secret_test_key"),
        page_ids=["2bffc6a6-4277-8024-97c9-d0f26faa4480"],
        embedding={
            "provider": "bedrock",
            "model": "cohere.embed-english-v3",
            "aws_region": "us-west-2",
            "allow_local_embedding_config": True,
        },
    )
    ctx = PipelineContext(run_id="test")
    source = NotionSource(config=config, ctx=ctx)

    # Use a page_id that matches the exact configured ID (will match prefix)
    data = {
        "elements": [
            {
                "text": "This is some longer content that definitely passes the minimum length requirement for filtering"
            }
        ],
        "metadata": {
            "data_source": {
                "record_locator": {"page_id": "2bffc6a6-4277-8024-97c9-d0f26faa4480"}
            }
        },
    }

    # Should NOT skip because prefix matches exactly and has enough content
    assert source._should_skip_file(data, set()) is False


def test_should_skip_file_page_id_filter_no_match():
    config = NotionSourceConfig(
        api_key=SecretStr("secret_test_key"),
        page_ids=["2bffc6a6-4277-8024-97c9-d0f26faa4480"],
        embedding={
            "provider": "bedrock",
            "model": "cohere.embed-english-v3",
            "aws_region": "us-west-2",
            "allow_local_embedding_config": True,
        },
    )
    ctx = PipelineContext(run_id="test")
    source = NotionSource(config=config, ctx=ctx)

    data = {
        "elements": [{"text": "Some content"}],
        "metadata": {
            "data_source": {
                "record_locator": {"page_id": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"}
            }
        },
    }

    assert source._should_skip_file(data, set()) is True


def test_should_skip_file_empty_document():
    config = NotionSourceConfig(
        api_key=SecretStr("secret_test_key"),
        database_ids=["abcdef01234567890123456789012345"],
        embedding={
            "provider": "bedrock",
            "model": "cohere.embed-english-v3",
            "aws_region": "us-west-2",
            "allow_local_embedding_config": True,
        },
    )
    ctx = PipelineContext(run_id="test")
    source = NotionSource(config=config, ctx=ctx)

    data: dict[str, Any] = {"elements": [], "metadata": {}}

    assert source._should_skip_file(data, set()) is True


def test_should_skip_file_text_too_short():
    config = NotionSourceConfig(
        api_key=SecretStr("secret_test_key"),
        database_ids=["abcdef01234567890123456789012345"],
        embedding={
            "provider": "bedrock",
            "model": "cohere.embed-english-v3",
            "aws_region": "us-west-2",
            "allow_local_embedding_config": True,
        },
    )
    ctx = PipelineContext(run_id="test")
    source = NotionSource(config=config, ctx=ctx)

    data = {"elements": [{"text": "Short"}], "metadata": {}}

    assert source._should_skip_file(data, set()) is True


def test_should_skip_file_text_long_enough():
    config = NotionSourceConfig(
        api_key=SecretStr("secret_test_key"),
        database_ids=["abcdef01234567890123456789012345"],
        embedding={
            "provider": "bedrock",
            "model": "cohere.embed-english-v3",
            "aws_region": "us-west-2",
            "allow_local_embedding_config": True,
        },
    )
    ctx = PipelineContext(run_id="test")
    source = NotionSource(config=config, ctx=ctx)

    data = {
        "elements": [
            {
                "text": "This is a longer piece of text that should pass the minimum length check"
            }
        ],
        "metadata": {},
    }

    assert source._should_skip_file(data, set()) is False


# Stateful Ingestion Tests


def test_stateful_ingestion_handler_initialized(notion_source):
    """Verify that StaleEntityRemovalHandler is initialized."""
    assert hasattr(notion_source, "stale_entity_removal_handler")
    assert isinstance(
        notion_source.stale_entity_removal_handler, StaleEntityRemovalHandler
    )


def test_report_is_notion_source_report(notion_source):
    """Verify report is NotionSourceReport with both stateful and file tracking fields."""
    assert isinstance(notion_source.report, NotionSourceReport)
    # Check stateful ingestion report fields exist
    assert hasattr(notion_source.report, "soft_deleted_stale_entities")
    assert hasattr(notion_source.report, "last_state_non_deletable_entities")
    # Check file tracking fields exist
    assert hasattr(notion_source.report, "num_files_scanned")
    assert hasattr(notion_source.report, "num_files_processed")
    assert hasattr(notion_source.report, "report_file_skipped")


def test_source_platform_attribute():
    """Verify platform attribute is set for checkpoint job_id generation."""
    config = NotionSourceConfig(
        api_key=SecretStr("secret_test_key"),
        page_ids=["2bffc6a6-4277-8024-97c9-d0f26faa4480"],
        embedding={
            "provider": "bedrock",
            "model": "cohere.embed-english-v3",
            "aws_region": "us-west-2",
            "allow_local_embedding_config": True,
        },
    )
    ctx = PipelineContext(run_id="test")
    source = NotionSource(config=config, ctx=ctx)

    assert source.platform == "notion"


def test_extract_notion_parent_urn_with_ingested_pages():
    """Test that parent URNs are only emitted for pages that are being ingested."""
    config = NotionSourceConfig(
        api_key=SecretStr("secret_test_key"),
        page_ids=["2bffc6a6-4277-8024-97c9-d0f26faa4480"],
        embedding={
            "provider": "bedrock",
            "model": "cohere.embed-english-v3",
            "aws_region": "us-west-2",
            "allow_local_embedding_config": True,
        },
    )
    ctx = PipelineContext(run_id="test")
    source = NotionSource(config=config, ctx=ctx)

    # Setup parent metadata
    source.notion_parent_metadata = {
        "child-page-id": {"parent": {"type": "page_id", "page_id": "parent-page-id"}}
    }

    metadata = {"data_source": {"record_locator": {"page_id": "child-page-id"}}}

    # Test 1: Parent page is in ingested set - should return URN
    ingested_page_ids = {"parent-page-id", "child-page-id"}
    parent_urn = source._extract_notion_parent_urn([], metadata, ingested_page_ids)
    assert parent_urn is not None
    assert "urn:li:document:" in parent_urn

    # Test 2: Parent page is NOT in ingested set - should return None
    ingested_page_ids_without_parent = {"child-page-id"}
    parent_urn = source._extract_notion_parent_urn(
        [], metadata, ingested_page_ids_without_parent
    )
    assert parent_urn is None


def test_stateful_ingestion_config_disabled_by_default():
    """Test that stateful ingestion is disabled by default (no config set)."""
    config = NotionSourceConfig(
        api_key=SecretStr("secret_test_key"),
        page_ids=["2bffc6a6-4277-8024-97c9-d0f26faa4480"],
        embedding={
            "provider": "bedrock",
            "model": "cohere.embed-english-v3",
            "aws_region": "us-west-2",
            "allow_local_embedding_config": True,
        },
    )

    # Stateful ingestion is not configured (None or enabled=False)
    assert (
        config.stateful_ingestion is None or config.stateful_ingestion.enabled is False
    )


def test_stateful_ingestion_config_settings():
    """Test that stateful ingestion config can be parsed with correct settings."""
    config = NotionSourceConfig(
        api_key=SecretStr("secret_test_key"),
        page_ids=["2bffc6a6-4277-8024-97c9-d0f26faa4480"],
        embedding={
            "provider": "bedrock",
            "model": "cohere.embed-english-v3",
            "aws_region": "us-west-2",
            "allow_local_embedding_config": True,
        },
        stateful_ingestion={
            "enabled": True,
            "remove_stale_metadata": True,
            "fail_safe_threshold": 75.0,
        },
    )

    # Verify stateful ingestion config is parsed correctly
    assert config.stateful_ingestion is not None
    assert config.stateful_ingestion.enabled is True
    assert config.stateful_ingestion.remove_stale_metadata is True
    assert config.stateful_ingestion.fail_safe_threshold == 75.0


# Content-based Change Detection Tests


def test_get_processing_config_fingerprint():
    """Test that processing config fingerprint includes all relevant settings."""
    config = NotionSourceConfig(
        api_key=SecretStr("secret_test_key"),
        page_ids=["2bffc6a6-4277-8024-97c9-d0f26faa4480"],
        chunking={"strategy": "by_title", "max_characters": 500},
        embedding={
            "provider": "bedrock",
            "model": "cohere.embed-english-v3",
            "aws_region": "us-west-2",
            "allow_local_embedding_config": True,
        },
    )
    ctx = PipelineContext(run_id="test")
    source = NotionSource(config=config, ctx=ctx)

    fingerprint = source._get_processing_config_fingerprint()

    # Verify all config elements are included
    assert fingerprint["chunking_enabled"] is True
    assert fingerprint["chunking_strategy"] == "by_title"
    assert fingerprint["chunking_max_characters"] == 500
    assert fingerprint["embedding_enabled"] is True
    assert fingerprint["embedding_provider"] == "bedrock"
    assert fingerprint["embedding_model"] == "cohere.embed-english-v3"
    assert "partition_strategy" in fingerprint
    assert "hierarchy_enabled" in fingerprint


def test_get_processing_config_fingerprint_no_chunking():
    """Test config fingerprint when chunking is disabled (no embedding provider)."""
    config = NotionSourceConfig(
        api_key=SecretStr("secret_test_key"),
        page_ids=["2bffc6a6-4277-8024-97c9-d0f26faa4480"],
        # No embedding provider = chunking/embedding disabled
        embedding={
            "allow_local_embedding_config": True,
        },
    )
    ctx = PipelineContext(run_id="test")
    source = NotionSource(config=config, ctx=ctx)

    fingerprint = source._get_processing_config_fingerprint()

    # When chunking disabled, chunking/embedding settings should be None
    assert fingerprint["chunking_enabled"] is False
    assert fingerprint["chunking_strategy"] is None
    assert fingerprint["embedding_enabled"] is False
    assert fingerprint["embedding_provider"] is None


def test_calculate_document_hash_includes_config():
    """Test that document hash includes both content and config."""
    config = NotionSourceConfig(
        api_key=SecretStr("secret_test_key"),
        page_ids=["2bffc6a6-4277-8024-97c9-d0f26faa4480"],
        chunking={"strategy": "by_title"},
        embedding={
            "provider": "bedrock",
            "model": "cohere.embed-english-v3",
            "aws_region": "us-west-2",
            "allow_local_embedding_config": True,
        },
    )
    ctx = PipelineContext(run_id="test")
    source = NotionSource(config=config, ctx=ctx)

    text = "Test document content"
    page_id = "test-page-id"

    hash1 = source._calculate_document_hash(text, page_id)

    # Hash should be deterministic
    hash2 = source._calculate_document_hash(text, page_id)
    assert hash1 == hash2

    # Hash should be valid SHA256 (64 hex characters)
    assert len(hash1) == 64
    assert all(c in "0123456789abcdef" for c in hash1)


def test_calculate_document_hash_changes_with_content():
    """Test that hash changes when content changes."""
    config = NotionSourceConfig(
        api_key=SecretStr("secret_test_key"),
        page_ids=["2bffc6a6-4277-8024-97c9-d0f26faa4480"],
        embedding={
            "provider": "bedrock",
            "model": "cohere.embed-english-v3",
            "aws_region": "us-west-2",
            "allow_local_embedding_config": True,
        },
    )
    ctx = PipelineContext(run_id="test")
    source = NotionSource(config=config, ctx=ctx)

    hash1 = source._calculate_document_hash("Content A", "page-1")
    hash2 = source._calculate_document_hash("Content B", "page-1")

    assert hash1 != hash2


def test_calculate_document_hash_changes_with_config():
    """Test that hash changes when processing config changes."""
    # Config without embedding (chunking disabled)
    config1 = NotionSourceConfig(
        api_key=SecretStr("secret_test_key"),
        page_ids=["2bffc6a6-4277-8024-97c9-d0f26faa4480"],
        embedding={
            "allow_local_embedding_config": True,
        },
    )
    ctx = PipelineContext(run_id="test")
    source1 = NotionSource(config=config1, ctx=ctx)

    # Same config but with embedding provider (chunking enabled)
    config2 = NotionSourceConfig(
        api_key=SecretStr("secret_test_key"),
        page_ids=["2bffc6a6-4277-8024-97c9-d0f26faa4480"],
        chunking={"strategy": "by_title"},
        embedding={
            "provider": "bedrock",
            "model": "cohere.embed-english-v3",
            "aws_region": "us-west-2",
            "allow_local_embedding_config": True,
        },
    )
    source2 = NotionSource(config=config2, ctx=ctx)

    text = "Same content"
    page_id = "same-page"

    hash1 = source1._calculate_document_hash(text, page_id)
    hash2 = source2._calculate_document_hash(text, page_id)

    # Hash should differ because config changed
    assert hash1 != hash2


def test_should_process_document_new_document():
    """Test that new documents are processed."""
    config = NotionSourceConfig(
        api_key=SecretStr("secret_test_key"),
        page_ids=["2bffc6a6-4277-8024-97c9-d0f26faa4480"],
        stateful_ingestion={
            "enabled": True,
            "state_provider": {
                "type": "file",
                "config": {"filename": "/tmp/notion_test_state.json"},
            },
        },
        embedding={
            "provider": "bedrock",
            "model": "cohere.embed-english-v3",
            "aws_region": "us-west-2",
            "allow_local_embedding_config": True,
        },
    )
    ctx = PipelineContext(run_id="test", pipeline_name="test_pipeline")
    source = NotionSource(config=config, ctx=ctx)

    # New document (not in state)
    assert source._should_process_document("urn:li:document:new", "content", "page-1")


def test_should_process_document_unchanged():
    """Test that unchanged documents are skipped."""
    config = NotionSourceConfig(
        api_key=SecretStr("secret_test_key"),
        page_ids=["2bffc6a6-4277-8024-97c9-d0f26faa4480"],
        stateful_ingestion={
            "enabled": True,
            "state_provider": {
                "type": "file",
                "config": {"filename": "/tmp/notion_test_state.json"},
            },
        },
        embedding={
            "provider": "bedrock",
            "model": "cohere.embed-english-v3",
            "aws_region": "us-west-2",
            "allow_local_embedding_config": True,
        },
    )
    ctx = PipelineContext(run_id="test", pipeline_name="test_pipeline")
    source = NotionSource(config=config, ctx=ctx)

    # Add document to state
    doc_urn = "urn:li:document:existing"
    text = "existing content"
    page_id = "page-1"
    source._update_document_state(doc_urn, text, page_id)

    # Same content should be skipped
    assert not source._should_process_document(doc_urn, text, page_id)


def test_should_process_document_content_changed():
    """Test that documents with changed content are processed."""
    config = NotionSourceConfig(
        api_key=SecretStr("secret_test_key"),
        page_ids=["2bffc6a6-4277-8024-97c9-d0f26faa4480"],
        stateful_ingestion={
            "enabled": True,
            "state_provider": {
                "type": "file",
                "config": {"filename": "/tmp/notion_test_state.json"},
            },
        },
        embedding={
            "provider": "bedrock",
            "model": "cohere.embed-english-v3",
            "aws_region": "us-west-2",
            "allow_local_embedding_config": True,
        },
    )
    ctx = PipelineContext(run_id="test", pipeline_name="test_pipeline")
    source = NotionSource(config=config, ctx=ctx)

    # Add document to state
    doc_urn = "urn:li:document:existing"
    source._update_document_state(doc_urn, "old content", "page-1")

    # Changed content should trigger reprocessing
    assert source._should_process_document(doc_urn, "new content", "page-1")


def test_should_process_document_stateful_disabled():
    """Test that all documents are processed when stateful ingestion is disabled."""
    config = NotionSourceConfig(
        api_key=SecretStr("secret_test_key"),
        page_ids=["2bffc6a6-4277-8024-97c9-d0f26faa4480"],
        embedding={
            "provider": "bedrock",
            "model": "cohere.embed-english-v3",
            "aws_region": "us-west-2",
            "allow_local_embedding_config": True,
        },
    )
    ctx = PipelineContext(run_id="test")
    source = NotionSource(config=config, ctx=ctx)

    # Should always return True when stateful ingestion disabled
    assert source._should_process_document("urn:li:document:any", "content", "page-1")


def test_update_document_state():
    """Test that document state is updated correctly."""
    config = NotionSourceConfig(
        api_key=SecretStr("secret_test_key"),
        page_ids=["2bffc6a6-4277-8024-97c9-d0f26faa4480"],
        stateful_ingestion={
            "enabled": True,
            "state_provider": {
                "type": "file",
                "config": {"filename": "/tmp/notion_test_state.json"},
            },
        },
        embedding={
            "provider": "bedrock",
            "model": "cohere.embed-english-v3",
            "aws_region": "us-west-2",
            "allow_local_embedding_config": True,
        },
    )
    ctx = PipelineContext(run_id="test", pipeline_name="test_pipeline")
    source = NotionSource(config=config, ctx=ctx)

    doc_urn = "urn:li:document:test"
    text = "test content"
    page_id = "test-page-id"

    source._update_document_state(doc_urn, text, page_id)

    # Verify state was updated
    assert doc_urn in source.document_state
    state = source.document_state[doc_urn]
    assert "content_hash" in state
    assert "last_processed" in state
    assert "page_id" in state
    assert state["page_id"] == page_id


def test_max_documents_default():
    """Test that max_documents defaults to 10000."""
    config = NotionSourceConfig(
        api_key=SecretStr("secret_test_key"),
        page_ids=["2bffc6a6-4277-8024-97c9-d0f26faa4480"],
        embedding={
            "provider": "bedrock",
            "model": "cohere.embed-english-v3",
            "aws_region": "us-west-2",
            "allow_local_embedding_config": True,
        },
    )
    ctx = PipelineContext(run_id="test")
    source = NotionSource(config=config, ctx=ctx)
    assert source.chunking_source.config.max_documents == 10000


def test_max_documents_limit_raises_error():
    """Test that RuntimeError propagates from chunking_source when max_documents is hit."""
    config = NotionSourceConfig(
        api_key=SecretStr("secret_test_key"),
        page_ids=["2bffc6a6-4277-8024-97c9-d0f26faa4480"],
        embedding={
            "provider": "bedrock",
            "model": "cohere.embed-english-v3",
            "aws_region": "us-west-2",
            "allow_local_embedding_config": True,
        },
    )
    ctx = PipelineContext(run_id="test")
    source = NotionSource(config=config, ctx=ctx)
    source.chunking_source.config.max_documents = 2

    def make_data(page_id: str) -> dict:
        return {
            "elements": [{"text": "Some content", "type": "NarrativeText"}],
            "metadata": {
                "data_source": {"record_locator": {"page_id": page_id}},
                "filetype": "text/plain",
            },
        }

    mock_doc = MagicMock()
    mock_doc.urn = "urn:li:document:notion.page-1"
    mock_doc.as_workunits.return_value = iter([])

    # Disable embedding and stub chunking to avoid needing `unstructured` installed
    source.chunking_source.embedding_model = None
    dummy_chunk = [{"text": "Some content", "type": "NarrativeText"}]

    with (
        patch.object(
            source.document_builder, "build_document_entity", return_value=mock_doc
        ),
        patch.object(
            source.chunking_source, "_chunk_elements", return_value=dummy_chunk
        ),
    ):
        # First document — should succeed
        list(source._create_document_entity(make_data("page-1")))
        assert source.report.num_documents_created == 1
        assert source.report.num_documents_limit_reached is False

        # Second document — hits the limit
        with pytest.raises(RuntimeError, match="Document limit of 2 reached"):
            list(source._create_document_entity(make_data("page-2")))

    assert source.report.num_documents_limit_reached is True
    assert source.report.num_documents_created == 1


def test_embedding_stats_aggregation(notion_source):
    """Test that embedding statistics from chunking source are aggregated into notion report."""
    # Verify embedding fields exist on the report
    assert hasattr(notion_source.report, "num_documents_with_embeddings")
    assert hasattr(notion_source.report, "num_embedding_failures")
    assert hasattr(notion_source.report, "embedding_failures")

    # Simulate chunking source having some embedding stats
    notion_source.chunking_source.report.num_documents_with_embeddings = 5
    notion_source.chunking_source.report.num_embedding_failures = 2
    notion_source.chunking_source.report.embedding_failures.append(
        "urn:li:document:test1: Error 1"
    )
    notion_source.chunking_source.report.embedding_failures.append(
        "urn:li:document:test2: Error 2"
    )

    # Get the report (should aggregate stats)
    final_report = notion_source.get_report()

    # Verify stats were copied
    assert final_report.num_documents_with_embeddings == 5
    assert final_report.num_embedding_failures == 2
    assert len(final_report.embedding_failures) == 2
    # Convert LossyList to list for comparison
    failures_list = list(final_report.embedding_failures)
    assert "urn:li:document:test1: Error 1" in failures_list
    assert "urn:li:document:test2: Error 2" in failures_list
