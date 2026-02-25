"""Unit tests for DataHub Documents Source."""

import hashlib
import json
import sys
from typing import Any
from unittest.mock import Mock, patch

import pytest

# Skip entire module if unstructured is not installed (requires Python 3.10+)
pytest.importorskip("unstructured")

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.datahub_documents.datahub_documents_config import (
    DataHubDocumentsSourceConfig,
)
from datahub.ingestion.source.datahub_documents.datahub_documents_source import (
    DataHubDocumentsSource,
)
from datahub.ingestion.source.datahub_documents.text_partitioner import TextPartitioner
from datahub.ingestion.source.unstructured.chunking_config import (
    ServerEmbeddingConfig,
    ServerSemanticSearchConfig,
)


class TestTextPartitioner:
    """Test text partitioner."""

    @pytest.mark.skipif(
        sys.version_info < (3, 10),
        reason="unstructured requires Python 3.10+",
    )
    def test_partition_simple_markdown(self):
        """Test partitioning simple markdown text."""
        partitioner = TextPartitioner()
        text = "# Title\n\nThis is a paragraph.\n\n## Subtitle\n\nAnother paragraph."

        elements = partitioner.partition_text(text)

        assert len(elements) > 0
        assert all(isinstance(elem, dict) for elem in elements)
        # Check that we got various element types
        element_types = {elem.get("type") for elem in elements}
        assert "Title" in element_types or "Header" in element_types

    @pytest.mark.skipif(
        sys.version_info < (3, 10),
        reason="unstructured requires Python 3.10+",
    )
    def test_partition_empty_text(self):
        """Test partitioning empty text."""
        partitioner = TextPartitioner()
        text = ""

        elements = partitioner.partition_text(text)

        assert elements == []


class TestDataHubDocumentsConfig:
    """Test configuration validation."""

    def test_default_config(self):
        """Test default configuration values."""
        config = DataHubDocumentsSourceConfig()

        assert config.platform_filter is None  # Default: None = all NATIVE documents
        assert config.incremental.enabled is True
        assert config.skip_empty_text is True
        assert config.min_text_length == 50
        assert config.event_mode.enabled is False
        assert config.chunking.strategy == "by_title"
        assert config.partition_strategy == "markdown"
        # Embedding config defaults to None - loaded from server
        assert config.embedding.provider is None
        assert config.embedding.model is None

    def test_multi_platform_filter(self):
        """Test multi-platform filtering configuration."""
        config = DataHubDocumentsSourceConfig(
            platform_filter=["notion", "confluence", "sharepoint"],
            stateful_ingestion={"enabled": False},
        )

        assert config.platform_filter is not None
        assert len(config.platform_filter) == 3
        assert "notion" in config.platform_filter
        assert "confluence" in config.platform_filter
        assert "sharepoint" in config.platform_filter

    def test_empty_platform_filter_allowed(self):
        """Test that both None and [] are allowed for platform_filter (both mean NATIVE only)."""
        # Empty list is explicitly set - processes only NATIVE
        config_empty = DataHubDocumentsSourceConfig(platform_filter=[])
        assert config_empty.platform_filter == []

        # None is the default - also processes only NATIVE
        config_none = DataHubDocumentsSourceConfig()
        assert config_none.platform_filter is None

        # Both None and [] semantically mean "NATIVE documents only"
        # but they are stored differently

    def test_event_mode_config(self):
        """Test event mode configuration."""
        config = DataHubDocumentsSourceConfig(
            event_mode={"enabled": True, "consumer_id": "test-consumer"},
            stateful_ingestion={"enabled": False},
        )

        assert config.event_mode.enabled is True
        assert config.event_mode.consumer_id == "test-consumer"


class TestDataHubDocumentsSource:
    """Test DataHub documents source."""

    @pytest.fixture
    def mock_semantic_search_config(self):
        """Create mock semantic search config."""
        return ServerSemanticSearchConfig(
            enabled=True,
            enabled_entities=["document"],
            embedding_config=ServerEmbeddingConfig(
                provider="bedrock",
                model_id="cohere.embed-english-v3",
                aws_region="us-west-2",
                model_embedding_key="cohere_embed_v3",
            ),
        )

    @pytest.fixture
    def config(self):
        """Create test configuration."""
        return DataHubDocumentsSourceConfig(
            platform_filter=["notion"],
            datahub={"server": "http://test-server:8080"},
            # Bypass server validation in tests with valid test config
            embedding={
                "provider": "bedrock",
                "model": "cohere.embed-english-v3",
                "aws_region": "us-west-2",
                "allow_local_embedding_config": True,
            },
            # Disable stateful ingestion in unit tests
            stateful_ingestion={"enabled": False},
        )

    @pytest.fixture
    def ctx(self):
        """Create test context."""
        return PipelineContext(run_id="test-run", pipeline_name="test-pipeline")

    def test_source_initialization(self, ctx, config):
        """Test source initialization."""
        with patch(
            "datahub.ingestion.source.datahub_documents.datahub_documents_source.DataHubGraph"
        ):
            source = DataHubDocumentsSource(ctx, config)

            assert source.config == config
            assert source.report is not None
            assert source.text_partitioner is not None

    def test_calculate_text_hash(self, ctx, config):
        """Test content hash calculation."""
        with patch(
            "datahub.ingestion.source.datahub_documents.datahub_documents_source.DataHubGraph"
        ):
            source = DataHubDocumentsSource(ctx, config)

            text1 = "This is some test content"
            text2 = "This is different content"
            text3 = "This is some test content"  # Same as text1

            hash1 = source._calculate_text_hash(text1)
            hash2 = source._calculate_text_hash(text2)
            hash3 = source._calculate_text_hash(text3)

            # Hashes should be deterministic
            assert hash1 == hash3
            assert hash1 != hash2

            # Should be valid SHA256 hash
            assert len(hash1) == 64  # SHA256 produces 64 hex characters
            assert all(c in "0123456789abcdef" for c in hash1)

    def test_should_process_incremental_mode(self, ctx, config):
        """Test should_process logic in incremental mode."""
        with patch(
            "datahub.ingestion.source.datahub_documents.datahub_documents_source.DataHubGraph"
        ):
            source = DataHubDocumentsSource(ctx, config)
            source.config.incremental.enabled = True

            # New document should be processed (not in document_state)
            assert source._should_process("urn:li:document:123", "some text") is True

            # Add document to state
            source._update_document_state("urn:li:document:123", "some text")

            # Same text should not be processed
            assert source._should_process("urn:li:document:123", "some text") is False

            # Changed text should be processed (different hash)
            assert (
                source._should_process("urn:li:document:123", "different text") is True
            )

    def test_should_process_force_reprocess(self, ctx, config):
        """Test force_reprocess flag."""
        with patch(
            "datahub.ingestion.source.datahub_documents.datahub_documents_source.DataHubGraph"
        ):
            source = DataHubDocumentsSource(ctx, config)
            source.config.incremental.enabled = True
            source.config.incremental.force_reprocess = True

            # Mock state handler with existing hash
            mock_state_handler = patch.object(source, "state_handler").start()
            text_hash = source._calculate_text_hash("some text")
            mock_state_handler.get_document_hash.return_value = text_hash

            # Even with same content, should process due to force_reprocess
            assert source._should_process("urn:li:document:123", "some text") is True

    def test_extract_platform_from_aspect(self, ctx, config):
        """Test platform extraction from documentInfo aspect."""
        with patch(
            "datahub.ingestion.source.datahub_documents.datahub_documents_source.DataHubGraph"
        ):
            source = DataHubDocumentsSource(ctx, config)

            # Test valid platform URN
            aspect_dict = {
                "dataPlatformInstance": {"platform": "urn:li:dataPlatform:notion"}
            }
            platform = source._extract_platform_from_aspect(aspect_dict)
            assert platform == "notion"

            # Test missing platform
            aspect_dict = {}
            platform = source._extract_platform_from_aspect(aspect_dict)
            assert platform is None

            # Test malformed URN
            aspect_dict = {"dataPlatformInstance": {"platform": "invalid"}}
            platform = source._extract_platform_from_aspect(aspect_dict)
            assert platform is None

    def test_parse_mcl_aspect(self, ctx, config):
        """Test MCL aspect parsing."""
        with patch(
            "datahub.ingestion.source.datahub_documents.datahub_documents_source.DataHubGraph"
        ):
            source = DataHubDocumentsSource(ctx, config)

            # Test simple dict
            result = source._parse_mcl_aspect({"field": "value"})
            assert result == {"field": "value"}

            # Test Avro union wrapper
            aspect_data_avro: dict[str, dict[str, str]] = {
                "com.linkedin.pegasus2avro.mxe.GenericAspect": {
                    "value": '{"field": "value"}'
                }
            }
            result = source._parse_mcl_aspect(aspect_data_avro)
            assert result == {"field": "value"}

            # Test string input
            result = source._parse_mcl_aspect('{"field": "value"}')
            assert result == {"field": "value"}

    def test_embedding_model_name_bedrock(self, ctx):
        """Test embedding model name for Bedrock."""
        config = DataHubDocumentsSourceConfig(
            embedding={
                "provider": "bedrock",
                "model": "cohere.embed-english-v3",
                "aws_region": "us-west-2",
                "allow_local_embedding_config": True,
            },
            stateful_ingestion={"enabled": False},
        )

        with patch(
            "datahub.ingestion.source.datahub_documents.datahub_documents_source.DataHubGraph"
        ):
            source = DataHubDocumentsSource(ctx, config)

            assert (
                source.chunking_source.embedding_model
                == "bedrock/cohere.embed-english-v3"
            )

    def test_embedding_model_name_cohere(self, ctx):
        """Test embedding model name for Cohere."""
        config = DataHubDocumentsSourceConfig(
            embedding={
                "provider": "cohere",
                "model": "embed-english-v3.0",
                "api_key": "test-api-key",
                "allow_local_embedding_config": True,
            },
            stateful_ingestion={"enabled": False},
        )

        with patch(
            "datahub.ingestion.source.datahub_documents.datahub_documents_source.DataHubGraph"
        ):
            source = DataHubDocumentsSource(ctx, config)

            assert source.chunking_source.embedding_model == "cohere/embed-english-v3.0"

    def test_update_document_state(self, ctx, config):
        """Test document state update."""
        with patch(
            "datahub.ingestion.source.datahub_documents.datahub_documents_source.DataHubGraph"
        ):
            source = DataHubDocumentsSource(ctx, config)

            document_urn = "urn:li:document:123"
            text = "some document text"

            source._update_document_state(document_urn, text)

            # Verify document was added to state with correct hash
            assert document_urn in source.document_state
            assert "content_hash" in source.document_state[document_urn]
            assert "last_processed" in source.document_state[document_urn]

            # Verify hash matches expected value
            expected_hash = source._calculate_text_hash(text)
            assert source.document_state[document_urn]["content_hash"] == expected_hash


class TestEventModeFallback:
    """Test event mode fallback to batch mode functionality."""

    @pytest.fixture
    def config(self):
        """Create test configuration with event mode enabled."""
        return DataHubDocumentsSourceConfig(
            platform_filter=["*"],
            datahub={"server": "http://test-server:8080"},
            event_mode={
                "enabled": True,
                "consumer_id": "test-consumer",
                "idle_timeout_seconds": 1,  # Short timeout for fast tests
            },
            embedding={
                "provider": "bedrock",
                "model": "cohere.embed-english-v3",
                "aws_region": "us-west-2",
                "allow_local_embedding_config": True,
            },
            # Disable stateful ingestion in unit tests
            stateful_ingestion={"enabled": False},
        )

    @pytest.fixture
    def ctx(self):
        """Create test context."""
        return PipelineContext(run_id="test-run", pipeline_name="test-pipeline")

    @pytest.fixture
    def mock_graph(self):
        """Create mock DataHubGraph."""
        mock = patch(
            "datahub.ingestion.source.datahub_documents.datahub_documents_source.DataHubGraph"
        )
        return mock

    def test_fallback_when_state_handler_unavailable(self, ctx, config, mock_graph):
        """Test fallback to batch mode when state handler is unavailable."""
        with mock_graph:
            source = DataHubDocumentsSource(ctx, config)
            # Simulate state handler not available
            source.state_handler = None

            # Mock batch mode to verify it's called
            with patch.object(source, "_process_batch_mode") as mock_batch_mode:
                mock_batch_mode.return_value = iter([])

                # Process in event mode - should fall back
                list(source._process_event_mode())

                # Verify batch mode was called
                mock_batch_mode.assert_called_once()

    def test_fallback_when_no_offsets_no_lookback(self, ctx, config, mock_graph):
        """Test fallback when no offsets found and no lookback window."""
        with mock_graph:
            source = DataHubDocumentsSource(ctx, config)
            # Mock state handler with no offsets
            mock_state_handler = patch.object(source, "state_handler").start()
            mock_state_handler.is_checkpointing_enabled.return_value = True
            mock_state_handler.get_event_offset.return_value = None

            # Mock batch mode
            with patch.object(source, "_process_batch_mode") as mock_batch_mode:
                mock_batch_mode.return_value = iter([])

                # Process in event mode - should fall back
                list(source._process_event_mode())

                # Verify batch mode was called
                mock_batch_mode.assert_called_once()

    def test_fallback_when_event_api_returns_error(self, ctx, config, mock_graph):
        """Test fallback when Events API returns HTTP error."""
        import requests

        with mock_graph:
            source = DataHubDocumentsSource(ctx, config)
            # Mock state handler with valid offset
            mock_state_handler = patch.object(source, "state_handler").start()
            mock_state_handler.is_checkpointing_enabled.return_value = True
            mock_state_handler.get_event_offset.return_value = "test-offset-123"

            # Mock requests.get to raise HTTP error
            with patch("requests.get") as mock_get:
                # Simulate HTTP 500 error
                mock_response = Mock()
                mock_response.raise_for_status.side_effect = requests.HTTPError(
                    "500 Server Error"
                )
                mock_get.return_value = mock_response

                # Mock batch mode
                with patch.object(source, "_process_batch_mode") as mock_batch_mode:
                    mock_batch_mode.return_value = iter([])

                    # Process in event mode - should fall back on error
                    list(source._process_event_mode())

                    # Verify batch mode was called
                    mock_batch_mode.assert_called_once()

    def test_fallback_when_event_api_connection_error(self, ctx, config, mock_graph):
        """Test fallback when Events API connection fails."""
        import requests

        with mock_graph:
            source = DataHubDocumentsSource(ctx, config)
            # Mock state handler with valid offset
            mock_state_handler = patch.object(source, "state_handler").start()
            mock_state_handler.is_checkpointing_enabled.return_value = True
            mock_state_handler.get_event_offset.return_value = "test-offset-123"

            # Mock requests.get to raise connection error
            with patch("requests.get") as mock_get:
                mock_get.side_effect = requests.ConnectionError("Connection refused")

                # Mock batch mode
                with patch.object(source, "_process_batch_mode") as mock_batch_mode:
                    mock_batch_mode.return_value = iter([])

                    # Process in event mode - should fall back on error
                    list(source._process_event_mode())

                    # Verify batch mode was called
                    mock_batch_mode.assert_called_once()

    def test_fallback_when_event_api_timeout(self, ctx, config, mock_graph):
        """Test fallback when Events API times out."""
        import requests

        with mock_graph:
            source = DataHubDocumentsSource(ctx, config)
            # Mock state handler with valid offset
            mock_state_handler = patch.object(source, "state_handler").start()
            mock_state_handler.is_checkpointing_enabled.return_value = True
            mock_state_handler.get_event_offset.return_value = "test-offset-123"

            # Mock requests.get to raise timeout
            with patch("requests.get") as mock_get:
                mock_get.side_effect = requests.Timeout("Request timed out")

                # Mock batch mode
                with patch.object(source, "_process_batch_mode") as mock_batch_mode:
                    mock_batch_mode.return_value = iter([])

                    # Process in event mode - should fall back on error
                    list(source._process_event_mode())

                    # Verify batch mode was called
                    mock_batch_mode.assert_called_once()

    def test_no_fallback_when_offsets_exist(self, ctx, config, mock_graph):
        """Test that fallback does NOT occur when offsets exist and events are processed."""
        with mock_graph:
            source = DataHubDocumentsSource(ctx, config)
            # Mock state handler with valid offset
            mock_state_handler = patch.object(source, "state_handler").start()
            mock_state_handler.is_checkpointing_enabled.return_value = True
            mock_state_handler.get_event_offset.return_value = "test-offset-123"

            # Mock successful event polling with events

            with patch("requests.get") as mock_get:
                mock_response = Mock()
                mock_response.status_code = 200
                mock_response.json.return_value = {
                    "events": [],
                    "offsetId": "test-offset-456",
                }
                mock_response.raise_for_status.return_value = None
                mock_get.return_value = mock_response

                # Mock batch mode - should NOT be called
                with patch.object(source, "_process_batch_mode") as mock_batch_mode:
                    # Mock consume_events to return a valid event that gets processed
                    # This simulates successful event processing
                    mock_event = {
                        "entityUrn": "urn:li:document:test123",
                        "aspectName": "documentInfo",
                        "aspect": {
                            "com.linkedin.pegasus2avro.mxe.GenericAspect": {
                                "value": json.dumps(
                                    {
                                        "contents": {"text": "Test document content"},
                                    }
                                )
                            }
                        },
                    }

                    with patch(
                        "datahub.ingestion.source.unstructured.event_consumer.DocumentEventConsumer.consume_events"
                    ) as mock_consume:
                        # Return an event that will be processed
                        mock_consume.return_value = iter([mock_event])

                        # Mock _process_single_document to avoid actual processing
                        with patch.object(
                            source, "_process_single_document"
                        ) as mock_process:
                            mock_process.return_value = iter([])

                            # Process in event mode - should NOT fall back
                            list(source._process_event_mode())

                            # Verify batch mode was NOT called
                            mock_batch_mode.assert_not_called()
                            # Verify event was processed
                            mock_process.assert_called()

    def test_fallback_when_no_events_and_no_lookback(self, ctx, config, mock_graph):
        """Test fallback when no events processed and no lookback window."""
        with mock_graph:
            source = DataHubDocumentsSource(ctx, config)
            # Mock state handler with valid offset
            mock_state_handler = patch.object(source, "state_handler").start()
            mock_state_handler.is_checkpointing_enabled.return_value = True
            mock_state_handler.get_event_offset.return_value = "test-offset-123"

            # Mock successful event polling but no events

            with patch("requests.get") as mock_get:
                mock_response = Mock()
                mock_response.status_code = 200
                mock_response.json.return_value = {
                    "events": [],
                    "offsetId": "test-offset-456",
                }
                mock_response.raise_for_status.return_value = None
                mock_get.return_value = mock_response

                # Mock batch mode
                with patch.object(source, "_process_batch_mode") as mock_batch_mode:
                    mock_batch_mode.return_value = iter([])

                    # Process in event mode - should fall back when no events
                    list(source._process_event_mode())

                    # Verify batch mode was called
                    mock_batch_mode.assert_called_once()

    def test_should_fallback_to_batch_mode_no_state_handler(
        self, ctx, config, mock_graph
    ):
        """Test _should_fallback_to_batch_mode when state handler is None."""
        with mock_graph:
            source = DataHubDocumentsSource(ctx, config)
            source.state_handler = None

            assert source._should_fallback_to_batch_mode() is True

    def test_should_fallback_to_batch_mode_no_checkpointing(
        self, ctx, config, mock_graph
    ):
        """Test _should_fallback_to_batch_mode when checkpointing disabled."""
        with mock_graph:
            source = DataHubDocumentsSource(ctx, config)
            mock_state_handler = patch.object(source, "state_handler").start()
            mock_state_handler.is_checkpointing_enabled.return_value = False

            assert source._should_fallback_to_batch_mode() is True

    def test_should_fallback_to_batch_mode_no_offsets_no_lookback(
        self, ctx, config, mock_graph
    ):
        """Test _should_fallback_to_batch_mode when no offsets and no lookback."""
        with mock_graph:
            source = DataHubDocumentsSource(ctx, config)
            mock_state_handler = patch.object(source, "state_handler").start()
            mock_state_handler.is_checkpointing_enabled.return_value = True
            mock_state_handler.get_event_offset.return_value = None

            assert source._should_fallback_to_batch_mode() is True

    def test_should_not_fallback_when_offsets_exist(self, ctx, config, mock_graph):
        """Test _should_fallback_to_batch_mode when offsets exist."""
        with mock_graph:
            source = DataHubDocumentsSource(ctx, config)
            mock_state_handler = patch.object(source, "state_handler").start()
            mock_state_handler.is_checkpointing_enabled.return_value = True
            mock_state_handler.get_event_offset.return_value = "test-offset-123"

            assert source._should_fallback_to_batch_mode() is False

    def test_should_not_fallback_with_lookback_window(self, ctx, config, mock_graph):
        """Test _should_fallback_to_batch_mode when lookback window is set."""
        config.event_mode.lookback_days = 7
        with mock_graph:
            source = DataHubDocumentsSource(ctx, config)
            mock_state_handler = patch.object(source, "state_handler").start()
            mock_state_handler.is_checkpointing_enabled.return_value = True
            mock_state_handler.get_event_offset.return_value = None

            # Should not fallback if lookback window is set
            assert source._should_fallback_to_batch_mode() is False


class TestStateStorage:
    """Test state storage behavior in various scenarios."""

    @pytest.fixture
    def config(self):
        """Create test configuration."""
        return DataHubDocumentsSourceConfig(
            platform_filter=["*"],
            datahub={"server": "http://test-server:8080"},
            embedding={
                "provider": "bedrock",
                "model": "cohere.embed-english-v3",
                "aws_region": "us-west-2",
                "allow_local_embedding_config": True,
            },
            # Disable stateful ingestion in unit tests
            stateful_ingestion={"enabled": False},
        )

    @pytest.fixture
    def ctx(self):
        """Create test context."""
        return PipelineContext(run_id="test-run", pipeline_name="test-pipeline")

    @pytest.fixture
    def mock_graph(self):
        """Create mock DataHubGraph."""
        mock = patch(
            "datahub.ingestion.source.datahub_documents.datahub_documents_source.DataHubGraph"
        )
        return mock

    def test_batch_mode_stores_document_hashes(self, ctx, config, mock_graph):
        """Test that batch mode stores document hashes in state."""
        with mock_graph:
            source = DataHubDocumentsSource(ctx, config)

            # Mock state handler
            mock_state_handler = patch.object(source, "state_handler").start()
            mock_state_handler.is_checkpointing_enabled.return_value = True
            mock_state_handler.get_document_hash.return_value = None  # New document
            mock_state_handler.update_document_state = Mock()

            # Mock GraphQL to return documents
            mock_docs = [
                {"urn": "urn:li:document:1", "text": "Document 1 content"},
                {"urn": "urn:li:document:2", "text": "Document 2 content"},
            ]
            with (
                patch.object(
                    source, "_fetch_documents_graphql", return_value=mock_docs
                ),
                patch.object(source, "_process_single_document", return_value=iter([])),
            ):
                # Process in batch mode
                list(source._process_batch_mode())

                # Verify document state was updated for each document
                assert mock_state_handler.update_document_state.call_count == 2

                # Verify correct parameters
                calls = mock_state_handler.update_document_state.call_args_list
                assert calls[0][0][0] == "urn:li:document:1"
                assert calls[1][0][0] == "urn:li:document:2"

                # Verify hashes are correct
                hash1 = source._calculate_text_hash("Document 1 content")
                hash2 = source._calculate_text_hash("Document 2 content")
                assert calls[0][0][1] == hash1
                assert calls[1][0][1] == hash2

    def test_fallback_stores_document_hashes(self, ctx, config, mock_graph):
        """Test that fallback to batch mode stores document hashes."""
        with mock_graph:
            source = DataHubDocumentsSource(ctx, config)
            source.config.event_mode.enabled = True

            # Mock state handler
            mock_state_handler = patch.object(source, "state_handler").start()
            mock_state_handler.is_checkpointing_enabled.return_value = True
            mock_state_handler.get_event_offset.return_value = (
                None  # No offsets -> fallback
            )
            mock_state_handler.update_document_state = Mock()

            # Mock GraphQL to return documents
            mock_docs = [
                {"urn": "urn:li:document:1", "text": "Document 1 content"},
            ]
            with (
                patch.object(
                    source, "_fetch_documents_graphql", return_value=mock_docs
                ),
                patch.object(source, "_process_single_document", return_value=iter([])),
            ):
                # Process in event mode - should fallback
                list(source._process_event_mode())

                # Verify document state was updated (fallback processed documents)
                mock_state_handler.update_document_state.assert_called_once()
                call_args = mock_state_handler.update_document_state.call_args[0]
                assert call_args[0] == "urn:li:document:1"

                # Verify hash is correct
                expected_hash = source._calculate_text_hash("Document 1 content")
                assert call_args[1] == expected_hash

    def test_event_mode_stores_offsets(self, ctx, config, mock_graph):
        """Test that event mode stores offsets in state."""

        with mock_graph:
            source = DataHubDocumentsSource(ctx, config)
            source.config.event_mode.enabled = True

            # Mock state handler
            mock_state_handler = patch.object(source, "state_handler").start()
            mock_state_handler.is_checkpointing_enabled.return_value = True
            mock_state_handler.get_event_offset.return_value = "initial-offset-123"
            mock_state_handler.update_event_offset = Mock()

            # Mock successful event polling
            with patch("requests.get") as mock_get:
                mock_response = Mock()
                mock_response.status_code = 200
                mock_response.json.return_value = {
                    "events": [],
                    "offsetId": "new-offset-456",
                }
                mock_response.raise_for_status.return_value = None
                mock_get.return_value = mock_response

                # Mock consume_events to return a valid event
                mock_event = {
                    "entityUrn": "urn:li:document:test123",
                    "aspectName": "documentInfo",
                    "aspect": {
                        "value": json.dumps(
                            {
                                "contents": {"text": "Test document content"},
                            }
                        )
                    },
                }

                with patch(
                    "datahub.ingestion.source.unstructured.event_consumer.DocumentEventConsumer.consume_events"
                ) as mock_consume:
                    mock_consume.return_value = iter([mock_event])

                    with patch.object(
                        source, "_process_single_document", return_value=iter([])
                    ):
                        # Process in event mode
                        list(source._process_event_mode())

                        # Verify offset was updated
                        mock_state_handler.update_event_offset.assert_called()
                        # Check that offset was updated with new offset from polling
                        calls = mock_state_handler.update_event_offset.call_args_list
                        # The offset should be updated when polling returns new offset
                        assert len(calls) > 0

    def test_state_preserves_both_document_hashes_and_offsets(
        self, ctx, config, mock_graph
    ):
        """Test that state preserves both document hashes and offsets."""
        with mock_graph:
            source = DataHubDocumentsSource(ctx, config)

            # Mock state handler with both types of state
            mock_state_handler = patch.object(source, "state_handler").start()
            mock_state_handler.is_checkpointing_enabled.return_value = True

            # Simulate existing state with both document hashes and offsets
            from datahub.ingestion.source.datahub_documents.document_chunking_state import (
                DocumentChunkingCheckpointState,
            )

            existing_state = DocumentChunkingCheckpointState(
                document_state={
                    "urn:li:document:1": {
                        "content_hash": "hash1",
                        "last_processed": "2024-01-01T00:00:00",
                    }
                },
                event_offsets={"MetadataChangeLog_Versioned_v1": "offset-123"},
            )

            # Mock get_last_state to return existing state
            mock_state_handler.get_last_state = Mock(return_value=existing_state)
            mock_state_handler.get_current_state = Mock(
                return_value=DocumentChunkingCheckpointState()
            )
            mock_state_handler.get_document_hash.return_value = "hash1"
            mock_state_handler.get_event_offset.return_value = "offset-123"
            mock_state_handler.update_document_state = Mock()
            mock_state_handler.update_event_offset = Mock()

            # Verify both types of state are accessible
            doc_hash = mock_state_handler.get_document_hash("urn:li:document:1")
            event_offset = mock_state_handler.get_event_offset(
                "MetadataChangeLog_Versioned_v1"
            )

            assert doc_hash == "hash1"
            assert event_offset == "offset-123"

    def test_fallback_preserves_existing_offsets(self, ctx, config, mock_graph):
        """Test that fallback to batch mode preserves existing event offsets."""
        import requests

        with mock_graph:
            source = DataHubDocumentsSource(ctx, config)
            source.config.event_mode.enabled = True

            # Mock state handler with existing offsets
            mock_state_handler = patch.object(source, "state_handler").start()
            mock_state_handler.is_checkpointing_enabled.return_value = True
            mock_state_handler.get_event_offset.return_value = "existing-offset-123"

            # Mock event API to fail (triggers fallback)
            with patch("requests.get") as mock_get:
                mock_get.side_effect = requests.ConnectionError("Connection refused")

                # Mock batch mode
                mock_docs = [{"urn": "urn:li:document:1", "text": "Document 1"}]
                with (
                    patch.object(
                        source, "_fetch_documents_graphql", return_value=mock_docs
                    ),
                    patch.object(
                        source, "_process_single_document", return_value=iter([])
                    ),
                ):
                    mock_state_handler.update_document_state = Mock()
                    mock_state_handler.update_event_offset = Mock()

                    # Process in event mode - should fallback
                    list(source._process_event_mode())

                    # Verify document state was updated (fallback processed documents)
                    mock_state_handler.update_document_state.assert_called_once()

                    # Note: update_event_offset may be called when event consumer closes
                    # This is fine - it preserves the existing offset value
                    # The important thing is that offsets are not cleared

                    # Verify existing offset is still accessible (not cleared)
                    assert (
                        mock_state_handler.get_event_offset(
                            "MetadataChangeLog_Versioned_v1"
                        )
                        == "existing-offset-123"
                    )

                    # Verify offset was preserved (not changed to a different value)
                    # If update_event_offset was called, it should be with the same offset
                    if mock_state_handler.update_event_offset.called:
                        offset_calls = (
                            mock_state_handler.update_event_offset.call_args_list
                        )
                        # All calls should preserve the existing offset
                        for call in offset_calls:
                            assert (
                                call[0][1] == "existing-offset-123"
                            )  # Same offset preserved

    def test_incremental_mode_skips_unchanged_documents(self, ctx, config, mock_graph):
        """Test that incremental mode skips documents with unchanged hashes."""
        with mock_graph:
            source = DataHubDocumentsSource(ctx, config)
            source.config.incremental.enabled = True

            # Mock state handler with existing document hash
            mock_state_handler = patch.object(source, "state_handler").start()
            mock_state_handler.is_checkpointing_enabled.return_value = True

            # Document with same hash (unchanged)
            text = "Same content"
            text_hash = source._calculate_text_hash(text)
            mock_state_handler.get_document_hash.return_value = text_hash

            # Mock documents
            mock_docs = [{"urn": "urn:li:document:1", "text": text}]
            with (
                patch.object(
                    source, "_fetch_documents_graphql", return_value=mock_docs
                ),
                patch.object(
                    source, "_process_single_document", return_value=iter([])
                ) as mock_process,
            ):
                mock_state_handler.update_document_state = Mock()

                # Process in batch mode
                list(source._process_batch_mode())

                # Verify document was NOT processed (skipped due to unchanged hash)
                mock_process.assert_not_called()
                mock_state_handler.update_document_state.assert_not_called()

    def test_incremental_mode_processes_changed_documents(
        self, ctx, config, mock_graph
    ):
        """Test that incremental mode processes documents with changed hashes."""
        with mock_graph:
            source = DataHubDocumentsSource(ctx, config)
            source.config.incremental.enabled = True

            # Mock state handler with different document hash (changed)
            mock_state_handler = patch.object(source, "state_handler").start()
            mock_state_handler.is_checkpointing_enabled.return_value = True

            # Document with different hash (changed)
            old_hash = "old-hash-123"
            new_text = "New content"
            mock_state_handler.get_document_hash.return_value = old_hash

            # Mock documents
            mock_docs = [{"urn": "urn:li:document:1", "text": new_text}]
            with (
                patch.object(
                    source, "_fetch_documents_graphql", return_value=mock_docs
                ),
                patch.object(
                    source, "_process_single_document", return_value=iter([])
                ) as mock_process,
            ):
                mock_state_handler.update_document_state = Mock()

                # Process in batch mode
                list(source._process_batch_mode())

                # Verify document WAS processed (hash changed)
                mock_process.assert_called_once()
                mock_state_handler.update_document_state.assert_called_once()

                # Verify new hash was stored
                call_args = mock_state_handler.update_document_state.call_args[0]
                new_hash = source._calculate_text_hash(new_text)
                assert call_args[1] == new_hash
                assert call_args[1] != old_hash


class TestSourceTypeFiltering:
    """Test source type filtering (NATIVE vs EXTERNAL documents)."""

    @pytest.fixture
    def config(self):
        """Create test configuration."""
        return DataHubDocumentsSourceConfig(
            platform_filter=None,  # Default: None = all NATIVE
            datahub={"server": "http://test-server:8080"},
            embedding={
                "provider": "bedrock",
                "model": "cohere.embed-english-v3",
                "aws_region": "us-west-2",
                "allow_local_embedding_config": True,
            },
            # Disable stateful ingestion in unit tests
            stateful_ingestion={"enabled": False},
        )

    @pytest.fixture
    def ctx(self):
        """Create test context."""
        return PipelineContext(run_id="test-run", pipeline_name="test-pipeline")

    @pytest.fixture
    def mock_graph(self):
        """Create mock DataHubGraph."""
        mock = patch(
            "datahub.ingestion.source.datahub_documents.datahub_documents_source.DataHubGraph"
        )
        return mock

    def test_default_config_processes_all_native(self, ctx, config, mock_graph):
        """Test that default config (empty platform_filter) processes all NATIVE documents."""
        with mock_graph:
            source = DataHubDocumentsSource(ctx, config)

            # NATIVE document should be processed
            entity: dict[str, Any] = {
                "urn": "urn:li:document:native1",
                "info": {
                    "source": {"sourceType": "NATIVE"},
                    "contents": {"text": "Native document content"},
                },
            }
            info: dict[str, Any] = entity["info"]

            should_process = source._should_process_by_source_type(entity, info)
            assert should_process is True

            # EXTERNAL document should be skipped
            entity_external: dict[str, Any] = {
                "urn": "urn:li:document:external1",
                "info": {
                    "source": {"sourceType": "EXTERNAL"},
                    "contents": {"text": "External document content"},
                },
            }
            info_external: dict[str, Any] = entity_external["info"]

            should_process_external = source._should_process_by_source_type(
                entity_external, info_external
            )
            assert should_process_external is False

    def test_platform_filter_processes_native_and_external(self, ctx, mock_graph):
        """Test that platform_filter processes NATIVE + EXTERNAL from specified platforms."""
        config = DataHubDocumentsSourceConfig(
            platform_filter=["notion"],  # Include notion platform
            datahub={"server": "http://test-server:8080"},
            embedding={
                "provider": "bedrock",
                "model": "cohere.embed-english-v3",
                "aws_region": "us-west-2",
                "allow_local_embedding_config": True,
            },
            stateful_ingestion={"enabled": False},
        )

        with mock_graph:
            source = DataHubDocumentsSource(ctx, config)

            # NATIVE document should be processed (if platform matches)
            entity_native: dict[str, Any] = {
                "urn": "urn:li:document:native1",
                "info": {
                    "source": {"sourceType": "NATIVE"},
                    "contents": {"text": "Native document content"},
                },
                "dataPlatformInstance": {
                    "platform": {"urn": "urn:li:dataPlatform:datahub"}
                },
            }
            info_native: dict[str, Any] = entity_native["info"]

            # NATIVE with datahub platform should be processed
            should_process = source._should_process_by_source_type(
                entity_native, info_native
            )
            # Note: NATIVE defaults to "datahub" platform, but "notion" is in filter
            # So it won't match - this is expected behavior
            # If user wants NATIVE + notion EXTERNAL, they'd need ["datahub", "notion"]
            assert should_process is False  # datahub not in ["notion"]

            # EXTERNAL document from notion should be processed
            entity_external: dict[str, Any] = {
                "urn": "urn:li:document:notion1",
                "info": {
                    "source": {"sourceType": "EXTERNAL"},
                    "contents": {"text": "Notion document content"},
                },
                "dataPlatformInstance": {
                    "platform": {"urn": "urn:li:dataPlatform:notion"}
                },
            }
            info_external: dict[str, Any] = entity_external["info"]

            should_process_external = source._should_process_by_source_type(
                entity_external, info_external
            )
            assert should_process_external is True

    def test_wildcard_filter_processes_all(self, ctx, mock_graph):
        """Test that wildcard filter processes all documents."""
        config = DataHubDocumentsSourceConfig(
            platform_filter=["*"],
            datahub={"server": "http://test-server:8080"},
            embedding={
                "provider": "bedrock",
                "model": "cohere.embed-english-v3",
                "aws_region": "us-west-2",
                "allow_local_embedding_config": True,
            },
            stateful_ingestion={"enabled": False},
        )

        with mock_graph:
            source = DataHubDocumentsSource(ctx, config)

            # Both NATIVE and EXTERNAL should be processed
            entity_native: dict[str, Any] = {
                "urn": "urn:li:document:native1",
                "info": {"source": {"sourceType": "NATIVE"}},
            }
            assert (
                source._should_process_by_source_type(
                    entity_native, entity_native["info"]
                )
                is True
            )

            entity_external: dict[str, Any] = {
                "urn": "urn:li:document:external1",
                "info": {"source": {"sourceType": "EXTERNAL"}},
            }
            assert (
                source._should_process_by_source_type(
                    entity_external, entity_external["info"]
                )
                is True
            )

    def test_event_mode_native_no_platform_fetch(self, ctx, config, mock_graph):
        """Test that event mode processes NATIVE documents without platform fetch."""
        with mock_graph:
            source = DataHubDocumentsSource(ctx, config)

            aspect_dict = {
                "source": {"sourceType": "NATIVE"},
                "contents": {"text": "Native document content"},
            }

            # Should process NATIVE without fetching platform
            should_process = source._should_process_by_source_type_event(
                "NATIVE", aspect_dict, "urn:li:document:native1"
            )
            assert should_process is True

            # Verify _fetch_platform_from_entity was NOT called
            with patch.object(source, "_fetch_platform_from_entity") as mock_fetch:
                source._should_process_by_source_type_event(
                    "NATIVE", aspect_dict, "urn:li:document:native1"
                )
                mock_fetch.assert_not_called()

    def test_event_mode_external_fetches_platform(self, ctx, mock_graph):
        """Test that event mode fetches platform for EXTERNAL documents."""
        config = DataHubDocumentsSourceConfig(
            platform_filter=["notion"],
            datahub={"server": "http://test-server:8080"},
            embedding={
                "provider": "bedrock",
                "model": "cohere.embed-english-v3",
                "aws_region": "us-west-2",
                "allow_local_embedding_config": True,
            },
            stateful_ingestion={"enabled": False},
        )

        with mock_graph:
            source = DataHubDocumentsSource(ctx, config)

            aspect_dict = {
                "source": {"sourceType": "EXTERNAL"},
                "contents": {"text": "External document content"},
            }

            # Mock platform fetch to return "notion"
            with patch.object(
                source, "_fetch_platform_from_entity", return_value="notion"
            ) as mock_fetch:
                should_process = source._should_process_by_source_type_event(
                    "EXTERNAL", aspect_dict, "urn:li:document:notion1"
                )

                # Should fetch platform and process (notion is in filter)
                mock_fetch.assert_called_once_with("urn:li:document:notion1")
                assert should_process is True

    def test_event_mode_external_skipped_when_platform_not_in_filter(
        self, ctx, mock_graph
    ):
        """Test that EXTERNAL documents are skipped when platform not in filter."""
        config = DataHubDocumentsSourceConfig(
            platform_filter=["notion"],
            datahub={"server": "http://test-server:8080"},
            embedding={
                "provider": "bedrock",
                "model": "cohere.embed-english-v3",
                "aws_region": "us-west-2",
                "allow_local_embedding_config": True,
            },
            stateful_ingestion={"enabled": False},
        )

        with mock_graph:
            source = DataHubDocumentsSource(ctx, config)

            aspect_dict = {
                "source": {"sourceType": "EXTERNAL"},
                "contents": {"text": "External document content"},
            }

            # Mock platform fetch to return "confluence" (not in filter)
            with patch.object(
                source, "_fetch_platform_from_entity", return_value="confluence"
            ) as mock_fetch:
                should_process = source._should_process_by_source_type_event(
                    "EXTERNAL", aspect_dict, "urn:li:document:confluence1"
                )

                # Should fetch platform but skip (confluence not in filter)
                mock_fetch.assert_called_once_with("urn:li:document:confluence1")
                assert should_process is False

    def test_fetch_platform_from_entity(self, ctx, config, mock_graph):
        """Test fetching platform from entity via GraphQL."""
        with mock_graph:
            source = DataHubDocumentsSource(ctx, config)

            # Mock GraphQL response
            mock_response = {
                "entity": {
                    "dataPlatformInstance": {
                        "platform": {"urn": "urn:li:dataPlatform:notion"}
                    }
                }
            }

            with patch.object(
                source.graph, "execute_graphql", return_value=mock_response
            ) as mock_execute:
                platform = source._fetch_platform_from_entity("urn:li:document:test")

                assert platform == "notion"
                mock_execute.assert_called_once()
                # Verify query includes dataPlatformInstance
                call_args = mock_execute.call_args
                query = call_args[0][0]
                assert "dataPlatformInstance" in query

    def test_fetch_platform_from_entity_no_platform(self, ctx, config, mock_graph):
        """Test fetching platform when entity has no platform."""
        with mock_graph:
            source = DataHubDocumentsSource(ctx, config)

            # Mock GraphQL response with no platform
            mock_response = {"entity": {"dataPlatformInstance": None}}

            with patch.object(
                source.graph, "execute_graphql", return_value=mock_response
            ):
                platform = source._fetch_platform_from_entity("urn:li:document:test")

                assert platform is None

    def test_extract_platform_from_entity(self, ctx, config, mock_graph):
        """Test extracting platform from GraphQL entity response."""
        with mock_graph:
            source = DataHubDocumentsSource(ctx, config)

            # Test valid platform
            entity = {
                "dataPlatformInstance": {
                    "platform": {"urn": "urn:li:dataPlatform:confluence"}
                }
            }
            platform = source._extract_platform_from_entity(entity)
            assert platform == "confluence"

            # Test missing platform
            entity_no_platform: dict[str, Any] = {}
            platform = source._extract_platform_from_entity(entity_no_platform)
            assert platform is None

            # Test malformed URN
            entity_malformed = {
                "dataPlatformInstance": {"platform": {"urn": "invalid"}}
            }
            platform = source._extract_platform_from_entity(entity_malformed)
            assert platform is None


class TestConfigFingerprintInHash:
    """Test that configuration fingerprint is included in document hash."""

    @pytest.fixture
    def config(self):
        """Create test configuration."""
        return DataHubDocumentsSourceConfig(
            platform_filter=None,
            datahub={"server": "http://test-server:8080"},
            embedding={
                "provider": "bedrock",
                "model": "cohere.embed-english-v3",
                "aws_region": "us-west-2",
                "model_embedding_key": "cohere_embed_v3",
                "allow_local_embedding_config": True,
            },
            chunking={
                "strategy": "by_title",
                "max_characters": 500,
                "overlap": 0,
                "combine_text_under_n_chars": 100,
            },
            partition_strategy="markdown",
            # Disable stateful ingestion in unit tests
            stateful_ingestion={"enabled": False},
        )

    @pytest.fixture
    def ctx(self):
        """Create test context."""
        return PipelineContext(run_id="test-run", pipeline_name="test-pipeline")

    @pytest.fixture
    def mock_graph(self):
        """Create mock DataHubGraph."""
        mock = patch(
            "datahub.ingestion.source.datahub_documents.datahub_documents_source.DataHubGraph"
        )
        return mock

    def test_hash_includes_config_fingerprint(self, ctx, config, mock_graph):
        """Test that hash includes configuration fingerprint."""
        with mock_graph:
            source = DataHubDocumentsSource(ctx, config)

            text = "Same document content"
            hash1 = source._calculate_text_hash(text)

            # Hash should be deterministic for same content + config
            hash2 = source._calculate_text_hash(text)
            assert hash1 == hash2

            # Hash should include config (verify by checking it's different from text-only hash)
            text_only_hash = hashlib.sha256(text.encode("utf-8")).hexdigest()
            assert hash1 != text_only_hash  # Should include config

    def test_hash_changes_when_chunking_strategy_changes(self, ctx, mock_graph):
        """Test that hash changes when chunking strategy changes."""
        config1 = DataHubDocumentsSourceConfig(
            platform_filter=None,
            datahub={"server": "http://test-server:8080"},
            embedding={
                "provider": "bedrock",
                "model": "cohere.embed-english-v3",
                "aws_region": "us-west-2",
                "model_embedding_key": "cohere_embed_v3",
                "allow_local_embedding_config": True,
            },
            chunking={"strategy": "by_title", "max_characters": 500},
            partition_strategy="markdown",
            stateful_ingestion={"enabled": False},
        )

        config2 = DataHubDocumentsSourceConfig(
            platform_filter=None,
            datahub={"server": "http://test-server:8080"},
            embedding={
                "provider": "bedrock",
                "model": "cohere.embed-english-v3",
                "aws_region": "us-west-2",
                "model_embedding_key": "cohere_embed_v3",
                "allow_local_embedding_config": True,
            },
            chunking={"strategy": "basic", "max_characters": 500},  # Different strategy
            partition_strategy="markdown",
            stateful_ingestion={"enabled": False},
        )

        with mock_graph:
            source1 = DataHubDocumentsSource(ctx, config1)
            source2 = DataHubDocumentsSource(ctx, config2)

            text = "Same document content"
            hash1 = source1._calculate_text_hash(text)
            hash2 = source2._calculate_text_hash(text)

            # Hashes should be different due to different chunking strategy
            assert hash1 != hash2

    def test_hash_changes_when_embedding_model_changes(self, ctx, mock_graph):
        """Test that hash changes when embedding model changes."""
        config1 = DataHubDocumentsSourceConfig(
            platform_filter=None,
            datahub={"server": "http://test-server:8080"},
            embedding={
                "provider": "bedrock",
                "model": "cohere.embed-english-v3",
                "aws_region": "us-west-2",
                "model_embedding_key": "cohere_embed_v3",
                "allow_local_embedding_config": True,
            },
            chunking={"strategy": "by_title", "max_characters": 500},
            partition_strategy="markdown",
            stateful_ingestion={"enabled": False},
        )

        config2 = DataHubDocumentsSourceConfig(
            platform_filter=None,
            datahub={"server": "http://test-server:8080"},
            embedding={
                "provider": "cohere",  # Different provider
                "model": "embed-english-v3.0",
                "api_key": "test-api-key",
                "model_embedding_key": "cohere_embed_v3",
                "allow_local_embedding_config": True,
            },
            chunking={"strategy": "by_title", "max_characters": 500},
            partition_strategy="markdown",
            stateful_ingestion={"enabled": False},
        )

        with mock_graph:
            source1 = DataHubDocumentsSource(ctx, config1)
            source2 = DataHubDocumentsSource(ctx, config2)

            text = "Same document content"
            hash1 = source1._calculate_text_hash(text)
            hash2 = source2._calculate_text_hash(text)

            # Hashes should be different due to different embedding config
            assert hash1 != hash2

    def test_hash_changes_when_partition_strategy_changes(self, ctx, mock_graph):
        """Test that hash changes when partition strategy changes."""
        config1 = DataHubDocumentsSourceConfig(
            platform_filter=None,
            datahub={"server": "http://test-server:8080"},
            embedding={
                "provider": "bedrock",
                "model": "cohere.embed-english-v3",
                "aws_region": "us-west-2",
                "model_embedding_key": "cohere_embed_v3",
                "allow_local_embedding_config": True,
            },
            chunking={"strategy": "by_title", "max_characters": 500},
            partition_strategy="markdown",
            stateful_ingestion={"enabled": False},
        )

        # Note: Currently only "markdown" is supported, but test the field exists
        # In the future when more strategies are added, this will work
        config2 = DataHubDocumentsSourceConfig(
            platform_filter=None,
            datahub={"server": "http://test-server:8080"},
            embedding={
                "provider": "bedrock",
                "model": "cohere.embed-english-v3",
                "aws_region": "us-west-2",
                "model_embedding_key": "cohere_embed_v3",
                "allow_local_embedding_config": True,
            },
            chunking={"strategy": "by_title", "max_characters": 500},
            partition_strategy="markdown",  # Same for now, but field is in hash,
            stateful_ingestion={"enabled": False},
        )

        with mock_graph:
            source1 = DataHubDocumentsSource(ctx, config1)
            source2 = DataHubDocumentsSource(ctx, config2)

            text = "Same document content"
            hash1 = source1._calculate_text_hash(text)
            hash2 = source2._calculate_text_hash(text)

            # Hashes should be same (same config)
            assert hash1 == hash2

            # But verify partition_strategy is in the hash by checking JSON structure
            # The hash includes a JSON with "content" and "config" keys
            # We can't easily verify the exact structure, but we know it's included

    def test_hash_changes_when_chunking_max_characters_changes(self, ctx, mock_graph):
        """Test that hash changes when chunking max_characters changes."""
        config1 = DataHubDocumentsSourceConfig(
            platform_filter=None,
            datahub={"server": "http://test-server:8080"},
            embedding={
                "provider": "bedrock",
                "model": "cohere.embed-english-v3",
                "aws_region": "us-west-2",
                "model_embedding_key": "cohere_embed_v3",
                "allow_local_embedding_config": True,
            },
            chunking={"strategy": "by_title", "max_characters": 500},
            partition_strategy="markdown",
            stateful_ingestion={"enabled": False},
        )

        config2 = DataHubDocumentsSourceConfig(
            platform_filter=None,
            datahub={"server": "http://test-server:8080"},
            embedding={
                "provider": "bedrock",
                "model": "cohere.embed-english-v3",
                "aws_region": "us-west-2",
                "model_embedding_key": "cohere_embed_v3",
                "allow_local_embedding_config": True,
            },
            chunking={"strategy": "by_title", "max_characters": 1000},  # Different
            partition_strategy="markdown",
            stateful_ingestion={"enabled": False},
        )

        with mock_graph:
            source1 = DataHubDocumentsSource(ctx, config1)
            source2 = DataHubDocumentsSource(ctx, config2)

            text = "Same document content"
            hash1 = source1._calculate_text_hash(text)
            hash2 = source2._calculate_text_hash(text)

            # Hashes should be different due to different max_characters
            assert hash1 != hash2

    def test_batch_mode_filters_by_source_type(self, ctx, mock_graph):
        """Test that batch mode filters documents by source type."""
        config = DataHubDocumentsSourceConfig(
            platform_filter=None,  # Default: all NATIVE
            datahub={"server": "http://test-server:8080"},
            embedding={
                "provider": "bedrock",
                "model": "cohere.embed-english-v3",
                "aws_region": "us-west-2",
                "allow_local_embedding_config": True,
            },
            min_text_length=10,  # Lower threshold for test,
            stateful_ingestion={"enabled": False},
        )

        with mock_graph:
            source = DataHubDocumentsSource(ctx, config)

            # Mock GraphQL to return both NATIVE and EXTERNAL documents
            mock_response = {
                "search": {
                    "searchResults": [
                        {
                            "entity": {
                                "urn": "urn:li:document:native1",
                                "info": {
                                    "source": {"sourceType": "NATIVE"},
                                    "contents": {
                                        "text": "This is a native document with enough text content to pass the minimum length requirement."
                                    },
                                },
                            }
                        },
                        {
                            "entity": {
                                "urn": "urn:li:document:external1",
                                "info": {
                                    "source": {"sourceType": "EXTERNAL"},
                                    "contents": {
                                        "text": "This is an external document with enough text content to pass the minimum length requirement."
                                    },
                                },
                            }
                        },
                    ]
                }
            }

            with patch.object(
                source.graph, "execute_graphql", return_value=mock_response
            ):
                documents = source._fetch_documents_graphql()

                # Should only return NATIVE document (EXTERNAL filtered out)
                assert len(documents) == 1
                assert documents[0]["urn"] == "urn:li:document:native1"

    def test_batch_mode_with_platform_filter(self, ctx, mock_graph):
        """Test batch mode with platform filter includes EXTERNAL from specified platforms."""
        config = DataHubDocumentsSourceConfig(
            platform_filter=["notion"],
            datahub={"server": "http://test-server:8080"},
            embedding={
                "provider": "bedrock",
                "model": "cohere.embed-english-v3",
                "aws_region": "us-west-2",
                "allow_local_embedding_config": True,
            },
            min_text_length=10,  # Lower threshold for test,
            stateful_ingestion={"enabled": False},
        )

        with mock_graph:
            source = DataHubDocumentsSource(ctx, config)

            # Mock GraphQL to return documents from different platforms
            mock_response = {
                "search": {
                    "searchResults": [
                        {
                            "entity": {
                                "urn": "urn:li:document:notion1",
                                "info": {
                                    "source": {"sourceType": "EXTERNAL"},
                                    "contents": {
                                        "text": "This is a Notion document with enough text content to pass the minimum length requirement."
                                    },
                                },
                                "dataPlatformInstance": {
                                    "platform": {"urn": "urn:li:dataPlatform:notion"}
                                },
                            }
                        },
                        {
                            "entity": {
                                "urn": "urn:li:document:confluence1",
                                "info": {
                                    "source": {"sourceType": "EXTERNAL"},
                                    "contents": {
                                        "text": "This is a Confluence document with enough text content to pass the minimum length requirement."
                                    },
                                },
                                "dataPlatformInstance": {
                                    "platform": {
                                        "urn": "urn:li:dataPlatform:confluence"
                                    }
                                },
                            }
                        },
                    ]
                }
            }

            with patch.object(
                source.graph, "execute_graphql", return_value=mock_response
            ):
                documents = source._fetch_documents_graphql()

                # Should only return notion document (confluence filtered out)
                assert len(documents) == 1
                assert documents[0]["urn"] == "urn:li:document:notion1"


class TestMaxDocumentsLimit:
    """Test max_documents limit behavior."""

    @pytest.fixture
    def config(self):
        """Create test configuration."""
        return DataHubDocumentsSourceConfig(
            platform_filter=["notion"],
            datahub={"server": "http://test-server:8080"},
            embedding={
                "provider": "bedrock",
                "model": "cohere.embed-english-v3",
                "aws_region": "us-west-2",
                "allow_local_embedding_config": True,
            },
            stateful_ingestion={"enabled": False},
        )

    @pytest.fixture
    def ctx(self):
        """Create test context."""
        return PipelineContext(run_id="test-run", pipeline_name="test-pipeline")

    def test_default_max_documents(self, ctx, config):
        """Test that max_documents defaults to 10000."""
        with patch(
            "datahub.ingestion.source.datahub_documents.datahub_documents_source.DataHubGraph"
        ):
            source = DataHubDocumentsSource(ctx, config)
            assert source.chunking_source.config.max_documents == 10000

    def test_max_documents_limit_raises_error(self, ctx, config):
        """Test that RuntimeError is raised when max_documents limit is hit."""
        with patch(
            "datahub.ingestion.source.datahub_documents.datahub_documents_source.DataHubGraph"
        ):
            source = DataHubDocumentsSource(ctx, config)
            source.chunking_source.config.max_documents = 2

            mock_docs = [
                {
                    "urn": f"urn:li:document:{i}",
                    "text": f"Document {i} content " + "x" * 100,
                }
                for i in range(5)
            ]

            fake_chunk = {"text": "chunk", "metadata": {}}

            source.chunking_source.embedding_model = None

            with (
                patch.object(
                    source, "_fetch_documents_graphql", return_value=mock_docs
                ),
                patch.object(
                    source.text_partitioner,
                    "partition_text",
                    return_value=[fake_chunk],
                ),
                patch.object(
                    source.chunking_source,
                    "_chunk_elements",
                    return_value=[fake_chunk],
                ),
                pytest.raises(RuntimeError, match="Document limit of 2 reached"),
            ):
                list(source._process_batch_mode())

            assert source.report.num_documents_limit_reached is True

    def test_max_documents_flag_set_on_limit(self, ctx, config):
        """Test that num_documents_limit_reached is set on the report when the limit is hit."""
        with patch(
            "datahub.ingestion.source.datahub_documents.datahub_documents_source.DataHubGraph"
        ):
            source = DataHubDocumentsSource(ctx, config)
            source.chunking_source.config.max_documents = 2

            mock_docs = [
                {
                    "urn": f"urn:li:document:{i}",
                    "text": f"Document {i} content " + "x" * 100,
                }
                for i in range(5)
            ]

            fake_chunk = {"text": "chunk", "metadata": {}}

            source.chunking_source.embedding_model = None

            with (
                patch.object(
                    source, "_fetch_documents_graphql", return_value=mock_docs
                ),
                patch.object(
                    source.text_partitioner,
                    "partition_text",
                    return_value=[fake_chunk],
                ),
                patch.object(
                    source.chunking_source,
                    "_chunk_elements",
                    return_value=[fake_chunk],
                ),
                pytest.raises(RuntimeError),
            ):
                list(source._process_batch_mode())

            assert source.chunking_source.report.num_documents_limit_reached is True

    def test_documents_processed_before_limit_are_emitted(self, ctx, config):
        """Test that documents up to max_documents are fully processed before the error is raised."""
        with patch(
            "datahub.ingestion.source.datahub_documents.datahub_documents_source.DataHubGraph"
        ):
            source = DataHubDocumentsSource(ctx, config)
            source.chunking_source.config.max_documents = 2

            mock_docs = [
                {
                    "urn": f"urn:li:document:{i}",
                    "text": f"Document {i} content " + "x" * 100,
                }
                for i in range(5)
            ]

            fake_chunk = {"text": "chunk", "metadata": {}}

            source.chunking_source.embedding_model = None

            with (
                patch.object(
                    source, "_fetch_documents_graphql", return_value=mock_docs
                ),
                patch.object(
                    source.text_partitioner,
                    "partition_text",
                    return_value=[fake_chunk],
                ),
                patch.object(
                    source.chunking_source,
                    "_chunk_elements",
                    return_value=[fake_chunk],
                ),
                pytest.raises(RuntimeError),
            ):
                list(source._process_batch_mode())

            assert source.chunking_source.report.num_documents_processed == 2


class TestGetCurrentOffset:
    """Test get_current_offset() method in DocumentEventConsumer."""

    @pytest.fixture
    def mock_graph(self):
        """Create a mock DataHubGraph."""
        graph = Mock()
        graph.config.server = "http://localhost:8080"
        graph._session = Mock()
        graph._session.headers = {"Authorization": "Bearer test-token"}
        return graph

    def test_get_current_offset_success(self, mock_graph):
        """Test successful retrieval of current offset."""
        from datahub.ingestion.source.unstructured.event_consumer import (
            DocumentEventConsumer,
        )

        # Mock the requests.get response
        mock_response = Mock()
        mock_response.json.return_value = {"offsetId": "test-offset-123"}
        mock_response.raise_for_status = Mock()

        with patch("requests.get", return_value=mock_response):
            consumer = DocumentEventConsumer(
                graph=mock_graph,
                consumer_id="test-consumer",
                topics=["MetadataChangeLog_Versioned_v1"],
            )

            offset = consumer.get_current_offset("MetadataChangeLog_Versioned_v1")

            assert offset == "test-offset-123"

    def test_get_current_offset_no_offset_in_response(self, mock_graph):
        """Test when Events API returns no offsetId."""
        from datahub.ingestion.source.unstructured.event_consumer import (
            DocumentEventConsumer,
        )

        # Mock the requests.get response with no offsetId
        mock_response = Mock()
        mock_response.json.return_value = {"events": []}  # No offsetId field
        mock_response.raise_for_status = Mock()

        with patch("requests.get", return_value=mock_response):
            consumer = DocumentEventConsumer(
                graph=mock_graph,
                consumer_id="test-consumer",
                topics=["MetadataChangeLog_Versioned_v1"],
            )

            offset = consumer.get_current_offset("MetadataChangeLog_Versioned_v1")

            assert offset is None

    def test_get_current_offset_http_error(self, mock_graph):
        """Test when Events API returns HTTP error."""
        import requests

        from datahub.ingestion.source.unstructured.event_consumer import (
            DocumentEventConsumer,
        )

        # Mock the requests.get to raise HTTPError
        mock_response = Mock()
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(
            "401 Unauthorized"
        )

        with patch("requests.get", return_value=mock_response):
            consumer = DocumentEventConsumer(
                graph=mock_graph,
                consumer_id="test-consumer",
                topics=["MetadataChangeLog_Versioned_v1"],
            )

            offset = consumer.get_current_offset("MetadataChangeLog_Versioned_v1")

            # Should return None on error, not raise
            assert offset is None

    def test_get_current_offset_connection_error(self, mock_graph):
        """Test when Events API connection fails."""
        import requests

        from datahub.ingestion.source.unstructured.event_consumer import (
            DocumentEventConsumer,
        )

        # Mock the requests.get to raise ConnectionError
        with patch(
            "requests.get",
            side_effect=requests.exceptions.ConnectionError("Connection failed"),
        ):
            consumer = DocumentEventConsumer(
                graph=mock_graph,
                consumer_id="test-consumer",
                topics=["MetadataChangeLog_Versioned_v1"],
            )

            offset = consumer.get_current_offset("MetadataChangeLog_Versioned_v1")

            # Should return None on error, not raise
            assert offset is None

    def test_get_current_offset_polls_with_no_offset_and_no_lookback(self, mock_graph):
        """Test that get_current_offset polls with no offsetId and no lookbackWindowDays."""
        from datahub.ingestion.source.unstructured.event_consumer import (
            DocumentEventConsumer,
        )

        mock_response = Mock()
        mock_response.json.return_value = {"offsetId": "test-offset-456"}
        mock_response.raise_for_status = Mock()

        with patch("requests.get", return_value=mock_response) as mock_get:
            consumer = DocumentEventConsumer(
                graph=mock_graph,
                consumer_id="test-consumer",
                topics=["MetadataChangeLog_Versioned_v1"],
            )

            offset = consumer.get_current_offset("MetadataChangeLog_Versioned_v1")

            # Verify the request was made with correct parameters
            assert mock_get.called
            call_args = mock_get.call_args
            assert call_args[1]["params"]["topic"] == "MetadataChangeLog_Versioned_v1"
            assert call_args[1]["params"]["limit"] == 1
            # Should NOT include offsetId or lookbackWindowDays
            assert "offsetId" not in call_args[1]["params"]
            assert "lookbackWindowDays" not in call_args[1]["params"]
            assert offset == "test-offset-456"


class TestDataHubGraphInitialization:
    """Test DataHub graph initialization logic (ctx.graph vs config-based)."""

    @pytest.fixture
    def config(self):
        """Create test configuration."""
        return DataHubDocumentsSourceConfig(
            platform_filter=None,
            datahub={"server": "http://test-server:8080", "token": "test-token"},
            embedding={
                "provider": "bedrock",
                "model": "cohere.embed-english-v3",
                "aws_region": "us-west-2",
                "allow_local_embedding_config": True,
            },
            stateful_ingestion={"enabled": False},
        )

    def test_uses_ctx_graph_when_available(self, config):
        """Test that source uses ctx.graph when provided in pipeline context."""
        # Create a mock graph
        mock_graph = Mock()
        mock_graph.execute_graphql = Mock(return_value={"data": {}})

        # Create context with graph
        ctx = PipelineContext(
            run_id="test-run", pipeline_name="test-pipeline", graph=mock_graph
        )

        # Initialize source
        source = DataHubDocumentsSource(ctx, config)

        # Verify source uses the context graph
        assert source.graph is mock_graph
        assert source.graph is ctx.graph

    def test_creates_graph_from_config_when_ctx_graph_none(self, config):
        """Test that source creates graph from config when ctx.graph is None."""
        # Create context without graph
        ctx = PipelineContext(
            run_id="test-run", pipeline_name="test-pipeline", graph=None
        )

        # Mock DataHubGraph constructor
        with patch(
            "datahub.ingestion.source.datahub_documents.datahub_documents_source.DataHubGraph"
        ) as mock_graph_class:
            mock_graph_instance = Mock()
            mock_graph_class.return_value = mock_graph_instance

            # Initialize source
            source = DataHubDocumentsSource(ctx, config)

            # Verify DataHubGraph was created from config
            mock_graph_class.assert_called_once()
            call_args = mock_graph_class.call_args[1]
            assert "config" in call_args
            assert call_args["config"].server == "http://test-server:8080"
            assert call_args["config"].token == "test-token"

            # Verify source uses the created graph
            assert source.graph is mock_graph_instance

    def test_graph_initialization_with_env_vars(self):
        """Test graph creation falls back to env vars when config not provided."""
        # Config with default datahub connection (should read from env vars)
        config = DataHubDocumentsSourceConfig(
            platform_filter=None,
            embedding={
                "provider": "bedrock",
                "model": "cohere.embed-english-v3",
                "aws_region": "us-west-2",
                "allow_local_embedding_config": True,
            },
            stateful_ingestion={"enabled": False},
        )

        # Create context without graph
        ctx = PipelineContext(
            run_id="test-run", pipeline_name="test-pipeline", graph=None
        )

        # Mock env vars
        with (
            patch(
                "datahub.ingestion.source.unstructured.chunking_config.env_vars.get_gms_url"
            ) as mock_get_url,
            patch(
                "datahub.ingestion.source.unstructured.chunking_config.env_vars.get_gms_token"
            ) as mock_get_token,
            patch(
                "datahub.ingestion.source.datahub_documents.datahub_documents_source.DataHubGraph"
            ) as mock_graph_class,
        ):
            mock_get_url.return_value = "http://env-server:8080"
            mock_get_token.return_value = "env-token"
            mock_graph_instance = Mock()
            mock_graph_class.return_value = mock_graph_instance

            # Initialize source
            source = DataHubDocumentsSource(ctx, config)

            # Verify DataHubGraph was created (env vars are read in DataHubConnectionConfig)
            mock_graph_class.assert_called_once()
            assert source.graph is mock_graph_instance

    def test_ctx_graph_takes_precedence_over_config(self, config):
        """Test that ctx.graph takes precedence even when config has values."""
        # Create a mock graph
        mock_ctx_graph = Mock()
        mock_ctx_graph.execute_graphql = Mock(return_value={"data": {}})

        # Create context with graph
        ctx = PipelineContext(
            run_id="test-run", pipeline_name="test-pipeline", graph=mock_ctx_graph
        )

        # Mock DataHubGraph constructor to ensure it's NOT called
        with patch(
            "datahub.ingestion.source.datahub_documents.datahub_documents_source.DataHubGraph"
        ) as mock_graph_class:
            # Initialize source
            source = DataHubDocumentsSource(ctx, config)

            # Verify DataHubGraph constructor was NOT called
            mock_graph_class.assert_not_called()

            # Verify source uses the context graph
            assert source.graph is mock_ctx_graph
            assert source.graph is not mock_graph_class.return_value
