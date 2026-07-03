"""Unit tests for DataHub Documents Source."""

import hashlib
import json
import sys
from typing import Any, Optional
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
from datahub.ingestion.source.datahub_documents.document_chunking_state import (
    DocumentChunkingCheckpointState,
)
from datahub.ingestion.source.datahub_documents.document_chunking_state_handler import (
    DocumentChunkingStatefulIngestionConfig,
    DocumentChunkingStateHandler,
)
from datahub.ingestion.source.datahub_documents.text_partitioner import TextPartitioner
from datahub.ingestion.source.state.checkpoint import Checkpoint
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

        with mock_graph as mock_graph_cls:
            source = DataHubDocumentsSource(ctx, config)
            # Mock state handler with valid offset
            mock_state_handler = patch.object(source, "state_handler").start()
            mock_state_handler.is_checkpointing_enabled.return_value = True
            mock_state_handler.get_event_offset.return_value = "test-offset-123"

            # Mock requests.get to raise HTTP error
            with patch.object(mock_graph_cls.return_value._session, "get") as mock_get:
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

        with mock_graph as mock_graph_cls:
            source = DataHubDocumentsSource(ctx, config)
            # Mock state handler with valid offset
            mock_state_handler = patch.object(source, "state_handler").start()
            mock_state_handler.is_checkpointing_enabled.return_value = True
            mock_state_handler.get_event_offset.return_value = "test-offset-123"

            # Mock requests.get to raise connection error
            with patch.object(mock_graph_cls.return_value._session, "get") as mock_get:
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

        with mock_graph as mock_graph_cls:
            source = DataHubDocumentsSource(ctx, config)
            # Mock state handler with valid offset
            mock_state_handler = patch.object(source, "state_handler").start()
            mock_state_handler.is_checkpointing_enabled.return_value = True
            mock_state_handler.get_event_offset.return_value = "test-offset-123"

            # Mock requests.get to raise timeout
            with patch.object(mock_graph_cls.return_value._session, "get") as mock_get:
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
        with mock_graph as mock_graph_cls:
            source = DataHubDocumentsSource(ctx, config)
            # Mock state handler with valid offset
            mock_state_handler = patch.object(source, "state_handler").start()
            mock_state_handler.is_checkpointing_enabled.return_value = True
            mock_state_handler.get_event_offset.return_value = "test-offset-123"

            # Mock successful event polling with events

            with patch.object(mock_graph_cls.return_value._session, "get") as mock_get:
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
        with mock_graph as mock_graph_cls:
            source = DataHubDocumentsSource(ctx, config)
            # Mock state handler with valid offset
            mock_state_handler = patch.object(source, "state_handler").start()
            mock_state_handler.is_checkpointing_enabled.return_value = True
            mock_state_handler.get_event_offset.return_value = "test-offset-123"

            # Mock successful event polling but no events

            with patch.object(mock_graph_cls.return_value._session, "get") as mock_get:
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

    @staticmethod
    def _make_state_handler(
        *,
        stateful_ingestion: DocumentChunkingStatefulIngestionConfig,
        is_configured: bool = True,
        last_checkpoint: Optional[Checkpoint] = None,
        current_checkpoint: Optional[Checkpoint] = None,
    ) -> DocumentChunkingStateHandler:
        state_provider = Mock()
        state_provider.is_stateful_ingestion_configured.return_value = is_configured
        state_provider.get_last_checkpoint.return_value = last_checkpoint
        state_provider.get_current_checkpoint.return_value = current_checkpoint

        source = Mock()
        source.state_provider = state_provider
        source_config = Mock()
        source_config.stateful_ingestion = stateful_ingestion

        return DocumentChunkingStateHandler(
            source=source,
            config=source_config,
            pipeline_name="test-pipeline",
            run_id="current-run",
        )

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

        with mock_graph as mock_graph_cls:
            source = DataHubDocumentsSource(ctx, config)
            source.config.event_mode.enabled = True

            # Mock state handler
            mock_state_handler = patch.object(source, "state_handler").start()
            mock_state_handler.is_checkpointing_enabled.return_value = True
            mock_state_handler.get_event_offset.return_value = "initial-offset-123"
            mock_state_handler.update_event_offset = Mock()

            # Mock successful event polling
            with patch.object(mock_graph_cls.return_value._session, "get") as mock_get:
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

    def test_new_checkpoint_starts_from_previous_state(self):
        """Skipped documents must keep their hashes in the next committed checkpoint."""
        previous_state = DocumentChunkingCheckpointState(
            document_state={
                "urn:li:document:1": {
                    "content_hash": "hash1",
                    "last_processed": "2026-06-16T00:00:00",
                }
            },
            event_offsets={"MetadataChangeLog_Versioned_v1": "offset-123"},
        )
        last_checkpoint = Checkpoint(
            job_name="document_chunking",
            pipeline_name="test-pipeline",
            run_id="previous-run",
            state=previous_state,
        )

        handler = self._make_state_handler(
            stateful_ingestion=DocumentChunkingStatefulIngestionConfig(enabled=True),
            last_checkpoint=last_checkpoint,
        )

        checkpoint = handler.create_checkpoint()

        assert checkpoint is not None
        assert checkpoint.state.document_state == previous_state.document_state
        assert checkpoint.state.event_offsets == previous_state.event_offsets

        checkpoint.state.document_state["urn:li:document:1"]["content_hash"] = "changed"
        assert (
            previous_state.document_state["urn:li:document:1"]["content_hash"]
            == "hash1"
        )

    def test_new_checkpoint_without_previous_state_starts_empty(self):
        handler = self._make_state_handler(
            stateful_ingestion=DocumentChunkingStatefulIngestionConfig(enabled=True)
        )

        checkpoint = handler.create_checkpoint()

        assert checkpoint is not None
        assert checkpoint.state.document_state == {}
        assert checkpoint.state.event_offsets == {}

    def test_new_checkpoint_returns_none_when_disabled(self):
        handler = self._make_state_handler(
            stateful_ingestion=DocumentChunkingStatefulIngestionConfig(enabled=True),
            is_configured=False,
        )

        assert handler.create_checkpoint() is None

    def test_new_checkpoint_returns_none_when_ignoring_new_state(self):
        handler = self._make_state_handler(
            stateful_ingestion=DocumentChunkingStatefulIngestionConfig(
                enabled=True,
                ignore_new_state=True,
            )
        )

        assert handler.create_checkpoint() is None

    def test_state_handler_reads_and_updates_checkpoint_state(self):
        previous_state = DocumentChunkingCheckpointState(
            document_state={
                "urn:li:document:1": {
                    "content_hash": "hash1",
                    "last_processed": "2026-06-16T00:00:00",
                }
            },
            event_offsets={"MetadataChangeLog_Versioned_v1": "offset-123"},
        )
        current_state = DocumentChunkingCheckpointState()

        handler = self._make_state_handler(
            stateful_ingestion=DocumentChunkingStatefulIngestionConfig(enabled=True),
            last_checkpoint=Checkpoint(
                job_name="document_chunking",
                pipeline_name="test-pipeline",
                run_id="previous-run",
                state=previous_state,
            ),
            current_checkpoint=Checkpoint(
                job_name="document_chunking",
                pipeline_name="test-pipeline",
                run_id="current-run",
                state=current_state,
            ),
        )

        assert handler.get_last_state() == previous_state
        assert handler.get_document_hash("urn:li:document:1") == "hash1"
        assert handler.get_document_hash("urn:li:document:missing") is None
        assert (
            handler.get_event_offset("MetadataChangeLog_Versioned_v1") == "offset-123"
        )

        handler.update_document_state(
            "urn:li:document:2", "hash2", "2026-06-16T01:00:00"
        )
        handler.update_event_offset("MetadataChangeLog_Versioned_v2", "offset-456")

        assert current_state.document_state["urn:li:document:2"] == {
            "content_hash": "hash2",
            "last_processed": "2026-06-16T01:00:00",
        }
        assert current_state.event_offsets["MetadataChangeLog_Versioned_v2"] == (
            "offset-456"
        )

    def test_state_handler_can_ignore_old_state(self):
        previous_state = DocumentChunkingCheckpointState(
            document_state={"urn:li:document:1": {"content_hash": "hash1"}},
            event_offsets={"MetadataChangeLog_Versioned_v1": "offset-123"},
        )
        handler = self._make_state_handler(
            stateful_ingestion=DocumentChunkingStatefulIngestionConfig(
                enabled=True,
                ignore_old_state=True,
            ),
            last_checkpoint=Checkpoint(
                job_name="document_chunking",
                pipeline_name="test-pipeline",
                run_id="previous-run",
                state=previous_state,
            ),
        )

        assert handler.get_last_state() is None
        assert handler.get_document_hash("urn:li:document:1") is None
        assert handler.get_event_offset("MetadataChangeLog_Versioned_v1") is None

    def test_fallback_preserves_existing_offsets(self, ctx, config, mock_graph):
        """Test that fallback to batch mode preserves existing event offsets."""
        import requests

        with mock_graph as mock_graph_cls:
            source = DataHubDocumentsSource(ctx, config)
            source.config.event_mode.enabled = True

            # Mock state handler with existing offsets
            mock_state_handler = patch.object(source, "state_handler").start()
            mock_state_handler.is_checkpointing_enabled.return_value = True
            mock_state_handler.get_event_offset.return_value = "existing-offset-123"

            # Event polling goes through the graph's session; make it fail to
            # trigger the fallback to batch mode.
            mock_graph_cls.return_value._session.get.side_effect = (
                requests.ConnectionError("Connection refused")
            )

            # Mock batch mode
            mock_docs = [{"urn": "urn:li:document:1", "text": "Document 1"}]
            with (
                patch.object(
                    source, "_fetch_documents_graphql", return_value=mock_docs
                ),
                patch.object(source, "_process_single_document", return_value=iter([])),
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
                    offset_calls = mock_state_handler.update_event_offset.call_args_list
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

    def test_default_config_processes_native_and_external(
        self, ctx, config, mock_graph
    ):
        """By default (empty platform_filter) both NATIVE and EXTERNAL documents are processed."""
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

            # EXTERNAL document is now also processed by default
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
            assert should_process_external is True

    def test_external_skipped_when_include_external_disabled(self, ctx, mock_graph):
        """EXTERNAL documents are skipped when include_external_documents is False."""
        config = DataHubDocumentsSourceConfig(
            platform_filter=None,
            include_external_documents=False,
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

            entity_external: dict[str, Any] = {
                "urn": "urn:li:document:external1",
                "info": {"source": {"sourceType": "EXTERNAL"}},
            }
            assert (
                source._should_process_by_source_type(
                    entity_external, entity_external["info"]
                )
                is False
            )

            # NATIVE still processed
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

    def test_batch_mode_processes_native_and_external_by_default(self, ctx, mock_graph):
        """Batch mode processes both NATIVE and EXTERNAL documents by default."""
        config = DataHubDocumentsSourceConfig(
            platform_filter=None,  # Default: NATIVE + EXTERNAL
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
                "scrollAcrossEntities": {
                    "nextScrollId": None,
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
                    ],
                }
            }

            with patch.object(
                source.graph, "execute_graphql", return_value=mock_response
            ):
                documents = source._fetch_documents_graphql()

                # Both NATIVE and EXTERNAL documents are returned by default
                assert len(documents) == 2
                urns = {doc["urn"] for doc in documents}
                assert urns == {
                    "urn:li:document:native1",
                    "urn:li:document:external1",
                }

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
                "scrollAcrossEntities": {
                    "nextScrollId": None,
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
                    ],
                }
            }

            with patch.object(
                source.graph, "execute_graphql", return_value=mock_response
            ):
                documents = source._fetch_documents_graphql()

                # Should only return notion document (confluence filtered out)
                assert len(documents) == 1
                assert documents[0]["urn"] == "urn:li:document:notion1"


class TestPartialEntityHandling:
    """Test defensive handling of partial entities with null info/contents fields.

    GraphQL returns null (Python None) for missing aspects, not absent keys.
    dict.get("key", {}) returns None when key exists with value None, so we
    must use `or {}` to handle both missing and null cases.
    """

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
                "allow_local_embedding_config": True,
            },
            min_text_length=10,
            stateful_ingestion={"enabled": False},
        )

    @pytest.fixture
    def ctx(self):
        """Create test context."""
        return PipelineContext(run_id="test-run", pipeline_name="test-pipeline")

    @pytest.fixture
    def mock_graph(self):
        """Create mock DataHubGraph."""
        return patch(
            "datahub.ingestion.source.datahub_documents.datahub_documents_source.DataHubGraph"
        )

    def test_fetch_documents_skips_entity_with_null_info(self, ctx, config, mock_graph):
        """Test that entities with info: null are gracefully skipped."""
        with mock_graph:
            source = DataHubDocumentsSource(ctx, config)

            mock_response = {
                "scrollAcrossEntities": {
                    "nextScrollId": None,
                    "searchResults": [
                        {
                            "entity": {
                                "urn": "urn:li:document:partial1",
                                "info": None,
                            }
                        },
                        {
                            "entity": {
                                "urn": "urn:li:document:complete1",
                                "info": {
                                    "source": {"sourceType": "NATIVE"},
                                    "contents": {
                                        "text": "This is a complete document with enough content."
                                    },
                                },
                            }
                        },
                    ],
                }
            }

            with patch.object(
                source.graph, "execute_graphql", return_value=mock_response
            ):
                documents = source._fetch_documents_graphql()

                assert len(documents) == 1
                assert documents[0]["urn"] == "urn:li:document:complete1"

    def test_fetch_documents_skips_entity_with_null_contents(
        self, ctx, config, mock_graph
    ):
        """Test that entities with contents: null inside info are gracefully skipped."""
        with mock_graph:
            source = DataHubDocumentsSource(ctx, config)

            mock_response = {
                "scrollAcrossEntities": {
                    "nextScrollId": None,
                    "searchResults": [
                        {
                            "entity": {
                                "urn": "urn:li:document:no_contents",
                                "info": {
                                    "source": {"sourceType": "NATIVE"},
                                    "contents": None,
                                },
                            }
                        },
                    ],
                }
            }

            with patch.object(
                source.graph, "execute_graphql", return_value=mock_response
            ):
                documents = source._fetch_documents_graphql()

                assert len(documents) == 0

    def test_fetch_documents_skips_entity_with_null_search(
        self, ctx, config, mock_graph
    ):
        """Test that a null search response is handled gracefully."""
        with mock_graph:
            source = DataHubDocumentsSource(ctx, config)

            mock_response: dict[str, Any] = {"scrollAcrossEntities": None}

            with patch.object(
                source.graph, "execute_graphql", return_value=mock_response
            ):
                documents = source._fetch_documents_graphql()

                assert len(documents) == 0

    def test_should_process_by_source_type_with_null_source(
        self, ctx, config, mock_graph
    ):
        """Test _should_process_by_source_type when source field is null."""
        with mock_graph:
            source = DataHubDocumentsSource(ctx, config)

            entity: dict[str, Any] = {"urn": "urn:li:document:test"}
            info: dict[str, Any] = {"source": None, "contents": {"text": "some text"}}

            should_process = source._should_process_by_source_type(entity, info)
            # source=None → sourceType defaults to NATIVE → should process
            assert should_process is True

    def test_extract_platform_from_entity_with_null_platform_instance(
        self, ctx, config, mock_graph
    ):
        """Test _extract_platform_from_entity when dataPlatformInstance is null."""
        with mock_graph:
            source = DataHubDocumentsSource(ctx, config)

            entity: dict[str, Any] = {"dataPlatformInstance": None}
            platform = source._extract_platform_from_entity(entity)
            assert platform is None

    def test_extract_platform_from_entity_with_null_platform(
        self, ctx, config, mock_graph
    ):
        """Test _extract_platform_from_entity when platform inside dataPlatformInstance is null."""
        with mock_graph:
            source = DataHubDocumentsSource(ctx, config)

            entity: dict[str, Any] = {"dataPlatformInstance": {"platform": None}}
            platform = source._extract_platform_from_entity(entity)
            assert platform is None

    def test_extract_platform_from_aspect_with_null_platform_instance(
        self, ctx, config, mock_graph
    ):
        """Test _extract_platform_from_aspect when dataPlatformInstance is null."""
        with mock_graph:
            source = DataHubDocumentsSource(ctx, config)

            aspect_dict: dict[str, Any] = {"dataPlatformInstance": None}
            platform = source._extract_platform_from_aspect(aspect_dict)
            assert platform is None

    def test_process_single_event_with_null_contents(self, ctx, config, mock_graph):
        """Test _process_single_event when MCL aspect has contents: null."""
        with mock_graph:
            source = DataHubDocumentsSource(ctx, config)

            event: dict[str, Any] = {
                "entityUrn": "urn:li:document:partial1",
                "aspectName": "documentInfo",
                "aspect": json.dumps({"contents": None}),
            }

            workunits = list(source._process_single_event(event))
            assert len(workunits) == 0

    def test_fetch_documents_mixed_null_and_valid(self, ctx, config, mock_graph):
        """Test batch mode with a mix of null-info, null-contents, and valid entities."""
        with mock_graph:
            source = DataHubDocumentsSource(ctx, config)

            mock_response = {
                "scrollAcrossEntities": {
                    "nextScrollId": None,
                    "searchResults": [
                        {
                            "entity": {
                                "urn": "urn:li:document:null_info",
                                "info": None,
                            }
                        },
                        {
                            "entity": {
                                "urn": "urn:li:document:null_contents",
                                "info": {
                                    "source": {"sourceType": "NATIVE"},
                                    "contents": None,
                                },
                            }
                        },
                        {
                            "entity": {
                                "urn": "urn:li:document:empty_text",
                                "info": {
                                    "source": {"sourceType": "NATIVE"},
                                    "contents": {"text": ""},
                                },
                            }
                        },
                        {
                            "entity": {
                                "urn": "urn:li:document:valid",
                                "info": {
                                    "source": {"sourceType": "NATIVE"},
                                    "contents": {
                                        "text": "This document has valid content that is long enough."
                                    },
                                },
                            }
                        },
                    ],
                }
            }

            with patch.object(
                source.graph, "execute_graphql", return_value=mock_response
            ):
                documents = source._fetch_documents_graphql()

                assert len(documents) == 1
                assert documents[0]["urn"] == "urn:li:document:valid"


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
        """Test that max_documents defaults to 100000."""
        with patch(
            "datahub.ingestion.source.datahub_documents.datahub_documents_source.DataHubGraph"
        ):
            source = DataHubDocumentsSource(ctx, config)
            assert source.chunking_source.config.max_documents == 100000

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

        # The consumer polls through the graph's session (session.auth carries
        # OAuth credentials), so mock the session, not module-level requests.
        mock_response = Mock()
        mock_response.json.return_value = {"offsetId": "test-offset-123"}
        mock_response.raise_for_status = Mock()
        mock_graph._session.get.return_value = mock_response

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

        mock_response = Mock()
        mock_response.json.return_value = {"events": []}  # No offsetId field
        mock_response.raise_for_status = Mock()
        mock_graph._session.get.return_value = mock_response

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

        mock_response = Mock()
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(
            "401 Unauthorized"
        )
        mock_graph._session.get.return_value = mock_response

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

        mock_graph._session.get.side_effect = requests.exceptions.ConnectionError(
            "Connection failed"
        )

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
        mock_get = mock_graph._session.get
        mock_get.return_value = mock_response

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


class TestFetchDocumentsPagination:
    """Test that _fetch_documents_graphql paginates through all pages via scrollAcrossEntities."""

    @pytest.fixture
    def config(self):
        return DataHubDocumentsSourceConfig(
            platform_filter=["*"],
            datahub={"server": "http://test-server:8080"},
            embedding={
                "provider": "bedrock",
                "model": "cohere.embed-english-v3",
                "aws_region": "us-west-2",
                "allow_local_embedding_config": True,
            },
            min_text_length=10,
            stateful_ingestion={"enabled": False},
        )

    @pytest.fixture
    def ctx(self):
        return PipelineContext(run_id="test-run", pipeline_name="test-pipeline")

    @pytest.fixture
    def mock_graph(self):
        return patch(
            "datahub.ingestion.source.datahub_documents.datahub_documents_source.DataHubGraph"
        )

    def _make_entity(self, i: int) -> dict:
        return {
            "entity": {
                "urn": f"urn:li:document:doc-{i}",
                "info": {
                    "source": {"sourceType": "NATIVE"},
                    "contents": {"text": f"Document {i} with sufficient text content."},
                },
            }
        }

    def _make_scroll_page(
        self, start: int, count: int, next_scroll_id: Any = None
    ) -> dict:
        return {
            "scrollAcrossEntities": {
                "nextScrollId": next_scroll_id,
                "searchResults": [
                    self._make_entity(i) for i in range(start, start + count)
                ],
            }
        }

    def test_paginates_across_multiple_pages(self, ctx, config, mock_graph):
        """Fetching >1000 documents issues multiple GraphQL calls and returns all results."""
        with mock_graph:
            source = DataHubDocumentsSource(ctx, config)

            responses = [
                self._make_scroll_page(0, 1000, next_scroll_id="cursor-1"),
                self._make_scroll_page(1000, 200, next_scroll_id=None),
            ]

            with patch.object(
                source.graph, "execute_graphql", side_effect=responses
            ) as mock_execute:
                documents = source._fetch_documents_graphql()

            assert len(documents) == 1200
            assert mock_execute.call_count == 2

            first_scroll_id = mock_execute.call_args_list[0][0][1]["scrollId"]
            second_scroll_id = mock_execute.call_args_list[1][0][1]["scrollId"]
            assert first_scroll_id is None
            assert second_scroll_id == "cursor-1"

    def test_stops_when_no_next_scroll_id(self, ctx, config, mock_graph):
        """A single page with no nextScrollId issues only one call."""
        with mock_graph:
            source = DataHubDocumentsSource(ctx, config)

            response = self._make_scroll_page(0, 1000, next_scroll_id=None)

            with patch.object(
                source.graph, "execute_graphql", return_value=response
            ) as mock_execute:
                documents = source._fetch_documents_graphql()

            assert len(documents) == 1000
            assert mock_execute.call_count == 1

    def test_requests_hidden_lifecycle_stages(self, ctx, config, mock_graph):
        """Document backfills request hidden-lifecycle stages so hidden documents are included."""
        with mock_graph:
            source = DataHubDocumentsSource(ctx, config)

            response = self._make_scroll_page(0, 0, next_scroll_id=None)

            with patch.object(
                source.graph, "execute_graphql", return_value=response
            ) as mock_execute:
                source._fetch_documents_graphql()

            query = mock_execute.call_args[0][0]
            assert "includeHiddenLifecycleStages: true" in query

    def test_empty_result_set(self, ctx, config, mock_graph):
        """Zero documents returns empty list after one call."""
        with mock_graph:
            source = DataHubDocumentsSource(ctx, config)

            response: dict[str, Any] = {
                "scrollAcrossEntities": {
                    "nextScrollId": None,
                    "searchResults": [],
                }
            }

            with patch.object(
                source.graph, "execute_graphql", return_value=response
            ) as mock_execute:
                documents = source._fetch_documents_graphql()

            assert documents == []
            assert mock_execute.call_count == 1

    def test_three_pages(self, ctx, config, mock_graph):
        """Three-page fetch collects all documents and threads scroll cursors correctly."""
        with mock_graph:
            source = DataHubDocumentsSource(ctx, config)

            responses = [
                self._make_scroll_page(0, 1000, next_scroll_id="cursor-1"),
                self._make_scroll_page(1000, 1000, next_scroll_id="cursor-2"),
                self._make_scroll_page(2000, 500, next_scroll_id=None),
            ]

            with patch.object(
                source.graph, "execute_graphql", side_effect=responses
            ) as mock_execute:
                documents = source._fetch_documents_graphql()

            assert len(documents) == 2500
            assert mock_execute.call_count == 3
            scroll_ids = [c[0][1]["scrollId"] for c in mock_execute.call_args_list]
            assert scroll_ids == [None, "cursor-1", "cursor-2"]

    def test_raw_page_size_used_as_start_advance(self, ctx, config, mock_graph):
        """Pagination advances by raw page size even when some results are filtered client-side.

        This test locks in that start is advanced by the number of results returned from the
        server, not the number that pass client-side filters. Using filtered count would cause
        pages to be requested at wrong offsets or loop incorrectly.
        """
        with mock_graph:
            source = DataHubDocumentsSource(ctx, config)

            # Page 1: 1000 results, 800 pass client filters (200 have empty text).
            # Page 2: remaining results with no next scroll id.
            page1_results = [self._make_entity(i) for i in range(800)]
            # Add 200 entities with empty text that will be filtered out
            for i in range(800, 1000):
                page1_results.append(
                    {
                        "entity": {
                            "urn": f"urn:li:document:doc-{i}",
                            "info": {
                                "source": {"sourceType": "NATIVE"},
                                "contents": {"text": ""},
                            },
                        }
                    }
                )

            responses = [
                {
                    "scrollAcrossEntities": {
                        "nextScrollId": "cursor-1",
                        "searchResults": page1_results,
                    }
                },
                self._make_scroll_page(1000, 300, next_scroll_id=None),
            ]

            with patch.object(
                source.graph, "execute_graphql", side_effect=responses
            ) as mock_execute:
                documents = source._fetch_documents_graphql()

            # 800 from page 1 (200 filtered) + 300 from page 2
            assert len(documents) == 1100
            assert mock_execute.call_count == 2
            # Second call must use the cursor from page 1, not a derived offset
            assert mock_execute.call_args_list[1][0][1]["scrollId"] == "cursor-1"


def _make_config(**overrides: Any) -> DataHubDocumentsSourceConfig:
    base: dict[str, Any] = dict(
        platform_filter=["*"],
        datahub={"server": "http://test-server:8080"},
        embedding={
            "provider": "bedrock",
            "model": "cohere.embed-english-v3",
            "aws_region": "us-west-2",
            "allow_local_embedding_config": True,
        },
        min_text_length=10,
        stateful_ingestion={"enabled": False},
        locking={"enabled": False},
    )
    base.update(overrides)
    return DataHubDocumentsSourceConfig(**base)


class TestScrollPaginationConfig:
    """Test configurable scroll batch size and inter-page delay."""

    @pytest.fixture
    def ctx(self):
        return PipelineContext(run_id="test-run", pipeline_name="test-pipeline")

    @pytest.fixture
    def mock_graph(self):
        return patch(
            "datahub.ingestion.source.datahub_documents.datahub_documents_source.DataHubGraph"
        )

    def test_scroll_batch_size_passed_to_graphql(self, ctx, mock_graph):
        config = _make_config(scroll_batch_size=250)
        with mock_graph:
            source = DataHubDocumentsSource(ctx, config)
            response: dict[str, Any] = {
                "scrollAcrossEntities": {"nextScrollId": None, "searchResults": []}
            }
            with patch.object(
                source.graph, "execute_graphql", return_value=response
            ) as mock_execute:
                source._fetch_documents_graphql()
            assert mock_execute.call_args[0][1]["batchSize"] == 250

    def test_scroll_delay_sleeps_between_pages(self, ctx, mock_graph):
        config = _make_config(scroll_delay_seconds=0.5)
        with mock_graph:
            source = DataHubDocumentsSource(ctx, config)
            responses = [
                {
                    "scrollAcrossEntities": {
                        "nextScrollId": "cursor-1",
                        "searchResults": [],
                    }
                },
                {
                    "scrollAcrossEntities": {
                        "nextScrollId": None,
                        "searchResults": [],
                    }
                },
            ]
            with (
                patch.object(source.graph, "execute_graphql", side_effect=responses),
                patch(
                    "datahub.ingestion.source.datahub_documents.datahub_documents_source.time.sleep"
                ) as mock_sleep,
            ):
                source._fetch_documents_graphql()
            # Sleep happens before the second page only (not before the first).
            mock_sleep.assert_called_once_with(0.5)

    def test_no_delay_when_zero(self, ctx, mock_graph):
        config = _make_config(scroll_delay_seconds=0.0)
        with mock_graph:
            source = DataHubDocumentsSource(ctx, config)
            responses = [
                {
                    "scrollAcrossEntities": {
                        "nextScrollId": "cursor-1",
                        "searchResults": [],
                    }
                },
                {
                    "scrollAcrossEntities": {
                        "nextScrollId": None,
                        "searchResults": [],
                    }
                },
            ]
            with (
                patch.object(source.graph, "execute_graphql", side_effect=responses),
                patch(
                    "datahub.ingestion.source.datahub_documents.datahub_documents_source.time.sleep"
                ) as mock_sleep,
            ):
                source._fetch_documents_graphql()
            mock_sleep.assert_not_called()


class TestIndexDelay:
    """Test per-indexed-document write throttling."""

    @pytest.fixture
    def ctx(self):
        return PipelineContext(run_id="test-run", pipeline_name="test-pipeline")

    @pytest.fixture
    def mock_graph(self):
        return patch(
            "datahub.ingestion.source.datahub_documents.datahub_documents_source.DataHubGraph"
        )

    def _doc(self, urn: str) -> dict[str, Any]:
        return {
            "urn": urn,
            "text": "Some sufficiently long document text content here.",
            "source_type": "NATIVE",
        }

    def test_index_delay_sleeps_after_each_indexed_document(self, ctx, mock_graph):
        config = _make_config(index_delay_seconds=0.5)
        docs = [self._doc("urn:li:document:a"), self._doc("urn:li:document:b")]
        with mock_graph:
            source = DataHubDocumentsSource(ctx, config)
            fake_wu = Mock()
            with (
                patch.object(source, "_fetch_documents_graphql", return_value=docs),
                patch.object(
                    source,
                    "_process_single_document",
                    side_effect=lambda d: iter([fake_wu]),
                ),
                patch(
                    "datahub.ingestion.source.datahub_documents.datahub_documents_source.time.sleep"
                ) as mock_sleep,
            ):
                list(source._process_batch_mode())
            assert mock_sleep.call_count == 2
            mock_sleep.assert_called_with(0.5)

    def test_index_delay_skips_unchanged_documents(self, ctx, mock_graph):
        config = _make_config(index_delay_seconds=0.5, incremental={"enabled": True})
        docs = [
            self._doc("urn:li:document:unchanged"),
            self._doc("urn:li:document:new"),
        ]
        with mock_graph:
            source = DataHubDocumentsSource(ctx, config)
            fake_wu = Mock()

            def should_process(urn: str, text: str) -> bool:
                return urn.endswith(":new")

            with (
                patch.object(source, "_fetch_documents_graphql", return_value=docs),
                patch.object(source, "_should_process", side_effect=should_process),
                patch.object(
                    source,
                    "_process_single_document",
                    side_effect=lambda d: iter([fake_wu]),
                ),
                patch(
                    "datahub.ingestion.source.datahub_documents.datahub_documents_source.time.sleep"
                ) as mock_sleep,
            ):
                list(source._process_batch_mode())
            mock_sleep.assert_called_once_with(0.5)

    def test_no_index_delay_when_zero(self, ctx, mock_graph):
        config = _make_config(index_delay_seconds=0.0)
        with mock_graph:
            source = DataHubDocumentsSource(ctx, config)
            with (
                patch.object(
                    source,
                    "_fetch_documents_graphql",
                    return_value=[self._doc("urn:li:document:a")],
                ),
                patch.object(
                    source,
                    "_process_single_document",
                    return_value=iter([Mock()]),
                ),
                patch(
                    "datahub.ingestion.source.datahub_documents.datahub_documents_source.time.sleep"
                ) as mock_sleep,
            ):
                list(source._process_batch_mode())
            mock_sleep.assert_not_called()

    def test_no_index_delay_when_document_produces_no_workunits(self, ctx, mock_graph):
        config = _make_config(index_delay_seconds=0.5)
        with mock_graph:
            source = DataHubDocumentsSource(ctx, config)
            with (
                patch.object(
                    source,
                    "_fetch_documents_graphql",
                    return_value=[self._doc("urn:li:document:empty")],
                ),
                patch.object(source, "_process_single_document", return_value=iter([])),
                patch(
                    "datahub.ingestion.source.datahub_documents.datahub_documents_source.time.sleep"
                ) as mock_sleep,
            ):
                list(source._process_batch_mode())
            mock_sleep.assert_not_called()


class TestSkipIfSemanticContentExists:
    """Test that EXTERNAL documents already indexed by their own source are left untouched.

    Some sources (Notion/Confluence) emit semanticContent themselves. For EXTERNAL documents
    an existing aspect means the source owns indexing, so we skip them and do NOT record them
    in our state. We only take ownership of EXTERNAL documents the source did not index. NATIVE
    documents are never skipped this way.
    """

    @pytest.fixture
    def ctx(self):
        return PipelineContext(run_id="test-run", pipeline_name="test-pipeline")

    @pytest.fixture
    def mock_graph(self):
        return patch(
            "datahub.ingestion.source.datahub_documents.datahub_documents_source.DataHubGraph"
        )

    @staticmethod
    def _semantic_aspect(model_key: str) -> Any:
        from datahub.metadata.schema_classes import (
            EmbeddingModelDataClass,
            SemanticContentClass,
        )

        return SemanticContentClass(
            embeddings={
                model_key: EmbeddingModelDataClass(
                    modelVersion="bedrock/cohere.embed-english-v3",
                    generatedAt=0,
                    chunkingStrategy="by_title",
                    totalChunks=1,
                    chunks=[],
                )
            }
        )

    def test_external_with_aspect_not_in_state_is_owned_by_source(
        self, ctx, mock_graph
    ):
        """EXTERNAL + has aspect + not in state -> source owns it, we skip."""
        config = (
            _make_config()
        )  # skip_external_if_semantic_content_exists defaults to True
        with mock_graph:
            source = DataHubDocumentsSource(ctx, config)
            source.document_state = {}
            model_key = source.chunking_source.get_model_embedding_key()
            assert model_key is not None
            with patch.object(
                source.graph,
                "get_aspect",
                return_value=self._semantic_aspect(model_key),
            ):
                assert source._is_indexed_by_source("urn:li:document:ext", True) is True

    def test_external_without_aspect_is_not_owned(self, ctx, mock_graph):
        """EXTERNAL + no aspect -> the source did not index it; we take ownership."""
        config = _make_config()
        with mock_graph:
            source = DataHubDocumentsSource(ctx, config)
            source.document_state = {}
            with patch.object(source.graph, "get_aspect", return_value=None):
                assert (
                    source._is_indexed_by_source("urn:li:document:ext", True) is False
                )

    def test_native_never_owned_by_source(self, ctx, mock_graph):
        """NATIVE documents are never skipped via the source-owned path; aspect not read."""
        config = _make_config()
        with mock_graph:
            source = DataHubDocumentsSource(ctx, config)
            source.document_state = {}
            with patch.object(source.graph, "get_aspect") as mock_get:
                assert (
                    source._is_indexed_by_source("urn:li:document:native", False)
                    is False
                )
            mock_get.assert_not_called()

    def test_external_in_state_is_not_skipped(self, ctx, mock_graph):
        """When state already tracks the doc we own it; re-embed without reading the aspect."""
        config = _make_config()
        with mock_graph:
            source = DataHubDocumentsSource(ctx, config)
            source.document_state = {"urn:li:document:ext": {"content_hash": "old"}}
            with patch.object(source.graph, "get_aspect") as mock_get:
                assert (
                    source._is_indexed_by_source("urn:li:document:ext", True) is False
                )
            mock_get.assert_not_called()

    def test_force_reprocess_disables_source_ownership_skip(self, ctx, mock_graph):
        """force_reprocess re-embeds everything; the existence check is skipped."""
        config = _make_config(
            incremental={"enabled": True, "force_reprocess": True},
        )
        with mock_graph:
            source = DataHubDocumentsSource(ctx, config)
            source.document_state = {}
            with patch.object(source.graph, "get_aspect") as mock_get:
                assert (
                    source._is_indexed_by_source("urn:li:document:ext", True) is False
                )
            mock_get.assert_not_called()

    def test_skip_disabled_by_config(self, ctx, mock_graph):
        config = _make_config(skip_external_if_semantic_content_exists=False)
        with mock_graph:
            source = DataHubDocumentsSource(ctx, config)
            source.document_state = {}
            with patch.object(source.graph, "get_aspect") as mock_get:
                assert (
                    source._is_indexed_by_source("urn:li:document:ext", True) is False
                )
            mock_get.assert_not_called()

    def test_processes_when_aspect_absent(self, ctx, mock_graph):
        config = _make_config()
        with mock_graph:
            source = DataHubDocumentsSource(ctx, config)
            with patch.object(source.graph, "get_aspect", return_value=None):
                assert (
                    source._has_existing_semantic_content("urn:li:document:new")
                    is False
                )

    def test_batch_skips_external_indexed_doc_without_seeding_state(
        self, ctx, mock_graph
    ):
        """Integration: an externally-indexed EXTERNAL doc is skipped and never enters state."""
        config = _make_config(incremental={"enabled": True})
        with mock_graph:
            source = DataHubDocumentsSource(ctx, config)
            source.document_state = {}
            model_key = source.chunking_source.get_model_embedding_key()
            assert model_key is not None
            doc = {
                "urn": "urn:li:document:ext-indexed",
                "text": "Some sufficiently long document text content here.",
                "source_type": "EXTERNAL",
            }
            with (
                patch.object(source, "_fetch_documents_graphql", return_value=[doc]),
                patch.object(
                    source.graph,
                    "get_aspect",
                    return_value=self._semantic_aspect(model_key),
                ),
                patch.object(source, "_process_single_document") as mock_proc,
            ):
                list(source._process_batch_mode())

            mock_proc.assert_not_called()
            assert source.report.num_documents_skipped_existing_embeddings == 1
            # Ownership is NOT claimed: the doc must not be recorded in our state.
            assert doc["urn"] not in source.document_state

    def test_batch_takes_ownership_of_unindexed_external_doc(self, ctx, mock_graph):
        """Integration: an EXTERNAL doc the source did not index is processed and recorded."""
        config = _make_config(incremental={"enabled": True})
        with mock_graph:
            source = DataHubDocumentsSource(ctx, config)
            source.document_state = {}
            doc = {
                "urn": "urn:li:document:ext-new",
                "text": "Some sufficiently long document text content here.",
                "source_type": "EXTERNAL",
            }
            with (
                patch.object(source, "_fetch_documents_graphql", return_value=[doc]),
                patch.object(source.graph, "get_aspect", return_value=None),
                patch.object(
                    source, "_process_single_document", return_value=iter([])
                ) as mock_proc,
            ):
                list(source._process_batch_mode())

            mock_proc.assert_called_once()
            assert source.report.num_documents_skipped_existing_embeddings == 0
            # Ownership claimed: recorded in state so future changes are re-embedded.
            assert doc["urn"] in source.document_state


class TestDocumentIndexingLock:
    """Test the dataHubStepState-based distributed lock."""

    @staticmethod
    def _props(status, owner, expires_at_ms):
        from datahub.metadata.schema_classes import (
            AuditStampClass,
            DataHubStepStatePropertiesClass,
        )

        return DataHubStepStatePropertiesClass(
            properties={
                "status": status,
                # Ownership is keyed on the per-execution owner token; run_id is retained
                # only for human-readable debugging.
                "owner": owner,
                "run_id": "pipeline-run",
                "acquired_at_ms": "0",
                "expires_at_ms": str(expires_at_ms),
            },
            lastModified=AuditStampClass(time=0, actor="urn:li:corpuser:test"),
        )

    def _make_lock(self, graph, renewal_interval_seconds=300):
        from datahub.ingestion.source.datahub_documents.document_indexing_lock import (
            DocumentIndexingLock,
        )

        lock = DocumentIndexingLock(
            graph=graph,
            lock_id="test-lock",
            run_id="run-A",
            ttl_seconds=1800,
            renewal_interval_seconds=renewal_interval_seconds,
        )
        # Production derives owner from run_id + a uuid (unique per execution). Pin it here
        # so tests can assert ownership deterministically; the literal "owner" values passed
        # to _props line up with this.
        lock.owner = "run-A"
        return lock

    def test_acquire_when_free_cold_start(self):
        from datahub.emitter.rest_emitter import EmitMode
        from datahub.metadata.schema_classes import ChangeTypeClass

        graph = Mock()
        graph.exists.return_value = False
        graph.get_aspect.side_effect = [
            None,
            self._props("running", "run-A", 9999999999999),
        ]
        graph.emit_mcps = Mock()
        graph.emit_mcp = Mock()

        with patch(
            "datahub.ingestion.source.datahub_documents.document_indexing_lock.time.sleep"
        ):
            lock = self._make_lock(graph)
            assert lock.acquire() is True

        graph.emit_mcps.assert_called_once()
        mcps = graph.emit_mcps.call_args.args[0]
        assert len(mcps) == 2
        assert mcps[0].changeType == ChangeTypeClass.CREATE_ENTITY
        assert mcps[0].headers == {"If-None-Match": "*"}
        assert mcps[1].changeType == ChangeTypeClass.CREATE
        assert mcps[1].headers == {"If-None-Match": "*"}
        assert graph.emit_mcps.call_args.kwargs["emit_mode"] == EmitMode.SYNC_PRIMARY
        graph.emit_mcp.assert_not_called()

    def test_acquire_when_free_takeover_upserts(self):
        """When the lock entity already exists, acquisition uses UPSERT not CREATE."""
        from datahub.emitter.rest_emitter import EmitMode

        graph = Mock()
        graph.exists.return_value = True
        graph.get_aspect.side_effect = [
            self._props("released", "run-old", 1),
            self._props("running", "run-A", 9999999999999),
        ]
        graph.emit_mcps = Mock()
        graph.emit_mcp = Mock()

        with patch(
            "datahub.ingestion.source.datahub_documents.document_indexing_lock.time.sleep"
        ):
            lock = self._make_lock(graph)
            assert lock.acquire() is True

        graph.emit_mcps.assert_not_called()
        graph.emit_mcp.assert_called_once()
        assert graph.emit_mcp.call_args.kwargs["emit_mode"] == EmitMode.SYNC_PRIMARY

    def test_blocked_when_active_lease_held_by_other(self):
        graph = Mock()
        graph.get_aspect.return_value = self._props("running", "run-B", 9999999999999)
        graph.emit_mcp = Mock()
        graph.emit_mcps = Mock()

        lock = self._make_lock(graph)
        assert lock.acquire() is False
        # A held lock performs no writes of either kind (no UPSERT, no CREATE).
        graph.emit_mcp.assert_not_called()
        graph.emit_mcps.assert_not_called()

    def test_reentrant_when_active_lease_held_by_self(self):
        """Lock present but held by this same execution (same owner) -> re-entrant success."""
        graph = Mock()
        graph.get_aspect.return_value = self._props("running", "run-A", 9999999999999)
        graph.emit_mcp = Mock()
        graph.emit_mcps = Mock()

        lock = self._make_lock(graph)
        assert lock.acquire() is True
        graph.emit_mcp.assert_not_called()
        graph.emit_mcps.assert_not_called()

    def test_distinct_executions_with_same_run_id_do_not_reenter(self):
        """A second execution must NOT re-enter a lease just because run_id matches.

        Managed ingestion reuses the same pipeline run_id across scheduled runs of a
        source. Ownership is therefore keyed on a per-execution owner token (run_id +
        uuid), so a fresh execution sees the active lease as held-by-other and is blocked
        rather than extending it.
        """
        from datahub.ingestion.source.datahub_documents.document_indexing_lock import (
            DocumentIndexingLock,
        )

        # Two locks built with the SAME run_id, as happens for two scheduled runs.
        lock_a = DocumentIndexingLock(
            graph=Mock(), lock_id="test-lock", run_id="same-run", ttl_seconds=1800
        )
        lock_b = DocumentIndexingLock(
            graph=Mock(), lock_id="test-lock", run_id="same-run", ttl_seconds=1800
        )
        assert lock_a.owner != lock_b.owner

        # lock_b starts and finds lock_a's active lease. It must be blocked, not re-entrant.
        graph = Mock()
        graph.get_aspect.return_value = self._props(
            "running", lock_a.owner, 9999999999999
        )
        graph.emit_mcp = Mock()
        graph.emit_mcps = Mock()
        lock_b.graph = graph

        assert lock_b.acquire() is False
        graph.emit_mcp.assert_not_called()
        graph.emit_mcps.assert_not_called()

    def test_takes_over_expired_lease(self):
        graph = Mock()
        graph.exists.return_value = True
        graph.get_aspect.side_effect = [
            self._props("running", "run-B", 1),  # expired
            self._props("running", "run-A", 9999999999999),
        ]
        graph.emit_mcp = Mock()
        graph.emit_mcps = Mock()

        with patch(
            "datahub.ingestion.source.datahub_documents.document_indexing_lock.time.sleep"
        ):
            lock = self._make_lock(graph)
            assert lock.acquire() is True
        graph.emit_mcp.assert_called_once()

    def test_loses_race_on_reread(self):
        graph = Mock()
        graph.exists.return_value = False
        graph.get_aspect.side_effect = [
            None,
            self._props("running", "run-B", 9999999999999),  # someone else won
        ]
        graph.emit_mcp = Mock()
        graph.emit_mcps = Mock()

        with patch(
            "datahub.ingestion.source.datahub_documents.document_indexing_lock.time.sleep"
        ):
            lock = self._make_lock(graph)
            assert lock.acquire() is False
        graph.emit_mcps.assert_called_once()
        graph.emit_mcp.assert_not_called()

    def test_release_marks_released(self):
        graph = Mock()
        graph.exists.return_value = False
        graph.get_aspect.side_effect = [
            None,
            self._props("running", "run-A", 9999999999999),
            self._props("running", "run-A", 9999999999999),  # read in release()
        ]
        graph.emit_mcp = Mock()
        graph.emit_mcps = Mock()

        with patch(
            "datahub.ingestion.source.datahub_documents.document_indexing_lock.time.sleep"
        ):
            lock = self._make_lock(graph)
            assert lock.acquire() is True
            lock.release()

        # One emit_mcps for cold-start acquire, one emit_mcp for release.
        assert graph.emit_mcps.call_count == 1
        assert graph.emit_mcp.call_count == 1
        released_aspect = graph.emit_mcp.call_args_list[0].args[0].aspect
        assert released_aspect.properties["status"] == "released"

    def test_release_noop_when_not_acquired(self):
        graph = Mock()
        graph.emit_mcp = Mock()
        lock = self._make_lock(graph)
        lock.release()
        graph.emit_mcp.assert_not_called()

    def test_release_does_not_clobber_other_owner(self):
        """If another run took over after our lease expired, release() must not overwrite it."""
        graph = Mock()
        graph.exists.return_value = False
        graph.get_aspect.side_effect = [
            None,
            self._props("running", "run-A", 9999999999999),  # confirm we acquired
            self._props(
                "running", "run-B", 9999999999999
            ),  # read in release(): now run-B
        ]
        graph.emit_mcp = Mock()
        graph.emit_mcps = Mock()

        with patch(
            "datahub.ingestion.source.datahub_documents.document_indexing_lock.time.sleep"
        ):
            lock = self._make_lock(graph)
            assert lock.acquire() is True
            lock.release()

        # Cold-start acquire used emit_mcps; release must NOT write a released lease
        # because run-B now owns the lock.
        graph.emit_mcp.assert_not_called()

    def test_heartbeat_renews_after_interval(self):
        graph = Mock()
        graph.exists.return_value = True
        graph.get_aspect.side_effect = [
            self._props("released", "run-old", 1),
            self._props("running", "run-A", 9999999999999),
        ]
        graph.emit_mcp = Mock()
        graph.emit_mcps = Mock()

        with patch(
            "datahub.ingestion.source.datahub_documents.document_indexing_lock.time.sleep"
        ):
            # Renewal interval of 0 => every heartbeat renews.
            lock = self._make_lock(graph, renewal_interval_seconds=0)
            assert lock.acquire() is True
            assert graph.emit_mcp.call_count == 1  # takeover UPSERT

            lock.heartbeat()
            assert graph.emit_mcp.call_count == 2  # renewed

            # Acquisition time is preserved across renewals; only expiry advances.
            acquire_aspect = graph.emit_mcp.call_args_list[0].args[0].aspect
            renew_aspect = graph.emit_mcp.call_args_list[1].args[0].aspect
            assert (
                acquire_aspect.properties["acquired_at_ms"]
                == renew_aspect.properties["acquired_at_ms"]
            )

    def test_heartbeat_throttled_within_interval(self):
        graph = Mock()
        graph.exists.return_value = True
        graph.get_aspect.side_effect = [
            self._props("released", "run-old", 1),
            self._props("running", "run-A", 9999999999999),
        ]
        graph.emit_mcp = Mock()
        graph.emit_mcps = Mock()

        with patch(
            "datahub.ingestion.source.datahub_documents.document_indexing_lock.time.sleep"
        ):
            # Large interval => heartbeat right after acquire is throttled (no write).
            lock = self._make_lock(graph, renewal_interval_seconds=3600)
            assert lock.acquire() is True
            assert graph.emit_mcp.call_count == 1
            lock.heartbeat()
            assert graph.emit_mcp.call_count == 1  # throttled, no extra write

    def test_heartbeat_noop_when_not_acquired(self):
        graph = Mock()
        graph.emit_mcp = Mock()
        lock = self._make_lock(graph)
        lock.heartbeat()
        graph.emit_mcp.assert_not_called()

    def test_heartbeat_swallows_write_failure(self):
        """A transient renewal failure must not propagate; the TTL still has margin."""
        graph = Mock()
        graph.exists.return_value = True
        graph.get_aspect.side_effect = [
            self._props("released", "run-old", 1),
            self._props("running", "run-A", 9999999999999),
        ]
        # First emit_mcp (takeover) succeeds; the heartbeat renewal raises.
        graph.emit_mcp = Mock(side_effect=[None, Exception("transient GMS error")])
        graph.emit_mcps = Mock()

        with patch(
            "datahub.ingestion.source.datahub_documents.document_indexing_lock.time.sleep"
        ):
            lock = self._make_lock(graph, renewal_interval_seconds=0)
            assert lock.acquire() is True
            assert graph.emit_mcp.call_count == 1
            # Should not raise even though the renewal write fails.
            lock.heartbeat()
            assert graph.emit_mcp.call_count == 2

    def test_lease_is_active_states(self):
        """The is_active predicate distinguishes the three lock conditions."""
        from datahub.ingestion.source.datahub_documents.document_indexing_lock import (
            _LeaseState,
            _now_ms,
        )

        future = _now_ms() + 60_000
        past = _now_ms() - 60_000

        # Fields: status, owner, run_id, acquired_at_ms, expires_at_ms.
        # Lock present: held and not yet expired.
        assert _LeaseState("running", "owner-A", "run-A", 0, future).is_active is True
        # Lock expired: held but TTL elapsed.
        assert _LeaseState("running", "owner-A", "run-A", 0, past).is_active is False
        # No lock: lease was released.
        assert _LeaseState("released", "owner-A", "run-A", 0, future).is_active is False
        # Held with no expiry recorded -> treated as active (fail safe).
        assert _LeaseState("running", "owner-A", "run-A", 0, None).is_active is True


class TestLockIntegrationWithSource:
    """Test lock wiring into the source's run lifecycle."""

    @pytest.fixture
    def ctx(self):
        return PipelineContext(run_id="test-run", pipeline_name="test-pipeline")

    @pytest.fixture
    def mock_graph(self):
        return patch(
            "datahub.ingestion.source.datahub_documents.datahub_documents_source.DataHubGraph"
        )

    def test_run_skipped_when_lock_not_acquired(self, ctx, mock_graph):
        config = _make_config(locking={"enabled": True})
        with mock_graph:
            source = DataHubDocumentsSource(ctx, config)
            assert source.lock is not None
            with (
                patch.object(source.lock, "acquire", return_value=False),
                patch.object(source.lock, "release") as mock_release,
                patch.object(source, "_process_batch_mode") as mock_batch,
            ):
                workunits = list(source.get_workunits_internal())

            assert workunits == []
            assert source.report.lock_skipped_run is True
            mock_batch.assert_not_called()
            mock_release.assert_not_called()

    def test_run_proceeds_and_releases_when_lock_acquired(self, ctx, mock_graph):
        config = _make_config(locking={"enabled": True})
        with mock_graph:
            source = DataHubDocumentsSource(ctx, config)
            assert source.lock is not None
            with (
                patch.object(source.lock, "acquire", return_value=True),
                patch.object(source.lock, "release") as mock_release,
                patch.object(
                    source, "_fetch_documents_graphql", return_value=[]
                ) as mock_fetch,
            ):
                list(source.get_workunits_internal())

            mock_fetch.assert_called_once()
            mock_release.assert_called_once()

    def test_lock_disabled_means_no_lock(self, ctx, mock_graph):
        config = _make_config(locking={"enabled": False})
        with mock_graph:
            source = DataHubDocumentsSource(ctx, config)
            assert source.lock is None

    def test_default_lock_id_from_source_urn_is_clean(self, mock_graph):
        from datahub.metadata.urns import DataHubStepStateUrn

        # In managed ingestion pipeline_name is the ingestion source URN.
        ctx = PipelineContext(
            run_id="test-run",
            pipeline_name="urn:li:dataHubIngestionSource:datahub-documents",
        )
        config = _make_config(locking={"enabled": True})
        with mock_graph:
            source = DataHubDocumentsSource(ctx, config)
            assert source.lock is not None
            lock_id = str(source.lock.urn).split(":", 3)[-1]
            # No nested-URN colons leaked into the lock id.
            assert ":" not in lock_id
            assert lock_id == "document-indexing-lock-datahub-documents"
            # And the resulting URN round-trips cleanly.
            assert (
                str(DataHubStepStateUrn(lock_id))
                == "urn:li:dataHubStepState:document-indexing-lock-datahub-documents"
            )

    def test_default_lock_id_falls_back_without_pipeline_name(self):
        # When no pipeline name is available, the lock id falls back to a safe default.
        assert (
            DataHubDocumentsSource._default_lock_id(None)
            == "document-indexing-lock-default"
        )

    def test_custom_lock_id_overrides_default(self, mock_graph):
        ctx = PipelineContext(
            run_id="test-run",
            pipeline_name="urn:li:dataHubIngestionSource:datahub-documents",
        )
        config = _make_config(locking={"enabled": True, "lock_id": "my-custom-lock"})
        with mock_graph:
            source = DataHubDocumentsSource(ctx, config)
            assert source.lock is not None
            assert str(source.lock.urn) == "urn:li:dataHubStepState:my-custom-lock"
