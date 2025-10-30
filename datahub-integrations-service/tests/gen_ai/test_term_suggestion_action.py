"""
Unit tests for term_suggestion_action.py, focusing on Shell Entity handling and _mark_entity_as_processed logic.
"""

from typing import Any, Dict
from unittest.mock import MagicMock, patch

import datahub.metadata.schema_classes as models
import pytest
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DataHubGraph

from datahub_integrations.gen_ai.description_context import ShellEntityError
from datahub_integrations.gen_ai.router import SuggestedTerms
from datahub_integrations.gen_ai.term_suggestion_action import (
    _TERMS_ALGO_VERSION,
    AutomationApplyType,
    BulkTermSuggester,
    TermSuggestionActionConfig,
)


@pytest.fixture
def mock_config() -> TermSuggestionActionConfig:
    """Create a mock config for testing."""
    return TermSuggestionActionConfig(
        entity_types_enabled=["SCHEMA_FIELD"],
        platforms=["test"],
        glossary_term_urns=["urn:li:glossaryTerm:TestTerm"],
        recommendation_action=AutomationApplyType.PROPOSE,
    )


@pytest.fixture
def mock_graph() -> MagicMock:
    """Create a mock DataHubGraph for testing."""
    return MagicMock(spec=DataHubGraph)


@pytest.fixture
def bulk_suggester(
    mock_graph: MagicMock, mock_config: TermSuggestionActionConfig
) -> BulkTermSuggester:
    """Create a BulkTermSuggester instance for testing."""
    return BulkTermSuggester(graph=mock_graph, config=mock_config)


class TestBulkTermSuggesterShellEntityHandling:
    """Test Shell Entity handling logic in BulkTermSuggester."""

    def test_mark_entity_as_processed_emits_correct_mcp(
        self, bulk_suggester: BulkTermSuggester, mock_graph: MagicMock
    ) -> None:
        """Test that _mark_entity_as_processed emits the correct MCP to prevent infinite retries."""

        test_urn = "urn:li:dataset:(urn:li:dataPlatform:test,shell_entity,PROD)"

        # Call the method
        bulk_suggester._mark_entity_as_processed(test_urn)

        # Verify emit_mcp was called
        mock_graph.emit_mcp.assert_called_once()

        # Get the emitted MCP and verify its structure
        emitted_mcp = mock_graph.emit_mcp.call_args[0][0]
        assert isinstance(emitted_mcp, MetadataChangeProposalWrapper)
        assert emitted_mcp.entityUrn == test_urn
        assert emitted_mcp.aspect is not None

        # Verify the aspect is EntityInferenceMetadataClass
        aspect = emitted_mcp.aspect
        assert isinstance(aspect, models.EntityInferenceMetadataClass)
        assert aspect.glossaryTermsInference is not None

        # Verify the inference metadata
        inference = aspect.glossaryTermsInference
        assert isinstance(inference, models.InferenceGroupMetadataClass)
        assert inference.version == _TERMS_ALGO_VERSION
        assert inference.lastInferredAt is not None

        # Verify async_flag is False
        assert mock_graph.emit_mcp.call_args[1]["async_flag"] is False

    @patch("datahub_integrations.gen_ai.term_suggestion_action.fetch_glossary_info")
    @patch("datahub_integrations.gen_ai.term_suggestion_action._suggest_terms_batch")
    def test_shell_entity_error_marks_as_processed(
        self,
        mock_suggest_terms_batch: MagicMock,
        mock_fetch_glossary_info: MagicMock,
        bulk_suggester: BulkTermSuggester,
        mock_graph: MagicMock,
    ) -> None:
        """Test that ShellEntityError causes entity to be marked as processed."""

        test_urn = "urn:li:dataset:(urn:li:dataPlatform:test,shell_entity,PROD)"

        # Mock glossary info and _suggest_terms_batch to raise ShellEntityError
        mock_fetch_glossary_info.return_value = MagicMock()
        mock_suggest_terms_batch.side_effect = ShellEntityError(
            "Entity has no schema metadata"
        )

        # Process the URN
        bulk_suggester.process_urns([test_urn])

        # Verify _suggest_terms_batch was called
        mock_suggest_terms_batch.assert_called_once()

        # Verify _mark_entity_as_processed was called (via emit_mcp)
        # Should be called once for marking as processed
        assert mock_graph.emit_mcp.call_count == 1

        # Verify the MCP has the correct structure for marking as processed
        emitted_mcp = mock_graph.emit_mcp.call_args_list[0][0][0]
        assert emitted_mcp.entityUrn == test_urn
        assert isinstance(emitted_mcp.aspect, models.EntityInferenceMetadataClass)
        assert emitted_mcp.aspect.glossaryTermsInference is not None
        assert emitted_mcp.aspect.glossaryTermsInference.version == _TERMS_ALGO_VERSION

    @patch("datahub_integrations.gen_ai.term_suggestion_action.fetch_glossary_info")
    @patch("datahub_integrations.gen_ai.term_suggestion_action._suggest_terms_batch")
    def test_other_exceptions_do_not_mark_as_processed(
        self,
        mock_suggest_terms_batch: MagicMock,
        mock_fetch_glossary_info: MagicMock,
        bulk_suggester: BulkTermSuggester,
        mock_graph: MagicMock,
    ) -> None:
        """Test that non-Shell exceptions do NOT mark entity as processed (allowing retry)."""

        test_urn = "urn:li:dataset:(urn:li:dataPlatform:test,retry_entity,PROD)"

        # Mock glossary info and _suggest_terms_batch to raise a generic exception (network error, etc.)
        mock_fetch_glossary_info.return_value = MagicMock()
        mock_suggest_terms_batch.side_effect = RuntimeError("Network connection failed")

        # Process the URN
        bulk_suggester.process_urns([test_urn])

        # Verify _suggest_terms_batch was called
        mock_suggest_terms_batch.assert_called_once()

        # Verify _mark_entity_as_processed was NOT called
        # No emit_mcp calls should be made for retry-able failures
        mock_graph.emit_mcp.assert_not_called()

    @patch("datahub_integrations.gen_ai.term_suggestion_action.fetch_glossary_info")
    @patch("datahub_integrations.gen_ai.term_suggestion_action._suggest_terms_batch")
    def test_successful_processing_marks_as_processed(
        self,
        mock_suggest_terms_batch: MagicMock,
        mock_fetch_glossary_info: MagicMock,
        bulk_suggester: BulkTermSuggester,
        mock_graph: MagicMock,
    ) -> None:
        """Test that successful processing also marks entity as processed."""

        test_urn = "urn:li:dataset:(urn:li:dataPlatform:test,success_entity,PROD)"

        # Mock glossary info and successful term suggestions
        mock_fetch_glossary_info.return_value = MagicMock()
        mock_suggestions = {
            test_urn: SuggestedTerms(table_terms=None, column_terms={"test_field": []})
        }
        mock_suggest_terms_batch.return_value = mock_suggestions

        # Process the URN
        bulk_suggester.process_urns([test_urn])

        # Verify _suggest_terms_batch was called
        mock_suggest_terms_batch.assert_called_once()

        # Verify entity is marked as processed through update_entity flow
        # This should result in emit_mcp being called (once for inference metadata)
        assert mock_graph.emit_mcp.call_count >= 1

        # Find the EntityInferenceMetadata call
        inference_calls = [
            call
            for call in mock_graph.emit_mcp.call_args_list
            if isinstance(call[0][0].aspect, models.EntityInferenceMetadataClass)
        ]
        assert len(inference_calls) == 1

        # Verify the inference metadata MCP
        inference_mcp = inference_calls[0][0][0]
        assert inference_mcp.entityUrn == test_urn
        assert (
            inference_mcp.aspect.glossaryTermsInference.version == _TERMS_ALGO_VERSION
        )

    @patch("datahub_integrations.gen_ai.term_suggestion_action.fetch_glossary_info")
    @patch("datahub_integrations.gen_ai.term_suggestion_action._suggest_terms_batch")
    def test_no_suggestions_returned_still_marks_as_processed(
        self,
        mock_suggest_terms_batch: MagicMock,
        mock_fetch_glossary_info: MagicMock,
        bulk_suggester: BulkTermSuggester,
        mock_graph: MagicMock,
    ) -> None:
        """Test that when no suggestions are returned, entity is still marked as processed."""

        test_urn = (
            "urn:li:dataset:(urn:li:dataPlatform:test,no_suggestions_entity,PROD)"
        )

        # Mock glossary info and _suggest_terms_batch returning no suggestions for this URN
        mock_fetch_glossary_info.return_value = MagicMock()
        mock_suggest_terms_batch.return_value = {}  # Empty dict, no suggestions

        # Process the URN
        bulk_suggester.process_urns([test_urn])

        # Verify _suggest_terms_batch was called
        mock_suggest_terms_batch.assert_called_once()

        # Verify entity is marked as processed even when no suggestions
        assert mock_graph.emit_mcp.call_count == 1

        # Verify it's the correct MCP for marking as processed
        emitted_mcp = mock_graph.emit_mcp.call_args[0][0]
        assert emitted_mcp.entityUrn == test_urn
        assert isinstance(emitted_mcp.aspect, models.EntityInferenceMetadataClass)
        assert emitted_mcp.aspect.glossaryTermsInference is not None
        assert emitted_mcp.aspect.glossaryTermsInference.version == _TERMS_ALGO_VERSION

    @patch("datahub_integrations.gen_ai.term_suggestion_action.fetch_glossary_info")
    @patch("datahub_integrations.gen_ai.term_suggestion_action._suggest_terms_batch")
    def test_multiple_urns_shell_entity_handling(
        self,
        mock_suggest_terms_batch: MagicMock,
        mock_fetch_glossary_info: MagicMock,
        bulk_suggester: BulkTermSuggester,
        mock_graph: MagicMock,
    ) -> None:
        """Test that Shell Entity handling works correctly when processing multiple URNs."""

        shell_urn = "urn:li:dataset:(urn:li:dataPlatform:test,shell_entity,PROD)"
        retry_urn = "urn:li:dataset:(urn:li:dataPlatform:test,retry_entity,PROD)"
        success_urn = "urn:li:dataset:(urn:li:dataPlatform:test,success_entity,PROD)"

        # Mock glossary info
        mock_fetch_glossary_info.return_value = MagicMock()

        # Mock different responses for different URNs
        def side_effect(
            graph: Any,
            entity_urns: list[str],
            glossary_info: Any,
            custom_instructions: str | None = None,
        ) -> Dict[str, SuggestedTerms]:
            urn = entity_urns[0]  # Processing one at a time
            if urn == shell_urn:
                raise ShellEntityError("No schema metadata")
            elif urn == retry_urn:
                raise RuntimeError("Network error")
            else:  # success_urn
                return {urn: SuggestedTerms(table_terms=None, column_terms={})}

        mock_suggest_terms_batch.side_effect = side_effect

        # Process all URNs
        urns = [shell_urn, retry_urn, success_urn]
        bulk_suggester.process_urns(urns)

        # Verify _suggest_terms_batch was called for each URN
        assert mock_suggest_terms_batch.call_count == 3

        # Verify emit_mcp behavior:
        # - shell_urn: marked as processed (1 call)
        # - retry_urn: NOT marked as processed (0 calls)
        # - success_urn: marked as processed via update_entity (1 call)
        # Total: 2 calls
        assert mock_graph.emit_mcp.call_count == 2

        # Verify the shell entity and success entity were marked as processed
        emitted_urns = [
            call[0][0].entityUrn for call in mock_graph.emit_mcp.call_args_list
        ]
        assert shell_urn in emitted_urns
        assert success_urn in emitted_urns
        assert retry_urn not in emitted_urns


class TestBulkTermSuggesterEdgeCases:
    """Test edge cases and error handling in BulkTermSuggester."""

    def test_mark_entity_as_processed_with_special_characters_in_urn(
        self, bulk_suggester: BulkTermSuggester, mock_graph: MagicMock
    ) -> None:
        """Test _mark_entity_as_processed works with URNs containing special characters."""

        # URN with special characters (like the ones from our field encoding tests)
        test_urn = "urn:li:dataset:(urn:li:dataPlatform:tableau,aece3879-1ccc-db69-cb8e-36c342303ef8,PROD)"

        # Call the method
        bulk_suggester._mark_entity_as_processed(test_urn)

        # Verify emit_mcp was called with the correct URN
        mock_graph.emit_mcp.assert_called_once()
        emitted_mcp = mock_graph.emit_mcp.call_args[0][0]
        assert emitted_mcp.entityUrn == test_urn

    @patch("datahub_integrations.gen_ai.term_suggestion_action.fetch_glossary_info")
    @patch("datahub_integrations.gen_ai.term_suggestion_action._suggest_terms_batch")
    @patch("datahub_integrations.gen_ai.term_suggestion_action.logger")
    def test_shell_entity_error_preserves_original_message(
        self,
        mock_logger: MagicMock,
        mock_suggest_terms_batch: MagicMock,
        mock_fetch_glossary_info: MagicMock,
        bulk_suggester: BulkTermSuggester,
        mock_graph: MagicMock,
    ) -> None:
        """Test that ShellEntityError logging preserves the original error message."""

        test_urn = "urn:li:dataset:(urn:li:dataPlatform:test,shell_entity,PROD)"
        error_message = "Schema metadata not found; likely a shell entity"

        # Mock glossary info and _suggest_terms_batch to raise ShellEntityError with specific message
        mock_fetch_glossary_info.return_value = MagicMock()
        mock_suggest_terms_batch.side_effect = ShellEntityError(error_message)

        # Process the URN
        bulk_suggester.process_urns([test_urn])

        # Verify the logger.info was called with correct message
        mock_logger.info.assert_called_with(
            f"Skipping shell entity {test_urn}: {error_message}"
        )

        # Verify entity is still marked as processed
        mock_graph.emit_mcp.assert_called_once()


class TestInferenceMetadataStructure:
    """Test the structure of inference metadata emitted by _mark_entity_as_processed."""

    def test_inference_metadata_has_correct_version(
        self, bulk_suggester: BulkTermSuggester, mock_graph: MagicMock
    ) -> None:
        """Test that inference metadata uses the correct algorithm version."""

        test_urn = "urn:li:dataset:(urn:li:dataPlatform:test,version_test,PROD)"

        bulk_suggester._mark_entity_as_processed(test_urn)

        # Extract the emitted MCP
        emitted_mcp = mock_graph.emit_mcp.call_args[0][0]
        aspect = emitted_mcp.aspect

        # Verify version matches current algorithm version
        assert aspect.glossaryTermsInference.version == _TERMS_ALGO_VERSION

    def test_inference_metadata_has_timestamp(
        self, bulk_suggester: BulkTermSuggester, mock_graph: MagicMock
    ) -> None:
        """Test that inference metadata includes a lastInferredAt timestamp."""

        test_urn = "urn:li:dataset:(urn:li:dataPlatform:test,timestamp_test,PROD)"

        bulk_suggester._mark_entity_as_processed(test_urn)

        # Extract the emitted MCP
        emitted_mcp = mock_graph.emit_mcp.call_args[0][0]
        aspect = emitted_mcp.aspect

        # Verify timestamp is present and reasonable
        timestamp = aspect.glossaryTermsInference.lastInferredAt
        assert timestamp is not None
        assert timestamp > 0  # Should be a valid Unix timestamp

    def test_inference_metadata_uses_patch_structure(
        self, bulk_suggester: BulkTermSuggester, mock_graph: MagicMock
    ) -> None:
        """Test that the MCP structure is correct for preventing infinite retry."""

        test_urn = "urn:li:dataset:(urn:li:dataPlatform:test,patch_test,PROD)"

        bulk_suggester._mark_entity_as_processed(test_urn)

        # Extract the emitted MCP
        emitted_mcp = mock_graph.emit_mcp.call_args[0][0]

        # Verify the MCP is structured to update glossaryTermsInference
        aspect = emitted_mcp.aspect
        assert isinstance(aspect, models.EntityInferenceMetadataClass)
        assert hasattr(aspect, "glossaryTermsInference")

        # This structure should prevent the entity from being selected again
        # by get_urns_to_process() which filters based on glossaryTermsVersion


class TestShellEntityErrorHandlingIntegration:
    """Integration tests for Shell Entity error handling in the full process_urns flow."""

    @patch("datahub_integrations.gen_ai.term_suggestion_action.fetch_glossary_info")
    @patch("datahub_integrations.gen_ai.term_suggestion_action._suggest_terms_batch")
    def test_shell_entity_error_does_not_break_batch_processing(
        self,
        mock_suggest_terms_batch: MagicMock,
        mock_fetch_glossary_info: MagicMock,
        bulk_suggester: BulkTermSuggester,
        mock_graph: MagicMock,
    ) -> None:
        """Test that a ShellEntityError for one URN doesn't break processing of other URNs."""

        shell_urn = "urn:li:dataset:(urn:li:dataPlatform:test,shell_entity,PROD)"
        success_urn = "urn:li:dataset:(urn:li:dataPlatform:test,success_entity,PROD)"

        # Mock glossary info
        mock_fetch_glossary_info.return_value = MagicMock()

        call_count = 0

        def side_effect(
            graph: Any,
            entity_urns: list[str],
            glossary_info: Any,
            custom_instructions: str | None = None,
        ) -> Dict[str, SuggestedTerms]:
            nonlocal call_count
            call_count += 1
            urn = entity_urns[0]

            if urn == shell_urn:
                raise ShellEntityError("No schema metadata")
            else:  # success_urn
                return {urn: SuggestedTerms(table_terms=None, column_terms={})}

        mock_suggest_terms_batch.side_effect = side_effect

        # Process both URNs
        bulk_suggester.process_urns([shell_urn, success_urn])

        # Verify both URNs were processed (_suggest_terms_batch called twice)
        assert mock_suggest_terms_batch.call_count == 2

        # Verify both entities are marked as processed
        # shell_urn: via _mark_entity_as_processed (ShellEntityError handling)
        # success_urn: via update_entity (successful processing)
        assert mock_graph.emit_mcp.call_count == 2

        # Verify both URNs are in the emitted MCPs
        emitted_urns = [
            call[0][0].entityUrn for call in mock_graph.emit_mcp.call_args_list
        ]
        assert shell_urn in emitted_urns
        assert success_urn in emitted_urns


class TestCustomInstructions:
    """Test custom instructions feature in term suggestion."""

    @patch("datahub_integrations.gen_ai.term_suggestion_action.fetch_glossary_info")
    @patch("datahub_integrations.gen_ai.term_suggestion_action._suggest_terms_batch")
    def test_custom_instructions_passed_to_suggest_terms_batch(
        self,
        mock_suggest_terms_batch: MagicMock,
        mock_fetch_glossary_info: MagicMock,
        mock_graph: MagicMock,
    ) -> None:
        """Test that custom instructions from config are passed to _suggest_terms_batch."""

        # Create config WITH custom instructions
        config = TermSuggestionActionConfig(
            entity_types_enabled=["SCHEMA_FIELD"],
            platforms=["test"],
            glossary_term_urns=["urn:li:glossaryTerm:TestTerm"],
            recommendation_action=AutomationApplyType.PROPOSE,
            custom_instructions="Focus on PII classification and be conservative with confidence scores.",
        )

        bulk_suggester = BulkTermSuggester(graph=mock_graph, config=config)

        # Mock glossary info and successful suggestions
        mock_glossary_info = MagicMock()
        mock_fetch_glossary_info.return_value = mock_glossary_info
        test_urn = "urn:li:dataset:(urn:li:dataPlatform:test,test_entity,PROD)"
        mock_suggest_terms_batch.return_value = {
            test_urn: SuggestedTerms(table_terms=None, column_terms={})
        }

        # Process URN
        bulk_suggester.process_urns([test_urn])

        # Verify _suggest_terms_batch was called with custom_instructions
        mock_suggest_terms_batch.assert_called_once()
        call_args = mock_suggest_terms_batch.call_args
        assert call_args[1]["custom_instructions"] == config.custom_instructions
        assert (
            call_args[1]["custom_instructions"]
            == "Focus on PII classification and be conservative with confidence scores."
        )

    @patch("datahub_integrations.gen_ai.term_suggestion_action.fetch_glossary_info")
    @patch("datahub_integrations.gen_ai.term_suggestion_action._suggest_terms_batch")
    def test_custom_instructions_none_by_default(
        self,
        mock_suggest_terms_batch: MagicMock,
        mock_fetch_glossary_info: MagicMock,
        mock_graph: MagicMock,
    ) -> None:
        """Test that custom_instructions defaults to None when not provided."""

        # Create config WITHOUT custom instructions
        config = TermSuggestionActionConfig(
            entity_types_enabled=["SCHEMA_FIELD"],
            platforms=["test"],
            glossary_term_urns=["urn:li:glossaryTerm:TestTerm"],
            recommendation_action=AutomationApplyType.PROPOSE,
        )

        bulk_suggester = BulkTermSuggester(graph=mock_graph, config=config)

        # Mock glossary info and successful suggestions
        mock_glossary_info = MagicMock()
        mock_fetch_glossary_info.return_value = mock_glossary_info
        test_urn = "urn:li:dataset:(urn:li:dataPlatform:test,test_entity,PROD)"
        mock_suggest_terms_batch.return_value = {
            test_urn: SuggestedTerms(table_terms=None, column_terms={})
        }

        # Process URN
        bulk_suggester.process_urns([test_urn])

        # Verify _suggest_terms_batch was called with custom_instructions=None
        mock_suggest_terms_batch.assert_called_once()
        call_args = mock_suggest_terms_batch.call_args
        assert call_args[1]["custom_instructions"] is None

    @patch("datahub_integrations.gen_ai.term_suggestion_action.fetch_glossary_info")
    @patch("datahub_integrations.gen_ai.term_suggestion_action._suggest_terms_batch")
    def test_custom_instructions_empty_string(
        self,
        mock_suggest_terms_batch: MagicMock,
        mock_fetch_glossary_info: MagicMock,
        mock_graph: MagicMock,
    ) -> None:
        """Test that empty string custom_instructions are handled correctly."""

        # Create config with empty string custom instructions
        config = TermSuggestionActionConfig(
            entity_types_enabled=["SCHEMA_FIELD"],
            platforms=["test"],
            glossary_term_urns=["urn:li:glossaryTerm:TestTerm"],
            recommendation_action=AutomationApplyType.PROPOSE,
            custom_instructions="",
        )

        bulk_suggester = BulkTermSuggester(graph=mock_graph, config=config)

        # Mock glossary info and successful suggestions
        mock_glossary_info = MagicMock()
        mock_fetch_glossary_info.return_value = mock_glossary_info
        test_urn = "urn:li:dataset:(urn:li:dataPlatform:test,test_entity,PROD)"
        mock_suggest_terms_batch.return_value = {
            test_urn: SuggestedTerms(table_terms=None, column_terms={})
        }

        # Process URN
        bulk_suggester.process_urns([test_urn])

        # Verify _suggest_terms_batch was called with empty string
        mock_suggest_terms_batch.assert_called_once()
        call_args = mock_suggest_terms_batch.call_args
        assert call_args[1]["custom_instructions"] == ""


class TestPerformanceOptimization:
    """Test performance optimizations in BulkTermSuggester to prevent regression."""

    @patch("datahub_integrations.gen_ai.term_suggestion_action.fetch_glossary_info")
    @patch("datahub_integrations.gen_ai.term_suggestion_action._suggest_terms_batch")
    def test_fetch_glossary_info_called_once_for_multiple_urns(
        self,
        mock_suggest_terms_batch: MagicMock,
        mock_fetch_glossary_info: MagicMock,
        bulk_suggester: BulkTermSuggester,
        mock_graph: MagicMock,
    ) -> None:
        """Test that fetch_glossary_info is called only once even when processing multiple URNs.

        This is a regression test for the performance optimization where we fetch glossary info
        once per batch instead of once per URN.
        """

        # Set up test URNs
        urns = [
            "urn:li:dataset:(urn:li:dataPlatform:test,entity1,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:test,entity2,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:test,entity3,PROD)",
        ]

        # Mock glossary info
        mock_glossary_info = MagicMock()
        mock_fetch_glossary_info.return_value = mock_glossary_info

        # Mock successful suggestions for all URNs
        def suggest_terms_side_effect(
            graph: Any, entity_urns: list[str], glossary_info: Any
        ) -> dict[str, SuggestedTerms]:
            urn = entity_urns[0]  # Individual processing
            return {urn: SuggestedTerms(table_terms=None, column_terms={})}

        mock_suggest_terms_batch.side_effect = suggest_terms_side_effect

        # Process multiple URNs
        bulk_suggester.process_urns(urns)

        # CRITICAL: fetch_glossary_info should be called exactly once, not once per URN
        mock_fetch_glossary_info.assert_called_once()

        # Verify the glossary universe config passed to fetch_glossary_info
        call_args = mock_fetch_glossary_info.call_args
        assert call_args[1]["graph_client"] == mock_graph

        # _suggest_terms_batch should be called once per URN with the shared glossary_info
        assert mock_suggest_terms_batch.call_count == len(urns)

        # Verify each call to _suggest_terms_batch uses the same glossary_info instance
        for call in mock_suggest_terms_batch.call_args_list:
            assert call[1]["glossary_info"] == mock_glossary_info
            assert len(call[1]["entity_urns"]) == 1  # Individual processing

        # Verify all URNs were processed
        processed_urns = [
            call[1]["entity_urns"][0]
            for call in mock_suggest_terms_batch.call_args_list
        ]
        assert set(processed_urns) == set(urns)

    @patch("datahub_integrations.gen_ai.term_suggestion_action.fetch_glossary_info")
    @patch("datahub_integrations.gen_ai.term_suggestion_action._suggest_terms_batch")
    def test_fetch_glossary_info_called_once_with_shell_entities(
        self,
        mock_suggest_terms_batch: MagicMock,
        mock_fetch_glossary_info: MagicMock,
        bulk_suggester: BulkTermSuggester,
        mock_graph: MagicMock,
    ) -> None:
        """Test that fetch_glossary_info is called only once even when some URNs are shell entities.

        This ensures the optimization works correctly even when exceptions occur.
        """

        # Set up test URNs: mix of shell entities and normal entities
        shell_urn = "urn:li:dataset:(urn:li:dataPlatform:test,shell_entity,PROD)"
        success_urn = "urn:li:dataset:(urn:li:dataPlatform:test,success_entity,PROD)"
        retry_urn = "urn:li:dataset:(urn:li:dataPlatform:test,retry_entity,PROD)"
        urns = [shell_urn, success_urn, retry_urn]

        # Mock glossary info
        mock_glossary_info = MagicMock()
        mock_fetch_glossary_info.return_value = mock_glossary_info

        # Mock different responses for different URNs
        def suggest_terms_side_effect(
            graph: Any, entity_urns: list[str], glossary_info: Any
        ) -> dict[str, SuggestedTerms]:
            urn = entity_urns[0]
            if urn == shell_urn:
                raise ShellEntityError("No schema metadata")
            elif urn == retry_urn:
                raise RuntimeError("Network error")
            else:  # success_urn
                return {urn: SuggestedTerms(table_terms=None, column_terms={})}

        mock_suggest_terms_batch.side_effect = suggest_terms_side_effect

        # Process all URNs
        bulk_suggester.process_urns(urns)

        # CRITICAL: fetch_glossary_info should still be called exactly once
        # even when there are exceptions during processing
        mock_fetch_glossary_info.assert_called_once()

        # _suggest_terms_batch should be called once per URN with the same glossary_info
        assert mock_suggest_terms_batch.call_count == len(urns)

        # Verify each call uses the shared glossary_info
        for call in mock_suggest_terms_batch.call_args_list:
            assert call[1]["glossary_info"] == mock_glossary_info

    @patch("datahub_integrations.gen_ai.term_suggestion_action.fetch_glossary_info")
    @patch("datahub_integrations.gen_ai.term_suggestion_action._suggest_terms_batch")
    def test_fetch_glossary_info_called_once_single_urn(
        self,
        mock_suggest_terms_batch: MagicMock,
        mock_fetch_glossary_info: MagicMock,
        bulk_suggester: BulkTermSuggester,
        mock_graph: MagicMock,
    ) -> None:
        """Test that fetch_glossary_info is called once even for single URN processing."""

        # Single URN
        urn = "urn:li:dataset:(urn:li:dataPlatform:test,single_entity,PROD)"

        # Mock glossary info
        mock_glossary_info = MagicMock()
        mock_fetch_glossary_info.return_value = mock_glossary_info

        # Mock successful suggestion
        mock_suggest_terms_batch.return_value = {
            urn: SuggestedTerms(table_terms=None, column_terms={})
        }

        # Process single URN
        bulk_suggester.process_urns([urn])

        # fetch_glossary_info should be called exactly once
        mock_fetch_glossary_info.assert_called_once()

        # _suggest_terms_batch should be called once with the glossary_info
        mock_suggest_terms_batch.assert_called_once()
        call_args = mock_suggest_terms_batch.call_args
        assert call_args[1]["glossary_info"] == mock_glossary_info
        assert call_args[1]["entity_urns"] == [urn]
