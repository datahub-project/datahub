"""
Unit tests for smart_search module.

Tests the main smart_search function and result selection logic.
Note: _extract_keywords_from_query is already tested in test_keyword_extraction.py
"""

from unittest.mock import MagicMock, patch

from datahub_integrations.smart_search.models import SmartSearchResponse
from datahub_integrations.smart_search.reranker import RerankResult
from datahub_integrations.smart_search.smart_search import (
    _select_results_within_budget,
    smart_search,
)


class TestSelectResultsWithinBudget:
    """Tests for _select_results_within_budget generator function."""

    @patch("datahub_integrations.smart_search.smart_search.clean_get_entities_response")
    @patch("datahub_integrations.smart_search.smart_search.TokenCountEstimator")
    def test_selects_results_up_to_max_results(
        self, mock_estimator, mock_clean_response
    ):
        """Test that generator yields up to max_results."""
        # Setup mocks
        mock_estimator.estimate_dict_tokens.return_value = 100
        mock_clean_response.side_effect = lambda x: x  # Return as-is

        # Create test data
        rerank_results = [
            RerankResult(index=0, score=0.9),
            RerankResult(index=1, score=0.8),
            RerankResult(index=2, score=0.7),
            RerankResult(index=3, score=0.6),
            RerankResult(index=4, score=0.5),
        ]
        candidates = [{"entity": {"urn": f"urn:li:dataset:{i}"}} for i in range(5)]

        # Execute with max_results=3
        results = list(
            _select_results_within_budget(
                rerank_results=rerank_results,
                candidates=candidates,
                max_results=3,
                token_budget=10000,
            )
        )

        # Verify only 3 results returned
        assert len(results) == 3

    @patch("datahub_integrations.smart_search.smart_search.clean_get_entities_response")
    @patch("datahub_integrations.smart_search.smart_search.TokenCountEstimator")
    def test_stops_at_token_budget(self, mock_estimator, mock_clean_response):
        """Test that generator stops when token budget would be exceeded."""
        # Setup mocks
        mock_estimator.estimate_dict_tokens.return_value = (
            1000  # Each entity is 1000 tokens
        )
        mock_clean_response.side_effect = lambda x: x

        # Create test data
        rerank_results = [
            RerankResult(index=i, score=1.0 - (i * 0.1)) for i in range(10)
        ]
        candidates = [{"entity": {"urn": f"urn:li:dataset:{i}"}} for i in range(10)]

        # Execute with budget for 2.5 entities
        results = list(
            _select_results_within_budget(
                rerank_results=rerank_results,
                candidates=candidates,
                max_results=10,
                token_budget=2500,  # Should fit 2 entities (2000 tokens), not 3rd (3000 total)
            )
        )

        # Verify stopped at 2 entities
        assert len(results) == 2

    @patch("datahub_integrations.smart_search.smart_search.clean_get_entities_response")
    @patch("datahub_integrations.smart_search.smart_search.TokenCountEstimator")
    @patch("datahub_integrations.smart_search.smart_search.logger")
    def test_always_yields_at_least_one_result(
        self, mock_logger, mock_estimator, mock_clean_response
    ):
        """Test that at least one result is yielded even if it exceeds budget."""
        # Setup mocks - first entity exceeds budget
        mock_estimator.estimate_dict_tokens.return_value = 10000
        mock_clean_response.side_effect = lambda x: x

        # Create test data
        rerank_results = [RerankResult(index=0, score=0.9)]
        candidates = [{"entity": {"urn": "urn:li:dataset:1"}}]

        # Execute with small budget
        results = list(
            _select_results_within_budget(
                rerank_results=rerank_results,
                candidates=candidates,
                max_results=10,
                token_budget=1000,  # Much smaller than entity size
            )
        )

        # Verify at least 1 result returned
        assert len(results) == 1

        # Verify warning was logged
        mock_logger.warning.assert_called_once()
        warning_msg = mock_logger.warning.call_args[0][0]
        assert "exceeds budget" in warning_msg

    @patch("datahub_integrations.smart_search.smart_search.clean_get_entities_response")
    @patch("datahub_integrations.smart_search.smart_search.TokenCountEstimator")
    def test_uses_default_token_budget(self, mock_estimator, mock_clean_response):
        """Test that default token budget is 90% of limit."""
        mock_estimator.estimate_dict_tokens.return_value = 1000
        mock_clean_response.side_effect = lambda x: x

        rerank_results = [RerankResult(index=0, score=0.9)]
        candidates = [{"entity": {"urn": "urn:li:dataset:1"}}]

        # Execute without token_budget parameter
        results = list(
            _select_results_within_budget(
                rerank_results=rerank_results,
                candidates=candidates,
                max_results=10,
                token_budget=None,  # Should use default
            )
        )

        assert len(results) == 1

    @patch("datahub_integrations.smart_search.smart_search.clean_get_entities_response")
    @patch("datahub_integrations.smart_search.smart_search.TokenCountEstimator")
    def test_cleans_entities_before_yielding(self, mock_estimator, mock_clean_response):
        """Test that entities are cleaned before being yielded."""
        # Setup mocks
        mock_estimator.estimate_dict_tokens.return_value = 100
        mock_clean_response.return_value = {"cleaned": True}

        # Create test data
        rerank_results = [RerankResult(index=0, score=0.9)]
        candidates = [{"entity": {"urn": "urn:li:dataset:1", "raw": "data"}}]

        # Execute
        results = list(
            _select_results_within_budget(
                rerank_results=rerank_results,
                candidates=candidates,
                max_results=10,
                token_budget=10000,
            )
        )

        # Verify clean_get_entities_response was called
        mock_clean_response.assert_called_once()
        assert results[0] == {"cleaned": True}

    @patch("datahub_integrations.smart_search.smart_search.clean_get_entities_response")
    @patch("datahub_integrations.smart_search.smart_search.TokenCountEstimator")
    def test_respects_rerank_order(self, mock_estimator, mock_clean_response):
        """Test that results are yielded in rerank score order."""
        mock_estimator.estimate_dict_tokens.return_value = 100
        mock_clean_response.side_effect = lambda x: x  # Return entity as-is

        # Create test data with specific order
        rerank_results = [
            RerankResult(index=2, score=0.95),  # Highest score
            RerankResult(index=0, score=0.85),
            RerankResult(index=1, score=0.75),
        ]
        candidates = [
            {"entity": {"urn": "urn:li:dataset:0"}},
            {"entity": {"urn": "urn:li:dataset:1"}},
            {"entity": {"urn": "urn:li:dataset:2"}},
        ]

        # Execute
        results = list(
            _select_results_within_budget(
                rerank_results=rerank_results,
                candidates=candidates,
                max_results=10,
                token_budget=10000,
            )
        )

        # Verify order matches rerank results
        # Note: clean_get_entities_response returns just the entity, not wrapped
        assert results[0]["urn"] == "urn:li:dataset:2"
        assert results[1]["urn"] == "urn:li:dataset:0"
        assert results[2]["urn"] == "urn:li:dataset:1"


class TestSmartSearch:
    """Tests for smart_search function."""

    @patch("datahub_integrations.smart_search.smart_search.create_reranker")
    @patch("datahub_integrations.smart_search.smart_search._search_implementation")
    def test_smart_search_basic_functionality(
        self, mock_search_impl, mock_create_reranker
    ):
        """Test basic smart_search execution."""
        # Setup mocks
        mock_search_impl.return_value = {
            "searchResults": [
                {"entity": {"urn": "urn:li:dataset:1"}},
                {"entity": {"urn": "urn:li:dataset:2"}},
            ],
            "facets": [{"field": "platform", "values": ["snowflake"]}],
        }

        mock_reranker = MagicMock()
        mock_reranker.rerank.return_value = [
            RerankResult(index=1, score=0.9),
            RerankResult(index=0, score=0.8),
        ]
        mock_create_reranker.return_value = mock_reranker

        # Execute
        result = smart_search(
            semantic_query="find datasets",
            keyword_search_query="/q datasets",
            filters=None,
        )

        # Verify
        assert isinstance(result, SmartSearchResponse)
        assert result.total_candidates == 2
        assert len(result.facets) == 1

        # Verify search was called
        mock_search_impl.assert_called_once()
        call_args = mock_search_impl.call_args
        assert call_args[1]["query"] == "/q datasets"
        assert call_args[1]["num_results"] == 100
        assert call_args[1]["search_strategy"] == "ersatz_semantic"

        # Verify reranker was called
        mock_reranker.rerank.assert_called_once()

    @patch("datahub_integrations.smart_search.smart_search.create_reranker")
    @patch("datahub_integrations.smart_search.smart_search._search_implementation")
    def test_smart_search_no_candidates(self, mock_search_impl, mock_create_reranker):
        """Test smart_search when no candidates are found."""
        # Setup mocks
        mock_search_impl.return_value = {
            "searchResults": [],
            "facets": [],
        }

        # Execute
        result = smart_search(
            semantic_query="find datasets",
            keyword_search_query="/q nonexistent",
            filters=None,
        )

        # Verify
        assert isinstance(result, SmartSearchResponse)
        assert result.total_candidates == 0
        assert len(result.results) == 0
        assert len(result.facets) == 0

        # Verify reranker was NOT called
        mock_create_reranker.return_value.rerank.assert_not_called()

    @patch("datahub_integrations.smart_search.smart_search.create_reranker")
    @patch("datahub_integrations.smart_search.smart_search._search_implementation")
    def test_smart_search_with_filters(self, mock_search_impl, mock_create_reranker):
        """Test that filters are passed to search implementation."""
        # Setup mocks
        mock_search_impl.return_value = {
            "searchResults": [{"entity": {"urn": "urn:li:dataset:1"}}],
            "facets": [],
        }

        mock_reranker = MagicMock()
        mock_reranker.rerank.return_value = [RerankResult(index=0, score=0.9)]
        mock_create_reranker.return_value = mock_reranker

        # Execute with filters
        filters = {"entity_type": ["DATASET"]}
        smart_search(
            semantic_query="find datasets",
            keyword_search_query="/q datasets",
            filters=filters,
        )

        # Verify filters were passed
        call_args = mock_search_impl.call_args
        assert call_args[1]["filters"] == filters

    @patch("datahub_integrations.smart_search.smart_search.create_reranker")
    @patch("datahub_integrations.smart_search.smart_search._search_implementation")
    def test_smart_search_limits_to_5_results(
        self, mock_search_impl, mock_create_reranker
    ):
        """Test that smart_search returns maximum 5 results."""
        # Setup mocks with 10 candidates
        candidates = [{"entity": {"urn": f"urn:li:dataset:{i}"}} for i in range(10)]
        mock_search_impl.return_value = {
            "searchResults": candidates,
            "facets": [],
        }

        mock_reranker = MagicMock()
        mock_reranker.rerank.return_value = [
            RerankResult(index=i, score=1.0 - (i * 0.1)) for i in range(10)
        ]
        mock_create_reranker.return_value = mock_reranker

        # Execute
        result = smart_search(
            semantic_query="find datasets",
            keyword_search_query="/q datasets",
            filters=None,
        )

        # Verify max 5 results returned
        assert len(result.results) <= 5
        assert result.total_candidates == 10

    @patch("datahub_integrations.smart_search.smart_search.create_reranker")
    @patch("datahub_integrations.smart_search.smart_search._search_implementation")
    def test_smart_search_passes_queries_to_reranker(
        self, mock_search_impl, mock_create_reranker
    ):
        """Test that both queries are passed to reranker."""
        # Setup mocks
        mock_search_impl.return_value = {
            "searchResults": [{"entity": {"urn": "urn:li:dataset:1"}}],
            "facets": [],
        }

        mock_reranker = MagicMock()
        mock_reranker.rerank.return_value = [RerankResult(index=0, score=0.9)]
        mock_create_reranker.return_value = mock_reranker

        # Execute
        semantic_query = "find premium pricing datasets"
        keyword_query = "/q premium+pricing"
        smart_search(
            semantic_query=semantic_query,
            keyword_search_query=keyword_query,
            filters=None,
        )

        # Verify reranker received both queries
        call_args = mock_reranker.rerank.call_args
        assert call_args[1]["semantic_query"] == semantic_query
        assert call_args[1]["keyword_search_query"] == keyword_query

    @patch("datahub_integrations.smart_search.smart_search.create_reranker")
    @patch("datahub_integrations.smart_search.smart_search._search_implementation")
    def test_smart_search_extracts_entities_for_reranking(
        self, mock_search_impl, mock_create_reranker
    ):
        """Test that entities are extracted from candidates for reranking."""
        # Setup mocks
        mock_search_impl.return_value = {
            "searchResults": [
                {
                    "entity": {"urn": "urn:li:dataset:1", "name": "Dataset 1"},
                    "score": 0.5,
                },
                {
                    "entity": {"urn": "urn:li:dataset:2", "name": "Dataset 2"},
                    "score": 0.4,
                },
            ],
            "facets": [],
        }

        mock_reranker = MagicMock()
        mock_reranker.rerank.return_value = [
            RerankResult(index=0, score=0.9),
            RerankResult(index=1, score=0.8),
        ]
        mock_create_reranker.return_value = mock_reranker

        # Execute
        smart_search(
            semantic_query="find datasets",
            keyword_search_query="/q datasets",
            filters=None,
        )

        # Verify entities were extracted and passed to reranker
        call_args = mock_reranker.rerank.call_args
        entities = call_args[1]["entities"]
        assert len(entities) == 2
        assert entities[0] == {"urn": "urn:li:dataset:1", "name": "Dataset 1"}
        assert entities[1] == {"urn": "urn:li:dataset:2", "name": "Dataset 2"}

    @patch("datahub_integrations.smart_search.smart_search.create_reranker")
    @patch("datahub_integrations.smart_search.smart_search._search_implementation")
    @patch("datahub_integrations.smart_search.smart_search.logger")
    def test_smart_search_logs_appropriately(
        self, mock_logger, mock_search_impl, mock_create_reranker
    ):
        """Test that smart_search logs key steps."""
        # Setup mocks
        mock_search_impl.return_value = {
            "searchResults": [{"entity": {"urn": "urn:li:dataset:1"}}],
            "facets": [],
        }

        mock_reranker = MagicMock()
        mock_reranker.rerank.return_value = [RerankResult(index=0, score=0.9)]
        mock_create_reranker.return_value = mock_reranker

        # Execute
        smart_search(
            semantic_query="find datasets",
            keyword_search_query="/q datasets",
            filters=None,
        )

        # Verify logging
        log_calls = [str(call) for call in mock_logger.info.call_args_list]
        log_messages = " ".join(log_calls)

        assert "smart_search called" in log_messages
        assert "Found" in log_messages
        assert "candidates" in log_messages
        assert "Returning" in log_messages
