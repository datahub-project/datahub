"""
Unit tests for smart_search module.

Tests the main smart_search function and result selection logic.
Note: _extract_keywords_from_query is already tested in test_keyword_extraction.py
"""

from unittest.mock import MagicMock, patch

from datahub_integrations.smart_search.models import SmartSearchResponse
from datahub_integrations.smart_search.reranker import RerankResult
from datahub_integrations.smart_search.smart_search import (
    _find_elbow_point,
    _select_entities_by_score_quality,
    _select_results_within_budget,
    smart_search,
)


class TestFindElbowPoint:
    """Tests for _find_elbow_point function - the core elbow detection algorithm."""

    def test_finds_elbow_in_curved_data(self):
        """Test that elbow is found in clearly curved score distribution."""
        # Steep decline then flatten - classic elbow shape
        scores = [0.95, 0.90, 0.85, 0.75, 0.60, 0.55, 0.52, 0.50, 0.49, 0.48]
        #                              ^-- elbow around here (index 4)

        elbow_idx = _find_elbow_point(scores)

        # Should find elbow in the middle area where curve bends
        assert elbow_idx is not None
        assert 3 <= elbow_idx <= 5  # Around the transition point

    def test_returns_none_for_linear_data(self):
        """Test that no elbow is found in perfectly linear score distribution."""
        # Perfect linear decline - no elbow
        scores = [1.0, 0.9, 0.8, 0.7, 0.6, 0.5, 0.4, 0.3, 0.2, 0.1]

        elbow_idx = _find_elbow_point(scores)

        # For perfectly linear data, all points lie exactly on the line from
        # first to last, so perpendicular distance is 0 for all points.
        # Since we only update elbow when dist > max_dist (starts at 0),
        # no elbow is selected.
        assert elbow_idx is None

    def test_returns_none_for_flat_data(self):
        """Test that no elbow is found when all scores are identical."""
        scores = [0.5, 0.5, 0.5, 0.5, 0.5, 0.5]

        elbow_idx = _find_elbow_point(scores)

        # Denominator becomes 0 when all scores are the same
        assert elbow_idx is None

    def test_returns_none_for_too_few_points(self):
        """Test that None is returned when fewer than 3 points."""
        assert _find_elbow_point([0.9, 0.8]) is None
        assert _find_elbow_point([0.9]) is None
        assert _find_elbow_point([]) is None

    def test_finds_elbow_at_sharp_transition(self):
        """Test elbow detection at a sharp score transition."""
        # Sharp drop in the middle
        scores = [0.90, 0.88, 0.85, 0.82, 0.40, 0.38, 0.36, 0.34]
        #                              ^-- sharp transition

        elbow_idx = _find_elbow_point(scores)

        assert elbow_idx is not None
        # Should be around the transition point
        assert 3 <= elbow_idx <= 4

    def test_handles_three_points_minimum(self):
        """Test that exactly 3 points can find an elbow."""
        scores = [1.0, 0.5, 0.4]  # Elbow at middle point

        elbow_idx = _find_elbow_point(scores)

        assert elbow_idx == 1  # Only middle point can be elbow


class TestSelectEntitiesByScoreQuality:
    """Tests for _select_entities_by_score_quality function."""

    def test_returns_minimum_initial_k_entities(self):
        """Test that at least initial_k entities are returned."""
        # Low quality scores, but should still return initial_k=4
        results = [RerankResult(index=i, score=0.2) for i in range(10)]

        selected, has_more, reason = _select_entities_by_score_quality(
            results, initial_k=4, max_entities=30
        )

        assert len(selected) >= 4
        # has_more is False because we stopped due to quality (plateau), not a limit
        assert has_more is False

    def test_detects_score_drop(self):
        """Test that large score drop triggers cutoff."""
        # Clear drop after position 3: 0.85 → 0.30
        results = [
            RerankResult(index=0, score=0.85),
            RerankResult(index=1, score=0.78),
            RerankResult(index=2, score=0.72),
            RerankResult(index=3, score=0.68),
            RerankResult(index=4, score=0.30),  # Large drop (>50% of top)
            RerankResult(index=5, score=0.28),
        ]

        selected, has_more, reason = _select_entities_by_score_quality(
            results, initial_k=4, relative_drop=0.5
        )

        # Should stop at 4 (initial_k), not include position 4 due to drop
        assert len(selected) == 4
        # has_more is False because we stopped due to quality drop, not a limit
        assert has_more is False
        assert reason == "score_drop_detected"

    def test_detects_elbow_point(self):
        """Test that elbow point in scores triggers cutoff."""
        # Clear elbow: steep decline then flattening
        results = [
            RerankResult(index=0, score=0.90),
            RerankResult(index=1, score=0.85),
            RerankResult(index=2, score=0.80),
            RerankResult(index=3, score=0.75),
            RerankResult(index=4, score=0.70),  # Elbow point - curve bends here
            RerankResult(index=5, score=0.68),  # Flattening out
            RerankResult(index=6, score=0.66),
            RerankResult(index=7, score=0.64),
            RerankResult(index=8, score=0.62),
            RerankResult(index=9, score=0.60),
        ]

        selected, has_more, reason = _select_entities_by_score_quality(
            results, initial_k=4, elbow_min_distance=0.03
        )

        # Should stop at or before elbow point
        assert len(selected) < 10
        # has_more is False because we stopped due to elbow detection, not a limit
        assert has_more is False
        assert reason == "elbow_detected"

    def test_returns_all_when_high_quality(self):
        """Test that all entities are returned when quality remains high."""
        # All scores within 40% of top (no 50% drop trigger)
        # Enough variation to avoid plateau (>0.05)
        results = [
            RerankResult(index=0, score=0.85),
            RerankResult(index=1, score=0.80),
            RerankResult(index=2, score=0.76),
            RerankResult(index=3, score=0.71),
            RerankResult(index=4, score=0.67),
            RerankResult(index=5, score=0.62),
            RerankResult(index=6, score=0.58),
            RerankResult(index=7, score=0.54),  # Still >50% of top (0.425)
            RerankResult(index=8, score=0.50),
            RerankResult(index=9, score=0.46),
        ]

        selected, has_more, reason = _select_entities_by_score_quality(
            results, initial_k=4, max_entities=30
        )

        # Should return all 10 (no cutoff triggered)
        assert len(selected) == 10
        assert has_more is False
        assert reason is None

    def test_respects_max_entities_limit(self):
        """Test that max_entities limit is enforced."""
        # 50 results with gradual linear decline (no elbow)
        results = [RerankResult(index=i, score=0.90 - (i * 0.005)) for i in range(50)]

        selected, has_more, reason = _select_entities_by_score_quality(
            results,
            initial_k=4,
            max_entities=30,
            relative_drop=0.9,  # Disable drop detection (90% tolerance)
            elbow_min_distance=0.5,  # High threshold to disable elbow detection
        )

        # Should stop at max_entities=30
        assert len(selected) == 30
        assert has_more is True
        assert reason == "max_entities_reached"

    def test_handles_fewer_than_initial_k(self):
        """Test handling when total results < initial_k."""
        results = [RerankResult(index=0, score=0.85), RerankResult(index=1, score=0.75)]

        selected, has_more, reason = _select_entities_by_score_quality(
            results, initial_k=4
        )

        # Should return all 2 (can't reach initial_k=4)
        assert len(selected) == 2
        assert has_more is False
        assert reason is None

    def test_handles_empty_results(self):
        """Test handling of empty result list."""
        selected, has_more, reason = _select_entities_by_score_quality([], initial_k=4)

        assert selected == []
        assert has_more is False
        assert reason is None

    def test_continues_past_initial_k_when_quality_good(self):
        """Test that selection continues beyond initial_k when quality is good."""
        # Good quality throughout - perfectly linear decline (no elbow)
        # All scores stay above 50% of top score (0.70 * 0.5 = 0.35)
        results = [
            RerankResult(index=0, score=0.70),
            RerankResult(index=1, score=0.66),
            RerankResult(index=2, score=0.62),
            RerankResult(index=3, score=0.58),
            RerankResult(index=4, score=0.54),
            RerankResult(index=5, score=0.50),
            RerankResult(index=6, score=0.46),
            RerankResult(index=7, score=0.42),
            RerankResult(index=8, score=0.38),
            RerankResult(index=9, score=0.36),
        ]

        selected, has_more, reason = _select_entities_by_score_quality(
            results,
            initial_k=4,
            max_entities=30,
            elbow_min_distance=0.5,  # Disable elbow detection (require 50% of range)
        )

        # Should return more than initial_k (no cutoff triggered)
        assert len(selected) > 4
        assert len(selected) == 10  # All of them
        assert has_more is False

    def test_low_confidence_query_returns_only_initial_k(self):
        """Test that low top scores trigger low_confidence_query cutoff."""
        # All scores below min_usable_score threshold (default 0.2)
        results = [
            RerankResult(index=0, score=0.15),  # Below 0.2 threshold
            RerankResult(index=1, score=0.12),
            RerankResult(index=2, score=0.10),
            RerankResult(index=3, score=0.08),
            RerankResult(index=4, score=0.06),
            RerankResult(index=5, score=0.04),
        ]

        selected, has_more, reason = _select_entities_by_score_quality(
            results, initial_k=4, min_usable_score=0.2
        )

        # Should return only initial_k when top score is below threshold
        assert len(selected) == 4
        assert has_more is False
        assert reason == "low_confidence_query"

    def test_min_usable_score_respects_custom_threshold(self):
        """Test that custom min_usable_score threshold is respected."""
        results = [
            RerankResult(index=0, score=0.35),  # Above 0.2, but below 0.4
            RerankResult(index=1, score=0.30),
            RerankResult(index=2, score=0.25),
            RerankResult(index=3, score=0.20),
            RerankResult(index=4, score=0.15),
            RerankResult(index=5, score=0.10),
        ]

        # With default threshold (0.2), should return more than initial_k
        selected1, _, reason1 = _select_entities_by_score_quality(
            results, initial_k=3, min_usable_score=0.2
        )

        # With higher threshold (0.4), should trigger low_confidence
        selected2, _, reason2 = _select_entities_by_score_quality(
            results, initial_k=3, min_usable_score=0.4
        )

        assert reason1 != "low_confidence_query"
        assert reason2 == "low_confidence_query"
        assert len(selected2) == 3  # Only initial_k

    def test_elbow_ignored_when_before_initial_k(self):
        """Test that elbow within initial_k range doesn't trigger cutoff."""
        # Elbow at position 2, but initial_k=4 should override
        results = [
            RerankResult(index=0, score=0.95),
            RerankResult(index=1, score=0.90),
            RerankResult(index=2, score=0.50),  # Sharp drop - potential elbow
            RerankResult(index=3, score=0.48),
            RerankResult(index=4, score=0.46),
            RerankResult(index=5, score=0.44),
        ]

        selected, has_more, reason = _select_entities_by_score_quality(
            results, initial_k=4, elbow_min_distance=0.01
        )

        # Should still include at least initial_k even if elbow is earlier
        assert len(selected) >= 4

    def test_all_identical_scores(self):
        """Test handling of all identical scores."""
        results = [RerankResult(index=i, score=0.5) for i in range(10)]

        selected, has_more, reason = _select_entities_by_score_quality(
            results, initial_k=4, max_entities=30
        )

        # Should return all results (no elbow, no drop)
        # Since all scores are identical, has_more is False because we returned all
        assert len(selected) >= 4
        # With identical scores, there's no quality-based reason to stop

    def test_cliff_detection_takes_priority_over_elbow(self):
        """Test that dramatic cliff (>50% drop) is detected even if elbow exists."""
        # Both elbow and cliff present - cliff should trigger first
        results = [
            RerankResult(index=0, score=0.90),
            RerankResult(index=1, score=0.85),
            RerankResult(index=2, score=0.80),
            RerankResult(index=3, score=0.75),
            RerankResult(index=4, score=0.35),  # >50% drop from top (cliff)
            RerankResult(index=5, score=0.30),
            RerankResult(index=6, score=0.28),
        ]

        selected, has_more, reason = _select_entities_by_score_quality(
            results, initial_k=4, relative_drop=0.5
        )

        # Cliff detection should stop at position 4
        assert len(selected) == 4
        assert reason == "score_drop_detected"
        assert has_more is False  # Quality-based cutoff


class TestSelectResultsWithinBudget:
    """Tests for _select_results_within_budget generator function."""

    @patch("datahub_integrations.smart_search.smart_search.clean_get_entities_response")
    @patch("datahub_integrations.mcp.mcp_server.TokenCountEstimator")
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

        # Lambda to extract entity
        def get_entity(rr):
            return candidates[rr.index]["entity"]

        # Execute with max_results=3
        results = list(
            _select_results_within_budget(
                results=iter(rerank_results),
                fetch_entity=get_entity,
                max_results=3,
                token_budget=10000,
            )
        )

        # Should yield exactly 3 RerankResult objects
        assert len(results) == 3
        assert results[0].index == 0
        assert results[1].index == 1
        assert results[2].index == 2

    @patch("datahub_integrations.smart_search.smart_search.clean_get_entities_response")
    @patch("datahub_integrations.mcp.mcp_server.TokenCountEstimator")
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

        # Lambda to extract entity
        def get_entity(rr):
            return candidates[rr.index]["entity"]

        # Execute with budget for 2.5 entities
        results = list(
            _select_results_within_budget(
                results=iter(rerank_results),
                fetch_entity=get_entity,
                max_results=10,
                token_budget=2500,  # Should fit 2 entities (2000 tokens), not 3rd (3000 total)
            )
        )

        # Should stop at 2 results due to budget
        assert len(results) == 2
        assert results[0].index == 0
        assert results[1].index == 1

    @patch("datahub_integrations.smart_search.smart_search.clean_get_entities_response")
    @patch("datahub_integrations.mcp.mcp_server.TokenCountEstimator")
    @patch("datahub_integrations.mcp.mcp_server.logger")
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

        # Lambda to extract entity
        def get_entity(rr):
            return candidates[rr.index]["entity"]

        # Execute with small budget
        results = list(
            _select_results_within_budget(
                results=iter(rerank_results),
                fetch_entity=get_entity,
                max_results=10,
                token_budget=1000,  # Much smaller than entity size
            )
        )

        # Should still yield 1 result despite exceeding budget
        assert len(results) == 1
        assert results[0].index == 0

        # Should have logged a warning
        mock_logger.warning.assert_called_once()
        warning_msg = mock_logger.warning.call_args[0][0]
        assert "exceeds budget" in warning_msg

    @patch("datahub_integrations.smart_search.smart_search.clean_get_entities_response")
    @patch("datahub_integrations.mcp.mcp_server.TokenCountEstimator")
    def test_uses_default_token_budget(self, mock_estimator, mock_clean_response):
        """Test that default token budget is 90% of limit."""
        mock_estimator.estimate_dict_tokens.return_value = 1000
        mock_clean_response.side_effect = lambda x: x

        rerank_results = [RerankResult(index=0, score=0.9)]
        candidates = [{"entity": {"urn": "urn:li:dataset:1"}}]

        # Lambda to extract entity
        def get_entity(rr):
            return candidates[rr.index]["entity"]

        # Execute without token_budget parameter
        results = list(
            _select_results_within_budget(
                results=iter(rerank_results),
                fetch_entity=get_entity,
                max_results=10,
                token_budget=None,  # Should use default
            )
        )

        assert len(results) == 1

    @patch("datahub_integrations.smart_search.smart_search.truncate_descriptions")
    @patch("datahub_integrations.smart_search.smart_search.clean_get_entities_response")
    @patch("datahub_integrations.mcp.mcp_server.TokenCountEstimator")
    def test_truncates_and_cleans_entities_before_yielding(
        self, mock_estimator, mock_clean_response, mock_truncate
    ):
        """Test that entities are truncated and cleaned before being yielded."""
        # Setup mocks
        mock_estimator.estimate_dict_tokens.return_value = 100
        mock_clean_response.return_value = {"cleaned": True}

        # Create test data
        rerank_results = [RerankResult(index=0, score=0.9)]
        raw_entity = {"urn": "urn:li:dataset:1", "raw": "data"}
        candidates = [{"entity": raw_entity}]

        # Lambda that mimics smart_search's get_cleaned_entity
        def get_entity(rr):
            candidate = candidates[rr.index]
            entity = candidate["entity"]
            mock_truncate(entity)
            cleaned = mock_clean_response(entity)
            candidate["entity"] = cleaned
            return cleaned

        # Execute
        results = list(
            _select_results_within_budget(
                results=iter(rerank_results),
                fetch_entity=get_entity,
                max_results=10,
                token_budget=10000,
            )
        )

        # Verify we got the RerankResult back
        assert len(results) == 1
        assert results[0].index == 0

        # Verify entity in candidate was cleaned
        assert candidates[0]["entity"] == {"cleaned": True}

        # Verify truncate_descriptions was called
        mock_truncate.assert_called_once_with(raw_entity)

        # Verify clean_get_entities_response was called
        mock_clean_response.assert_called_once_with(raw_entity)

    @patch("datahub_integrations.smart_search.smart_search.clean_get_entities_response")
    @patch("datahub_integrations.mcp.mcp_server.TokenCountEstimator")
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

        # Lambda to extract entity
        def get_entity(rr):
            return candidates[rr.index]["entity"]

        # Execute
        results = list(
            _select_results_within_budget(
                results=iter(rerank_results),
                fetch_entity=get_entity,
                max_results=10,
                token_budget=10000,
            )
        )

        # Should yield RerankResult objects in rerank order (by score)
        assert len(results) == 3
        assert results[0].index == 2  # Highest score
        assert results[1].index == 0  # Second score
        assert results[2].index == 1  # Lowest score


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
        assert result.candidates_reviewed == 2
        assert result.returned_count == len(result.results)
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
        assert result.candidates_reviewed == 0
        assert result.returned_count == 0
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
    def test_smart_search_dynamic_result_count(
        self, mock_search_impl, mock_create_reranker
    ):
        """Test that smart_search uses dynamic result count based on scores."""
        # Setup mocks with 10 candidates
        candidates = [{"entity": {"urn": f"urn:li:dataset:{i}"}} for i in range(10)]
        mock_search_impl.return_value = {
            "searchResults": candidates,
            "facets": [],
        }

        mock_reranker = MagicMock()
        # High scores that gradually decline
        mock_reranker.rerank.return_value = [
            RerankResult(index=i, score=0.80 - (i * 0.05)) for i in range(10)
        ]
        mock_create_reranker.return_value = mock_reranker

        # Execute
        result = smart_search(
            semantic_query="find datasets",
            keyword_search_query="/q datasets",
            filters=None,
        )

        # Should use quality-based selection (not fixed at 5)
        assert result.candidates_reviewed == 10
        # has_more_selected_results should be set
        assert result.has_more_selected_results is not None
        assert result.returned_count == len(result.results)

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

        # Verify logging - check that key logging events occurred
        log_calls = [str(call) for call in mock_logger.info.call_args_list]
        log_messages = " ".join(log_calls)

        # Check for reranker call and smart_search completion logs
        assert "Calling reranker" in log_messages
        assert "entities" in log_messages
