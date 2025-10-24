"""
Unit tests for search_executor module.

Tests query building, two-pass search execution, and result merging/deduplication
for the ersatz semantic search implementation.
"""

from unittest.mock import MagicMock, patch

from datahub_integrations.smart_search.models import SearchConfig
from datahub_integrations.smart_search.search_executor import (
    _merge_and_dedupe,
    build_query_string,
    execute_two_pass_search,
)


class TestBuildQueryString:
    """Tests for build_query_string function."""

    def test_anchors_only(self):
        """Test building query with only anchors."""
        result = build_query_string(anchors=["premium", "plan"])
        assert result == "/q premium OR plan"

    def test_single_anchor(self):
        """Test building query with single anchor."""
        result = build_query_string(anchors=["premium"])
        assert result == "/q premium"

    def test_anchors_and_phrases(self):
        """Test building query with anchors and phrases."""
        result = build_query_string(
            anchors=["premium", "plan"],
            phrases=["subscription model", "pricing tier"],
        )
        assert (
            result == '/q premium OR plan OR "subscription model"~2 OR "pricing tier"~2'
        )

    def test_phrases_only(self):
        """Test building query with only phrases."""
        result = build_query_string(anchors=[], phrases=["subscription model"])
        assert result == '/q "subscription model"~2'

    def test_synonyms_only(self):
        """Test building query with only synonyms."""
        result = build_query_string(anchors=[], synonyms=["cost", "price", "rate"])
        assert result == "/q cost OR price OR rate"

    def test_all_keyword_types(self):
        """Test building query with anchors, phrases, and synonyms."""
        result = build_query_string(
            anchors=["premium"],
            phrases=["subscription model"],
            synonyms=["cost", "price"],
        )
        assert result == '/q premium OR "subscription model"~2 OR cost OR price'

    def test_empty_inputs_returns_wildcard(self):
        """Test that empty inputs return wildcard query."""
        result = build_query_string(anchors=[])
        assert result == "*"

    def test_none_inputs_returns_wildcard(self):
        """Test that None inputs return wildcard query."""
        result = build_query_string(anchors=[], phrases=None, synonyms=None)
        assert result == "*"

    def test_multiple_phrases(self):
        """Test multiple phrases are properly formatted."""
        result = build_query_string(
            anchors=[],
            phrases=["user profile", "account settings", "premium plan"],
        )
        assert (
            result == '/q "user profile"~2 OR "account settings"~2 OR "premium plan"~2'
        )

    def test_phrase_proximity_marker(self):
        """Test that phrases include ~2 proximity marker."""
        result = build_query_string(anchors=[], phrases=["test phrase"])
        assert "~2" in result
        assert '"test phrase"~2' in result

    def test_order_anchors_phrases_synonyms(self):
        """Test that query parts are ordered: anchors, phrases, synonyms."""
        result = build_query_string(
            anchors=["anchor1"],
            phrases=["phrase1"],
            synonyms=["syn1"],
        )
        # Should be in order: anchor, phrase, synonym
        parts = result.replace("/q ", "").split(" OR ")
        assert parts[0] == "anchor1"
        assert parts[1] == '"phrase1"~2'
        assert parts[2] == "syn1"


class TestMergeAndDedupe:
    """Tests for _merge_and_dedupe function."""

    def test_merge_no_duplicates(self):
        """Test merging when there are no duplicate URNs."""
        results_a = [
            {"entity": {"urn": "urn:li:dataset:1"}},
            {"entity": {"urn": "urn:li:dataset:2"}},
        ]
        results_b = [
            {"entity": {"urn": "urn:li:dataset:3"}},
            {"entity": {"urn": "urn:li:dataset:4"}},
        ]

        merged = _merge_and_dedupe(results_a, results_b, max_candidates=10)

        assert len(merged) == 4
        urns = [r["entity"]["urn"] for r in merged]
        assert urns == [
            "urn:li:dataset:1",
            "urn:li:dataset:2",
            "urn:li:dataset:3",
            "urn:li:dataset:4",
        ]

    def test_merge_with_duplicates(self):
        """Test that duplicate URNs are removed."""
        results_a = [
            {"entity": {"urn": "urn:li:dataset:1"}},
            {"entity": {"urn": "urn:li:dataset:2"}},
        ]
        results_b = [
            {"entity": {"urn": "urn:li:dataset:2"}},  # Duplicate
            {"entity": {"urn": "urn:li:dataset:3"}},
        ]

        merged = _merge_and_dedupe(results_a, results_b, max_candidates=10)

        assert len(merged) == 3
        urns = [r["entity"]["urn"] for r in merged]
        assert urns == ["urn:li:dataset:1", "urn:li:dataset:2", "urn:li:dataset:3"]

    def test_pass_a_prioritized_over_pass_b(self):
        """Test that Pass A results appear before Pass B results."""
        results_a = [
            {"entity": {"urn": "urn:li:dataset:a1"}},
            {"entity": {"urn": "urn:li:dataset:a2"}},
        ]
        results_b = [
            {"entity": {"urn": "urn:li:dataset:b1"}},
            {"entity": {"urn": "urn:li:dataset:b2"}},
        ]

        merged = _merge_and_dedupe(results_a, results_b, max_candidates=10)

        urns = [r["entity"]["urn"] for r in merged]
        # Pass A results should appear first
        assert urns[:2] == ["urn:li:dataset:a1", "urn:li:dataset:a2"]
        assert urns[2:] == ["urn:li:dataset:b1", "urn:li:dataset:b2"]

    def test_max_candidates_limit(self):
        """Test that results are capped at max_candidates."""
        results_a = [{"entity": {"urn": f"urn:li:dataset:a{i}"}} for i in range(5)]
        results_b = [{"entity": {"urn": f"urn:li:dataset:b{i}"}} for i in range(5)]

        merged = _merge_and_dedupe(results_a, results_b, max_candidates=3)

        assert len(merged) == 3
        # Should only include first 3 from Pass A
        urns = [r["entity"]["urn"] for r in merged]
        assert urns == ["urn:li:dataset:a0", "urn:li:dataset:a1", "urn:li:dataset:a2"]

    def test_max_candidates_includes_pass_b_when_pass_a_small(self):
        """Test that Pass B results fill remaining slots when Pass A is small."""
        results_a = [
            {"entity": {"urn": "urn:li:dataset:a1"}},
        ]
        results_b = [
            {"entity": {"urn": "urn:li:dataset:b1"}},
            {"entity": {"urn": "urn:li:dataset:b2"}},
            {"entity": {"urn": "urn:li:dataset:b3"}},
        ]

        merged = _merge_and_dedupe(results_a, results_b, max_candidates=3)

        assert len(merged) == 3
        urns = [r["entity"]["urn"] for r in merged]
        assert urns == ["urn:li:dataset:a1", "urn:li:dataset:b1", "urn:li:dataset:b2"]

    def test_empty_results_a(self):
        """Test with empty Pass A results."""
        results_a = []
        results_b = [
            {"entity": {"urn": "urn:li:dataset:1"}},
            {"entity": {"urn": "urn:li:dataset:2"}},
        ]

        merged = _merge_and_dedupe(results_a, results_b, max_candidates=10)

        assert len(merged) == 2

    def test_empty_results_b(self):
        """Test with empty Pass B results."""
        results_a = [
            {"entity": {"urn": "urn:li:dataset:1"}},
            {"entity": {"urn": "urn:li:dataset:2"}},
        ]
        results_b = []

        merged = _merge_and_dedupe(results_a, results_b, max_candidates=10)

        assert len(merged) == 2

    def test_both_empty(self):
        """Test with both passes empty."""
        merged = _merge_and_dedupe([], [], max_candidates=10)
        assert len(merged) == 0

    def test_missing_urn_skipped(self):
        """Test that results without URN are skipped."""
        results_a = [
            {"entity": {"urn": "urn:li:dataset:1"}},
            {"entity": {}},  # Missing URN
            {"entity": {"urn": "urn:li:dataset:2"}},
        ]
        results_b = []

        merged = _merge_and_dedupe(results_a, results_b, max_candidates=10)

        assert len(merged) == 2
        urns = [r["entity"]["urn"] for r in merged]
        assert urns == ["urn:li:dataset:1", "urn:li:dataset:2"]

    def test_missing_entity_key_skipped(self):
        """Test that results without entity key are skipped."""
        results_a = [
            {"entity": {"urn": "urn:li:dataset:1"}},
            {},  # Missing entity key
            {"entity": {"urn": "urn:li:dataset:2"}},
        ]
        results_b = []

        merged = _merge_and_dedupe(results_a, results_b, max_candidates=10)

        assert len(merged) == 2

    def test_duplicate_within_pass_a(self):
        """Test deduplication within Pass A results."""
        results_a = [
            {"entity": {"urn": "urn:li:dataset:1"}},
            {"entity": {"urn": "urn:li:dataset:1"}},  # Duplicate
            {"entity": {"urn": "urn:li:dataset:2"}},
        ]
        results_b = []

        merged = _merge_and_dedupe(results_a, results_b, max_candidates=10)

        assert len(merged) == 2
        urns = [r["entity"]["urn"] for r in merged]
        assert urns == ["urn:li:dataset:1", "urn:li:dataset:2"]

    def test_duplicate_within_pass_b(self):
        """Test deduplication within Pass B results."""
        results_a = []
        results_b = [
            {"entity": {"urn": "urn:li:dataset:1"}},
            {"entity": {"urn": "urn:li:dataset:1"}},  # Duplicate
            {"entity": {"urn": "urn:li:dataset:2"}},
        ]

        merged = _merge_and_dedupe(results_a, results_b, max_candidates=10)

        assert len(merged) == 2
        urns = [r["entity"]["urn"] for r in merged]
        assert urns == ["urn:li:dataset:1", "urn:li:dataset:2"]


class TestExecuteTwoPassSearch:
    """Tests for execute_two_pass_search function."""

    @patch("datahub_integrations.smart_search.search_executor.with_datahub_client")
    @patch("datahub_integrations.smart_search.search_executor._search_implementation")
    def test_execute_both_passes(self, mock_search_impl, mock_with_client):
        """Test executing both Pass A and Pass B."""
        # Setup mocks
        mock_client = MagicMock()
        mock_with_client.return_value.__enter__ = MagicMock(return_value=None)
        mock_with_client.return_value.__exit__ = MagicMock(return_value=False)

        # Mock search responses
        mock_search_impl.side_effect = [
            # Pass A response
            {
                "searchResults": [
                    {"entity": {"urn": "urn:li:dataset:1"}},
                    {"entity": {"urn": "urn:li:dataset:2"}},
                ]
            },
            # Pass B response
            {
                "searchResults": [
                    {"entity": {"urn": "urn:li:dataset:3"}},
                ]
            },
        ]

        # Execute
        config = SearchConfig(
            anchors_budget=100, synonyms_budget=50, max_candidates=200
        )
        result = execute_two_pass_search(
            client=mock_client,
            anchors=["premium"],
            phrases=["subscription model"],
            synonyms=["cost"],
            filters=None,
            config=config,
        )

        # Verify
        assert result["pass_a_count"] == 2
        assert result["pass_b_count"] == 1
        assert result["merged_count"] == 3
        assert len(result["results"]) == 3

        # Verify both searches were called
        assert mock_search_impl.call_count == 2

        # Verify Pass A call
        pass_a_call = mock_search_impl.call_args_list[0]
        assert '/q premium OR "subscription model"~2' == pass_a_call[1]["query"]
        assert pass_a_call[1]["num_results"] == 100
        assert pass_a_call[1]["search_strategy"] == "ersatz_semantic"

        # Verify Pass B call
        pass_b_call = mock_search_impl.call_args_list[1]
        assert "/q cost" == pass_b_call[1]["query"]
        assert pass_b_call[1]["num_results"] == 50
        assert pass_b_call[1]["search_strategy"] == "ersatz_semantic"

    @patch("datahub_integrations.smart_search.search_executor.with_datahub_client")
    @patch("datahub_integrations.smart_search.search_executor._search_implementation")
    def test_execute_pass_a_only(self, mock_search_impl, mock_with_client):
        """Test executing only Pass A when no synonyms provided."""
        # Setup mocks
        mock_client = MagicMock()
        mock_with_client.return_value.__enter__ = MagicMock(return_value=None)
        mock_with_client.return_value.__exit__ = MagicMock(return_value=False)

        mock_search_impl.return_value = {
            "searchResults": [
                {"entity": {"urn": "urn:li:dataset:1"}},
            ]
        }

        # Execute without synonyms
        config = SearchConfig()
        result = execute_two_pass_search(
            client=mock_client,
            anchors=["premium"],
            phrases=None,
            synonyms=None,  # No Pass B
            filters=None,
            config=config,
        )

        # Verify only Pass A executed
        assert mock_search_impl.call_count == 1
        assert result["pass_a_count"] == 1
        assert result["pass_b_count"] == 0
        assert result["merged_count"] == 1

    @patch("datahub_integrations.smart_search.search_executor.with_datahub_client")
    @patch("datahub_integrations.smart_search.search_executor._search_implementation")
    def test_execute_pass_b_only(self, mock_search_impl, mock_with_client):
        """Test executing only Pass B when no anchors/phrases provided."""
        # Setup mocks
        mock_client = MagicMock()
        mock_with_client.return_value.__enter__ = MagicMock(return_value=None)
        mock_with_client.return_value.__exit__ = MagicMock(return_value=False)

        mock_search_impl.return_value = {
            "searchResults": [
                {"entity": {"urn": "urn:li:dataset:1"}},
            ]
        }

        # Execute with only synonyms
        config = SearchConfig()
        result = execute_two_pass_search(
            client=mock_client,
            anchors=[],  # No Pass A
            phrases=None,
            synonyms=["cost"],
            filters=None,
            config=config,
        )

        # Verify only Pass B executed
        assert mock_search_impl.call_count == 1
        assert result["pass_a_count"] == 0
        assert result["pass_b_count"] == 1
        assert result["merged_count"] == 1

    @patch("datahub_integrations.smart_search.search_executor.with_datahub_client")
    @patch("datahub_integrations.smart_search.search_executor._search_implementation")
    def test_execute_with_filters(self, mock_search_impl, mock_with_client):
        """Test that filters are passed to search implementation."""
        # Setup mocks
        mock_client = MagicMock()
        mock_with_client.return_value.__enter__ = MagicMock(return_value=None)
        mock_with_client.return_value.__exit__ = MagicMock(return_value=False)

        mock_search_impl.return_value = {"searchResults": []}

        # Execute with filters
        config = SearchConfig()
        filters = "platform=snowflake"
        execute_two_pass_search(
            client=mock_client,
            anchors=["premium"],
            phrases=None,
            synonyms=None,
            filters=filters,
            config=config,
        )

        # Verify filters were passed
        mock_search_impl.assert_called_once()
        call_args = mock_search_impl.call_args
        assert call_args[1]["filters"] == filters

    @patch("datahub_integrations.smart_search.search_executor.with_datahub_client")
    @patch("datahub_integrations.smart_search.search_executor._search_implementation")
    def test_execute_respects_config_budgets(self, mock_search_impl, mock_with_client):
        """Test that config budgets are used for num_results."""
        # Setup mocks
        mock_client = MagicMock()
        mock_with_client.return_value.__enter__ = MagicMock(return_value=None)
        mock_with_client.return_value.__exit__ = MagicMock(return_value=False)

        mock_search_impl.side_effect = [
            {"searchResults": []},
            {"searchResults": []},
        ]

        # Execute with custom budgets
        config = SearchConfig(
            anchors_budget=150,
            synonyms_budget=75,
            max_candidates=250,
        )
        execute_two_pass_search(
            client=mock_client,
            anchors=["premium"],
            phrases=None,
            synonyms=["cost"],
            filters=None,
            config=config,
        )

        # Verify budgets were used
        pass_a_call = mock_search_impl.call_args_list[0]
        assert pass_a_call[1]["num_results"] == 150

        pass_b_call = mock_search_impl.call_args_list[1]
        assert pass_b_call[1]["num_results"] == 75

    @patch("datahub_integrations.smart_search.search_executor.with_datahub_client")
    @patch("datahub_integrations.smart_search.search_executor._search_implementation")
    def test_execute_deduplicates_results(self, mock_search_impl, mock_with_client):
        """Test that duplicate URNs across passes are deduplicated."""
        # Setup mocks
        mock_client = MagicMock()
        mock_with_client.return_value.__enter__ = MagicMock(return_value=None)
        mock_with_client.return_value.__exit__ = MagicMock(return_value=False)

        # Mock search responses with duplicate URN
        mock_search_impl.side_effect = [
            # Pass A
            {
                "searchResults": [
                    {"entity": {"urn": "urn:li:dataset:1"}},
                    {"entity": {"urn": "urn:li:dataset:2"}},
                ]
            },
            # Pass B (with duplicate)
            {
                "searchResults": [
                    {"entity": {"urn": "urn:li:dataset:2"}},  # Duplicate
                    {"entity": {"urn": "urn:li:dataset:3"}},
                ]
            },
        ]

        # Execute
        config = SearchConfig()
        result = execute_two_pass_search(
            client=mock_client,
            anchors=["premium"],
            phrases=None,
            synonyms=["cost"],
            filters=None,
            config=config,
        )

        # Verify deduplication
        assert result["pass_a_count"] == 2
        assert result["pass_b_count"] == 2
        assert result["merged_count"] == 3  # Deduplicated from 4 to 3

    @patch("datahub_integrations.smart_search.search_executor.with_datahub_client")
    @patch("datahub_integrations.smart_search.search_executor._search_implementation")
    def test_execute_respects_max_candidates(self, mock_search_impl, mock_with_client):
        """Test that max_candidates limit is enforced."""
        # Setup mocks
        mock_client = MagicMock()
        mock_with_client.return_value.__enter__ = MagicMock(return_value=None)
        mock_with_client.return_value.__exit__ = MagicMock(return_value=False)

        # Mock search responses with many results
        mock_search_impl.side_effect = [
            # Pass A
            {
                "searchResults": [
                    {"entity": {"urn": f"urn:li:dataset:a{i}"}} for i in range(100)
                ]
            },
            # Pass B
            {
                "searchResults": [
                    {"entity": {"urn": f"urn:li:dataset:b{i}"}} for i in range(100)
                ]
            },
        ]

        # Execute with small max_candidates
        config = SearchConfig(max_candidates=50)
        result = execute_two_pass_search(
            client=mock_client,
            anchors=["premium"],
            phrases=None,
            synonyms=["cost"],
            filters=None,
            config=config,
        )

        # Verify capped at max_candidates
        assert result["pass_a_count"] == 100
        assert result["pass_b_count"] == 100
        assert result["merged_count"] == 50  # Capped
        assert len(result["results"]) == 50

    @patch("datahub_integrations.smart_search.search_executor.with_datahub_client")
    @patch("datahub_integrations.smart_search.search_executor._search_implementation")
    def test_execute_with_empty_search_results(
        self, mock_search_impl, mock_with_client
    ):
        """Test handling of empty search results."""
        # Setup mocks
        mock_client = MagicMock()
        mock_with_client.return_value.__enter__ = MagicMock(return_value=None)
        mock_with_client.return_value.__exit__ = MagicMock(return_value=False)

        mock_search_impl.side_effect = [
            {"searchResults": []},
            {"searchResults": []},
        ]

        # Execute
        config = SearchConfig()
        result = execute_two_pass_search(
            client=mock_client,
            anchors=["premium"],
            phrases=None,
            synonyms=["cost"],
            filters=None,
            config=config,
        )

        # Verify empty results handled correctly
        assert result["pass_a_count"] == 0
        assert result["pass_b_count"] == 0
        assert result["merged_count"] == 0
        assert len(result["results"]) == 0

    @patch("datahub_integrations.smart_search.search_executor.with_datahub_client")
    @patch("datahub_integrations.smart_search.search_executor._search_implementation")
    @patch("datahub_integrations.smart_search.search_executor.logger")
    def test_execute_logs_queries_and_counts(
        self, mock_logger, mock_search_impl, mock_with_client
    ):
        """Test that queries and result counts are logged."""
        # Setup mocks
        mock_client = MagicMock()
        mock_with_client.return_value.__enter__ = MagicMock(return_value=None)
        mock_with_client.return_value.__exit__ = MagicMock(return_value=False)

        mock_search_impl.side_effect = [
            {"searchResults": [{"entity": {"urn": "urn:li:dataset:1"}}]},
            {"searchResults": [{"entity": {"urn": "urn:li:dataset:2"}}]},
        ]

        # Execute
        config = SearchConfig()
        execute_two_pass_search(
            client=mock_client,
            anchors=["premium"],
            phrases=None,
            synonyms=["cost"],
            filters=None,
            config=config,
        )

        # Verify logging
        log_calls = [str(call) for call in mock_logger.info.call_args_list]
        log_messages = " ".join(log_calls)

        assert "Pass A query" in log_messages
        assert "Pass B query" in log_messages
        assert "Pass A returned" in log_messages
        assert "Pass B returned" in log_messages
