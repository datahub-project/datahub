"""
Unit tests for _extract_keywords_from_query.

Tests keyword extraction from /q search queries, including edge cases:
- Quoted phrases
- Field-specific syntax
- Boolean operators
- Special markers (proximity, wildcards)
"""

from datahub_integrations.smart_search.smart_search import _extract_keywords_from_query


class TestExtractKeywordsFromQuery:
    """Tests for the _extract_keywords_from_query function."""

    def test_basic_single_keyword(self):
        """Test extraction of a single keyword."""
        result = _extract_keywords_from_query("/q premium")
        assert result == ["premium"]

    def test_basic_multiple_keywords_with_plus(self):
        """Test extraction with + operator (AND)."""
        result = _extract_keywords_from_query("/q premium+plan")
        assert result == ["premium", "plan"]

    def test_multiple_keywords_with_or(self):
        """Test extraction with OR operator."""
        result = _extract_keywords_from_query("/q premium OR plan")
        assert result == ["premium", "plan"]

    def test_complex_boolean_query(self):
        """Test complex query with multiple operators."""
        result = _extract_keywords_from_query(
            "/q premium+plan OR organization+subscription"
        )
        assert result == ["premium", "plan", "organization", "subscription"]

    def test_operators_are_excluded(self):
        """Test that boolean operators are not included as keywords."""
        result = _extract_keywords_from_query(
            "/q premium AND plan OR organization NOT test"
        )
        assert result == ["premium", "plan", "organization", "test"]
        # Verify operators are not in results
        assert "and" not in result
        assert "or" not in result
        assert "not" not in result

    def test_operators_case_insensitive(self):
        """Test that operators are filtered regardless of case."""
        result = _extract_keywords_from_query("/q premium AND plan OR organization")
        assert result == ["premium", "plan", "organization"]

    def test_quoted_phrase(self):
        """Test extraction from quoted phrases - splits into individual words."""
        result = _extract_keywords_from_query('/q "premium plan"')
        assert result == ["premium", "plan"]

    def test_quoted_phrase_with_other_keywords(self):
        """Test quoted phrase combined with regular keywords."""
        result = _extract_keywords_from_query('/q "premium plan" OR subscription')
        assert result == ["subscription", "premium", "plan"]

    def test_multiple_quoted_phrases(self):
        """Test multiple quoted phrases."""
        result = _extract_keywords_from_query(
            '/q "premium plan" AND "organization subscription"'
        )
        assert result == ["premium", "plan", "organization", "subscription"]

    def test_field_syntax_extracts_value_only(self):
        """Test that field:value syntax extracts only the value."""
        result = _extract_keywords_from_query("/q name:premium")
        assert result == ["premium"]
        assert "name" not in result

    def test_field_syntax_with_other_keywords(self):
        """Test field syntax combined with regular keywords."""
        result = _extract_keywords_from_query("/q name:premium+plan")
        assert result == ["premium", "plan"]

    def test_multiple_field_syntax(self):
        """Test multiple field:value pairs."""
        result = _extract_keywords_from_query("/q name:premium description:plan")
        assert result == ["premium", "plan"]
        assert "name" not in result
        assert "description" not in result

    def test_proximity_marker_removed(self):
        """Test that proximity markers (~N) are removed."""
        result = _extract_keywords_from_query('/q "premium plan"~2')
        assert result == ["premium", "plan"]

    def test_wildcard_removed(self):
        """Test that wildcards (*) are removed."""
        result = _extract_keywords_from_query("/q user*")
        assert result == ["user"]

    def test_wildcard_with_multiple_keywords(self):
        """Test wildcard removal with multiple keywords."""
        result = _extract_keywords_from_query("/q user*+profile*")
        assert result == ["user", "profile"]

    def test_parentheses_handled(self):
        """Test that parentheses are properly handled."""
        result = _extract_keywords_from_query("/q (premium OR plan) AND organization")
        assert result == ["premium", "plan", "organization"]

    def test_complex_real_world_query(self):
        """Test a complex real-world query with multiple features."""
        result = _extract_keywords_from_query(
            '/q (revenue OR sales) AND quarterly name:report* "financial data"~5'
        )
        assert set(result) == {
            "revenue",
            "sales",
            "quarterly",
            "report",
            "financial",
            "data",
        }

    def test_empty_query(self):
        """Test empty query returns empty list."""
        result = _extract_keywords_from_query("/q ")
        assert result == []

    def test_only_operators(self):
        """Test query with only operators returns empty list."""
        result = _extract_keywords_from_query("/q AND OR NOT")
        assert result == []

    def test_deduplication(self):
        """Test that duplicate keywords are removed while preserving order."""
        result = _extract_keywords_from_query("/q premium+plan+premium")
        assert result == ["premium", "plan"]

    def test_deduplication_with_quoted_phrases(self):
        """Test deduplication with quoted phrases."""
        result = _extract_keywords_from_query('/q premium+"premium plan"')
        assert result == ["premium", "plan"]

    def test_mixed_case_normalized(self):
        """Test that keywords are normalized to lowercase."""
        result = _extract_keywords_from_query("/q PREMIUM+Plan+ORGanization")
        assert result == ["premium", "plan", "organization"]

    def test_plus_operator_not_included_as_keyword(self):
        """Test that + operator itself is not included as a keyword."""
        result = _extract_keywords_from_query("/q premium+plan+organization")
        assert result == ["premium", "plan", "organization"]
        assert "+" not in result

    def test_whitespace_handling(self):
        """Test various whitespace scenarios."""
        result = _extract_keywords_from_query("/q   premium    plan   ")
        assert result == ["premium", "plan"]

    def test_special_chars_in_field_value(self):
        """Test field syntax with special characters in value."""
        result = _extract_keywords_from_query("/q name:user_profile*")
        assert result == ["user_profile"]

    def test_empty_tokens_filtered(self):
        """Test that empty tokens are filtered out."""
        result = _extract_keywords_from_query("/q premium++plan")
        assert result == ["premium", "plan"]

    def test_no_q_prefix(self):
        """Test query without /q prefix (edge case)."""
        result = _extract_keywords_from_query("premium+plan")
        assert result == ["premium", "plan"]
