"""Tests for document search and grep tools."""

from unittest.mock import Mock

import pytest

from datahub_agent_context.context import DataHubContext
from datahub_agent_context.mcp_tools.documents import (
    _add_top_results,
    _annotate_search_type,
    _build_urn_lookup,
    _interleave_remaining_results,
    _merge_search_results,
    grep_documents,
    search_documents,
)


@pytest.fixture
def mock_graph():
    """Create a mock DataHubGraph."""
    mock = Mock()
    mock.execute_graphql = Mock()
    mock.frontend_base_url = "http://localhost:9002"
    return mock


@pytest.fixture
def mock_doc_search_response():
    """Sample document search response."""
    return {
        "searchAcrossEntities": {
            "start": 0,
            "count": 2,
            "total": 2,
            "searchResults": [
                {
                    "entity": {
                        "urn": "urn:li:document:doc1",
                        "subType": "Runbook",
                        "platform": {
                            "urn": "urn:li:dataPlatform:notion",
                            "name": "Notion",
                        },
                        "info": {
                            "title": "Deployment Guide",
                            "source": {
                                "sourceType": "EXTERNAL",
                                "externalUrl": "https://notion.so/doc1",
                            },
                        },
                    }
                },
            ],
            "facets": [],
        }
    }


@pytest.fixture
def mock_doc_content_response():
    """Sample document content response for grep."""
    return {
        "entities": [
            {
                "urn": "urn:li:document:doc1",
                "info": {
                    "title": "Deployment Guide",
                    "contents": {
                        "text": "This guide explains how to deploy applications to production. "
                        "First, ensure you have kubectl installed. Then run kubectl apply -f deployment.yaml. "
                        "After deployment, verify the pods are running with kubectl get pods."
                    },
                },
            },
            {
                "urn": "urn:li:document:doc2",
                "info": {
                    "title": "Troubleshooting Guide",
                    "contents": {
                        "text": "Common errors and solutions. Error: Connection refused - check network. "
                        "Error: Timeout - increase timeout value. Warning: Deprecated API - update client."
                    },
                },
            },
        ]
    }


# Tests for search_documents


def test_search_documents_basic(mock_graph, mock_doc_search_response):
    """Test basic document search."""
    mock_graph.execute_graphql.return_value = mock_doc_search_response

    with DataHubContext(mock_graph):
        result = search_documents(query="deployment")
    assert "total" in result
    assert "searchResults" in result
    assert len(result["searchResults"]) == 1


def test_search_documents_with_platforms(mock_graph, mock_doc_search_response):
    """Test filtering by platforms."""
    mock_graph.execute_graphql.return_value = mock_doc_search_response

    with DataHubContext(mock_graph):
        result = search_documents(query="*", platforms=["urn:li:dataPlatform:notion"])

    assert result is not None
    call_args = mock_graph.execute_graphql.call_args
    assert call_args.kwargs["operation_name"] == "documentSearch"


def test_search_documents_with_domains(mock_graph, mock_doc_search_response):
    """Test filtering by domains."""
    mock_graph.execute_graphql.return_value = mock_doc_search_response

    with DataHubContext(mock_graph):
        result = search_documents(query="*", domains=["urn:li:domain:engineering"])

    assert result is not None


def test_search_documents_with_tags(mock_graph, mock_doc_search_response):
    """Test filtering by tags."""
    mock_graph.execute_graphql.return_value = mock_doc_search_response

    with DataHubContext(mock_graph):
        result = search_documents(query="*", tags=["urn:li:tag:critical"])
    assert result is not None


def test_search_documents_with_glossary_terms(mock_graph, mock_doc_search_response):
    """Test filtering by glossary terms."""
    mock_graph.execute_graphql.return_value = mock_doc_search_response

    with DataHubContext(mock_graph):
        result = search_documents(query="*", glossary_terms=["urn:li:glossaryTerm:pii"])

    assert result is not None


def test_search_documents_with_owners(mock_graph, mock_doc_search_response):
    """Test filtering by owners."""
    mock_graph.execute_graphql.return_value = mock_doc_search_response

    with DataHubContext(mock_graph):
        result = search_documents(query="*", owners=["urn:li:corpuser:alice"])
    assert result is not None


def test_search_documents_with_multiple_filters(mock_graph, mock_doc_search_response):
    """Test multiple filters combined."""
    mock_graph.execute_graphql.return_value = mock_doc_search_response

    with DataHubContext(mock_graph):
        result = search_documents(
            query="*",
            platforms=["urn:li:dataPlatform:notion"],
            domains=["urn:li:domain:engineering"],
        )

    assert result is not None


def test_search_documents_pagination(mock_graph, mock_doc_search_response):
    """Test pagination parameters."""
    mock_graph.execute_graphql.return_value = mock_doc_search_response

    with DataHubContext(mock_graph):
        search_documents(query="*", num_results=20, offset=10)
    call_args = mock_graph.execute_graphql.call_args
    variables = call_args.kwargs["variables"]
    assert variables["count"] == 20
    assert variables["start"] == 10


def test_search_documents_num_results_capped_at_50(
    mock_graph, mock_doc_search_response
):
    """Test that num_results is capped at 50."""
    mock_graph.execute_graphql.return_value = mock_doc_search_response

    with DataHubContext(mock_graph):
        search_documents(query="*", num_results=100)
    call_args = mock_graph.execute_graphql.call_args
    variables = call_args.kwargs["variables"]
    assert variables["count"] == 50


def test_search_documents_facet_only(mock_graph):
    """Test facet-only query with num_results=0."""
    # Mock response with non-empty facets to verify they're preserved
    mock_graph.execute_graphql.return_value = {
        "searchAcrossEntities": {
            "start": 0,
            "count": 0,
            "total": 100,
            "searchResults": [],
            "facets": [
                {
                    "field": "platform",
                    "aggregations": [{"value": "notion", "count": 50}],
                }
            ],
        }
    }

    with DataHubContext(mock_graph):
        result = search_documents(query="*", num_results=0)
    # Verify searchResults is removed for facet-only queries
    assert "searchResults" not in result
    assert "count" not in result
    # Facets should be preserved when non-empty
    assert "facets" in result
    assert len(result["facets"]) == 1


def test_search_documents_no_content_in_response(mock_graph, mock_doc_search_response):
    """Test that response does not contain document content."""
    mock_graph.execute_graphql.return_value = mock_doc_search_response

    with DataHubContext(mock_graph):
        result = search_documents(query="*")
    # Verify no content field in results
    for search_result in result.get("searchResults", []):
        entity = search_result.get("entity", {})
        info = entity.get("info", {})
        assert "contents" not in info


# Tests for grep_documents


def test_grep_documents_basic(mock_graph, mock_doc_content_response):
    """Test basic pattern matching."""
    mock_graph.execute_graphql.return_value = mock_doc_content_response

    with DataHubContext(mock_graph):
        result = grep_documents(
            urns=["urn:li:document:doc1", "urn:li:document:doc2"],
            pattern="kubectl",
        )

    assert result["documents_with_matches"] == 1
    assert result["total_matches"] >= 2
    assert len(result["results"]) == 1
    assert result["results"][0]["urn"] == "urn:li:document:doc1"
    assert result["results"][0]["title"] == "Deployment Guide"
    assert len(result["results"][0]["matches"]) >= 2


def test_grep_documents_case_insensitive(mock_graph, mock_doc_content_response):
    """Test case insensitive matching using (?i) inline flag."""
    mock_graph.execute_graphql.return_value = mock_doc_content_response

    with DataHubContext(mock_graph):
        result = grep_documents(
            urns=["urn:li:document:doc1", "urn:li:document:doc2"],
            pattern="(?i)error",
        )

    # Should match "Error:" in doc2
    assert result["documents_with_matches"] == 1
    assert result["total_matches"] >= 2


def test_grep_documents_regex_pattern(mock_graph, mock_doc_content_response):
    """Test regex pattern matching."""
    mock_graph.execute_graphql.return_value = mock_doc_content_response

    with DataHubContext(mock_graph):
        result = grep_documents(
            urns=["urn:li:document:doc1", "urn:li:document:doc2"],
            pattern="Error|Warning",
        )

    # Should match "Error:" and "Warning:" in doc2
    assert result["documents_with_matches"] == 1
    assert result["total_matches"] == 3  # 2 Error + 1 Warning


def test_grep_documents_max_matches_per_doc(mock_graph):
    """Test that max_matches_per_doc limits excerpts returned."""
    mock_graph.execute_graphql.return_value = {
        "entities": [
            {
                "urn": "urn:li:document:doc1",
                "info": {
                    "title": "Test Doc",
                    "contents": {
                        "text": "word word word word word word word word word word"
                    },
                },
            }
        ]
    }

    with DataHubContext(mock_graph):
        result = grep_documents(
            urns=["urn:li:document:doc1"],
            pattern="word",
            max_matches_per_doc=3,
        )

    # Should have 10 total matches but only 3 excerpts
    assert result["results"][0]["total_matches"] == 10
    assert len(result["results"][0]["matches"]) == 3


def test_grep_documents_context_chars(mock_graph):
    """Test that context_chars controls excerpt size."""
    text = "A" * 100 + "MATCH" + "B" * 100
    mock_graph.execute_graphql.return_value = {
        "entities": [
            {
                "urn": "urn:li:document:doc1",
                "info": {
                    "title": "Test Doc",
                    "contents": {"text": text},
                },
            }
        ]
    }

    with DataHubContext(mock_graph):
        result = grep_documents(
            urns=["urn:li:document:doc1"],
            pattern="MATCH",
            context_chars=50,
        )

    excerpt = result["results"][0]["matches"][0]["excerpt"]
    # Should have ellipsis on both ends and be roughly 50+5+50 chars
    assert excerpt.startswith("...")
    assert excerpt.endswith("...")
    assert "MATCH" in excerpt


def test_grep_documents_empty_urns(mock_graph):
    """Test with empty URN list."""
    with DataHubContext(mock_graph):
        result = grep_documents(urns=[], pattern="test")
    assert result["results"] == []
    assert result["total_matches"] == 0
    assert result["documents_with_matches"] == 0
    # GraphQL should not be called
    mock_graph.execute_graphql.assert_not_called()


def test_grep_documents_invalid_regex(mock_graph, mock_doc_content_response):
    """Test handling of invalid regex pattern."""
    mock_graph.execute_graphql.return_value = mock_doc_content_response

    with DataHubContext(mock_graph):
        result = grep_documents(
            urns=["urn:li:document:doc1"],
            pattern="[invalid",  # Unclosed bracket
        )

    assert "error" in result
    assert "Invalid regex pattern" in result["error"]
    assert result["results"] == []


def test_grep_documents_no_matches(mock_graph, mock_doc_content_response):
    """Test when pattern doesn't match any content."""
    mock_graph.execute_graphql.return_value = mock_doc_content_response

    with DataHubContext(mock_graph):
        result = grep_documents(
            urns=["urn:li:document:doc1", "urn:li:document:doc2"],
            pattern="nonexistent_pattern_xyz",
        )

    assert result["results"] == []
    assert result["total_matches"] == 0
    assert result["documents_with_matches"] == 0


def test_grep_documents_without_content(mock_graph):
    """Test handling of document with missing content."""
    mock_graph.execute_graphql.return_value = {
        "entities": [
            {
                "urn": "urn:li:document:doc1",
                "info": {
                    "title": "Empty Doc",
                    "contents": None,
                },
            },
            {
                "urn": "urn:li:document:doc2",
                "info": {
                    "title": "Doc with Content",
                    "contents": {"text": "Some text with pattern here"},
                },
            },
        ]
    }

    with DataHubContext(mock_graph):
        result = grep_documents(
            urns=["urn:li:document:doc1", "urn:li:document:doc2"],
            pattern="pattern",
        )

    # Should only find match in doc2
    assert result["documents_with_matches"] == 1
    assert result["results"][0]["urn"] == "urn:li:document:doc2"


def test_grep_documents_match_position(mock_graph):
    """Test that match position is correctly reported."""
    mock_graph.execute_graphql.return_value = {
        "entities": [
            {
                "urn": "urn:li:document:doc1",
                "info": {
                    "title": "Test Doc",
                    "contents": {"text": "prefix MATCH suffix"},
                },
            }
        ]
    }

    with DataHubContext(mock_graph):
        result = grep_documents(
            urns=["urn:li:document:doc1"],
            pattern="MATCH",
        )

    # Position should be 7 (after "prefix ")
    assert result["results"][0]["matches"][0]["position"] == 7


def test_grep_documents_graphql_called_correctly(mock_graph, mock_doc_content_response):
    """Test that GraphQL is called with correct parameters."""
    mock_graph.execute_graphql.return_value = mock_doc_content_response

    with DataHubContext(mock_graph):
        grep_documents(
            urns=["urn:li:document:doc1", "urn:li:document:doc2"],
            pattern="test",
        )

    call_args = mock_graph.execute_graphql.call_args
    assert call_args.kwargs["operation_name"] == "documentContent"
    assert call_args.kwargs["variables"]["urns"] == [
        "urn:li:document:doc1",
        "urn:li:document:doc2",
    ]


def test_grep_documents_start_offset_skips_beginning(mock_graph):
    """Test that start_offset skips characters at the beginning."""
    # Document has MATCH at position 10 and at position 50
    text = "0123456789MATCH" + "A" * 35 + "MATCH" + "B" * 100
    mock_graph.execute_graphql.return_value = {
        "entities": [
            {
                "urn": "urn:li:document:doc1",
                "info": {
                    "title": "Test Doc",
                    "contents": {"text": text},
                },
            }
        ]
    }

    # With start_offset=20, should skip the first MATCH at position 10
    with DataHubContext(mock_graph):
        result = grep_documents(
            urns=["urn:li:document:doc1"],
            pattern="MATCH",
            start_offset=20,
        )

    # Should only find the second MATCH (at position 50)
    assert result["total_matches"] == 1
    assert len(result["results"]) == 1


def test_grep_documents_start_offset_reports_absolute_position(mock_graph):
    """Test that positions are absolute (not relative to offset)."""
    # MATCH is at position 50 in the original text
    text = "A" * 50 + "MATCH" + "B" * 50
    mock_graph.execute_graphql.return_value = {
        "entities": [
            {
                "urn": "urn:li:document:doc1",
                "info": {
                    "title": "Test Doc",
                    "contents": {"text": text},
                },
            }
        ]
    }

    with DataHubContext(mock_graph):
        result = grep_documents(
            urns=["urn:li:document:doc1"],
            pattern="MATCH",
            start_offset=30,
        )

    # Position should be 50 (absolute), not 20 (relative to offset)
    assert result["results"][0]["matches"][0]["position"] == 50


def test_grep_documents_start_offset_includes_content_length(mock_graph):
    """Test that content_length is included when using start_offset."""
    text = "A" * 50 + "MATCH" + "B" * 50
    mock_graph.execute_graphql.return_value = {
        "entities": [
            {
                "urn": "urn:li:document:doc1",
                "info": {
                    "title": "Test Doc",
                    "contents": {"text": text},
                },
            }
        ]
    }

    with DataHubContext(mock_graph):
        result = grep_documents(
            urns=["urn:li:document:doc1"],
            pattern="MATCH",
            start_offset=30,
        )

    # Should include content_length for pagination awareness
    assert result["results"][0]["content_length"] == len(text)


def test_grep_documents_start_offset_zero_no_content_length(mock_graph):
    """Test that content_length is NOT included when start_offset=0."""
    mock_graph.execute_graphql.return_value = {
        "entities": [
            {
                "urn": "urn:li:document:doc1",
                "info": {
                    "title": "Test Doc",
                    "contents": {"text": "Some MATCH text"},
                },
            }
        ]
    }

    with DataHubContext(mock_graph):
        result = grep_documents(
            urns=["urn:li:document:doc1"],
            pattern="MATCH",
            start_offset=0,
        )

    # Should NOT include content_length when not using offset
    assert "content_length" not in result["results"][0]


def test_grep_documents_start_offset_beyond_document_length(mock_graph):
    """Test that offset beyond document length skips the document."""
    mock_graph.execute_graphql.return_value = {
        "entities": [
            {
                "urn": "urn:li:document:doc1",
                "info": {
                    "title": "Short Doc",
                    "contents": {"text": "Short text MATCH"},  # 16 chars
                },
            },
            {
                "urn": "urn:li:document:doc2",
                "info": {
                    "title": "Longer Doc",
                    "contents": {"text": "A" * 100 + "MATCH" + "B" * 100},  # 205 chars
                },
            },
        ]
    }

    with DataHubContext(mock_graph):
        result = grep_documents(
            urns=["urn:li:document:doc1", "urn:li:document:doc2"],
            pattern="MATCH",
            start_offset=50,  # Beyond doc1 length, but within doc2
        )

    # Should only find match in doc2 (doc1 is skipped)
    assert result["documents_with_matches"] == 1
    assert result["results"][0]["urn"] == "urn:li:document:doc2"


# Tests for helper functions


def test_annotate_search_type_adds_type_to_results():
    """Test that _annotate_search_type adds searchType to all results."""
    results = {
        "searchResults": [
            {"entity": {"urn": "urn:li:document:doc1"}},
            {"entity": {"urn": "urn:li:document:doc2"}},
        ]
    }

    annotated = _annotate_search_type(results, "keyword")

    assert annotated["searchResults"][0]["searchType"] == "keyword"
    assert annotated["searchResults"][1]["searchType"] == "keyword"


def test_annotate_search_type_handles_empty_results():
    """Test that _annotate_search_type handles empty searchResults."""
    results = {"searchResults": []}

    annotated = _annotate_search_type(results, "semantic")

    assert annotated["searchResults"] == []


def test_annotate_search_type_returns_same_dict():
    """Test that _annotate_search_type returns the same dict object."""
    results = {"searchResults": [{"entity": {"urn": "urn:li:document:doc1"}}]}

    annotated = _annotate_search_type(results, "both")

    # Should be the same object (mutated in place)
    assert annotated is results


def test_build_urn_lookup_creates_correct_mapping():
    """Test that _build_urn_lookup builds correct URN to result mapping."""
    results = [
        {"entity": {"urn": "urn:li:document:doc1"}, "score": 10},
        {"entity": {"urn": "urn:li:document:doc2"}, "score": 8},
    ]

    urn_map = _build_urn_lookup(results)

    assert len(urn_map) == 2
    assert "urn:li:document:doc1" in urn_map
    assert "urn:li:document:doc2" in urn_map
    assert urn_map["urn:li:document:doc1"]["score"] == 10
    assert urn_map["urn:li:document:doc2"]["score"] == 8


def test_build_urn_lookup_handles_missing_urns():
    """Test that _build_urn_lookup skips results without URNs."""
    results = [
        {"entity": {"urn": "urn:li:document:doc1"}},
        {"entity": {}},  # No URN
        {"other": "data"},  # No entity
    ]

    urn_map = _build_urn_lookup(results)

    assert len(urn_map) == 1
    assert "urn:li:document:doc1" in urn_map


def test_build_urn_lookup_empty_list():
    """Test that _build_urn_lookup handles empty list."""
    urn_map = _build_urn_lookup([])

    assert urn_map == {}


def test_add_top_results_both_present():
    """Test _add_top_results with both keyword and semantic results."""
    merged_results = []
    seen_urns = set()
    keyword_results = [
        {"entity": {"urn": "urn:li:document:k1"}},
        {"entity": {"urn": "urn:li:document:k2"}},
    ]
    semantic_results = [
        {"entity": {"urn": "urn:li:document:s1"}},
        {"entity": {"urn": "urn:li:document:s2"}},
    ]
    both_urns = set()

    _add_top_results(
        merged_results, seen_urns, keyword_results, semantic_results, both_urns
    )

    # Should have 2 results: top keyword and top semantic
    assert len(merged_results) == 2
    assert merged_results[0]["entity"]["urn"] == "urn:li:document:k1"
    assert merged_results[0]["searchType"] == "keyword"
    assert merged_results[1]["entity"]["urn"] == "urn:li:document:s1"
    assert merged_results[1]["searchType"] == "semantic"
    assert len(seen_urns) == 2


def test_add_top_results_duplicate_top_result():
    """Test _add_top_results when top result appears in both searches."""
    merged_results = []
    seen_urns = set()
    keyword_results = [{"entity": {"urn": "urn:li:document:same"}}]
    semantic_results = [{"entity": {"urn": "urn:li:document:same"}}]
    both_urns = {"urn:li:document:same"}

    _add_top_results(
        merged_results, seen_urns, keyword_results, semantic_results, both_urns
    )

    # Should have only 1 result (deduplicated)
    assert len(merged_results) == 1
    assert merged_results[0]["entity"]["urn"] == "urn:li:document:same"
    assert merged_results[0]["searchType"] == "both"


def test_add_top_results_empty_keyword():
    """Test _add_top_results with empty keyword results."""
    merged_results = []
    seen_urns = set()
    keyword_results = []
    semantic_results = [{"entity": {"urn": "urn:li:document:s1"}}]
    both_urns = set()

    _add_top_results(
        merged_results, seen_urns, keyword_results, semantic_results, both_urns
    )

    # Should have only semantic result
    assert len(merged_results) == 1
    assert merged_results[0]["entity"]["urn"] == "urn:li:document:s1"


def test_add_top_results_empty_semantic():
    """Test _add_top_results with empty semantic results."""
    merged_results = []
    seen_urns = set()
    keyword_results = [{"entity": {"urn": "urn:li:document:k1"}}]
    semantic_results = []
    both_urns = set()

    _add_top_results(
        merged_results, seen_urns, keyword_results, semantic_results, both_urns
    )

    # Should have only keyword result
    assert len(merged_results) == 1
    assert merged_results[0]["entity"]["urn"] == "urn:li:document:k1"


def test_interleave_remaining_results_alternates():
    """Test _interleave_remaining_results alternates between sources."""
    merged_results = []
    seen_urns = set()
    keyword_remaining = [
        {"entity": {"urn": "urn:li:document:k1"}},
        {"entity": {"urn": "urn:li:document:k2"}},
    ]
    semantic_remaining = [
        {"entity": {"urn": "urn:li:document:s1"}},
        {"entity": {"urn": "urn:li:document:s2"}},
    ]
    both_urns = set()

    _interleave_remaining_results(
        merged_results, seen_urns, keyword_remaining, semantic_remaining, both_urns
    )

    # Should alternate: k1, s1, k2, s2
    assert len(merged_results) == 4
    assert merged_results[0]["entity"]["urn"] == "urn:li:document:k1"
    assert merged_results[1]["entity"]["urn"] == "urn:li:document:s1"
    assert merged_results[2]["entity"]["urn"] == "urn:li:document:k2"
    assert merged_results[3]["entity"]["urn"] == "urn:li:document:s2"


def test_interleave_remaining_results_unequal_lengths():
    """Test _interleave_remaining_results with unequal list lengths."""
    merged_results = []
    seen_urns = set()
    keyword_remaining = [{"entity": {"urn": "urn:li:document:k1"}}]
    semantic_remaining = [
        {"entity": {"urn": "urn:li:document:s1"}},
        {"entity": {"urn": "urn:li:document:s2"}},
        {"entity": {"urn": "urn:li:document:s3"}},
    ]
    both_urns = set()

    _interleave_remaining_results(
        merged_results, seen_urns, keyword_remaining, semantic_remaining, both_urns
    )

    # Should handle unequal lengths: k1, s1, s2, s3
    assert len(merged_results) == 4


def test_interleave_remaining_results_deduplicates():
    """Test _interleave_remaining_results deduplicates already-seen URNs."""
    merged_results = []
    seen_urns = {"urn:li:document:k1"}  # k1 already seen
    keyword_remaining = [
        {"entity": {"urn": "urn:li:document:k1"}},  # Should be skipped
        {"entity": {"urn": "urn:li:document:k2"}},
    ]
    semantic_remaining = [{"entity": {"urn": "urn:li:document:s1"}}]
    both_urns = set()

    _interleave_remaining_results(
        merged_results, seen_urns, keyword_remaining, semantic_remaining, both_urns
    )

    # k1 should be skipped, but alternation continues, so result is: s1, k2
    assert len(merged_results) == 2
    assert merged_results[0]["entity"]["urn"] == "urn:li:document:s1"
    assert merged_results[1]["entity"]["urn"] == "urn:li:document:k2"


def test_interleave_remaining_results_marks_both():
    """Test _interleave_remaining_results marks results in both_urns as 'both'."""
    merged_results = []
    seen_urns = set()
    keyword_remaining = [{"entity": {"urn": "urn:li:document:k1"}}]
    semantic_remaining = [{"entity": {"urn": "urn:li:document:s1"}}]
    both_urns = {"urn:li:document:k1"}  # k1 appears in both

    _interleave_remaining_results(
        merged_results, seen_urns, keyword_remaining, semantic_remaining, both_urns
    )

    assert merged_results[0]["searchType"] == "both"
    assert merged_results[1]["searchType"] == "semantic"


def test_merge_search_results_both_empty():
    """Test _merge_search_results with both results empty."""
    result = _merge_search_results(None, None)

    assert result == {"searchResults": [], "total": 0, "count": 0}


def test_merge_search_results_only_keyword():
    """Test _merge_search_results with only keyword results."""
    keyword_results = {
        "searchResults": [{"entity": {"urn": "urn:li:document:k1"}}],
        "total": 1,
        "count": 1,
    }

    result = _merge_search_results(keyword_results, None)

    assert len(result["searchResults"]) == 1
    assert result["searchResults"][0]["searchType"] == "keyword"


def test_merge_search_results_only_semantic():
    """Test _merge_search_results with only semantic results."""
    semantic_results = {
        "searchResults": [{"entity": {"urn": "urn:li:document:s1"}}],
        "total": 1,
        "count": 1,
    }

    result = _merge_search_results(None, semantic_results)

    assert len(result["searchResults"]) == 1
    assert result["searchResults"][0]["searchType"] == "semantic"


def test_merge_search_results_empty_semantic_with_keyword_logs_warning(caplog):
    """Test _merge_search_results logs warning for empty semantic with non-empty keyword."""
    keyword_results = {
        "searchResults": [{"entity": {"urn": "urn:li:document:k1"}}],
        "total": 1,
        "count": 1,
    }
    semantic_results = {"searchResults": [], "total": 0, "count": 0}

    result = _merge_search_results(keyword_results, semantic_results)

    # Should return keyword results with warning logged
    assert len(result["searchResults"]) == 1
    assert result["searchResults"][0]["searchType"] == "keyword"
    assert "Semantic search returned 0 results" in caplog.text


def test_merge_search_results_deduplicates_by_urn():
    """Test _merge_search_results deduplicates results by URN."""
    keyword_results = {
        "searchResults": [
            {"entity": {"urn": "urn:li:document:both"}},
            {"entity": {"urn": "urn:li:document:k1"}},
        ]
    }
    semantic_results = {
        "searchResults": [
            {"entity": {"urn": "urn:li:document:both"}},
            {"entity": {"urn": "urn:li:document:s1"}},
        ]
    }

    result = _merge_search_results(keyword_results, semantic_results)

    # Should have 3 unique results: both (deduplicated), k1, s1
    assert len(result["searchResults"]) == 3
    urns = [r["entity"]["urn"] for r in result["searchResults"]]
    assert urns.count("urn:li:document:both") == 1  # Not duplicated


def test_merge_search_results_marks_duplicates_as_both():
    """Test _merge_search_results marks duplicates with searchType='both'."""
    keyword_results = {"searchResults": [{"entity": {"urn": "urn:li:document:both"}}]}
    semantic_results = {"searchResults": [{"entity": {"urn": "urn:li:document:both"}}]}

    result = _merge_search_results(keyword_results, semantic_results)

    assert result["searchResults"][0]["searchType"] == "both"


def test_merge_search_results_keyword_first():
    """Test _merge_search_results places keyword result first."""
    keyword_results = {
        "searchResults": [
            {"entity": {"urn": "urn:li:document:k1"}},
            {"entity": {"urn": "urn:li:document:k2"}},
        ]
    }
    semantic_results = {
        "searchResults": [
            {"entity": {"urn": "urn:li:document:s1"}},
            {"entity": {"urn": "urn:li:document:s2"}},
        ]
    }

    result = _merge_search_results(keyword_results, semantic_results)

    # First result should be top keyword
    assert result["searchResults"][0]["entity"]["urn"] == "urn:li:document:k1"
    # Second should be top semantic
    assert result["searchResults"][1]["entity"]["urn"] == "urn:li:document:s1"


def test_merge_search_results_interleaves_remaining():
    """Test _merge_search_results interleaves remaining results."""
    keyword_results = {
        "searchResults": [
            {"entity": {"urn": "urn:li:document:k1"}},
            {"entity": {"urn": "urn:li:document:k2"}},
            {"entity": {"urn": "urn:li:document:k3"}},
        ]
    }
    semantic_results = {
        "searchResults": [
            {"entity": {"urn": "urn:li:document:s1"}},
            {"entity": {"urn": "urn:li:document:s2"}},
            {"entity": {"urn": "urn:li:document:s3"}},
        ]
    }

    result = _merge_search_results(keyword_results, semantic_results)

    # Should be: k1, s1 (tops), k2, s2, k3, s3 (interleaved)
    urns = [r["entity"]["urn"] for r in result["searchResults"]]
    assert urns == [
        "urn:li:document:k1",
        "urn:li:document:s1",
        "urn:li:document:k2",
        "urn:li:document:s2",
        "urn:li:document:k3",
        "urn:li:document:s3",
    ]


def test_merge_search_results_preserves_facets():
    """Test _merge_search_results preserves facets from keyword search."""
    keyword_results = {
        "searchResults": [{"entity": {"urn": "urn:li:document:k1"}}],
        "facets": [{"field": "platform", "aggregations": []}],
    }
    semantic_results = {
        "searchResults": [{"entity": {"urn": "urn:li:document:s1"}}],
    }

    result = _merge_search_results(keyword_results, semantic_results)

    assert "facets" in result
    assert result["facets"] == keyword_results["facets"]


def test_merge_search_results_preserves_start_offset():
    """Test _merge_search_results preserves start/offset from keyword search."""
    keyword_results = {
        "searchResults": [{"entity": {"urn": "urn:li:document:k1"}}],
        "start": 10,
    }
    semantic_results = {
        "searchResults": [{"entity": {"urn": "urn:li:document:s1"}}],
    }

    result = _merge_search_results(keyword_results, semantic_results)

    assert result["start"] == 10


def test_merge_search_results_updates_total_and_count():
    """Test _merge_search_results updates total and count correctly."""
    keyword_results = {
        "searchResults": [
            {"entity": {"urn": "urn:li:document:k1"}},
            {"entity": {"urn": "urn:li:document:k2"}},
        ]
    }
    semantic_results = {"searchResults": [{"entity": {"urn": "urn:li:document:s1"}}]}

    result = _merge_search_results(keyword_results, semantic_results)

    # Should have correct count based on merged results
    assert result["total"] == 3
    assert result["count"] == 3
