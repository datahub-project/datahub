"""Unit tests for grep_documents MCP tool."""

from unittest.mock import MagicMock, patch

import pytest

from datahub_integrations.mcp.mcp_server import async_background, grep_documents

pytestmark = pytest.mark.anyio


class TestGrepDocuments:
    """Tests for grep_documents tool."""

    @pytest.fixture
    def mock_client(self):
        """Mock DataHub client."""
        client = MagicMock()
        client._graph = MagicMock()
        return client

    @pytest.fixture
    def mock_gql_response(self):
        """Sample GraphQL response with document content."""
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

    @patch("datahub_integrations.mcp.mcp_server.get_datahub_client")
    @patch("datahub_integrations.mcp.mcp_server.execute_graphql")
    async def test_basic_pattern_match(
        self,
        mock_execute_graphql,
        mock_get_client,
        mock_client,
        mock_gql_response,
    ):
        """Test basic pattern matching."""
        mock_get_client.return_value = mock_client
        mock_execute_graphql.return_value = mock_gql_response

        result = await async_background(grep_documents)(
            urns=["urn:li:document:doc1", "urn:li:document:doc2"],
            pattern="kubectl",
        )

        assert result["documents_with_matches"] == 1
        assert result["total_matches"] >= 2  # Multiple kubectl occurrences
        assert len(result["results"]) == 1
        assert result["results"][0]["urn"] == "urn:li:document:doc1"
        assert result["results"][0]["title"] == "Deployment Guide"
        assert len(result["results"][0]["matches"]) >= 2

    @patch("datahub_integrations.mcp.mcp_server.get_datahub_client")
    @patch("datahub_integrations.mcp.mcp_server.execute_graphql")
    async def test_case_insensitive_search(
        self,
        mock_execute_graphql,
        mock_get_client,
        mock_client,
        mock_gql_response,
    ):
        """Test case insensitive matching using (?i) inline flag."""
        mock_get_client.return_value = mock_client
        mock_execute_graphql.return_value = mock_gql_response

        result = await async_background(grep_documents)(
            urns=["urn:li:document:doc1", "urn:li:document:doc2"],
            pattern="(?i)error",  # Use inline flag for case insensitivity
        )

        # Should match "Error:" in doc2
        assert result["documents_with_matches"] == 1
        assert result["total_matches"] >= 2  # Multiple "Error:" occurrences

    @patch("datahub_integrations.mcp.mcp_server.get_datahub_client")
    @patch("datahub_integrations.mcp.mcp_server.execute_graphql")
    async def test_regex_pattern(
        self,
        mock_execute_graphql,
        mock_get_client,
        mock_client,
        mock_gql_response,
    ):
        """Test regex pattern matching."""
        mock_get_client.return_value = mock_client
        mock_execute_graphql.return_value = mock_gql_response

        result = await async_background(grep_documents)(
            urns=["urn:li:document:doc1", "urn:li:document:doc2"],
            pattern="Error|Warning",
        )

        # Should match "Error:" and "Warning:" in doc2
        assert result["documents_with_matches"] == 1
        assert result["total_matches"] == 3  # 2 Error + 1 Warning

    @patch("datahub_integrations.mcp.mcp_server.get_datahub_client")
    @patch("datahub_integrations.mcp.mcp_server.execute_graphql")
    async def test_max_matches_per_doc(
        self,
        mock_execute_graphql,
        mock_get_client,
        mock_client,
    ):
        """Test that max_matches_per_doc limits excerpts returned."""
        mock_get_client.return_value = mock_client
        mock_execute_graphql.return_value = {
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

        result = await async_background(grep_documents)(
            urns=["urn:li:document:doc1"],
            pattern="word",
            max_matches_per_doc=3,
        )

        # Should have 10 total matches but only 3 excerpts
        assert result["results"][0]["total_matches"] == 10
        assert len(result["results"][0]["matches"]) == 3

    @patch("datahub_integrations.mcp.mcp_server.get_datahub_client")
    @patch("datahub_integrations.mcp.mcp_server.execute_graphql")
    async def test_context_chars(
        self,
        mock_execute_graphql,
        mock_get_client,
        mock_client,
    ):
        """Test that context_chars controls excerpt size."""
        mock_get_client.return_value = mock_client
        text = "A" * 100 + "MATCH" + "B" * 100
        mock_execute_graphql.return_value = {
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

        result = await async_background(grep_documents)(
            urns=["urn:li:document:doc1"],
            pattern="MATCH",
            context_chars=50,
        )

        excerpt = result["results"][0]["matches"][0]["excerpt"]
        # Should have ellipsis on both ends and be roughly 50+5+50 chars
        assert excerpt.startswith("...")
        assert excerpt.endswith("...")
        assert "MATCH" in excerpt

    @patch("datahub_integrations.mcp.mcp_server.get_datahub_client")
    @patch("datahub_integrations.mcp.mcp_server.execute_graphql")
    async def test_empty_urns(
        self,
        mock_execute_graphql,
        mock_get_client,
        mock_client,
    ):
        """Test with empty URN list."""
        mock_get_client.return_value = mock_client

        result = await async_background(grep_documents)(
            urns=[],
            pattern="test",
        )

        assert result["results"] == []
        assert result["total_matches"] == 0
        assert result["documents_with_matches"] == 0
        # GraphQL should not be called
        mock_execute_graphql.assert_not_called()

    @patch("datahub_integrations.mcp.mcp_server.get_datahub_client")
    @patch("datahub_integrations.mcp.mcp_server.execute_graphql")
    async def test_invalid_regex(
        self,
        mock_execute_graphql,
        mock_get_client,
        mock_client,
        mock_gql_response,
    ):
        """Test handling of invalid regex pattern."""
        mock_get_client.return_value = mock_client
        mock_execute_graphql.return_value = mock_gql_response

        result = await async_background(grep_documents)(
            urns=["urn:li:document:doc1"],
            pattern="[invalid",  # Unclosed bracket
        )

        assert "error" in result
        assert "Invalid regex pattern" in result["error"]
        assert result["results"] == []

    @patch("datahub_integrations.mcp.mcp_server.get_datahub_client")
    @patch("datahub_integrations.mcp.mcp_server.execute_graphql")
    async def test_no_matches(
        self,
        mock_execute_graphql,
        mock_get_client,
        mock_client,
        mock_gql_response,
    ):
        """Test when pattern doesn't match any content."""
        mock_get_client.return_value = mock_client
        mock_execute_graphql.return_value = mock_gql_response

        result = await async_background(grep_documents)(
            urns=["urn:li:document:doc1", "urn:li:document:doc2"],
            pattern="nonexistent_pattern_xyz",
        )

        assert result["results"] == []
        assert result["total_matches"] == 0
        assert result["documents_with_matches"] == 0

    @patch("datahub_integrations.mcp.mcp_server.get_datahub_client")
    @patch("datahub_integrations.mcp.mcp_server.execute_graphql")
    async def test_document_without_content(
        self,
        mock_execute_graphql,
        mock_get_client,
        mock_client,
    ):
        """Test handling of document with missing content."""
        mock_get_client.return_value = mock_client
        mock_execute_graphql.return_value = {
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

        result = await async_background(grep_documents)(
            urns=["urn:li:document:doc1", "urn:li:document:doc2"],
            pattern="pattern",
        )

        # Should only find match in doc2
        assert result["documents_with_matches"] == 1
        assert result["results"][0]["urn"] == "urn:li:document:doc2"

    @patch("datahub_integrations.mcp.mcp_server.get_datahub_client")
    @patch("datahub_integrations.mcp.mcp_server.execute_graphql")
    async def test_match_position_reported(
        self,
        mock_execute_graphql,
        mock_get_client,
        mock_client,
    ):
        """Test that match position is correctly reported."""
        mock_get_client.return_value = mock_client
        mock_execute_graphql.return_value = {
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

        result = await async_background(grep_documents)(
            urns=["urn:li:document:doc1"],
            pattern="MATCH",
        )

        # Position should be 7 (after "prefix ")
        assert result["results"][0]["matches"][0]["position"] == 7

    @patch("datahub_integrations.mcp.mcp_server.get_datahub_client")
    @patch("datahub_integrations.mcp.mcp_server.execute_graphql")
    async def test_graphql_called_correctly(
        self,
        mock_execute_graphql,
        mock_get_client,
        mock_client,
        mock_gql_response,
    ):
        """Test that GraphQL is called with correct parameters."""
        mock_get_client.return_value = mock_client
        mock_execute_graphql.return_value = mock_gql_response

        await async_background(grep_documents)(
            urns=["urn:li:document:doc1", "urn:li:document:doc2"],
            pattern="test",
        )

        call_args = mock_execute_graphql.call_args
        assert call_args.kwargs["operation_name"] == "documentContent"
        assert call_args.kwargs["variables"]["urns"] == [
            "urn:li:document:doc1",
            "urn:li:document:doc2",
        ]

    @patch("datahub_integrations.mcp.mcp_server.get_datahub_client")
    @patch("datahub_integrations.mcp.mcp_server.execute_graphql")
    async def test_start_offset_skips_beginning(
        self,
        mock_execute_graphql,
        mock_get_client,
        mock_client,
    ):
        """Test that start_offset skips characters at the beginning."""
        mock_get_client.return_value = mock_client
        # Document has MATCH at position 10 and at position 50
        text = "0123456789MATCH" + "A" * 35 + "MATCH" + "B" * 100
        mock_execute_graphql.return_value = {
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
        result = await async_background(grep_documents)(
            urns=["urn:li:document:doc1"],
            pattern="MATCH",
            start_offset=20,
        )

        # Should only find the second MATCH (at position 50)
        assert result["total_matches"] == 1
        assert len(result["results"]) == 1

    @patch("datahub_integrations.mcp.mcp_server.get_datahub_client")
    @patch("datahub_integrations.mcp.mcp_server.execute_graphql")
    async def test_start_offset_reports_absolute_position(
        self,
        mock_execute_graphql,
        mock_get_client,
        mock_client,
    ):
        """Test that positions are absolute (not relative to offset)."""
        mock_get_client.return_value = mock_client
        # MATCH is at position 50 in the original text
        text = "A" * 50 + "MATCH" + "B" * 50
        mock_execute_graphql.return_value = {
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

        result = await async_background(grep_documents)(
            urns=["urn:li:document:doc1"],
            pattern="MATCH",
            start_offset=30,
        )

        # Position should be 50 (absolute), not 20 (relative to offset)
        assert result["results"][0]["matches"][0]["position"] == 50

    @patch("datahub_integrations.mcp.mcp_server.get_datahub_client")
    @patch("datahub_integrations.mcp.mcp_server.execute_graphql")
    async def test_start_offset_includes_content_length(
        self,
        mock_execute_graphql,
        mock_get_client,
        mock_client,
    ):
        """Test that content_length is included when using start_offset."""
        mock_get_client.return_value = mock_client
        text = "A" * 50 + "MATCH" + "B" * 50
        mock_execute_graphql.return_value = {
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

        result = await async_background(grep_documents)(
            urns=["urn:li:document:doc1"],
            pattern="MATCH",
            start_offset=30,
        )

        # Should include content_length for pagination awareness
        assert result["results"][0]["content_length"] == len(text)

    @patch("datahub_integrations.mcp.mcp_server.get_datahub_client")
    @patch("datahub_integrations.mcp.mcp_server.execute_graphql")
    async def test_start_offset_without_offset_no_content_length(
        self,
        mock_execute_graphql,
        mock_get_client,
        mock_client,
    ):
        """Test that content_length is NOT included when start_offset=0."""
        mock_get_client.return_value = mock_client
        mock_execute_graphql.return_value = {
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

        result = await async_background(grep_documents)(
            urns=["urn:li:document:doc1"],
            pattern="MATCH",
            start_offset=0,
        )

        # Should NOT include content_length when not using offset
        assert "content_length" not in result["results"][0]

    @patch("datahub_integrations.mcp.mcp_server.get_datahub_client")
    @patch("datahub_integrations.mcp.mcp_server.execute_graphql")
    async def test_start_offset_beyond_document_length(
        self,
        mock_execute_graphql,
        mock_get_client,
        mock_client,
    ):
        """Test that offset beyond document length skips the document."""
        mock_get_client.return_value = mock_client
        mock_execute_graphql.return_value = {
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
                        "contents": {
                            "text": "A" * 100 + "MATCH" + "B" * 100
                        },  # 205 chars
                    },
                },
            ]
        }

        result = await async_background(grep_documents)(
            urns=["urn:li:document:doc1", "urn:li:document:doc2"],
            pattern="MATCH",
            start_offset=50,  # Beyond doc1 length, but within doc2
        )

        # Should only find match in doc2 (doc1 is skipped)
        assert result["documents_with_matches"] == 1
        assert result["results"][0]["urn"] == "urn:li:document:doc2"

    @patch("datahub_integrations.mcp.mcp_server.get_datahub_client")
    @patch("datahub_integrations.mcp.mcp_server.execute_graphql")
    async def test_start_offset_raw_content_continuation(
        self,
        mock_execute_graphql,
        mock_get_client,
        mock_client,
    ):
        """Test using start_offset with pattern='.*' to continue reading."""
        mock_get_client.return_value = mock_client
        # Simulate a document that was truncated at position 100
        text = "A" * 100 + "B" * 100 + "C" * 100  # 300 chars total
        mock_execute_graphql.return_value = {
            "entities": [
                {
                    "urn": "urn:li:document:doc1",
                    "info": {
                        "title": "Large Doc",
                        "contents": {"text": text},
                    },
                }
            ]
        }

        # Continue reading from position 100 (where truncation occurred)
        result = await async_background(grep_documents)(
            urns=["urn:li:document:doc1"],
            pattern=".*",
            context_chars=150,  # Larger context to get more content
            start_offset=100,
        )

        # Should find a match and include content from position 100 onwards
        assert result["documents_with_matches"] == 1
        excerpt = result["results"][0]["matches"][0]["excerpt"]
        # Excerpt should contain Bs and Cs (content after position 100)
        assert "B" in excerpt
        # Should report full content length for pagination
        assert result["results"][0]["content_length"] == 300
