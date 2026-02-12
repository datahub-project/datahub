"""Unit tests for search_documents MCP tool."""

from unittest.mock import MagicMock, patch

import pytest

from datahub_integrations.mcp.mcp_server import async_background, search_documents
from datahub_integrations.mcp.tools.documents import (
    _merge_search_results,
    _search_documents_impl,
)

pytestmark = pytest.mark.anyio


class TestSearchDocuments:
    """Tests for search_documents tool."""

    @pytest.fixture
    def mock_client(self):
        """Mock DataHub client."""
        client = MagicMock()
        client._graph = MagicMock()
        return client

    @pytest.fixture
    def mock_gql_response(self):
        """Sample GraphQL response for document search."""
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
                                "lastModified": {
                                    "time": 1234567890,
                                    "actor": {"urn": "urn:li:corpuser:alice"},
                                },
                                "created": {
                                    "time": 1234567800,
                                    "actor": {"urn": "urn:li:corpuser:bob"},
                                },
                            },
                            "domain": {
                                "domain": {
                                    "urn": "urn:li:domain:engineering",
                                    "properties": {"name": "Engineering"},
                                }
                            },
                            "tags": {"tags": []},
                            "glossaryTerms": {"terms": []},
                        }
                    },
                    {
                        "entity": {
                            "urn": "urn:li:document:doc2",
                            "subType": "FAQ",
                            "platform": {
                                "urn": "urn:li:dataPlatform:datahub",
                                "name": "DataHub",
                            },
                            "info": {
                                "title": "Common Questions",
                                "source": None,
                                "lastModified": {
                                    "time": 1234567891,
                                    "actor": {"urn": "urn:li:corpuser:charlie"},
                                },
                                "created": {
                                    "time": 1234567801,
                                    "actor": {"urn": "urn:li:corpuser:charlie"},
                                },
                            },
                            "domain": None,
                            "tags": {"tags": []},
                            "glossaryTerms": {"terms": []},
                        }
                    },
                ],
                "facets": [
                    {
                        "field": "subTypes",
                        "displayName": "Type",
                        "aggregations": [
                            {"value": "Runbook", "count": 10, "displayName": "Runbook"},
                            {"value": "FAQ", "count": 5, "displayName": "FAQ"},
                        ],
                    },
                    {
                        "field": "platform",
                        "displayName": "Platform",
                        "aggregations": [
                            {
                                "value": "urn:li:dataPlatform:notion",
                                "count": 8,
                                "displayName": "Notion",
                            },
                        ],
                    },
                ],
            }
        }

    @pytest.fixture
    def mock_semantic_gql_response(self):
        """Sample GraphQL response for semantic document search."""
        return {
            "semanticSearchAcrossEntities": {
                "count": 1,
                "total": 1,
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
                                "source": None,
                                "lastModified": {
                                    "time": 1234567890,
                                    "actor": {"urn": "urn:li:corpuser:alice"},
                                },
                                "created": {
                                    "time": 1234567800,
                                    "actor": {"urn": "urn:li:corpuser:bob"},
                                },
                            },
                            "domain": None,
                            "tags": {"tags": []},
                            "glossaryTerms": {"terms": []},
                        }
                    }
                ],
                "facets": [],
            }
        }

    @patch("datahub_integrations.mcp.mcp_server.fetch_global_default_view")
    @patch("datahub_integrations.mcp.mcp_server.get_datahub_client")
    @patch("datahub_integrations.mcp.mcp_server.execute_graphql")
    async def test_basic_keyword_search(
        self,
        mock_execute_graphql,
        mock_get_client,
        mock_fetch_view,
        mock_client,
        mock_gql_response,
    ):
        """Test basic keyword search without filters."""
        mock_get_client.return_value = mock_client
        mock_execute_graphql.return_value = mock_gql_response
        mock_fetch_view.return_value = None

        result = await async_background(search_documents)(query="deployment")

        # Verify GraphQL was called correctly
        call_args = mock_execute_graphql.call_args
        assert call_args.kwargs["operation_name"] == "documentSearch"
        variables = call_args.kwargs["variables"]
        assert variables["query"] == "deployment"
        assert variables["orFilters"] == []

        # Verify response structure
        assert "total" in result
        assert "searchResults" in result
        assert "facets" in result

    @patch("datahub_integrations.mcp.mcp_server.fetch_global_default_view")
    @patch("datahub_integrations.mcp.mcp_server.get_datahub_client")
    @patch("datahub_integrations.mcp.mcp_server.execute_graphql")
    async def test_semantic_search(
        self,
        mock_execute_graphql,
        mock_get_client,
        mock_fetch_view,
        mock_client,
        mock_semantic_gql_response,
    ):
        """Test semantic search strategy via internal implementation."""
        mock_get_client.return_value = mock_client
        mock_execute_graphql.return_value = mock_semantic_gql_response
        mock_fetch_view.return_value = None

        # Note: search_strategy is only available in the internal _search_documents_impl
        # The public search_documents() function only does keyword search
        result = await async_background(_search_documents_impl)(
            query="how to deploy to production", search_strategy="semantic"
        )

        # Verify semantic search GraphQL was called
        call_args = mock_execute_graphql.call_args
        assert call_args.kwargs["operation_name"] == "documentSemanticSearch"

        # Verify response
        assert "total" in result
        assert result["total"] == 1

    @patch("datahub_integrations.mcp.mcp_server.fetch_global_default_view")
    @patch("datahub_integrations.mcp.mcp_server.get_datahub_client")
    @patch("datahub_integrations.mcp.mcp_server.execute_graphql")
    async def test_filter_by_sub_types(
        self,
        mock_execute_graphql,
        mock_get_client,
        mock_fetch_view,
        mock_client,
        mock_gql_response,
    ):
        """Test filtering by document sub_types (via internal implementation)."""
        mock_get_client.return_value = mock_client
        mock_execute_graphql.return_value = mock_gql_response
        mock_fetch_view.return_value = None

        # Note: sub_types is only available in the internal _search_documents_impl
        # The public search_documents() function does not expose this parameter
        await async_background(_search_documents_impl)(sub_types=["Runbook", "FAQ"])

        call_args = mock_execute_graphql.call_args
        variables = call_args.kwargs["variables"]
        or_filters = variables["orFilters"]

        assert len(or_filters) == 1
        and_filters = or_filters[0]["and"]
        assert {"field": "subTypes", "values": ["Runbook", "FAQ"]} in and_filters

    @patch("datahub_integrations.mcp.mcp_server.fetch_global_default_view")
    @patch("datahub_integrations.mcp.mcp_server.get_datahub_client")
    @patch("datahub_integrations.mcp.mcp_server.execute_graphql")
    async def test_filter_by_platforms(
        self,
        mock_execute_graphql,
        mock_get_client,
        mock_fetch_view,
        mock_client,
        mock_gql_response,
    ):
        """Test filtering by platforms."""
        mock_get_client.return_value = mock_client
        mock_execute_graphql.return_value = mock_gql_response
        mock_fetch_view.return_value = None

        await async_background(search_documents)(
            platforms=["urn:li:dataPlatform:notion"]
        )

        call_args = mock_execute_graphql.call_args
        variables = call_args.kwargs["variables"]
        or_filters = variables["orFilters"]

        assert len(or_filters) == 1
        and_filters = or_filters[0]["and"]
        assert {
            "field": "platform",
            "values": ["urn:li:dataPlatform:notion"],
        } in and_filters

    @patch("datahub_integrations.mcp.mcp_server.fetch_global_default_view")
    @patch("datahub_integrations.mcp.mcp_server.get_datahub_client")
    @patch("datahub_integrations.mcp.mcp_server.execute_graphql")
    async def test_filter_by_domains(
        self,
        mock_execute_graphql,
        mock_get_client,
        mock_fetch_view,
        mock_client,
        mock_gql_response,
    ):
        """Test filtering by domains."""
        mock_get_client.return_value = mock_client
        mock_execute_graphql.return_value = mock_gql_response
        mock_fetch_view.return_value = None

        await async_background(search_documents)(domains=["urn:li:domain:engineering"])

        call_args = mock_execute_graphql.call_args
        variables = call_args.kwargs["variables"]
        or_filters = variables["orFilters"]

        assert len(or_filters) == 1
        and_filters = or_filters[0]["and"]
        assert {
            "field": "domains",
            "values": ["urn:li:domain:engineering"],
        } in and_filters

    @patch("datahub_integrations.mcp.mcp_server.fetch_global_default_view")
    @patch("datahub_integrations.mcp.mcp_server.get_datahub_client")
    @patch("datahub_integrations.mcp.mcp_server.execute_graphql")
    async def test_filter_by_tags(
        self,
        mock_execute_graphql,
        mock_get_client,
        mock_fetch_view,
        mock_client,
        mock_gql_response,
    ):
        """Test filtering by tags."""
        mock_get_client.return_value = mock_client
        mock_execute_graphql.return_value = mock_gql_response
        mock_fetch_view.return_value = None

        await async_background(search_documents)(tags=["urn:li:tag:critical"])

        call_args = mock_execute_graphql.call_args
        variables = call_args.kwargs["variables"]
        or_filters = variables["orFilters"]

        assert len(or_filters) == 1
        and_filters = or_filters[0]["and"]
        assert {"field": "tags", "values": ["urn:li:tag:critical"]} in and_filters

    @patch("datahub_integrations.mcp.mcp_server.fetch_global_default_view")
    @patch("datahub_integrations.mcp.mcp_server.get_datahub_client")
    @patch("datahub_integrations.mcp.mcp_server.execute_graphql")
    async def test_filter_by_glossary_terms(
        self,
        mock_execute_graphql,
        mock_get_client,
        mock_fetch_view,
        mock_client,
        mock_gql_response,
    ):
        """Test filtering by glossary terms."""
        mock_get_client.return_value = mock_client
        mock_execute_graphql.return_value = mock_gql_response
        mock_fetch_view.return_value = None

        await async_background(search_documents)(
            glossary_terms=["urn:li:glossaryTerm:pii"]
        )

        call_args = mock_execute_graphql.call_args
        variables = call_args.kwargs["variables"]
        or_filters = variables["orFilters"]

        assert len(or_filters) == 1
        and_filters = or_filters[0]["and"]
        assert {
            "field": "glossaryTerms",
            "values": ["urn:li:glossaryTerm:pii"],
        } in and_filters

    @patch("datahub_integrations.mcp.mcp_server.fetch_global_default_view")
    @patch("datahub_integrations.mcp.mcp_server.get_datahub_client")
    @patch("datahub_integrations.mcp.mcp_server.execute_graphql")
    async def test_filter_by_owners(
        self,
        mock_execute_graphql,
        mock_get_client,
        mock_fetch_view,
        mock_client,
        mock_gql_response,
    ):
        """Test filtering by owners."""
        mock_get_client.return_value = mock_client
        mock_execute_graphql.return_value = mock_gql_response
        mock_fetch_view.return_value = None

        await async_background(search_documents)(owners=["urn:li:corpuser:alice"])

        call_args = mock_execute_graphql.call_args
        variables = call_args.kwargs["variables"]
        or_filters = variables["orFilters"]

        assert len(or_filters) == 1
        and_filters = or_filters[0]["and"]
        assert {"field": "owners", "values": ["urn:li:corpuser:alice"]} in and_filters

    @patch("datahub_integrations.mcp.mcp_server.fetch_global_default_view")
    @patch("datahub_integrations.mcp.mcp_server.get_datahub_client")
    @patch("datahub_integrations.mcp.mcp_server.execute_graphql")
    async def test_multiple_filters_combined(
        self,
        mock_execute_graphql,
        mock_get_client,
        mock_fetch_view,
        mock_client,
        mock_gql_response,
    ):
        """Test that multiple filters are ANDed together."""
        mock_get_client.return_value = mock_client
        mock_execute_graphql.return_value = mock_gql_response
        mock_fetch_view.return_value = None

        await async_background(search_documents)(
            platforms=["urn:li:dataPlatform:notion"],
            domains=["urn:li:domain:engineering"],
        )

        call_args = mock_execute_graphql.call_args
        variables = call_args.kwargs["variables"]
        or_filters = variables["orFilters"]

        assert len(or_filters) == 1
        and_filters = or_filters[0]["and"]
        assert len(and_filters) == 2
        assert {
            "field": "platform",
            "values": ["urn:li:dataPlatform:notion"],
        } in and_filters
        assert {
            "field": "domains",
            "values": ["urn:li:domain:engineering"],
        } in and_filters

    @patch("datahub_integrations.mcp.mcp_server.fetch_global_default_view")
    @patch("datahub_integrations.mcp.mcp_server.get_datahub_client")
    @patch("datahub_integrations.mcp.mcp_server.execute_graphql")
    async def test_pagination(
        self,
        mock_execute_graphql,
        mock_get_client,
        mock_fetch_view,
        mock_client,
        mock_gql_response,
    ):
        """Test pagination parameters."""
        mock_get_client.return_value = mock_client
        mock_execute_graphql.return_value = mock_gql_response
        mock_fetch_view.return_value = None

        await async_background(search_documents)(num_results=20, offset=10)

        call_args = mock_execute_graphql.call_args
        variables = call_args.kwargs["variables"]
        assert variables["count"] == 20
        assert variables["start"] == 10

    @patch("datahub_integrations.mcp.mcp_server.fetch_global_default_view")
    @patch("datahub_integrations.mcp.mcp_server.get_datahub_client")
    @patch("datahub_integrations.mcp.mcp_server.execute_graphql")
    async def test_num_results_capped_at_50(
        self,
        mock_execute_graphql,
        mock_get_client,
        mock_fetch_view,
        mock_client,
        mock_gql_response,
    ):
        """Test that num_results is capped at 50."""
        mock_get_client.return_value = mock_client
        mock_execute_graphql.return_value = mock_gql_response
        mock_fetch_view.return_value = None

        await async_background(search_documents)(num_results=100)

        call_args = mock_execute_graphql.call_args
        variables = call_args.kwargs["variables"]
        assert variables["count"] == 50

    @patch("datahub_integrations.mcp.mcp_server.fetch_global_default_view")
    @patch("datahub_integrations.mcp.mcp_server.get_datahub_client")
    @patch("datahub_integrations.mcp.mcp_server.execute_graphql")
    async def test_facet_only_query(
        self,
        mock_execute_graphql,
        mock_get_client,
        mock_fetch_view,
        mock_client,
        mock_gql_response,
    ):
        """Test facet-only query with num_results=0."""
        mock_get_client.return_value = mock_client
        mock_execute_graphql.return_value = mock_gql_response
        mock_fetch_view.return_value = None

        result = await async_background(search_documents)(num_results=0)

        # Verify searchResults is removed for facet-only queries
        assert "searchResults" not in result
        assert "facets" in result

    @patch("datahub_integrations.mcp.mcp_server.fetch_global_default_view")
    @patch("datahub_integrations.mcp.mcp_server.get_datahub_client")
    @patch("datahub_integrations.mcp.mcp_server.execute_graphql")
    async def test_response_does_not_contain_content(
        self,
        mock_execute_graphql,
        mock_get_client,
        mock_fetch_view,
        mock_client,
        mock_gql_response,
    ):
        """Test that response does not contain document content."""
        mock_get_client.return_value = mock_client
        mock_execute_graphql.return_value = mock_gql_response
        mock_fetch_view.return_value = None

        result = await async_background(search_documents)(query="*")

        # Verify no content field in results
        for search_result in result.get("searchResults", []):
            entity = search_result.get("entity", {})
            info = entity.get("info", {})
            assert "contents" not in info

    @patch("datahub_integrations.mcp.mcp_server.fetch_global_default_view")
    @patch("datahub_integrations.mcp.mcp_server.get_datahub_client")
    @patch("datahub_integrations.mcp.mcp_server.execute_graphql")
    async def test_default_view_applied(
        self,
        mock_execute_graphql,
        mock_get_client,
        mock_fetch_view,
        mock_client,
        mock_gql_response,
    ):
        """Test that default view is applied when set."""
        mock_get_client.return_value = mock_client
        mock_execute_graphql.return_value = mock_gql_response
        mock_fetch_view.return_value = "urn:li:dataHubView:default"

        await async_background(search_documents)(query="*")

        call_args = mock_execute_graphql.call_args
        variables = call_args.kwargs["variables"]
        assert variables["viewUrn"] == "urn:li:dataHubView:default"


class TestMergeSearchResults:
    """Tests for _merge_search_results function."""

    def test_merge_both_empty(self):
        """Test merging when both results are None."""
        result = _merge_search_results(None, None)
        assert result["searchResults"] == []
        assert result["total"] == 0

    def test_merge_keyword_only(self):
        """Test merging when only keyword results exist."""
        keyword_results = {
            "searchResults": [
                {"entity": {"urn": "urn:li:document:doc1"}, "score": 0.9}
            ],
            "total": 1,
            "count": 1,
            "facets": [{"field": "platform"}],
        }
        result = _merge_search_results(keyword_results, None)

        assert len(result["searchResults"]) == 1
        assert result["searchResults"][0]["searchType"] == "keyword"
        assert "facets" in result

    def test_merge_semantic_only(self):
        """Test merging when only semantic results exist."""
        semantic_results = {
            "searchResults": [
                {"entity": {"urn": "urn:li:document:doc1"}, "score": 0.85}
            ],
            "total": 1,
            "count": 1,
        }
        result = _merge_search_results(None, semantic_results)

        assert len(result["searchResults"]) == 1
        assert result["searchResults"][0]["searchType"] == "semantic"

    def test_merge_deduplication(self):
        """Test that duplicate URNs are deduplicated and marked as 'both'."""
        keyword_results = {
            "searchResults": [
                {"entity": {"urn": "urn:li:document:doc1"}, "score": 0.9},
                {"entity": {"urn": "urn:li:document:doc2"}, "score": 0.8},
            ],
            "total": 2,
            "count": 2,
            "facets": [],
        }
        semantic_results = {
            "searchResults": [
                {"entity": {"urn": "urn:li:document:doc1"}, "score": 0.85},
                {"entity": {"urn": "urn:li:document:doc3"}, "score": 0.75},
            ],
            "total": 2,
            "count": 2,
        }

        result = _merge_search_results(keyword_results, semantic_results)

        # Should have 3 unique results (doc1 deduplicated)
        assert len(result["searchResults"]) == 3

        # Find doc1 and verify it's marked as "both"
        doc1_result = next(
            r
            for r in result["searchResults"]
            if r["entity"]["urn"] == "urn:li:document:doc1"
        )
        assert doc1_result["searchType"] == "both"

        # Verify other results have appropriate searchType
        urns_and_types = {
            r["entity"]["urn"]: r["searchType"] for r in result["searchResults"]
        }
        assert urns_and_types["urn:li:document:doc2"] == "keyword"
        assert urns_and_types["urn:li:document:doc3"] == "semantic"

    def test_merge_keyword_first(self):
        """Test that top keyword result appears first."""
        keyword_results = {
            "searchResults": [
                {"entity": {"urn": "urn:li:document:keyword_top"}, "score": 0.95}
            ],
            "total": 1,
            "count": 1,
            "facets": [],
        }
        semantic_results = {
            "searchResults": [
                {"entity": {"urn": "urn:li:document:semantic_top"}, "score": 0.9}
            ],
            "total": 1,
            "count": 1,
        }

        result = _merge_search_results(keyword_results, semantic_results)

        # First result should be the top keyword result
        assert (
            result["searchResults"][0]["entity"]["urn"] == "urn:li:document:keyword_top"
        )
        assert result["searchResults"][0]["searchType"] == "keyword"

    def test_merge_empty_semantic_warning(self):
        """Test that empty semantic results still return keyword results."""
        keyword_results = {
            "searchResults": [
                {"entity": {"urn": "urn:li:document:doc1"}, "score": 0.9}
            ],
            "total": 1,
            "count": 1,
            "facets": [],
        }
        semantic_results = {
            "searchResults": [],
            "total": 0,
            "count": 0,
        }

        result = _merge_search_results(keyword_results, semantic_results)

        # Should return keyword results with searchType
        assert len(result["searchResults"]) == 1
        assert result["searchResults"][0]["searchType"] == "keyword"


class TestHybridSearchDocuments:
    """Tests for hybrid search functionality."""

    @pytest.fixture
    def mock_client(self):
        """Mock DataHub client."""
        client = MagicMock()
        client._graph = MagicMock()
        return client

    @pytest.fixture
    def mock_keyword_response(self):
        """Sample keyword search GraphQL response."""
        return {
            "searchAcrossEntities": {
                "start": 0,
                "count": 2,
                "total": 2,
                "searchResults": [
                    {
                        "entity": {"urn": "urn:li:document:doc1"},
                        "score": 0.9,
                    },
                    {
                        "entity": {"urn": "urn:li:document:doc2"},
                        "score": 0.8,
                    },
                ],
                "facets": [{"field": "platform", "aggregations": []}],
            }
        }

    @pytest.fixture
    def mock_semantic_response(self):
        """Sample semantic search GraphQL response."""
        return {
            "semanticSearchAcrossEntities": {
                "count": 2,
                "total": 2,
                "searchResults": [
                    {
                        "entity": {"urn": "urn:li:document:doc1"},
                        "score": 0.85,
                    },
                    {
                        "entity": {"urn": "urn:li:document:doc3"},
                        "score": 0.75,
                    },
                ],
                "facets": [],
            }
        }

    @patch("datahub_integrations.mcp.mcp_server.fetch_global_default_view")
    @patch("datahub_integrations.mcp.mcp_server.get_datahub_client")
    @patch("datahub_integrations.mcp.mcp_server.execute_graphql")
    async def test_hybrid_search_merges_results(
        self,
        mock_execute_graphql,
        mock_get_client,
        mock_fetch_view,
        mock_client,
        mock_keyword_response,
        mock_semantic_response,
    ):
        """Test that hybrid search merges keyword and semantic results."""
        mock_get_client.return_value = mock_client
        mock_fetch_view.return_value = None

        # Return different responses for keyword vs semantic search
        def side_effect(*args, **kwargs):
            operation_name = kwargs.get("operation_name", "")
            if operation_name == "documentSearch":
                return mock_keyword_response
            elif operation_name == "documentSemanticSearch":
                return mock_semantic_response
            return {}

        mock_execute_graphql.side_effect = side_effect

        result = await async_background(search_documents)(
            query="deployment", semantic_query="how to deploy applications"
        )

        # Verify both searches were called
        call_operations = [
            call.kwargs["operation_name"]
            for call in mock_execute_graphql.call_args_list
        ]
        assert "documentSearch" in call_operations
        assert "documentSemanticSearch" in call_operations

        # Verify results are merged with searchType
        assert "searchResults" in result
        for search_result in result["searchResults"]:
            assert "searchType" in search_result
            assert search_result["searchType"] in ("keyword", "semantic", "both")

    @patch("datahub_integrations.mcp.mcp_server.fetch_global_default_view")
    @patch("datahub_integrations.mcp.mcp_server.get_datahub_client")
    @patch("datahub_integrations.mcp.mcp_server.execute_graphql")
    async def test_hybrid_search_semantic_unavailable_fallback(
        self,
        mock_execute_graphql,
        mock_get_client,
        mock_fetch_view,
        mock_client,
        mock_keyword_response,
    ):
        """Test graceful fallback when semantic search is unavailable."""
        mock_get_client.return_value = mock_client
        mock_fetch_view.return_value = None

        # Return keyword response, but raise for semantic
        def side_effect(*args, **kwargs):
            operation_name = kwargs.get("operation_name", "")
            if operation_name == "documentSearch":
                return mock_keyword_response
            elif operation_name == "documentSemanticSearch":
                raise Exception("Semantic search not available")
            return {}

        mock_execute_graphql.side_effect = side_effect

        # Should not raise, should gracefully fall back to keyword only
        result = await async_background(search_documents)(
            query="deployment", semantic_query="how to deploy applications"
        )

        # Verify we got keyword results
        assert "searchResults" in result
        assert len(result["searchResults"]) > 0

        # All results should be marked as keyword (since semantic failed)
        for search_result in result["searchResults"]:
            assert search_result["searchType"] == "keyword"

    @patch("datahub_integrations.mcp.mcp_server.fetch_global_default_view")
    @patch("datahub_integrations.mcp.mcp_server.get_datahub_client")
    @patch("datahub_integrations.mcp.mcp_server.execute_graphql")
    async def test_hybrid_search_deduplication(
        self,
        mock_execute_graphql,
        mock_get_client,
        mock_fetch_view,
        mock_client,
        mock_keyword_response,
        mock_semantic_response,
    ):
        """Test that duplicate URNs are properly deduplicated."""
        mock_get_client.return_value = mock_client
        mock_fetch_view.return_value = None

        def side_effect(*args, **kwargs):
            operation_name = kwargs.get("operation_name", "")
            if operation_name == "documentSearch":
                return mock_keyword_response
            elif operation_name == "documentSemanticSearch":
                return mock_semantic_response
            return {}

        mock_execute_graphql.side_effect = side_effect

        result = await async_background(search_documents)(
            query="deployment", semantic_query="how to deploy applications"
        )

        # Count unique URNs in results
        urns = [r["entity"]["urn"] for r in result["searchResults"]]
        assert len(urns) == len(set(urns)), "Results should not contain duplicate URNs"

        # doc1 appears in both responses - should be marked as "both"
        doc1_results = [
            r
            for r in result["searchResults"]
            if r["entity"]["urn"] == "urn:li:document:doc1"
        ]
        assert len(doc1_results) == 1, "doc1 should appear exactly once"
        assert doc1_results[0]["searchType"] == "both"

    @patch("datahub_integrations.mcp.mcp_server.fetch_global_default_view")
    @patch("datahub_integrations.mcp.mcp_server.get_datahub_client")
    @patch("datahub_integrations.mcp.mcp_server.execute_graphql")
    async def test_keyword_only_when_no_semantic_query(
        self,
        mock_execute_graphql,
        mock_get_client,
        mock_fetch_view,
        mock_client,
        mock_keyword_response,
    ):
        """Test that only keyword search is called when semantic_query is not provided."""
        mock_get_client.return_value = mock_client
        mock_fetch_view.return_value = None
        mock_execute_graphql.return_value = mock_keyword_response

        await async_background(search_documents)(query="deployment")

        # Verify only keyword search was called
        assert mock_execute_graphql.call_count == 1
        call_args = mock_execute_graphql.call_args
        assert call_args.kwargs["operation_name"] == "documentSearch"

    @patch("datahub_integrations.mcp.mcp_server.fetch_global_default_view")
    @patch("datahub_integrations.mcp.mcp_server.get_datahub_client")
    @patch("datahub_integrations.mcp.mcp_server.execute_graphql")
    async def test_hybrid_search_pagination(
        self,
        mock_execute_graphql,
        mock_get_client,
        mock_fetch_view,
        mock_client,
    ):
        """Test that hybrid search fetches enough results for pagination."""
        mock_get_client.return_value = mock_client
        mock_fetch_view.return_value = None

        # Create responses with multiple results
        keyword_response = {
            "searchAcrossEntities": {
                "start": 0,
                "count": 5,
                "total": 5,
                "searchResults": [
                    {
                        "entity": {"urn": f"urn:li:document:kw{i}"},
                        "score": 0.9 - i * 0.1,
                    }
                    for i in range(5)
                ],
                "facets": [],
            }
        }
        semantic_response = {
            "semanticSearchAcrossEntities": {
                "count": 5,
                "total": 5,
                "searchResults": [
                    {
                        "entity": {"urn": f"urn:li:document:sem{i}"},
                        "score": 0.85 - i * 0.1,
                    }
                    for i in range(5)
                ],
                "facets": [],
            }
        }

        def side_effect(*args, **kwargs):
            operation_name = kwargs.get("operation_name", "")
            if operation_name == "documentSearch":
                return keyword_response
            elif operation_name == "documentSemanticSearch":
                return semantic_response
            return {}

        mock_execute_graphql.side_effect = side_effect

        # Request page 2 (offset=3, num_results=3)
        result = await async_background(search_documents)(
            query="deployment",
            semantic_query="how to deploy",
            num_results=3,
            offset=3,
        )

        # Both searches should be called with count = offset + num_results = 6
        for call in mock_execute_graphql.call_args_list:
            variables = call.kwargs["variables"]
            assert variables["count"] == 6  # offset (3) + num_results (3)
            assert variables.get("start", 0) == 0  # Always fetch from beginning

        # Verify pagination metadata
        assert result["start"] == 3
        assert result["count"] == 3
        assert len(result["searchResults"]) == 3
