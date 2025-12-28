"""Unit tests for search_documents MCP tool."""

from unittest.mock import MagicMock, patch

import pytest

from datahub_integrations.mcp.mcp_server import async_background, search_documents
from datahub_integrations.mcp.tools.documents import _search_documents_impl

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
