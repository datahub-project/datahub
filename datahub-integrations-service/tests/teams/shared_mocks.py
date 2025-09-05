"""
Shared mock objects and utilities for Teams integration tests.
This module provides reusable mocks to ensure consistency across test files.
"""

from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock

from datahub_integrations.teams.config import TeamsAppDetails, TeamsConnection


class MockEntityBuilder:
    """Builder pattern for creating mock entity data with various edge cases."""

    def __init__(
        self,
        entity_type: str = "DATASET",
        urn: str = "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)",
    ):
        self.entity_data: Dict[str, Any] = {
            "urn": urn,
            "type": entity_type,
            "properties": {},
            "editableProperties": {},
            "info": {},
            "platform": {},
            "ownership": {},
            "globalTags": {},
            "glossaryTerms": {},
            "statsSummary": {},
            "subTypes": {},
            "deprecation": {},
        }

    def with_name(self, name: str, location: str = "properties") -> "MockEntityBuilder":
        """Add name to specified location (properties, editableProperties, info, or root)."""
        if location == "properties":
            self.entity_data["properties"]["name"] = name
        elif location == "editableProperties":
            self.entity_data["editableProperties"]["displayName"] = name
        elif location == "info":
            self.entity_data["info"]["name"] = name
        elif location == "root":
            self.entity_data["name"] = name
        return self

    def with_description(self, description: str) -> "MockEntityBuilder":
        """Add description to entity."""
        self.entity_data["properties"]["description"] = description
        return self

    def with_platform(
        self, platform_name: str, display_name: Optional[str] = None
    ) -> "MockEntityBuilder":
        """Add platform information."""
        self.entity_data["platform"] = {
            "name": platform_name,
            "properties": {"displayName": display_name or platform_name},
        }
        return self

    def with_owner(
        self, owner_name: str, owner_type: str = "CORP_USER"
    ) -> "MockEntityBuilder":
        """Add ownership information."""
        self.entity_data["ownership"] = {
            "owners": [
                {
                    "owner": {
                        "type": owner_type,
                        "username": owner_name,
                        "properties": {"displayName": owner_name},
                        "info": {"displayName": owner_name},
                    }
                }
            ]
        }
        return self

    def with_tags(self, tag_names: List[str]) -> "MockEntityBuilder":
        """Add global tags."""
        self.entity_data["globalTags"] = {
            "tags": [{"tag": {"properties": {"name": name}}} for name in tag_names]
        }
        return self

    def with_certification(self, certified: bool = True) -> "MockEntityBuilder":
        """Add certification status via tags."""
        if certified:
            return self.with_tags(["certified", "production"])
        else:
            return self.with_deprecation(True)

    def with_deprecation(self, deprecated: bool = True) -> "MockEntityBuilder":
        """Add deprecation status."""
        self.entity_data["deprecation"] = {"deprecated": deprecated}
        return self

    def with_stats(
        self,
        query_count: Optional[int] = None,
        view_count: Optional[int] = None,
        query_percentile: Optional[float] = None,
        view_percentile: Optional[float] = None,
        user_percentile: Optional[float] = None,
    ) -> "MockEntityBuilder":
        """Add statistics information."""
        stats: Dict[str, Any] = {}
        if query_count is not None:
            stats["queryCountLast30Days"] = query_count
        if view_count is not None:
            stats["viewCountLast30Days"] = view_count
        if query_percentile is not None:
            stats["queryCountPercentileLast30Days"] = query_percentile
        if view_percentile is not None:
            stats["viewCountPercentileLast30Days"] = view_percentile
        if user_percentile is not None:
            stats["uniqueUserPercentileLast30Days"] = user_percentile

        self.entity_data["statsSummary"] = stats
        return self

    def with_subtype(self, subtype: str) -> "MockEntityBuilder":
        """Add subtype information."""
        self.entity_data["subTypes"] = {"typeNames": [subtype]}
        return self

    def empty_properties(self) -> "MockEntityBuilder":
        """Remove all property fields to test fallback behavior."""
        self.entity_data["properties"] = {}
        self.entity_data["editableProperties"] = {}
        self.entity_data["info"] = {}
        if "name" in self.entity_data:
            del self.entity_data["name"]
        return self

    def malformed_data(
        self, corruption_type: str = "missing_nested"
    ) -> "MockEntityBuilder":
        """Create malformed data for testing error handling."""
        if corruption_type == "missing_nested":
            # Remove nested dictionaries
            for key in ["properties", "platform", "ownership"]:
                self.entity_data[key] = None
        elif corruption_type == "wrong_types":
            # Set wrong data types
            self.entity_data["globalTags"] = "not_a_dict"
            self.entity_data["ownership"] = ["not_a_dict"]
        elif corruption_type == "partial_corruption":
            # Partially corrupt nested data
            self.entity_data["platform"] = {"properties": None}
            self.entity_data["ownership"] = {"owners": "not_a_list"}
        return self

    def build(self) -> Dict[str, Any]:
        """Return the built entity data."""
        return self.entity_data.copy()


class MockGraphQLResponses:
    """Factory for creating various GraphQL response scenarios."""

    @staticmethod
    def successful_entity_response(entity: Dict[str, Any]) -> Dict[str, Any]:
        """Create a successful GraphQL response with entity data."""
        return {"entity": entity}

    @staticmethod
    def entity_not_found_response() -> Dict[str, Any]:
        """Create a response where entity is not found."""
        return {"entity": None}

    @staticmethod
    def empty_response() -> Dict[str, Any]:
        """Create an empty/malformed response."""
        return {}

    @staticmethod
    def error_response() -> Exception:
        """Create a GraphQL error."""
        return Exception("GraphQL query failed")


class MockTeamsConfig:
    """Factory for creating Teams configuration mocks."""

    @staticmethod
    def valid_config() -> MagicMock:
        """Create a valid Teams configuration mock."""
        app_details = MagicMock(spec=TeamsAppDetails)
        app_details.app_id = "test-app-id"
        app_details.app_password = "test-app-password"
        app_details.tenant_id = "test-tenant-id"

        config = MagicMock(spec=TeamsConnection)
        config.app_details = app_details
        config.webhook_url = "https://test.webhook.url"
        config.enable_conversation_history = True
        return config

    @staticmethod
    def missing_app_details() -> MagicMock:
        """Create config with missing app details."""
        config = MagicMock(spec=TeamsConnection)
        config.app_details = None
        return config

    @staticmethod
    def incomplete_credentials() -> MagicMock:
        """Create config with incomplete credentials."""
        app_details = MagicMock(spec=TeamsAppDetails)
        app_details.app_id = "test-app-id"
        app_details.app_password = None  # Missing password
        app_details.tenant_id = "test-tenant-id"

        config = MagicMock(spec=TeamsConnection)
        config.app_details = app_details
        return config


class MockDataHubGraph:
    """Factory for creating DataHub graph client mocks."""

    @staticmethod
    def successful_graph(entity_response: Dict[str, Any]) -> MagicMock:
        """Create a graph client that returns successful responses."""
        graph = MagicMock()
        graph.execute_graphql.return_value = entity_response
        return graph

    @staticmethod
    def failing_graph(error: Exception) -> MagicMock:
        """Create a graph client that raises errors."""
        graph = MagicMock()
        graph.execute_graphql.side_effect = error
        return graph


class MockResponseText:
    """Factory for creating various response text scenarios for entity extraction."""

    @staticmethod
    def with_url_entities() -> str:
        """Response text with URL-based entity references."""
        return """
        Based on your query, I found these relevant datasets:
        - [Customer Orders](https://datahub.company.com/dataset/urn:li:dataset:(urn:li:dataPlatform:snowflake,orders,PROD))
        - [Product Catalog](https://datahub.company.com/chart/urn:li:chart:(urn:li:dataPlatform:looker,dashboard.chart_123,PROD))
        
        You might also be interested in this dashboard:
        [Sales Dashboard](https://datahub.company.com/dashboard/urn:li:dashboard:(urn:li:dataPlatform:looker,sales_dashboard,PROD))
        """

    @staticmethod
    def with_urn_entities() -> str:
        """Response text with URN-based entity references."""
        return """
        Here are the entities you should check:
        - [User Table](urn:li:dataset:(urn:li:dataPlatform:postgres,public.users,PROD))
        - [Analytics User](urn:li:corpuser:john.doe)
        - [Data Team](urn:li:corpGroup:data-team)
        """

    @staticmethod
    def with_mixed_entities() -> str:
        """Response text with both URL and URN references."""
        return """
        Check these out:
        - [Orders](https://datahub.company.com/dataset/encoded_urn_here)
        - [Users](urn:li:dataset:(urn:li:dataPlatform:postgres,users,PROD))
        """

    @staticmethod
    def with_malformed_entities() -> str:
        """Response text with malformed entity references."""
        return """
        Here are some broken references:
        - [Broken URL](https://not-datahub.com/invalid/path)
        - [Incomplete URN](urn:li:incomplete)
        - [Missing Name]()
        - []()
        """

    @staticmethod
    def with_no_entities() -> str:
        """Response text with no entity references."""
        return "This is just plain text with no entity references at all."

    @staticmethod
    def with_duplicates() -> str:
        """Response text with duplicate entity references."""
        return """
        Multiple references to the same entity:
        - [Orders](https://datahub.company.com/dataset/urn:li:dataset:orders)
        - [Orders Again](https://datahub.company.com/dataset/urn:li:dataset:orders)
        - [Orders URN](urn:li:dataset:orders)
        """

    @staticmethod
    def with_edge_case_formats() -> str:
        """Response text with edge case URL formats."""
        return """
        Edge cases:
        - [URL with query params](https://datahub.company.com/dataset/urn?param=value)
        - [URL with fragment](https://datahub.company.com/dataset/urn#section)
        - [URL with spaces in name](https://datahub.company.com/dataset/urn%20with%20spaces)
        """


class MockHttpResponses:
    """Factory for creating HTTP response mocks for external API calls."""

    @staticmethod
    def graph_api_token_success() -> MagicMock:
        """Mock successful Graph API token response."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "access_token": "test-access-token",
            "token_type": "Bearer",
            "expires_in": 3600,
        }
        return mock_response

    @staticmethod
    def graph_api_token_failure() -> MagicMock:
        """Mock failed Graph API token response."""
        mock_response = MagicMock()
        mock_response.status_code = 401
        mock_response.text = "Unauthorized"
        return mock_response

    @staticmethod
    def teams_webhook_activity() -> Dict[str, Any]:
        """Mock Teams webhook activity."""
        return {
            "type": "message",
            "id": "test-activity-id",
            "timestamp": "2023-01-01T00:00:00Z",
            "from": {"id": "test-user-id", "name": "Test User"},
            "conversation": {"id": "test-conversation-id"},
            "text": "Hello DataHub!",
        }
