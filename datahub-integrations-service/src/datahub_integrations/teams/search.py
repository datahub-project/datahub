"""Teams search functionality for users and channels."""

from typing import List, Optional

from loguru import logger
from pydantic import BaseModel

from datahub_integrations.teams.config import TeamsConnection, teams_config
from datahub_integrations.teams.graph_api import GraphApiClient


class SearchResult(BaseModel):
    """Represents a search result for type-ahead."""

    id: str
    type: str  # "user" or "channel"
    displayName: str
    email: Optional[str] = None
    userPrincipalName: Optional[str] = None
    teamId: Optional[str] = None
    teamName: Optional[str] = None
    description: Optional[str] = None
    membershipType: Optional[str] = None


class SearchResponse(BaseModel):
    """Response format for search APIs."""

    results: List[SearchResult]
    hasMore: bool
    totalCount: int


class TeamsSearchService:
    """Service for searching Teams users and channels."""

    def __init__(self, config: Optional[TeamsConnection] = None):
        self.config = config or teams_config.get_config()
        self._graph_client: Optional[GraphApiClient] = None

    @property
    def graph_client(self) -> GraphApiClient:
        """Get or create Graph API client."""
        if not self._graph_client:
            self._graph_client = GraphApiClient(self.config)
        return self._graph_client

    @graph_client.setter
    def graph_client(self, value: GraphApiClient) -> None:
        """Set the Graph API client (used for testing)."""
        self._graph_client = value

    @graph_client.deleter
    def graph_client(self) -> None:
        """Delete the Graph API client (used for testing)."""
        self._graph_client = None

    async def search_channels(
        self, query: str, limit: int = 25, team_ids: Optional[List[str]] = None
    ) -> SearchResponse:
        """
        Search for Teams channels.

        Args:
            query: Search term for channel names
            limit: Maximum number of results to return
            team_ids: Specific team IDs to search in (optional)

        Returns:
            SearchResponse with channel results
        """
        try:
            if not query.strip():
                return SearchResponse(results=[], hasMore=False, totalCount=0)

            logger.info(f"Searching channels with query: '{query}', limit: {limit}")

            # Use Graph API client to search channels
            raw_results = await self.graph_client.search_channels(
                query=query.strip(),
                limit=limit + 1,  # Get one extra to determine hasMore
                team_ids=team_ids,
            )

            # Convert to SearchResult objects
            results = []
            for raw_result in raw_results[:limit]:
                search_result = SearchResult(
                    id=raw_result["id"],
                    type="channel",
                    displayName=raw_result["displayName"],
                    teamId=raw_result.get("teamId"),
                    teamName=raw_result.get("teamName"),
                    description=raw_result.get("description"),
                    membershipType=raw_result.get("membershipType", "standard"),
                )
                results.append(search_result)

            has_more = len(raw_results) > limit
            total_count = len(results)

            logger.info(
                f"Channel search returned {len(results)} results, hasMore: {has_more}"
            )

            return SearchResponse(
                results=results, hasMore=has_more, totalCount=total_count
            )

        except Exception as e:
            logger.error(f"Error in channel search: {e}")
            return SearchResponse(results=[], hasMore=False, totalCount=0)

    async def search_users(self, query: str, limit: int = 25) -> SearchResponse:
        """
        Search for users in the organization.

        Args:
            query: Search term for user names or emails
            limit: Maximum number of results to return

        Returns:
            SearchResponse with user results
        """
        try:
            if not query.strip():
                return SearchResponse(results=[], hasMore=False, totalCount=0)

            logger.info(f"Searching users with query: '{query}', limit: {limit}")

            # Use Graph API client to search users
            raw_results = await self.graph_client.search_users(
                query=query.strip(),
                limit=limit + 1,  # Get one extra to determine hasMore
            )

            # Convert to SearchResult objects
            results = []
            for raw_result in raw_results[:limit]:
                search_result = SearchResult(
                    id=raw_result["id"],
                    type="user",
                    displayName=raw_result["displayName"],
                    email=raw_result.get("email"),
                    userPrincipalName=raw_result.get("userPrincipalName"),
                )
                results.append(search_result)

            has_more = len(raw_results) > limit
            total_count = len(results)

            logger.info(
                f"User search returned {len(results)} results, hasMore: {has_more}"
            )

            return SearchResponse(
                results=results, hasMore=has_more, totalCount=total_count
            )

        except Exception as e:
            logger.error(f"Error in user search: {e}")
            return SearchResponse(results=[], hasMore=False, totalCount=0)

    async def search_all(
        self,
        query: str,
        limit: int = 25,
        search_users: bool = True,
        search_channels: bool = True,
        team_ids: Optional[List[str]] = None,
    ) -> SearchResponse:
        """
        Search for both users and channels.

        Args:
            query: Search term
            limit: Maximum number of results to return total
            search_users: Whether to include user results
            search_channels: Whether to include channel results
            team_ids: Specific team IDs to search channels in (optional)

        Returns:
            SearchResponse with combined results
        """
        try:
            if not query.strip():
                return SearchResponse(results=[], hasMore=False, totalCount=0)

            logger.info(f"Searching all with query: '{query}', limit: {limit}")

            all_results = []

            # Search users if requested
            if search_users:
                user_limit = limit // 2 if search_channels else limit
                user_response = await self.search_users(query, user_limit)
                all_results.extend(user_response.results)

            # Search channels if requested
            if search_channels:
                channel_limit = limit - len(all_results) if search_users else limit
                if channel_limit > 0:
                    channel_response = await self.search_channels(
                        query, channel_limit, team_ids
                    )
                    all_results.extend(channel_response.results)

            # Sort results: users first, then channels, then alphabetically within each type
            all_results.sort(
                key=lambda x: (0 if x.type == "user" else 1, x.displayName.lower())
            )

            # Limit results
            final_results = all_results[:limit]
            has_more = len(all_results) > limit

            logger.info(
                f"Combined search returned {len(final_results)} results, hasMore: {has_more}"
            )

            return SearchResponse(
                results=final_results, hasMore=has_more, totalCount=len(final_results)
            )

        except Exception as e:
            logger.error(f"Error in combined search: {e}")
            return SearchResponse(results=[], hasMore=False, totalCount=0)

    async def list_all_channels(self, limit: int = 100) -> SearchResponse:
        """
        List all available channels from all accessible teams.
        This is useful for pre-loading channel lists without requiring a search query.

        Args:
            limit: Maximum number of channels to return

        Returns:
            SearchResponse with channel results
        """
        try:
            logger.info(f"Listing all channels with limit: {limit}")

            # Use Graph API client to list all channels
            raw_results = await self.graph_client.list_all_channels(
                limit=limit + 1
            )  # Get one extra to determine hasMore

            # Convert to SearchResult objects
            results = []
            for raw_result in raw_results[:limit]:
                search_result = SearchResult(
                    id=raw_result["id"],
                    type="channel",
                    displayName=raw_result["displayName"],
                    teamId=raw_result.get("teamId"),
                    teamName=raw_result.get("teamName"),
                    description=raw_result.get("description"),
                    membershipType=raw_result.get("membershipType", "standard"),
                )
                results.append(search_result)

            has_more = len(raw_results) > limit
            total_count = len(results)

            logger.info(f"Listed {len(results)} total channels, hasMore: {has_more}")

            return SearchResponse(
                results=results, hasMore=has_more, totalCount=total_count
            )

        except Exception as e:
            logger.error(f"Error listing all channels: {e}")
            return SearchResponse(results=[], hasMore=False, totalCount=0)


# Global search service instance
_search_service: Optional[TeamsSearchService] = None


def get_search_service() -> TeamsSearchService:
    """Get or create the global search service instance."""
    global _search_service
    if not _search_service:
        _search_service = TeamsSearchService()
    return _search_service
