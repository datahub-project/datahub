"""Microsoft Graph API client for Teams integration."""

from typing import Any, Dict, List, Optional, Union

import httpx
from loguru import logger
from pydantic import BaseModel

from datahub_integrations.teams.config import TeamsConnection


class GraphApiChannel(BaseModel):
    """Represents a Teams channel from Graph API."""

    id: str
    displayName: str
    description: Optional[str] = None
    membershipType: Optional[str] = None


class GraphApiTeam(BaseModel):
    """Represents a Teams team from Graph API."""

    id: str
    displayName: str
    description: Optional[str] = None


class GraphApiUser(BaseModel):
    """Represents a user from Graph API."""

    id: str
    displayName: str
    mail: Optional[str] = None
    userPrincipalName: Optional[str] = None


class GraphApiClient:
    """Client for Microsoft Graph API operations."""

    def __init__(self, config: TeamsConnection):
        self.config = config
        self._access_token: Optional[str] = None

    async def _get_graph_access_token(self, use_delegated: bool = False) -> str:
        """
        Get Microsoft Graph API access token.

        Args:
            use_delegated: If True, attempt to use delegated permissions with stored refresh token

        Returns:
            Access token for Graph API
        """
        if not self.config.app_details:
            raise ValueError("Teams app details not configured")

        app_id = self.config.app_details.app_id
        app_password = self.config.app_details.app_password
        tenant_id = self.config.app_details.tenant_id

        if not app_id or not app_password or not tenant_id:
            raise ValueError(
                "Missing Teams app credentials (app_id, app_password, or tenant_id)"
            )

        token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"

        if use_delegated:
            # Try delegated permissions with refresh token (if available)
            # This would require storing refresh tokens from user consent flow
            # For now, fall back to application permissions
            logger.info(
                "Delegated permission flow not yet implemented, using application permissions"
            )

        # Use application permissions (client credentials flow)
        data = {
            "grant_type": "client_credentials",
            "client_id": app_id,
            "client_secret": app_password,
            "scope": "https://graph.microsoft.com/.default",
        }

        headers = {"Content-Type": "application/x-www-form-urlencoded"}

        async with httpx.AsyncClient() as client:
            response = await client.post(token_url, data=data, headers=headers)

            if response.status_code != 200:
                logger.error(
                    f"Failed to get Graph API token: {response.status_code} {response.text}"
                )
                raise Exception("Failed to authenticate with Microsoft Graph API")

            token_data = response.json()
            return token_data["access_token"]

    async def _make_graph_request(
        self, endpoint: str, params: Optional[Dict[str, Union[str, int]]] = None
    ) -> Dict[str, Any]:
        """Make a request to Microsoft Graph API."""
        if not self._access_token:
            self._access_token = await self._get_graph_access_token()

        headers = {
            "Authorization": f"Bearer {self._access_token}",
            "Content-Type": "application/json",
        }

        url = f"https://graph.microsoft.com/v1.0{endpoint}"

        async with httpx.AsyncClient() as client:
            response = await client.get(url, headers=headers, params=params or {})

            if response.status_code == 401:
                # Token might be expired, try refreshing once
                logger.info("Graph API token expired, refreshing...")
                self._access_token = await self._get_graph_access_token()
                headers["Authorization"] = f"Bearer {self._access_token}"
                response = await client.get(url, headers=headers, params=params or {})

            if response.status_code not in [200, 201]:
                logger.error(
                    f"Graph API request failed: {response.status_code} {response.text}"
                )
                raise Exception(f"Graph API request failed: {response.status_code}")

            return response.json()

    async def get_teams(self, limit: int = 50) -> List[GraphApiTeam]:
        """Get list of Teams that the app has access to."""
        try:
            # Get teams where the app is installed
            params: Dict[str, Union[str, int]] = {
                "$select": "id,displayName,description",
                "$top": limit,
            }

            data = await self._make_graph_request("/teams", params)
            teams: List[GraphApiTeam] = []

            for team_data in data.get("value", []):
                teams.append(GraphApiTeam(**team_data))

            logger.info(f"Retrieved {len(teams)} teams from Graph API")
            return teams

        except Exception as e:
            logger.error(f"Error retrieving teams: {e}")
            return []

    async def search_channels(
        self, query: str, limit: int = 25, team_ids: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """
        Search for channels across teams.

        Args:
            query: Search term for channel names
            limit: Maximum number of results to return
            team_ids: Specific team IDs to search in (if None, searches all accessible teams)

        Returns:
            List of channel results with team information
        """
        try:
            # If no specific teams provided, get all accessible teams
            if not team_ids:
                teams = await self.get_teams(
                    limit=100
                )  # Get more teams for comprehensive search
                team_ids = [team.id for team in teams]
                teams_dict = {team.id: team for team in teams}
            else:
                # Fetch team details for provided team IDs
                teams_dict = {}
                for team_id in team_ids:
                    try:
                        team_data = await self._make_graph_request(f"/teams/{team_id}")
                        teams_dict[team_id] = GraphApiTeam(**team_data)
                    except Exception as e:
                        logger.warning(f"Could not fetch team {team_id}: {e}")

            all_channels = []

            # Search channels in each team
            for team_id in team_ids[:20]:  # Limit to 20 teams to avoid API limits
                try:
                    # Get channels for this team
                    params: Dict[str, Union[str, int]] = {
                        "$select": "id,displayName,description,membershipType",
                    }
                    if query:
                        params["$filter"] = (
                            f"contains(tolower(displayName), tolower('{query}'))"
                        )

                    channels_data = await self._make_graph_request(
                        f"/teams/{team_id}/channels", params
                    )

                    team_info = teams_dict.get(team_id)
                    team_name = team_info.displayName if team_info else "Unknown Team"

                    for channel_data in channels_data.get("value", []):
                        channel_result = {
                            "id": channel_data["id"],
                            "type": "channel",
                            "displayName": channel_data["displayName"],
                            "description": channel_data.get("description"),
                            "teamId": team_id,
                            "teamName": team_name,
                            "membershipType": channel_data.get(
                                "membershipType", "standard"
                            ),
                        }
                        all_channels.append(channel_result)

                        # Stop if we've reached the limit
                        if len(all_channels) >= limit:
                            break

                    if len(all_channels) >= limit:
                        break

                except Exception as e:
                    logger.warning(f"Error searching channels in team {team_id}: {e}")
                    continue

            # Sort by relevance (exact matches first, then alphabetical)
            query_lower = query.lower() if query else ""
            all_channels.sort(
                key=lambda x: (
                    0 if x["displayName"].lower() == query_lower else 1,
                    1 if query_lower in x["displayName"].lower() else 2,
                    x["displayName"].lower(),
                )
            )

            logger.info(f"Found {len(all_channels)} channels matching '{query}'")
            return all_channels[:limit]

        except Exception as e:
            logger.error(f"Error searching channels: {e}")
            return []

    async def search_users(self, query: str, limit: int = 25) -> List[Dict[str, Any]]:
        """
        Search for users in the organization.

        Args:
            query: Search term for user names or emails
            limit: Maximum number of results to return

        Returns:
            List of user results
        """
        try:
            # Use Microsoft Graph search capabilities
            params: Dict[str, Union[str, int]] = {
                "$search": f'"displayName:{query}" OR "mail:{query}" OR "userPrincipalName:{query}"',
                "$select": "id,displayName,mail,userPrincipalName",
                "$top": limit,
                "$count": "true",
            }

            # Add ConsistencyLevel header for $search
            headers = {
                "Authorization": f"Bearer {self._access_token or await self._get_graph_access_token()}",
                "Content-Type": "application/json",
                "ConsistencyLevel": "eventual",
            }

            url = "https://graph.microsoft.com/v1.0/users"

            async with httpx.AsyncClient() as client:
                response = await client.get(url, headers=headers, params=params)

                if response.status_code == 401:
                    # Token might be expired, try refreshing once
                    self._access_token = await self._get_graph_access_token()
                    headers["Authorization"] = f"Bearer {self._access_token}"
                    response = await client.get(url, headers=headers, params=params)

                if response.status_code not in [200, 201]:
                    logger.error(
                        f"User search failed: {response.status_code} {response.text}"
                    )
                    return []

                data = response.json()
                users: List[Dict[str, Any]] = []

                for user_data in data.get("value", []):
                    user_result = {
                        "id": user_data["id"],
                        "type": "user",
                        "displayName": user_data["displayName"],
                        "email": user_data.get("mail")
                        or user_data.get("userPrincipalName"),
                        "userPrincipalName": user_data.get("userPrincipalName"),
                    }
                    users.append(user_result)

                logger.info(f"Found {len(users)} users matching '{query}'")
                return users

        except Exception as e:
            logger.error(f"Error searching users: {e}")
            return []

    async def check_permissions(self) -> Dict[str, Any]:
        """
        Check what permissions the app has by testing various Graph API endpoints.

        Returns:
            Dict with permission check results
        """
        results: Dict[str, Any] = {
            "token_valid": False,
            "user_read": False,
            "teams_read": False,
            "channel_read": False,
            "permissions": [],
            "errors": [],
        }

        try:
            # Test if we can get a token
            token = await self._get_graph_access_token()
            results["token_valid"] = True

            headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            }

            async with httpx.AsyncClient() as client:
                # Test 1: Check if we can read users
                try:
                    response = await client.get(
                        "https://graph.microsoft.com/v1.0/users?$top=1", headers=headers
                    )
                    if response.status_code in [200, 201]:
                        results["user_read"] = True
                    else:
                        results["errors"].append(
                            f"User read failed: {response.status_code} - {response.text[:200]}"
                        )
                except Exception as e:
                    results["errors"].append(f"User read error: {str(e)}")

                # Test 2: Check if we can read teams
                try:
                    response = await client.get(
                        "https://graph.microsoft.com/v1.0/teams?$top=1", headers=headers
                    )
                    if response.status_code in [200, 201]:
                        results["teams_read"] = True
                        teams_data = response.json()
                        results["teams_count"] = len(teams_data.get("value", []))
                    else:
                        results["errors"].append(
                            f"Teams read failed: {response.status_code} - {response.text[:200]}"
                        )
                except Exception as e:
                    results["errors"].append(f"Teams read error: {str(e)}")

                # Test 3: Check if we can read the current app's permissions
                try:
                    # Get the app's service principal to see what permissions it has
                    app_id = (
                        self.config.app_details.app_id
                        if self.config.app_details
                        else None
                    )
                    if app_id:
                        response = await client.get(
                            f"https://graph.microsoft.com/v1.0/servicePrincipals?$filter=appId eq '{app_id}'&$select=appRoles,oauth2PermissionScopes",
                            headers=headers,
                        )
                        if response.status_code in [200, 201]:
                            sp_data = response.json()
                            if sp_data.get("value"):
                                sp = sp_data["value"][0]
                                results["app_roles"] = sp.get("appRoles", [])
                                results["oauth2_scopes"] = sp.get(
                                    "oauth2PermissionScopes", []
                                )
                except Exception as e:
                    results["errors"].append(f"Service principal check error: {str(e)}")

                # Test 4: Try to get a single team's channels (if we have any teams)
                teams_count = results.get("teams_count", 0)
                if (
                    results.get("teams_read")
                    and isinstance(teams_count, int)
                    and teams_count > 0
                ):
                    try:
                        # Get first team
                        teams_response = await client.get(
                            "https://graph.microsoft.com/v1.0/teams?$top=1",
                            headers=headers,
                        )
                        if teams_response.status_code in [200, 201]:
                            teams_data = teams_response.json()
                            if teams_data.get("value"):
                                team_id = teams_data["value"][0]["id"]

                                # Try to get channels for this team
                                channels_response = await client.get(
                                    f"https://graph.microsoft.com/v1.0/teams/{team_id}/channels",
                                    headers=headers,
                                )
                                if channels_response.status_code in [200, 201]:
                                    results["channel_read"] = True
                                    channels_data = channels_response.json()
                                    results["sample_channels_count"] = len(
                                        channels_data.get("value", [])
                                    )
                                else:
                                    results["errors"].append(
                                        f"Channel read failed: {channels_response.status_code} - {channels_response.text[:200]}"
                                    )
                    except Exception as e:
                        results["errors"].append(f"Channel read test error: {str(e)}")

        except Exception as e:
            results["errors"].append(f"Token acquisition failed: {str(e)}")

        return results

    async def get_application_permissions(self) -> Dict[str, Any]:
        """Get the actual permissions granted to this application."""
        try:
            token = await self._get_graph_access_token()
            headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            }

            app_id = self.config.app_details.app_id if self.config.app_details else None
            if not app_id:
                return {"error": "No app ID configured"}

            async with httpx.AsyncClient() as client:
                # Get service principal for our app
                response = await client.get(
                    f"https://graph.microsoft.com/v1.0/servicePrincipals?$filter=appId eq '{app_id}'",
                    headers=headers,
                )

                if response.status_code not in [200, 201]:
                    return {
                        "error": f"Failed to get service principal: {response.status_code}"
                    }

                data = response.json()
                if not data.get("value"):
                    return {"error": "Service principal not found"}

                service_principal = data["value"][0]
                sp_id = service_principal["id"]

                # Get app role assignments (application permissions)
                app_roles_response = await client.get(
                    f"https://graph.microsoft.com/v1.0/servicePrincipals/{sp_id}/appRoleAssignments",
                    headers=headers,
                )

                app_permissions = []
                if app_roles_response.status_code in [200, 201]:
                    assignments = app_roles_response.json().get("value", [])
                    for assignment in assignments:
                        app_permissions.append(
                            {
                                "id": assignment.get("appRoleId"),
                                "principalDisplayName": assignment.get(
                                    "principalDisplayName"
                                ),
                                "resourceDisplayName": assignment.get(
                                    "resourceDisplayName"
                                ),
                            }
                        )

                # Get delegated permissions (oauth2PermissionGrants)
                delegated_response = await client.get(
                    f"https://graph.microsoft.com/v1.0/servicePrincipals/{sp_id}/oauth2PermissionGrants",
                    headers=headers,
                )

                delegated_permissions = []
                if delegated_response.status_code in [200, 201]:
                    grants = delegated_response.json().get("value", [])
                    for grant in grants:
                        delegated_permissions.append(
                            {
                                "scope": grant.get("scope"),
                                "resourceId": grant.get("resourceId"),
                                "consentType": grant.get("consentType"),
                            }
                        )

                return {
                    "app_id": app_id,
                    "service_principal_id": sp_id,
                    "application_permissions": app_permissions,
                    "delegated_permissions": delegated_permissions,
                }

        except Exception as e:
            return {"error": f"Failed to get permissions: {str(e)}"}

    async def list_all_channels(self, limit: int = 100) -> List[Dict[str, Any]]:
        """
        List all channels from all accessible teams.

        Args:
            limit: Maximum number of channels to return

        Returns:
            List of channel results with team information
        """
        try:
            logger.info(f"Listing all channels with limit: {limit}")

            # Get all accessible teams first
            teams = await self.get_teams(
                limit=100
            )  # Get more teams for comprehensive listing

            all_channels: list[dict] = []

            for team in teams:
                try:
                    # Get channels for this team
                    # Note: $top is not supported on the /teams/{teamId}/channels endpoint
                    params: Dict[str, Union[str, int]] = {
                        "$select": "id,displayName,description,membershipType",
                    }

                    endpoint = f"/teams/{team.id}/channels"
                    data = await self._make_graph_request(endpoint, params)

                    team_channels = data.get("value", [])
                    logger.debug(
                        f"Found {len(team_channels)} channels in team '{team.displayName}'"
                    )

                    # Add team information to each channel
                    for channel_data in team_channels:
                        # Stop if we've reached the limit
                        if len(all_channels) >= limit:
                            break

                        channel_result = {
                            "id": channel_data["id"],
                            "displayName": channel_data["displayName"],
                            "description": channel_data.get("description"),
                            "membershipType": channel_data.get(
                                "membershipType", "standard"
                            ),
                            "teamId": team.id,
                            "teamName": team.displayName,
                        }
                        all_channels.append(channel_result)

                except Exception as team_error:
                    logger.warning(
                        f"Error getting channels for team '{team.displayName}': {team_error}"
                    )
                    continue

                # Stop if we've reached the limit
                if len(all_channels) >= limit:
                    break

            # Sort channels by team name, then by channel name
            all_channels.sort(
                key=lambda x: (x["teamName"].lower(), x["displayName"].lower())
            )

            # Apply final limit
            result_channels = all_channels[:limit]

            logger.info(
                f"Listed {len(result_channels)} total channels from {len(teams)} teams"
            )
            return result_channels

        except Exception as e:
            logger.error(f"Error listing all channels: {e}")
            return []

    async def send_activity_feed_notification(
        self,
        user_id: str,
        activity_type: str,
        preview_text: str,
        template_parameters: Optional[Dict[str, str]] = None,
        web_url: Optional[str] = None,
    ) -> bool:
        """
        Send an activity feed notification to a user.

        Args:
            user_id: Azure AD user ID
            activity_type: Type of activity (e.g., "datasetUpdated", "entityChanged")
            preview_text: Preview text for the notification
            template_parameters: Optional template parameters for the notification
            web_url: Optional URL to make the notification clickable

        Returns:
            True if notification was sent successfully, False otherwise
        """
        try:
            if not self.config.app_details:
                logger.error("Teams app details not configured")
                return False

            app_id = self.config.app_details.app_id
            if not app_id:
                logger.error("Teams app ID not configured")
                return False

            # Get access token
            if not self._access_token:
                self._access_token = await self._get_graph_access_token()

            headers = {
                "Authorization": f"Bearer {self._access_token}",
                "Content-Type": "application/json",
            }

            # Build notification payload
            payload: Dict[str, Any] = {
                "topic": {
                    "source": "entityUrl",
                    "value": f"https://graph.microsoft.com/v1.0/appCatalogs/teamsApps/{app_id}",
                },
                "activityType": activity_type,
                "previewText": {"content": preview_text},
                "recipient": {
                    "@odata.type": "microsoft.graph.aadUserNotificationRecipient",
                    "userId": user_id,
                },
            }

            # Add template parameters if provided
            if template_parameters:
                payload["templateParameters"] = [
                    {"name": key, "value": value}
                    for key, value in template_parameters.items()
                ]

            url = f"https://graph.microsoft.com/v1.0/users/{user_id}/teamwork/sendActivityNotification"

            async with httpx.AsyncClient() as client:
                response = await client.post(url, json=payload, headers=headers)

                if response.status_code == 401:
                    # Token might be expired, try refreshing once
                    logger.info("Graph API token expired, refreshing...")
                    self._access_token = await self._get_graph_access_token()
                    headers["Authorization"] = f"Bearer {self._access_token}"
                    response = await client.post(url, json=payload, headers=headers)

                if response.status_code in [200, 201, 202, 204]:
                    logger.info(
                        f"Successfully sent activity feed notification to user {user_id}"
                    )
                    return True
                else:
                    logger.error(
                        f"Failed to send activity feed notification: {response.status_code} {response.text}"
                    )
                    return False

        except Exception as e:
            logger.error(f"Error sending activity feed notification to {user_id}: {e}")
            return False
