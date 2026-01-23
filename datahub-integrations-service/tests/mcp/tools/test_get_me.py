"""Tests for get_me user information tool."""

from unittest.mock import MagicMock, patch

import pytest

from datahub_integrations.mcp.tools.get_me import get_me


@pytest.fixture
def mock_datahub_client():
    """Create a mock DataHub client."""
    mock_client = MagicMock()
    mock_client._graph = MagicMock()
    mock_client._graph.execute_graphql = MagicMock()
    return mock_client


def test_get_me_successful(mock_datahub_client):
    """Test successfully retrieving authenticated user information."""
    # Mock response with full user data
    mock_datahub_client._graph.execute_graphql.return_value = {
        "me": {
            "corpUser": {
                "type": "CORP_USER",
                "urn": "urn:li:corpuser:john.doe",
                "username": "john.doe",
                "info": {
                    "active": True,
                    "displayName": "John Doe",
                    "title": "Data Engineer",
                    "firstName": "John",
                    "lastName": "Doe",
                    "fullName": "John Doe",
                    "email": "john.doe@example.com",
                },
                "editableProperties": {
                    "displayName": "John Doe",
                    "title": "Data Engineer",
                    "pictureLink": "https://example.com/picture.jpg",
                    "teams": ["data-team", "engineering"],
                    "skills": ["Python", "SQL"],
                },
                "groups": {
                    "relationships": [
                        {
                            "entity": {
                                "urn": "urn:li:corpGroup:data-engineering",
                                "name": "data-engineering",
                                "properties": {"displayName": "Data Engineering"},
                            }
                        }
                    ]
                },
                "settings": {
                    "appearance": {
                        "showSimplifiedHomepage": False,
                        "showThemeV2": True,
                    },
                    "views": {"defaultView": {"urn": "urn:li:dataHubView:default"}},
                },
            },
            "platformPrivileges": {
                "viewAnalytics": True,
                "managePolicies": False,
                "viewMetadataProposals": True,
                "manageIdentities": False,
                "generatePersonalAccessTokens": True,
                "manageIngestion": True,
                "manageSecrets": False,
                "manageTokens": False,
                "manageDomains": True,
                "viewTests": True,
                "manageTests": False,
                "manageGlossaries": True,
                "manageUserCredentials": False,
                "manageTags": False,
                "viewManageTags": True,
                "createDomains": True,
                "createTags": True,
                "manageGlobalSettings": False,
                "manageGlobalViews": False,
                "manageOwnershipTypes": False,
                "manageGlobalAnnouncements": False,
                "createBusinessAttributes": False,
                "manageBusinessAttributes": False,
                "manageStructuredProperties": False,
                "viewStructuredPropertiesPage": True,
                "manageApplications": False,
                "manageFeatures": False,
                "manageHomePageTemplates": False,
                "manageDocumentationForms": False,
                "viewDocumentationFormsPage": True,
                "manageOrganizationDisplayPreferences": False,
                "proposeCreateGlossaryTerm": True,
                "proposeCreateGlossaryNode": True,
                "canViewIngestionPage": True,
                "createSupportTickets": True,
                "manageDocuments": False,
            },
        }
    }

    with patch(
        "datahub_integrations.mcp.mcp_server.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        result = get_me()

    assert result["success"] is True
    assert result["data"] is not None
    assert "Successfully retrieved authenticated user information" in result["message"]

    # Verify corpUser data
    corp_user = result["data"]["corpUser"]
    assert corp_user["urn"] == "urn:li:corpuser:john.doe"
    assert corp_user["username"] == "john.doe"
    assert corp_user["info"]["email"] == "john.doe@example.com"
    assert corp_user["info"]["fullName"] == "John Doe"

    # Verify platform privileges
    privileges = result["data"]["platformPrivileges"]
    assert privileges["viewAnalytics"] is True
    assert privileges["managePolicies"] is False
    assert privileges["manageIngestion"] is True

    # Verify GraphQL was called correctly
    assert mock_datahub_client._graph.execute_graphql.call_count == 1
    call_args = mock_datahub_client._graph.execute_graphql.call_args
    assert call_args.kwargs["operation_name"] == "getMe"
    assert "query getMe" in call_args.kwargs["query"]


def test_get_me_minimal_user_data(mock_datahub_client):
    """Test retrieving user information with minimal data."""
    # Mock response with minimal user data
    mock_datahub_client._graph.execute_graphql.return_value = {
        "me": {
            "corpUser": {
                "type": "CORP_USER",
                "urn": "urn:li:corpuser:minimal",
                "username": "minimal",
                "info": {
                    "active": True,
                    "displayName": "Minimal User",
                    "email": "minimal@example.com",
                },
                "groups": {"relationships": []},
            },
            "platformPrivileges": {
                "viewAnalytics": False,
                "managePolicies": False,
                "viewMetadataProposals": False,
                "manageIdentities": False,
                "generatePersonalAccessTokens": False,
                "manageIngestion": False,
                "manageSecrets": False,
                "manageTokens": False,
                "manageDomains": False,
                "viewTests": False,
                "manageTests": False,
                "manageGlossaries": False,
                "manageUserCredentials": False,
                "manageTags": False,
                "viewManageTags": False,
                "createDomains": False,
                "createTags": False,
                "manageGlobalSettings": False,
                "manageGlobalViews": False,
                "manageOwnershipTypes": False,
                "manageGlobalAnnouncements": False,
                "createBusinessAttributes": False,
                "manageBusinessAttributes": False,
                "manageStructuredProperties": False,
                "viewStructuredPropertiesPage": False,
                "manageApplications": False,
                "manageFeatures": False,
                "manageHomePageTemplates": False,
                "manageDocumentationForms": False,
                "viewDocumentationFormsPage": False,
                "manageOrganizationDisplayPreferences": False,
                "proposeCreateGlossaryTerm": False,
                "proposeCreateGlossaryNode": False,
                "canViewIngestionPage": False,
                "createSupportTickets": False,
                "manageDocuments": False,
            },
        }
    }

    with patch(
        "datahub_integrations.mcp.mcp_server.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        result = get_me()

    assert result["success"] is True
    assert result["data"] is not None
    assert result["data"]["corpUser"]["username"] == "minimal"
    assert len(result["data"]["corpUser"]["groups"]["relationships"]) == 0


def test_get_me_user_with_multiple_groups(mock_datahub_client):
    """Test retrieving user with multiple group memberships."""
    mock_datahub_client._graph.execute_graphql.return_value = {
        "me": {
            "corpUser": {
                "type": "CORP_USER",
                "urn": "urn:li:corpuser:multi.group",
                "username": "multi.group",
                "info": {"active": True, "email": "multi.group@example.com"},
                "groups": {
                    "relationships": [
                        {
                            "entity": {
                                "urn": "urn:li:corpGroup:data-engineering",
                                "name": "data-engineering",
                                "properties": {"displayName": "Data Engineering"},
                            }
                        },
                        {
                            "entity": {
                                "urn": "urn:li:corpGroup:analytics",
                                "name": "analytics",
                                "properties": {"displayName": "Analytics Team"},
                            }
                        },
                        {
                            "entity": {
                                "urn": "urn:li:corpGroup:platform",
                                "name": "platform",
                                "properties": {"displayName": "Platform Team"},
                            }
                        },
                    ]
                },
            },
            "platformPrivileges": {
                "viewAnalytics": True,
                "managePolicies": True,
                "viewMetadataProposals": True,
                "manageIdentities": True,
                "generatePersonalAccessTokens": True,
                "manageIngestion": True,
                "manageSecrets": True,
                "manageTokens": True,
                "manageDomains": True,
                "viewTests": True,
                "manageTests": True,
                "manageGlossaries": True,
                "manageUserCredentials": True,
                "manageTags": True,
                "viewManageTags": True,
                "createDomains": True,
                "createTags": True,
                "manageGlobalSettings": True,
                "manageGlobalViews": True,
                "manageOwnershipTypes": True,
                "manageGlobalAnnouncements": True,
                "createBusinessAttributes": True,
                "manageBusinessAttributes": True,
                "manageStructuredProperties": True,
                "viewStructuredPropertiesPage": True,
                "manageApplications": True,
                "manageFeatures": True,
                "manageHomePageTemplates": True,
                "manageDocumentationForms": True,
                "viewDocumentationFormsPage": True,
                "manageOrganizationDisplayPreferences": True,
                "proposeCreateGlossaryTerm": True,
                "proposeCreateGlossaryNode": True,
                "canViewIngestionPage": True,
                "createSupportTickets": True,
                "manageDocuments": True,
            },
        }
    }

    with patch(
        "datahub_integrations.mcp.mcp_server.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        result = get_me()

    assert result["success"] is True
    groups = result["data"]["corpUser"]["groups"]["relationships"]
    assert len(groups) == 3
    group_names = [g["entity"]["name"] for g in groups]
    assert "data-engineering" in group_names
    assert "analytics" in group_names
    assert "platform" in group_names


def test_get_me_no_authenticated_user(mock_datahub_client):
    """Test when no authenticated user is found (e.g., invalid token)."""
    # Mock response with no me data
    mock_datahub_client._graph.execute_graphql.return_value = {"me": None}

    with patch(
        "datahub_integrations.mcp.mcp_server.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        with pytest.raises(RuntimeError, match="No authenticated user found"):
            get_me()


def test_get_me_graphql_exception(mock_datahub_client):
    """Test handling of GraphQL execution errors."""
    # Mock GraphQL exception
    mock_datahub_client._graph.execute_graphql.side_effect = Exception(
        "Authentication failed"
    )

    with patch(
        "datahub_integrations.mcp.mcp_server.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        with pytest.raises(RuntimeError, match="Authentication failed"):
            get_me()


def test_get_me_network_error(mock_datahub_client):
    """Test handling of network errors during GraphQL call."""
    # Mock network error
    mock_datahub_client._graph.execute_graphql.side_effect = ConnectionError(
        "Network unreachable"
    )

    with patch(
        "datahub_integrations.mcp.mcp_server.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        with pytest.raises(RuntimeError, match="Network unreachable"):
            get_me()


def test_get_me_admin_user(mock_datahub_client):
    """Test retrieving information for an admin user with all privileges."""
    mock_datahub_client._graph.execute_graphql.return_value = {
        "me": {
            "corpUser": {
                "type": "CORP_USER",
                "urn": "urn:li:corpuser:admin",
                "username": "admin",
                "info": {
                    "active": True,
                    "displayName": "Administrator",
                    "email": "admin@example.com",
                    "fullName": "System Administrator",
                },
                "groups": {
                    "relationships": [
                        {
                            "entity": {
                                "urn": "urn:li:corpGroup:admins",
                                "name": "admins",
                                "properties": {"displayName": "Administrators"},
                            }
                        }
                    ]
                },
            },
            "platformPrivileges": {
                "viewAnalytics": True,
                "managePolicies": True,
                "viewMetadataProposals": True,
                "manageIdentities": True,
                "generatePersonalAccessTokens": True,
                "manageIngestion": True,
                "manageSecrets": True,
                "manageTokens": True,
                "manageDomains": True,
                "viewTests": True,
                "manageTests": True,
                "manageGlossaries": True,
                "manageUserCredentials": True,
                "manageTags": True,
                "viewManageTags": True,
                "createDomains": True,
                "createTags": True,
                "manageGlobalSettings": True,
                "manageGlobalViews": True,
                "manageOwnershipTypes": True,
                "manageGlobalAnnouncements": True,
                "createBusinessAttributes": True,
                "manageBusinessAttributes": True,
                "manageStructuredProperties": True,
                "viewStructuredPropertiesPage": True,
                "manageApplications": True,
                "manageFeatures": True,
                "manageHomePageTemplates": True,
                "manageDocumentationForms": True,
                "viewDocumentationFormsPage": True,
                "manageOrganizationDisplayPreferences": True,
                "proposeCreateGlossaryTerm": True,
                "proposeCreateGlossaryNode": True,
                "canViewIngestionPage": True,
                "createSupportTickets": True,
                "manageDocuments": True,
            },
        }
    }

    with patch(
        "datahub_integrations.mcp.mcp_server.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        result = get_me()

    assert result["success"] is True
    privileges = result["data"]["platformPrivileges"]

    # Verify all critical admin privileges are True
    assert privileges["managePolicies"] is True
    assert privileges["manageIdentities"] is True
    assert privileges["manageSecrets"] is True
    assert privileges["manageTokens"] is True
    assert privileges["manageGlobalSettings"] is True


def test_get_me_readonly_user(mock_datahub_client):
    """Test retrieving information for a read-only user with minimal privileges."""
    mock_datahub_client._graph.execute_graphql.return_value = {
        "me": {
            "corpUser": {
                "type": "CORP_USER",
                "urn": "urn:li:corpuser:readonly",
                "username": "readonly",
                "info": {
                    "active": True,
                    "displayName": "Read Only User",
                    "email": "readonly@example.com",
                },
                "groups": {"relationships": []},
            },
            "platformPrivileges": {
                "viewAnalytics": True,
                "managePolicies": False,
                "viewMetadataProposals": True,
                "manageIdentities": False,
                "generatePersonalAccessTokens": False,
                "manageIngestion": False,
                "manageSecrets": False,
                "manageTokens": False,
                "manageDomains": False,
                "viewTests": True,
                "manageTests": False,
                "manageGlossaries": False,
                "manageUserCredentials": False,
                "manageTags": False,
                "viewManageTags": True,
                "createDomains": False,
                "createTags": False,
                "manageGlobalSettings": False,
                "manageGlobalViews": False,
                "manageOwnershipTypes": False,
                "manageGlobalAnnouncements": False,
                "createBusinessAttributes": False,
                "manageBusinessAttributes": False,
                "manageStructuredProperties": False,
                "viewStructuredPropertiesPage": True,
                "manageApplications": False,
                "manageFeatures": False,
                "manageHomePageTemplates": False,
                "manageDocumentationForms": False,
                "viewDocumentationFormsPage": True,
                "manageOrganizationDisplayPreferences": False,
                "proposeCreateGlossaryTerm": False,
                "proposeCreateGlossaryNode": False,
                "canViewIngestionPage": False,
                "createSupportTickets": False,
                "manageDocuments": False,
            },
        }
    }

    with patch(
        "datahub_integrations.mcp.mcp_server.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        result = get_me()

    assert result["success"] is True
    privileges = result["data"]["platformPrivileges"]

    # Verify read-only user has view permissions but not manage permissions
    assert privileges["viewAnalytics"] is True
    assert privileges["viewTests"] is True
    assert privileges["managePolicies"] is False
    assert privileges["manageIdentities"] is False
    assert privileges["manageSecrets"] is False
    assert privileges["manageTags"] is False
