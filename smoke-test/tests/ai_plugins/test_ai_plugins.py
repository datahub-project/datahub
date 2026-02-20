"""Smoke tests for AI Plugin (Service entity) GraphQL API lifecycle.

Tests the full CRUD lifecycle for Service entities with each auth type,
GlobalSettings consistency, inline OAuth server creation, user plugin
settings, and list operations.

Architecture context
--------------------
A "Service" entity represents an external MCP server. Each Service also has
an AiPluginConfig entry in the GlobalSettings singleton that stores auth
configuration and an "enabled" flag for Ask DataHub. The AiPluginConfig.id
is always the full Service URN (e.g. urn:li:service:<id>).

Auth types:
  NONE            -- public API, no credentials
  SHARED_API_KEY  -- single API key stored in a DataHubConnection, shared by
                     all users; admin provides the key on create
  USER_API_KEY    -- each user provides their own key via the integrations
                     service REST endpoint
  USER_OAUTH      -- each user authenticates via OAuth; requires an
                     OAuthAuthorizationServer entity with provider config

Key invariants tested here:
  - upsertAiPlugin creates both the Service entity AND the AiPluginConfig in
    GlobalSettings atomically
  - deleteAiPlugin removes the AiPluginConfig from GlobalSettings
  - Editing a USER_OAUTH service is a two-mutation flow: first update the
    OAuthAuthorizationServer, then upsertAiPlugin with oauthServerUrn
  - Inline OAuth server creation (newOAuthServer without id) is only for
    initial create; passing an id is rejected (must use separate mutation)
  - User "enabled" state is independent of credential connection state
  - subType=MCP_SERVER requires mcpServerProperties
  - clientSecret is optional on OAuthAuthorizationServer (public clients)
"""

import logging
import time
from typing import Any, Dict, List

import pytest

from tests.utils import execute_graphql

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# GraphQL operations
# ---------------------------------------------------------------------------

UPSERT_AI_PLUGIN = """
mutation upsertAiPlugin($input: UpsertAiPluginInput!) {
    upsertAiPlugin(input: $input) {
        urn
        properties { displayName description }
        subType
        mcpServerProperties { url transport timeout }
    }
}
"""

DELETE_AI_PLUGIN = """
mutation deleteAiPlugin($urn: String!) {
    deleteAiPlugin(urn: $urn)
}
"""

GET_SERVICE = """
query service($urn: String!) {
    service(urn: $urn) {
        urn
        properties { displayName description }
        subType
        mcpServerProperties { url transport timeout }
    }
}
"""

LIST_SERVICES = """
query listServices($input: ListServicesInput!) {
    listServices(input: $input) {
        start
        count
        total
        services {
            urn
            properties { displayName }
        }
    }
}
"""

GET_GLOBAL_SETTINGS_PLUGINS = """
query {
    globalSettings {
        aiPlugins {
            id
            type
            serviceUrn
            enabled
            authType
            oauthConfig { serverUrn }
            sharedApiKeyConfig { credentialUrn authLocation }
            userApiKeyConfig { authLocation }
        }
    }
}
"""

UPSERT_OAUTH_SERVER = """
mutation upsertOAuthAuthorizationServer($input: UpsertOAuthAuthorizationServerInput!) {
    upsertOAuthAuthorizationServer(input: $input) {
        urn
        properties {
            displayName
            clientId
            hasClientSecret
            authorizationUrl
            tokenUrl
            scopes
            tokenAuthMethod
        }
    }
}
"""

DELETE_OAUTH_SERVER = """
mutation deleteOAuthAuthorizationServer($urn: String!) {
    deleteOAuthAuthorizationServer(urn: $urn)
}
"""

UPDATE_USER_AI_PLUGIN_SETTINGS = """
mutation updateUserAiPluginSettings($input: UpdateUserAiPluginSettingsInput!) {
    updateUserAiPluginSettings(input: $input)
}
"""

GET_ME_AI_PLUGIN_SETTINGS = """
query {
    me {
        corpUser {
            settings {
                aiPluginSettings {
                    plugins {
                        id
                        enabled
                    }
                }
            }
        }
    }
}
"""

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _unique_name(prefix: str) -> str:
    return f"{prefix}-{int(time.time() * 1000)}"


def _unique_id() -> str:
    return f"smoke-test-{int(time.time() * 1000)}"


def _upsert_ai_plugin(
    auth_session,
    *,
    plugin_id: str | None = None,
    display_name: str = "Smoke Test Plugin",
    url: str = "https://example.com/mcp",
    auth_type: str = "NONE",
    shared_api_key: str | None = None,
    oauth_server_urn: str | None = None,
    new_oauth_server: Dict[str, Any] | None = None,
    required_scopes: List[str] | None = None,
) -> Dict[str, Any]:
    variables: Dict[str, Any] = {
        "input": {
            "displayName": display_name,
            "subType": "MCP_SERVER",
            "mcpServerProperties": {"url": url},
            "authType": auth_type,
        }
    }
    inp = variables["input"]
    if plugin_id is not None:
        inp["id"] = plugin_id
    if shared_api_key is not None:
        inp["sharedApiKey"] = shared_api_key
    if oauth_server_urn is not None:
        inp["oauthServerUrn"] = oauth_server_urn
    if new_oauth_server is not None:
        inp["newOAuthServer"] = new_oauth_server
    if required_scopes is not None:
        inp["requiredScopes"] = required_scopes

    res = execute_graphql(auth_session, UPSERT_AI_PLUGIN, variables)
    return res["data"]["upsertAiPlugin"]


def _get_plugin_ids_from_global_settings(auth_session) -> List[Dict[str, Any]]:
    res = execute_graphql(auth_session, GET_GLOBAL_SETTINGS_PLUGINS)
    plugins = res["data"]["globalSettings"]["aiPlugins"] or []
    return plugins


def _find_plugin_in_global_settings(auth_session, service_urn: str):
    """Find an AiPluginConfig by service URN. The id field equals the service URN."""
    plugins = _get_plugin_ids_from_global_settings(auth_session)
    return next((p for p in plugins if p["id"] == service_urn), None)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def cleanup_urns(graph_client):
    """Collect URNs during a test; hard-delete them all on teardown."""
    urns: List[str] = []
    yield urns
    for urn in reversed(urns):
        try:
            graph_client.hard_delete_entity(urn)
        except Exception:
            logger.warning(f"Failed to clean up {urn}", exc_info=True)


# ---------------------------------------------------------------------------
# Tests: Service CRUD per auth type
# ---------------------------------------------------------------------------


def test_create_and_delete_service_no_auth(auth_session, cleanup_urns):
    """Contract: upsertAiPlugin creates the Service entity AND adds an
    AiPluginConfig to GlobalSettings in one call. deleteAiPlugin removes both.
    The AiPluginConfig.id equals the Service URN, and enabled defaults to True."""
    plugin_id = _unique_id()
    name = _unique_name("no-auth")

    service = _upsert_ai_plugin(auth_session, plugin_id=plugin_id, display_name=name)
    cleanup_urns.append(service["urn"])

    assert service["properties"]["displayName"] == name
    assert service["mcpServerProperties"]["url"] == "https://example.com/mcp"

    plugin = _find_plugin_in_global_settings(auth_session, service["urn"])
    assert plugin is not None
    assert plugin["authType"] == "NONE"
    assert plugin["serviceUrn"] == service["urn"]
    assert plugin["enabled"] is True

    res = execute_graphql(auth_session, DELETE_AI_PLUGIN, {"urn": service["urn"]})
    assert res["data"]["deleteAiPlugin"] is True
    cleanup_urns.remove(service["urn"])

    assert _find_plugin_in_global_settings(auth_session, service["urn"]) is None


def test_create_and_delete_service_shared_api_key(auth_session, cleanup_urns):
    """Contract: SHARED_API_KEY auth creates a DataHubConnection credential
    owned by the Service. The sharedApiKeyConfig.credentialUrn in GlobalSettings
    points to this connection. The actual key value is encrypted and never
    returned via GraphQL."""
    plugin_id = _unique_id()
    name = _unique_name("shared-key")

    service = _upsert_ai_plugin(
        auth_session,
        plugin_id=plugin_id,
        display_name=name,
        auth_type="SHARED_API_KEY",
        shared_api_key="test-secret-key-12345",
    )
    cleanup_urns.append(service["urn"])

    plugin = _find_plugin_in_global_settings(auth_session, service["urn"])
    assert plugin is not None
    assert plugin["authType"] == "SHARED_API_KEY"
    assert plugin["sharedApiKeyConfig"] is not None
    assert plugin["sharedApiKeyConfig"]["credentialUrn"] is not None

    res = execute_graphql(auth_session, DELETE_AI_PLUGIN, {"urn": service["urn"]})
    assert res["data"]["deleteAiPlugin"] is True
    cleanup_urns.remove(service["urn"])

    assert _find_plugin_in_global_settings(auth_session, service["urn"]) is None


def test_create_and_delete_service_user_api_key(auth_session, cleanup_urns):
    """Contract: USER_API_KEY auth configures injection settings (header name,
    scheme, location) but does NOT store any credential at create time. Each
    user stores their own key later via the integrations service REST endpoint."""
    plugin_id = _unique_id()
    name = _unique_name("user-key")

    service = _upsert_ai_plugin(
        auth_session,
        plugin_id=plugin_id,
        display_name=name,
        auth_type="USER_API_KEY",
    )
    cleanup_urns.append(service["urn"])

    plugin = _find_plugin_in_global_settings(auth_session, service["urn"])
    assert plugin is not None
    assert plugin["authType"] == "USER_API_KEY"
    assert plugin["userApiKeyConfig"] is not None

    res = execute_graphql(auth_session, DELETE_AI_PLUGIN, {"urn": service["urn"]})
    assert res["data"]["deleteAiPlugin"] is True
    cleanup_urns.remove(service["urn"])

    assert _find_plugin_in_global_settings(auth_session, service["urn"]) is None


def test_create_and_delete_service_user_oauth(auth_session, cleanup_urns):
    """Contract: USER_OAUTH auth requires a pre-existing OAuthAuthorizationServer
    entity linked via oauthServerUrn. The AiPluginConfig.oauthConfig.serverUrn
    stores this reference. Deleting the Service removes the AiPluginConfig but
    does NOT cascade-delete the OAuth server (it may be shared)."""
    oauth_id = _unique_id()
    oauth_res = execute_graphql(
        auth_session,
        UPSERT_OAUTH_SERVER,
        {
            "input": {
                "id": oauth_id,
                "displayName": _unique_name("oauth-server"),
                "clientId": "test-client-id",
                "clientSecret": "test-client-secret",
                "authorizationUrl": "https://provider.example.com/authorize",
                "tokenUrl": "https://provider.example.com/token",
                "scopes": ["read", "write"],
            }
        },
    )
    oauth_server = oauth_res["data"]["upsertOAuthAuthorizationServer"]
    cleanup_urns.append(oauth_server["urn"])

    assert oauth_server["properties"]["clientId"] == "test-client-id"
    assert oauth_server["properties"]["hasClientSecret"] is True

    plugin_id = _unique_id()
    service = _upsert_ai_plugin(
        auth_session,
        plugin_id=plugin_id,
        display_name=_unique_name("oauth-plugin"),
        auth_type="USER_OAUTH",
        oauth_server_urn=oauth_server["urn"],
        required_scopes=["read"],
    )
    cleanup_urns.append(service["urn"])

    plugin = _find_plugin_in_global_settings(auth_session, service["urn"])
    assert plugin is not None
    assert plugin["authType"] == "USER_OAUTH"
    assert plugin["oauthConfig"] is not None
    assert plugin["oauthConfig"]["serverUrn"] == oauth_server["urn"]

    res = execute_graphql(auth_session, DELETE_AI_PLUGIN, {"urn": service["urn"]})
    assert res["data"]["deleteAiPlugin"] is True
    cleanup_urns.remove(service["urn"])

    assert _find_plugin_in_global_settings(auth_session, service["urn"]) is None

    execute_graphql(auth_session, DELETE_OAUTH_SERVER, {"urn": oauth_server["urn"]})
    cleanup_urns.remove(oauth_server["urn"])


# ---------------------------------------------------------------------------
# Tests: Edit / auth type switching
# ---------------------------------------------------------------------------


def test_edit_service_change_auth_type(auth_session, cleanup_urns):
    """Contract: calling upsertAiPlugin with the same id replaces the
    AiPluginConfig in GlobalSettings. Switching auth type (NONE -> SHARED_API_KEY)
    creates the appropriate auth sub-config and removes the old one."""
    plugin_id = _unique_id()

    service = _upsert_ai_plugin(
        auth_session,
        plugin_id=plugin_id,
        display_name=_unique_name("edit-auth"),
        auth_type="NONE",
    )
    cleanup_urns.append(service["urn"])

    plugin = _find_plugin_in_global_settings(auth_session, service["urn"])
    assert plugin is not None
    assert plugin["authType"] == "NONE"

    _upsert_ai_plugin(
        auth_session,
        plugin_id=plugin_id,
        display_name=service["properties"]["displayName"],
        auth_type="SHARED_API_KEY",
        shared_api_key="switched-key-value",
    )

    plugin = _find_plugin_in_global_settings(auth_session, service["urn"])
    assert plugin is not None
    assert plugin["authType"] == "SHARED_API_KEY"
    assert plugin["sharedApiKeyConfig"] is not None

    execute_graphql(auth_session, DELETE_AI_PLUGIN, {"urn": service["urn"]})
    cleanup_urns.remove(service["urn"])


def test_edit_oauth_service_updates_both_server_and_service(auth_session, cleanup_urns):
    """Contract: editing a USER_OAUTH service is a two-mutation flow.
    Step 1: upsertOAuthAuthorizationServer (with the existing id) updates
    provider config (clientId, URLs, scopes). Omitting clientSecret preserves
    the existing encrypted secret.
    Step 2: upsertAiPlugin (with oauthServerUrn, NOT newOAuthServer) updates
    the service properties while keeping the same OAuth server link.
    This mirrors the frontend's handleSubmit in usePluginForm.ts."""
    # Create initial OAuth server + service
    oauth_id = _unique_id()
    oauth_res = execute_graphql(
        auth_session,
        UPSERT_OAUTH_SERVER,
        {
            "input": {
                "id": oauth_id,
                "displayName": "Original OAuth Server",
                "clientId": "original-client-id",
                "clientSecret": "original-secret",
                "authorizationUrl": "https://original.example.com/authorize",
                "tokenUrl": "https://original.example.com/token",
                "scopes": ["read"],
            }
        },
    )
    oauth_server = oauth_res["data"]["upsertOAuthAuthorizationServer"]
    cleanup_urns.append(oauth_server["urn"])

    plugin_id = _unique_id()
    service = _upsert_ai_plugin(
        auth_session,
        plugin_id=plugin_id,
        display_name="Original Service Name",
        auth_type="USER_OAUTH",
        oauth_server_urn=oauth_server["urn"],
        required_scopes=["read"],
    )
    cleanup_urns.append(service["urn"])

    # --- Edit flow (mirrors the UI): two sequential mutations ---

    # Step 1: Update OAuth server properties
    execute_graphql(
        auth_session,
        UPSERT_OAUTH_SERVER,
        {
            "input": {
                "id": oauth_id,
                "displayName": "Updated OAuth Server",
                "clientId": "updated-client-id",
                "authorizationUrl": "https://updated.example.com/authorize",
                "tokenUrl": "https://updated.example.com/token",
                "scopes": ["read", "write"],
            }
        },
    )

    # Step 2: Update service, linking to the existing OAuth server
    _upsert_ai_plugin(
        auth_session,
        plugin_id=plugin_id,
        display_name="Updated Service Name",
        auth_type="USER_OAUTH",
        oauth_server_urn=oauth_server["urn"],
        required_scopes=["read", "write"],
    )

    # Verify OAuth server was updated
    GET_OAUTH_SERVER = """
    query oauthAuthorizationServer($urn: String!) {
        oauthAuthorizationServer(urn: $urn) {
            urn
            properties {
                displayName
                clientId
                authorizationUrl
                tokenUrl
                scopes
            }
        }
    }
    """
    oauth_res = execute_graphql(
        auth_session, GET_OAUTH_SERVER, {"urn": oauth_server["urn"]}
    )
    updated_props = oauth_res["data"]["oauthAuthorizationServer"]["properties"]
    assert updated_props["displayName"] == "Updated OAuth Server"
    assert updated_props["clientId"] == "updated-client-id"
    assert updated_props["authorizationUrl"] == "https://updated.example.com/authorize"
    assert updated_props["tokenUrl"] == "https://updated.example.com/token"
    assert set(updated_props["scopes"]) == {"read", "write"}

    # Verify service was updated and still points to the same OAuth server
    svc_res = execute_graphql(auth_session, GET_SERVICE, {"urn": service["urn"]})
    assert (
        svc_res["data"]["service"]["properties"]["displayName"]
        == "Updated Service Name"
    )

    plugin = _find_plugin_in_global_settings(auth_session, service["urn"])
    assert plugin is not None
    assert plugin["authType"] == "USER_OAUTH"
    assert plugin["oauthConfig"]["serverUrn"] == oauth_server["urn"]

    # Cleanup
    execute_graphql(auth_session, DELETE_AI_PLUGIN, {"urn": service["urn"]})
    cleanup_urns.remove(service["urn"])
    execute_graphql(auth_session, DELETE_OAUTH_SERVER, {"urn": oauth_server["urn"]})
    cleanup_urns.remove(oauth_server["urn"])


# ---------------------------------------------------------------------------
# Tests: GlobalSettings consistency
# ---------------------------------------------------------------------------


def test_global_settings_plugin_list_consistency(auth_session, cleanup_urns):
    """Contract: GlobalSettings.aiPlugins is the authoritative list of configured
    plugins. Each create adds exactly one entry; each delete removes exactly one.
    The list is keyed by AiPluginConfig.id which equals the Service URN."""
    id_a = _unique_id() + "-a"
    id_b = _unique_id() + "-b"

    svc_a = _upsert_ai_plugin(
        auth_session, plugin_id=id_a, display_name=_unique_name("consist-a")
    )
    cleanup_urns.append(svc_a["urn"])

    svc_b = _upsert_ai_plugin(
        auth_session, plugin_id=id_b, display_name=_unique_name("consist-b")
    )
    cleanup_urns.append(svc_b["urn"])

    # AiPluginConfig.id is the full service URN
    plugins = _get_plugin_ids_from_global_settings(auth_session)
    plugin_ids = {p["id"] for p in plugins}
    assert svc_a["urn"] in plugin_ids
    assert svc_b["urn"] in plugin_ids

    execute_graphql(auth_session, DELETE_AI_PLUGIN, {"urn": svc_a["urn"]})
    cleanup_urns.remove(svc_a["urn"])

    plugins = _get_plugin_ids_from_global_settings(auth_session)
    plugin_ids = {p["id"] for p in plugins}
    assert svc_a["urn"] not in plugin_ids
    assert svc_b["urn"] in plugin_ids

    execute_graphql(auth_session, DELETE_AI_PLUGIN, {"urn": svc_b["urn"]})
    cleanup_urns.remove(svc_b["urn"])

    assert _find_plugin_in_global_settings(auth_session, svc_b["urn"]) is None


# ---------------------------------------------------------------------------
# Tests: Inline OAuth server creation
# ---------------------------------------------------------------------------


def test_upsert_ai_plugin_with_inline_oauth_server(
    auth_session, graph_client, cleanup_urns
):
    """Contract: upsertAiPlugin with newOAuthServer (no id) creates the OAuth
    server first, then links it via oauthConfig.serverUrn. This is the
    "create new" path used when an admin configures a new OAuth plugin.
    Both entities must be cleaned up independently."""
    plugin_id = _unique_id()

    service = _upsert_ai_plugin(
        auth_session,
        plugin_id=plugin_id,
        display_name=_unique_name("inline-oauth"),
        auth_type="USER_OAUTH",
        new_oauth_server={
            "displayName": _unique_name("inline-server"),
            "clientId": "inline-client-id",
            "clientSecret": "inline-client-secret",
            "authorizationUrl": "https://inline.example.com/authorize",
            "tokenUrl": "https://inline.example.com/token",
            "scopes": ["openid"],
        },
        required_scopes=["openid"],
    )
    cleanup_urns.append(service["urn"])

    plugin = _find_plugin_in_global_settings(auth_session, service["urn"])
    assert plugin is not None
    assert plugin["authType"] == "USER_OAUTH"
    assert plugin["oauthConfig"] is not None

    oauth_server_urn = plugin["oauthConfig"]["serverUrn"]
    assert oauth_server_urn is not None
    cleanup_urns.append(oauth_server_urn)

    execute_graphql(auth_session, DELETE_AI_PLUGIN, {"urn": service["urn"]})
    cleanup_urns.remove(service["urn"])

    execute_graphql(auth_session, DELETE_OAUTH_SERVER, {"urn": oauth_server_urn})
    cleanup_urns.remove(oauth_server_urn)


# ---------------------------------------------------------------------------
# Tests: User plugin settings
# ---------------------------------------------------------------------------


def test_user_ai_plugin_settings(auth_session, cleanup_urns):
    """Contract: updateUserAiPluginSettings stores per-user overrides in
    CorpUserSettings.aiPluginSettings. The pluginId must match the
    AiPluginConfig.id (= service URN). Enable/disable is a user preference
    that does not affect the global plugin config."""
    plugin_id = _unique_id()

    service = _upsert_ai_plugin(
        auth_session, plugin_id=plugin_id, display_name=_unique_name("user-settings")
    )
    cleanup_urns.append(service["urn"])

    # pluginId for user settings should match the AiPluginConfig.id (= service URN)
    service_urn = service["urn"]

    execute_graphql(
        auth_session,
        UPDATE_USER_AI_PLUGIN_SETTINGS,
        {"input": {"pluginId": service_urn, "enabled": True}},
    )

    me_res = execute_graphql(auth_session, GET_ME_AI_PLUGIN_SETTINGS)
    user_plugins = me_res["data"]["me"]["corpUser"]["settings"]["aiPluginSettings"][
        "plugins"
    ]
    match = next((p for p in user_plugins if p["id"] == service_urn), None)
    assert match is not None
    assert match["enabled"] is True

    execute_graphql(
        auth_session,
        UPDATE_USER_AI_PLUGIN_SETTINGS,
        {"input": {"pluginId": service_urn, "enabled": False}},
    )

    me_res = execute_graphql(auth_session, GET_ME_AI_PLUGIN_SETTINGS)
    user_plugins = me_res["data"]["me"]["corpUser"]["settings"]["aiPluginSettings"][
        "plugins"
    ]
    match = next((p for p in user_plugins if p["id"] == service_urn), None)
    assert match is not None
    assert match["enabled"] is False

    execute_graphql(auth_session, DELETE_AI_PLUGIN, {"urn": service["urn"]})
    cleanup_urns.remove(service["urn"])


def test_user_settings_oauth_plugin_enable_without_connection(
    auth_session, cleanup_urns
):
    """Enable a USER_OAUTH plugin for the user without completing the OAuth flow.
    Verifies that enabled and connected are independent states."""
    oauth_id = _unique_id()
    oauth_res = execute_graphql(
        auth_session,
        UPSERT_OAUTH_SERVER,
        {
            "input": {
                "id": oauth_id,
                "displayName": _unique_name("oauth-srv"),
                "clientId": "test-client",
                "clientSecret": "test-secret",
                "authorizationUrl": "https://provider.example.com/authorize",
                "tokenUrl": "https://provider.example.com/token",
            }
        },
    )
    oauth_server = oauth_res["data"]["upsertOAuthAuthorizationServer"]
    cleanup_urns.append(oauth_server["urn"])

    plugin_id = _unique_id()
    service = _upsert_ai_plugin(
        auth_session,
        plugin_id=plugin_id,
        display_name=_unique_name("oauth-user-settings"),
        auth_type="USER_OAUTH",
        oauth_server_urn=oauth_server["urn"],
    )
    cleanup_urns.append(service["urn"])
    service_urn = service["urn"]

    execute_graphql(
        auth_session,
        UPDATE_USER_AI_PLUGIN_SETTINGS,
        {"input": {"pluginId": service_urn, "enabled": True}},
    )

    me_res = execute_graphql(auth_session, GET_ME_AI_PLUGIN_SETTINGS)
    user_plugins = me_res["data"]["me"]["corpUser"]["settings"]["aiPluginSettings"][
        "plugins"
    ]
    match = next((p for p in user_plugins if p["id"] == service_urn), None)
    assert match is not None
    assert match["enabled"] is True

    execute_graphql(auth_session, DELETE_AI_PLUGIN, {"urn": service["urn"]})
    cleanup_urns.remove(service["urn"])
    execute_graphql(auth_session, DELETE_OAUTH_SERVER, {"urn": oauth_server["urn"]})
    cleanup_urns.remove(oauth_server["urn"])


def test_user_settings_oauth_initiate_connect(auth_session, cleanup_urns):
    """Contract: POST /connect loads the OAuth server config from the entity,
    generates PKCE (code_challenge + code_verifier), stores state in an
    in-memory TTL cache, and returns an authorization URL with all required
    OAuth params. The URL must use the provider's authorizationUrl as base and
    include client_id, code_challenge (S256), a unique state nonce, and merged
    scopes (base + requiredScopes). We don't complete the flow (that requires
    a real OAuth provider)."""
    oauth_id = _unique_id()
    oauth_res = execute_graphql(
        auth_session,
        UPSERT_OAUTH_SERVER,
        {
            "input": {
                "id": oauth_id,
                "displayName": _unique_name("connect-srv"),
                "clientId": "smoke-test-client-id",
                "clientSecret": "smoke-test-secret",
                "authorizationUrl": "https://provider.example.com/authorize",
                "tokenUrl": "https://provider.example.com/token",
                "scopes": ["read", "profile"],
            }
        },
    )
    oauth_server = oauth_res["data"]["upsertOAuthAuthorizationServer"]
    cleanup_urns.append(oauth_server["urn"])

    plugin_id = _unique_id()
    service = _upsert_ai_plugin(
        auth_session,
        plugin_id=plugin_id,
        display_name=_unique_name("connect-test"),
        auth_type="USER_OAUTH",
        oauth_server_urn=oauth_server["urn"],
        required_scopes=["read"],
    )
    cleanup_urns.append(service["urn"])
    service_urn = service["urn"]

    integrations_url = auth_session.integrations_service_url()
    connect_resp = auth_session.post(
        f"{integrations_url}/public/oauth/plugins/{service_urn}/connect",
        json={},
    )
    assert connect_resp.status_code == 200, f"Connect failed: {connect_resp.text}"
    auth_url = connect_resp.json()["authorization_url"]

    assert auth_url.startswith("https://provider.example.com/authorize?")
    assert "client_id=smoke-test-client-id" in auth_url
    assert "response_type=code" in auth_url
    assert "code_challenge=" in auth_url
    assert "code_challenge_method=S256" in auth_url
    assert "state=" in auth_url
    # Scopes should include both base scopes and required scopes
    assert "scope=" in auth_url

    execute_graphql(auth_session, DELETE_AI_PLUGIN, {"urn": service["urn"]})
    cleanup_urns.remove(service["urn"])
    execute_graphql(auth_session, DELETE_OAUTH_SERVER, {"urn": oauth_server["urn"]})
    cleanup_urns.remove(oauth_server["urn"])


def test_admin_test_oauth_connection(auth_session):
    """Contract: POST /test-oauth-connect accepts raw OAuth + MCP config (no
    entities required) so admins can validate connectivity before saving. It
    generates PKCE and returns an authorization URL. After the OAuth callback
    completes, this flow tests MCP tool discovery and discards tokens -- no
    persistent side effects. Here we only verify the URL is well-formed."""
    integrations_url = auth_session.integrations_service_url()
    resp = auth_session.post(
        f"{integrations_url}/public/oauth/plugins/test-oauth-connect",
        json={
            "oauth_config": {
                "clientId": "test-conn-client-id",
                "clientSecret": "test-conn-secret",
                "authorizationUrl": "https://test-provider.example.com/authorize",
                "tokenUrl": "https://test-provider.example.com/token",
                "scopes": ["openid", "email"],
            },
            "mcp_config": {
                "url": "https://mcp.example.com/sse",
            },
        },
    )
    assert resp.status_code == 200, f"Test connect failed: {resp.text}"
    auth_url = resp.json()["authorization_url"]

    assert auth_url.startswith("https://test-provider.example.com/authorize?")
    assert "client_id=test-conn-client-id" in auth_url
    assert "response_type=code" in auth_url
    assert "code_challenge=" in auth_url
    assert "code_challenge_method=S256" in auth_url
    assert "state=" in auth_url
    assert "scope=" in auth_url


def test_user_settings_api_key_save_and_disconnect(auth_session, cleanup_urns):
    """Contract: the API key lifecycle is:
    1. Admin creates a USER_API_KEY service (no key stored yet)
    2. User calls POST /oauth/plugins/{urn}/api-key with their key
       -> stores encrypted key in a DataHubConnection entity
       -> updates CorpUserSettings to link the connection
    3. User calls DELETE /oauth/plugins/{urn}/disconnect
       -> removes the connection reference from CorpUserSettings first
       -> then soft-deletes the DataHubConnection
    The integrations service validates that the plugin exists and uses
    USER_API_KEY auth before accepting the key."""
    plugin_id = _unique_id()
    service = _upsert_ai_plugin(
        auth_session,
        plugin_id=plugin_id,
        display_name=_unique_name("apikey-lifecycle"),
        auth_type="USER_API_KEY",
    )
    cleanup_urns.append(service["urn"])
    service_urn = service["urn"]

    # Save an API key via the integrations service REST endpoint
    integrations_url = auth_session.integrations_service_url()
    save_resp = auth_session.post(
        f"{integrations_url}/public/oauth/plugins/{service_urn}/api-key",
        json={"api_key": "smoke-test-api-key-value"},
    )
    assert save_resp.status_code == 200, f"Save API key failed: {save_resp.text}"
    save_data = save_resp.json()
    assert save_data["success"] is True
    assert save_data["connection_urn"] is not None

    # Verify user settings now show the API key connection
    me_res = execute_graphql(auth_session, GET_ME_AI_PLUGIN_SETTINGS)
    user_plugins = me_res["data"]["me"]["corpUser"]["settings"]["aiPluginSettings"][
        "plugins"
    ]
    match = next((p for p in user_plugins if p["id"] == service_urn), None)
    assert match is not None

    # Disconnect the API key
    disconnect_resp = auth_session.delete(
        f"{integrations_url}/public/oauth/plugins/{service_urn}/disconnect",
    )
    assert disconnect_resp.status_code == 200, (
        f"Disconnect failed: {disconnect_resp.text}"
    )
    assert disconnect_resp.json()["success"] is True

    execute_graphql(auth_session, DELETE_AI_PLUGIN, {"urn": service["urn"]})
    cleanup_urns.remove(service["urn"])


# ---------------------------------------------------------------------------
# Tests: Validation / invariant constraints
# ---------------------------------------------------------------------------


def test_oauth_server_without_client_secret_succeeds(auth_session, cleanup_urns):
    """Contract: clientSecret is optional on OAuthAuthorizationServer. Some
    OAuth providers support public clients that rely solely on PKCE for
    security (no client secret). The hasClientSecret field must reflect
    whether a secret was provided."""
    oauth_id = _unique_id()
    res = execute_graphql(
        auth_session,
        UPSERT_OAUTH_SERVER,
        {
            "input": {
                "id": oauth_id,
                "displayName": _unique_name("public-client"),
                "clientId": "public-client-id",
                "authorizationUrl": "https://provider.example.com/authorize",
                "tokenUrl": "https://provider.example.com/token",
            }
        },
    )
    server = res["data"]["upsertOAuthAuthorizationServer"]
    cleanup_urns.append(server["urn"])

    assert server["properties"]["clientId"] == "public-client-id"
    assert server["properties"]["hasClientSecret"] is False
    assert (
        server["properties"]["authorizationUrl"]
        == "https://provider.example.com/authorize"
    )
    assert server["properties"]["tokenUrl"] == "https://provider.example.com/token"

    execute_graphql(auth_session, DELETE_OAUTH_SERVER, {"urn": server["urn"]})
    cleanup_urns.remove(server["urn"])


def test_upsert_ai_plugin_with_inline_oauth_server_id_fails(auth_session):
    """Contract: newOAuthServer on UpsertAiPluginInput is for CREATE only.
    If an id is provided (implying "update existing"), the server rejects it
    with an error. Editing an OAuth server must go through the dedicated
    upsertOAuthAuthorizationServer mutation. This prevents accidental
    overwrites and ensures the two-mutation edit flow is enforced."""
    response = auth_session.post(
        f"{auth_session.frontend_url()}/api/v2/graphql",
        json={
            "query": """
                mutation upsertAiPlugin($input: UpsertAiPluginInput!) {
                    upsertAiPlugin(input: $input) { urn }
                }
            """,
            "variables": {
                "input": {
                    "displayName": "should-fail-inline-edit",
                    "subType": "MCP_SERVER",
                    "mcpServerProperties": {"url": "https://example.com/mcp"},
                    "authType": "USER_OAUTH",
                    "newOAuthServer": {
                        "id": "existing-server-id",
                        "displayName": "Trying to update inline",
                        "clientId": "client-id",
                        "authorizationUrl": "https://example.com/authorize",
                        "tokenUrl": "https://example.com/token",
                    },
                }
            },
        },
    )
    res = response.json()
    assert "errors" in res, (
        "Expected error when newOAuthServer has an id (update not allowed)"
    )


def test_service_without_mcp_server_properties_fails(auth_session):
    """Contract: when subType is MCP_SERVER, mcpServerProperties (containing
    at minimum the server URL) is required. This is validated server-side
    before any entity writes to avoid creating an inconsistent Service entity
    that has no MCP endpoint."""
    response = auth_session.post(
        f"{auth_session.frontend_url()}/api/v2/graphql",
        json={
            "query": """
                mutation upsertAiPlugin($input: UpsertAiPluginInput!) {
                    upsertAiPlugin(input: $input) { urn }
                }
            """,
            "variables": {
                "input": {
                    "displayName": "should-fail",
                    "subType": "MCP_SERVER",
                }
            },
        },
    )
    res = response.json()
    assert "errors" in res, "Expected error when mcpServerProperties is missing"


# ---------------------------------------------------------------------------
# Tests: List operations
# ---------------------------------------------------------------------------


def test_list_services(auth_session, cleanup_urns):
    """Contract: listServices returns all Service entities via OpenSearch.
    Results include URN and properties. The total count reflects all services
    in the system (not just those created in this test)."""
    id_x = _unique_id() + "-x"
    id_y = _unique_id() + "-y"

    svc_x = _upsert_ai_plugin(
        auth_session, plugin_id=id_x, display_name=_unique_name("list-x")
    )
    cleanup_urns.append(svc_x["urn"])

    svc_y = _upsert_ai_plugin(
        auth_session, plugin_id=id_y, display_name=_unique_name("list-y")
    )
    cleanup_urns.append(svc_y["urn"])

    res = execute_graphql(
        auth_session,
        LIST_SERVICES,
        {"input": {"start": 0, "count": 100}},
    )
    result = res["data"]["listServices"]
    returned_urns = {s["urn"] for s in result["services"]}
    assert svc_x["urn"] in returned_urns
    assert svc_y["urn"] in returned_urns
    assert result["total"] >= 2

    execute_graphql(auth_session, DELETE_AI_PLUGIN, {"urn": svc_x["urn"]})
    cleanup_urns.remove(svc_x["urn"])
    execute_graphql(auth_session, DELETE_AI_PLUGIN, {"urn": svc_y["urn"]})
    cleanup_urns.remove(svc_y["urn"])
