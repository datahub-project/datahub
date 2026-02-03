"""
AI Plugin configuration loader for external MCP servers.

This module provides the AiPluginLoader class for loading AI plugin configurations
from DataHub entities (GlobalSettings and CorpUserSettings) via GraphQL.

Design Decisions:
- Uses user's JWT-authenticated DataHubGraph for all queries
- Implements two-level enablement: plugin must be enabled by BOTH admin AND user
- Default is disabled: if plugin not in user's settings, it's NOT enabled
- Loads all necessary config to connect to MCP servers (URL, auth, etc.)
"""

from dataclasses import dataclass
from enum import Enum
from typing import Dict, List, Optional, TypedDict

from datahub.ingestion.graph.client import DataHubGraph
from loguru import logger

# TypedDicts for GraphQL response structures
# These provide type safety for the raw GraphQL data


class _CustomHeaderDict(TypedDict, total=False):
    key: str
    value: str


class _McpServerPropertiesDict(TypedDict, total=False):
    url: str
    transport: str
    timeout: float
    customHeaders: List[_CustomHeaderDict]


class _ServicePropertiesDict(TypedDict, total=False):
    displayName: str
    description: str


class _ServiceDict(TypedDict, total=False):
    urn: str
    properties: _ServicePropertiesDict
    mcpServerProperties: _McpServerPropertiesDict


class _OAuthConfigDict(TypedDict, total=False):
    serverUrn: str
    requiredScopes: List[str]


class _SharedApiKeyConfigDict(TypedDict, total=False):
    credentialUrn: str
    authLocation: str
    authHeaderName: str
    authScheme: str
    authQueryParam: str


class _UserApiKeyConfigDict(TypedDict, total=False):
    authLocation: str
    authHeaderName: str
    authScheme: str
    authQueryParam: str


class _GlobalPluginDict(TypedDict, total=False):
    """TypedDict for a global plugin from GlobalSettings.aiPlugins."""

    id: str
    enabled: bool
    instructions: str
    authType: str
    service: _ServiceDict
    oauthConfig: _OAuthConfigDict
    sharedApiKeyConfig: _SharedApiKeyConfigDict
    userApiKeyConfig: _UserApiKeyConfigDict


class _UserOAuthConfigDict(TypedDict, total=False):
    isConnected: bool


class _UserApiKeyConnectionDict(TypedDict, total=False):
    isConnected: bool


class _UserPluginDict(TypedDict, total=False):
    """TypedDict for a user plugin setting from CorpUserSettings."""

    id: str
    enabled: bool
    oauthConfig: _UserOAuthConfigDict
    apiKeyConfig: _UserApiKeyConnectionDict
    customHeaders: List[_CustomHeaderDict]


class AiPluginAuthType(Enum):
    """Authentication type for an AI plugin."""

    NONE = "NONE"
    SHARED_API_KEY = "SHARED_API_KEY"
    USER_API_KEY = "USER_API_KEY"
    USER_OAUTH = "USER_OAUTH"


class McpTransport(Enum):
    """Transport protocol for MCP communication."""

    HTTP = "HTTP"
    SSE = "SSE"


class AuthInjectionLocation(Enum):
    """Where to inject authentication in HTTP requests."""

    HEADER = "HEADER"
    QUERY = "QUERY"


@dataclass
class OAuthConfig:
    """OAuth configuration for a plugin."""

    server_urn: str
    required_scopes: List[str]


@dataclass
class SharedApiKeyConfig:
    """Shared API key configuration for a plugin."""

    credential_urn: str
    auth_location: AuthInjectionLocation
    auth_header_name: str
    auth_scheme: Optional[str]
    auth_query_param: Optional[str]


@dataclass
class UserApiKeyConfig:
    """User API key configuration for a plugin."""

    auth_location: AuthInjectionLocation
    auth_header_name: str
    auth_scheme: Optional[str]
    auth_query_param: Optional[str]


@dataclass
class McpServerConfig:
    """MCP server connection configuration."""

    url: str
    transport: McpTransport
    timeout: float
    custom_headers: Dict[str, str]


@dataclass
class AiPluginConfig:
    """
    Complete configuration for an AI plugin.

    Contains all information needed to connect to and authenticate with
    an external MCP server.
    """

    id: str
    service_urn: str
    display_name: str
    description: Optional[str]
    instructions: Optional[str]
    auth_type: AiPluginAuthType
    mcp_config: McpServerConfig
    oauth_config: Optional[OAuthConfig] = None
    shared_api_key_config: Optional[SharedApiKeyConfig] = None
    user_api_key_config: Optional[UserApiKeyConfig] = None
    user_is_connected: bool = False
    user_custom_headers: Optional[Dict[str, str]] = None
    """User-specific custom headers that override/extend admin-configured headers."""


# GraphQL query to load global plugin settings with all necessary details
_LOAD_GLOBAL_PLUGINS_QUERY = """
query GetGlobalAiPlugins {
  globalSettings {
    aiPlugins {
      id
      enabled
      instructions
      authType
      service {
        urn
        properties {
          displayName
          description
        }
        mcpServerProperties {
          url
          transport
          timeout
          customHeaders {
            key
            value
          }
        }
      }
      oauthConfig {
        serverUrn
        requiredScopes
      }
      sharedApiKeyConfig {
        credentialUrn
        authLocation
        authHeaderName
        authScheme
        authQueryParam
      }
      userApiKeyConfig {
        authLocation
        authHeaderName
        authScheme
        authQueryParam
      }
    }
  }
}
""".strip()

# GraphQL query to load user's plugin settings via `me` (scoped to JWT)
_LOAD_USER_PLUGIN_SETTINGS_QUERY = """
query GetUserAiPluginSettings {
  me {
    corpUser {
      settings {
        aiPluginSettings {
          plugins {
            id
            enabled
            oauthConfig {
              isConnected
            }
            apiKeyConfig {
              isConnected
            }
            customHeaders {
              key
              value
            }
          }
        }
      }
    }
  }
}
""".strip()


@dataclass
class _UserPluginSetting:
    """Internal representation of a user's plugin setting."""

    enabled: bool
    is_connected: bool
    custom_headers: Dict[str, str]


class AiPluginLoader:
    """
    Loads AI plugin configuration combining Global and User settings.

    Uses user's JWT-authenticated DataHubGraph for all queries.
    This ensures user settings are properly scoped to the authenticated user.
    """

    def __init__(self, graph: DataHubGraph) -> None:
        """
        Initialize the loader.

        Args:
            graph: DataHubGraph client authenticated with user's JWT.
                   IMPORTANT: Must be user's JWT, not system credentials.
        """
        self._graph = graph

    def load_enabled_plugins(self) -> List[AiPluginConfig]:
        """
        Load all AI plugins that are enabled for the authenticated user.

        A plugin is enabled only if BOTH:
        - Admin enabled it globally (GlobalSettings.aiPlugins[id].enabled = true)
        - User enabled it (CorpUserSettings.aiPluginSettings.plugins[id].enabled = true)

        Default is disabled: if plugin not in user's settings, it's NOT enabled.

        Returns:
            List of fully configured AiPluginConfig objects for enabled plugins.
        """
        # Load global plugins (admin-enabled)
        global_plugins = self._load_global_plugins()
        logger.info(
            f"Loaded {len(global_plugins)} global AI plugins from GlobalSettings"
        )

        # Load user's plugin settings via `me` (scoped to JWT)
        user_settings = self._load_user_plugin_settings()
        logger.info(
            f"Loaded {len(user_settings)} user plugin settings from CorpUserSettings"
        )

        # Filter to only effectively enabled plugins
        enabled_plugins: List[AiPluginConfig] = []
        for plugin in global_plugins:
            plugin_id = plugin.get("id")
            service = plugin.get("service") or {}
            service_props = service.get("properties") or {}
            display_name = service_props.get("displayName", plugin_id)

            # Check if admin enabled globally
            if not plugin.get("enabled", False):
                logger.debug(
                    f"Plugin '{display_name}' ({plugin_id}): skipped - not enabled globally"
                )
                continue

            if not plugin_id:
                logger.debug(f"Plugin '{display_name}': skipped - no plugin ID")
                continue

            user_setting = user_settings.get(plugin_id)
            if not user_setting:
                logger.debug(
                    f"Plugin '{display_name}' ({plugin_id}): skipped - "
                    "not in user settings (default disabled)"
                )
                continue

            if not user_setting.enabled:
                logger.debug(
                    f"Plugin '{display_name}' ({plugin_id}): skipped - disabled by user"
                )
                continue

            # Parse into AiPluginConfig
            try:
                config = self._parse_plugin_config(plugin, user_setting)
                if config:
                    enabled_plugins.append(config)
                    logger.info(
                        f"Plugin '{display_name}' ({plugin_id}): ENABLED "
                        f"(connected={user_setting.is_connected})"
                    )
            except Exception as e:
                logger.warning(f"Failed to parse plugin config for {plugin_id}: {e}")

        logger.info(
            f"Total enabled plugins for user: {len(enabled_plugins)} "
            f"(of {len(global_plugins)} global)"
        )
        return enabled_plugins

    def _load_global_plugins(self) -> List[_GlobalPluginDict]:
        """Load global plugin configurations from GlobalSettings."""
        try:
            logger.debug("Querying GlobalSettings for AI plugins...")
            result = self._graph.execute_graphql(query=_LOAD_GLOBAL_PLUGINS_QUERY)
            plugins: List[_GlobalPluginDict] = (
                result.get("globalSettings", {}).get("aiPlugins") or []
            )
            # Log summary of global plugins
            for plugin in plugins:
                plugin_id = plugin.get("id")
                # Handle null/None as False - only explicit True enables the plugin
                enabled = plugin.get("enabled") is True
                service = plugin.get("service") or {}
                service_props = service.get("properties") or {}
                display_name = service_props.get("displayName", plugin_id)
                mcp_props = service.get("mcpServerProperties") or {}
                url = mcp_props.get("url", "N/A")
                logger.debug(
                    f"Global plugin: '{display_name}' ({plugin_id}) - "
                    f"enabled={enabled}, url={url}"
                )
            return plugins
        except Exception as e:
            logger.error(f"Failed to load global AI plugins: {e}")
            return []

    def _load_user_plugin_settings(self) -> Dict[str, _UserPluginSetting]:
        """
        Load user's plugin settings via `me` query.

        The `me` query is scoped to the authenticated user's JWT,
        ensuring we only see/modify that user's settings.
        """
        try:
            logger.debug("Querying user plugin settings via 'me' query...")
            result = self._graph.execute_graphql(query=_LOAD_USER_PLUGIN_SETTINGS_QUERY)

            plugins_data: List[_UserPluginDict] = (
                result.get("me", {})
                .get("corpUser", {})
                .get("settings", {})
                .get("aiPluginSettings", {})
                .get("plugins")
                or []
            )

            settings: Dict[str, _UserPluginSetting] = {}
            for plugin in plugins_data:
                plugin_id = plugin.get("id")
                if not plugin_id:
                    continue

                enabled = plugin.get("enabled", False)

                # Determine connection status
                oauth_config = plugin.get("oauthConfig") or {}
                api_key_config = plugin.get("apiKeyConfig") or {}
                is_connected = oauth_config.get(
                    "isConnected", False
                ) or api_key_config.get("isConnected", False)

                # Parse user custom headers
                custom_headers: Dict[str, str] = {}
                for header in plugin.get("customHeaders") or []:
                    key = header.get("key")
                    value = header.get("value")
                    if key and value:
                        custom_headers[key] = value

                settings[plugin_id] = _UserPluginSetting(
                    enabled=enabled,
                    is_connected=is_connected,
                    custom_headers=custom_headers,
                )
                logger.debug(
                    f"User plugin setting: {plugin_id} - "
                    f"enabled={enabled}, connected={is_connected}, "
                    f"custom_headers={len(custom_headers)}"
                )

            return settings

        except Exception as e:
            logger.error(f"Failed to load user AI plugin settings: {e}")
            return {}

    def _parse_plugin_config(
        self,
        plugin_data: _GlobalPluginDict,
        user_setting: _UserPluginSetting,
    ) -> Optional[AiPluginConfig]:
        """Parse raw GraphQL data into AiPluginConfig."""
        service = plugin_data.get("service")
        if not service:
            return None

        mcp_props = service.get("mcpServerProperties")
        if not mcp_props:
            return None

        # Parse MCP server config
        custom_headers: Dict[str, str] = {}
        for header in mcp_props.get("customHeaders") or []:
            key = header.get("key")
            value = header.get("value")
            if key and value:
                custom_headers[key] = value

        transport_str = mcp_props.get("transport", "HTTP")
        try:
            transport = McpTransport(transport_str)
        except ValueError:
            transport = McpTransport.HTTP

        mcp_config = McpServerConfig(
            url=mcp_props.get("url", ""),
            transport=transport,
            timeout=mcp_props.get("timeout", 30.0),
            custom_headers=custom_headers,
        )

        # Parse auth type
        auth_type_str = plugin_data.get("authType", "NONE")
        try:
            auth_type = AiPluginAuthType(auth_type_str)
        except ValueError:
            auth_type = AiPluginAuthType.NONE

        # Parse auth configs
        oauth_config = None
        if oauth_data := plugin_data.get("oauthConfig"):
            oauth_config = OAuthConfig(
                server_urn=oauth_data.get("serverUrn", ""),
                required_scopes=oauth_data.get("requiredScopes") or [],
            )

        shared_api_key_config = None
        if shared_data := plugin_data.get("sharedApiKeyConfig"):
            shared_api_key_config = SharedApiKeyConfig(
                credential_urn=shared_data.get("credentialUrn", ""),
                auth_location=AuthInjectionLocation(
                    shared_data.get("authLocation", "HEADER")
                ),
                auth_header_name=shared_data.get("authHeaderName", "Authorization"),
                auth_scheme=shared_data.get("authScheme"),
                auth_query_param=shared_data.get("authQueryParam"),
            )

        user_api_key_config = None
        if user_api_data := plugin_data.get("userApiKeyConfig"):
            user_api_key_config = UserApiKeyConfig(
                auth_location=AuthInjectionLocation(
                    user_api_data.get("authLocation", "HEADER")
                ),
                auth_header_name=user_api_data.get("authHeaderName", "Authorization"),
                auth_scheme=user_api_data.get("authScheme"),
                auth_query_param=user_api_data.get("authQueryParam"),
            )

        service_props = service.get("properties") or {}

        return AiPluginConfig(
            id=plugin_data.get("id", ""),
            service_urn=service.get("urn", ""),
            display_name=service_props.get("displayName", "Unknown Plugin"),
            description=service_props.get("description"),
            instructions=plugin_data.get("instructions"),
            auth_type=auth_type,
            mcp_config=mcp_config,
            oauth_config=oauth_config,
            shared_api_key_config=shared_api_key_config,
            user_api_key_config=user_api_key_config,
            user_is_connected=user_setting.is_connected,
            user_custom_headers=user_setting.custom_headers or None,
        )
