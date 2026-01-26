"""Manager for external MCP server connections.

This module provides the ExternalMCPManager class for connecting to and managing
external MCP servers, wrapping their tools for use with the DataCatalog agent.

Key Design Decisions:
- Loads plugin configuration from DataHub entities via AiPluginLoader
- Handles OAuth, API key, and shared API key authentication
- Raises PluginConnectionError on discovery failures or auth errors (401/403)
- Non-auth execution errors (bad params, server errors) return error strings
- Tools are wrapped with plugin metadata for error handling
"""

from __future__ import annotations

import dataclasses
import re
from typing import Callable, Dict, List, Optional

import asyncer
import httpx
import mlflow
import mlflow.entities
from datahub.ingestion.graph.client import DataHubGraph
from fastmcp import Client
from fastmcp.client.transports import SSETransport, StreamableHttpTransport
from loguru import logger
from mcp.types import TextContent

from datahub_integrations.mcp_integration.ai_plugin_loader import (
    AiPluginAuthType,
    AiPluginConfig,
    AiPluginLoader,
    AuthInjectionLocation,
    McpTransport,
)
from datahub_integrations.oauth.credential_store import (
    DataHubConnectionCredentialStore,
    TokenRefreshError,
)

# HTTP status codes that indicate credential failures during EXECUTION (should disconnect)
# Note: 403 Forbidden is intentionally NOT included - it means credentials are valid
# but user lacks permission for a specific resource. The agent should handle this
# gracefully and potentially try different resources.
_CREDENTIAL_ERROR_STATUS_CODES = {401}


def _is_credential_error(exc: Exception) -> bool:
    """Check if an exception indicates invalid credentials during EXECUTION (should disconnect).

    Returns True for:
    - TokenRefreshError (OAuth token refresh failed)
    - HTTP 401 Unauthorized

    Returns False for:
    - HTTP 403 Forbidden (valid credentials but no permission for specific resource)
    - Other errors

    The distinction matters: 401 means "reconnect needed", while 403 means
    "try a different resource" - the agent should handle 403 gracefully.

    Note: For DISCOVERY errors, use _is_discovery_auth_error() instead, which
    treats all 4xx errors as auth failures.
    """
    # Direct credential errors
    if isinstance(exc, TokenRefreshError):
        return True

    # HTTP 401 = invalid credentials
    if isinstance(exc, httpx.HTTPStatusError):
        return exc.response.status_code in _CREDENTIAL_ERROR_STATUS_CODES

    # Check error message for "unauthorized" only (not "forbidden")
    error_msg = str(exc).lower()
    if "unauthorized" in error_msg:
        return True

    # Check nested exception
    if exc.__cause__ is not None and isinstance(exc.__cause__, Exception):
        return _is_credential_error(exc.__cause__)

    return False


def _is_discovery_auth_error(exc: Exception) -> bool:
    """Check if an exception indicates auth failure during DISCOVERY (should disconnect).

    During discovery, we treat ALL 4xx HTTP errors as auth failures because:
    - 400 Bad Request: malformed/garbage token
    - 401 Unauthorized: invalid credentials
    - 403 Forbidden: insufficient permissions for the MCP server itself
    - Other 4xx: various client errors likely related to auth setup

    This is stricter than _is_credential_error() which is used for tool EXECUTION,
    where 403 might just mean the user lacks permission for a specific resource
    (not the whole plugin).

    Returns True for:
    - TokenRefreshError (OAuth token refresh failed)
    - Any HTTP 4xx error

    Returns False for:
    - HTTP 5xx errors (server-side issues, transient)
    - Connection errors (transient)
    - Other errors
    """
    # Direct credential errors
    if isinstance(exc, TokenRefreshError):
        return True

    # Any 4xx HTTP error during discovery = auth/config problem
    if isinstance(exc, httpx.HTTPStatusError):
        return 400 <= exc.response.status_code < 500

    # Check nested exception
    if exc.__cause__ is not None and isinstance(exc.__cause__, Exception):
        return _is_discovery_auth_error(exc.__cause__)

    return False


def _sanitize_tool_prefix(name: str) -> str:
    """Sanitize a display name into a valid tool name prefix.

    - Converts to lowercase
    - Replaces non-alphanumeric characters with underscores
    - Collapses multiple underscores into one
    - Strips leading/trailing underscores
    - Falls back to "plugin" if result is empty
    """
    # Lowercase and replace non-alphanumeric with underscore
    sanitized = re.sub(r"[^a-z0-9]", "_", name.lower())
    # Collapse multiple underscores
    sanitized = re.sub(r"_+", "_", sanitized)
    # Strip leading/trailing underscores
    sanitized = sanitized.strip("_")
    # Fallback if empty
    return sanitized if sanitized else "plugin"


class PluginConnectionError(Exception):
    """Raised when plugin connection, discovery, or execution fails.

    This exception triggers the unified error handling flow:
    - Auto-disable plugin in user settings
    - Stop agent loop
    - Send user message with deep-link to settings
    """

    def __init__(self, plugin_id: str, plugin_name: str, message: str) -> None:
        self.plugin_id = plugin_id
        self.plugin_name = plugin_name
        super().__init__(message)


@dataclasses.dataclass
class ExternalToolWrapper:
    """Wrapper for tools from external MCP servers.

    Unlike ToolWrapper which wraps fastmcp.tools.Tool, this wraps tool metadata
    from a remote MCP server and executes via the MCP client.

    Note on client_factory pattern:
        We use a factory function instead of a persistent client connection because
        fastmcp.Client is async-first and its session is bound to a specific event loop.
        Our agent loop is sync, so each tool.run() call uses asyncer.syncify() which
        creates a new event loop. A client connected in one event loop cannot be used
        from another. The workaround is to create a fresh connection per tool call.
        This adds overhead but avoids event loop isolation issues.

        Future optimization: Make the agent loop fully async to enable connection reuse.
    """

    name: str
    description: str
    parameters: dict
    _client_factory: "Callable[[], Client]"
    """Factory to create a fresh MCP client for each call (see note above)."""
    _original_name: str
    plugin_id: str
    """ID of the plugin this tool belongs to (for error handling)."""
    plugin_name: str
    """Display name of the plugin (for error messages)."""
    instructions: Optional[str] = None
    """Server-provided instructions for the LLM.
    
    Multiple tools from the same server will have identical instructions.
    When building prompts, collect unique instructions to avoid duplication.
    """

    def to_bedrock_spec(self) -> dict:
        return {
            "toolSpec": {
                "name": self.name,
                "description": self.description,
                "inputSchema": {"json": self.parameters},
            }
        }

    def run(self, arguments: dict) -> str | dict:
        """Run the tool via the MCP client.

        Creates a fresh client connection for each call to avoid event loop issues.
        Prioritizes structured_content when available, falls back to text content.

        Returns:
            Tool result on success.

        Raises:
            PluginConnectionError: For authentication/authorization failures
                (401, 403, token refresh errors). Triggers auto-disable flow.
            Exception: Other errors are re-raised for normal tool error handling.
        """
        with mlflow.start_span(
            f"run_tool_{self.name}",
            span_type=mlflow.entities.SpanType.TOOL,
            attributes={"tool_name": self.name, "external": True},
        ) as span:
            span.set_inputs(arguments)

            async def _call_tool() -> str | dict:
                client = self._client_factory()
                async with client:
                    result = await client.call_tool(self._original_name, arguments)

                    # Prioritize structured_content (machine-readable) over content (human-readable)
                    if result.structured_content is not None:
                        return result.structured_content

                    # Fall back to extracting text content from MCP response
                    if result.content:
                        texts = []
                        for content in result.content:
                            if isinstance(content, TextContent):
                                texts.append(content.text)
                            else:
                                texts.append(str(content))
                        return "\n".join(texts) if texts else ""
                    return ""

            try:
                return asyncer.syncify(_call_tool, raise_sync_error=False)()
            except Exception as e:
                # Convert auth errors (401, 403, token refresh) to PluginConnectionError
                # which triggers the auto-disable flow in ChatSessionManager.
                # Other errors are re-raised normally for agent_runner to handle.
                if _is_credential_error(e):
                    # Try to extract more details from HTTP response
                    # Check both the exception and its cause (MCP client may wrap errors)
                    response_body = None
                    status_code = None
                    http_error = e if isinstance(e, httpx.HTTPStatusError) else None
                    if http_error is None and e.__cause__ is not None:
                        if isinstance(e.__cause__, httpx.HTTPStatusError):
                            http_error = e.__cause__

                    if http_error is not None:
                        status_code = http_error.response.status_code
                        try:
                            response_body = http_error.response.text
                        except Exception:
                            pass

                    logger.bind(
                        plugin_id=self.plugin_id,
                        plugin_name=self.plugin_name,
                        tool_name=self.name,
                        error_type=type(e).__name__,
                        error_message=str(e),
                        cause_type=type(e.__cause__).__name__ if e.__cause__ else None,
                        status_code=status_code,
                        response_body=response_body,
                    ).opt(exception=True).warning(
                        f"Auth error calling external tool '{self.name}' "
                        f"from plugin '{self.plugin_name}'"
                    )
                    raise PluginConnectionError(
                        plugin_id=self.plugin_id,
                        plugin_name=self.plugin_name,
                        message=f"Authentication failed for {self.plugin_name}: {e}",
                    ) from e
                else:
                    # Re-raise for normal tool error handling
                    raise


class ExternalMCPManager:
    """Manager for external MCP server connections.

    Handles loading configuration from DataHub, connecting to external MCP servers,
    and providing their tools for agent use.

    Usage:
        manager = ExternalMCPManager.from_user_settings(graph, credential_store)
        tools = manager.get_tools()  # Returns list of ExternalToolWrapper objects
    """

    def __init__(
        self,
        plugins: List[AiPluginConfig],
        credential_store: DataHubConnectionCredentialStore,
        user_urn: str,
    ) -> None:
        self._plugins = plugins
        self._credential_store = credential_store
        self._user_urn = user_urn
        self._tools: Optional[List[ExternalToolWrapper]] = None

    @classmethod
    def from_user_settings(
        cls,
        graph: DataHubGraph,
        credential_store: DataHubConnectionCredentialStore,
        user_urn: str,
    ) -> ExternalMCPManager:
        """Create a manager from user's enabled plugins.

        Args:
            graph: DataHubGraph client authenticated with user's JWT.
            credential_store: Store for retrieving user credentials.
            user_urn: URN of the authenticated user.

        Returns:
            ExternalMCPManager configured for the user's enabled plugins.
        """
        loader = AiPluginLoader(graph)
        plugins = loader.load_enabled_plugins()
        return cls(plugins, credential_store, user_urn)

    def _get_auth_headers(self, plugin: AiPluginConfig) -> Dict[str, str]:
        """Get authentication headers for a plugin based on its auth type."""
        headers: Dict[str, str] = {}

        if plugin.auth_type == AiPluginAuthType.NONE:
            return headers

        if plugin.auth_type == AiPluginAuthType.SHARED_API_KEY:
            if not plugin.shared_api_key_config:
                logger.warning(
                    f"Plugin {plugin.id} has SHARED_API_KEY auth but no config"
                )
                return headers

            shared_config = plugin.shared_api_key_config
            # Shared API key is stored in a DataHubConnection
            creds = self._credential_store.get_credentials(
                user_urn="urn:li:corpuser:__system__",  # Shared creds use system user
                plugin_id=shared_config.credential_urn,
            )
            if creds and creds.api_key:
                headers = self._build_auth_headers(
                    token=creds.api_key.api_key,
                    location=shared_config.auth_location,
                    header_name=shared_config.auth_header_name,
                    scheme=shared_config.auth_scheme,
                )

        elif plugin.auth_type == AiPluginAuthType.USER_API_KEY:
            if not plugin.user_api_key_config:
                logger.warning(
                    f"Plugin {plugin.id} has USER_API_KEY auth but no config"
                )
                return headers

            user_api_config = plugin.user_api_key_config
            creds = self._credential_store.get_credentials(
                user_urn=self._user_urn,
                plugin_id=plugin.id,
            )
            if creds and creds.api_key:
                headers = self._build_auth_headers(
                    token=creds.api_key.api_key,
                    location=user_api_config.auth_location,
                    header_name=user_api_config.auth_header_name,
                    scheme=user_api_config.auth_scheme,
                )

        elif plugin.auth_type == AiPluginAuthType.USER_OAUTH:
            if not plugin.oauth_config:
                logger.warning(f"Plugin {plugin.id} has USER_OAUTH auth but no config")
                return headers

            # Let TokenRefreshError propagate - it will be handled as an auth error
            # and trigger the plugin disable flow
            access_token = self._credential_store.get_access_token(
                user_urn=self._user_urn,
                plugin_id=plugin.id,
                oauth_server_urn=plugin.oauth_config.server_urn,
            )
            headers["Authorization"] = f"Bearer {access_token}"

        return headers

    def _build_auth_headers(
        self,
        token: str,
        location: AuthInjectionLocation,
        header_name: str,
        scheme: Optional[str],
    ) -> Dict[str, str]:
        """Build authentication headers from token and config."""
        if location == AuthInjectionLocation.QUERY:
            # Query params not supported in headers - would need URL modification
            return {}

        # Header-based auth
        if scheme:
            return {header_name: f"{scheme} {token}"}
        return {header_name: token}

    def _create_client(self, plugin: AiPluginConfig) -> Client:
        """Create an MCP client for the given plugin configuration."""
        mcp_config = plugin.mcp_config

        # Start with custom headers from config
        headers = dict(mcp_config.custom_headers)

        # Add authentication headers
        auth_headers = self._get_auth_headers(plugin)
        headers.update(auth_headers)

        transport: SSETransport | StreamableHttpTransport
        if mcp_config.transport == McpTransport.SSE:
            transport = SSETransport(url=mcp_config.url, headers=headers)
        else:  # HTTP
            transport = StreamableHttpTransport(url=mcp_config.url, headers=headers)

        return Client(transport, timeout=mcp_config.timeout)

    async def _discover_tools(
        self, plugin: AiPluginConfig, tool_prefix: str
    ) -> List[ExternalToolWrapper]:
        """Connect to an MCP server to discover its tools.

        Creates a temporary connection to list available tools, then creates
        tool wrappers with a factory function for on-demand connections.

        Args:
            plugin: The plugin configuration.
            tool_prefix: Unique prefix for tool names (e.g., "github", "github_2").

        Raises:
            PluginConnectionError: If discovery fails due to auth error (401/403).
            Exception: Other errors are re-raised for caller to handle.
        """

        # Create a client factory that tool wrappers will use for each call
        def client_factory() -> Client:
            return self._create_client(plugin)

        def make_tool_name(original_name: str) -> str:
            """Create tool name, truncating if needed to stay under 64 chars."""
            full_name = f"{tool_prefix}__{original_name}"
            if len(full_name) <= 64:
                return full_name
            # Truncate tool name portion, keeping prefix
            max_tool_len = 64 - len(tool_prefix) - 2  # 2 for "__"
            return f"{tool_prefix}__{original_name[:max_tool_len]}"

        # Temporarily connect to discover available tools
        try:
            client = client_factory()
            async with client:
                logger.bind(
                    plugin_id=plugin.id,
                    plugin_name=plugin.display_name,
                    mcp_url=plugin.mcp_config.url,
                    tool_prefix=tool_prefix,
                ).info(
                    f"Discovering tools from MCP server: {plugin.display_name} "
                    f"(prefix={tool_prefix})"
                )
                tools = await client.list_tools()
                logger.bind(
                    plugin_id=plugin.id,
                    plugin_name=plugin.display_name,
                    tool_prefix=tool_prefix,
                    tool_count=len(tools),
                ).info(
                    f"Plugin '{plugin.display_name}' provides {len(tools)} tools "
                    f"(prefix={tool_prefix})"
                )

                # Log individual tools with their schemas for debugging
                for tool in tools:
                    logger.bind(
                        plugin_name=plugin.display_name,
                        tool_name=tool.name,
                        tool_description=tool.description,
                        tool_input_schema=tool.inputSchema,
                    ).debug(f"Discovered tool: {tool.name}")

                return [
                    ExternalToolWrapper(
                        name=make_tool_name(tool.name),
                        description=tool.description or "",
                        parameters=tool.inputSchema if tool.inputSchema else {},
                        _client_factory=client_factory,
                        _original_name=tool.name,
                        plugin_id=plugin.id,
                        plugin_name=plugin.display_name,
                        instructions=plugin.instructions,
                    )
                    for tool in tools
                ]
        except Exception as e:
            # During discovery, any 4xx error indicates auth/config problem (triggers disable flow)
            # 5xx and connection errors are re-raised for caller to decide how to handle
            if _is_discovery_auth_error(e):
                logger.bind(
                    plugin_id=plugin.id,
                    plugin_name=plugin.display_name,
                    mcp_url=plugin.mcp_config.url,
                    error_type=type(e).__name__,
                ).opt(exception=True).warning(
                    f"Auth error discovering tools from plugin "
                    f"'{plugin.display_name}' ({plugin.id}, url={plugin.mcp_config.url})"
                )
                raise PluginConnectionError(
                    plugin_id=plugin.id,
                    plugin_name=plugin.display_name,
                    message=f"Authentication failed for {plugin.display_name}: {e}",
                ) from e
            else:
                raise

    def _compute_unique_prefixes(self) -> Dict[str, str]:
        """Compute unique tool name prefixes for all plugins.

        Uses sanitized display names with collision handling.
        E.g., if two plugins are named "GitHub", they get "github" and "github_2".

        Returns:
            Dict mapping plugin ID to unique prefix.
        """
        prefixes: Dict[str, str] = {}
        prefix_counts: Dict[str, int] = {}

        for plugin in self._plugins:
            base_prefix = _sanitize_tool_prefix(plugin.display_name)

            # Check for collision and add suffix if needed
            if base_prefix not in prefix_counts:
                prefix_counts[base_prefix] = 1
                prefixes[plugin.id] = base_prefix
            else:
                prefix_counts[base_prefix] += 1
                prefixes[plugin.id] = f"{base_prefix}_{prefix_counts[base_prefix]}"

        return prefixes

    async def _discover_all_tools(self) -> List[ExternalToolWrapper]:
        """Discover tools from all enabled MCP servers.

        Auth errors (PluginConnectionError) are raised immediately to trigger
        the disable flow. Other errors (transient issues) are logged and the
        plugin is skipped.

        Raises:
            PluginConnectionError: If any plugin has an auth error.
        """
        all_tools: List[ExternalToolWrapper] = []

        # Compute unique prefixes for all plugins upfront
        prefixes = self._compute_unique_prefixes()

        for plugin in self._plugins:
            tool_prefix = prefixes[plugin.id]
            try:
                tools = await self._discover_tools(plugin, tool_prefix)
                all_tools.extend(tools)
            except PluginConnectionError:
                # Auth error - re-raise to trigger disable flow
                raise
            except Exception as e:
                # Non-auth error (transient) - log warning with stack trace and skip
                logger.bind(
                    plugin_id=plugin.id,
                    plugin_name=plugin.display_name,
                    tool_prefix=tool_prefix,
                    mcp_url=plugin.mcp_config.url,
                    error_type=type(e).__name__,
                ).opt(exception=True).warning(
                    f"Skipping plugin '{plugin.display_name}' ({plugin.id}) due to "
                    f"discovery error (url={plugin.mcp_config.url}). "
                    f"The plugin will be retried on next request."
                )
                continue

        return all_tools

    def get_tools(self) -> List[ExternalToolWrapper]:
        """Get all tools from external MCP servers.

        Lazily discovers tools on first call. Results are cached.
        Each tool call creates its own connection (no persistent connections).

        Returns:
            List of ExternalToolWrapper objects compatible with agent tool interfaces

        Raises:
            PluginConnectionError: If any plugin fails during discovery.
        """
        if self._tools is not None:
            return self._tools

        # Use syncify to run async tool discovery
        self._tools = asyncer.syncify(
            self._discover_all_tools, raise_sync_error=False
        )()
        return self._tools
