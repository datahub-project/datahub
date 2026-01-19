"""
API Dependencies - Shared dependencies for FastAPI routes.

Provides dependency injection for chat engine, config, and other shared resources.
"""

from typing import Optional

from fastapi import Depends, HTTPException, Header
from loguru import logger

# Import from parent directory
import sys
from pathlib import Path

parent_dir = Path(__file__).parent.parent.parent.parent
if str(parent_dir) not in sys.path:
    sys.path.insert(0, str(parent_dir))

from connection_manager import ConnectionConfig, ConnectionManager, ConnectionMode

# Add backend directory to path for core imports
backend_dir = Path(__file__).parent.parent
if str(backend_dir) not in sys.path:
    sys.path.insert(0, str(backend_dir))

from core.chat_engine import ChatEngine
from core.telemetry_client import TelemetryClient
from core.datahub_conversation_client import ChatUIDataHubClient

try:
    from datahub.ingestion.graph.client import DataHubGraph, DatahubClientConfig
except ImportError:
    DataHubGraph = None  # type: ignore
    DatahubClientConfig = None  # type: ignore

# Global instances
_chat_engine: Optional[ChatEngine] = None
_connection_manager: Optional[ConnectionManager] = None
_config: Optional[ConnectionConfig] = None
_integrations_service: Optional[dict] = None  # Store service process info
_datahub_graph: Optional["DataHubGraph"] = None  # type: ignore
_telemetry_client: Optional[TelemetryClient] = None
_conversation_clients: dict[str, ChatUIDataHubClient] = {}  # Per-profile clients keyed by server URL


def get_connection_manager() -> ConnectionManager:
    """
    Get or create the connection manager singleton.

    Returns:
        ConnectionManager instance
    """
    global _connection_manager

    if _connection_manager is None:
        _connection_manager = ConnectionManager()
        logger.info("Created ConnectionManager singleton")

    return _connection_manager


def get_config(
    manager: ConnectionManager = Depends(get_connection_manager),
) -> ConnectionConfig:
    """
    Get the current connection configuration.

    Always reloads from disk to pick up token updates from other processes.

    Args:
        manager: ConnectionManager instance (injected)

    Returns:
        Current ConnectionConfig
    """
    global _config

    # Always reload from disk to pick up config changes (e.g., token updates from port 8001)
    _config = manager.load_active_config()
    logger.debug(f"Reloaded connection config with mode: {_config.mode}")

    return _config


def get_datahub_graph(config: ConnectionConfig = Depends(get_config)) -> "DataHubGraph":  # type: ignore
    """
    Get or create the DataHub graph client singleton.

    Updates the client's token on every request to pick up token changes.

    Args:
        config: ConnectionConfig instance (injected, reloaded from disk)

    Returns:
        DataHubGraph instance

    Raises:
        HTTPException: If DataHub SDK is not installed
    """
    global _datahub_graph

    if DataHubGraph is None:
        raise HTTPException(
            status_code=500,
            detail="DataHub SDK not installed. Install with: pip install acryl-datahub",
        )

    if _datahub_graph is None:
        # Strip /api/gms suffix if present - DataHubGraph will append /api/graphql
        server_url = config.gms_url
        if server_url.endswith("/api/gms"):
            server_url = server_url[:-8]  # Remove /api/gms
        graph_config = DatahubClientConfig(server=server_url, token=config.gms_token)
        _datahub_graph = DataHubGraph(config=graph_config)
        logger.info(f"Created DataHubGraph singleton with server: {server_url}, token: {config.gms_token[:20]}...")
    else:
        # Update token if it has changed
        old_token = _datahub_graph.config.token
        new_token = config.gms_token
        logger.debug(f"Checking token: old={old_token[:20] if old_token else 'None'}..., new={new_token[:20] if new_token else 'None'}...")
        if old_token != new_token:
            logger.info("Token changed, recreating DataHubGraph")
            server_url = config.gms_url
            if server_url.endswith("/api/gms"):
                server_url = server_url[:-8]  # Remove /api/gms
            graph_config = DatahubClientConfig(server=server_url, token=config.gms_token)
            _datahub_graph = DataHubGraph(config=graph_config)
            logger.info(f"Recreated DataHubGraph with new token: {new_token[:20]}...")

    return _datahub_graph


def get_telemetry_client(
    graph: "DataHubGraph" = Depends(get_datahub_graph),  # type: ignore
) -> TelemetryClient:
    """
    Get or create the TelemetryClient singleton.

    Maintains a single client instance to preserve query cache across requests.

    Args:
        graph: DataHubGraph instance (injected)

    Returns:
        TelemetryClient instance
    """
    global _telemetry_client

    if _telemetry_client is None:
        _telemetry_client = TelemetryClient(graph, cache_ttl=300)
        logger.info("Created TelemetryClient singleton with 5-minute cache")
    else:
        # Update graph reference if it changed (e.g., token refresh)
        if _telemetry_client.graph != graph:
            logger.info("Graph changed, updating TelemetryClient graph reference")
            _telemetry_client.graph = graph
            _telemetry_client.base_url = graph.config.server.rstrip("/")
            _telemetry_client.token = graph.config.token

    return _telemetry_client


def get_conversation_client(
    graph: "DataHubGraph" = Depends(get_datahub_graph),  # type: ignore
) -> ChatUIDataHubClient:
    """
    Get or create the ChatUIDataHubClient for the current profile.

    Maintains separate client instances per profile (keyed by server URL) to preserve
    conversation caches when switching between profiles. This enables:
    - Efficient filtering/sorting without re-fetching conversations
    - Instant switching between profiles without losing cached data

    Args:
        graph: DataHubGraph instance (injected)

    Returns:
        ChatUIDataHubClient instance with cached conversation data for this profile
    """
    global _conversation_clients

    server_key = graph.config.server.rstrip("/")

    if server_key not in _conversation_clients:
        _conversation_clients[server_key] = ChatUIDataHubClient(graph, cache_ttl=3600)
        logger.info(f"Created ChatUIDataHubClient for profile: {server_key}, token: {graph.config.token[:20] if graph.config.token else 'None'}...")
    else:
        client = _conversation_clients[server_key]
        # Update graph reference if token changed for this profile
        old_token = client.graph.config.token
        new_token = graph.config.token

        if old_token != new_token:
            logger.info(f"Token changed for profile {server_key}, updating graph reference (keeping caches)")
            client.graph = graph
        elif client.graph != graph:
            # Graph object changed but token is same (shouldn't happen often)
            logger.debug(f"Graph object changed for profile {server_key} (same token), updating reference")
            client.graph = graph

    return _conversation_clients[server_key]


def get_chat_engine(config: ConnectionConfig = Depends(get_config)) -> ChatEngine:
    """
    Get or create the chat engine singleton.

    Updates the engine's config on every request to pick up token changes.

    Args:
        config: ConnectionConfig instance (injected, reloaded from disk)

    Returns:
        ChatEngine instance
    """
    global _chat_engine

    if _chat_engine is None:
        _chat_engine = ChatEngine(config)
        logger.info("Created ChatEngine singleton")
    else:
        # Update config if it has changed (e.g., token was regenerated)
        if _chat_engine.config.gms_token != config.gms_token:
            logger.info("Token changed, updating ChatEngine config")
            _chat_engine.update_config(config)

    return _chat_engine


def update_chat_engine_config(config: ConnectionConfig) -> None:
    """
    Update the global chat engine configuration.

    Args:
        config: New configuration
    """
    global _chat_engine, _config

    _config = config

    if _chat_engine is not None:
        _chat_engine.update_config(config)
        logger.info(f"Updated chat engine config to mode: {config.mode}")


def verify_api_key(x_api_key: Optional[str] = Header(None)) -> None:
    """
    Verify API key (optional - for future authentication).

    Args:
        x_api_key: API key from header

    Raises:
        HTTPException: If API key is invalid
    """
    # For now, we don't require API key authentication
    # This is a placeholder for future security enhancements
    pass


def get_conversation_or_404(
    conv_id: str, engine: ChatEngine = Depends(get_chat_engine)
):
    """
    Get a conversation or raise 404.

    Args:
        conv_id: Conversation ID
        engine: ChatEngine instance (injected)

    Returns:
        Conversation object

    Raises:
        HTTPException: If conversation not found
    """
    conv = engine.get_conversation(conv_id)
    if not conv:
        raise HTTPException(status_code=404, detail=f"Conversation {conv_id} not found")
    return conv


def ensure_integrations_service(config: ConnectionConfig) -> dict:
    """
    Auto-start integrations service on port 9003 if not running.

    Args:
        config: Connection configuration

    Returns:
        Service status dict
    """
    global _integrations_service

    # Only auto-start for local_service mode
    if config.mode != ConnectionMode.LOCAL_SERVICE:
        return {"status": "not_needed", "mode": config.mode.value}

    # Import the manager
    from local_integrations_manager import LocalIntegrationsManager, IntegrationsServiceConfig, ServiceStatus

    try:
        manager = LocalIntegrationsManager()

        # Check if we already started a service with this config
        if _integrations_service is not None:
            stored_config = _integrations_service.get("config")

            # Verify config hasn't changed
            if stored_config and (
                stored_config.gms_url == config.gms_url
                and stored_config.gms_token == config.gms_token
                and stored_config.aws_region == config.aws_region
                and stored_config.aws_profile == config.aws_profile
            ):
                logger.debug("Integrations service already running with correct config")
                return {"status": "running", "port": 9003}
            else:
                # Config changed - restart service
                logger.info("Config changed, restarting integrations service...")
                try:
                    process = _integrations_service["process"]
                    manager.stop_service(process)
                except Exception as e:
                    logger.warning(f"Failed to stop old service: {e}")
                _integrations_service = None

        # Check if service already running on 9003 (started externally)
        if manager.get_service_status(9003) == ServiceStatus.RUNNING:
            logger.warning(
                "Integrations service already running on port 9003 (started externally). "
                "Using existing service - config may not match!"
            )
            return {"status": "running", "port": 9003, "external": True}

        # Start service on port 9003
        logger.info("Auto-starting integrations service on port 9003...")

        if not config.gms_url or not config.gms_token:
            return {"status": "error", "error": "Missing GMS URL or token"}

        service_config = IntegrationsServiceConfig(
            gms_url=config.gms_url,
            gms_token=config.gms_token,
            service_port=9003,
            aws_region=config.aws_region,
            aws_profile=config.aws_profile,
        )

        service_process = manager.start_service(service_config, wait_for_ready=True)

        # Store for cleanup later with config for comparison
        _integrations_service = {
            "process": service_process,
            "manager": manager,
            "config": service_config,
        }

        logger.success(f"✓ Integrations service started (PID: {service_process.pid})")
        return {"status": "started", "port": 9003}

    except Exception as e:
        logger.error(f"Failed to start integrations service: {e}")
        return {"status": "error", "error": str(e)}


def cleanup_integrations_service():
    """Clean up integrations service on shutdown."""
    global _integrations_service

    if _integrations_service is not None:
        logger.info("Cleaning up integrations service...")
        try:
            manager = _integrations_service["manager"]
            process = _integrations_service["process"]
            manager.stop_service(process)
            logger.success("✓ Integrations service cleaned up")
        except Exception as e:
            logger.error(f"Error cleaning up service: {e}")
        finally:
            _integrations_service = None
