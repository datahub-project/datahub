# ruff: noqa: INP001
"""
Shared connection configuration for the Observe Control Panel Streamlit app.

This module provides utilities for managing DataHub API connection configuration
that is shared across all pages (Monitors, Tools, Time Series Explorer).
"""

import sys
from pathlib import Path
from typing import Optional

import streamlit as st
from datahub.ingestion.graph.client import DataHubGraph, get_default_graph
from datahub.ingestion.graph.config import DatahubClientConfig

# Add scripts directory to path for direct execution
_script_dir = Path(__file__).parent
if str(_script_dir) not in sys.path:
    sys.path.insert(0, str(_script_dir))

# Import env config utilities using importlib to avoid circular imports
# (shared.py imports from shared_config, but common/__init__.py imports from shared,
# so we can't go through the package __init__.py)
import importlib.util as _importlib_util  # noqa: E402

_env_config_path = _script_dir / "streamlit_explorer" / "common" / "env_config.py"
_spec = _importlib_util.spec_from_file_location("env_config", _env_config_path)
_env_config_module = _importlib_util.module_from_spec(_spec)  # type: ignore[arg-type]
_spec.loader.exec_module(_env_config_module)  # type: ignore[union-attr]

DataHubEnvConfig = _env_config_module.DataHubEnvConfig
get_env_config_summary = _env_config_module.get_env_config_summary
list_env_files = _env_config_module.list_env_files
load_env_config = _env_config_module.load_env_config
load_env_config_from_file = _env_config_module.load_env_config_from_file

# Session state keys for connection configuration
ACTIVE_ENV_CONFIG = "shared_active_env_config"
CUSTOM_ENV_PATH = "shared_custom_env_path"
USING_DEFAULT_GRAPH = "shared_using_default_graph"

# Session state keys for cached clients (used by assertions_ui.py and others)
# These are cleared when connection settings change
GRAPH_CLIENT_KEY = "shared_graph_client"
METRICS_CLIENT_KEY = "shared_metrics_client"
MONITOR_CLIENT_KEY = "shared_monitor_client"

# Page reference for navigation (set by assertions_ui.py after page creation)
_connection_settings_page = None


def set_connection_settings_page(page):
    """Set the connection settings page reference for navigation."""
    global _connection_settings_page
    _connection_settings_page = page


def _try_default_graph() -> Optional[DataHubGraph]:
    """Try to get the default graph client, returning None if it fails."""
    try:
        return get_default_graph()
    except Exception:
        return None


def get_active_config() -> Optional[DataHubEnvConfig]:
    """Get the currently active DataHub configuration from session state.

    On first call, automatically loads from ~/.datahubenv if it exists.
    If no env file exists, attempts to use the default graph client's configuration.
    """
    if ACTIVE_ENV_CONFIG not in st.session_state:
        # Try to auto-load default config on first access
        default_config = load_env_config()
        if default_config:
            st.session_state[ACTIVE_ENV_CONFIG] = default_config
            st.session_state[USING_DEFAULT_GRAPH] = False
        else:
            # No env file found - try to use default graph client
            graph = _try_default_graph()
            if graph is not None:
                # Create a config from the graph's configuration
                st.session_state[ACTIVE_ENV_CONFIG] = DataHubEnvConfig(
                    server=graph.config.server,
                    token=graph.config.token,
                    source_file="default graph client",
                )
                st.session_state[USING_DEFAULT_GRAPH] = True
    return st.session_state.get(ACTIVE_ENV_CONFIG)


def clear_cached_clients() -> None:
    """Clear cached graph/metrics/monitor clients when configuration changes.

    This ensures that pages like All Monitors, Monitor Details, and Create Assertion
    will use the new connection settings after they are changed.
    """
    for key in [GRAPH_CLIENT_KEY, METRICS_CLIENT_KEY, MONITOR_CLIENT_KEY]:
        if key in st.session_state:
            del st.session_state[key]


def set_active_config(config: DataHubEnvConfig) -> None:
    """Set the active DataHub configuration in session state.

    Also clears any cached clients so they will be recreated with the new config.
    """
    # Clear cached clients first so they'll be recreated with new config
    clear_cached_clients()
    st.session_state[ACTIVE_ENV_CONFIG] = config
    st.session_state[USING_DEFAULT_GRAPH] = False


def get_configured_graph(use_cache: bool = True) -> Optional[DataHubGraph]:
    """
    Get the single DataHub graph client for the entire Streamlit app.

    This function caches the client in session state. The cache is automatically
    cleared when connection settings change (via set_active_config), ensuring
    all pages (Create Assertion, All Monitors, Monitor Details, etc.) use the
    new connection.

    If an explicit config is set, uses that. Otherwise falls back to the
    default graph client (which uses env vars or ~/.datahubenv).

    Args:
        use_cache: If True (default), wrap execute_graphql with st.cache_data
                   for better performance. Set to False for operations that
                   must always hit the server (e.g., mutations).

    Returns:
        DataHubGraph instance if configured, None if no configuration is available.
    """
    # Return cached client if available
    if GRAPH_CLIENT_KEY in st.session_state:
        return st.session_state[GRAPH_CLIENT_KEY]

    # If we're using the default graph, try to get it
    if st.session_state.get(USING_DEFAULT_GRAPH, False):
        graph = _try_default_graph()
    else:
        config = get_active_config()
        if not config:
            return None

        graph = DataHubGraph(
            config=DatahubClientConfig(
                server=config.server,
                token=config.token,
            )
        )

    # Cache for this session and optionally add query caching
    if graph is not None:
        if use_cache:
            # Wrap execute_graphql with st.cache_data for query caching
            graph.execute_graphql = st.cache_data(graph.execute_graphql)  # type: ignore
        st.session_state[GRAPH_CLIENT_KEY] = graph

    return graph


def render_connection_status(show_link: bool = True) -> bool:
    """
    Render a compact connection status bar at the top of a page.

    Args:
        show_link: Whether to show a link/button to navigate to Connection Settings.

    Returns:
        True if connected, False if not configured.
    """
    config = get_active_config()

    if config:
        cols = st.columns([3, 1])
        with cols[0]:
            st.markdown(
                f"🔗 **Connected to:** `{config.display_name}` "
                f"<span style='color: gray; font-size: 0.8em;'>({config.source_file or 'manual'})</span>",
                unsafe_allow_html=True,
            )
        with cols[1]:
            if show_link and _connection_settings_page:
                if st.button("⚙️ Settings", key="connection_status_settings_btn"):
                    st.switch_page(_connection_settings_page)
        return True
    else:
        cols = st.columns([3, 1])
        with cols[0]:
            st.warning("⚠️ No DataHub connection configured")
        with cols[1]:
            if show_link and _connection_settings_page:
                if st.button(
                    "Configure →", key="connection_status_configure_btn", type="primary"
                ):
                    st.switch_page(_connection_settings_page)
        return False


def require_connection() -> Optional[DataHubGraph]:
    """
    Check that a connection is configured and return the graph client.

    Shows a warning with link to configure if not connected.

    Returns:
        DataHubGraph instance if connected, None if not configured.
    """
    if not render_connection_status():
        st.info("Please configure a DataHub connection to use this page.")
        return None
    return get_configured_graph()


def render_connection_settings_page():
    """Render the full connection settings page."""
    st.header("Connection Settings")
    st.markdown(
        "Configure connection to the DataHub API. "
        "This configuration is used across all pages in the app."
    )

    st.markdown("---")

    # Current config status
    current_config = get_active_config()

    if current_config:
        st.success(f"✓ Connected to: **{current_config.display_name}**")
        summary = get_env_config_summary(current_config)
        cols = st.columns(4)
        for i, (key, val) in enumerate(summary.items()):
            cols[i % 4].markdown(f"**{key}:** `{val}`")
    else:
        st.warning("No environment configured")

    st.markdown("---")

    # Config source selection
    config_source = st.radio(
        "Configuration Source",
        options=[
            "Default (~/.datahubenv)",
            "Select from discovered files",
            "Custom path",
        ],
        horizontal=True,
        key="connection_config_source",
    )

    if config_source == "Default (~/.datahubenv)":
        if st.button("Load Default Config", key="load_default_config"):
            config = load_env_config(allow_env_override=True)
            if config:
                set_active_config(config)
                st.success(f"Loaded config from: {config.source_file}")
                st.rerun()
            else:
                st.error(
                    "No valid configuration found in ~/.datahubenv or environment variables"
                )

    elif config_source == "Select from discovered files":
        # Discover available env files
        env_files = list_env_files()
        if env_files:
            file_options = {
                f"{path} ({config.hostname if config else 'invalid'})": (path, config)
                for path, config in env_files
            }
            selected = st.selectbox(
                "Available Configuration Files",
                options=list(file_options.keys()),
                key="discovered_env_files",
            )
            if selected and st.button(
                "Load Selected Config", key="load_selected_config"
            ):
                path, config = file_options[selected]
                if config:
                    set_active_config(config)
                    st.success(f"Loaded config from: {path}")
                    st.rerun()
                else:
                    st.error(f"Invalid configuration in: {path}")
        else:
            st.info("No configuration files discovered")

    elif config_source == "Custom path":
        custom_path = st.text_input(
            "Path to env file",
            value=st.session_state.get(CUSTOM_ENV_PATH, ""),
            placeholder="~/.datahub/production.env",
            key="custom_env_path_input",
        )
        st.session_state[CUSTOM_ENV_PATH] = custom_path

        if custom_path and st.button("Load Custom Config", key="load_custom_config"):
            config = load_env_config_from_file(custom_path)
            if config:
                set_active_config(config)
                st.success(f"Loaded config from: {config.source_file}")
                st.rerun()
            else:
                st.error(f"Failed to load configuration from: {custom_path}")

    # Manual override option
    st.markdown("---")
    show_manual = st.checkbox(
        "✏️ Manual Override", value=False, key="show_manual_override"
    )
    if show_manual:
        st.markdown(
            "Manually specify connection details (not persisted across sessions)."
        )
        manual_server = st.text_input(
            "Server URL",
            placeholder="https://your-datahub.com",
            key="manual_server_url",
        )
        manual_token = st.text_input(
            "API Token",
            type="password",
            key="manual_api_token",
        )

        if manual_server and st.button(
            "Apply Manual Config", key="apply_manual_config"
        ):
            config = DataHubEnvConfig(
                server=manual_server,
                token=manual_token if manual_token else None,
                source_file="manual input",
            )
            set_active_config(config)
            st.success(f"Applied manual config for: {config.hostname}")
            st.rerun()

    # Clear configuration option
    if current_config:
        st.markdown("---")
        if st.button("🗑️ Clear Configuration", key="clear_config"):
            if ACTIVE_ENV_CONFIG in st.session_state:
                del st.session_state[ACTIVE_ENV_CONFIG]
            if USING_DEFAULT_GRAPH in st.session_state:
                del st.session_state[USING_DEFAULT_GRAPH]
            st.success("Configuration cleared")
            st.rerun()


# Re-export env_config types for convenience
__all__ = [
    "ACTIVE_ENV_CONFIG",
    "CUSTOM_ENV_PATH",
    "GRAPH_CLIENT_KEY",
    "METRICS_CLIENT_KEY",
    "MONITOR_CLIENT_KEY",
    "DataHubEnvConfig",
    "clear_cached_clients",
    "get_active_config",
    "set_active_config",
    "get_configured_graph",
    "render_connection_status",
    "require_connection",
    "render_connection_settings_page",
    "set_connection_settings_page",
    "get_env_config_summary",
    "list_env_files",
    "load_env_config",
    "load_env_config_from_file",
]
