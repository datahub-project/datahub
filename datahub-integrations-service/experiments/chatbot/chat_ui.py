# Load environment variables first before any other imports
import os
import pathlib
import sys
from datetime import datetime
from typing import List, Optional

from dotenv import load_dotenv

# Load .env file if it exists
load_dotenv()

from loguru import logger

# Setup file logging for the chat UI and all related modules
_LOG_DIR = pathlib.Path(__file__).parent / "logs"
_LOG_DIR.mkdir(parents=True, exist_ok=True)

# Create timestamped log filename for this session
_TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
_LOG_FILE = _LOG_DIR / f"chat_ui_{_TIMESTAMP}.log"

# Remove default logger and add file handler with DEBUG level
logger.remove()  # Remove default handler
logger.add(
    _LOG_FILE,
    level="DEBUG",
    format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} | {message}",
    rotation="10 MB",  # Still rotate within session if it gets large
    retention="7 days",
    compression="zip",
)
# Also add console output for INFO and above
logger.add(
    sys.stderr,
    level="INFO",
    format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} | {message}",
)
logger.info(f"Chat UI logging initialized - session log: {_LOG_FILE.name}")

# =============================================================================
# Local Cost Tracker - MUST BE INJECTED BEFORE OTHER IMPORTS
# =============================================================================
# We need to inject our tracker before any imports that might call get_cost_tracker().
# This includes agent imports, MCP server imports, etc.

from local_cost_tracker import LocalCostTracker

from datahub_integrations.observability import cost

# Inject BEFORE any other imports that might trigger get_cost_tracker()
_local_tracker = LocalCostTracker()
cost._global_tracker = _local_tracker  # type: ignore[assignment]
logger.info("LocalCostTracker injected as global cost tracker")

# =============================================================================
# Now safe to import modules that may use cost tracking
# =============================================================================

from datahub_integrations.experimentation.ai_init import AI_EXPERIMENTATION_INITIALIZED

import streamlit as st
from datahub.sdk.main_client import DataHubClient

from datahub_integrations.chat.agent import AgentRunner
from datahub_integrations.chat.agents import create_data_catalog_explorer_agent
from datahub_integrations.chat.chat_history import HumanMessage
from datahub_integrations.experimentation.chatbot.st_chat_history import st_chat_history
from datahub_integrations.mcp.mcp_server import mcp
from datahub_integrations.mcp.tool_context import ToolContext
from datahub_integrations.mcp.view_preference import NoView, UseDefaultView
from datahub_integrations.mcp_integration.external_mcp_manager import (
    ExternalMCPManager,
    ExternalToolWrapper,
)

assert AI_EXPERIMENTATION_INITIALIZED

# Path to external MCP servers configuration (optional, via environment variable)
# If EXTERNAL_MCP_CONFIG is not set, no external tools will be loaded
_MCP_CONFIG_PATH: Optional[pathlib.Path] = None
if _mcp_config_env := os.environ.get("EXTERNAL_MCP_CONFIG"):
    _MCP_CONFIG_PATH = pathlib.Path(_mcp_config_env)


@st.cache_resource
def client() -> DataHubClient:
    return DataHubClient.from_env()


@st.cache_resource
def external_mcp_manager() -> Optional[ExternalMCPManager]:
    """Load external MCP manager if config path is set via EXTERNAL_MCP_CONFIG env var."""
    if _MCP_CONFIG_PATH is None:
        return None
    return ExternalMCPManager.from_config_file_if_exists(_MCP_CONFIG_PATH)


def get_external_tools() -> List[ExternalToolWrapper]:
    """Get tools from external MCP servers."""
    manager = external_mcp_manager()
    if manager is None:
        return []
    try:
        tools = manager.get_tools()
        if tools:
            logger.info(f"Loaded {len(tools)} tools from external MCP servers")
        return tools
    except Exception as e:
        logger.warning(f"Failed to load external MCP tools: {e}")
        return []


@st.cache_data
def frontend_url() -> str:
    return client()._graph.frontend_base_url


st.markdown(f"Connected to {frontend_url()}")


def _make_empty_agent() -> AgentRunner:
    st.text("Resetting agent session")

    # Get external MCP tools (if any configured)
    external_tools = get_external_tools()
    if external_tools:
        st.info(
            f"Loaded {len(external_tools)} external MCP tools: {[t.name for t in external_tools]}"
        )

    # Create agent with default MCP + external tools
    # Combine default MCP server with external tools
    # Note: external tools implement the same interface as ToolWrapper (duck typing)
    all_tools: list = [mcp] + list(external_tools)  # type: ignore[assignment]

    return create_data_catalog_explorer_agent(client=client(), tools=all_tools)


def _get_agent() -> AgentRunner:
    # Initialize agent if it doesn't exist
    if "chat_session" not in st.session_state:
        st.session_state.chat_session = _make_empty_agent()
    return st.session_state.chat_session


_get_agent()

_has_history = bool(_get_agent().history.messages)

col1, col2, col3 = st.columns(3)

with col1:
    show_thinking = st.toggle("Show internal thinking", value=True)
with col2:  # Add a clear chat button
    if st.button("Clear Chat", disabled=not _has_history):
        st.session_state.chat_session = _make_empty_agent()
        # Clear token/cost tracking
        st.session_state.last_turn_usage = None
        st.session_state.turn_totals = None
        st.session_state.session_totals = None
        _local_tracker.reset_all()
        st.rerun()
with col3:  # Add a download history button
    st.download_button(
        "Save Chat",
        _get_agent().history.json(),
        file_name="history.json",
        mime="application/json",
        disabled=not _has_history,
    )

# Global view controls
_view_col1, _view_col2 = st.columns([1, 3])
with _view_col1:
    _disable_view = st.checkbox(
        "Disable global view",
        help="When checked, search results are not filtered by the organization's default view.",
    )
with _view_col2:
    if _disable_view:
        st.caption("Global view: **disabled** — searches return unfiltered results")
    else:
        try:
            _active_view = UseDefaultView().get_view(client()._graph)
            if _active_view:
                st.caption(f"Global view: **active** — `{_active_view}`")
            else:
                st.caption("Global view: **none configured**")
        except Exception as e:
            st.caption(f"Global view: could not fetch ({e})")

# Apply view preference to the current agent via tool context
_view_pref = NoView() if _disable_view else UseDefaultView()
_get_agent().tool_context = ToolContext([_view_pref])

st.divider()

# Display token usage if available
if "last_turn_usage" in st.session_state and st.session_state.last_turn_usage:
    turn_models = st.session_state.last_turn_usage
    session_totals = st.session_state.get("session_totals", {})

    # Show per-model breakdown for last turn
    for model_key, stats in turn_models.items():
        # "new" = prompt + cache_write (full price input)
        # "cached" = cache_read (discounted input, 10x cheaper)
        new_tokens = stats.get("prompt", 0) + stats.get("cache_write", 0)
        cached_tokens = stats.get("cache_read", 0)

        st.caption(
            f"📊 **{model_key}:** {new_tokens:,} new + {cached_tokens:,} cached / "
            f"{stats.get('completion', 0):,} out (${stats.get('cost', 0):.4f})"
        )

    # Show session totals
    session_new = session_totals.get("prompt", 0) + session_totals.get("cache_write", 0)
    session_cached = session_totals.get("cache_read", 0)
    st.caption(
        f"💰 **Session total:** {session_new:,} new + {session_cached:,} cached / "
        f"{session_totals.get('completion', 0):,} out • ${session_totals.get('cost', 0):.4f}"
    )

# Chat UI.
st_chat_history(
    _get_agent().history,
    show_thinking=show_thinking,
)
if prompt := st.chat_input("Type your message here..."):
    # Reset turn counters before processing new message
    _local_tracker.reset_turn()

    # Add user message to agent session
    user_message = HumanMessage(text=prompt)
    agent = _get_agent()
    agent.add_message(user_message)

    # Generate bot response
    def update_progress(messages):
        """Update status to show all reasoning messages on separate lines."""
        if messages:
            # Update label with current step count
            status.update(
                label=f"💭 Thinking... ({len(messages)} steps)", state="running"
            )
            # Append the latest message (messages accumulate naturally in the status widget)
            status.write(f"{len(messages)}. {messages[-1].text}")

    with (
        st.status("Generating response...", expanded=True) as status,
        _get_agent().set_progress_callback(update_progress),
    ):
        try:
            response = _get_agent().generate_formatted_message()

            # Store per-model usage in session state for persistent display after rerun
            st.session_state.last_turn_usage = {
                f"{key.provider}/{key.model}": {
                    "prompt": stats.prompt_tokens,
                    "completion": stats.completion_tokens,
                    "cache_read": stats.cache_read_tokens,
                    "cache_write": stats.cache_write_tokens,
                    "cost": stats.total_cost,
                }
                for key, stats in _local_tracker.turn_usage.items()
            }
            # Session totals - aggregate across all models
            st.session_state.session_totals = {
                "prompt": sum(
                    s.prompt_tokens for s in _local_tracker.total_usage.values()
                ),
                "completion": sum(
                    s.completion_tokens for s in _local_tracker.total_usage.values()
                ),
                "cache_read": sum(
                    s.cache_read_tokens for s in _local_tracker.total_usage.values()
                ),
                "cache_write": sum(
                    s.cache_write_tokens for s in _local_tracker.total_usage.values()
                ),
                "cost": _local_tracker.total_total_cost,
            }

            st.rerun()

        except Exception as e:
            st.error(f"Error generating response: {str(e)}")
