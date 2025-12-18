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

from datahub_integrations.experimentation.ai_init import AI_EXPERIMENTATION_INITIALIZED

import streamlit as st
from datahub.sdk.main_client import DataHubClient

from datahub_integrations.chat.agent import AgentRunner
from datahub_integrations.chat.agents import create_data_catalog_explorer_agent
from datahub_integrations.chat.chat_history import HumanMessage
from datahub_integrations.experimentation.chatbot.st_chat_history import st_chat_history
from datahub_integrations.mcp.mcp_server import mcp
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
        st.info(f"Loaded {len(external_tools)} external MCP tools: {[t.name for t in external_tools]}")

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
        st.rerun()
with col3:  # Add a download history button
    st.download_button(
        "Save Chat",
        _get_agent().history.json(),
        file_name="history.json",
        mime="application/json",
        disabled=not _has_history,
    )

st.divider()

# Chat UI.
st_chat_history(
    _get_agent().history,
    show_thinking=show_thinking,
)
if prompt := st.chat_input("Type your message here..."):
    # Add user message to agent session
    user_message = HumanMessage(text=prompt)
    _get_agent()._add_message(user_message)

    # Generate bot response
    def update_progress(messages):
        """Update status to show all reasoning messages on separate lines."""
        if messages:
            # Update label with current step count
            status.update(label=f"💭 Thinking... ({len(messages)} steps)", state="running")
            # Append the latest message (messages accumulate naturally in the status widget)
            status.write(f"{len(messages)}. {messages[-1].text}")
    
    with (
        st.status("Generating response...", expanded=True) as status,
        _get_agent().set_progress_callback(update_progress),
    ):
        try:
            response = _get_agent().generate_formatted_message()
            st.rerun()

        except Exception as e:
            st.error(f"Error generating response: {str(e)}")
