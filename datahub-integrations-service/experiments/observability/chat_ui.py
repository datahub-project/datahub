#!/usr/bin/env python3
"""
Interactive Chat UI with Auto-Chat Mode

This Streamlit app provides:
1. Manual chat interface for DataHub conversations
2. Auto-chat mode that generates realistic questions automatically
3. Real-time conversation display
4. Conversation history and statistics
5. Built-in connection settings with kubectl integration
"""

import os
import random
import subprocess
import time
from configparser import ConfigParser
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Optional

import boto3
import streamlit as st

if TYPE_CHECKING:
    from datahub_integrations.chat.agents import AgentRunner

# Import shared components from chat simulator
from chat_simulator import (
    ChatClient,
    ConversationGenerator,
    DataHubSketcher,
)
from connection_manager import (
    ConnectionConfig,
    ConnectionManager,
    ConnectionMode,
    ConnectionProfile,
)
from kubectl_manager import ConnectionStatus, KubectlManager, PortForwardProcess
from local_integrations_manager import (
    IntegrationsServiceConfig,
    LocalIntegrationsManager,
    ServiceProcess,
)
from loguru import logger
from progress_tracker import ProgressTracker

# Question generation modes
QUESTION_MODE_EMBEDDED = "embedded"  # Generate questions locally using Bedrock
QUESTION_MODE_ENDPOINT = "endpoint"  # Call integrations service endpoint

# Page config
st.set_page_config(
    page_title="Ask DataHub - The Admin Version",
    page_icon="🤖",
    layout="wide",
)


# All shared classes (DataHubSketch, DataHubSketcher, ConversationGenerator, ChatClient)
# are now imported from chat_simulator above

# Note: Agent imports are deferred until needed to avoid requiring DATAHUB_GMS_URL at startup
# They're imported inside create_embedded_agent() when actually needed


# UI Helper Functions
def render_spinner_with_progress(progress_content: str) -> str:
    """Generate spinner HTML with progress content."""
    spinner_html = """
    <style>
    .spinner {
        display: inline-block;
        width: 14px;
        height: 14px;
        border: 2px solid rgba(0,0,0,0.1);
        border-radius: 50%;
        border-top-color: #3498db;
        animation: spin 0.8s linear infinite;
    }
    @keyframes spin {
        to { transform: rotate(360deg); }
    }
    </style>
    <div style="display: flex; align-items: flex-start; gap: 8px;">
        <div class="spinner" style="margin-top: 4px;"></div>
        <div style="flex: 1;">PROGRESS_CONTENT</div>
    </div>
    """
    content = progress_content.replace("\n", "<br>")
    return spinner_html.replace("PROGRESS_CONTENT", content)


def render_spinner_placeholder() -> str:
    """Generate spinner HTML with 'Thinking...' placeholder."""
    return """
    <style>
    .spinner {
        display: inline-block;
        width: 14px;
        height: 14px;
        border: 2px solid rgba(0,0,0,0.1);
        border-radius: 50%;
        border-top-color: #3498db;
        animation: spin 0.8s linear infinite;
    }
    @keyframes spin {
        to { transform: rotate(360deg); }
    }
    </style>
    <div style="display: flex; align-items: center; gap: 8px;">
        <div class="spinner"></div>
        <div><strong>Thinking...</strong></div>
    </div>
    """


def set_pending_message(conv_id: str, message: str, is_auto: bool):
    """Set up processing state for a pending message."""
    proc_state = get_processing_state(conv_id)
    proc_state["is_processing"] = True
    proc_state["current_progress_display"] = None
    proc_state["last_progress_display"] = None
    proc_state["pending_message"] = {"text": message, "is_auto": is_auto}


def create_embedded_agent() -> Optional["AgentRunner"]:
    """Create an agent for embedded mode execution."""
    logger.info("=== create_embedded_agent() called ===")
    try:
        # Get GMS URL and token from config FIRST (before imports)
        logger.info("Getting config from session state...")
        config: ConnectionConfig = st.session_state.connection_config
        logger.info(f"Config retrieved: gms_url={config.gms_url}, token={'***' if config.gms_token else None}")

        # Set environment variables BEFORE importing agent modules
        # For SaaS instances (*.acryl.io with /api/gms), use the full URL as-is
        # The SDK handles /api/gms URLs correctly for SaaS deployments
        server_url = config.gms_url
        logger.info(f"Setting DataHub environment variables with URL: {server_url}...")
        os.environ["DATAHUB_GMS_URL"] = server_url
        os.environ["DATAHUB_GMS_TOKEN"] = config.gms_token
        logger.info(f"Set DATAHUB_GMS_URL={server_url}")

        logger.info("Starting imports...")
        # Import agent dependencies only when needed (avoids requiring DATAHUB_GMS_URL at startup)
        from datahub.sdk.main_client import DataHubClient

        logger.info("Imported DataHubClient")
        from datahub_integrations.chat.agents import create_data_catalog_explorer_agent
        logger.info("Imported create_data_catalog_explorer_agent")
        from datahub_integrations.mcp.mcp_server import mcp
        logger.info("Imported mcp")

        logger.info(f"Creating embedded agent with GMS URL: {config.gms_url}, token: {'***' if config.gms_token else None}")

        # Create DataHub client (server_url already stripped above)
        logger.info(f"Creating DataHubClient with server={server_url}, token={'***' if config.gms_token else 'None'}")
        client = DataHubClient(
            server=server_url,
            token=config.gms_token,
        )
        logger.info(f"✓ DataHubClient created successfully, server: {client.config.server}")

        # Create agent with MCP tools
        logger.info("Creating agent with MCP tools...")
        agent = create_data_catalog_explorer_agent(client=client, tools=[mcp])
        logger.info("✓ Created embedded agent successfully")
        return agent
    except ImportError as e:
        logger.error(f"Agent dependencies not available: {e}")
        st.error(
            "❌ Agent dependencies not available - install datahub-integrations package"
        )
        return None
    except Exception as e:
        logger.error(f"Failed to create embedded agent: {e}")
        st.error(f"❌ Failed to create agent: {e}")
        return None


def get_embedded_agent() -> Optional["AgentRunner"]:
    """Get or create the embedded agent for the processing conversation."""
    # Each conversation gets its own agent instance
    active_conv = get_processing_conversation()

    if "agent" not in active_conv:
        active_conv["agent"] = create_embedded_agent()

    return active_conv["agent"]


def initialize_session_state():
    """Initialize Streamlit session state."""
    # NEW: Multi-conversation support
    if "conversations" not in st.session_state:
        # Create first conversation
        first_conv = {
            "id": f"conv-{int(time.time())}-{random.randint(1000, 9999)}",
            "urn": f"urn:li:conversation:ui-{int(time.time())}-{random.randint(1000, 9999)}",
            "messages": [],
            "created_at": datetime.now(),
            "title": "New conversation",  # Actual title shown is first message
        }
        st.session_state.conversations = [first_conv]

    if "active_conversation_id" not in st.session_state:
        # Set active to first conversation
        if st.session_state.conversations:
            st.session_state.active_conversation_id = st.session_state.conversations[0][
                "id"
            ]

    # NEW: Separate viewing state from processing state
    if "viewing_conversation_id" not in st.session_state:
        # Client state: which conversation user is viewing
        # Initialize to same as active (processing) conversation
        st.session_state.viewing_conversation_id = st.session_state.active_conversation_id

    if "conversation_processing_state" not in st.session_state:
        # Per-conversation processing flags (not global)
        # Structure: {
        #   "conv-id-123": {
        #       "is_processing": False,
        #       "pending_message": None,
        #       "current_progress_display": None,
        #       "last_progress_display": None,
        #   }
        # }
        st.session_state.conversation_processing_state = {}

    # LEGACY: Keep for backward compatibility (will be migrated)
    if "messages" not in st.session_state:
        st.session_state.messages = []

    if "conversation_urn" not in st.session_state:
        st.session_state.conversation_urn = (
            f"urn:li:conversation:ui-{int(time.time())}-{random.randint(1000, 9999)}"
        )

    if "user_urn" not in st.session_state:
        st.session_state.user_urn = (
            f"urn:li:corpuser:ui-user-{random.randint(100, 999)}"
        )

    if "auto_chat_enabled" not in st.session_state:
        st.session_state.auto_chat_enabled = False

    if "auto_chat_count" not in st.session_state:
        st.session_state.auto_chat_count = 0

    if "auto_chat_max" not in st.session_state:
        st.session_state.auto_chat_max = 10

    if "auto_chat_max_messages_per_conv" not in st.session_state:
        st.session_state.auto_chat_max_messages_per_conv = 5

    if "auto_chat_max_conversations" not in st.session_state:
        st.session_state.auto_chat_max_conversations = 2

    if "auto_chat_paused" not in st.session_state:
        st.session_state.auto_chat_paused = False

    if "auto_chat_conversation_ids" not in st.session_state:
        st.session_state.auto_chat_conversation_ids = []

    if "sketch" not in st.session_state:
        st.session_state.sketch = None

    if "question_generator" not in st.session_state:
        st.session_state.question_generator = None

    if "bedrock_client" not in st.session_state:
        st.session_state.bedrock_client = None

    if "stats" not in st.session_state:
        st.session_state.stats = {
            "total_messages": 0,
            "success_count": 0,
            "error_count": 0,
            "total_duration": 0.0,
        }

    # Connection management state
    if "connection_manager" not in st.session_state:
        st.session_state.connection_manager = ConnectionManager()

    if "kubectl_manager" not in st.session_state:
        try:
            st.session_state.kubectl_manager = KubectlManager()
        except RuntimeError as e:
            logger.warning(f"kubectl not available: {e}")
            st.session_state.kubectl_manager = None

    if "connection_config" not in st.session_state:
        # Try to load from active profile, fallback to environment or default
        manager: ConnectionManager = st.session_state.connection_manager
        st.session_state.connection_config = manager.load_active_config()

    if "port_forward_process" not in st.session_state:
        st.session_state.port_forward_process: Optional[PortForwardProcess] = None

    if "connection_status" not in st.session_state:
        st.session_state.connection_status = ConnectionStatus.DISCONNECTED

    # Local integrations service management
    if "local_integrations_manager" not in st.session_state:
        try:
            st.session_state.local_integrations_manager = LocalIntegrationsManager()
        except RuntimeError as e:
            logger.warning(f"Local integrations manager not available: {e}")
            st.session_state.local_integrations_manager = None

    if "local_service_process" not in st.session_state:
        st.session_state.local_service_process: Optional[ServiceProcess] = None

    if "process_detection_done" not in st.session_state:
        st.session_state.process_detection_done = False


def get_available_aws_profiles() -> list[str]:
    """
    Get list of available AWS profiles from ~/.aws/config and ~/.aws/credentials.

    Returns:
        List of profile names, with "default" first if it exists
    """
    profiles = set()

    # Read from ~/.aws/config
    config_file = Path.home() / ".aws" / "config"
    if config_file.exists():
        try:
            config = ConfigParser()
            config.read(config_file)
            for section in config.sections():
                # Sections are named like "profile my-profile" or "default"
                if section == "default":
                    profiles.add("default")
                elif section.startswith("profile "):
                    profiles.add(section.replace("profile ", ""))
        except Exception as e:
            logger.debug(f"Could not read AWS config file: {e}")

    # Read from ~/.aws/credentials
    credentials_file = Path.home() / ".aws" / "credentials"
    if credentials_file.exists():
        try:
            config = ConfigParser()
            config.read(credentials_file)
            profiles.update(config.sections())
        except Exception as e:
            logger.debug(f"Could not read AWS credentials file: {e}")

    # Sort with "default" first, then alphabetically
    profile_list = sorted(profiles)
    if "default" in profile_list:
        profile_list.remove("default")
        profile_list = ["default"] + profile_list

    return profile_list


# =============================================================================
# Conversation Management Functions
# =============================================================================


def get_active_conversation():
    """Get the currently active conversation."""
    for conv in st.session_state.conversations:
        if conv["id"] == st.session_state.active_conversation_id:
            return conv
    # Fallback: return first conversation
    if st.session_state.conversations:
        return st.session_state.conversations[0]
    # If no conversations exist, create one
    new_conv = create_new_conversation()
    return new_conv


def get_viewing_conversation():
    """Get the conversation user is currently viewing (may differ from processing)."""
    for conv in st.session_state.conversations:
        if conv["id"] == st.session_state.viewing_conversation_id:
            return conv
    # Fallback: return first conversation
    if st.session_state.conversations:
        return st.session_state.conversations[0]
    # If no conversations exist, create one
    return create_new_conversation()


def get_processing_conversation():
    """Get the conversation that is actively being processed (auto-chat target)."""
    for conv in st.session_state.conversations:
        if conv["id"] == st.session_state.active_conversation_id:
            return conv
    # Fallback to viewing conversation
    return get_viewing_conversation()


def get_processing_state(conv_id: str) -> dict:
    """Get processing state for a specific conversation."""
    if conv_id not in st.session_state.conversation_processing_state:
        st.session_state.conversation_processing_state[conv_id] = {
            "is_processing": False,
            "pending_message": None,
            "current_progress_display": None,
            "last_progress_display": None,
        }
    return st.session_state.conversation_processing_state[conv_id]


def set_viewing_conversation(conv_id: str):
    """Switch which conversation user is viewing (client state - does NOT affect processing)."""
    st.session_state.viewing_conversation_id = conv_id


def set_processing_conversation(conv_id: str):
    """Switch which conversation is being processed (backend state - auto-chat target)."""
    st.session_state.active_conversation_id = conv_id


def is_viewing_processing_conversation() -> bool:
    """Check if user is viewing the same conversation that's being processed."""
    return st.session_state.viewing_conversation_id == st.session_state.active_conversation_id


def create_new_conversation():
    """Create a new conversation and add it to the list."""
    conv_id = f"conv-{int(time.time())}-{random.randint(1000, 9999)}"
    new_conv = {
        "id": conv_id,
        "urn": f"urn:li:conversation:ui-{int(time.time())}-{random.randint(1000, 9999)}",
        "messages": [],
        "created_at": datetime.now(),
        "title": "New conversation",  # Actual title shown is first message
    }
    st.session_state.conversations.append(new_conv)

    # Initialize processing state for this conversation
    st.session_state.conversation_processing_state[conv_id] = {
        "is_processing": False,
        "pending_message": None,
        "current_progress_display": None,
        "last_progress_display": None,
    }

    return new_conv


def delete_conversation(conv_id: str):
    """Delete a conversation by ID."""
    # Prevent deleting processing conversation during auto-chat
    if (
        st.session_state.get("auto_chat_enabled", False)
        and conv_id == st.session_state.active_conversation_id
    ):
        st.error("❌ Cannot delete conversation being processed by auto-chat")
        return False

    # Find and remove the conversation
    st.session_state.conversations = [
        c for c in st.session_state.conversations if c["id"] != conv_id
    ]

    # Clean up processing state
    if conv_id in st.session_state.conversation_processing_state:
        del st.session_state.conversation_processing_state[conv_id]

    # If deleted viewing conversation, switch to another
    if st.session_state.viewing_conversation_id == conv_id:
        if st.session_state.conversations:
            set_viewing_conversation(st.session_state.conversations[0]["id"])
        else:
            # No conversations left - create a new one
            new_conv = create_new_conversation()
            set_viewing_conversation(new_conv["id"])

    # If deleted processing conversation, switch to viewing conversation
    if st.session_state.active_conversation_id == conv_id:
        set_processing_conversation(st.session_state.viewing_conversation_id)

    return True


def switch_to_conversation(conv_id: str):
    """Switch to a different conversation (viewing state)."""
    set_viewing_conversation(conv_id)


def render_conversation_list():
    """Render the conversation list UI with delete buttons."""
    st.subheader(f"💬 Conversations ({len(st.session_state.conversations)})")

    # Check if auto-chat is enabled
    auto_chat_enabled = st.session_state.get("auto_chat_enabled", False)

    # New chat button - disabled in auto-chat mode
    col1, col2 = st.columns([3, 1])
    with col1:
        pass  # Spacer
    with col2:
        if st.button(
            "➕ New",
            use_container_width=True,
            key="new_chat_button",
            disabled=auto_chat_enabled,
            help="Disabled during auto-chat mode"
            if auto_chat_enabled
            else "Create new conversation",
        ):
            # Create new conversation
            new_conv = create_new_conversation()
            # NEW: Set both viewing and processing to new conversation
            set_viewing_conversation(new_conv["id"])
            set_processing_conversation(new_conv["id"])
            st.rerun()

    # Conversation list (auto-expanded)
    with st.expander("", expanded=True):
        if not st.session_state.conversations:
            st.info("No conversations yet. Click '➕ New' to start!")
        else:
            # Get viewing and processing conversation IDs
            viewing_conv_id = st.session_state.viewing_conversation_id
            processing_conv_id = st.session_state.active_conversation_id

            # Sort by created_at (newest first)
            sorted_convs = sorted(
                st.session_state.conversations,
                key=lambda c: c["created_at"],
                reverse=True,
            )

            for conv in sorted_convs:
                is_viewing = conv["id"] == viewing_conv_id
                is_processing = conv["id"] == processing_conv_id
                msg_count = len(conv["messages"])

                # Get truncated first message as title
                if conv["messages"]:
                    first_msg = next(
                        (m for m in conv["messages"] if m["role"] == "user"), None
                    )
                    if first_msg:
                        title = first_msg["content"][:40] + (
                            "..." if len(first_msg["content"]) > 40 else ""
                        )
                    else:
                        title = "New conversation"
                else:
                    title = "New conversation"

                # NEW: Build label with visual indicators
                label_parts = []
                if is_viewing:
                    label_parts.append("👁️")  # Eye icon for viewing
                if is_processing and auto_chat_enabled:
                    label_parts.append("⚙️")  # Gear icon for processing

                label_prefix = " ".join(label_parts)
                label = f"{label_prefix} {title} ({msg_count} msgs)".strip()

                # Create row with conversation info and delete button
                col1, col2 = st.columns([4, 1])

                with col1:
                    # Button to switch to this conversation
                    # NEW: Always enabled - can view any conversation
                    button_type = "primary" if is_viewing else "secondary"

                    help_text = None
                    if is_processing and auto_chat_enabled:
                        help_text = "Auto-chat is processing this conversation"
                    elif auto_chat_enabled:
                        help_text = "Click to view (read-only during auto-chat)"

                    if st.button(
                        label,
                        key=f"conv_{conv['id']}",
                        use_container_width=True,
                        type=button_type,
                        disabled=False,  # NEW: Never disabled
                        help=help_text,
                    ):
                        if not is_viewing:
                            # NEW: Use setter to switch viewing conversation
                            set_viewing_conversation(conv["id"])
                            st.rerun()

                with col2:
                    # Delete button (disabled in auto-chat mode or if it's the only conversation)
                    can_delete = (
                        len(st.session_state.conversations) > 1
                        and not auto_chat_enabled
                    )
                    help_text = (
                        "Disabled during auto-chat mode"
                        if auto_chat_enabled
                        else (
                            "Delete conversation"
                            if len(st.session_state.conversations) > 1
                            else "Can't delete last conversation"
                        )
                    )
                    if st.button(
                        "🗑️",
                        key=f"del_{conv['id']}",
                        disabled=not can_delete,
                        help=help_text,
                    ):
                        delete_conversation(conv["id"])
                        st.rerun()


def clear_sketch():
    """Clear the DataHub sketch and question generator when switching GMS instances."""
    if st.session_state.sketch is not None:
        logger.info("Clearing DataHub sketch (GMS instance changed)")
        st.session_state.sketch = None
    if st.session_state.question_generator is not None:
        st.session_state.question_generator = None


def save_connection_as_active():
    """Save the current connection configuration as the active profile."""
    try:
        config: ConnectionConfig = st.session_state.connection_config
        manager: ConnectionManager = st.session_state.connection_manager

        # Create or update "last-connection" profile
        profile_name = "last-connection"
        profile = ConnectionProfile(name=profile_name, config=config)

        manager.save_profile(profile)
        manager.set_active_profile(profile_name)
        logger.info(f"✓ Saved connection as active profile: {profile_name}")
    except Exception as e:
        logger.warning(f"Failed to save connection as active profile: {e}")


def detect_and_reconnect_processes():
    """
    Smart detection: check if child processes are still running and reconnect.

    Returns:
        Dict with status info about detected processes
    """
    config: ConnectionConfig = st.session_state.connection_config
    kubectl: Optional[KubectlManager] = st.session_state.kubectl_manager
    local_mgr: Optional[LocalIntegrationsManager] = (
        st.session_state.local_integrations_manager
    )

    result = {
        "local_service_detected": False,
        "port_forward_detected": False,
        "local_service_healthy": False,
        "port_forward_healthy": False,
    }

    # Check for LOCAL_SERVICE mode
    if config.mode == ConnectionMode.LOCAL_SERVICE and local_mgr:
        port = config.local_service_port
        if local_mgr._is_port_in_use(port):
            logger.info(
                f"Detected process on port {port}, checking if it's integrations service..."
            )
            result["local_service_detected"] = True

            # Check if it's a healthy integrations service
            health_info = local_mgr.check_service_health(port)
            if health_info.get("status") == "healthy":
                result["local_service_healthy"] = True
                logger.info(
                    f"✓ Reconnected to running integrations service on port {port}"
                )
                config.integrations_url = f"http://localhost:{port}"
            else:
                logger.warning(
                    f"Port {port} in use but service not healthy: {health_info}"
                )

    # Check for REMOTE mode (port-forward)
    elif config.mode == ConnectionMode.REMOTE and kubectl:
        port = config.local_port
        if local_mgr and local_mgr._is_port_in_use(port):
            logger.info(
                f"Detected process on port {port}, checking if it's port-forward..."
            )
            result["port_forward_detected"] = True

            # Test if port-forward is working
            connection_result = kubectl.check_connection(f"http://localhost:{port}")
            if connection_result.status == ConnectionStatus.CONNECTED:
                result["port_forward_healthy"] = True
                logger.info(f"✓ Reconnected to running port-forward on port {port}")
                config.integrations_url = f"http://localhost:{port}"
                st.session_state.connection_status = ConnectionStatus.CONNECTED
            else:
                logger.warning(
                    f"Port {port} in use but connection failed: {connection_result.error_message}"
                )

    return result


def initialize_bedrock():
    """Initialize AWS Bedrock client and DataHub sketch."""
    config: ConnectionConfig = st.session_state.connection_config

    if st.session_state.bedrock_client is None:
        try:
            with st.spinner("Initializing AWS Bedrock..."):
                if config.aws_profile:
                    session = boto3.Session(
                        profile_name=config.aws_profile, region_name=config.aws_region
                    )
                else:
                    session = boto3.Session(region_name=config.aws_region)

                st.session_state.bedrock_client = session.client("bedrock-runtime")
                logger.info(f"✓ Connected to Bedrock in {config.aws_region}")
        except Exception as e:
            error_msg = str(e)

            # Check if it's an SSO token expiry error
            if "Token has expired" in error_msg or (
                "sso" in error_msg.lower() and "refresh failed" in error_msg.lower()
            ):
                st.error("🔐 AWS SSO token has expired!")

                # Determine which profile to use
                profile_name = config.aws_profile
                if not profile_name:
                    # Try AWS_PROFILE env variable
                    profile_name = os.environ.get("AWS_PROFILE")
                    if not profile_name:
                        # Try to find first available SSO profile
                        available = get_available_aws_profiles()
                        if available:
                            profile_name = available[0]
                            st.warning(
                                f"⚠️ No AWS profile configured - using first available: {profile_name}"
                            )
                            st.info(
                                "💡 Configure AWS profile in Settings tab for a better experience"
                            )
                        else:
                            profile_name = "default"
                            st.warning("⚠️ No AWS profiles found - trying 'default'")

                st.code(f"aws sso login --profile {profile_name}", language="bash")

                # Offer to open SSO login in browser
                if st.button("🌐 Open AWS SSO Login", key="sso_login_btn"):
                    import subprocess

                    try:
                        # Don't capture stdout/stderr - let aws sso login interact with browser
                        subprocess.Popen(
                            ["aws", "sso", "login", "--profile", profile_name],
                            start_new_session=True,
                        )
                        st.success(
                            f"✅ Opening AWS SSO login for profile: {profile_name}"
                        )
                        st.info("After logging in, click 'Refresh' to continue.")
                    except Exception as cmd_error:
                        st.error(f"❌ Failed to run aws sso login: {cmd_error}")
                        st.info(
                            f"Please run manually: aws sso login --profile {profile_name}"
                        )
            else:
                st.error(f"Failed to initialize Bedrock: {e}")
                st.info("Make sure you have AWS credentials configured")

            return False

    if st.session_state.sketch is None:
        try:
            with st.spinner("Sketching DataHub backend..."):
                sketcher = DataHubSketcher(config.gms_url, config.gms_token or "")
                st.session_state.sketch = sketcher.create_sketch()
                logger.info("✓ DataHub sketch created")
        except Exception as e:
            st.error(f"Failed to create DataHub sketch: {e}")
            return False

    if st.session_state.question_generator is None:
        st.session_state.question_generator = ConversationGenerator(
            st.session_state.bedrock_client, st.session_state.sketch
        )

    return True


def _format_duration(seconds: float) -> str:
    """Format duration in seconds to human-readable string."""
    if seconds < 1:
        return f"{int(seconds * 1000)}ms"
    elif seconds < 60:
        return f"{int(seconds)}s"
    else:
        mins = int(seconds // 60)
        secs = int(seconds % 60)
        return f"{mins}m {secs}s"


def _send_message_embedded(
    active_conv: dict,
    proc_state: dict,
    progress_display,
) -> tuple[bool, str | None]:
    """Handle message sending in embedded mode (agent runs in Streamlit process).

    Args:
        active_conv: The active conversation dict
        proc_state: Processing state for this conversation
        progress_display: Streamlit empty container for progress updates

    Returns:
        tuple of (success: bool, error_msg: str | None)
    """
    agent = get_embedded_agent()
    if not agent:
        st.error("❌ Agent not available - check EMBEDDED mode configuration")
        return False, "Agent not available"

    try:
        # Set up progress tracking
        tracker = ProgressTracker() if progress_display else None

        def update_progress(messages):
            if tracker and progress_display and messages:
                from datahub_integrations.chat.utils import parse_reasoning_message

                latest = messages[-1]
                parsed = parse_reasoning_message(latest.text)
                user_visible = parsed.to_user_visible_message()

                # Strip markdown formatting ONLY for checking
                user_visible_stripped = user_visible.lstrip("*").strip()

                # Check for plan header (using stripped version)
                if user_visible_stripped.startswith("Plan:"):
                    # Extract plan name from ORIGINAL (preserves markdown)
                    plan_lines_original = user_visible.split("\n")
                    plan_first_line = plan_lines_original[0]
                    # Remove "Plan:" prefix but keep markdown formatting
                    if "Plan:" in plan_first_line:
                        plan_name = plan_first_line.split("Plan:", 1)[1].strip()
                    else:
                        plan_name = plan_first_line.strip()
                    tracker.set_plan(plan_name)

                    # Process step indicators in the plan (use stripped for parsing)
                    plan_lines_stripped = user_visible_stripped.split("\n")
                    for line in plan_lines_stripped[1:]:
                        line = line.strip().lstrip("*•-").strip()
                        if line and any(
                            icon in line for icon in ["▶", "✓", "⏳", "✗"]
                        ):
                            # Extract step text and icon
                            step_icon = None
                            for icon in ["▶", "✓", "⏳", "✗"]:
                                if icon in line:
                                    step_icon = icon
                                    step_text = line.replace(icon, "").strip()
                                    break

                            if step_icon and step_text:
                                if tracker.update_step(step_text, step_icon):
                                    # Meaningful update - re-render
                                    lines = tracker.render()
                                    rendered = "\n".join(lines)
                                    # Save to conversation processing state for display persistence
                                    proc_state["current_progress_display"] = rendered
                                    # Render with spinner
                                    progress_display.markdown(
                                        render_spinner_with_progress(rendered),
                                        unsafe_allow_html=True,
                                    )

        # Run agent with progress updates
        if tracker:
            with agent.set_progress_callback(update_progress):
                agent.generate_formatted_message()
        else:
            agent.generate_formatted_message()

        # Agent updates its own history, so we just reference it
        # Replace the conversation history with agent's history
        active_conv["history"] = agent.history

        # Save progress tracker for display after rerun
        if tracker:
            proc_state["last_progress_display"] = "\n".join(tracker.render())

        return True, None

    except Exception as e:
        logger.error(f"Embedded agent error: {e}")
        error_msg = str(e)

        # Check for connection errors
        is_connection_error = any(
            err_keyword in error_msg.lower()
            for err_keyword in [
                "connection refused",
                "connection error",
                "failed to connect",
                "network is unreachable",
                "timeout",
                "connection reset",
            ]
        )

        if is_connection_error:
            st.error(f"❌ Connection Error: {error_msg}")
            st.error("⚠️ Cannot connect to the service. Auto-chat will be disabled.")

            # Disable auto-chat on connection errors
            if st.session_state.get("auto_chat_enabled", False):
                st.session_state.auto_chat_enabled = False
                logger.warning("Auto-chat disabled due to connection error")
        else:
            st.error(f"❌ Error: {error_msg}")

        return False, error_msg


def _send_message_http(
    message: str,
    active_conv: dict,
    proc_state: dict,
    config: ConnectionConfig,
    progress_display,
) -> tuple[bool, str | None]:
    """Handle message sending via HTTP streaming to backend service.

    Args:
        message: The user's message text
        active_conv: The active conversation dict
        proc_state: Processing state for this conversation
        config: Connection configuration
        progress_display: Streamlit empty container for progress updates

    Returns:
        tuple of (success: bool, error_msg: str | None)
    """
    chat_client = ChatClient(config.integrations_url, config.gms_token or "")
    success = False
    error_msg = None

    try:
        # Set up progress tracking
        tracker = ProgressTracker() if progress_display else None
        message_count = 0

        from datahub_integrations.chat.chat_history import (
            HumanMessage as HM,
            ReasoningMessage,
        )
        from datahub_integrations.chat.utils import parse_reasoning_message

        for event in chat_client.send_message_stream(
            message, active_conv["urn"], st.session_state.user_urn
        ):
            event_type = event["event_type"]

            if event_type == "message":
                # New message arrived - add to history and update progress
                new_msg = event["message"]

                # Skip duplicate user message
                if isinstance(new_msg, HM) and new_msg.text == message:
                    continue

                active_conv["history"].add_message(new_msg)
                message_count += 1

                # Update progress display if provided
                if tracker and progress_display and isinstance(new_msg, ReasoningMessage):
                    try:
                        parsed = parse_reasoning_message(new_msg.text)
                        user_visible = parsed.to_user_visible_message()

                        # Strip markdown formatting ONLY for checking
                        user_visible_stripped = user_visible.lstrip("*").strip()

                        # Check for plan header (using stripped version)
                        if user_visible_stripped.startswith("Plan:"):
                            # Extract plan name from ORIGINAL (preserves markdown)
                            plan_lines_original = user_visible.split("\n")
                            plan_first_line = plan_lines_original[0]
                            # Remove "Plan:" prefix but keep markdown formatting
                            if "Plan:" in plan_first_line:
                                plan_name = plan_first_line.split("Plan:", 1)[1].strip()
                            else:
                                plan_name = plan_first_line.strip()
                            tracker.set_plan(plan_name)

                            # Process step indicators in the plan (use stripped for parsing)
                            plan_lines_stripped = user_visible_stripped.split("\n")
                            for line in plan_lines_stripped[1:]:
                                line = line.strip().lstrip("*•-").strip()
                                if line and any(
                                    icon in line for icon in ["▶", "✓", "⏳", "✗"]
                                ):
                                    # Extract step text and icon
                                    step_icon = None
                                    for icon in ["▶", "✓", "⏳", "✗"]:
                                        if icon in line:
                                            step_icon = icon
                                            step_text = line.replace(icon, "").strip()
                                            break

                                    if step_icon and step_text:
                                        if tracker.update_step(
                                            step_text, step_icon
                                        ):
                                            # Meaningful update - re-render
                                            lines = tracker.render()
                                            rendered = "\n".join(lines)
                                            # Save to conversation processing state for display persistence
                                            proc_state["current_progress_display"] = rendered
                                            # Render with spinner
                                            progress_display.markdown(
                                                render_spinner_with_progress(rendered),
                                                unsafe_allow_html=True,
                                            )
                    except Exception:
                        # Fallback if parsing fails
                        pass

            elif event_type == "complete":
                success = True
                break

            elif event_type == "error":
                success = False
                error_msg = event["error"]

                # Check for connection errors
                is_connection_error = any(
                    err_keyword in str(error_msg).lower()
                    for err_keyword in [
                        "connection refused",
                        "connection error",
                        "failed to connect",
                        "network is unreachable",
                        "timeout",
                        "connection reset",
                        "errno 61",
                    ]
                )

                if is_connection_error:
                    st.error(f"❌ Connection Error: {error_msg}")
                    st.error(
                        "⚠️ Cannot connect to the integrations service. Auto-chat will be disabled."
                    )

                    if st.session_state.get("auto_chat_enabled", False):
                        st.session_state.auto_chat_enabled = False
                        logger.warning("Auto-chat disabled due to connection error in stream")
                else:
                    st.error(f"❌ Error: {error_msg}")

                break

        # Save progress tracker for display after rerun
        if tracker:
            proc_state["last_progress_display"] = "\n".join(tracker.render())

    except Exception as e:
        logger.error(f"HTTP stream error: {e}")
        error_msg = str(e)

        # Check for connection errors in exception
        is_connection_error = any(
            err_keyword in error_msg.lower()
            for err_keyword in [
                "connection refused",
                "connection error",
                "failed to connect",
                "network is unreachable",
                "timeout",
                "connection reset",
                "errno 61",
            ]
        )

        if is_connection_error:
            st.error(f"❌ Connection Error: {error_msg}")
            st.error(
                "⚠️ Cannot connect to the integrations service. Auto-chat will be disabled."
            )

            if st.session_state.get("auto_chat_enabled", False):
                st.session_state.auto_chat_enabled = False
                logger.warning("Auto-chat disabled due to connection error")
        else:
            st.error(f"❌ Error: {error_msg}")

        success = False

    return success, error_msg


def send_message(message: str, is_auto: bool = False, progress_display=None):
    """Send a message and update the conversation with rich ChatHistory.

    Dispatches to the appropriate handler based on connection mode (embedded vs HTTP).

    Args:
        message: The user's message
        is_auto: Whether this is an auto-generated message
        progress_display: Optional st.empty() container for progress updates
    """
    import time

    config: ConnectionConfig = st.session_state.connection_config

    # Get processing conversation (not viewing)
    active_conv = get_processing_conversation()

    # Get per-conversation processing state
    proc_state = get_processing_state(active_conv["id"])

    # Initialize history if needed
    if "history" not in active_conv:
        from datahub_integrations.chat.chat_history import ChatHistory

        active_conv["history"] = ChatHistory()

    # Note: User message should already be in history (added before calling this function)
    # This allows the message to appear on screen before processing starts
    # Progress display flags are now managed per-conversation in proc_state

    start_time = time.time()

    # Dispatch to appropriate handler based on connection mode
    if config.mode == ConnectionMode.EMBEDDED:
        success, error_msg = _send_message_embedded(
            active_conv, proc_state, progress_display
        )
    else:
        # HTTP modes (LOCAL, REMOTE, LOCAL_SERVICE, CUSTOM)
        success, error_msg = _send_message_http(
            message, active_conv, proc_state, config, progress_display
        )

    # Cleanup: Calculate duration and update stats
    duration = time.time() - start_time
    st.session_state.stats["total_messages"] += 1
    st.session_state.stats["total_duration"] += duration

    if success:
        st.session_state.stats["success_count"] += 1
    else:
        st.session_state.stats["error_count"] += 1

    # Add assistant response to messages array
    # (User message should already be in the array before send_message is called)
    # Count events from ChatHistory (it's a Pydantic object, not a dict)
    event_count = 0
    if "history" in active_conv and active_conv["history"] is not None:
        event_count = len(active_conv["history"].messages)

    assistant_msg = {
        "role": "assistant",
        "content": "Response generated" if success else f"Error: {error_msg}",
        "timestamp": datetime.now(),
        "duration": duration,
        "event_count": event_count,
        "success": success,
        "is_auto": is_auto,
    }
    active_conv["messages"].append(assistant_msg)
    st.session_state.messages.append(assistant_msg)

    # Update stats
    if is_auto:
        st.session_state.auto_chat_count += 1


def render_connection_settings():
    """Render the connection settings UI panel."""
    config: ConnectionConfig = st.session_state.connection_config
    manager: ConnectionManager = st.session_state.connection_manager
    kubectl: Optional[KubectlManager] = st.session_state.kubectl_manager

    # Smart detection: check for running processes on first render
    if not st.session_state.process_detection_done:
        detection_result = detect_and_reconnect_processes()
        st.session_state.process_detection_done = True

        # Show reconnection status outside the expander
        if detection_result["local_service_healthy"]:
            st.success(
                f"🔄 Reconnected to running integrations service on port {config.local_service_port}"
            )
        elif detection_result["local_service_detected"]:
            st.warning(
                f"⚠️ Port {config.local_service_port} in use but service unhealthy. Use 'Stop Service' or restart."
            )

        if detection_result["port_forward_healthy"]:
            st.success(
                f"🔄 Reconnected to running port-forward on port {config.local_port}"
            )
        elif detection_result["port_forward_detected"]:
            st.warning(
                f"⚠️ Port {config.local_port} in use but connection failed. Stop and restart port-forward."
            )

    with st.expander("⚙️ Connection Settings", expanded=True):
        # Connection mode selector
        mode_options = [
            "Local",
            "Remote GMS + Local Service",
            "Remote (kubectl)",
            "Custom URL",
            "Embedded Agent",
        ]
        mode_map = {
            "Local": ConnectionMode.LOCAL,
            "Remote GMS + Local Service": ConnectionMode.LOCAL_SERVICE,
            "Remote (kubectl)": ConnectionMode.REMOTE,
            "Custom URL": ConnectionMode.CUSTOM,
            "Embedded Agent": ConnectionMode.EMBEDDED,
        }
        reverse_mode_map = {v: k for k, v in mode_map.items()}

        current_mode_label = reverse_mode_map.get(config.mode, "Local")
        selected_mode_label = st.selectbox(
            "Connection Mode",
            mode_options,
            index=mode_options.index(current_mode_label),
        )
        selected_mode = mode_map[selected_mode_label]

        st.divider()

        # AWS Profile selector (applies to all modes that use AWS)
        st.markdown("### 🔐 AWS Configuration")
        st.caption("Used for Parameter Store token retrieval and Bedrock (auto-chat)")

        # Get available profiles
        available_profiles = get_available_aws_profiles()
        env_profile = os.environ.get("AWS_PROFILE")

        # Determine default selection
        if available_profiles:
            # Options: None (use env/default) + actual profiles
            profile_options = [
                "(Auto: use AWS_PROFILE env or default)"
            ] + available_profiles

            # Find current selection
            if config.aws_profile and config.aws_profile in available_profiles:
                default_index = available_profiles.index(config.aws_profile) + 1
            elif env_profile:
                default_index = 0  # Show auto mode
            else:
                default_index = 0

            selected_profile_option = st.selectbox(
                "AWS Profile",
                profile_options,
                index=default_index,
                help=f"Currently: AWS_PROFILE={env_profile or 'not set'}"
                if env_profile
                else "Select AWS profile for credentials",
            )

            # Update config based on selection
            if selected_profile_option == "(Auto: use AWS_PROFILE env or default)":
                config.aws_profile = None  # Will use env or default
                if env_profile:
                    st.info(f"💡 Using AWS_PROFILE={env_profile} from environment")
                else:
                    st.info("💡 Using default AWS credentials chain")
            else:
                config.aws_profile = selected_profile_option
        else:
            # No profiles found
            st.warning("⚠️ No AWS profiles found in ~/.aws/config or ~/.aws/credentials")
            if env_profile:
                st.info(f"💡 Will use AWS_PROFILE={env_profile} from environment")
                config.aws_profile = None
            else:
                st.info("💡 Will use default AWS credentials if available")
                config.aws_profile = None

        st.divider()

        # Remote (kubectl) mode
        if selected_mode == ConnectionMode.REMOTE:
            if kubectl is None:
                st.error(
                    "⚠️ kubectl not found. Please install kubectl to use remote mode."
                )
                st.markdown(
                    "[Install kubectl](https://kubernetes.io/docs/tasks/tools/)"
                )
            else:
                # Context selector
                try:
                    contexts = kubectl.get_contexts()
                    current_context = kubectl.get_current_context()

                    st.info(f"📍 Current context: **{current_context}**")

                    if len(contexts) > 1:
                        # Find current selection index
                        current_idx = (
                            contexts.index(current_context)
                            if current_context in contexts
                            else 0
                        )

                        selected_context = st.selectbox(
                            "Kubernetes Context",
                            contexts,
                            index=current_idx,
                            help="Select the k8s context to use for port-forwarding",
                        )

                        if selected_context != current_context:
                            if st.button(
                                "🔄 Switch to this context", key="switch_context_btn"
                            ):
                                with st.spinner(
                                    f"Switching to context: {selected_context}..."
                                ):
                                    result = subprocess.run(
                                        [
                                            "kubectl",
                                            "config",
                                            "use-context",
                                            selected_context,
                                        ],
                                        capture_output=True,
                                        text=True,
                                    )
                                    if result.returncode == 0:
                                        st.success(
                                            f"✓ Switched to context: {selected_context}"
                                        )
                                        config.kube_context = selected_context
                                        st.rerun()
                                    else:
                                        st.error(
                                            f"Failed to switch context: {result.stderr}"
                                        )
                        else:
                            config.kube_context = selected_context

                    # Namespace selector
                    try:
                        namespaces = kubectl.get_namespaces(config.kube_context)

                        namespace = st.selectbox(
                            "Namespace (from cluster)",
                            namespaces,
                            index=namespaces.index(config.kube_namespace)
                            if config.kube_namespace in namespaces
                            else 0,
                        )

                        # Add manual entry option (starts empty, overrides dropdown if filled)
                        namespace_input = st.text_input(
                            "Or type namespace manually (overrides dropdown)",
                            value="",
                            placeholder="my-custom-namespace",
                            help="Only fill this if your namespace isn't in the dropdown above",
                        )

                        # Use manual input if provided, otherwise use dropdown
                        config.kube_namespace = (
                            namespace_input if namespace_input.strip() else namespace
                        )
                    except RuntimeError as e:
                        error_msg = str(e)
                        if "VPN" in error_msg:
                            st.error(f"🔌 {error_msg}", icon="🔌")
                            st.info(
                                "💡 Connect to VPN, then refresh this page or change the context dropdown to retry"
                            )
                        else:
                            st.error(f"❌ Failed to get namespaces: {error_msg}")

                        # Still allow manual namespace entry even if kubectl fails
                        config.kube_namespace = st.text_input(
                            "Enter namespace manually",
                            value=config.kube_namespace or "",
                            placeholder="my-namespace",
                            help="Type the namespace name since auto-discovery failed",
                        )

                    st.divider()

                    # Quick Connect button - auto-discover everything!
                    st.markdown("### 🚀 Quick Connect")
                    st.caption(
                        "Auto-discover GMS URL, get token, and connect to integrations service"
                    )

                    if st.button(
                        "⚡ Quick Connect", use_container_width=True, type="primary"
                    ):
                        success_count = 0
                        with st.spinner(
                            f"🔍 Auto-discovering services in {config.kube_namespace}..."
                        ):
                            # 1. Auto-discover GMS URL
                            gms_url = kubectl.get_gms_url_from_namespace(
                                config.kube_namespace, config.kube_context
                            )
                            if gms_url:
                                # Clear sketch if GMS URL changed
                                if config.gms_url != gms_url:
                                    clear_sketch()
                                config.gms_url = gms_url
                                st.success(f"✓ Discovered GMS: {gms_url}")
                                success_count += 1
                            else:
                                st.info(
                                    "💡 Tip: GMS URL usually follows pattern: `https://<instance>.acryl.io/api/gms`"
                                )

                            # 2. Get DataHub token (needs GMS URL first)
                            if gms_url:
                                # Convert GMS URL to frontend URL (remove /api/gms suffix)
                                frontend_url = gms_url.replace("/api/gms", "")
                                token = kubectl.get_datahub_token(
                                    config.kube_namespace,
                                    frontend_url,
                                    config.kube_context,
                                    config.aws_profile,
                                )
                                if token:
                                    config.gms_token = token
                                    st.success("✓ Got DataHub token")
                                    success_count += 1
                                else:
                                    st.info(
                                        "💡 Tip: Token generation requires AWS credentials configured"
                                    )
                            else:
                                st.info("💡 Tip: Need GMS URL first to generate token")

                            # 3. Find integrations service pods
                            pods = kubectl.find_integrations_service_pods(
                                config.kube_namespace, config.kube_context
                            )
                            if pods:
                                st.session_state.available_pods = pods
                                config.pod_name = pods[0].name
                                st.success(
                                    f"✓ Found {len(pods)} integrations service pod(s): {pods[0].name}"
                                )
                                success_count += 1

                                # 4. Start port-forward automatically
                                try:
                                    # Find available port if 9003 is in use
                                    local_port = config.local_port
                                    if kubectl.is_port_in_use(local_port):
                                        st.info(
                                            f"💡 Port {local_port} is in use, finding available port..."
                                        )
                                        # Try ports 9003-9020
                                        for port in range(9003, 9021):
                                            if not kubectl.is_port_in_use(port):
                                                local_port = port
                                                config.local_port = port
                                                st.info(f"Using port {local_port}")
                                                break
                                        else:
                                            raise RuntimeError(
                                                "No available ports in range 9003-9020"
                                            )

                                    process = kubectl.start_port_forward(
                                        config.pod_name,
                                        config.kube_namespace,
                                        local_port,
                                        config.remote_port,
                                        config.kube_context,
                                    )
                                    st.session_state.port_forward_process = process
                                    config.integrations_url = (
                                        f"http://localhost:{local_port}"
                                    )
                                    st.session_state.connection_status = (
                                        ConnectionStatus.CONNECTED
                                    )
                                    st.success(
                                        f"✓ Connected! Port-forward: localhost:{local_port}"
                                    )
                                    # Save this connection as active for next time
                                    save_connection_as_active()
                                    success_count += 1
                                    st.balloons()
                                except Exception as e:
                                    st.error(f"Failed to start port-forward: {e}")
                            else:
                                st.info(
                                    "💡 Tip: Check namespace. Searched labels: app=datahub-integrations"
                                )

                        if success_count == 0:
                            st.warning(
                                "⚠️ Auto-discovery didn't find anything. Please use manual configuration below."
                            )

                    st.divider()
                    st.markdown("### 🔧 Manual Configuration")
                    st.caption("Or configure manually if auto-discovery doesn't work")

                    # Pod selector with option to show all pods
                    col1, col2 = st.columns(2)
                    with col1:
                        if st.button(
                            "🔍 Find Integrations Pods", use_container_width=True
                        ):
                            with st.spinner(
                                "Searching for integrations service pods..."
                            ):
                                pods = kubectl.find_integrations_service_pods(
                                    config.kube_namespace, config.kube_context
                                )
                                st.session_state.available_pods = pods
                                if pods:
                                    st.success(f"Found {len(pods)} pod(s)")
                                else:
                                    st.warning(
                                        "No integrations service pods found with standard labels"
                                    )

                    with col2:
                        if st.button("📋 Show All Pods", use_container_width=True):
                            with st.spinner(
                                f"Listing all pods in {config.kube_namespace}..."
                            ):
                                all_pods = kubectl.get_pods(
                                    config.kube_namespace,
                                    label_selector=None,
                                    context=config.kube_context,
                                )
                                st.session_state.available_pods = all_pods
                                if all_pods:
                                    st.info(f"Found {len(all_pods)} pod(s) total")
                                else:
                                    st.error("No pods found in namespace")

                    if (
                        "available_pods" in st.session_state
                        and st.session_state.available_pods
                    ):
                        pod_options = [p.name for p in st.session_state.available_pods]
                        selected_pod = st.selectbox("Pod", pod_options)
                        config.pod_name = selected_pod

                        # Port configuration
                        col1, col2 = st.columns(2)
                        with col1:
                            config.local_port = st.number_input(
                                "Local Port",
                                value=config.local_port,
                                min_value=1024,
                                max_value=65535,
                            )
                        with col2:
                            config.remote_port = st.number_input(
                                "Remote Port",
                                value=config.remote_port,
                                min_value=1,
                                max_value=65535,
                            )

                        # Connect/Disconnect button
                        if st.session_state.port_forward_process is None:
                            if st.button(
                                "🔌 Connect", use_container_width=True, type="primary"
                            ):
                                with st.spinner(
                                    f"Starting port-forward to {selected_pod}..."
                                ):
                                    try:
                                        # Find available port if default is in use
                                        local_port = config.local_port
                                        if kubectl.is_port_in_use(local_port):
                                            st.info(
                                                f"💡 Port {local_port} is in use, finding available port..."
                                            )
                                            for port in range(9003, 9021):
                                                if not kubectl.is_port_in_use(port):
                                                    local_port = port
                                                    config.local_port = port
                                                    st.info(f"Using port {local_port}")
                                                    break
                                            else:
                                                raise RuntimeError(
                                                    "No available ports in range 9003-9020"
                                                )

                                        process = kubectl.start_port_forward(
                                            selected_pod,
                                            namespace,
                                            local_port,
                                            config.remote_port,
                                            config.kube_context,
                                        )
                                        st.session_state.port_forward_process = process
                                        config.integrations_url = (
                                            f"http://localhost:{local_port}"
                                        )
                                        st.session_state.connection_status = (
                                            ConnectionStatus.CONNECTED
                                        )
                                        st.success(f"✓ Connected to {selected_pod}")
                                        # Save this connection as active for next time
                                        save_connection_as_active()
                                        st.rerun()
                                    except Exception as e:
                                        st.error(f"Failed to connect: {e}")
                        else:
                            st.success(f"✓ Connected to localhost:{config.local_port}")
                            if st.button("🔴 Disconnect", use_container_width=True):
                                kubectl.stop_port_forward(
                                    st.session_state.port_forward_process
                                )
                                st.session_state.port_forward_process = None
                                st.session_state.connection_status = (
                                    ConnectionStatus.DISCONNECTED
                                )
                                st.rerun()

                except Exception as e:
                    st.error(f"Error: {e}")

        # Local Service mode (Remote GMS + Local Integrations)
        elif selected_mode == ConnectionMode.LOCAL_SERVICE:
            local_mgr: Optional[LocalIntegrationsManager] = (
                st.session_state.local_integrations_manager
            )

            if local_mgr is None:
                st.error("⚠️ Local integrations manager not available.")
                st.info(
                    "💡 Make sure you're running from datahub-integrations-service directory"
                )
            else:
                st.info("🏠 Run local integrations service connected to remote GMS")

                if kubectl is None:
                    st.error(
                        "⚠️ kubectl not found. Please install kubectl to auto-discover GMS credentials."
                    )
                    st.markdown(
                        "[Install kubectl](https://kubernetes.io/docs/tasks/tools/)"
                    )
                else:
                    # Context and namespace selector for auto-discovery
                    try:
                        contexts = kubectl.get_contexts()
                        current_context = kubectl.get_current_context()

                        st.info(f"📍 Current context: **{current_context}**")

                        if len(contexts) > 1:
                            # Find current selection index
                            current_idx = (
                                contexts.index(current_context)
                                if current_context in contexts
                                else 0
                            )

                            selected_context = st.selectbox(
                                "Kubernetes Context",
                                contexts,
                                index=current_idx,
                                help="Select the k8s context to use for auto-discovery",
                                key="local_service_context",
                            )

                            if selected_context != current_context:
                                if st.button(
                                    "🔄 Switch to this context",
                                    key="switch_context_local_btn",
                                ):
                                    with st.spinner(
                                        f"Switching to context: {selected_context}..."
                                    ):
                                        result = subprocess.run(
                                            [
                                                "kubectl",
                                                "config",
                                                "use-context",
                                                selected_context,
                                            ],
                                            capture_output=True,
                                            text=True,
                                        )
                                        if result.returncode == 0:
                                            st.success(
                                                f"✓ Switched to context: {selected_context}"
                                            )
                                            config.kube_context = selected_context
                                            st.rerun()
                                        else:
                                            st.error(
                                                f"Failed to switch context: {result.stderr}"
                                            )
                            else:
                                config.kube_context = selected_context

                        # Namespace selector
                        namespaces = kubectl.get_namespaces(config.kube_context)

                        namespace = st.selectbox(
                            "Namespace (from cluster)",
                            namespaces,
                            index=namespaces.index(config.kube_namespace)
                            if config.kube_namespace in namespaces
                            else 0,
                        )

                        # Add manual entry option
                        namespace_input = st.text_input(
                            "Or type namespace manually (overrides dropdown)",
                            value="",
                            placeholder="my-custom-namespace",
                            help="Only fill this if your namespace isn't in the dropdown above",
                        )

                        # Use manual input if provided, otherwise use dropdown
                        config.kube_namespace = (
                            namespace_input if namespace_input.strip() else namespace
                        )

                        st.divider()

                        # Quick Connect button - auto-discover and start service!
                        st.markdown("### 🚀 Quick Setup & Start")
                        st.caption(
                            "Auto-discover GMS credentials and start local service with one click"
                        )

                        if st.button(
                            "⚡ Auto-Configure & Start",
                            use_container_width=True,
                            type="primary",
                        ):
                            success_count = 0
                            with st.spinner(
                                f"🔍 Auto-discovering GMS credentials from {config.kube_namespace}..."
                            ):
                                # 1. Auto-discover GMS URL
                                gms_url = kubectl.get_gms_url_from_namespace(
                                    config.kube_namespace, config.kube_context
                                )
                                if gms_url:
                                    # Clear sketch if GMS URL changed
                                    if config.gms_url != gms_url:
                                        clear_sketch()
                                    config.gms_url = gms_url
                                    st.success(f"✓ Discovered GMS: {gms_url}")
                                    success_count += 1
                                else:
                                    st.error("❌ Could not discover GMS URL")
                                    st.info(
                                        "💡 Tip: GMS URL usually follows pattern: `https://<instance>.acryl.io/api/gms`"
                                    )

                                # 2. Get DataHub token
                                if gms_url:
                                    frontend_url = gms_url.replace("/api/gms", "")
                                    token = kubectl.get_datahub_token(
                                        config.kube_namespace,
                                        frontend_url,
                                        config.kube_context,
                                        config.aws_profile,
                                    )
                                    if token:
                                        config.gms_token = token
                                        st.success("✓ Got DataHub token")
                                        success_count += 1
                                    else:
                                        st.error("❌ Failed to generate token")
                                        st.info(
                                            "💡 Tip: Token generation requires AWS credentials and admin password stored in Parameter Store. You can configure the token manually below."
                                        )
                                else:
                                    st.error("❌ Need GMS URL first to generate token")

                                # 3. Start local service if we have credentials
                                if success_count == 2:
                                    with st.spinner(
                                        f"🚀 Starting local integrations service on port {config.local_service_port}..."
                                    ):
                                        try:
                                            # Stop any existing service on the port first
                                            if local_mgr._is_port_in_use(
                                                config.local_service_port
                                            ):
                                                st.info(
                                                    f"🔄 Port {config.local_service_port} is in use, stopping existing service..."
                                                )

                                                # If we have a tracked process, stop it cleanly
                                                if (
                                                    st.session_state.local_service_process
                                                    is not None
                                                ):
                                                    local_mgr.stop_service(
                                                        st.session_state.local_service_process
                                                    )
                                                    st.session_state.local_service_process = None
                                                else:
                                                    # Unknown process on port - try to kill it
                                                    try:
                                                        # Find all PIDs on port (macOS/Linux)
                                                        result = subprocess.run(
                                                            [
                                                                "lsof",
                                                                "-ti",
                                                                f":{config.local_service_port}",
                                                            ],
                                                            capture_output=True,
                                                            text=True,
                                                            timeout=5,
                                                        )
                                                        if result.stdout.strip():
                                                            pids = result.stdout.strip().split(
                                                                "\n"
                                                            )
                                                            logger.info(
                                                                f"Found PIDs on port {config.local_service_port}: {pids}"
                                                            )

                                                            for pid in pids:
                                                                pid = pid.strip()
                                                                if pid:
                                                                    logger.info(
                                                                        f"Killing PID {pid}"
                                                                    )
                                                                    # Try SIGTERM first
                                                                    subprocess.run(
                                                                        ["kill", pid],
                                                                        timeout=5,
                                                                        check=False,
                                                                    )

                                                            # Wait a moment for graceful shutdown
                                                            time.sleep(2)

                                                            # Check if still running, then force kill
                                                            result2 = subprocess.run(
                                                                [
                                                                    "lsof",
                                                                    "-ti",
                                                                    f":{config.local_service_port}",
                                                                ],
                                                                capture_output=True,
                                                                text=True,
                                                                timeout=5,
                                                            )
                                                            if result2.stdout.strip():
                                                                remaining_pids = result2.stdout.strip().split(
                                                                    "\n"
                                                                )
                                                                logger.info(
                                                                    f"Force killing remaining PIDs: {remaining_pids}"
                                                                )
                                                                for (
                                                                    pid
                                                                ) in remaining_pids:
                                                                    pid = pid.strip()
                                                                    if pid:
                                                                        subprocess.run(
                                                                            [
                                                                                "kill",
                                                                                "-9",
                                                                                pid,
                                                                            ],
                                                                            timeout=5,
                                                                            check=False,
                                                                        )

                                                            st.info(
                                                                f"✓ Stopped existing service (PIDs: {', '.join(pids)})"
                                                            )
                                                    except Exception as e:
                                                        logger.error(
                                                            f"Could not kill process on port: {e}"
                                                        )
                                                        st.warning(
                                                            f"⚠️ Could not automatically stop service: {e}"
                                                        )

                                                # Wait for port to be released (with retry)
                                                max_wait = 8
                                                for i in range(max_wait):
                                                    time.sleep(1)
                                                    if not local_mgr._is_port_in_use(
                                                        config.local_service_port
                                                    ):
                                                        logger.info(
                                                            f"Port {config.local_service_port} is now available after {i + 1}s"
                                                        )
                                                        st.info(
                                                            f"✓ Port {config.local_service_port} is now available"
                                                        )
                                                        break
                                                    else:
                                                        logger.debug(
                                                            f"Port {config.local_service_port} still in use, waiting... ({i + 1}/{max_wait})"
                                                        )
                                                    if i == max_wait - 1:
                                                        # Final check for what's using the port
                                                        result_final = subprocess.run(
                                                            [
                                                                "lsof",
                                                                "-ti",
                                                                f":{config.local_service_port}",
                                                            ],
                                                            capture_output=True,
                                                            text=True,
                                                        )
                                                        remaining = (
                                                            result_final.stdout.strip()
                                                        )
                                                        error_msg = f"Port {config.local_service_port} still in use after {max_wait}s. PIDs: {remaining}"
                                                        logger.error(error_msg)
                                                        raise RuntimeError(error_msg)

                                            service_config = IntegrationsServiceConfig(
                                                gms_url=config.gms_url,
                                                gms_token=config.gms_token,
                                                service_port=config.local_service_port,
                                                aws_region=config.aws_region,
                                                aws_profile=config.aws_profile,
                                            )
                                            process = local_mgr.start_service(
                                                service_config
                                            )
                                            st.session_state.local_service_process = (
                                                process
                                            )
                                            config.integrations_url = (
                                                f"http://localhost:{process.port}"
                                            )
                                            config.use_local_service = True
                                            st.success(
                                                f"✓ Service started on port {process.port}"
                                            )
                                            # Save this connection as active for next time
                                            save_connection_as_active()
                                            st.balloons()
                                            st.rerun()
                                        except Exception as e:
                                            st.error(f"Failed to start service: {e}")
                                else:
                                    st.warning(
                                        "⚠️ Auto-discovery incomplete. Please configure manually below."
                                    )

                        st.divider()
                        st.markdown("### 🔧 Manual Configuration")
                        st.caption(
                            "Or configure manually if auto-discovery doesn't work"
                        )

                    except Exception as e:
                        st.error(f"Error: {e}")

                # Service port configuration
                config.local_service_port = st.number_input(
                    "Local Service Port",
                    value=config.local_service_port,
                    min_value=1024,
                    max_value=65535,
                    help="Port for local integrations service",
                )

                # Check if service is already running
                current_process: Optional[ServiceProcess] = (
                    st.session_state.local_service_process
                )

                # Check if port is in use (even if not managed by us)
                port_in_use = local_mgr._is_port_in_use(config.local_service_port)

                if current_process is not None:
                    # Service is managed by us
                    st.success(
                        f"✓ Local service running on port {current_process.port}"
                    )
                    st.text(f"PID: {current_process.pid}")
                    if current_process.log_file:
                        st.text(f"Logs: {current_process.log_file}")

                    col1, col2 = st.columns(2)
                    with col1:
                        if st.button("🔄 Restart Service", use_container_width=True):
                            with st.spinner("Restarting service..."):
                                try:
                                    new_process = local_mgr.restart_service(
                                        current_process
                                    )
                                    st.session_state.local_service_process = new_process
                                    config.integrations_url = (
                                        f"http://localhost:{new_process.port}"
                                    )
                                    # Clear sketch since we might be connecting to a different GMS
                                    clear_sketch()
                                    st.success("✓ Service restarted")
                                    st.rerun()
                                except Exception as e:
                                    st.error(f"Failed to restart: {e}")

                    with col2:
                        if st.button("🛑 Stop Service", use_container_width=True):
                            with st.spinner("Stopping service..."):
                                try:
                                    local_mgr.stop_service(current_process)
                                    st.session_state.local_service_process = None
                                    # Clear sketch when stopping service
                                    clear_sketch()
                                    st.success("✓ Service stopped")
                                    st.rerun()
                                except Exception as e:
                                    st.error(f"Failed to stop: {e}")

                elif port_in_use:
                    # Port is in use by external process (detected but not managed)
                    st.info(
                        f"ℹ️ Port {config.local_service_port} is in use (externally managed)"
                    )

                    # Check if it's actually healthy
                    health = local_mgr.check_service_health(config.local_service_port)
                    if health.get("status") == "healthy":
                        st.success(
                            f"✓ Service is healthy (response time: {health.get('response_time_ms', 0):.0f}ms)"
                        )
                        if (
                            config.integrations_url
                            != f"http://localhost:{config.local_service_port}"
                        ):
                            config.integrations_url = (
                                f"http://localhost:{config.local_service_port}"
                            )
                    else:
                        st.warning("⚠️ Port occupied but service not responding")

                    col1, col2 = st.columns(2)
                    with col1:
                        if st.button("🔄 Restart & Manage", use_container_width=True):
                            st.info(
                                "This will stop the external service and start a managed one..."
                            )
                            # The existing "Start Local Service" logic will handle this
                            st.rerun()

                    with col2:
                        if st.button("🧹 Clean Up Port", use_container_width=True):
                            with st.spinner(
                                f"Stopping process on port {config.local_service_port}..."
                            ):
                                try:
                                    result = subprocess.run(
                                        [
                                            "lsof",
                                            "-ti",
                                            f":{config.local_service_port}",
                                        ],
                                        capture_output=True,
                                        text=True,
                                        timeout=5,
                                    )
                                    if result.stdout.strip():
                                        pid = result.stdout.strip().split()[0]
                                        subprocess.run(["kill", "-9", pid], timeout=5)
                                        st.success(f"✓ Stopped process (PID: {pid})")
                                        time.sleep(1)
                                        st.rerun()
                                    else:
                                        st.warning("No process found on port")
                                except Exception as e:
                                    st.error(f"Failed to clean up: {e}")

                    st.divider()

                else:
                    # No service running - show normal start options
                    pass  # Continue to start service section below

                    # Show start button only if credentials are already configured
                    if config.gms_url and config.gms_token:
                        st.success("✓ GMS credentials configured")
                        st.text(f"GMS: {config.gms_url}")
                        st.text(f"Token: {'*' * 20}...")

                        if st.button(
                            "🚀 Start Local Service",
                            use_container_width=True,
                            type="primary",
                        ):
                            with st.spinner(
                                f"Starting local integrations service on port {config.local_service_port}..."
                            ):
                                try:
                                    # Stop any existing service on the port first
                                    if local_mgr._is_port_in_use(
                                        config.local_service_port
                                    ):
                                        st.info(
                                            f"🔄 Port {config.local_service_port} is in use, stopping existing service..."
                                        )

                                        # If we have a tracked process, stop it cleanly
                                        if (
                                            st.session_state.local_service_process
                                            is not None
                                        ):
                                            local_mgr.stop_service(
                                                st.session_state.local_service_process
                                            )
                                            st.session_state.local_service_process = (
                                                None
                                            )
                                        else:
                                            # Unknown process on port - try to kill it
                                            try:
                                                # Find and kill process on port (macOS/Linux)
                                                result = subprocess.run(
                                                    [
                                                        "lsof",
                                                        "-ti",
                                                        f":{config.local_service_port}",
                                                    ],
                                                    capture_output=True,
                                                    text=True,
                                                    timeout=5,
                                                )
                                                if result.stdout.strip():
                                                    pid = result.stdout.strip().split()[
                                                        0
                                                    ]
                                                    subprocess.run(
                                                        ["kill", "-9", pid], timeout=5
                                                    )
                                                    st.info(
                                                        f"✓ Stopped existing service (PID: {pid})"
                                                    )
                                                    time.sleep(
                                                        1
                                                    )  # Give it a moment to release the port
                                            except Exception as e:
                                                logger.warning(
                                                    f"Could not kill process on port: {e}"
                                                )

                                    service_config = IntegrationsServiceConfig(
                                        gms_url=config.gms_url,
                                        gms_token=config.gms_token,
                                        service_port=config.local_service_port,
                                        aws_region=config.aws_region,
                                        aws_profile=config.aws_profile,
                                    )
                                    process = local_mgr.start_service(service_config)
                                    st.session_state.local_service_process = process
                                    config.integrations_url = (
                                        f"http://localhost:{process.port}"
                                    )
                                    config.use_local_service = True
                                    st.success(
                                        f"✓ Service started on port {process.port}"
                                    )
                                    # Save this connection as active for next time
                                    save_connection_as_active()
                                    st.balloons()
                                    st.rerun()
                                except Exception as e:
                                    st.error(f"Failed to start service: {e}")
                    else:
                        st.info(
                            "💡 Use 'Auto-Configure & Start' above to get GMS credentials via kubectl"
                        )

        # Custom URL mode
        elif selected_mode == ConnectionMode.CUSTOM:
            config.integrations_url = st.text_input(
                "Integrations Service URL",
                value=config.integrations_url,
                placeholder="http://localhost:9003",
            )

        # Embedded agent mode
        elif selected_mode == ConnectionMode.EMBEDDED:
            st.success("✓ Running agent directly in Streamlit (no HTTP calls)")
            st.info(
                "💡 Agent will be initialized with GMS credentials below. Provides real-time progress updates."
            )
            st.info(
                "⚠️ Note: Requires agent dependencies to be installed. Will show error if not available."
            )
            # No integrations URL needed for embedded mode
            config.integrations_url = "embedded://local"

        # Local mode (default)
        else:  # ConnectionMode.LOCAL
            config.integrations_url = "http://localhost:9003"
            st.info(f"🏠 Using local service: {config.integrations_url}")

        st.divider()

        # GMS Configuration
        st.subheader("🗄️ DataHub GMS")

        # For LOCAL_SERVICE mode, GMS config is auto-discovered via kubectl
        # Show as read-only display
        if selected_mode == ConnectionMode.LOCAL_SERVICE:
            if config.gms_url:
                st.text_input(
                    "GMS URL (Auto-discovered)",
                    value=config.gms_url,
                    disabled=True,
                    help="Auto-discovered via kubectl. Use 'Auto-Configure & Start' to update.",
                )
            else:
                st.info("💡 GMS URL will be auto-discovered via kubectl")

            if config.gms_token:
                st.text_input(
                    "GMS Token (Auto-generated)",
                    value="*" * 40,
                    type="password",
                    disabled=True,
                    help="Auto-generated via kubectl. Use 'Auto-Configure & Start' to regenerate.",
                )
            else:
                st.info("💡 GMS token will be auto-generated via kubectl")
        else:
            # For other modes, allow manual entry
            config.gms_url = st.text_input(
                "GMS URL",
                value=config.gms_url,
                placeholder="http://localhost:8080",
            )
            config.gms_token = st.text_input(
                "GMS Token",
                value=config.gms_token or "",
                type="password",
                placeholder="Enter your DataHub token",
            )

            # For embedded mode, add token generation workflow (same as REMOTE mode)
            if selected_mode == ConnectionMode.EMBEDDED:
                kubectl: Optional[KubectlManager] = st.session_state.kubectl_manager

                if kubectl:
                    st.caption("💡 Or auto-configure GMS credentials")
                    with st.expander("🔑 Auto-Configure GMS Credentials"):
                        st.markdown("**Auto-discover GMS and generate token**")
                        st.caption("Uses kubectl to discover GMS URL and generate token via AWS Parameter Store")

                        # Context selector (same as REMOTE mode)
                        try:
                            contexts = kubectl.get_contexts()
                            current_context = kubectl.get_current_context()

                            st.info(f"📍 Current context: **{current_context}**")

                            if len(contexts) > 1:
                                # Find current selection index
                                current_idx = (
                                    contexts.index(current_context)
                                    if current_context in contexts
                                    else 0
                                )

                                selected_context = st.selectbox(
                                    "Kubernetes Context",
                                    contexts,
                                    index=current_idx,
                                    help="Select the k8s context to use",
                                    key="embedded_context_selector",
                                )

                                if selected_context != current_context:
                                    if st.button(
                                        "🔄 Switch to this context", key="embedded_switch_context_btn"
                                    ):
                                        with st.spinner(
                                            f"Switching to context: {selected_context}..."
                                        ):
                                            result = subprocess.run(
                                                [
                                                    "kubectl",
                                                    "config",
                                                    "use-context",
                                                    selected_context,
                                                ],
                                                capture_output=True,
                                                text=True,
                                            )
                                            if result.returncode == 0:
                                                st.success(
                                                    f"✓ Switched to context: {selected_context}"
                                                )
                                                config.kube_context = selected_context
                                                st.rerun()
                                            else:
                                                st.error(
                                                    f"Failed to switch context: {result.stderr}"
                                                )
                                else:
                                    config.kube_context = selected_context

                            # Namespace selector (same as REMOTE mode)
                            try:
                                namespaces = kubectl.get_namespaces(config.kube_context)

                                namespace = st.selectbox(
                                    "Namespace (from cluster)",
                                    namespaces,
                                    index=namespaces.index(config.kube_namespace)
                                    if config.kube_namespace in namespaces
                                    else 0,
                                    key="embedded_namespace_selector",
                                )

                                # Add manual entry option (starts empty, overrides dropdown if filled)
                                namespace_input = st.text_input(
                                    "Or type namespace manually (overrides dropdown)",
                                    value="",
                                    placeholder="my-custom-namespace",
                                    help="Only fill this if your namespace isn't in the dropdown above",
                                    key="embedded_namespace_manual",
                                )

                                # Use manual input if provided, otherwise use dropdown
                                selected_namespace = (
                                    namespace_input if namespace_input.strip() else namespace
                                )
                            except RuntimeError as e:
                                error_msg = str(e)
                                if "VPN" in error_msg:
                                    st.error(f"🔌 {error_msg}", icon="🔌")
                                    st.info(
                                        "💡 Connect to VPN, then refresh this page or change the context dropdown to retry"
                                    )
                                else:
                                    st.error(f"❌ Failed to get namespaces: {error_msg}")

                                # Still allow manual namespace entry even if kubectl fails
                                selected_namespace = st.text_input(
                                    "Enter namespace manually",
                                    value=config.kube_namespace or "",
                                    placeholder="my-namespace",
                                    help="Type the namespace name since auto-discovery failed",
                                    key="embedded_namespace_manual_fallback",
                                )

                            st.divider()

                            # Quick Auto-Configure button (same as Quick Connect in REMOTE mode)
                            st.markdown("### 🚀 Auto-Configure")
                            st.caption(
                                "Auto-discover GMS URL and generate token for embedded agent"
                            )

                            # Handle SSO login button OUTSIDE the auto-configure button to avoid nested button issues
                            if "embedded_sso_error" in st.session_state and st.session_state.embedded_sso_error:
                                st.error("🔐 AWS SSO token has expired!")

                                profile_name = config.aws_profile or os.environ.get("AWS_PROFILE", "default")

                                st.code(
                                    f"aws sso login --profile {profile_name}",
                                    language="bash",
                                )

                                col1, col2 = st.columns([1, 1])
                                with col1:
                                    if st.button(
                                        "🌐 Open AWS SSO Login", key="embedded_sso_login"
                                    ):
                                        logger.info(f"SSO login button clicked for profile: {profile_name}")
                                        try:
                                            # Don't capture stdout/stderr - let aws sso login interact with browser
                                            proc = subprocess.Popen(
                                                [
                                                    "aws",
                                                    "sso",
                                                    "login",
                                                    "--profile",
                                                    profile_name,
                                                ],
                                                start_new_session=True,
                                            )
                                            logger.info(f"Started aws sso login process with PID: {proc.pid}")
                                            st.success(
                                                f"✅ Opening browser for AWS SSO (PID: {proc.pid})..."
                                            )
                                            st.info("Browser should open automatically. After logging in, click 'Retry After Login'.")
                                        except Exception as cmd_error:
                                            logger.error(f"Failed to start aws sso login: {cmd_error}")
                                            st.error(f"❌ Failed: {cmd_error}")
                                with col2:
                                    if st.button(
                                        "🔄 Retry After Login", key="embedded_retry_after_sso"
                                    ):
                                        # Clear error and retry
                                        st.session_state.embedded_sso_error = False
                                        st.rerun()

                            if st.button(
                                "⚡ Auto-Configure GMS", use_container_width=True, type="primary", key="embedded_auto_configure"
                            ):
                                # Clear any previous SSO error
                                st.session_state.embedded_sso_error = False

                                success_count = 0
                                with st.spinner(
                                    f"🔍 Auto-discovering services in {selected_namespace}..."
                                ):
                                    # 1. Auto-discover GMS URL
                                    gms_url = kubectl.get_gms_url_from_namespace(
                                        selected_namespace, config.kube_context
                                    )
                                    if gms_url:
                                        config.gms_url = gms_url
                                        st.success(f"✓ Discovered GMS: {gms_url}")
                                        success_count += 1
                                    else:
                                        st.info(
                                            "💡 Tip: GMS URL usually follows pattern: `https://<instance>.acryl.io/api/gms`"
                                        )

                                    # 2. Get DataHub token (needs GMS URL first)
                                    if gms_url:
                                        # Convert GMS URL to frontend URL (remove /api/gms suffix)
                                        frontend_url = gms_url.replace("/api/gms", "")

                                        try:
                                            token = kubectl.get_datahub_token(
                                                selected_namespace,
                                                frontend_url,
                                                config.kube_context,
                                                config.aws_profile,
                                            )
                                            if token:
                                                config.gms_token = token
                                                config.kube_namespace = selected_namespace
                                                st.success("✓ Got DataHub token")
                                                success_count += 1
                                            else:
                                                st.info(
                                                    "💡 Tip: Token generation requires AWS credentials configured"
                                                )
                                        except Exception as token_error:
                                            error_msg = str(token_error)

                                            # Check if it's SSO token expiry
                                            if (
                                                "Token has expired" in error_msg
                                                or "sso" in error_msg.lower()
                                                and "refresh failed" in error_msg.lower()
                                            ):
                                                # Set session state flag to show SSO error UI outside this button
                                                st.session_state.embedded_sso_error = True
                                                st.rerun()
                                            else:
                                                st.error(f"❌ Token generation failed: {error_msg}")
                                                st.info(
                                                    "💡 Tip: Check AWS credentials and kubectl access"
                                                )
                                    else:
                                        st.info("💡 Tip: Need GMS URL first to generate token")

                                    if success_count == 2:
                                        st.success("✅ GMS credentials configured!")
                                        st.balloons()
                                        st.rerun()

                        except Exception as e:
                            st.error(f"❌ Error: {e}")
                else:
                    st.caption("💡 Install kubectl for automated GMS configuration")

        # Check for unsaved changes
        manager: ConnectionManager = st.session_state.connection_manager
        active_profile_name = manager.get_active_profile()

        if active_profile_name:
            active_profile = manager.get_profile(active_profile_name)
            if active_profile:
                # Compare current config with saved profile (ignore description/name metadata)
                current_dict = config.to_dict()
                saved_dict = active_profile.config.to_dict()

                # Remove metadata fields for comparison
                for key in ["name", "description"]:
                    current_dict.pop(key, None)
                    saved_dict.pop(key, None)

                has_unsaved_changes = current_dict != saved_dict

                if has_unsaved_changes:
                    st.warning("⚠️ You have unsaved changes", icon="⚠️")
                    col1, col2 = st.columns([3, 1])
                    with col1:
                        st.caption(
                            "Your current settings differ from the saved profile. Save to persist across refreshes."
                        )
                    with col2:
                        if st.button(
                            "💾 Save", use_container_width=True, type="primary"
                        ):
                            save_connection_as_active()
                            st.success("✓ Saved!")
                            st.rerun()

        # Profile Management
        st.subheader("💾 Profiles")
        profiles = manager.list_profiles()

        if profiles:
            profile_name = st.selectbox("Load Profile", [""] + profiles)
            if profile_name:
                if st.button(f"📂 Load '{profile_name}'", use_container_width=True):
                    profile = manager.get_profile(profile_name)
                    if profile:
                        st.session_state.connection_config = profile.config
                        manager.set_active_profile(profile_name)
                        st.success(f"Loaded profile: {profile_name}")
                        st.rerun()

        # Save current config as profile
        new_profile_name = st.text_input("Save As", placeholder="my-profile")
        if st.button("💾 Save Profile", use_container_width=True):
            if new_profile_name:
                profile = ConnectionProfile(name=new_profile_name, config=config)
                if manager.save_profile(profile):
                    st.success(f"Saved profile: {new_profile_name}")
                else:
                    st.error("Failed to save profile")
            else:
                st.warning("Please enter a profile name")

        # Update config mode
        config.mode = selected_mode
        st.session_state.connection_config = config


def main():
    """Main Streamlit application."""
    st.title("Ask DataHub - The Admin Version")

    initialize_session_state()

    # Determine connection status for tab indicator
    config = st.session_state.connection_config
    has_token = bool(config.gms_token)

    # Connection is "good" only if we have an active token
    # This is the most reliable indicator that the connection is configured
    connection_ok = has_token
    settings_tab_label = "⚙️ Settings ✅" if connection_ok else "⚙️ Settings ⚠️"

    # Main tab navigation
    tab_chat, tab_stats, tab_settings = st.tabs(
        ["💬 Chat", "📊 Statistics", settings_tab_label]
    )

    # Sidebar
    with st.sidebar:
        st.header("⚙️ Controls")

        # Chat display settings
        st.subheader("💬 Display Settings")
        show_thinking = st.toggle(
            "Show internal thinking",
            value=st.session_state.get("show_thinking", False),
            help="Display reasoning, tool calls, and tool results in chat history",
        )
        st.session_state.show_thinking = show_thinking

        # Chat controls
        viewing_conv = get_viewing_conversation()
        has_history = "history" in viewing_conv and viewing_conv["history"].messages

        col1, col2 = st.columns(2)
        with col1:
            if st.button(
                "🗑️ Clear Chat", disabled=not has_history, use_container_width=True
            ):
                # Clear the viewing conversation
                viewing_conv["messages"] = []
                if "history" in viewing_conv:
                    from datahub_integrations.chat.chat_history import ChatHistory

                    viewing_conv["history"] = ChatHistory()
                if "agent" in viewing_conv:
                    # Recreate agent for embedded mode
                    viewing_conv["agent"] = create_embedded_agent()
                st.rerun()

        with col2:
            if has_history:
                st.download_button(
                    "💾 Save",
                    data=viewing_conv["history"].json(),
                    file_name=f"chat_{viewing_conv['id']}.json",
                    mime="application/json",
                    use_container_width=True,
                )
            else:
                st.button("💾 Save", disabled=True, use_container_width=True)

        st.divider()

        # Sketch refresh control
        st.subheader("📊 DataHub Catalog Sketch")

        sketch_status = "✓ Loaded" if st.session_state.sketch is not None else "Not loaded"
        st.caption(f"Status: {sketch_status}")

        col1, col2 = st.columns(2)
        with col1:
            if st.button("🔄 Refresh Sketch", use_container_width=True, help="Re-fetch metadata from DataHub to update catalog sketch"):
                clear_sketch()

                # Recreate sketch immediately
                config: ConnectionConfig = st.session_state.connection_config
                try:
                    with st.spinner("Refreshing DataHub catalog sketch..."):
                        from chat_simulator import DataHubSketcher
                        sketcher = DataHubSketcher(config.gms_url, config.gms_token or "")
                        st.session_state.sketch = sketcher.create_sketch()
                        logger.info("✓ DataHub sketch refreshed")
                        st.success("✓ Sketch refreshed successfully!")
                except Exception as e:
                    st.error(f"Failed to refresh sketch: {e}")
                    logger.error(f"Sketch refresh failed: {e}", exc_info=True)

                st.rerun()
        with col2:
            if st.button("📖 View Sketch", use_container_width=True, disabled=st.session_state.sketch is None, help="View the current catalog sketch"):
                st.session_state.show_sketch_detail = not st.session_state.get("show_sketch_detail", False)
                st.rerun()

        # Show sketch details if toggled
        if st.session_state.get("show_sketch_detail", False) and st.session_state.sketch is not None:
            with st.expander("📊 Sketch Details", expanded=True):
                sketch = st.session_state.sketch

                # Summary stats
                st.subheader("Summary")
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Platforms", len(sketch.platforms))
                with col2:
                    total_entities = sum(sketch.entity_counts.values())
                    st.metric("Total Entities", total_entities)
                with col3:
                    st.metric("Entity Types", len(sketch.entity_counts))

                # Entity type breakdown
                st.subheader("Entity Types")
                for entity_type, count in sorted(sketch.entity_counts.items(), key=lambda x: x[1], reverse=True):
                    st.text(f"  • {entity_type}: {count:,}")

                # Platforms
                st.subheader("Platforms")
                if sketch.platforms:
                    for platform in sorted(sketch.platforms):
                        st.text(f"  • {platform}")
                else:
                    st.caption("No platforms found")

                # Top datasets
                st.subheader("Top Datasets")
                if sketch.top_datasets:
                    for dataset in sketch.top_datasets[:10]:
                        if isinstance(dataset, dict):
                            name = dataset.get('name', dataset.get('urn', 'Unknown'))
                            st.text(f"  • {name}")
                        else:
                            st.text(f"  • {str(dataset)[:100]}")
                else:
                    st.caption("No datasets found")

                # Top dashboards
                st.subheader("Top Dashboards")
                if sketch.top_dashboards:
                    for dashboard in sketch.top_dashboards[:10]:
                        if isinstance(dashboard, dict):
                            name = dashboard.get('name', dashboard.get('urn', 'Unknown'))
                            st.text(f"  • {name}")
                        else:
                            st.text(f"  • {str(dashboard)[:100]}")
                else:
                    st.caption("No dashboards found")

                # Sample tags
                st.subheader("Sample Tags")
                if sketch.sample_tags:
                    st.text(", ".join(sketch.sample_tags[:20]))
                else:
                    st.caption("No tags found")

                # Sample glossary terms
                st.subheader("Sample Glossary Terms")
                if sketch.sample_glossary_terms:
                    st.text(", ".join(sketch.sample_glossary_terms[:20]))
                else:
                    st.caption("No glossary terms found")

                # Sample domains
                if sketch.sample_domains:
                    st.subheader("Sample Domains")
                    st.text(", ".join(sketch.sample_domains[:20]))

                # Download button
                st.divider()
                import json
                sketch_json = json.dumps({
                    "platforms": list(sketch.platforms),
                    "entity_counts": sketch.entity_counts,
                    "total_entities": sum(sketch.entity_counts.values()),
                    "top_datasets": sketch.top_datasets[:10],
                    "top_dashboards": sketch.top_dashboards[:10],
                    "sample_tags": sketch.sample_tags,
                    "sample_glossary_terms": sketch.sample_glossary_terms,
                    "sample_domains": sketch.sample_domains,
                }, indent=2)

                st.download_button(
                    "💾 Download Sketch Summary (JSON)",
                    data=sketch_json,
                    file_name="datahub_sketch_summary.json",
                    mime="application/json",
                )

        st.divider()

        # Auto-chat controls (moved to top, closer to conversations)
        st.subheader("🤖 Auto-Chat Mode")

        auto_chat_enabled = st.toggle(
            "Enable Auto-Chat",
            value=st.session_state.auto_chat_enabled,
            help="Automatically generate and send realistic questions",
        )

        if auto_chat_enabled != st.session_state.auto_chat_enabled:
            st.session_state.auto_chat_enabled = auto_chat_enabled
            if auto_chat_enabled:
                # Initialize Bedrock when enabling auto-chat
                if not initialize_bedrock():
                    st.session_state.auto_chat_enabled = False
                    st.rerun()
                else:
                    # Unpause when enabling auto-chat
                    st.session_state.auto_chat_paused = False
                    # Track current processing conversation as part of auto-chat session
                    processing_conv = get_processing_conversation()
                    st.session_state.auto_chat_conversation_ids = [processing_conv["id"]]

        if st.session_state.auto_chat_enabled:
            # Configuration
            col1, col2 = st.columns(2)
            with col1:
                st.session_state.auto_chat_max_messages_per_conv = st.number_input(
                    "Messages/conv",
                    min_value=1,
                    max_value=20,
                    value=st.session_state.auto_chat_max_messages_per_conv,
                    key="auto_chat_msgs",
                )
            with col2:
                st.session_state.auto_chat_max_conversations = st.number_input(
                    "Max convs",
                    min_value=1,
                    max_value=10,
                    value=st.session_state.auto_chat_max_conversations,
                    key="auto_chat_convs",
                )

            # Get processing conversation and count messages
            processing_conv = get_processing_conversation()
            # Count only user messages (questions), not tool calls/results/reasoning
            # This is simple: just count what's in the array
            current_conv_msg_count = len(
                [m for m in processing_conv["messages"] if m.get("role") == "user"]
            )
            total_auto_convs = len(st.session_state.auto_chat_conversation_ids)

            # Status indicator
            if st.session_state.auto_chat_paused:
                st.warning(
                    f"⏸️ Paused: Conv {total_auto_convs}/{st.session_state.auto_chat_max_conversations}, "
                    f"Msg {current_conv_msg_count}/{st.session_state.auto_chat_max_messages_per_conv}"
                )
            else:
                st.info(
                    f"▶️ Running: Conv {total_auto_convs}/{st.session_state.auto_chat_max_conversations}, "
                    f"Msg {current_conv_msg_count}/{st.session_state.auto_chat_max_messages_per_conv}"
                )

            # Pause/Resume button
            col1, col2 = st.columns(2)
            with col1:
                if st.session_state.auto_chat_paused:
                    if st.button("▶️ Resume", use_container_width=True):
                        st.session_state.auto_chat_paused = False
                        st.rerun()
                else:
                    if st.button("⏸️ Pause", use_container_width=True):
                        st.session_state.auto_chat_paused = True
                        st.rerun()

            with col2:
                if st.button("⏭️ Skip", use_container_width=True):
                    st.rerun()

        st.divider()

        # Conversation list (NEW - placed after auto-chat controls)
        render_conversation_list()

        st.divider()

        # Quick connection status in sidebar
        config = st.session_state.connection_config
        st.caption("🔗 Connection")
        st.caption(f"Mode: {config.mode.value}")
        if config.gms_token:
            st.caption("✅ Token: Active")
        else:
            st.caption("⚠️ Token: Not set")

    # TAB 1: Chat
    with tab_chat:
        # NEW: Get viewing conversation (not processing)
        viewing_conv = get_viewing_conversation()
        processing_conv = get_processing_conversation()
        is_viewing_processing = is_viewing_processing_conversation()

        # Get processing state for viewing conversation (for display)
        viewing_proc_state = get_processing_state(viewing_conv["id"])
        # Also get processing state for actual processing conversation (for pending messages)
        processing_proc_state = get_processing_state(processing_conv["id"])

        auto_chat_enabled = st.session_state.get("auto_chat_enabled", False)

        # NEW: Show notification if viewing different conversation than processing
        if auto_chat_enabled and not is_viewing_processing:
            # Get processing conversation title
            proc_title = "Unknown"
            if processing_conv["messages"]:
                first_msg = next(
                    (m for m in processing_conv["messages"] if m["role"] == "user"),
                    None,
                )
                if first_msg:
                    proc_title = first_msg["content"][:40] + (
                        "..." if len(first_msg["content"]) > 40 else ""
                    )

            st.info(
                f"ℹ️ **Viewing read-only conversation** | Auto-chat is processing: \"{proc_title}\""
            )

            # Button to switch to processing conversation
            col1, col2, col3 = st.columns([1, 2, 1])
            with col2:
                if st.button(
                    "🔄 Switch to Processing Conversation",
                    use_container_width=True,
                    key="switch_to_proc",
                ):
                    set_viewing_conversation(processing_conv["id"])
                    st.rerun()

        # NEW: Show read-only mode indicator if viewing but not processing
        if not is_viewing_processing and auto_chat_enabled:
            st.caption("🔒 Read-only mode - viewing past conversation")

        # Use rich rendering if ChatHistory is available
        if "history" in viewing_conv and viewing_conv["history"].messages:
            try:
                # Import from package instead of local copy (has better import safety)
                from datahub_integrations.chat.chat_history import ChatHistory
                from datahub_integrations.experimentation.chatbot.st_chat_history import (
                    st_chat_history,
                )

                # Add toggle for showing internal thinking
                show_thinking = st.session_state.get("show_thinking", False)

                # Check if we should split rendering to insert progress display
                messages = viewing_conv["history"].messages

                # NEW: Get processing state for VIEWING conversation
                has_progress = (
                    viewing_proc_state.get("last_progress_display")
                    and not viewing_proc_state.get("is_processing", False)
                )
                is_proc = viewing_proc_state.get("is_processing", False)

                # Debug logging
                prog_display = st.session_state.get("last_progress_display", "")
                prog_len = len(prog_display) if prog_display else 0
                logger.debug(
                    f"Render check: has_progress={has_progress}, is_processing={is_proc}, progress_len={prog_len}"
                )

                if has_progress and not is_proc:
                    # Find last respond_to_user (final assistant response)
                    from datahub_integrations.chat.chat_history import (
                        AssistantMessage,
                        ToolResult,
                    )

                    last_response_idx = None

                    # Debug: log all message types
                    logger.debug(
                        f"Message types in history: {[type(m).__name__ for m in messages[-5:]]}"
                    )

                    for i in range(len(messages) - 1, -1, -1):
                        msg = messages[i]
                        # Look for either ToolResult(respond_to_user) OR final AssistantMessage
                        if (
                            isinstance(msg, ToolResult)
                            and msg.tool_request.tool_name == "respond_to_user"
                        ):
                            last_response_idx = i
                            break
                        elif isinstance(msg, AssistantMessage):
                            # Found final assistant message
                            last_response_idx = i
                            break

                    if last_response_idx is not None:
                        logger.debug(
                            f"Splitting messages at idx {last_response_idx} of {len(messages)} total messages"
                        )

                        # Render messages up to (but not including) final response
                        messages_before = ChatHistory()
                        messages_before.messages = messages[:last_response_idx]
                        st_chat_history(
                            messages_before,
                            show_thinking=show_thinking,
                            conversation_id=viewing_conv["id"],
                        )

                        # Show progress display here (between thinking and response)
                        progress_text = viewing_proc_state.get("last_progress_display", "")
                        logger.debug(f"Showing progress display: {progress_text[:100]}")
                        st.markdown("✅ " + progress_text.replace("\n", "  \n"))

                        # Render final response
                        logger.debug(
                            f"Rendering final {len(messages) - last_response_idx} messages"
                        )
                        final_message = ChatHistory()
                        final_message.messages = messages[last_response_idx:]
                        st_chat_history(
                            final_message,
                            show_thinking=show_thinking,
                            conversation_id=viewing_conv["id"] + "_final",
                        )
                    else:
                        # No final response yet, render normally
                        logger.debug(
                            "No respond_to_user found, rendering all messages normally"
                        )
                        st_chat_history(
                            viewing_conv["history"],
                            show_thinking=show_thinking,
                            conversation_id=viewing_conv["id"],
                        )
                else:
                    # No progress to insert, render normally
                    st_chat_history(
                        viewing_conv["history"],
                        show_thinking=show_thinking,
                        conversation_id=viewing_conv["id"],
                    )

            except (ImportError, ValueError, Exception) as e:
                # Fallback to simple rendering if rich renderer can't load
                st.warning(
                    f"⚠️ Rich rendering unavailable (using fallback): {type(e).__name__}: {e}"
                )
                logger.error(f"Rich rendering failed: {e}", exc_info=True)
                # Show messages in simple format
                for msg in viewing_conv["history"].messages:
                    role = (
                        "user"
                        if hasattr(msg, "type") and msg.type == "human"
                        else "assistant"
                    )
                    with st.chat_message(role):
                        st.text(msg.text if hasattr(msg, "text") else str(msg))

        # Progress tracker display (only during active processing)
        # Completed progress is shown inline with messages above
        progress_display = st.empty()

        # Only show live progress if viewing the processing conversation
        if is_viewing_processing and viewing_proc_state.get("is_processing"):
            # Currently processing - show live progress or initial placeholder
            if viewing_proc_state.get("current_progress_display"):
                # Show spinner with live progress
                progress_display.markdown(
                    render_spinner_with_progress(viewing_proc_state["current_progress_display"]),
                    unsafe_allow_html=True,
                )
            else:
                # Initial placeholder before server responds
                progress_display.markdown(
                    render_spinner_placeholder(),
                    unsafe_allow_html=True,
                )

        # Check for pending message to process in PROCESSING conversation (not viewing)
        if processing_proc_state.get("pending_message"):
            pending_msg = processing_proc_state["pending_message"]
            processing_proc_state["pending_message"] = None  # Clear flag
            # is_processing should already be True from when message was submitted

            send_message(
                pending_msg["text"],
                is_auto=pending_msg.get("is_auto", False),
                progress_display=progress_display,
            )

            processing_proc_state["is_processing"] = False  # Clear processing flag

            # Add delay for auto-chat messages before rerunning to avoid rapid-fire questions
            if pending_msg.get("is_auto", False):
                time.sleep(1)

            st.rerun()

        # Input area (still in chat tab)
        if st.session_state.auto_chat_enabled:
            # Auto-chat mode with conversation rotation
            active_conv = get_processing_conversation()
            # Count only user messages (questions), not tool calls/results/reasoning
            # Simple: just count what's in the array (we add messages before rerunning now)
            current_conv_msg_count = len(
                [m for m in active_conv["messages"] if m.get("role") == "user"]
            )
            total_auto_convs = len(st.session_state.auto_chat_conversation_ids)

            # Check if we need to rotate to a new conversation
            should_rotate = (
                current_conv_msg_count
                >= st.session_state.auto_chat_max_messages_per_conv
            )
            can_create_more = (
                total_auto_convs < st.session_state.auto_chat_max_conversations
            )

            # Debug logging
            print(f"[AUTO-CHAT DEBUG] Conv: {active_conv['id'][:8]}, UserMsgCount: {current_conv_msg_count}, TotalConvs: {total_auto_convs}, ShouldRotate: {should_rotate}, CanCreateMore: {can_create_more}")

            if should_rotate and can_create_more:
                # Create new conversation and switch to it
                new_conv = create_new_conversation()
                set_processing_conversation(new_conv["id"])
                st.session_state.auto_chat_conversation_ids.append(new_conv["id"])

                # Optionally follow to new conversation (default: yes)
                auto_follow = st.session_state.get("auto_follow_processing", True)
                if auto_follow:
                    set_viewing_conversation(new_conv["id"])

                st.rerun()
            elif should_rotate and not can_create_more:
                # Reached max conversations
                st.info(
                    f"✅ Auto-chat completed! Generated {total_auto_convs} conversations with "
                    f"{st.session_state.auto_chat_max_messages_per_conv} messages each."
                )
            else:
                # Continue in current conversation
                if not st.session_state.auto_chat_paused:
                    # Only generate and send if not paused
                    try:
                        with st.spinner("Generating question..."):
                            if len(active_conv["messages"]) == 0:
                                question = st.session_state.question_generator.generate_initial_question()
                            else:
                                question = st.session_state.question_generator.generate_followup_question(
                                    active_conv["messages"]
                                )
                    except Exception as e:
                        error_msg = str(e)
                        # Check if it's SSO token expiry
                        if (
                            "SSO token has expired" in error_msg
                            or "aws sso login" in error_msg
                        ):
                            st.error("🔐 AWS SSO token has expired!")
                            config: ConnectionConfig = (
                                st.session_state.connection_config
                            )

                            # Determine which profile to use
                            profile_name = config.aws_profile
                            if not profile_name:
                                profile_name = os.environ.get("AWS_PROFILE")
                                if not profile_name:
                                    available = get_available_aws_profiles()
                                    if available:
                                        profile_name = available[0]
                                        st.warning(
                                            f"⚠️ No AWS profile configured - using: {profile_name}"
                                        )
                                    else:
                                        profile_name = "default"
                                        st.warning(
                                            "⚠️ No AWS profiles found - trying 'default'"
                                        )

                            st.code(
                                f"aws sso login --profile {profile_name}",
                                language="bash",
                            )

                            col1, col2 = st.columns([1, 1])
                            with col1:
                                if st.button(
                                    "🌐 Open AWS SSO Login", key="sso_auto_chat"
                                ):
                                    import subprocess

                                    try:
                                        # Don't capture stdout/stderr - let aws sso login interact with browser
                                        subprocess.Popen(
                                            [
                                                "aws",
                                                "sso",
                                                "login",
                                                "--profile",
                                                profile_name,
                                            ],
                                            start_new_session=True,
                                        )
                                        st.success("✅ Opening browser for AWS SSO...")
                                    except Exception as cmd_error:
                                        st.error(f"❌ Failed: {cmd_error}")
                            with col2:
                                if st.button(
                                    "🔄 Refresh Page", key="refresh_after_sso"
                                ):
                                    st.rerun()
                        else:
                            st.error(f"Failed to generate question: {e}")

                        # Disable auto-chat on errors
                        st.session_state.auto_chat_enabled = False
                        logger.warning("Auto-chat disabled due to error during question generation")
                        return  # Stop auto-chat on error

                    # Add user message to messages array IMMEDIATELY (before rerun)
                    # This ensures the count updates correctly on the next render
                    user_msg = {
                        "role": "user",
                        "content": question,
                        "timestamp": datetime.now(),
                        "is_auto": True,
                    }
                    active_conv["messages"].append(user_msg)
                    st.session_state.messages.append(user_msg)

                    # Add to chat history too
                    if "history" not in active_conv:
                        from datahub_integrations.chat.chat_history import ChatHistory

                        active_conv["history"] = ChatHistory()

                    from datahub_integrations.chat.chat_history import HumanMessage

                    active_conv["history"].add_message(HumanMessage(text=question))

                    # Set flags for processing (same as manual mode)
                    set_pending_message(active_conv["id"], question, is_auto=True)
                    st.rerun()
                else:
                    # Paused - show message and don't auto-rerun
                    st.info("⏸️ Auto-chat is paused. Click Resume to continue.")
        else:
            # Manual mode - check if input should be enabled
            show_input = True
            input_disabled_msg = None

            # Get current viewing/processing state
            current_viewing_conv = get_viewing_conversation()
            current_is_viewing_processing = is_viewing_processing_conversation()

            # Hide input when viewing non-processing conversation (would be confusing)
            if not current_is_viewing_processing:
                show_input = False
                processing_conv = get_processing_conversation()
                proc_title = "Unknown"
                if processing_conv["messages"]:
                    first_msg = next(
                        (m for m in processing_conv["messages"] if m["role"] == "user"),
                        None,
                    )
                    if first_msg:
                        proc_title = first_msg["content"][:40] + (
                            "..." if len(first_msg["content"]) > 40 else ""
                        )
                input_disabled_msg = (
                    f"💬 Chat input is disabled while viewing past conversations. "
                    f"Switch to the processing conversation to send messages."
                )

            if not show_input and input_disabled_msg:
                st.caption(input_disabled_msg)

            if show_input:
                if prompt := st.chat_input("Ask about your data..."):
                    # If viewing different conversation, switch processing to it
                    if not current_is_viewing_processing:
                        set_processing_conversation(current_viewing_conv["id"])

                    active_conv = get_viewing_conversation()

                    # Add user message to messages array IMMEDIATELY (before rerun)
                    user_msg = {
                        "role": "user",
                        "content": prompt,
                        "timestamp": datetime.now(),
                        "is_auto": False,
                    }
                    active_conv["messages"].append(user_msg)
                    st.session_state.messages.append(user_msg)

                    # Add to chat history too
                    if "history" not in active_conv:
                        from datahub_integrations.chat.chat_history import ChatHistory

                        active_conv["history"] = ChatHistory()

                    from datahub_integrations.chat.chat_history import HumanMessage

                    active_conv["history"].add_message(HumanMessage(text=prompt))

                    # Set flags for processing
                    set_pending_message(active_conv["id"], prompt, is_auto=False)
                    st.rerun()

    # TAB 2: Statistics
    with tab_stats:
        st.header("📊 Chat Statistics")

        stats = st.session_state.stats

        # Overview metrics
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Messages", stats["total_messages"])
        with col2:
            st.metric("Success", stats["success_count"], delta_color="normal")
        with col3:
            st.metric("Errors", stats["error_count"], delta_color="inverse")
        with col4:
            if stats["total_messages"] > 0:
                success_rate = (stats["success_count"] / stats["total_messages"]) * 100
                st.metric("Success Rate", f"{success_rate:.1f}%")
            else:
                st.metric("Success Rate", "0%")

        st.divider()

        # Performance metrics
        if stats["total_messages"] > 0:
            st.subheader("⚡ Performance")
            col1, col2 = st.columns(2)
            with col1:
                avg_duration = stats["total_duration"] / stats["total_messages"]
                st.metric("Avg Duration", f"{avg_duration:.2f}s")
            with col2:
                st.metric("Total Duration", f"{stats['total_duration']:.1f}s")

        st.divider()

        # Conversation stats
        st.subheader("💬 Conversations")
        total_convs = len(st.session_state.conversations)
        active_conv_id = st.session_state.active_conversation_id
        active_conv = next(
            c for c in st.session_state.conversations if c["id"] == active_conv_id
        )

        col1, col2 = st.columns(2)
        with col1:
            st.metric("Total Conversations", total_convs)
        with col2:
            st.metric("Messages in Current", len(active_conv["messages"]))

        # Auto-chat stats
        if st.session_state.auto_chat_enabled:
            st.divider()
            st.subheader("🤖 Auto-Chat Stats")
            st.metric("Auto Messages Sent", st.session_state.auto_chat_count)

    # TAB 3: Settings
    with tab_settings:
        st.header("⚙️ Connection Settings")

        # Service Status Check at the very top
        col_header1, col_header2 = st.columns([3, 1])
        with col_header1:
            st.subheader("🔌 Service Status")
        with col_header2:
            if st.button(
                "🔄 Refresh", key="refresh_service_status", use_container_width=True
            ):
                st.rerun()

        # Check integrations service port
        import socket
        import subprocess
        from urllib.parse import urlparse

        parsed_url = urlparse(config.integrations_url)
        expected_host = parsed_url.hostname or "localhost"
        expected_port = parsed_url.port or 9003

        def check_port_listening(host: str, port: int) -> bool:
            """Check if a port is listening."""
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                    sock.settimeout(1)
                    result = sock.connect_ex((host, port))
                    return result == 0
            except Exception:
                return False

        def get_process_on_port(port: int) -> str:
            """Get process info for a port."""
            try:
                result = subprocess.run(
                    ["lsof", "-i", f":{port}", "-t"],
                    capture_output=True,
                    text=True,
                    timeout=2,
                )
                if result.stdout.strip():
                    pid = result.stdout.strip().split()[0]
                    ps_result = subprocess.run(
                        ["ps", "-p", pid, "-o", "command="],
                        capture_output=True,
                        text=True,
                        timeout=2,
                    )
                    return ps_result.stdout.strip()
                return ""
            except Exception:
                return ""

        col1, col2 = st.columns([2, 3])

        with col1:
            st.write(f"**Expected:** `{config.integrations_url}`")
            st.write(f"**Port:** `{expected_port}`")
            st.caption(f"⏱️ Checked: {datetime.now().strftime('%H:%M:%S')}")

        with col2:
            is_listening = check_port_listening(expected_host, expected_port)
            if is_listening:
                process_info = get_process_on_port(expected_port)
                st.success(f"✅ Port {expected_port} is listening")
                if process_info:
                    if "uvicorn" in process_info:
                        st.caption("🟢 Local integrations service")
                    elif "kubectl" in process_info:
                        st.caption("🟢 kubectl port-forward")
                    else:
                        st.caption(f"🟢 {process_info[:50]}...")
            else:
                st.error(f"❌ Port {expected_port} not listening")
                st.caption("💡 Click 🔄 Refresh after starting service")

                # Check if local_service_port is different and is running
                if config.mode == ConnectionMode.REMOTE and config.use_local_service:
                    if (
                        config.local_service_port
                        and config.local_service_port != expected_port
                    ):
                        local_service_running = check_port_listening(
                            "localhost", config.local_service_port
                        )
                        if local_service_running:
                            st.warning(
                                f"⚠️ Local service is running on port {config.local_service_port}, but config expects {expected_port}"
                            )
                            if st.button(
                                "🔧 Fix: Update to port "
                                + str(config.local_service_port),
                                key="fix_port_mismatch",
                            ):
                                config.integrations_url = (
                                    f"http://localhost:{config.local_service_port}"
                                )
                                config.local_port = config.local_service_port
                                # Save the updated config
                                manager = ConnectionManager()
                                profile = ConnectionProfile(
                                    name=st.session_state.active_profile_name,
                                    config=config,
                                )
                                manager.save_profile(profile)
                                st.success(
                                    f"✅ Updated integrations URL to http://localhost:{config.local_service_port}"
                                )
                                st.rerun()

        st.divider()

        # Connection Health Check
        st.subheader("🏥 Connection Health")

        col1, col2 = st.columns(2)

        with col1:
            # DataHub Health
            st.write("**DataHub**")
            if config.gms_url and config.gms_token:
                if st.button(
                    "🧪 Test DataHub", use_container_width=True, key="test_datahub_tab"
                ):
                    with st.spinner("Testing..."):
                        try:
                            # Quick health check - try to search
                            import httpx

                            response = httpx.post(
                                f"{config.gms_url}/entities?action=search",
                                json={
                                    "input": "*",
                                    "entity": "dataset",
                                    "start": 0,
                                    "count": 1,
                                },
                                headers={"Authorization": f"Bearer {config.gms_token}"},
                                timeout=5.0,
                            )
                            if response.status_code == 200:
                                st.success("✅ Connected")
                            else:
                                st.error(f"❌ HTTP {response.status_code}")
                        except Exception as e:
                            st.error(f"❌ {str(e)[:100]}")
            else:
                st.info("⚠️ Not configured")

        with col2:
            # AWS Bedrock Health
            st.write("**AWS Bedrock**")
            # Check if AWS is configured (either explicit profile or env variable)
            aws_profile = config.aws_profile or os.environ.get("AWS_PROFILE")
            has_aws_config = (
                config.aws_profile
                or os.environ.get("AWS_PROFILE")
                or os.path.exists(os.path.expanduser("~/.aws/credentials"))
            )

            if has_aws_config:
                if st.button(
                    "🧪 Test Bedrock", use_container_width=True, key="test_bedrock_tab"
                ):
                    with st.spinner("Testing..."):
                        try:
                            import boto3

                            # Use explicit profile if set, otherwise use default chain
                            if config.aws_profile:
                                session = boto3.Session(
                                    profile_name=config.aws_profile,
                                    region_name=config.aws_region,
                                )
                            else:
                                session = boto3.Session(region_name=config.aws_region)

                            client = session.client("bedrock-runtime")
                            # Simple test - list models (lightweight operation)
                            client.list_foundation_models = lambda: {
                                "modelSummaries": []
                            }
                            st.success("✅ Connected")
                        except Exception as e:
                            error_msg = str(e)
                            if (
                                "Token has expired" in error_msg
                                or "TokenRetrievalError" in error_msg
                            ):
                                st.error("🔐 SSO Token Expired")

                                # Determine which profile to show
                                profile_for_sso = aws_profile
                                if not profile_for_sso:
                                    available = get_available_aws_profiles()
                                    if available:
                                        profile_for_sso = available[0]
                                    else:
                                        profile_for_sso = "default"

                                st.code(
                                    f"aws sso login --profile {profile_for_sso}",
                                    language="bash",
                                )
                                if st.button(
                                    "🌐 Login",
                                    key="health_sso_login_tab",
                                    use_container_width=True,
                                ):
                                    import subprocess

                                    # Don't capture stdout/stderr - let aws sso login interact with browser
                                    subprocess.Popen(
                                        [
                                            "aws",
                                            "sso",
                                            "login",
                                            "--profile",
                                            profile_for_sso,
                                        ],
                                        start_new_session=True,
                                    )
                                    st.success("✅ Opening browser...")
                            else:
                                st.error(f"❌ {str(e)[:100]}")
            else:
                st.info("⚠️ Not configured")

        st.divider()

        # Connection status banner
        config = st.session_state.connection_config
        if config.gms_token:
            st.success("✅ Connection configured - Token is active", icon="✅")
        else:
            st.warning(
                "⚠️ Connection not configured - Please set up connection below", icon="⚠️"
            )

        st.divider()

        render_connection_settings()


if __name__ == "__main__":
    main()
