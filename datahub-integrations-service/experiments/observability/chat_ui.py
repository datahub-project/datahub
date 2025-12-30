#!/usr/bin/env python3
"""
Interactive Chat UI with Auto-Chat Mode

This Streamlit app provides:
1. Manual chat interface for DataHub conversations
2. Auto-chat mode that generates realistic questions automatically
3. Real-time conversation display
4. Conversation history and statistics
"""

import random
import time
from datetime import datetime

import boto3
import streamlit as st

# Import shared components from chat simulator
# Import config constants from chat simulator
from chat_simulator import (
    AWS_PROFILE,
    AWS_REGION,
    GMS_TOKEN,
    GMS_URL,
    SERVICE_URL,
    ChatClient,
    ConversationGenerator,
    DataHubSketcher,
)
from loguru import logger

# Question generation modes
QUESTION_MODE_EMBEDDED = "embedded"  # Generate questions locally using Bedrock
QUESTION_MODE_ENDPOINT = "endpoint"  # Call integrations service endpoint

# Page config
st.set_page_config(
    page_title="DataHub Chat with Auto-Chat",
    page_icon="🤖",
    layout="wide",
)


# All shared classes (DataHubSketch, DataHubSketcher, ConversationGenerator, ChatClient)
# are now imported from chat_simulator above


def initialize_session_state():
    """Initialize Streamlit session state."""
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

    if "auto_chat_paused" not in st.session_state:
        st.session_state.auto_chat_paused = False

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


def initialize_bedrock():
    """Initialize AWS Bedrock client and DataHub sketch."""
    if st.session_state.bedrock_client is None:
        try:
            with st.spinner("Initializing AWS Bedrock..."):
                if AWS_PROFILE:
                    session = boto3.Session(
                        profile_name=AWS_PROFILE, region_name=AWS_REGION
                    )
                else:
                    session = boto3.Session(region_name=AWS_REGION)

                st.session_state.bedrock_client = session.client("bedrock-runtime")
                logger.info(f"✓ Connected to Bedrock in {AWS_REGION}")
        except Exception as e:
            st.error(f"Failed to initialize Bedrock: {e}")
            st.info("Make sure you have AWS credentials configured")
            return False

    if st.session_state.sketch is None:
        try:
            with st.spinner("Sketching DataHub backend..."):
                sketcher = DataHubSketcher(GMS_URL, GMS_TOKEN)
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


def send_message(message: str, is_auto: bool = False):
    """Send a message and update the conversation."""
    chat_client = ChatClient(SERVICE_URL, GMS_TOKEN)

    # Add user message to display
    st.session_state.messages.append(
        {
            "role": "user",
            "content": message,
            "timestamp": datetime.now(),
            "is_auto": is_auto,
        }
    )

    # Send message and get response
    result = chat_client.send_message(
        message, st.session_state.conversation_urn, st.session_state.user_urn
    )

    # Update stats
    st.session_state.stats["total_messages"] += 1
    st.session_state.stats["total_duration"] += result["duration"]

    if result["success"]:
        st.session_state.stats["success_count"] += 1
    else:
        st.session_state.stats["error_count"] += 1

    # Add assistant response
    st.session_state.messages.append(
        {
            "role": "assistant",
            "content": result["response"]
            if result["success"]
            else f"Error: {result['error']}",
            "timestamp": datetime.now(),
            "duration": result["duration"],
            "event_count": result["event_count"],
            "success": result["success"],
            "is_auto": is_auto,
        }
    )

    if is_auto:
        st.session_state.auto_chat_count += 1


def main():
    """Main Streamlit application."""
    st.title("🤖 DataHub Chat with Auto-Chat")

    initialize_session_state()

    # Sidebar
    with st.sidebar:
        st.header("⚙️ Controls")

        # New conversation button
        if st.button("🔄 New Conversation", use_container_width=True):
            st.session_state.messages = []
            st.session_state.conversation_urn = f"urn:li:conversation:ui-{int(time.time())}-{random.randint(1000, 9999)}"
            st.session_state.auto_chat_enabled = False
            st.session_state.auto_chat_count = 0
            st.session_state.auto_chat_paused = False
            st.rerun()

        st.divider()

        # Auto-chat controls
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

        if st.session_state.auto_chat_enabled:
            st.session_state.auto_chat_max = st.number_input(
                "Messages per conversation",
                min_value=1,
                max_value=20,
                value=st.session_state.auto_chat_max,
            )

            # Status indicator
            if st.session_state.auto_chat_paused:
                st.warning(
                    f"⏸️ Paused: {st.session_state.auto_chat_count}/{st.session_state.auto_chat_max} messages"
                )
            else:
                st.info(
                    f"▶️ Running: {st.session_state.auto_chat_count}/{st.session_state.auto_chat_max} messages"
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

        # Statistics
        st.subheader("📊 Statistics")
        stats = st.session_state.stats
        st.metric("Total Messages", stats["total_messages"])

        col1, col2 = st.columns(2)
        with col1:
            st.metric("Success", stats["success_count"])
        with col2:
            st.metric("Errors", stats["error_count"])

        if stats["total_messages"] > 0:
            success_rate = (stats["success_count"] / stats["total_messages"]) * 100
            st.metric("Success Rate", f"{success_rate:.1f}%")
            avg_duration = stats["total_duration"] / stats["total_messages"]
            st.metric("Avg Duration", f"{avg_duration:.1f}s")

        st.divider()

        # Connection info
        with st.expander("🔗 Connection Info"):
            st.text(f"Service: {SERVICE_URL}")
            st.text(f"GMS: {GMS_URL}")
            st.text(f"Conversation: {st.session_state.conversation_urn[-20:]}")

    # Main chat area
    chat_container = st.container()

    with chat_container:
        # Display messages
        for msg in st.session_state.messages:
            is_auto = msg.get("is_auto", False)
            prefix = "🤖 " if is_auto else ""

            if msg["role"] == "user":
                with st.chat_message("user"):
                    timestamp = msg["timestamp"].strftime("%H:%M:%S")
                    st.markdown(f"**{prefix}[{timestamp}]** {msg['content']}")
            else:
                with st.chat_message("assistant"):
                    timestamp = msg["timestamp"].strftime("%H:%M:%S")
                    duration = msg.get("duration", 0)
                    events = msg.get("event_count", 0)
                    success = msg.get("success", True)

                    status = "✓" if success else "✗"
                    st.markdown(
                        f"**{prefix}[{timestamp}] {status}** _{duration:.1f}s, {events} events_"
                    )
                    st.markdown(msg["content"])

    # Input area
    if st.session_state.auto_chat_enabled:
        # Auto-chat mode
        if st.session_state.auto_chat_count < st.session_state.auto_chat_max:
            if not st.session_state.auto_chat_paused:
                # Only generate and send if not paused
                with st.spinner("Generating question..."):
                    if st.session_state.auto_chat_count == 0:
                        question = st.session_state.question_generator.generate_initial_question()
                    else:
                        question = st.session_state.question_generator.generate_followup_question(
                            st.session_state.messages
                        )

                with st.spinner(f"Sending: {question}"):
                    send_message(question, is_auto=True)

                time.sleep(1)
                st.rerun()
            else:
                # Paused - show message and don't auto-rerun
                st.info("⏸️ Auto-chat is paused. Click Resume to continue.")
        else:
            st.info(
                "✅ Auto-chat conversation completed! Start a new conversation or adjust the message limit."
            )
    else:
        # Manual mode
        if prompt := st.chat_input("Type your message..."):
            send_message(prompt, is_auto=False)
            st.rerun()


if __name__ == "__main__":
    main()
