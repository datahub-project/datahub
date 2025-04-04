import re

import streamlit as st
from typing_extensions import assert_never

from datahub_integrations.chat.chat_history import (
    AssistantMessage,
    ChatHistory,
    HumanMessage,
    ReasoningMessage,
    ToolCallRequest,
    ToolResult,
    ToolResultError,
)
from datahub_integrations.chat.chat_session import (
    ChatSession,
    NextMessage,
)
from datahub_integrations.chat.linkify import linkify_slack
from datahub_integrations.chat.mcp_server import get_client, mcp


def _make_empty_chat_session() -> ChatSession:
    st.text("Resetting chat session")
    return ChatSession(tools=mcp.get_all_tools())


def _chat_session() -> ChatSession:
    # Initialize chat session if it doesn't exist
    if "chat_session" not in st.session_state:
        st.session_state.chat_session = _make_empty_chat_session()
    return st.session_state.chat_session


# Add a clear chat button
if st.button("Clear Chat"):
    st.session_state.chat_session = _make_empty_chat_session()
    st.rerun()

_chat_session()

show_thinking = st.toggle("Show internal thinking", value=True)

st.divider()


@st.cache_data
def frontend_url() -> str:
    return get_client()._graph.frontend_base_url


def _format_slack_mrkdwn(text: str) -> str:
    slack_text = linkify_slack(frontend_url(), text)

    # Convert Slack-style links to markdown links.
    # Both <url> and <url|text> are supported.
    markdown_text = slack_text
    markdown_text = re.sub(r"<(https://[^>]+)\|([^>]+)>", r"[\2](\1)", markdown_text)
    markdown_text = re.sub(r"<(https://[^>]+)>", r"[\1](\1)", markdown_text)
    return markdown_text


def render_history(history: ChatHistory) -> None:
    for message in history.messages:
        if isinstance(message, ReasoningMessage):
            if show_thinking:
                with st.chat_message("assistant", avatar="🧠"):
                    st.markdown(f"<pre>{message.text}</pre>", unsafe_allow_html=True)
        elif isinstance(message, HumanMessage):
            with st.chat_message("user"):
                st.markdown(_format_slack_mrkdwn(message.text))
        elif isinstance(message, AssistantMessage):
            with st.chat_message("assistant"):
                st.markdown(_format_slack_mrkdwn(message.text))
        elif _chat_session().is_respond_to_user(message):
            with st.chat_message("assistant"):
                assert isinstance(message.result, NextMessage)
                st.markdown(_format_slack_mrkdwn(message.result.text))

                st.markdown("**Next Message Suggestions**")
                suggestions_list = "\n".join(
                    [f"- {suggestion}" for suggestion in message.result.suggestions]
                )
                if suggestions_list:
                    st.markdown(suggestions_list)
        elif isinstance(message, ToolResult):
            if show_thinking:
                with st.chat_message("tool", avatar="🔧"):
                    st.markdown(f"Tool `{message.tool_request.tool_name}` returned:")
                    st.json(message.result, expanded=2)
        elif isinstance(message, ToolCallRequest):
            if show_thinking:
                with st.chat_message("tool", avatar="📞"):
                    st.markdown(f"Calling `{message.tool_name}` tool")
                    st.code(str(message.tool_input), language="json")
        elif isinstance(message, ToolResultError):
            if show_thinking:
                with st.chat_message("tool", avatar="❌"):
                    st.markdown(
                        f"Tool `{message.tool_request.tool_name}` returned an error:"
                    )
                    st.code(str(message.error))
        else:
            st.error(f"Unknown message type: {type(message)}")
            assert_never(message)


# Chat UI.
render_history(_chat_session().history)
if prompt := st.chat_input("Type your message here..."):
    # Add user message to chat session
    user_message = HumanMessage(text=prompt)
    _chat_session()._add_message(user_message)

    # Generate bot response
    with (
        st.status("Generating response...", expanded=True) as status,
        _chat_session().set_progress_callback(
            lambda message: status.update(label=message)
        ),
    ):
        try:
            response = _chat_session().generate_next_message()
            st.rerun()

        except Exception as e:
            st.error(f"Error generating response: {str(e)}")
