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
from datahub_integrations.chat.chat_session import ChatSession, NextMessage
from datahub_integrations.chat.linkify import linkify_slack


def _format_slack_mrkdwn(text: str, frontend_url: str) -> str:
    slack_text = linkify_slack(frontend_url, text)

    # Convert Slack-style links to markdown links.
    # Both <url> and <url|text> are supported.
    markdown_text = slack_text
    markdown_text = re.sub(r"<(https://[^>]+)\|([^>]+)>", r"[\2](\1)", markdown_text)
    markdown_text = re.sub(r"<(https://[^>]+)>", r"[\1](\1)", markdown_text)
    return markdown_text


def st_chat_history(
    history: ChatHistory,
    *,
    show_thinking: bool = True,
    frontend_url: str = "https://dummy.acryl.io",
) -> None:
    for message in history.messages:
        if isinstance(message, ReasoningMessage):
            if show_thinking:
                with st.chat_message("assistant", avatar="🧠"):
                    st.markdown(f"<pre>{message.text}</pre>", unsafe_allow_html=True)
        elif isinstance(message, HumanMessage):
            with st.chat_message("user"):
                st.markdown(_format_slack_mrkdwn(message.text, frontend_url))
        elif isinstance(message, AssistantMessage):
            with st.chat_message("assistant"):
                st.markdown(_format_slack_mrkdwn(message.text, frontend_url))
        elif ChatSession.is_respond_to_user(message):
            with st.chat_message("assistant"):
                next_message = message.result
                if not isinstance(next_message, NextMessage):
                    # When restoring history from JSON, the message result loses its type.
                    next_message = NextMessage.parse_obj(next_message)

                markdown_tab, raw_tab = st.tabs(["Markdown", "Raw"])
                with markdown_tab:
                    st.markdown(_format_slack_mrkdwn(next_message.text, frontend_url))
                with raw_tab:
                    st.code(next_message.text, language="json")

                suggestions_list = "\n".join(
                    [f"- {suggestion}" for suggestion in next_message.suggestions]
                )
                if suggestions_list:
                    st.markdown("**Next Message Suggestions**")
                    st.markdown(suggestions_list)
                else:
                    st.markdown("**No Next Message Suggestions**")
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
