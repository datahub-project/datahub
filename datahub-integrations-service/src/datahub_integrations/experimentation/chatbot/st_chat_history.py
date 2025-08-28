import json

import streamlit as st
import tiktoken
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
from datahub_integrations.slack.utils.numbers import abbreviate_number


def _token_count(text: str) -> str:
    """Count tokens in text and return abbreviated count with 'tokens' suffix."""
    # This is just an approximation since different models have different tokenizers.
    encoding = tiktoken.encoding_for_model("gpt-4o")
    token_count = len(encoding.encode(text))
    return f"{abbreviate_number(token_count)} tokens"


def st_chat_history(
    history: ChatHistory,
    *,
    show_thinking: bool = True,
) -> None:
    for message in history.messages:
        if isinstance(message, ReasoningMessage):
            if show_thinking:
                with st.chat_message("assistant", avatar="🧠"):
                    st.caption(f"Reasoning · {_token_count(message.text)}")
                    st.markdown(message.text)
        elif isinstance(message, HumanMessage):
            with st.chat_message("user"):
                st.caption(f"User · {_token_count(message.text)}")
                st.markdown(message.text)
        elif isinstance(message, AssistantMessage):
            with st.chat_message("assistant"):
                st.caption(f"Assistant · {_token_count(message.text)}")
                st.markdown(message.text)
        elif ChatSession.is_respond_to_user(message):
            with st.chat_message("assistant"):
                next_message = NextMessage.model_validate(message.result)

                st.caption(f"Response · {_token_count(next_message.text)}")

                markdown_tab, raw_tab = st.tabs(["Markdown", "Raw"])
                with markdown_tab:
                    st.markdown(next_message.text)
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
                    st.caption(
                        f"Tool `{message.tool_request.tool_name}` returned · {_token_count(str(message.result))}"
                    )
                    st.json(message.result, expanded=1)
        elif isinstance(message, ToolCallRequest):
            if show_thinking:
                with st.chat_message("tool", avatar="📞"):
                    st.caption(
                        f"Calling `{message.tool_name}` tool · {_token_count(str(message.tool_input))}"
                    )
                    st.code(json.dumps(message.tool_input), language="json")
        elif isinstance(message, ToolResultError):
            if show_thinking:
                with st.chat_message("tool", avatar="❌"):
                    st.caption(
                        f"Tool `{message.tool_request.tool_name}` error · {_token_count(str(message.error))}"
                    )
                    st.code(str(message.error))
        else:
            st.error(f"Unknown message type: {type(message)}")
            assert_never(message)
