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


def st_chat_history(
    history: ChatHistory,
    *,
    show_thinking: bool = True,
) -> None:
    for message in history.messages:
        if isinstance(message, ReasoningMessage):
            if show_thinking:
                with st.chat_message("assistant", avatar="🧠"):
                    st.markdown(f"<pre>{message.text}</pre>", unsafe_allow_html=True)
        elif isinstance(message, HumanMessage):
            with st.chat_message("user"):
                st.markdown(message.text)
        elif isinstance(message, AssistantMessage):
            with st.chat_message("assistant"):
                st.markdown(message.text)
        elif ChatSession.is_respond_to_user(message):
            with st.chat_message("assistant"):
                next_message = NextMessage.model_validate(message.result)

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
