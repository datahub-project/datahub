import streamlit as st
from datahub.sdk.main_client import DataHubClient

from datahub_integrations.chat.chat_history import HumanMessage
from datahub_integrations.chat.chat_session import ChatSession
from datahub_integrations.chat.mcp_server import mcp
from datahub_integrations.experimentation.st_chat_history import st_chat_history


@st.cache_resource
def client() -> DataHubClient:
    return DataHubClient.from_env()


@st.cache_data
def frontend_url() -> str:
    return client()._graph.frontend_base_url


st.markdown(f"Connected to {frontend_url()}")


def _make_empty_chat_session() -> ChatSession:
    st.text("Resetting chat session")
    return ChatSession(
        tools=mcp.get_all_tools(),
        client=client(),
    )


def _chat_session() -> ChatSession:
    # Initialize chat session if it doesn't exist
    if "chat_session" not in st.session_state:
        st.session_state.chat_session = _make_empty_chat_session()
    return st.session_state.chat_session


_chat_session()

_has_history = bool(_chat_session().history.messages)

col1, col2, col3 = st.columns(3)

with col1:
    show_thinking = st.toggle("Show internal thinking", value=True)
with col2:  # Add a clear chat button
    if st.button("Clear Chat", disabled=not _has_history):
        st.session_state.chat_session = _make_empty_chat_session()
        st.rerun()
with col3:  # Add a download history button
    st.download_button(
        "Save Chat",
        _chat_session().history.json(indent=2),
        file_name="history.json",
        mime="application/json",
        disabled=not _has_history,
    )

st.divider()

# Chat UI.
st_chat_history(
    _chat_session().history,
    show_thinking=show_thinking,
    frontend_url=frontend_url(),
)
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
