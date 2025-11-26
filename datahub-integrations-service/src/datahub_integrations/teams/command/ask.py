from typing import Callable, List, Optional

from datahub.ingestion.graph.client import DataHubGraph
from datahub.sdk.main_client import DataHubClient
from loguru import logger

from datahub_integrations.chat.agent import AgentRunner
from datahub_integrations.chat.agent.progress_tracker import ProgressUpdate
from datahub_integrations.chat.agents import create_data_catalog_explorer_agent
from datahub_integrations.chat.chat_history import HumanMessage
from datahub_integrations.chat.types import ChatType, NextMessage
from datahub_integrations.teams.teams_history import TeamsConversationHistory


def _save_thinking_messages(
    agent: AgentRunner,
    response_text: str,
    message_ts: str,
    conv_history: TeamsConversationHistory,
    conversation_id: str,
) -> None:
    """
    Save AI thinking messages (tool calls, reasoning, etc.) to conversation history.

    Args:
        agent: The agent runner containing message history
        response_text: The final response text to exclude from thinking messages
        message_ts: Message timestamp for conversation history
        conv_history: Conversation history cache
        conversation_id: Conversation ID for logging
    """
    # Get the new messages that were added during this generation
    original_message_count = len(agent.history.messages) if agent.history else 0
    new_messages = agent.history.messages[original_message_count:]

    if not new_messages:
        return

    # Save the thinking messages (everything except the final response)
    thinking_messages = []
    for msg in new_messages:
        # Skip the final response message, keep the thinking/reasoning
        if not (hasattr(msg, "text") and msg.text == response_text):
            thinking_messages.append(msg)

    if thinking_messages:
        conv_history.add_thinking(message_ts, thinking_messages)
        logger.info(
            f"Saved {len(thinking_messages)} thinking messages to conversation history for {conversation_id}"
        )


async def handle_ask_command_teams(
    graph: DataHubGraph,
    question: str,
    user_urn: Optional[str] = None,
    progress_callback: Optional[Callable[[List[ProgressUpdate]], None]] = None,
    conversation_id: Optional[str] = None,
    message_ts: Optional[str] = None,
) -> dict:
    """Handle Teams ask commands using the DataHub AI chat session."""

    try:
        if not question.strip():
            return {
                "type": "message",
                "text": "Please provide a question after the ask command. For example: `/datahub ask What tables contain customer data?`",
            }

        logger.info(f"Processing AI question: {question}")

        # Create a new chat session for this question
        # We need to run this in a separate thread to avoid asyncio conflicts
        import asyncio
        import concurrent.futures

        def run_chat_session() -> tuple[NextMessage, AgentRunner]:
            """Run agent session in a separate thread to avoid asyncio conflicts."""
            # Check if conversation history is enabled
            from datahub_integrations.teams.config import teams_config

            config = teams_config.get_config()

            # Determine if we should use conversation history
            history = None
            if config.enable_conversation_history and conversation_id:
                # Get conversation history from cache
                conv_history = teams_config.get_teams_history_cache().get_conversation(
                    conversation_id
                )

                # Add the current user message to the history
                if message_ts:
                    conv_history.add_message(
                        message_ts, HumanMessage(text=question), is_latest_message=True
                    )

                # Get the chat history for the session
                history = conv_history.get_chat_history()
                logger.info(
                    f"Using conversation history with {len(history.messages)} messages"
                )

            agent = create_data_catalog_explorer_agent(
                client=DataHubClient(graph=graph),
                history=history,  # Use conversation history if enabled
                chat_type=ChatType.TEAMS,
            )

            # If no history or empty history, add the user's question to the chat history
            if history is None or not history.messages:
                agent.history.add_message(HumanMessage(text=question))

            # Use the provided progress callback or create a default one
            session_progress_callback = progress_callback or (
                lambda steps: logger.debug(
                    f"AI progress: {steps[-1].text if steps else 'Starting...'}"
                )
            )

            # Generate the AI response
            with agent.set_progress_callback(session_progress_callback):
                response = agent.generate_formatted_message()
                return response, agent

        # Run the chat session in a thread pool to avoid asyncio conflicts
        loop = asyncio.get_event_loop()
        with concurrent.futures.ThreadPoolExecutor() as executor:
            response, agent = await loop.run_in_executor(executor, run_chat_session)

        assert isinstance(response, NextMessage)

        # Convert markdown to Teams-friendly format
        response_text = response.text

        # Create the main text response (without suggestions in text)
        final_message = response_text

        # Save the assistant's response and thinking to conversation history if enabled
        from datahub_integrations.chat.chat_history import AssistantMessage
        from datahub_integrations.teams.config import teams_config

        config = teams_config.get_config()
        if config.enable_conversation_history and conversation_id and message_ts:
            conv_history = teams_config.get_teams_history_cache().get_conversation(
                conversation_id
            )

            # Save the assistant's response message
            conv_history.add_message(
                f"{message_ts}_response", AssistantMessage(text=response_text)
            )

            # Save AI thinking messages (tool calls, reasoning, etc.)
            _save_thinking_messages(
                agent, response_text, message_ts, conv_history, conversation_id
            )

            logger.info(
                f"Saved assistant response to conversation history for {conversation_id}"
            )

        # Return text-only response for message updates
        # Teams doesn't support updating messages with attachments
        result = {
            "type": "message",
            "text": final_message,
            "suggestions": response.suggestions,  # Pass suggestions separately for post-processing
        }

        return result

    except Exception as e:
        logger.error(f"Error in AI ask command: {e}")

        # Fallback to simple text response for now
        return {
            "type": "message",
            "text": f"I encountered an error processing your question: {str(e)}. Please try again or contact support if the issue persists.",
        }
