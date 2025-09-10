from typing import Callable, List, Optional

from datahub.ingestion.graph.client import DataHubGraph
from datahub.sdk.main_client import DataHubClient
from loguru import logger

from datahub_integrations.chat.chat_history import HumanMessage
from datahub_integrations.chat.chat_session import ChatSession, NextMessage
from datahub_integrations.mcp.mcp_server import mcp


async def handle_ask_command_teams(
    graph: DataHubGraph,
    question: str,
    user_urn: Optional[str] = None,
    progress_callback: Optional[Callable[[List[str]], None]] = None,
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

        def run_chat_session() -> str:
            """Run chat session in a separate thread to avoid asyncio conflicts."""
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

            chat_session = ChatSession(
                tools=[mcp],
                client=DataHubClient(graph=graph),
                history=history,  # Use conversation history if enabled
            )

            # If no history or empty history, add the user's question to the chat history
            if history is None or not history.messages:
                chat_session.history.add_message(HumanMessage(text=question))

            # Use the provided progress callback or create a default one
            session_progress_callback = progress_callback or (
                lambda steps: logger.debug(
                    f"AI progress: {steps[-1] if steps else 'Starting...'}"
                )
            )

            # Generate the AI response
            with chat_session.set_progress_callback(session_progress_callback):
                return chat_session.generate_next_message()

        # Run the chat session in a thread pool to avoid asyncio conflicts
        loop = asyncio.get_event_loop()
        with concurrent.futures.ThreadPoolExecutor() as executor:
            response = await loop.run_in_executor(executor, run_chat_session)

        assert isinstance(response, NextMessage)

        # Convert markdown to Teams-friendly format
        response_text = response.text

        # Create the main text response (without suggestions in text)
        final_message = response_text

        # Save the assistant's response to conversation history if enabled
        from datahub_integrations.chat.chat_history import AssistantMessage
        from datahub_integrations.teams.config import teams_config

        config = teams_config.get_config()
        if config.enable_conversation_history and conversation_id and message_ts:
            # For now, we'll use the same message_ts for the assistant response
            # In a more sophisticated implementation, we'd get the actual response message_ts
            conv_history = teams_config.get_teams_history_cache().get_conversation(
                conversation_id
            )
            conv_history.add_message(
                f"{message_ts}_response", AssistantMessage(text=response_text)
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
