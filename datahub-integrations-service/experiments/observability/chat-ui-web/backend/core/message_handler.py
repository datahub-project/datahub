"""
Message Handler - Handles message processing for embedded and HTTP modes.

Extracted from chat_ui.py to provide reusable message processing logic
that works with both embedded agents and HTTP streaming.
"""

import asyncio
import concurrent.futures
import queue
import time
from typing import Any, Callable, Dict, Generator, Optional

from loguru import logger

from .conversation_manager import Conversation, ProcessingState


class MessageHandler:
    """Handles message processing for different connection modes."""

    def __init__(self) -> None:
        """Initialize message handler."""
        pass

    def _run_agent_in_thread(
        self,
        agent: Any,
        message_queue: Optional[queue.Queue] = None,
    ) -> None:
        """
        Run the agent in a separate thread with its own event loop.

        This avoids the "Already running asyncio in this thread" error
        that occurs when running async MCP tools from within FastAPI's
        event loop.

        Args:
            agent: The agent to run
            message_queue: Optional queue to push messages as they're generated
        """
        # Create a new event loop for this thread
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        try:
            # Track messages already sent
            sent_count = 0

            # Set up callback to monitor new messages
            if message_queue:
                def check_for_new_messages(messages=None):
                    nonlocal sent_count
                    # Check if there are new messages in the agent's history
                    current_count = len(agent.history.messages)
                    if current_count > sent_count:
                        # Send new messages to the queue
                        for msg in agent.history.messages[sent_count:]:
                            message_queue.put(('message', msg))
                        sent_count = current_count

                # Run agent with progress callback that checks for new messages
                with agent.set_progress_callback(check_for_new_messages):
                    agent.generate_formatted_message()

                # Send any remaining messages
                check_for_new_messages()

                # Signal completion
                message_queue.put(('done', None))
            else:
                # Run agent without streaming
                agent.generate_formatted_message()
        except Exception as e:
            if message_queue:
                message_queue.put(('error', str(e)))
            raise
        finally:
            loop.close()

    def process_embedded_message(
        self,
        conversation: Conversation,
        agent: Any,
        progress_callback: Optional[Callable[[str], None]] = None,
    ) -> tuple[bool, Optional[str]]:
        """
        Process a message using embedded agent mode (blocking, non-streaming).

        Args:
            conversation: The conversation to process
            agent: The AgentRunner instance
            progress_callback: Optional callback for progress updates

        Returns:
            Tuple of (success: bool, error_msg: Optional[str])
        """
        try:
            from datahub_integrations.chat.chat_history import HumanMessage

            # Get the last user message from conversation history and add to agent
            # This is required for Bedrock API - conversation must start with user message
            if conversation.history.messages:
                last_message = conversation.history.messages[-1]
                if isinstance(last_message, HumanMessage):
                    agent.add_message(last_message)
                    logger.debug(f"Added user message to agent: {last_message.text[:100]}")

            # Run agent in a separate thread to avoid asyncio conflicts
            # FastAPI runs in an event loop, and MCP tools are async,
            # so we need a separate thread with its own event loop
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(
                    self._run_agent_in_thread,
                    agent,
                    None  # No queue for non-streaming mode
                )
                # Wait for completion (blocking)
                future.result()

            # Agent updates its own history, so we just reference it
            conversation.history = agent.history

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
                logger.error(f"Connection error: {error_msg}")
                return False, f"Connection Error: {error_msg}"
            else:
                return False, error_msg

    def process_embedded_message_stream(
        self,
        conversation: Conversation,
        agent: Any,
    ) -> Generator[tuple[str, Any], None, None]:
        """
        Process a message using embedded agent mode with real-time streaming.

        Yields messages as the agent generates them by polling the agent's history.

        Args:
            conversation: The conversation to process
            agent: The AgentRunner instance

        Yields:
            Tuples of (event_type, data):
                - ('message', Message): A new message from the agent
                - ('done', None): Processing completed successfully
                - ('error', str): An error occurred
        """
        try:
            from datahub_integrations.chat.chat_history import HumanMessage

            # Get the last user message from conversation history and add to agent
            if conversation.history.messages:
                last_message = conversation.history.messages[-1]
                if isinstance(last_message, HumanMessage):
                    agent.add_message(last_message)
                    logger.debug(f"Added user message to agent: {last_message.text[:100]}")

            # Track how many messages we've already streamed
            sent_count = len(agent.history.messages)
            logger.debug(f"Starting streaming from message index {sent_count}")

            # Run agent in a separate thread to avoid asyncio conflicts
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(
                    self._run_agent_in_thread,
                    agent,
                    None  # No queue - we'll poll the history instead
                )

                # Poll agent's history for new messages while thread runs
                while True:
                    # Check for new messages in agent history
                    current_count = len(agent.history.messages)
                    if current_count > sent_count:
                        # Stream new messages immediately
                        for msg in agent.history.messages[sent_count:]:
                            logger.debug(f"Streaming new message: {type(msg).__name__}")
                            yield ('message', msg)
                        sent_count = current_count

                    # Check if thread finished
                    if future.done():
                        try:
                            future.result()  # Raise exception if any
                            # Success - check for any final messages
                            current_count = len(agent.history.messages)
                            if current_count > sent_count:
                                for msg in agent.history.messages[sent_count:]:
                                    logger.debug(f"Streaming final message: {type(msg).__name__}")
                                    yield ('message', msg)
                            logger.debug("Agent processing completed successfully")
                            conversation.history = agent.history
                            yield ('done', None)
                            break
                        except Exception as e:
                            logger.error(f"Agent thread failed: {e}")
                            yield ('error', str(e))
                            break

                    # Small sleep to avoid busy-waiting
                    time.sleep(0.05)  # Poll every 50ms

        except Exception as e:
            logger.error(f"Embedded agent stream error: {e}", exc_info=True)
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
                yield ('error', f"Connection Error: {error_msg}")
            else:
                yield ('error', error_msg)

    def process_http_stream(
        self,
        conversation: Conversation,
        message: str,
        user_urn: str,
        integrations_url: str,
        gms_token: str,
        conversation_manager,
        progress_callback: Optional[Callable[[str], None]] = None,
    ) -> Generator[Dict[str, Any], None, None]:
        """
        Process a message using HTTP streaming mode.

        Yields events as they arrive from the backend service.

        Args:
            conversation: The conversation to process
            message: The user's message text
            user_urn: URN of the user
            integrations_url: URL of the integrations service
            gms_token: GMS authentication token
            progress_callback: Optional callback for progress updates

        Yields:
            Event dictionaries with keys:
                - event_type: "message" | "complete" | "error"
                - message: The message object (for "message" events)
                - error: Error message (for "error" events)
        """
        try:
            # Import ChatClient (from chat_simulator)
            import sys
            from pathlib import Path

            # Add parent directory to path to import chat_simulator
            parent_dir = Path(__file__).parent.parent.parent.parent
            if str(parent_dir) not in sys.path:
                sys.path.insert(0, str(parent_dir))

            from chat_simulator import ChatClient

            chat_client = ChatClient(integrations_url, gms_token)

            from datahub_integrations.chat.chat_history import (
                HumanMessage as HM,
                ReasoningMessage,
            )
            from datahub_integrations.chat.utils import parse_reasoning_message

            start_time = time.time()
            assistant_content_parts = []  # Collect all content for XML formatting
            final_text = ""
            thinking_count = 0
            tool_count = 0

            for event in chat_client.send_message_stream(
                message, conversation.urn, user_urn
            ):
                event_type = event["event_type"]

                if event_type == "message":
                    new_msg = event["message"]
                    msg_type = event.get("message_type", "TEXT")  # Get preserved message type
                    msg_data = event.get("message_data", {})  # Get original SSE data

                    # Skip duplicate user message
                    if isinstance(new_msg, HM) and new_msg.text == message:
                        continue

                    conversation.history.add_message(new_msg)

                    # Build XML-formatted content for frontend parser
                    if msg_type == "THINKING":
                        # Thinking message - wrap in XML
                        thinking_count += 1
                        wall_clock_time = time.time() - start_time
                        thinking_xml = f'<thinking eventId="thinking-{thinking_count}" wallClockTime="{wall_clock_time:.2f}">\n'
                        thinking_xml += new_msg.text
                        thinking_xml += '\n</thinking>\n'
                        assistant_content_parts.append(thinking_xml)

                        # Update progress display if provided
                        if progress_callback:
                            try:
                                parsed = parse_reasoning_message(new_msg.text)
                                user_visible = parsed.to_user_visible_message()
                                progress_callback(user_visible)
                            except Exception:
                                pass
                    elif msg_type == "TOOL_CALL":
                        # Tool call message - extract details from original SSE data
                        tool_count += 1
                        wall_clock_time = time.time() - start_time

                        # Extract tool info from message data
                        content = msg_data.get("content", {})
                        tool_name = content.get("toolName", "unknown")
                        tool_input = content.get("toolInput", {})

                        tool_xml = f'<tool_use eventId="tool-{tool_count}" wallClockTime="{wall_clock_time:.2f}">\n'
                        tool_xml += f'<tool_name>{tool_name}</tool_name>\n'
                        if tool_input:
                            import json
                            tool_xml += f'<parameters>{json.dumps(tool_input, indent=2)}</parameters>\n'
                        tool_xml += '</tool_use>\n'
                        assistant_content_parts.append(tool_xml)
                    elif msg_type == "TEXT":
                        # Final text message
                        if hasattr(new_msg, 'text') and new_msg.text:
                            final_text = new_msg.text

                    yield event

                elif event_type == "complete":
                    # Save assistant message to conversation storage
                    duration = time.time() - start_time
                    event_count = len(conversation.history.messages) if conversation.history else 0

                    # Combine XML parts with final text
                    full_content = "".join(assistant_content_parts)
                    if final_text:
                        full_content += final_text

                    logger.info(f"Saving assistant message to conversation {conversation.id} (thinking={thinking_count}, tools={tool_count})")
                    conversation_manager.add_assistant_message(
                        conv_id=conversation.id,
                        content=full_content or "Response generated",
                        duration=duration,
                        event_count=event_count,
                        success=True,
                        is_auto=False,
                    )
                    logger.info(f"Assistant message saved successfully to {conversation.id}")

                    yield event
                    break

                elif event_type == "error":
                    yield event
                    break

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
                yield {
                    "event_type": "error",
                    "error": f"Connection Error: {error_msg}",
                }
            else:
                yield {"event_type": "error", "error": error_msg}
