"""
Chat Engine - Core orchestration layer for chat functionality.

This module provides the main ChatEngine class that coordinates conversation
management, agent management, and message processing. It serves as the core
business logic layer that can be used by both Streamlit and FastAPI frontends.
"""

import time
from typing import Any, Callable, Dict, Generator, Optional

from loguru import logger

# Import ConnectionConfig from parent directory
import sys
from pathlib import Path

parent_dir = Path(__file__).parent.parent.parent.parent
if str(parent_dir) not in sys.path:
    sys.path.insert(0, str(parent_dir))

from connection_manager import ConnectionConfig, ConnectionMode

from .agent_manager import AgentManager
from .conversation_manager import Conversation, ConversationManager
from .message_handler import MessageHandler


class ChatEngine:
    """
    Core chat engine that orchestrates conversations, agents, and message processing.

    This class provides a unified interface for chat functionality that works
    across different connection modes (embedded, HTTP) and can be used by
    multiple frontend implementations.
    """

    def __init__(self, config: ConnectionConfig) -> None:
        """
        Initialize the chat engine.

        Args:
            config: Connection configuration
        """
        self.config = config
        self.conversation_manager = ConversationManager()
        self.agent_manager = AgentManager()
        self.message_handler = MessageHandler()
        self.user_urn = "urn:li:corpuser:api-user"

        logger.info(f"Initialized ChatEngine with mode: {config.mode}")

    def create_conversation(self, title: str = "New conversation") -> Conversation:
        """
        Create a new conversation.

        Args:
            title: Title for the conversation

        Returns:
            Created conversation
        """
        return self.conversation_manager.create_conversation(title)

    def get_conversation(self, conv_id: str) -> Optional[Conversation]:
        """
        Get a conversation by ID.

        Args:
            conv_id: Conversation ID

        Returns:
            Conversation or None if not found
        """
        return self.conversation_manager.get_conversation(conv_id)

    def list_conversations(self) -> list[Conversation]:
        """
        List all conversations.

        Returns:
            List of conversations
        """
        return self.conversation_manager.list_conversations()

    def delete_conversation(self, conv_id: str) -> bool:
        """
        Delete a conversation.

        Args:
            conv_id: Conversation ID

        Returns:
            True if deleted successfully
        """
        # Also clean up associated agent
        self.agent_manager.reset_agent(conv_id)
        return self.conversation_manager.delete_conversation(conv_id)

    def send_message(
        self,
        conv_id: str,
        message: str,
        is_auto: bool = False,
        progress_callback: Optional[Callable[[str], None]] = None,
    ) -> tuple[bool, Optional[str], float]:
        """
        Send a message in a conversation.

        Args:
            conv_id: Conversation ID
            message: User's message text
            is_auto: Whether this is an auto-generated message
            progress_callback: Optional callback for progress updates

        Returns:
            Tuple of (success: bool, error_msg: Optional[str], duration: float)
        """
        conv = self.conversation_manager.get_conversation(conv_id)
        if not conv:
            logger.error(f"Conversation not found: {conv_id}")
            return False, "Conversation not found", 0.0

        # Add user message to conversation
        self.conversation_manager.add_user_message(conv_id, message, is_auto)

        # Initialize history if needed
        if conv.history is None:
            try:
                from datahub_integrations.chat.chat_history import ChatHistory

                conv.history = ChatHistory()
                # Add the user message we just added
                from datahub_integrations.chat.chat_history import HumanMessage

                conv.history.add_message(HumanMessage(text=message))
            except ImportError:
                logger.warning("ChatHistory not available")

        start_time = time.time()

        # Dispatch based on connection mode
        if self.config.mode == ConnectionMode.EMBEDDED:
            success, error_msg = self._send_message_embedded(
                conv_id, conv, progress_callback
            )
        else:
            # HTTP modes (LOCAL, REMOTE, LOCAL_SERVICE, CUSTOM)
            success, error_msg = self._send_message_http(
                conv, message, progress_callback
            )

        duration = time.time() - start_time

        # Extract actual assistant response from agent history
        logger.info(f"Attempting to extract assistant response - success={success}, has_history={conv.history is not None}, msg_count={len(conv.history.messages) if conv.history else 0}")
        content = "Response generated"
        if success and conv.history and conv.history.messages:
            # Find the last assistant/respond_to_user message
            try:
                from datahub_integrations.chat.chat_history import AssistantMessage, ToolCallRequest

                logger.debug(f"Extracting assistant response from {len(conv.history.messages)} messages")
                for msg in reversed(conv.history.messages):
                    if isinstance(msg, AssistantMessage):
                        content = msg.text
                        logger.info(f"Extracted AssistantMessage: {content[:100]}...")
                        break
                    elif isinstance(msg, ToolCallRequest) and msg.tool_name == "respond_to_user":
                        # Extract response from respond_to_user tool call
                        if hasattr(msg, "tool_input") and isinstance(msg.tool_input, dict):
                            content = msg.tool_input.get("response", content)
                            logger.info(f"Extracted respond_to_user response: {content[:100]}...")
                        break
            except (ImportError, AttributeError) as e:
                logger.warning(f"Could not extract assistant message: {e}")
                content = "Response generated"

        if not success:
            content = f"Error: {error_msg}"

        # Add assistant response to conversation
        event_count = len(conv.history.messages) if conv.history else 0

        logger.info(f"Adding assistant message to conversation {conv_id}: {content[:100]}...")
        self.conversation_manager.add_assistant_message(
            conv_id=conv_id,
            content=content,
            duration=duration,
            event_count=event_count,
            success=success,
            is_auto=is_auto,
        )
        logger.info(f"Assistant message added successfully to {conv_id}")

        return success, error_msg, duration

    def send_message_stream(
        self,
        conv_id: str,
        message: str,
        is_auto: bool = False,
        progress_callback: Optional[Callable[[str], None]] = None,
    ) -> Generator[Dict[str, Any], None, None]:
        """
        Send a message and stream the response.

        This is primarily used for HTTP streaming mode.

        Args:
            conv_id: Conversation ID
            message: User's message text
            is_auto: Whether this is an auto-generated message
            progress_callback: Optional callback for progress updates

        Yields:
            Event dictionaries with response data
        """
        conv = self.conversation_manager.get_conversation(conv_id)
        if not conv:
            yield {"event_type": "error", "error": "Conversation not found"}
            return

        # Add user message to conversation
        self.conversation_manager.add_user_message(conv_id, message, is_auto)

        # Initialize history if needed
        if conv.history is None:
            try:
                from datahub_integrations.chat.chat_history import ChatHistory

                conv.history = ChatHistory()
            except ImportError:
                logger.warning("ChatHistory not available")

        start_time = time.time()

        # For embedded mode, stream messages as they're generated
        if self.config.mode == ConnectionMode.EMBEDDED:
            agent = self.agent_manager.get_or_create_agent(
                conv_id, self.config.gms_url, self.config.gms_token
            )

            if not agent:
                yield {"event_type": "error", "error": "Agent not available"}
                return

            # Track final content for saving (with all thinking/tool XML)
            content = "Response generated"
            success = False
            error_msg = None
            final_text = ""  # The actual response text
            events_xml = []  # List of XML blocks for thinking/tools (in order)

            # Track timing and tokens for each event
            event_start_times = {}  # event_id -> start_time
            event_token_counts = {}  # event_id -> token_count
            event_metadata = {}  # event_id -> {tokens, wallClockTime, duration, etc}
            thinking_event_id = 0
            tool_call_event_id = 0
            thinking_start_time = None  # Wall clock start time

            def estimate_tokens(text: str) -> int:
                """Estimate token count (rough: ~4 chars per token)"""
                return max(1, len(text) // 4)

            try:
                from datahub_integrations.chat.chat_history import (
                    AssistantMessage,
                    ToolCallRequest,
                    ToolResult,
                    ReasoningMessage,
                )

                # Stream messages in real-time from the agent thread
                for event_type, data in self.message_handler.process_embedded_message_stream(
                    conv, agent
                ):
                    if event_type == 'message':
                        msg = data
                        msg_type = type(msg).__name__
                        logger.debug(f"Streaming real-time message: {msg_type}")

                        # Skip the initial HumanMessage (already added to UI)
                        if msg_type == "HumanMessage":
                            continue

                        # Map agent message types to frontend message types
                        if isinstance(msg, ReasoningMessage):
                            # Initialize wall clock start time
                            if thinking_start_time is None:
                                thinking_start_time = time.time()

                            # Send thinking message immediately as it's generated
                            msg_text = msg.text if hasattr(msg, 'text') else str(msg)
                            thinking_event_id += 1
                            event_id = f"thinking-{thinking_event_id}"

                            # Track start time, tokens, and wall clock time
                            event_start_times[event_id] = time.time()
                            tokens = estimate_tokens(msg_text)
                            event_token_counts[event_id] = tokens
                            wall_clock_time = time.time() - thinking_start_time
                            event_metadata[event_id] = {
                                "tokens": tokens,
                                "wallClockTime": wall_clock_time,
                                "type": "thinking"
                            }

                            # Build XML for storage (will be updated with duration later)
                            thinking_xml = f'<thinking eventId="{event_id}" tokens="{tokens}" wallClockTime="{wall_clock_time:.2f}">{msg_text}</thinking>'
                            events_xml.append((event_id, thinking_xml))

                            yield {
                                "event_type": "message",
                                "message": {
                                    "type": "THINKING",
                                    "time": int(time.time() * 1000),
                                    "actor": {"type": "AGENT"},
                                    "content": {
                                        "text": msg_text,
                                        "tokens": tokens,
                                        "eventId": event_id
                                    }
                                }
                            }
                        elif isinstance(msg, ToolCallRequest):
                            # Calculate duration for previous thinking if any
                            if thinking_event_id > 0:
                                prev_event_id = f"thinking-{thinking_event_id}"
                                if prev_event_id in event_start_times:
                                    duration = time.time() - event_start_times[prev_event_id]
                                    event_metadata[prev_event_id]["duration"] = duration

                                    # Update stored XML with duration
                                    for i, (eid, xml) in enumerate(events_xml):
                                        if eid == prev_event_id:
                                            # Add duration attribute before closing tag
                                            updated_xml = xml.replace(
                                                f'wallClockTime="{event_metadata[prev_event_id]["wallClockTime"]:.2f}">',
                                                f'wallClockTime="{event_metadata[prev_event_id]["wallClockTime"]:.2f}" duration="{duration:.2f}">',
                                            )
                                            events_xml[i] = (eid, updated_xml)
                                            break

                                    yield {
                                        "event_type": "message",
                                        "message": {
                                            "type": "THINKING_COMPLETE",
                                            "time": int(time.time() * 1000),
                                            "content": {
                                                "eventId": prev_event_id,
                                                "duration": duration
                                            }
                                        }
                                    }

                            # Initialize wall clock start time
                            if thinking_start_time is None:
                                thinking_start_time = time.time()

                            # Send tool call message immediately
                            tool_name = msg.tool_name if hasattr(msg, 'tool_name') else 'unknown'
                            tool_input = msg.tool_input if hasattr(msg, 'tool_input') else {}
                            tool_call_event_id += 1
                            event_id = f"tool-{tool_call_event_id}"

                            # Estimate tokens for tool call (name + input)
                            tool_text = f"{tool_name} {str(tool_input)}"
                            tokens = estimate_tokens(tool_text)
                            event_start_times[event_id] = time.time()
                            event_token_counts[event_id] = tokens
                            wall_clock_time = time.time() - thinking_start_time
                            event_metadata[event_id] = {
                                "tokens": tokens,
                                "wallClockTime": wall_clock_time,
                                "type": "tool"
                            }

                            # Build XML for storage
                            import json
                            tool_xml = f'<tool_use eventId="{event_id}" tokens="{tokens}" wallClockTime="{wall_clock_time:.2f}">\n'
                            tool_xml += f'<tool_name>{tool_name}</tool_name>\n'
                            if tool_input:
                                tool_xml += f'<parameters>{json.dumps(tool_input, indent=2)}</parameters>\n'
                            tool_xml += '</tool_use>'
                            events_xml.append((event_id, tool_xml))

                            yield {
                                "event_type": "message",
                                "message": {
                                    "type": "TOOL_CALL",
                                    "time": int(time.time() * 1000),
                                    "actor": {"type": "AGENT"},
                                    "content": {
                                        "text": f"Calling tool: {tool_name}",
                                        "toolName": tool_name,
                                        "toolInput": tool_input,
                                        "tokens": tokens,
                                        "eventId": event_id
                                    }
                                }
                            }

                            # Extract final content from respond_to_user
                            if tool_name == "respond_to_user":
                                if hasattr(msg, "tool_input") and isinstance(msg.tool_input, dict):
                                    final_text = msg.tool_input.get("response", final_text)
                        elif isinstance(msg, AssistantMessage):
                            # Extract content for saving (final text)
                            final_text = msg.text if hasattr(msg, 'text') else str(msg)
                        elif isinstance(msg, ToolResult):
                            # Calculate duration for tool call
                            if tool_call_event_id > 0:
                                prev_event_id = f"tool-{tool_call_event_id}"
                                if prev_event_id in event_start_times:
                                    duration = time.time() - event_start_times[prev_event_id]
                                    # Estimate result tokens
                                    result_text = str(msg.result) if hasattr(msg, 'result') else ''
                                    result_tokens = estimate_tokens(result_text)

                                    event_metadata[prev_event_id]["duration"] = duration
                                    event_metadata[prev_event_id]["resultTokens"] = result_tokens

                                    # Update stored XML with duration and resultTokens
                                    for i, (eid, xml) in enumerate(events_xml):
                                        if eid == prev_event_id:
                                            # Add duration and resultTokens attributes
                                            updated_xml = xml.replace(
                                                f'wallClockTime="{event_metadata[prev_event_id]["wallClockTime"]:.2f}">',
                                                f'wallClockTime="{event_metadata[prev_event_id]["wallClockTime"]:.2f}" duration="{duration:.2f}" resultTokens="{result_tokens}">',
                                            )
                                            events_xml[i] = (eid, updated_xml)
                                            break

                                    yield {
                                        "event_type": "message",
                                        "message": {
                                            "type": "TOOL_COMPLETE",
                                            "time": int(time.time() * 1000),
                                            "content": {
                                                "eventId": prev_event_id,
                                                "duration": duration,
                                                "resultTokens": result_tokens
                                            }
                                        }
                                    }
                            continue

                    elif event_type == 'done':
                        # Agent finished successfully
                        logger.info("Agent processing completed")
                        success = True

                        duration = time.time() - start_time

                        # Build full content with all events and final text (like frontend does)
                        events_xml_strings = [xml for _, xml in events_xml]
                        full_content = '\n\n'.join(events_xml_strings)
                        if final_text:
                            full_content += '\n\n' + final_text

                        # Fallback if no content was captured
                        if not full_content.strip():
                            full_content = "Response generated"

                        # Save assistant message with full XML content
                        event_count = len(conv.history.messages) if conv.history else 0
                        logger.info(f"Adding assistant message to conversation {conv_id} with {len(events_xml)} events")
                        self.conversation_manager.add_assistant_message(
                            conv_id=conv_id,
                            content=full_content,
                            duration=duration,
                            event_count=event_count,
                            success=True,
                            is_auto=is_auto,
                        )
                        logger.info(f"Assistant message added successfully to {conv_id}")

                        # Send final TEXT message with the response (just the text, not XML)
                        yield {
                            "event_type": "message",
                            "message": {
                                "type": "TEXT",
                                "time": int(time.time() * 1000),
                                "actor": {"type": "AGENT"},
                                "content": {"text": final_text or "Response generated"}
                            }
                        }
                        yield {"event_type": "complete", "duration": duration}
                        break

                    elif event_type == 'error':
                        # Agent encountered an error
                        error_msg = data
                        logger.error(f"Agent error during streaming: {error_msg}")

                        duration = time.time() - start_time

                        # Save error message
                        self.conversation_manager.add_assistant_message(
                            conv_id=conv_id,
                            content=f"Error: {error_msg}",
                            duration=duration,
                            event_count=0,
                            success=False,
                            is_auto=is_auto,
                        )

                        yield {"event_type": "error", "error": error_msg}
                        break

            except Exception as e:
                logger.error(f"Embedded mode streaming error: {e}", exc_info=True)
                yield {"event_type": "error", "error": str(e)}

        else:
            # HTTP streaming mode
            for event in self.message_handler.process_http_stream(
                conv,
                message,
                self.user_urn,
                self.config.integrations_url,
                self.config.gms_token or "",
                self.conversation_manager,
                progress_callback,
            ):
                yield event

    def _send_message_embedded(
        self,
        conv_id: str,
        conv: Conversation,
        progress_callback: Optional[Callable[[str], None]],
    ) -> tuple[bool, Optional[str]]:
        """
        Handle message sending in embedded mode.

        Args:
            conv_id: Conversation ID
            conv: Conversation object
            progress_callback: Optional progress callback

        Returns:
            Tuple of (success: bool, error_msg: Optional[str])
        """
        agent = self.agent_manager.get_or_create_agent(
            conv_id, self.config.gms_url, self.config.gms_token
        )

        if not agent:
            return False, "Agent not available"

        return self.message_handler.process_embedded_message(
            conv, agent, progress_callback
        )

    def _send_message_http(
        self,
        conv: Conversation,
        message: str,
        progress_callback: Optional[Callable[[str], None]],
    ) -> tuple[bool, Optional[str]]:
        """
        Handle message sending via HTTP streaming.

        Args:
            conv: Conversation object
            message: User's message
            progress_callback: Optional progress callback

        Returns:
            Tuple of (success: bool, error_msg: Optional[str])
        """
        success = False
        error_msg = None

        for event in self.message_handler.process_http_stream(
            conv,
            message,
            self.user_urn,
            self.config.integrations_url,
            self.config.gms_token or "",
            self.conversation_manager,
            progress_callback,
        ):
            event_type = event["event_type"]

            if event_type == "complete":
                success = True
                break
            elif event_type == "error":
                success = False
                error_msg = event["error"]
                break

        return success, error_msg

    def update_config(self, config: ConnectionConfig) -> None:
        """
        Update the connection configuration.

        Args:
            config: New connection configuration
        """
        self.config = config
        # Reset all agents when config changes
        self.agent_manager.reset_all_agents()
        logger.info(f"Updated config to mode: {config.mode}")
