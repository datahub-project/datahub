"""
DataHub Teams Bot using Microsoft Bot Framework SDK.

This module provides a proper Bot Framework implementation to replace
the manual HTTP-based Teams integration.
"""

import queue
from typing import Any, List, Optional

from botbuilder.core import (
    ActivityHandler,
    MessageFactory,
    TurnContext,
)
from botbuilder.schema import (
    Activity,
    Attachment,
)
from loguru import logger

from datahub_integrations.app import graph
from datahub_integrations.identity.identity_provider import IdentityProvider
from datahub_integrations.teams.event_handlers import TeamsIncidentEventHandler


class DataHubTeamsBot(ActivityHandler):
    """
    DataHub Teams Bot using Bot Framework SDK.

    This replaces the manual HTTP-based implementation with proper
    Bot Framework SDK that handles authentication, conversations,
    and message formatting automatically.
    """

    def __init__(self) -> None:
        super().__init__()
        self._cached_bot_name: Optional[str] = None
        # Initialize incident event handler
        self.identity_provider = IdentityProvider(graph)
        self.incident_event_handler = TeamsIncidentEventHandler(
            graph, self.identity_provider
        )

    def _get_cached_bot_name(self, turn_context: TurnContext) -> Optional[str]:
        """Get bot name, preferring display name from mention entities over other sources."""
        logger.debug(
            f"_get_cached_bot_name called: cached_name={self._cached_bot_name}, has_recipient={bool(turn_context.activity.recipient)}"
        )

        # Try to get display name from mention entities first (this has the proper display name)
        display_name = self._extract_bot_display_name_from_mentions(
            turn_context.activity
        )
        if display_name and display_name != self._cached_bot_name:
            old_name = self._cached_bot_name
            self._cached_bot_name = display_name
            logger.info(
                f"🔄 Updated bot name from {old_name} to: @{self._cached_bot_name}"
            )
            return self._cached_bot_name

        # If we found display name and it's already cached, return it (don't continue to fallback)
        if display_name:
            logger.debug(f"Display name already cached: {self._cached_bot_name}")
            return self._cached_bot_name

        # Fallback: return the cached bot name, which could be None
        return self._cached_bot_name

    def _extract_bot_display_name_from_mentions(
        self, activity: Activity
    ) -> Optional[str]:
        """Extract bot's display name from mention entities."""
        if not activity.entities:
            return None

        mention_entities = [e for e in activity.entities if e.type == "mention"]
        if not mention_entities:
            return None

        for entity in mention_entities:
            # Check if this mention is for our bot
            try:
                mentioned = entity.mentioned
                if mentioned and mentioned.id == activity.recipient.id:
                    # This mention is for our bot - get the display name
                    display_name = mentioned.name
                    if display_name:
                        logger.debug(
                            f"Found bot display name in mention: {display_name}"
                        )
                        return display_name
            except AttributeError:
                # Bot Framework Python SDK stores mention data in additional_properties
                additional_props = getattr(entity, "additional_properties", {})
                if additional_props:
                    mentioned_id = additional_props.get("mentioned", {}).get("id")
                    mentioned_name = additional_props.get("mentioned", {}).get("name")

                    if mentioned_id == activity.recipient.id and mentioned_name:
                        logger.debug(
                            f"Found bot display name in additional_properties: {mentioned_name}"
                        )
                        return mentioned_name

        return None

    async def on_message_activity(self, turn_context: TurnContext) -> None:
        """
        Handle incoming messages from Teams.

        Args:
            turn_context: Bot Framework turn context containing the message
        """
        try:
            logger.debug("🔥 on_message_activity called - about to cache bot name")
            # Cache bot name on first message (with nice log line)
            self._get_cached_bot_name(turn_context)

            # Get the incoming message text
            message_text = turn_context.activity.text or ""

            logger.info(f"Received Teams message: {message_text}")
            logger.debug(f"Activity type: {turn_context.activity.type}")
            logger.debug(
                f"Activity has entities: {bool(turn_context.activity.entities)}"
            )
            logger.debug(f"Activity recipient: {turn_context.activity.recipient}")

            # Check if this is a button click (Action.Submit from adaptive card)
            if turn_context.activity.value and not message_text:
                logger.info(
                    f"Handling button click with value: {turn_context.activity.value}"
                )
                await self._handle_action_submission(turn_context)
                return

            # Handle different types of messages
            if message_text.startswith("/datahub"):
                await self._handle_command(turn_context, message_text)
            elif self._is_mention(turn_context.activity):
                await self._handle_mention(turn_context, message_text)
            else:
                await self._handle_general_message(turn_context, message_text)

        except Exception as e:
            logger.error(f"Error handling Teams message: {e}")
            error_message = MessageFactory.text(
                "Sorry, I encountered an error processing your message."
            )
            await turn_context.send_activity(error_message)

    async def on_invoke_activity(self, turn_context: TurnContext) -> None:
        """
        Handle invoke activities (button clicks, etc.).

        Args:
            turn_context: Bot Framework turn context containing the invoke
        """
        try:
            logger.info(f"Received Teams invoke: {turn_context.activity.name}")

            # Handle different invoke types
            if turn_context.activity.name == "composeExtension/query":
                # Handle search queries
                await self._handle_search_invoke(turn_context)
            elif turn_context.activity.name == "task/fetch":
                # Handle task module requests
                await self._handle_task_fetch(turn_context)
            else:
                logger.warning(f"Unhandled invoke type: {turn_context.activity.name}")

        except Exception as e:
            logger.error(f"Error handling Teams invoke: {e}")

    def _is_mention(self, activity: Activity) -> bool:
        """Check if the activity mentions the bot (handles multiple mentions)."""
        # First, try to find bot mentions in entities if they exist
        if activity.entities:
            mention_entities = [e for e in activity.entities if e.type == "mention"]
            if mention_entities:
                logger.debug(f"Found {len(mention_entities)} mention entities to check")

                for entity in mention_entities:
                    # Try the standard Bot Framework approach first
                    try:
                        mentioned = entity.mentioned
                        if mentioned and mentioned.id == activity.recipient.id:
                            logger.debug(
                                f"Bot mentioned via standard attribute (among {len(mention_entities)} mentions)"
                            )
                            return True
                    except AttributeError:
                        # Bot Framework Python SDK stores mention data in additional_properties
                        additional_props = getattr(entity, "additional_properties", {})
                        if additional_props:
                            # Extract mention info from additional properties
                            mentioned_id = additional_props.get("mentioned", {}).get(
                                "id"
                            )
                            mentioned_name = additional_props.get("mentioned", {}).get(
                                "name", "unknown"
                            )

                            logger.debug(
                                f"Checking mention: {mentioned_name} (id: {mentioned_id})"
                            )

                            if mentioned_id == activity.recipient.id:
                                logger.debug(
                                    f"Bot mentioned via additional_properties (among {len(mention_entities)} mentions)"
                                )
                                return True

                logger.debug(
                    f"Bot not found among {len(mention_entities)} mention entities"
                )

        # Fallback: check if the message text contains an @mention of the bot
        # This handles cases where entity parsing completely fails or there are no entities
        if activity.text and activity.recipient and activity.recipient.name:
            if f"<at>{activity.recipient.name}</at>" in activity.text:
                logger.debug("Bot mention detected via text parsing (fallback)")
                return True

        return False

    async def _handle_command(self, turn_context: TurnContext, command: str) -> None:
        """
        Handle /datahub commands.

        Args:
            turn_context: Bot Framework turn context
            command: The command string
        """
        command_lower = command.lower()

        if command_lower.startswith("/datahub search"):
            await self._handle_search_command(turn_context, command)
        elif command_lower.startswith("/datahub get"):
            await self._handle_get_command(turn_context, command)
        elif command_lower.startswith("/datahub ask"):
            await self._handle_ask_command(turn_context, command)
        elif command_lower == "/datahub help":
            await self._handle_help_command(turn_context)
        else:
            help_message = MessageFactory.text(
                "Unknown command. Use `/datahub help` to see available commands."
            )
            await turn_context.send_activity(help_message)

    async def _handle_mention(self, turn_context: TurnContext, message: str) -> None:
        """
        Handle @mentions of the bot.

        Args:
            turn_context: Bot Framework turn context
            message: The message text
        """
        # Remove the mention and treat as an ask command
        cleaned_message = self._remove_mention_text(message)
        await self._handle_ask_command(turn_context, f"ask {cleaned_message}")

    async def _handle_general_message(
        self, turn_context: TurnContext, message: str
    ) -> None:
        """
        Handle general messages (treat as ask commands).

        Args:
            turn_context: Bot Framework turn context
            message: The message text
        """
        await self._handle_ask_command(turn_context, f"ask {message}")

    async def _handle_search_command(
        self, turn_context: TurnContext, command: str
    ) -> None:
        """Handle search commands."""
        # Import here to avoid circular imports
        from datahub_integrations.app import graph
        from datahub_integrations.teams.command.search import (
            handle_search_command_teams,
        )
        from datahub_integrations.teams.context import SearchContext

        try:
            # Extract search query from command
            search_query = command[len("/datahub search") :].strip()

            if not search_query:
                help_message = MessageFactory.text(
                    "Please provide a search query. Example: `/datahub search user_table`"
                )
                await turn_context.send_activity(help_message)
                return

            # Create search context
            context = SearchContext(query=search_query)

            # Extract user URN from Teams activity
            user_urn = None
            if turn_context.activity.from_property:
                from datahub_integrations.teams.utils.datahub_user import (
                    get_user_information_from_teams_activity,
                )

                email, user_urn, user_urns = get_user_information_from_teams_activity(
                    turn_context.activity
                )

            # Process search command
            response = await handle_search_command_teams(graph, context, user_urn)

            if response and response.get("attachments"):
                # Create message with adaptive card
                message = MessageFactory.attachment(
                    Attachment(
                        content_type="application/vnd.microsoft.card.adaptive",
                        content=response["attachments"][0]["content"],
                    )
                )
                await turn_context.send_activity(message)
            else:
                no_results_message = MessageFactory.text(
                    f"No results found for: {search_query}"
                )
                await turn_context.send_activity(no_results_message)

        except Exception as e:
            logger.error(f"Error handling search command: {e}")
            error_message = MessageFactory.text(
                "Sorry, I encountered an error processing your search."
            )
            await turn_context.send_activity(error_message)

    async def _handle_get_command(
        self, turn_context: TurnContext, command: str
    ) -> None:
        """Handle get commands."""
        # Import here to avoid circular imports
        from datahub_integrations.app import graph
        from datahub_integrations.teams.command.get import handle_get_command_teams

        try:
            # Extract URN from command
            urn_text = command[len("/datahub get") :].strip()

            if not urn_text:
                help_message = MessageFactory.text(
                    "Please provide a URN. Example: `/datahub get urn:li:dataset:...`"
                )
                await turn_context.send_activity(help_message)
                return

            # Process get command
            response = await handle_get_command_teams(graph, urn_text)

            if response and response.get("attachments"):
                # Create message with adaptive card
                message = MessageFactory.attachment(
                    Attachment(
                        content_type="application/vnd.microsoft.card.adaptive",
                        content=response["attachments"][0]["content"],
                    )
                )
                await turn_context.send_activity(message)
            else:
                not_found_message = MessageFactory.text(
                    f"Could not find entity: {urn_text}"
                )
                await turn_context.send_activity(not_found_message)

        except Exception as e:
            logger.error(f"Error handling get command: {e}")
            error_message = MessageFactory.text(
                "Sorry, I encountered an error processing your request."
            )
            await turn_context.send_activity(error_message)

    async def _handle_ask_command(
        self, turn_context: TurnContext, command: str
    ) -> None:
        """Handle ask commands (AI chat)."""
        # Import here to avoid circular imports
        from datahub_integrations.app import graph

        try:
            # Extract question from command
            question = (
                command[len("ask") :].strip() if command.startswith("ask") else command
            )

            if not question:
                help_message = MessageFactory.text(
                    "Please ask a question. Example: `@DataHub what datasets are available?`"
                )
                await turn_context.send_activity(help_message)
                return

            # Send initial "thinking" message and store reference
            thinking_text, thinking_detail = self._build_teams_progress_message(
                ["🤔 Let me think about that..."]
            )
            thinking_message = MessageFactory.text(thinking_text)
            thinking_activity = await turn_context.send_activity(thinking_message)

            # Store the thinking message ID for updates
            thinking_message_id = thinking_activity.id if thinking_activity else None

            # Get conversation ID for history tracking (thread-aware for channels)
            from datahub_integrations.teams.conversation_utils import (
                get_teams_conversation_id,
            )

            conversation_id = get_teams_conversation_id(turn_context.activity)
            message_ts = turn_context.activity.id

            # Create thread-safe queue for progress updates
            import queue

            progress_queue: "queue.Queue[dict[str, Any]]" = queue.Queue()

            # Progress callback for AI reasoning steps
            def progress_callback(steps: list["ProgressUpdate"]) -> None:
                """Update progress with reasoning steps."""
                try:
                    if steps:
                        step_texts = [step.text for step in steps]
                        progress_text, progress_detail = (
                            self._build_teams_progress_message(step_texts)
                        )
                        logger.info(f"AI Progress: {progress_text}")

                        # Send progress update to queue (thread-safe)
                        progress_queue.put(
                            {
                                "type": "progress",
                                "detail": progress_detail,
                                "thinking_message_id": thinking_message_id,
                            }
                        )
                except Exception as e:
                    logger.warning(f"Failed to update progress: {e}")

            # Start background task to process progress updates
            import asyncio

            progress_task = asyncio.create_task(
                self._process_progress_queue(turn_context, progress_queue)
            )

            # Create agent for AI processing
            from datahub.sdk.main_client import DataHubClient

            from datahub_integrations.chat.agent import AgentRunner
            from datahub_integrations.chat.agent.progress_tracker import ProgressUpdate
            from datahub_integrations.chat.agents import (
                create_data_catalog_explorer_agent,
            )
            from datahub_integrations.chat.chat_history import HumanMessage
            from datahub_integrations.chat.types import ChatType, NextMessage
            from datahub_integrations.teams.config import teams_config

            config = teams_config.get_config()
            history = None

            if config.enable_conversation_history and conversation_id:
                conv_history = teams_config.get_teams_history_cache().get_conversation(
                    conversation_id
                )
                if message_ts:
                    conv_history.add_message(
                        message_ts, HumanMessage(text=question), is_latest_message=True
                    )
                history = conv_history.get_chat_history()

            # Run agent in executor to avoid asyncio conflicts
            import asyncio
            import concurrent.futures

            def run_agent() -> tuple[NextMessage, AgentRunner]:
                """Run agent in separate thread to avoid asyncio conflicts."""
                agent = create_data_catalog_explorer_agent(
                    client=DataHubClient(graph=graph),
                    history=history,
                    chat_type=ChatType.TEAMS,
                )

                # Generate response with progress updates
                with agent.set_progress_callback(progress_callback):
                    response = agent.generate_formatted_message()
                    return response, agent

            # Execute in thread pool to avoid "Already running asyncio" error
            loop = asyncio.get_event_loop()
            with concurrent.futures.ThreadPoolExecutor() as executor:
                ai_response, agent = await loop.run_in_executor(executor, run_agent)

            assert isinstance(ai_response, NextMessage)

            # Save AI thinking messages to conversation history if enabled
            if config.enable_conversation_history and conversation_id and message_ts:
                # Get the new messages that were added during this generation
                # We need to find messages that weren't in the original history
                original_message_count = len(history.messages) if history else 0
                new_messages = agent.history.messages[original_message_count:]

                if new_messages:
                    # Save the thinking messages (everything except the final response)
                    thinking_messages = []
                    for msg in new_messages:
                        # Skip the final response message, keep the thinking/reasoning
                        if not (hasattr(msg, "text") and msg.text == ai_response.text):
                            thinking_messages.append(msg)

                    if thinking_messages:
                        conv_history.add_thinking(message_ts, thinking_messages)
                        logger.info(
                            f"Saved {len(thinking_messages)} thinking messages to conversation history for {conversation_id}"
                        )

            # Signal progress updates to stop
            progress_queue.put({"type": "stop"})

            # Wait for progress task to finish and cancel it
            try:
                await asyncio.wait_for(progress_task, timeout=1.0)
            except asyncio.TimeoutError:
                progress_task.cancel()

            # Build Teams response with follow-up suggestions
            response_text = ai_response.text
            followup_suggestions = ai_response.suggestions or []

            # Create final response with follow-up questions as separate messages
            bot_name = self._get_cached_bot_name(turn_context)
            final_message, suggestions_message = self._build_teams_final_response(
                response_text, followup_suggestions, bot_name
            )

            # Update the thinking message with the final response instead of sending new message
            if thinking_message_id:
                await self._update_teams_message(
                    turn_context, thinking_message_id, final_message.text
                )
            else:
                # Fallback: send as new message if we couldn't update
                await turn_context.send_activity(final_message)

            # Send suggestions as a separate message if available
            if suggestions_message:
                await turn_context.send_activity(suggestions_message)

        except Exception as e:
            logger.error(f"Error handling ask command: {e}")
            error_message = MessageFactory.text(
                "Sorry, I encountered an error processing your question."
            )
            await turn_context.send_activity(error_message)

    async def _handle_help_command(self, turn_context: TurnContext) -> None:
        """Handle help commands."""
        help_text = """
**DataHub Bot Commands:**

🔍 **Search**: `/datahub search <query>` - Search for datasets, dashboards, etc.
📋 **Get**: `/datahub get <urn>` - Get details about a specific entity
🤖 **Ask**: `@DataHub <question>` or just message me directly - Ask AI about your data

**Examples:**
• `/datahub search user_table`
• `/datahub get urn:li:dataset:(urn:li:dataPlatform:bigquery,project.dataset.table,PROD)`
• `@DataHub what datasets are available for marketing?`

Need more help? Check out the [DataHub documentation](https://datahubproject.io/docs/).
        """

        help_message = MessageFactory.text(help_text.strip())
        await turn_context.send_activity(help_message)

    async def _handle_search_invoke(self, turn_context: TurnContext) -> None:
        """Handle search messaging extension invokes."""
        # This would handle Teams messaging extension search queries
        # For now, return empty response
        pass

    async def _handle_task_fetch(self, turn_context: TurnContext) -> None:
        """Handle task module fetch requests."""
        # This would handle Teams task module requests
        # For now, return empty response
        pass

    async def _update_teams_message(
        self, turn_context: TurnContext, message_id: str, new_text: str
    ) -> None:
        """Update a Teams message with new content."""
        try:
            from botbuilder.schema import Activity

            # Create proper activity for update
            update_activity = Activity(
                id=message_id,
                type="message",
                text=new_text,
            )

            await turn_context.update_activity(update_activity)
            logger.debug(f"Updated Teams message {message_id}")
        except Exception as e:
            logger.warning(f"Failed to update Teams message {message_id}: {e}")

    async def _process_progress_queue(
        self, turn_context: TurnContext, progress_queue: "queue.Queue[dict[str, Any]]"
    ) -> None:
        """Process progress updates from the queue in the main asyncio loop."""
        import asyncio
        import queue

        while True:
            try:
                # Check for progress updates with short timeout
                try:
                    progress_update = progress_queue.get(timeout=0.1)
                except queue.Empty:
                    # No updates, continue checking
                    await asyncio.sleep(0.1)
                    continue

                if progress_update.get("type") == "stop":
                    logger.debug("Progress queue processing stopped")
                    break

                if progress_update.get("type") == "progress":
                    thinking_message_id = progress_update.get("thinking_message_id")
                    progress_detail = progress_update.get("detail")

                    if thinking_message_id and progress_detail:
                        await self._update_teams_message(
                            turn_context, thinking_message_id, progress_detail
                        )
                        logger.debug(
                            f"Updated Teams message with progress: {progress_detail[:50]}..."
                        )

                # Mark this update as processed
                progress_queue.task_done()

            except Exception as e:
                logger.warning(f"Error processing progress queue: {e}")
                await asyncio.sleep(0.1)

    def _remove_mention_text(self, text: str) -> str:
        """Remove bot mention from message text."""
        # Remove @mentions - this is a simplified version
        # In reality, you'd want to parse the entities to remove mentions properly
        import re

        return re.sub(r"<at>.*?</at>", "", text).strip()

    def _build_teams_progress_message(self, steps: list[str]) -> tuple[str, str]:
        """Build Teams progress message with reasoning steps - todo list style with checkmarks."""
        if not steps:
            return "🤔 Thinking...", "🤔 Thinking..."

        # Current step is always the last one
        current_step = steps[-1]
        # Previous steps are everything except the last
        previous_steps = steps[:-1]

        # Show last 9 previous steps (to stay within Teams limits)
        shown_previous = previous_steps[-9:]

        # Build display elements for Teams - todo list style
        elements = []
        for step in shown_previous:
            elements.append(f"✅ {step}")
        elements.append(f"⏳ **{current_step}**")

        # Create the todo-list style progress text with newlines
        if elements:
            progress_text = "\n\n".join(elements)  # Double newline for better spacing
        else:
            progress_text = f"⏳ **{current_step}**"

        # Return both a simple fallback and the detailed progress
        return f"⏳ **{current_step}**", progress_text

    def _build_teams_final_response(
        self,
        response_text: str,
        followup_suggestions: list[str],
        bot_name: Optional[str] = None,
    ) -> tuple["MessageFactory", "MessageFactory | None"]:
        """Build final Teams response with follow-up suggestions as separate messages."""
        # Use response text directly without adding salutations
        message = response_text

        # Add feedback prompt and hint (similar to Slack implementation)
        message += "\n\n*💬 Was this helpful? Let me know if you need clarification or have follow-up questions!*"

        # Add hint with dynamic bot name
        if bot_name:
            bot_mention = f"@{bot_name}"
            message += (
                f"\n\n*💡 Hint: Mention {bot_mention} for responses on this thread.*"
            )

        # Create main message activity without user mentions
        main_activity = MessageFactory.text(message)

        # Create separate suggestions card using Bot Framework if we have suggestions
        suggestions_activity = None
        if followup_suggestions:
            from datahub_integrations.teams.cards.adaptive_cards import (
                create_teams_suggestions_card,
            )

            suggestions_card = create_teams_suggestions_card(followup_suggestions)
            if suggestions_card:
                # Convert adaptive card to Bot Framework attachment
                from botbuilder.schema import Attachment

                attachment = Attachment(
                    content_type="application/vnd.microsoft.card.adaptive",
                    content=suggestions_card["content"],
                )
                suggestions_activity = MessageFactory.attachment(attachment)

        return main_activity, suggestions_activity

    async def _handle_action_submission(self, turn_context: TurnContext) -> None:
        """
        Handle Action.Submit events from adaptive card buttons.

        Teams expects an immediate response to button clicks to avoid timeout errors.
        This method sends an immediate acknowledgment, then processes the action asynchronously.

        Args:
            turn_context: Bot Framework turn context containing the action data
        """
        try:
            action_data = turn_context.activity.value
            if not action_data:
                logger.warning("Action submission received with no data")
                return

            action_type = action_data.get("action")
            logger.info(f"Processing action submission of type: {action_type}")

            if action_type == "followup_question":
                # Extract the follow-up question from button click
                question = action_data.get("question")
                if not question:
                    logger.warning(
                        "Follow-up question action received with no question text"
                    )
                    return

                logger.info(f"Processing follow-up question: {question}")

                # Send immediate acknowledgment to prevent Teams timeout
                thinking_message = MessageFactory.text("🤔 Let me think about that...")
                thinking_activity = await turn_context.send_activity(thinking_message)
                thinking_message_id = (
                    thinking_activity.id if thinking_activity else None
                )

                # Process the question asynchronously to avoid blocking Teams response
                import asyncio

                asyncio.create_task(
                    self._process_followup_question_async(
                        turn_context, question, thinking_message_id
                    )
                )

            elif action_type in ["resolve_incident", "reopen_incident"]:
                # Handle incident management actions
                await self._handle_incident_action(turn_context, action_data)

            else:
                logger.warning(f"Unknown action type received: {action_type}")

        except Exception as e:
            logger.error(f"Error handling action submission: {e}")
            error_message = MessageFactory.text(
                "Sorry, I encountered an error processing that button click."
            )
            await turn_context.send_activity(error_message)

    async def _handle_incident_action(
        self, turn_context: TurnContext, action_data: dict
    ) -> None:
        """
        Handle incident management actions (resolve/reopen).

        Args:
            turn_context: Bot Framework turn context
            action_data: Data from the Teams Action.Submit
        """
        try:
            # Send immediate acknowledgment to prevent Teams timeout
            thinking_message = MessageFactory.text("🔄 Processing incident action...")
            await turn_context.send_activity(thinking_message)

            # Extract user information from Teams activity
            email: Optional[str] = None
            datahub_user_urn: Optional[str] = None
            user_urns: List[str] = []
            if turn_context.activity.from_property:
                from datahub_integrations.teams.utils.datahub_user import (
                    get_user_information_from_teams_activity,
                )

                email, datahub_user_urn, user_urns = (
                    get_user_information_from_teams_activity(turn_context.activity)
                )

            if not datahub_user_urn:
                teams_user_id = (
                    turn_context.activity.from_property.id
                    if turn_context.activity.from_property
                    else "unknown"
                )
                logger.warning(
                    f"Could not find corresponding DataHub user for Teams user {teams_user_id} (email: {email}). Using system user."
                )
            elif len(user_urns) > 1:
                logger.warning(
                    f"Found multiple DataHub users with email {email}. Using system user."
                )
                datahub_user_urn = None

            # Process the incident action using the shared event handler with DataHub user URN
            result = await self.incident_event_handler.handle_incident_action(
                action_data, datahub_user_urn
            )

            # Send response based on result
            if result.get("success"):
                success_message = MessageFactory.text(
                    f"✅ {result.get('message', 'Action completed successfully')}"
                )
                await turn_context.send_activity(success_message)
            else:
                error_message = MessageFactory.text(
                    f"❌ {result.get('error', 'Action failed')}"
                )
                await turn_context.send_activity(error_message)

        except Exception as e:
            logger.error(f"Error handling incident action: {e}")
            error_message = MessageFactory.text(
                "❌ Sorry, I encountered an error processing the incident action."
            )
            await turn_context.send_activity(error_message)

    async def _process_followup_question_async(
        self,
        turn_context: TurnContext,
        question: str,
        thinking_message_id: Optional[str] = None,
    ) -> None:
        """
        Process a follow-up question asynchronously after sending immediate acknowledgment.

        Args:
            turn_context: Bot Framework turn context
            question: The follow-up question to process
            thinking_message_id: ID of the thinking message to update with the response
        """
        try:
            logger.info(f"Asynchronously processing follow-up question: {question}")

            # Get conversation ID for history tracking (thread-aware for channels)
            from datahub_integrations.teams.conversation_utils import (
                get_teams_conversation_id,
            )

            conversation_id = get_teams_conversation_id(turn_context.activity)
            message_ts = turn_context.activity.id

            # Create thread-safe queue for progress updates
            import queue

            progress_queue: "queue.Queue[dict[str, Any]]" = queue.Queue()

            # Progress callback for AI reasoning steps
            def progress_callback(steps: list["ProgressUpdate"]) -> None:
                """Update progress with reasoning steps."""
                try:
                    if steps:
                        step_texts = [step.text for step in steps]
                        progress_text, progress_detail = (
                            self._build_teams_progress_message(step_texts)
                        )
                        logger.info(f"AI Progress: {progress_text}")

                        # Send progress update to queue (thread-safe)
                        progress_queue.put(
                            {
                                "type": "progress",
                                "detail": progress_detail,
                                "thinking_message_id": thinking_message_id,
                            }
                        )
                except Exception as e:
                    logger.warning(f"Failed to update progress: {e}")

            # Start background task to process progress updates
            import asyncio

            progress_task = asyncio.create_task(
                self._process_progress_queue(turn_context, progress_queue)
            )

            # Create agent for AI processing (same logic as _handle_ask_command)
            from datahub.sdk.main_client import DataHubClient

            from datahub_integrations.chat.agent import AgentRunner
            from datahub_integrations.chat.agent.progress_tracker import ProgressUpdate
            from datahub_integrations.chat.agents import (
                create_data_catalog_explorer_agent,
            )
            from datahub_integrations.chat.chat_history import HumanMessage
            from datahub_integrations.chat.types import ChatType, NextMessage
            from datahub_integrations.teams.config import teams_config

            config = teams_config.get_config()
            history = None

            if config.enable_conversation_history and conversation_id:
                conv_history = teams_config.get_teams_history_cache().get_conversation(
                    conversation_id
                )
                if message_ts:
                    conv_history.add_message(
                        message_ts, HumanMessage(text=question), is_latest_message=True
                    )
                history = conv_history.get_chat_history()

            # Run agent in executor to avoid asyncio conflicts
            import concurrent.futures

            def run_agent() -> tuple[NextMessage, AgentRunner]:
                """Run agent in separate thread to avoid asyncio conflicts."""
                from datahub_integrations.app import graph

                agent = create_data_catalog_explorer_agent(
                    client=DataHubClient(graph=graph),
                    history=history,
                    chat_type=ChatType.TEAMS,
                )

                # Generate response with progress updates
                with agent.set_progress_callback(progress_callback):
                    response = agent.generate_formatted_message()
                    return response, agent

            # Execute in thread pool to avoid "Already running asyncio" error
            loop = asyncio.get_event_loop()
            with concurrent.futures.ThreadPoolExecutor() as executor:
                ai_response, agent = await loop.run_in_executor(executor, run_agent)

            assert isinstance(ai_response, NextMessage)

            # Save AI thinking messages to conversation history if enabled
            if config.enable_conversation_history and conversation_id and message_ts:
                # Get the new messages that were added during this generation
                # We need to find messages that weren't in the original history
                original_message_count = len(history.messages) if history else 0
                new_messages = agent.history.messages[original_message_count:]

                if new_messages:
                    # Save the thinking messages (everything except the final response)
                    thinking_messages = []
                    for msg in new_messages:
                        # Skip the final response message, keep the thinking/reasoning
                        if not (hasattr(msg, "text") and msg.text == ai_response.text):
                            thinking_messages.append(msg)

                    if thinking_messages:
                        conv_history.add_thinking(message_ts, thinking_messages)
                        logger.info(
                            f"Saved {len(thinking_messages)} thinking messages to conversation history for {conversation_id}"
                        )

            # Signal progress updates to stop
            progress_queue.put({"type": "stop"})

            # Wait for progress task to finish and cancel it
            try:
                await asyncio.wait_for(progress_task, timeout=1.0)
            except asyncio.TimeoutError:
                progress_task.cancel()

            # Build Teams response with follow-up suggestions
            response_text = ai_response.text
            followup_suggestions = ai_response.suggestions or []

            # Create final response with follow-up questions as separate messages
            bot_name = self._get_cached_bot_name(turn_context)
            final_message, suggestions_message = self._build_teams_final_response(
                response_text, followup_suggestions, bot_name
            )

            # Update the thinking message with the final response
            if thinking_message_id:
                await self._update_teams_message(
                    turn_context, thinking_message_id, final_message.text
                )
            else:
                # Fallback: send as new message if we couldn't update
                await turn_context.send_activity(final_message)

            # Send suggestions as a separate message if available
            if suggestions_message:
                await turn_context.send_activity(suggestions_message)

        except Exception as e:
            logger.error(f"Error processing follow-up question asynchronously: {e}")

            # Send error response
            error_message = MessageFactory.text(
                "Sorry, I encountered an error processing your follow-up question."
            )

            # Update thinking message if possible, otherwise send new message
            if thinking_message_id:
                try:
                    await self._update_teams_message(
                        turn_context, thinking_message_id, error_message.text
                    )
                except Exception:
                    await turn_context.send_activity(error_message)
            else:
                await turn_context.send_activity(error_message)
