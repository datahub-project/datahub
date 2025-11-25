# DataHub-Backed Chat System

This module provides persistent chat conversations stored as DataHub aspects, enabling stateful agentic systems within the DataHub UI.

## Architecture

### Core Components

1. **`DataHubAiConversationClient`** - Handles DataHub persistence using DataHub-prefixed models
2. **`ChatSessionManager`** - Compositional wrapper around ChatSession with persistence capabilities
3. **`ChatSession`** - Data catalog exploration chat agent (uses AgentRunner infrastructure)
4. **`AgentRunner`** - Reusable agentic loop infrastructure
5. **`chat_api.py`** - REST API for DataHub UI integration

### Agent Infrastructure

The chat system is built on a **composition-based agent infrastructure** that separates reusable infrastructure from agent-specific business logic:

- **`agent/AgentRunner`**: Core infrastructure for agentic loops (tool execution, message handling, LLM interaction)
- **`agent/AgentConfig`**: Configuration for agent behavior (prompts, tools, inference params)
- **`agent/SystemPromptBuilder`**: Protocol for building system prompts
- **`agent/ProgressTracker`**: Progress tracking for streaming updates
- **`agent/tool_composition.py`**: Utilities for combining tools

This infrastructure enables:

- Creating specialized agents and subagents
- Reusing components across different agents
- Testing infrastructure independently from business logic
- Clear separation of concerns

See [Agent Infrastructure Design](../../../docs/AGENT_INFRASTRUCTURE_DESIGN.md) for detailed documentation on building custom agents.

### Data Models

The system uses the following DataHub-prefixed PDL models:

- `DataHubAiConversationInfo` - Main conversation aspect
- `DataHubAiConversationMessage` - Individual messages
- `DataHubAiConversationActor` - Message actors (USER/AGENT)
- `DataHubAiConversationMessageContent` - Message content
- `DataHubAiConversationMessageMention` - Entity mentions
- `DataHubAiConversationMessageAttachment` - Message attachments

## Usage

### Basic Chat Session

```python
from datahub_integrations.chat.chat_session import ChatSession
from datahub_integrations.chat.chat_session_manager import ChatSessionManager
from datahub_integrations.chat.types import ChatType
from datahub_integrations.mcp.mcp_server import mcp

# Create new conversation
chat_session = ChatSession(
    tools=[mcp],
    client=client,
    chat_type=ChatType.DATAHUB_UI,
)

chat_session_manager = ChatSessionManager(
    chat_session=chat_session,
    client=client,
    conversation_urn="urn:li:dataHubAiConversation:123",
    user_urn="urn:li:corpuser:user123",
)

# Send message
chat_session_manager.add_user_message("What datasets do we have?")
response = chat_session_manager.generate_next_message()
print(response.text)
```

### Resume Existing Conversation

```python
# Resume conversation
chat_session = ChatSession(
    tools=[mcp],
    client=client,
    chat_type=ChatType.DATAHUB_UI,
)

chat_session_manager = ChatSessionManager(
    chat_session=chat_session,
    client=client,
    conversation_urn="urn:li:conversation:123",
    user_urn="urn:li:corpuser:user123",
)
```

### REST API

```bash
# Send message
curl -X POST /api/chat/message \
  -H "Content-Type: application/json" \
  -d '{"text": "What datasets do we have?", "user_urn": "urn:li:corpuser:user123"}'

# List conversations
curl /api/chat/conversations/urn:li:corpuser:user123

# Resume conversation
curl -X POST /api/chat/conversation/urn:li:conversation:123/resume
```

## Features

- **Persistent Conversations**: Stored as DataHub aspects
- **Message Types**: TEXT, TOOL_CALL, TOOL_RESULT, THINKING
- **Rich Content**: Mentions, attachments, structured content
- **Auto-save**: Automatic conversation state management
- **Resume**: Continue conversations across sessions
- **Search**: Find and filter conversations
- **Composable Agents**: Build specialized agents using reusable infrastructure
- **Custom System Prompts**: Define agent behavior via SystemPromptBuilder
- **Dynamic Tool Selection**: Add/remove tools based on runtime conditions

## Integration with DataHub UI

The system provides REST API endpoints that can be integrated into the DataHub frontend:

1. **Chat Interface**: Send/receive messages
2. **Conversation List**: Browse user conversations
3. **Resume**: Continue existing conversations
4. **Search**: Find conversations by content

## Known Issues

⚠️ **Remaining PDL Model Issues**: The following issues still need to be fixed:

1. **DataHubAiConversationMessageAttachment.pdl**:

   ```pdl
   content: ConversationMessageContent  # Should be: DataHubAiConversationMessageContent
   ```

2. **DataHubAiConversationMessageContent.pdl**:
   ```pdl
   attachment: DataHubAiConversationAttachment  # Should be: DataHubAiConversationMessageAttachment
   mentions: DataHubAiConversationMention  # Should be: DataHubAiConversationMessageMention
   ```

✅ **Fixed Issues**:

- ✅ `text: string` in DataHubAiConversationMessageContent
- ✅ `array[DataHubAiConversationMessage]` in DataHubAiConversationInfo
- ✅ `DataHubAiConversationActor` and `DataHubAiConversationMessageContent` in DataHubAiConversationMessage
- ✅ Added `meta: DataHubAiConversationMetadata` field to DataHubAiConversationInfo

## Building Custom Agents

The agent infrastructure makes it easy to create specialized agents for specific tasks:

```python
from datahub_integrations.chat.agent import AgentConfig, AgentRunner, StaticPromptBuilder
from datahub_integrations.mcp.mcp_server import mcp

# Define custom prompt
prompt = "You are a data quality specialist..."

# Create configuration
config = AgentConfig(
    model_id="anthropic.claude-3-5-sonnet-20241022-v2:0",
    system_prompt_builder=StaticPromptBuilder(prompt),
    tools=[mcp],
    temperature=0.4,
)

# Create agent
agent = AgentRunner(config=config, client=client)
```

See `examples/schema_comparison_agent.py` for a complete example of building a specialized subagent.

For comprehensive documentation on the agent architecture and patterns, see [Agent Infrastructure Design](../../../docs/AGENT_INFRASTRUCTURE_DESIGN.md).

## Next Steps

1. Fix the PDL model issues listed above
2. Regenerate the schema classes
3. Test the conversation persistence
4. Add UI components for conversation management
5. Implement conversation search and filtering
