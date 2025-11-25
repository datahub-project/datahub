# DataHub Agent Infrastructure Design

## Overview

This document describes the reusable agent infrastructure that enables building multiple specialized agents and subagents within DataHub. The infrastructure separates the core agentic loop mechanics (tool execution, message handling, LLM interaction) from agent-specific business logic (prompts, tool selection, custom behaviors).

## Problem Statement

The original `ChatSession` class combined infrastructure concerns with DataHub-specific business logic:

- **Infrastructure**: Agentic loop, tool execution, message history management, progress tracking, LLM client interaction, token tracking, MLflow tracing
- **Business Logic**: DataHub-specific system prompts, tool selection (MCP tools, smart_search), internal tools (respond_to_user, planning tools), extra instructions fetching, chat type handling

This tight coupling made it difficult to:

1. Create new agents with different behaviors
2. Test infrastructure independently from business logic
3. Reuse components across multiple agents
4. Build specialized subagents for specific tasks

## Solution: Composition-Based Architecture

We use **composition over inheritance** where agents are built by assembling pluggable components rather than extending base classes.

### Core Components

```
┌─────────────────────────────────────────────────────────────┐
│                        AgentConfig                          │
│  ┌────────────────────────────────────────────────────┐    │
│  │ • system_prompt_builder: SystemPromptBuilder       │    │
│  │ • public_tools_factory: PublicToolsFactory         │    │
│  │ • internal_tools_factory: InternalToolsFactory     │    │
│  │ • context_reducers: Optional[List[Reducer]]        │    │
│  │ • model_id: str                                    │    │
│  │ • inference_config: dict                           │    │
│  └────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│                       AgentRunner                           │
│  ┌────────────────────────────────────────────────────────┐│
│  │ Infrastructure (reusable across all agents):          ││
│  │ • generate_next_message() - agentic loop             ││
│  │ • _generate_tool_call() - LLM interaction            ││
│  │ • _handle_tool_call_request() - tool execution       ││
│  │ • _prepare_messages() - message formatting/caching   ││
│  │ • set_progress_callback() - progress tracking        ││
│  │ • MLflow tracing for observability                   ││
│  │ • Token usage tracking                               ││
│  └────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│                    ChatSession (refactored)                 │
│  • Uses AgentRunner with DataHub-specific AgentConfig      │
│  • Maintains backward-compatible API                        │
│  • Adds DataHub-specific methods (get_plannable_tools())   │
│  • Manages plan_cache for planning tools                   │
└─────────────────────────────────────────────────────────────┘
```

## Architecture Details

### 1. AgentConfig

`AgentConfig` is a dataclass that defines all aspects of an agent's behavior through composition:

```python
@dataclass
class AgentConfig:
    """Configuration for agent behavior via composition."""

    # Core LLM settings
    model_id: str
    system_prompt_builder: SystemPromptBuilder

    # Tool factories (Protocol types for clarity)
    public_tools_factory: PublicToolsFactory
    internal_tools_factory: Optional[InternalToolsFactory] = None

    # Context management
    context_reducers: Optional[Iterable[ChatContextReducer]] = None

    # LLM inference settings
    use_prompt_caching: bool = True
    max_tool_calls: int = 30
    temperature: float = 0.5
    max_tokens: int = 4096
```

**Key Fields:**

- **`system_prompt_builder`**: A `SystemPromptBuilder` protocol implementation that constructs system messages. Allows agents to define their own prompts while reusing infrastructure.
- **`public_tools_factory`**: Factory for creating public tools (data-gathering, plannable tools). Receives `DataHubClient` and returns tools that define what the agent can do. Enables runtime decisions based on environment.

- **`internal_tools_factory`**: Optional factory for creating internal tools (control flow, orchestration). Receives `AgentRunner` instance and returns tools that control how the agent coordinates (e.g., `respond_to_user`, planning tools).

- **`context_reducers`**: Optional chain of context reducers for managing token limits (e.g., `ConversationSummarizer`, `SlidingWindowReducer`)

### Tool Factory Protocols

The tool factories use explicit Protocol types for clarity:

```python
class PublicToolsFactory(Protocol):
    """Factory for creating public tools (data-gathering, plannable tools)."""
    def __call__(self, client: DataHubClient) -> List[ToolWrapper]:
        """Create public tools with access to DataHub client."""
        ...

class InternalToolsFactory(Protocol):
    """Factory for creating internal tools (control flow, orchestration)."""
    def __call__(self, runner: AgentRunner) -> List[ToolWrapper]:
        """Create internal tools with access to agent runner."""
        ...
```

**Why two factories?**

- **Public tools**: Define what the agent **CAN DO** (capabilities exposed to LLM)
- **Internal tools**: Define how the agent **COORDINATES** (control flow, need runner access)

### 2. AgentRunner

`AgentRunner` is the core infrastructure class that implements the agentic loop. It's completely reusable and agent-agnostic.

**Key Responsibilities:**

- **Agentic Loop**: `generate_next_message()` orchestrates the reasoning → tool calling → response cycle
- **LLM Interaction**: Handles converse API calls with proper error handling
- **Tool Execution**: Executes tools with MLflow tracing and error recovery
- **Message Management**: Prepares messages with prompt caching support
- **Progress Tracking**: Manages progress callbacks for streaming UI updates
- **Observability**: MLflow tracing for debugging and monitoring

**Key Methods:**

```python
class AgentRunner:
    def __init__(
        self,
        config: AgentConfig,
        client: DataHubClient,
        history: Optional[ChatHistory] = None,
    ):
        """Initialize agent with configuration."""

    def generate_next_message(self) -> NextMessage:
        """Main agentic loop - generate next response via tool calls."""

    @contextlib.contextmanager
    def set_progress_callback(self, callback: ProgressCallback) -> Iterator[None]:
        """Set callback for progress updates during generation."""
```

### 3. SystemPromptBuilder Protocol

The `SystemPromptBuilder` protocol defines how agents construct their system prompts:

```python
class SystemPromptBuilder(Protocol):
    """Protocol for building system prompts."""

    def build_system_messages(
        self,
        client: DataHubClient
    ) -> List[SystemContentBlockTypeDef]:
        """Build system messages for the agent."""
        ...
```

This allows each agent to define its own prompt logic while maintaining a consistent interface.

### 4. Supporting Components

**ProgressTracker**: Generalizes the `FilteredProgressListener` pattern for tracking and streaming progress updates.

**ToolComposition**: Utilities for combining and filtering tools (e.g., `tools_from_fastmcp`, tool allowlists).

## Building Custom Agents

### Example 1: Simple Custom Agent with Different Prompt

Let's build a "Schema Comparison Agent" that specializes in comparing dataset schemas:

```python
from datahub.sdk.main_client import DataHubClient
from datahub_integrations.chat.agent import (
    AgentConfig,
    AgentRunner,
    StaticPromptBuilder,
    static_tools,  # Helper for simple tool lists
)
from datahub_integrations.mcp.mcp_server import mcp
from datahub_integrations.mcp_integration.tool import ToolWrapper

# Step 1: Define custom system prompt
SCHEMA_COMPARISON_PROMPT = """
You are a Schema Comparison Specialist for DataHub.

Your expertise is comparing dataset schemas to identify:
- Field additions and removals
- Type changes and incompatibilities
- Column renames and migrations
- Breaking vs non-breaking changes

Always present schema differences in a structured format with:
1. Summary of changes (added/removed/modified fields)
2. Impact assessment (breaking vs non-breaking)
3. Recommended migration strategy

Be concise and focus on actionable insights.
"""

# Step 2: Define agent-specific tools
def report_schema_differences(
    dataset_a_urn: str,
    dataset_b_urn: str,
    differences: dict
) -> str:
    """Format and report schema differences between two datasets."""
    summary = f"Comparing {dataset_a_urn} vs {dataset_b_urn}:\n"
    summary += f"- Added fields: {len(differences.get('added', []))}\n"
    summary += f"- Removed fields: {len(differences.get('removed', []))}\n"
    summary += f"- Modified fields: {len(differences.get('modified', []))}\n"
    return summary

# Step 3: Create agent configuration
def create_schema_comparison_agent(client: DataHubClient) -> AgentRunner:
    """Factory function to create a schema comparison agent."""

    # Define internal tools factory
    def create_internal_tools(runner):
        return [
            ToolWrapper.from_function(
                fn=report_schema_differences,
                name="report_schema_differences",
                description="Report differences between two dataset schemas"
            )
        ]

    config = AgentConfig(
        model_id="anthropic.claude-3-5-sonnet-20241022-v2:0",
        system_prompt_builder=StaticPromptBuilder(SCHEMA_COMPARISON_PROMPT),
        public_tools_factory=static_tools([mcp]),  # Simple helper for static tools!
        internal_tools_factory=create_internal_tools,
        temperature=0.3,  # Lower temperature for consistent comparisons
        max_tool_calls=15,  # Fewer calls needed for focused task
    )

    return AgentRunner(config=config, client=client)

# Step 4: Use the agent
agent = create_schema_comparison_agent(client)
agent.history.add_message(HumanMessage(
    text="Compare the schemas of prod.users and staging.users"
))
response = agent.generate_next_message()
print(response.text)
```

**Key Points:**

- Custom prompt using `StaticPromptBuilder` for simplicity
- Uses `static_tools()` helper for simple tool lists
- Reuses existing MCP tools (no duplication)
- Adds specialized internal tool for reporting
- Adjusts inference parameters for the task
- Clear separation: public tools (what agent can do) vs internal tools (how it coordinates)

### Example 2: Agent with Dynamic Tool Selection

Let's build an "Impact Analysis Agent" that adds tools dynamically based on runtime conditions:

```python
from datahub_integrations.chat.agent import StaticPromptBuilder, flatten_tools

IMPACT_ANALYSIS_PROMPT = """
You are an Impact Analysis Specialist for DataHub.

Your job is to analyze the downstream impact of changes to data assets.

For any proposed change:
1. Identify all directly affected entities
2. Trace downstream dependencies via lineage
3. Categorize impact severity (high/medium/low)
4. Identify stakeholders who should be notified

Use lineage tools extensively and be thorough in dependency analysis.
"""

def create_impact_analysis_tools(client: DataHubClient) -> List[ToolWrapper]:
    """
    Create public tools with conditional logic.

    This factory demonstrates runtime decisions based on environment.
    """
    # Start with base MCP tools
    tools = flatten_tools([mcp])

    # Add reranking search if available (runtime decision!)
    if bedrock_supports_rerank():
        from datahub_integrations.smart_search.smart_search import smart_search
        from datahub_integrations.mcp_integration.tool import async_background

        tools.append(
            ToolWrapper.from_function(
                fn=async_background(smart_search),
                name="smart_search",
                description="AI-powered search with relevance reranking"
            )
        )

    # Add bulk lineage fetching tool
    tools.append(
        ToolWrapper.from_function(
            fn=fetch_bulk_downstream_lineage,
            name="fetch_bulk_lineage",
            description="Efficiently fetch lineage for multiple entities"
        )
    )

    return tools

def create_impact_analysis_agent(client: DataHubClient) -> AgentRunner:
    """Create an agent specialized for impact analysis."""

    config = AgentConfig(
        model_id="anthropic.claude-3-5-sonnet-20241022-v2:0",
        system_prompt_builder=StaticPromptBuilder(IMPACT_ANALYSIS_PROMPT),
        public_tools_factory=create_impact_analysis_tools,  # Conditional logic in factory!
        temperature=0.4,
        max_tool_calls=40,  # Impact analysis may need many tool calls
    )

    return AgentRunner(config=config, client=client)
```

**Key Points:**

- Public tools factory makes runtime decisions (smart_search only if supported)
- All conditional logic in one clear place
- Factory receives `DataHubClient` for environment checks
- Clean separation of what the agent can do (public tools)

### Example 3: ChatSession-like Agent with Different Behavior

Here's how to build an agent similar to `ChatSession` but with customized prompts and logic:

```python
from datahub_integrations.chat.chat_history import ChatHistory, HumanMessage
from datahub_integrations.chat.types import ChatType
from datahub_integrations.chat.agent import static_tools
from typing import Optional

class CustomDataGovernancePromptBuilder(SystemPromptBuilder):
    """Custom prompt for data governance assistant."""

    def __init__(self, organization_name: str):
        self.organization_name = organization_name

    def build_system_messages(self, client: DataHubClient) -> List[dict]:
        # Base prompt
        base_prompt = f"""
You are the Data Governance Assistant for {self.organization_name}.

Your mission is to help users understand and enforce data governance policies.

Key responsibilities:
- Explain data classification and sensitivity levels
- Guide users on data access requests
- Help identify data quality issues
- Assist with compliance (GDPR, CCPA, etc.)

Be authoritative on governance policies but friendly in tone.
"""
        messages = [{"text": base_prompt}]

        # Fetch organization-specific instructions from DataHub
        # (Similar to get_extra_llm_instructions in ChatSession)
        extra_instructions = self._fetch_governance_policies(client)
        if extra_instructions:
            messages.append({
                "text": f"ORGANIZATION POLICIES:\n\n{extra_instructions}"
            })

        return messages

    def _fetch_governance_policies(self, client: DataHubClient) -> Optional[str]:
        """Fetch governance policies from DataHub."""
        # Custom GraphQL query or API call
        try:
            query = """
            query getGovernancePolicies {
                governanceSettings {
                    policies { text }
                }
            }
            """
            response = client._graph.execute_graphql(query)
            return response.get("governanceSettings", {}).get("policies", {}).get("text")
        except Exception:
            return None

class DataGovernanceAgent:
    """
    Custom agent similar to ChatSession but focused on data governance.

    This demonstrates how to build a ChatSession-like interface with
    different behavior while reusing the same infrastructure.
    """

    def __init__(
        self,
        client: DataHubClient,
        organization_name: str,
        history: Optional[ChatHistory] = None,
        chat_type: ChatType = ChatType.DEFAULT,
    ):
        self.client = client
        self.organization_name = organization_name
        self.chat_type = chat_type

        # Build configuration with governance-specific settings
        config = AgentConfig(
            model_id="anthropic.claude-3-5-sonnet-20241022-v2:0",
            system_prompt_builder=CustomDataGovernancePromptBuilder(organization_name),
            public_tools_factory=static_tools([mcp]),  # Simple static tools
            internal_tools_factory=self._create_governance_tools,
            temperature=0.6,  # Slightly higher for conversational feel
            max_tool_calls=25,
        )

        # Create the underlying agent runner
        self._runner = AgentRunner(
            config=config,
            client=client,
            history=history or ChatHistory(),
        )

    def _create_governance_tools(self, runner: AgentRunner) -> List[ToolWrapper]:
        """Create governance-specific internal tools."""

        def respond_with_governance_context(
            response: str,
            policy_citations: Optional[List[str]] = None,
        ) -> dict:
            """Respond to user with governance policy citations."""
            formatted_response = response

            if policy_citations:
                formatted_response += "\n\n**Referenced Policies:**\n"
                for citation in policy_citations:
                    formatted_response += f"- {citation}\n"

            return {"text": formatted_response, "suggestions": []}

        return [
            ToolWrapper.from_function(
                fn=respond_with_governance_context,
                name="respond_to_user",
                description="Respond to user with governance policy context"
            )
        ]

    # Public API similar to ChatSession
    @property
    def history(self) -> ChatHistory:
        """Access conversation history."""
        return self._runner.history

    @property
    def session_id(self) -> str:
        """Get session identifier."""
        return self._runner.session_id

    def generate_next_message(self) -> NextMessage:
        """Generate next message (delegates to AgentRunner)."""
        return self._runner.generate_next_message()

    @contextlib.contextmanager
    def set_progress_callback(self, callback: ProgressCallback) -> Iterator[None]:
        """Set progress callback for streaming updates."""
        with self._runner.set_progress_callback(callback):
            yield

# Usage
governance_agent = DataGovernanceAgent(
    client=client,
    organization_name="Acme Corp"
)
governance_agent.history.add_message(
    HumanMessage(text="What's our policy on sharing PII data?")
)
response = governance_agent.generate_next_message()
```

## Integration with Existing Infrastructure

### Working with ChatSessionManager

`ChatSessionManager` is responsible for:

- Creating and loading chat sessions
- Managing persistence via `DataHubAiConversationClient`
- Handling progress callbacks and streaming

Custom agents can integrate with `ChatSessionManager` by:

1. **Implementing the same interface as ChatSession**: Your custom agent should have `history`, `session_id`, `generate_next_message()`, and `set_progress_callback()` to work with existing manager code.

2. **Extending ChatSessionManager for custom agents**:

```python
class GovernanceChatSessionManager(ChatSessionManager):
    """Extended manager for governance agents."""

    def create_governance_session(
        self,
        organization_name: str,
        chat_type: ChatType = ChatType.DATAHUB_UI
    ) -> DataGovernanceAgent:
        """Create a governance-focused chat session."""
        return DataGovernanceAgent(
            client=self.tools_client,
            organization_name=organization_name,
            chat_type=chat_type,
        )

    def load_governance_session(
        self,
        conversation_urn: str,
        organization_name: str,
    ) -> DataGovernanceAgent:
        """Load an existing governance chat session."""
        history, chat_type = self.conversation_manager.load_conversation_with_metadata(
            conversation_urn
        )
        return DataGovernanceAgent(
            client=self.tools_client,
            organization_name=organization_name,
            history=history,
            chat_type=chat_type,
        )
```

3. **Using the generic manager with agent factories**:

```python
class UniversalChatSessionManager(ChatSessionManager):
    """Manager that supports multiple agent types via factories."""

    def __init__(
        self,
        system_client: DataHubClient,
        tools_client: DataHubClient,
        agent_factories: Dict[str, Callable],
    ):
        super().__init__(system_client, tools_client)
        self.agent_factories = agent_factories

    def create_session(
        self,
        agent_type: str,
        chat_type: ChatType = ChatType.DATAHUB_UI,
        **agent_kwargs
    ):
        """Create a session for any registered agent type."""
        if agent_type not in self.agent_factories:
            raise ValueError(f"Unknown agent type: {agent_type}")

        factory = self.agent_factories[agent_type]
        return factory(client=self.tools_client, chat_type=chat_type, **agent_kwargs)

# Usage
manager = UniversalChatSessionManager(
    system_client=system_client,
    tools_client=tools_client,
    agent_factories={
        "default": lambda client, chat_type: ChatSession(
            tools=[mcp], client=client, chat_type=chat_type
        ),
        "governance": lambda client, chat_type, org_name: DataGovernanceAgent(
            client=client, organization_name=org_name, chat_type=chat_type
        ),
        "schema_comparison": create_schema_comparison_agent,
    }
)

# Create different agent types
default_agent = manager.create_session("default")
gov_agent = manager.create_session("governance", org_name="Acme Corp")
schema_agent = manager.create_session("schema_comparison")
```

### Working with ChatHistory

`ChatHistory` is a shared, reusable component that all agents use. It's already well-abstracted and agent-agnostic:

```python
from datahub_integrations.chat.chat_history import (
    ChatHistory,
    HumanMessage,
    AssistantMessage,
    ToolCallRequest,
    ToolResult,
)

# All agents use the same ChatHistory interface
agent.history.add_message(HumanMessage(text="Hello"))
agent.history.add_message(ToolCallRequest(...))
agent.history.add_message(ToolResult(...))

# ChatHistory is preserved across sessions
saved_history = agent.history
new_agent = DataGovernanceAgent(client=client, history=saved_history)
```

**Key Points:**

- `ChatHistory` doesn't need any changes - it's already agent-agnostic
- All message types work across all agents
- Persistence via `DataHubAiConversationClient` works transparently

### Working with DataHubAiConversationClient

`DataHubAiConversationClient` handles persistence and works with any agent that uses `ChatHistory`:

```python
from datahub_integrations.chat.datahub_ai_conversation_client import (
    DataHubAiConversationClient
)

# Persistence works identically for all agents
conversation_client = DataHubAiConversationClient(client)

# Save conversation (works for any agent type)
conversation_client.save_message_to_conversation(
    conversation_urn="urn:li:dataHubAiConversation:governance_123",
    actor_urn="urn:li:corpuser:user123",
    actor_type=DataHubAiConversationActorTypeClass.USER,
    message_type=DataHubAiConversationMessageTypeClass.TEXT,
    text="What's our PII policy?",
    timestamp=int(time.time() * 1000),
)

# Load conversation (returns ChatHistory that any agent can use)
history, chat_type = conversation_client.load_conversation_with_metadata(
    "urn:li:dataHubAiConversation:governance_123"
)

# Use loaded history with any agent
governance_agent = DataGovernanceAgent(
    client=client,
    organization_name="Acme Corp",
    history=history,  # Works seamlessly
)
```

### Complete Integration Example

Here's a complete example showing how custom agents integrate with all infrastructure:

```python
from datahub_integrations.chat.chat_session_manager import ChatSessionManager
from datahub_integrations.chat.types import ChatType

# 1. Create manager with both client types
manager = ChatSessionManager(
    system_client=system_client,  # For persistence
    tools_client=tools_client,    # For tool execution
)

# 2. Extend manager to support custom agent
class CustomAgentManager(ChatSessionManager):
    def create_governance_session(self, org_name: str) -> DataGovernanceAgent:
        return DataGovernanceAgent(
            client=self.tools_client,
            organization_name=org_name,
            chat_type=ChatType.DATAHUB_UI,
        )

    def load_governance_session(
        self, conversation_urn: str, org_name: str
    ) -> DataGovernanceAgent:
        # Load history using parent's conversation_manager
        history, chat_type = self.conversation_manager.load_conversation_with_metadata(
            conversation_urn
        )

        # Create agent with loaded history
        return DataGovernanceAgent(
            client=self.tools_client,
            organization_name=org_name,
            history=history,
            chat_type=chat_type,
        )

# 3. Use in streaming endpoint (e.g., SSE API)
custom_manager = CustomAgentManager(system_client, tools_client)

def handle_governance_chat(text: str, user_urn: str, org_name: str):
    """Example endpoint handler for governance chat."""

    # Create or load session
    conversation_urn = f"urn:li:dataHubAiConversation:gov_{uuid.uuid4()}"
    agent = custom_manager.create_governance_session(org_name)

    # Add user message
    custom_manager.add_user_message(agent, text)

    # Generate with progress streaming
    def progress_callback(updates: List[ProgressUpdate]):
        for update in updates:
            # Send SSE event
            yield {
                "type": update.message_type,
                "text": update.text,
            }

    with agent.set_progress_callback(progress_callback):
        response = agent.generate_next_message()

    # Save conversation
    custom_manager.conversation_manager.save_message_to_conversation(
        conversation_urn=conversation_urn,
        actor_urn=user_urn,
        actor_type=DataHubAiConversationActorTypeClass.USER,
        message_type=DataHubAiConversationMessageTypeClass.TEXT,
        text=text,
        timestamp=int(time.time() * 1000),
    )

    return response
```

## Key Benefits

### 1. Reusability

- Core agentic loop tested once, reused everywhere
- Tools can be shared across agents
- Context reducers work with any agent
- Progress tracking is standardized

### 2. Testability

- Infrastructure can be tested with mock components
- Agent behavior can be tested independently
- Clear boundaries enable focused unit tests

### 3. Flexibility

- Easy to create specialized agents for specific tasks
- Agent behavior configured via composition, not code changes
- Dynamic tool selection based on runtime conditions
- Multiple agents can coexist in the same system

### 4. Maintainability

- Clear separation of concerns
- Infrastructure changes don't affect agent logic
- Agent changes don't affect infrastructure
- Well-defined interfaces between components

## Migration Strategy

### Phase 1: Create Infrastructure (Current)

- Implement `AgentConfig`, `AgentRunner`, `SystemPromptBuilder`
- Extract reusable components
- Create examples and documentation

### Phase 2: Refactor ChatSession

- Update `ChatSession` to use `AgentRunner` internally
- Maintain backward compatibility
- Verify existing callers work unchanged

### Phase 3: Build New Agents (Future)

- Create specialized agents as needed
- Extend `ChatSessionManager` for new agent types
- Gradually migrate features to agent-specific implementations

## Best Practices

### Creating New Agents

1. **Start with a factory function** that returns `AgentRunner` for simple cases
2. **Create a wrapper class** (like `DataGovernanceAgent`) when you need additional methods or state
3. **Implement ChatSession-like interface** if you want to work with existing managers
4. **Use descriptive prompts** that clearly define the agent's role and capabilities
5. **Reuse existing tools** before creating custom ones

### Tool Management

1. **Use `static_tools()` helper** for simple cases with fixed tool lists
2. **Create custom factory** for conditional tool inclusion based on runtime state
3. **Use `internal_tools_factory`** for agent-specific tools that need runner access
4. **Keep public tools focused** - only include what the agent needs for its task
5. **Document tool interactions** in system prompts

**Example:**

```python
# Simple case
public_tools_factory=static_tools([mcp])

# Complex case with conditional logic
def create_tools(client):
    tools = flatten_tools([mcp])
    if feature_enabled():
        tools.append(special_tool)
    return tools

public_tools_factory=create_tools
```

### Context Reduction

1. **Use default reducers** unless you have specific requirements
2. **Create custom reducers** for specialized agents with unique memory needs
3. **Test token limits** with realistic conversations

### Testing

1. **Mock DataHubClient** for unit tests
2. **Create fixture tools** for testing agent behavior
3. **Test system prompt building** independently
4. **Verify backward compatibility** when changing infrastructure

## Factory-Based Design

The agent configuration uses a **factory-based design** with explicit Protocol types for clarity and type safety.

### Why Factories Instead of Direct Tool Lists?

**Problem with direct lists:**

```python
# ❌ Can't make runtime decisions
tools=[mcp, smart_search]  # What if smart_search isn't available?
```

**Solution with factories:**

```python
# ✅ Can make runtime decisions
def create_tools(client):
    tools = flatten_tools([mcp])
    if _is_smart_search_enabled():  # Runtime check!
        tools.append(smart_search)
    return tools
```

### Two Factory Types with Clear Semantics

| Factory                | Purpose                       | Receives        | Examples                                          |
| ---------------------- | ----------------------------- | --------------- | ------------------------------------------------- |
| `PublicToolsFactory`   | What the agent **CAN DO**     | `DataHubClient` | MCP tools, smart_search, API integrations         |
| `InternalToolsFactory` | How the agent **COORDINATES** | `AgentRunner`   | respond_to_user, planning tools, state management |

### Helper for Simple Cases

For agents with static tool lists, use the `static_tools()` helper:

```python
from datahub_integrations.chat.agent import static_tools

# Simple case - no conditional logic needed
config = AgentConfig(
    model_id="...",
    system_prompt_builder=StaticPromptBuilder("..."),
    public_tools_factory=static_tools([mcp]),  # Helper converts list to factory
)
```

### Benefits of Factory Design

1. **Clearer Semantics**: "public" vs "internal" is self-explanatory
2. **Type Safety**: Protocol types with explicit documentation
3. **Runtime Decisions**: Factories can check environment, feature flags, permissions
4. **Better IDE Support**: Autocomplete and type checking work correctly
5. **Simpler Mental Model**: Two clear concepts instead of three fields

## Conclusion

This agent infrastructure enables building multiple specialized agents and subagents in DataHub by separating reusable infrastructure from agent-specific business logic. The composition-based approach provides flexibility, testability, and maintainability while preserving backward compatibility with existing code.

The factory-based design with Protocol types ensures clarity and type safety, making it easy to understand what each component does and when it's used.

For questions or suggestions, consult the implementation in:

- `src/datahub_integrations/chat/agent/` - Core infrastructure
- `src/datahub_integrations/chat/chat_session.py` - Reference implementation
- `src/datahub_integrations/chat/examples/` - Example agents
