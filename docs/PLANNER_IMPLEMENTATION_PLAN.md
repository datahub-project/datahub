# DataHub Agent Planning Tool - Implementation Plan (Revised)

## Overview

Incremental implementation plan for adding planning capabilities to the DataHub chatbot based on the [Rev 7 specification](./datahub_agent_planning_tool_handbook_rev7_soft.md). We'll start by implementing the planner as an MCP tool that the existing ChatSession can use naturally, then add orchestration only if needed.

## Key Architectural Decisions

1. **Planning tools introspect MCP registry** to get available tools (not passed as input)
2. **Cache is session-scoped** - lives for ChatSession lifetime, no persistence
3. **Recipes are baked into planner prompt** - NOT passed as tool inputs; planner selects relevant ones
4. **Tools return Pydantic models** - `create_plan` and `revise_plan` return `Plan` objects (FastMCP auto-serializes)
5. **Reasoning messages coordinate execution** - extend ReasoningMessage with plan_id/step fields
6. **Hybrid progress tracking** - `report_step_progress` tool can be called by code or LLM; reasoning messages provide signals for explicit calls
7. **Budget enforcement deferred** - Phase 1 relies on existing MAX_TOOL_CALLS, future enhancement if needed
8. **Failed step handling flexible** - Let LLM decide initially, add programmatic triggers if needed
9. **Planning tool needed** - Not just prompt engineering; planner's job is recipe selection to avoid context pollution
10. **Feature flag controlled** - Environment variable `CHATBOT_PLANNING_ENABLED` (default: True) to enable/disable planning tools
11. **Internal tools only** - Planning tools registered at same level as respond_to_user, not exposed to MCP customers

## Terminology

- **ChatSession**: The existing `ChatSession` class in `datahub_integrations/chat/chat_session.py` that handles user requests
- **generate_next_message()**: The existing method that processes requests, calls tools, and generates responses
- **LLM in ChatSession**: The existing Claude model that decides which tools to call (currently via Bedrock)
- **Planning Tools**: New MCP tools (`create_plan`, `revise_plan`) that will be available alongside existing tools
- **Reasoning Messages**: Extended to include optional `plan_id`, `plan_step`, `step_status`, `plan_status` to coordinate plan execution
- **Plan Cache**: In-memory storage scoped to session lifetime (no persistence needed)
- **Recipes**: Hardcoded/configured templates baked into the planning tool's prompt; planning tool selects relevant ones based on task

## Architecture Overview (Incremental Approach)

### Phase 1: Planner as MCP Tool (Current Focus)

```
┌─────────────────────────────────────────────────────────────┐
│                      User Request                            │
└───────────────────┬─────────────────────────────────────────┘
                    │
                    ▼
┌─────────────────────────────────────────────────────────────┐
│              ChatSession.generate_next_message()             │
│  - LLM decides when to use planner tool                      │
│  - Calls create_plan → gets back Plan with steps            │
│  - Emits reasoning messages with plan_id/step info           │
│  - Code introspects reasoning → triggers step progress      │
└───────────────────┬─────────────────────────────────────────┘
                    │
                    ▼
┌─────────────────────────────────────────────────────────────┐
│                   MCP Tools                                  │
│  ┌─────────────────────────────────────────────────────┐   │
│  │ Planning Tools (NEW):                                │   │
│  │ - create_plan: Introspects MCP registry,            │   │
│  │                selects recipes, generates plan       │   │
│  │ - revise_plan: Updates plan based on progress       │   │
│  │ - report_step_progress: Track step completion       │   │
│  │   (called by LLM or explicitly by code)             │   │
│  └─────────────────────────────────────────────────────┘   │
│  ┌─────────────────────────────────────────────────────┐   │
│  │ Existing Tools:                                      │   │
│  │ - search, get_lineage, get_entity, etc.             │   │
│  └─────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
                    │
                    ▼
┌─────────────────────────────────────────────────────────────┐
│            Plan Cache (Session-scoped)                       │
│  - Stores plans by plan_id                                  │
│  - Lives for duration of ChatSession                         │
└─────────────────────────────────────────────────────────────┘
```

### Phase 2: Orchestration (If Needed)

```
Only implement if ChatSession's LLM struggles to:
- Follow plans consistently
- Track progress accurately
- Know when to revise plans
- Report step completions
```

## Phase 1: Planning Tools Implementation (Days 1-3)

### 1.1 Data Models (`datahub_integrations/chat/planner/models.py`)

```python
# Core data structures following the handbook spec
- Plan: Main plan container with constraints and steps
- Step: Individual step with intent, done_when, dependencies
- Constraints: Tool allowlist, budgets, time limits
- Binding: Tool preferences and fallbacks
- OnFail: Retry/abort strategies

# Progress tracking models
- StepReport: Internal storage of step execution details (stored in cache, not returned)
  - Stores: step_id, status, evidence dict, confidence, timestamp
  - Used for telemetry, analysis, and evaluating "done_when" criteria

# Extension to existing ReasoningMessage
- Add optional fields: plan_id, plan_step, step_status, plan_status
- Enables coordination and signals for explicit report_step_progress calls

# Progress tracking (in-memory, session-scoped)
- Store step completion status alongside Plan in cache
- Updated via report_step_progress tool or reasoning message introspection
```

**Evidence Examples:**

Evidence is concrete data proving what was accomplished in a step:

```python
# Step: "Find orders dataset"
evidence = {
    "urns": ["urn:li:dataset:(urn:li:dataPlatform:snowflake,prod.orders,PROD)"],
    "dataset_names": ["prod.orders"],
    "platform": "snowflake",
    "search_query": "orders",
    "results_count": 3,
    "selected_urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,prod.orders,PROD)",
    "selection_reason": "Matches production environment and has recent usage"
}

# Step: "Get downstream lineage"
evidence = {
    "source_urn": "urn:li:dataset:(...,prod.orders,...)",
    "downstream_count": 15,
    "bi_assets_found": 4,
    "max_depth_reached": 2,
    "entity_types": ["dataset", "chart", "dashboard"],
    "looker_assets": ["urn:li:dashboard:(looker,sales_dashboard)"]
}

# Step: "Filter for Looker assets"
evidence = {
    "total_downstream": 15,
    "looker_dashboards": 2,
    "looker_looks": 1,
    "filtered_urns": ["urn:li:dashboard:(looker,sales_dashboard)", ...],
    "filter_criteria": {"platform": "looker", "entity_type": ["dashboard", "chart"]}
}

# Step: "Fetch dashboard details"
evidence = {
    "dashboards_fetched": 2,
    "details": [
        {
            "urn": "urn:li:dashboard:(looker,sales_dashboard)",
            "title": "Sales Performance Dashboard",
            "owner": "analytics-team@company.com",
            "url": "https://looker.company.com/dashboards/123"
        }
    ],
    "batch_size": 2,
    "fetch_duration_ms": 450
}
```

**Key Points about Evidence:**

- Should be **concrete and verifiable** - URNs, counts, specific values
- Should include **selection criteria** when choices were made
- Should capture **key metrics** (counts, durations) for telemetry
- Evidence dict is **flexible** - structure varies by step type
- Evidence is used to evaluate if "done_when" criteria are met

**Key Design Decisions:**

- Use Pydantic models for validation and JSON serialization
- Support both intent-level and tool-bound steps
- Return Plan objects from tools (FastMCP auto-serializes Pydantic models to JSON)
- Extend ReasoningMessage to carry plan coordination info

**Why Pydantic Models for Tool Returns?**

- Type safety and IDE autocomplete
- Clear documentation of what the tool returns
- Automatic validation
- FastMCP handles serialization transparently
- ChatSession receives well-formed JSON matching Plan schema

### 1.2 Plan Validation (`datahub_integrations/chat/planner/validation.py`)

```python
# Validation rules from the handbook
- JSON schema compliance
- DAG validation (no cycles in dependencies)
- Tool allowlist verification
- At least one stop condition
- Budget constraints are positive
```

### 1.3 MCP Planning Tools (`datahub_integrations/chat/planner/tools.py`)

```python
@mcp.tool(description="Create an execution plan for complex multi-step tasks. "
          "Returns a structured plan with steps and 'done_when' criteria for each step.")
def create_plan(
    task: str,
    context: Optional[str] = None,
    max_steps: int = 10,
    time_budget_seconds: int = 60
) -> Plan:
    """
    Generate a plan with natural language acceptance criteria.

    The planning tool will:
    1. Introspect MCP registry to get available tool names
    2. Select relevant recipes based on task keywords
    3. Use low-temp LLM (0.2-0.4) to generate structured plan
    4. Validate plan structure (DAG, allowlist, budgets)
    5. Store in session-scoped cache
    6. Return Plan object (FastMCP auto-serializes to JSON)

    Returns:
        Plan: Pydantic model containing plan_id, steps (with done_when),
              constraints, expected_deliverable, etc.
    """
    # Get tool allowlist from MCP registry
    tool_allowlist = [tool.name for tool in mcp._tool_manager._tools.values()]

    # Select applicable recipes (baked into planner prompt)
    # Generate plan with planner LLM
    # Validate and cache
    # Return Plan object

@mcp.tool(description="Revise an existing plan based on execution progress and issues encountered")
def revise_plan(
    plan_id: str,
    completed_steps: List[str],
    current_step: str,
    issue: str,
    evidence: Optional[dict] = None
) -> Plan:
    """
    Update plan when steps fail or need adjustment.

    Retrieves original plan from session cache, generates revised steps
    from current_step onward, increments version, updates cache.

    Returns:
        Plan: Updated plan with same plan_id but incremented version
    """
    # Retrieve from session cache (error if not found)
    # Generate revised plan with planner LLM
    # Increment version
    # Update cache
    # Return Plan object

@mcp.tool(description="Report progress on a plan step. "
          "Records step status and evidence for tracking.")
def report_step_progress(
    plan_id: str,
    step_id: str,
    status: Literal["started", "in_progress", "completed", "failed"],
    evidence: Optional[dict] = None,
    confidence: Optional[float] = None
) -> str:
    """
    Report progress on a step.

    This tool can be called in two ways:
    1. Explicitly by code when introspecting reasoning messages with plan info
    2. By the LLM itself (we'll observe if this happens naturally)

    Args:
        plan_id: ID of the plan being executed
        step_id: ID of the step being reported (e.g., "s0", "s1")
        status: Status to record for this step
        evidence: Concrete data proving step completion (URNs, counts, etc.)
        confidence: Confidence in step completion (0.0-1.0)

    Returns:
        str: Acknowledgment message with progress and any warnings/suggestions.
             Examples:
             - "Step recorded. 1/5 complete."
             - "Step s0 completed. Next: s1 (Get lineage). 1/5 complete."
             - "WARNING: Step s2 failed. Consider revising plan. 1/5 complete, 1 failed."
             - "Plan complete! All 5 steps finished."
    """
    # Retrieve plan from cache (raise error if not found)
    # Store step report (step_id, status, evidence, confidence, timestamp)
    # Calculate progress: completed, failed, total

    # Generate response message:
    # - Normal case: "Step recorded. X/Y complete."
    # - With suggestion: "Step s0 completed. Next: s1 (...). X/Y complete."
    # - With warning: "WARNING: Multiple failures. Consider revising plan."
    # - When done: "Plan complete! All X steps finished."

    # Return simple string message
```

**Note on Return Value:**

- Simple string keeps it flexible and human-readable
- Can include progress counts, suggestions, or warnings as needed
- LLM can read it naturally, code can parse if needed
- No complex object needed - the Plan is already in context/cache

**Implementation Notes:**

- Register tools with FastMCP framework
- **Critical**: Introspect MCP registry to get tool_allowlist for planner
- Session-scoped cache (dict keyed by plan_id, stores Plan objects)
- Recipes are hardcoded/configured in planner's system prompt, NOT passed as input
- Planner LLM details (model, temperature) deferred to implementation
- **Return Plan objects** - FastMCP automatically serializes Pydantic models to JSON for tool responses
- ChatSession receives JSON but can understand it via Plan schema in tool description
- **report_step_progress tool** - Hybrid approach:
  - Code can call it explicitly when detecting plan coordination in reasoning messages
  - LLM can also call it (we'll observe if this happens)
  - Provides progress tracking without requiring orchestration

### 1.4 ReasoningMessage Extension (`datahub_integrations/chat/chat_history.py`)

Extend the existing `ReasoningMessage` class to support plan coordination:

```python
class ReasoningMessage(Message):
    text: str
    # NEW: Optional plan coordination fields
    plan_id: Optional[str] = None
    plan_step: Optional[str] = None  # e.g., "s0", "s1"
    step_status: Optional[Literal["started", "in_progress", "completed", "failed"]] = None
    plan_status: Optional[Literal["active", "completed", "failed", "revised"]] = None
```

**Usage Pattern:**

1. ChatSession calls `create_plan`, gets back plan with `plan_id="plan_abc123"`
2. When working on step, LLM includes in reasoning: `{plan_id: "plan_abc123", plan_step: "s0", step_status: "started"}`
3. LLM executes tools to accomplish step, gathering evidence
4. Code introspects reasoning messages to track progress
5. Code can explicitly call `report_step_progress` with evidence when detecting step completion
6. Alternatively, LLM might call `report_step_progress` itself (we'll observe if this happens)
7. When step fails, code can detect it and suggest calling `revise_plan`

**Example Flow with Evidence:**

```python
# Step s0: "Find orders dataset"
search(query="orders", filters={"types": ["dataset"]})
# Returns multiple results

# LLM reasoning with plan coordination:
reasoning = ReasoningMessage(
    text="I found 3 datasets. Selecting prod.orders because it's in production and recently used.",
    plan_id="plan_abc123",
    plan_step="s0",
    step_status="completed"
)

# Code detects completion, calls report_step_progress:
message = report_step_progress(
    plan_id="plan_abc123",
    step_id="s0",
    status="completed",
    evidence={
        "urns": ["urn:li:dataset:(urn:li:dataPlatform:snowflake,prod.orders,PROD)"],
        "selected_urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,prod.orders,PROD)",
        "selection_reason": "Production environment, recent usage"
    },
    confidence=0.9
)
# Returns: "Step s0 completed. Next: s1 (Get downstream lineage). 1/5 complete."
```

**Benefits:**

- Natural integration with existing reasoning system
- Hybrid approach: code can call `report_step_progress` or let LLM do it
- Simple string return is flexible and human-readable
- Can include warnings/suggestions when needed ("WARNING: Consider revising plan")
- LLM can read progress naturally, code can parse if needed
- No over-engineered complex return types

**Open Question: Does Failed Step = Failed Plan?**

- **Option A**: Single step failure triggers `revise_plan` automatically
- **Option B**: LLM decides whether to continue, revise, or mark plan failed
- **Proposed**: Start with Option B (let LLM decide), add programmatic triggers in Phase 2 if LLM struggles

## Phase 2: Testing Tools in Isolation (Days 3-4)

### 2.1 Unit Tests (`tests/chat/planner/test_planning_tools.py`)

**Bottom-up testing approach:**

```python
def test_plan_models_serialization():
    """Test Plan/Step Pydantic models serialize correctly"""
    plan = Plan(plan_id="test", steps=[...], constraints=...)
    json_str = plan.model_dump_json()
    loaded = Plan.model_validate_json(json_str)
    assert loaded == plan

def test_plan_validation_dag():
    """Test DAG cycle detection"""
    # Create plan with circular dependency
    # Verify validation fails

def test_create_plan_tool_allowlist():
    """Test that create_plan introspects MCP registry correctly"""
    # Mock MCP registry with known tools
    # Call create_plan
    # Verify generated plan only uses tools from registry

def test_create_plan_simple_task():
    """Test plan generation for basic task (mock planner LLM response)"""
    with mock_planner_llm_response():
        plan = create_plan(
            task="Find all datasets with PII",
            max_steps=5
        )
        assert isinstance(plan, Plan)
        assert plan.plan_id.startswith("plan_")
        assert len(plan.steps) > 0
        assert all(step.done_when for step in plan.steps)

def test_revise_plan_retrieves_from_cache():
    """Test replanning retrieves original from cache"""
    # Create initial plan
    plan = create_plan(task="test task")
    plan_id = plan.plan_id

    # Revise it
    revised = revise_plan(
        plan_id=plan_id,
        completed_steps=["s0"],
        current_step="s1",
        issue="Step failed"
    )

    # Verify version incremented, plan_id same
    assert isinstance(revised, Plan)
    assert revised.plan_id == plan_id
    assert revised.version > plan.version

def test_revise_plan_missing_plan_errors():
    """Test revise_plan fails gracefully when plan not in cache"""
    with pytest.raises(ValueError, match="Plan .* not found"):
        revise_plan(plan_id="nonexistent", ...)

def test_report_step_progress():
    """Test progress reporting returns acknowledgment message"""
    # Create plan with 3 steps
    plan = create_plan(task="test task", max_steps=3)

    # Report progress on first step with evidence
    message = report_step_progress(
        plan_id=plan.plan_id,
        step_id="s0",
        status="completed",
        evidence={
            "urns": ["urn:li:dataset:(urn:li:dataPlatform:snowflake,prod.orders,PROD)"],
            "selected_urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,prod.orders,PROD)"
        },
        confidence=0.9
    )

    # Verify result is a useful string message
    assert isinstance(message, str)
    assert "1/3" in message or "1 of 3" in message  # Shows progress
    assert "complete" in message.lower()  # Mentions completion

def test_report_step_progress_failure_warning():
    """Test progress reporting includes warnings on failures"""
    plan = create_plan(task="test task", max_steps=3)

    # Report a failed step
    message = report_step_progress(
        plan_id=plan.plan_id,
        step_id="s0",
        status="failed",
        evidence={"error": "Dataset not found"}
    )

    # Should include warning or suggestion
    assert "WARNING" in message or "warning" in message or "failed" in message.lower()

def test_report_step_progress_missing_plan():
    """Test report_step_progress fails gracefully when plan not found"""
    with pytest.raises(ValueError, match="Plan .* not found"):
        report_step_progress(plan_id="nonexistent", step_id="s0", status="started")
```

### 2.2 Integration Test with ChatSession (`tests/chat/planner/test_chat_integration.py`)

**Testing strategy progression:**

```python
def test_plan_serialization_fits_in_context():
    """Test that Plan object serializes to reasonable size for ChatSession context"""
    plan = create_plan(task="Complex multi-step task", max_steps=10)
    plan_json = plan.model_dump_json()
    # Verify it's not too large
    assert len(plan_json) < 5000, "Plan too large for context window"

def test_reasoning_message_with_plan_fields():
    """Test that ReasoningMessage accepts plan coordination fields"""
    reasoning = ReasoningMessage(
        text="Starting step 0",
        plan_id="plan_abc123",
        plan_step="s0",
        step_status="started"
    )
    assert reasoning.plan_id == "plan_abc123"

def test_chat_session_can_call_planner():
    """Test that ChatSession can call planner (observational, may fail initially)"""
    chat_session = ChatSession(tools=[mcp])  # includes planning tools

    # Add complex multi-step request
    chat_session.history.add_message(
        HumanMessage(text="Analyze the impact of deprecating the orders dataset")
    )

    # This test is observational - we want to see IF ChatSession calls planner
    response = chat_session.generate_next_message()

    # Check if planner was called (this might not happen automatically at first)
    tool_calls = [msg for msg in chat_session.history.messages if isinstance(msg, ToolCallRequest)]
    planner_called = any("create_plan" in call.name for call in tool_calls)

    # Log result for analysis (not strict assertion)
    logger.info(f"Planner called: {planner_called}")

    # Eventually verify quality
    assert response.text  # At least got some response
```

## Phase 3: Evaluate and Iterate (Days 5-7)

### 3.1 Observe ChatSession Behavior

**No code changes needed initially!** Just observe how the ChatSession's LLM:

- Decides when to call `create_plan`
- Follows the generated plan steps
- Tracks its own progress
- Knows when to call `revise_plan`

**Metrics to track:**

- Does ChatSession call planner for complex tasks?
- Does it follow plan steps in order?
- Does it satisfy the "done_when" criteria?
- Does it know when to revise?

### 3.2 Recipe Library (`datahub_integrations/chat/planner/recipes.py`)

**Key Design Point:** Recipes are **baked into the planner LLM's system prompt**, not passed as tool inputs.

**Why?** The planning tool's job is to **select** the right recipes based on the task. If ChatSession had to pass recipes, it would need to know all recipes (polluting context) and decide which apply (duplicating planner logic).

```python
# Recipes are configuration for the planner tool
RECIPES = {
    "deprecation-impact": {
        "applicability": ["impact", "deprecate", "downstream"],
        "steps": [
            {
                "intent": "search.entities",
                "done_when": "Found target dataset(s)",
            },
            {
                "intent": "lineage.downstream",
                "done_when": "Identified downstream assets",
            }
        ]
    },
    "pii-scan": {
        "applicability": ["pii", "sensitive", "privacy"],
        "steps": [...]
    },
    "data-quality-check": {
        "applicability": ["quality", "validation", "checks"],
        "steps": [...]
    },
}

# The planner tool will:
# 1. Match task keywords against recipe applicability
# 2. Include relevant recipes in planner LLM prompt
# 3. Let planner LLM adapt/combine recipes for the task
```

**Implementation Approach:**

- Start with 3-5 hardcoded recipes for common tasks
- Make configurable via environment/config file later
- Planner prompt includes: "Here are relevant recipe templates to guide your planning..."

## Phase 4: Future Orchestration (Only if Needed)

Based on Phase 3 observations, implement orchestration **only if** the ChatSession's LLM struggles with:

### 4.1 When to Build Orchestrator

**Indicators that orchestration is needed:**

- ChatSession doesn't reliably follow plan order
- Poor tracking of which steps are complete
- Doesn't know when to revise plans
- Exceeds budgets frequently
- Gets confused with parallel steps

### 4.2 Potential Orchestrator Design

If needed, implement lightweight orchestration:

```python
class PlanOrchestrator:
    def guide_execution(self, plan, chat_session):
        """Gentle guidance, not hard control"""
        # Inject plan context into prompts
        # Track step completion externally
        # Suggest next steps when stuck
        # Enforce budgets if needed
```

## Testing Strategy

### Phase 1 Tests (Planning Tools)

- Unit tests for models and validation
- Tool registration and invocation
- Plan generation quality
- JSON serialization

### Phase 2 Tests (Integration)

- ChatSession's LLM uses planner appropriately
- Plans are followed reasonably well
- Revision happens when needed
- End-to-end task completion

### Phase 3 Tests (Golden Plans)

- Canonical plans for common tasks:
  - Dataset impact analysis
  - PII detection
  - Lineage exploration
  - Schema comparison

## Phase 5: Telemetry & Monitoring

### 5.1 Metrics to Track

```python
PlannerTelemetry:
    - plan_id
    - task_type (inferred)
    - num_steps
    - total_tool_calls
    - execution_time_ms
    - success_rate
    - replan_count

StepTelemetry:
    - step_id
    - intent_or_tool
    - tool_calls_made
    - duration_ms
    - confidence_score
    - complete_status
```

### 5.2 Integration with existing telemetry

- Extend `MCPTelemetryMiddleware`
- Add plan context to tool calls
- Track planning vs execution time

## Implementation Progress Tracker

### Phase 1: Core Implementation

- [x] Create planner directory structure (`chat/planner/`)
- [x] Implement Pydantic models (`models.py`)
  - [x] Plan, Step, Constraints, Binding, OnFail models
  - [x] All fields with examples and descriptions
  - [x] Removed unnecessary fields (depends_on, input_template, stop_conditions, success_criteria, StepReport)
- [x] Implement planning tool skeletons (`tools.py`)
  - [x] create_plan with comprehensive docstring
  - [x] revise_plan with comprehensive docstring
  - [x] report_step_progress with comprehensive docstring
  - [x] Tool registration function
- [x] Implement LLM integration
  - [x] Planner system prompt
  - [x] \_call_planner_llm helper
  - [x] MCP registry introspection
  - [x] create_plan with LLM calls and JSON parsing
  - [x] revise_plan with LLM calls and step preservation
  - [x] report_step_progress with progress tracking
  - [x] Session-scoped cache
  - [x] Error handling and fallbacks
- [x] Create simple experiment script (try_planner.py) in experiments/chatbot
- [x] Run script and validate planner generates real plans with LLM
- [x] Extend ReasoningMessage with plan coordination fields
  - [x] Added plan_id, plan_step, step_status, plan_status
  - [x] All fields optional (backward compatible)
  - [x] Field descriptions added
- [x] Integrate planning tools into ChatSession (same level as respond_to_user)
  - [x] Created \_get_internal_chatbot_tools() in chat_session.py
  - [x] Planning tools automatically added to all ChatSession instances
  - [x] No changes needed to ChatSession instantiation points
  - [x] Planning tools NOT added to customer-facing MCP server (kept separate)
  - [x] Follows same pattern as respond_to_user tool
  - [x] Added CHATBOT_PLANNING_ENABLED environment variable (default: True)
  - [x] Lazy import of planning tools (only when enabled)
  - [x] Feature flag logged on startup
  - [x] Updated \_SYSTEM_PROMPT with plan coordination fields in reasoning template
  - [x] Plan fields marked as OPTIONAL with clear usage guidance
  - [x] Added planning guidance to main system prompt (MUST use create_plan for 2+ tool operations BEFORE executing)
  - [x] Made guidance concrete with specific tool combinations (search+lineage, search+examine, multiple searches)
  - [x] Included exact example: "Find jobs that access PII data" → search for datasets, then get lineage
  - [x] Clear DO NOT plan examples to avoid over-use
- [ ] Implement plan validation (optional for Phase 1)
- [x] Implement recipe library
  - [x] Created recipes.py with get_recipe_guidance()
  - [x] XML format for human readability and no quote escaping
  - [x] Hybrid approach: structured keywords + natural language guidance
  - [x] One recipe: PII/Sensitive Data Keywords (comprehensive ~50 terms)
  - [x] Integrated as separate system message to planner LLM
  - [x] Flat keyword list (~50 terms) - removed categories to ensure ALL terms are used
  - [x] Guidance emphasizes using ALL keywords for comprehensive coverage
  - [x] Updated guidance to reference "all terms" instead of "categories"
  - [x] Fixed planner prompt to include FULL tool descriptions (not truncated)
  - [x] Validated: Planner now generates correct OR syntax in queries
  - [x] Updated create_plan description to be more directive (MUST use for 3+ step tasks)
- [ ] Write unit tests for planning tools
- [ ] Write integration tests with ChatSession

### Phase 2: Testing & Integration

- [ ] Test create_plan generates valid plans
- [ ] Test revise_plan preserves completed steps
- [ ] Test report_step_progress tracking
- [ ] Test tools with ChatSession
- [ ] Observe if ChatSession naturally uses planner
- [ ] Measure plan following accuracy

### Phase 3: Evaluation

- [ ] Analyze ChatSession behavior with planning tools
- [ ] Decide if orchestration is needed
- [ ] Add recipes if beneficial
- [ ] Refine tool descriptions based on usage

### Completed So Far:

✅ Models defined with Pydantic (simplified from spec)
✅ Tools with comprehensive docstrings (MUST use directive added)
✅ LLM calls implemented (create_plan, revise_plan with full tool descriptions)
✅ Session-scoped cache working  
✅ Progress tracking functional (report_step_progress)
✅ ReasoningMessage extended with plan fields
✅ Tools automatically added to all ChatSession instances (like respond_to_user)
✅ Planning tools kept separate from customer-facing MCP
✅ Feature flag (CHATBOT_PLANNING_ENABLED, default: True)
✅ Recipe library with PII keywords (XML format, ~50 comprehensive terms)
✅ Validated with real traces: Sequential execution, correct OR syntax, efficient search

### Next Up:

🔲 Plan validation (optional)
🔲 Unit tests  
🔲 Integration testing with ChatSession
🔲 Monitor planner adoption rate (is agent using it consistently?)
🔲 Add more recipes if needed (impact analysis, schema comparison)

## Risk Mitigation

### Technical Risks

1. **LLM Non-determinism**: Mitigate with low temperature, validation, golden tests
2. **Infinite loops**: ~~Hard budgets, timeout enforcement~~ **Phase 1: Not enforced** - rely on existing ChatSession MAX_TOOL_CALLS limit. Future: programmatic enforcement via reasoning message introspection
3. **Tool compatibility**: Validate against MCP registry upfront
4. **Plan cache misses**: Handle gracefully with clear error messages when plan not found

### Design Risks

1. **Over-planning simple tasks**: Phase 1 accepts this - LLM decides when to use planner
2. **Brittle acceptance criteria**: Natural language with self-check via reasoning messages
3. **Poor replanning**: Maintain context, limit replan attempts (future: programmatic trigger)
4. **ChatSession doesn't use planner**: This is an acceptable Phase 1 outcome - validates need for prompt engineering or explicit triggers

## Success Criteria

### Functional

- [ ] Plans generated for multi-step tasks
- [ ] Steps execute with natural language acceptance
- [ ] Replanning works when steps fail
- [ ] Progress updates shown to user

### Performance

- [ ] Plan generation < 2 seconds
- [ ] Step execution respects budgets
- [ ] Total execution time comparable to non-planned

### Quality

- [ ] 90% test coverage on core components
- [ ] Golden tests pass consistently
- [ ] Telemetry captures all key events

## Example: How ChatSession Uses Planning Tools

### User Request

"What Looker dashboards would be affected if we deprecate the orders dataset?"

### Expected ChatSession Behavior (No Orchestration Needed!)

```python
# ChatSession's LLM internal reasoning (simplified):

# 1. "This is complex, I should create a plan first"
create_plan(
    task="Find Looker dashboards affected by deprecating orders dataset",
    context="Need to trace lineage from orders to BI assets",
    max_steps=7,
    time_budget_seconds=60
)
# Returns: Plan with steps and done_when criteria

# 2. "Now I'll follow the plan steps"
# Step 1: Find orders dataset
search(query="orders", filters={"types": ["dataset"]})
# Check: done_when="Found production orders dataset" ✓

# Step 2: Get downstream lineage
get_lineage(urn="...", upstream=False, max_hops=3)
# Check: done_when="Have downstream assets list" ✓

# Step 3: Filter for Looker
# (ChatSession realizes it needs to filter results...)
# Check: done_when="Identified Looker dashboards" ✓

# 3. "If something goes wrong, I can revise"
# (If no Looker assets found...)
revise_plan(
    plan_id="...",
    completed_steps=["s1", "s2"],
    current_step="s3",
    issue="No Looker assets found in immediate downstream",
    evidence={"lineage_depth": 2}
)
# Returns: Updated plan suggesting deeper traversal
```

### Test to Verify This Works

```python
async def test_chat_session_uses_planner_naturally():
    """No orchestration - just give ChatSession the planning tools"""
    chat_session = ChatSession(tools=[mcp])  # mcp includes planning tools

    # Complex request
    chat_session.history.add_message(
        HumanMessage(text="What Looker dashboards would be affected if we deprecate orders?")
    )

    # ChatSession should naturally:
    # 1. Call create_plan
    # 2. Follow the steps
    # 3. Call revise_plan if needed
    response = chat_session.generate_next_message()

    # Verify it worked
    assert "Looker" in response.text
    assert any("create_plan" in call for call in chat_session.tool_calls)
```

## Key Questions for Incremental Approach

1. **Tool Discovery**: Will ChatSession's LLM naturally discover and use planning tools?

   - **Hypothesis**: Yes, with good tool descriptions
   - **Test**: Run complex tasks and observe

2. **Plan Following**: Can ChatSession follow plans without orchestration?

   - **Hypothesis**: Mostly yes, Claude is good at following structured instructions
   - **Test**: Check step completion rate

3. **Progress Tracking**: Does ChatSession need help tracking completed steps?

   - **Hypothesis**: May struggle with long plans
   - **Test**: Measure accuracy of self-reported progress

4. **Replan Triggers**: Will ChatSession know when to revise plans?

   - **Hypothesis**: May need prompting
   - **Test**: Introduce failures and observe

5. **Tool Descriptions**: How detailed should planning tool descriptions be?
   - **Proposed**: Start verbose, refine based on usage patterns

## Next Steps (Incremental Approach)

1. **Implement planning tools as MCP tools** (Phase 1)

   - Create data models
   - Build create_plan and revise_plan tools
   - Register with FastMCP

2. **Test in existing ChatSession** (Phase 2)

   - No changes to ChatSession needed initially
   - ChatSession's LLM will naturally discover and use planning tools
   - Create test scenarios to evaluate behavior

3. **Observe and measure** (Phase 3)

   - How often does ChatSession use the planner?
   - Does it follow the plans effectively?
   - When does it struggle?

4. **Only then decide** if we need:
   - Explicit orchestration
   - Forced planner invocation
   - Additional execution infrastructure

## Appendix: File Structure

### Phase 1 (Initial Implementation)

```
datahub-integrations-service/
├── src/datahub_integrations/
│   └── chat/
│       ├── chat_history.py    # MODIFY: Extend ReasoningMessage
│       ├── chat_session.py    # Existing (no changes needed initially)
│       └── planner/
│           ├── __init__.py
│           ├── models.py      # Plan/Step/Constraints Pydantic models
│           ├── validation.py  # DAG validation, constraint checks
│           ├── recipes.py     # Recipe templates (hardcoded)
│           └── tools.py       # MCP tools: create_plan, revise_plan, report_step_progress
└── tests/
    └── chat/
        └── planner/
            ├── __init__.py
            ├── test_models.py
            ├── test_tools.py      # Unit tests for planning tools
            └── test_integration.py # Integration with ChatSession
```

### Future Files (If Orchestration Needed)

```
datahub-integrations-service/
└── src/datahub_integrations/
    └── chat/
        └── planner/
            ├── executor.py        # Step execution logic (if needed)
            ├── orchestrator.py    # Explicit orchestration (if needed)
            └── telemetry.py       # Enhanced monitoring (Phase 2+)
```

**Note:**

- Planner lives under `chat/` since it's designed specifically for ChatSession
- Start minimal. Only add files when proven necessary by Phase 3 evaluation.
- If planner becomes useful for other contexts later, can move to top-level
