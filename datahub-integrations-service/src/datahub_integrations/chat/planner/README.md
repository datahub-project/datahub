# Planner System

The planner generates execution plans for multi-step tasks in DataHub AI. Plans provide **guidance, not constraints** — the agent uses them as context but is not forced to follow them rigidly.

## Philosophy

### Plans as Context, Not Commands

The planner creates detailed multi-step plans that help the agent understand:

- **What** needs to be accomplished (goal and expected deliverable)
- **How** to approach the task (suggested steps, tool hints)
- **When** each step is done (natural-language acceptance criteria)
- **What** could go wrong (failure conditions, fallback strategies)

However, the agent remains autonomous. Plans are injected into the conversation context to inform decision-making, but the agent can deviate based on actual results or new information discovered during execution.

### Why Planning Helps

Without planning, agents often:

- Miss important steps (e.g., not exploring entity metadata after search)
- Take inefficient paths (e.g., searching when they should get lineage)
- Lack domain knowledge (e.g., not knowing to start metric discovery from dashboards)

Plans encode best practices discovered through real-world usage.

---

## Architecture

```
planner/
├── models.py       # Core data models (Plan, Step, PlanTemplate, etc.)
├── tools.py        # Planner LLM interaction and tool dispatch
├── templates.py    # Reusable plan templates for common patterns
└── recipes/        # XML guidance for the planner LLM
    ├── broad_data_discovery.py
    ├── classification_tier_discovery.py
    ├── lineage_impact_analysis.py
    ├── metric_data_discovery.py
    ├── pii_curated_entities.py
    └── sql_generation.py
```

---

## Recipes: Domain Knowledge for the Planner

Recipes are XML-formatted guidance that teach the planner LLM how to approach common task patterns. They encode domain expertise about DataHub's metadata model and best practices.

### Examples of Recipe Knowledge

**Metadata Exploration:**

> "When searching for something, always explore the metadata of found entities before responding. Schema fields, descriptions, and lineage often contain critical context."

**Metric Discovery:**

> "When searching for metrics or KPIs, start from dashboards and charts—they often contain metric definitions that raw tables don't have."

**Impact Analysis (Rule of 10):**

> "For lineage impact: if ≤10 items, list all explicitly. If >10, aggregate by platform/type with counts."

**Search Disambiguation:**

> "If search returns multiple results (total > 1), stop and ask the user to choose before proceeding."

Recipes are injected into the planner's system prompt to guide plan generation.

---

## Performance Optimizations

### The Latency Challenge

With Claude 4.5 Sonnet, full plan generation takes **~15-20 seconds**—most of that time is spent producing output tokens for the detailed JSON plan structure.

For simple questions like "show me the schema for USERS table," generating a 5-step plan is overkill.

### Three Plan Types

The planner chooses the optimal approach based on task complexity:

| Type          | When to Use                                                           | Token Savings |
| ------------- | --------------------------------------------------------------------- | ------------- |
| **Noop**      | Task needs 1-2 tool calls (simple search, entity lookup)              | ~80-90%       |
| **Templated** | Task matches a common pattern (definition discovery, impact analysis) | ~50-60%       |
| **Full Plan** | Complex or unique multi-step task                                     | 0% (baseline) |

#### Noop Plans

For simple tasks, the planner returns a "noop" signal instead of a full plan:

```json
{
  "metadata": { "noop": true, "reason": "Simple search task" }
}
```

The agent executes directly without multi-step planning overhead.

**Examples:**

- "Show me the schema for STG_USERS"
- "Who owns the orders table?"
- "What dashboards exist in Looker?"

#### Templated Plans

For common patterns, the planner selects a pre-defined template and fills in only the parameters:

```json
{
  "template_id": "template-impact-analysis",
  "step_overrides": [
    { "step_id": "s0", "param_hints": { "query": "orders table" } }
  ]
}
```

The full step structure comes from the template, reducing LLM output.

**Available Templates** (based on analysis of 321 customer queries):

| Template               | Use Case                                     | Coverage |
| ---------------------- | -------------------------------------------- | -------- |
| `definition-discovery` | "What is the definition of MAU?"             | ~12%     |
| `data-location`        | "Where can I find churn rate data?"          | ~5%      |
| `join-discovery`       | "How do I join ACCOUNTS with ORGANIZATIONS?" | ~2%      |
| `impact-analysis`      | "What breaks if I delete users table?"       | ~1%      |
| `search-then-examine`  | "Find PII datasets and show their owners"    | ~5%      |

---

## Validation

The planner's tool selection is validated against categorized test queries:

```
experiments/chatbot/validate_planner_tool_selection.py
```

Example output:

```
NOOP: 8/10 passed (80%)
TEMPLATED (impact-analysis): 6/8 passed (75%)
TEMPLATED (definition-discovery): 10/12 passed (83%)
EXECUTION: 14/15 passed (93%)
Overall: 38/45 passed (84.4%)
```

---

## Future Improvements

### 1. Model Recommendation in Plans

Plans could recommend faster models for execution:

```json
{
  "plan_id": "plan_abc123",
  "recommended_model": "claude-3-haiku",
  "reasoning": "Simple search-and-present task doesn't need Sonnet"
}
```

This would allow:

- **Haiku** for simple tasks (~3x faster, ~10x cheaper)
- **Sonnet** for complex reasoning
- **Opus** for critical/high-stakes decisions

### 2. Dynamic Tool Exposure

Plans could specify which tools the agent needs:

```json
{
  "tools_required": ["search", "get_entities"],
  "tools_excluded": ["generate_sql", "get_lineage"]
}
```

Benefits:

- **Smaller context window** — agent sees fewer tool definitions
- **Faster inference** — less context to process
- **Better focus** — agent less likely to use wrong tools

Currently, the agent sees all ~15 tools regardless of task. For a simple search, it doesn't need `generate_sql` or `get_lineage_paths_between`.

### 3. Recipe Learning from Feedback

Future enhancement: learn new recipes from agent execution traces that deviate from plans successfully. If the agent consistently ignores a plan step and gets better results, that's signal for recipe improvement.

---

## Key Files

### `models.py`

Core data structures:

- `Plan` — The execution plan with steps, constraints, metadata
- `Step` — Individual step with intent, tool hints, acceptance criteria
- `PlanTemplate` — Reusable step structure for common patterns
- `OnFail` — Failure handling strategy (retry, revise, abort)

### `tools.py`

Planner LLM interaction:

- `create_plan()` — Main entry point, generates a plan for a task
- `revise_plan()` — Updates an existing plan based on execution results
- `_call_planner_llm()` — Low-level LLM call with tool dispatch
- Internal tools: `create_noop_plan`, `create_templated_plan`, `create_execution_plan`

### `templates.py`

Template registry:

- `PLAN_TEMPLATES` — Dictionary of available templates
- `get_template()` — Lookup by template ID
- `get_template_descriptions()` — Formatted string for planner prompt

### `recipes/`

XML guidance files that encode domain expertise:

- Each recipe has `<applicability>` (when to use) and `<guidance>` (how to execute)
- Combined into planner system prompt via `get_recipe_guidance()`

---

## How It Works

1. **Agent receives user query** → calls `create_plan(task=query)`

2. **Planner LLM evaluates task** and chooses:

   - `create_noop_plan` → Simple task, skip planning
   - `create_templated_plan` → Common pattern, use template
   - `create_execution_plan` → Complex task, generate full plan

3. **Plan returned to agent** → injected into conversation context

4. **Agent executes** using plan as guidance, but can deviate if needed

5. **If step fails** → agent may call `revise_plan()` to adapt

```
User Query
    │
    ▼
┌──────────────────┐
│  create_plan()   │
└──────────────────┘
    │
    ▼
┌──────────────────┐
│   Planner LLM    │ ← Recipes (domain knowledge)
│   evaluates      │ ← Templates (common patterns)
└──────────────────┘
    │
    ├─── Simple? ──────► Noop Plan (fast)
    │
    ├─── Common? ──────► Templated Plan (medium)
    │
    └─── Complex? ─────► Full Plan (slow)
    │
    ▼
┌──────────────────┐
│  Agent Executes  │ ← Plan as context, not constraint
└──────────────────┘
```
