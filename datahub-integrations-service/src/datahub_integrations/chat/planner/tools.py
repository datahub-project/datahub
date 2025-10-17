"""
MCP planning tools for ChatSession.

These tools are registered with the FastMCP framework and can be called
by the ChatSession's LLM or explicitly by code.
"""

import json
import uuid
from datetime import datetime
from typing import TYPE_CHECKING, Annotated, Any, Dict, List, Literal, Optional

from loguru import logger
from pydantic import Field

from datahub_integrations.chat.chat_session import get_extra_llm_instructions
from datahub_integrations.chat.planner.models import Constraints, OnFail, Plan, Step
from datahub_integrations.chat.planner.recipes import get_recipe_guidance
from datahub_integrations.gen_ai.bedrock import BedrockModel, get_bedrock_client
from datahub_integrations.mcp.mcp_server import get_datahub_client
from datahub_integrations.mcp.tool import ToolWrapper

if TYPE_CHECKING:
    from datahub_integrations.chat.chat_session import ChatSession

# Planner configuration
PLANNER_MODEL = BedrockModel.CLAUDE_37_SONNET
PLANNER_TEMPERATURE = 0.3  # Low temperature for consistent structured output


def get_plan_by_id(plan_id: str, session: "ChatSession") -> Optional[Plan]:
    """
    Retrieve a plan from the session's cache by its ID.

    Args:
        plan_id: The unique identifier of the plan
        session: The ChatSession instance containing the plan cache

    Returns:
        The Plan object if found, None otherwise
    """
    plan_data = session.plan_cache.get(plan_id)
    if plan_data:
        return plan_data.get("plan")
    return None


_PLANNER_SYSTEM_PROMPT = """\
You are a planning assistant for DataHub AI. Your job is to create structured execution plans for complex multi-step tasks.

Your plans should:
- Break down tasks into 3-7 sequential steps (prefer fewer when possible)
- Each step should be an OBJECTIVE, not a single tool call
- Each step must have a clear "done_when" criteria in natural language
- Steps are executed sequentially in array order (s0, s1, s2...)
- Select the most appropriate tools from the available tool list

For each step, provide:
- id: Sequential identifier (s0, s1, s2...)
- description: What this step accomplishes
- done_when: Natural language criteria for completion (e.g., "Search returned exactly 1 result")
- return_to_user_when: When user input needed (e.g., "Search returned more than 1 result - user must choose")
- tool: Specific tool name if critical, or leave empty for flexibility
- param_hints: Optional guidance (e.g., {"prefer_env": ["prod"], "max_results": 10})
- on_fail: {"action": "abort"} if user input needed, {"action": "retry"} if can retry

RECIPES:
You will receive recipe guidance in a separate message. When a task matches a recipe's applicability,
use the recipe's keywords and guidance to create comprehensive plans. The recipe keywords are especially
important for search-based tasks - use them in param_hints to ensure thorough coverage.

Return ONLY valid JSON matching this structure - no additional text or explanation."""


def _get_available_tools(session: "ChatSession") -> List[str]:
    """Get list of available tool names with proper datahub__ prefixes."""
    return [tool.name for tool in session.get_plannable_tools()]


def _get_tool_descriptions(session: "ChatSession") -> Dict[str, str]:
    """Get tool names and their descriptions with proper datahub__ prefixes."""
    return {
        tool.name: tool._tool.description or ""
        for tool in session.get_plannable_tools()
    }


def _call_planner_llm(prompt: str) -> str:
    """
    Call the planner LLM to generate plan JSON.

    Uses low temperature for consistent structured output.
    Includes custom instructions (if configured) and recipe guidance as separate system messages.
    Returns the raw text response which should be parseable JSON.
    """
    bedrock_client = get_bedrock_client()

    # Build system messages: main prompt + custom instructions (if any) + recipes
    system_messages = [
        {"text": _PLANNER_SYSTEM_PROMPT},
    ]

    # Add custom instructions if configured
    client = get_datahub_client()
    extra_instructions = get_extra_llm_instructions(client)
    if extra_instructions:
        formatted_instructions = (
            f"CUSTOMER-SPECIFIC REQUIREMENTS - You must follow these in addition to base instructions:\n\n"
            f"{extra_instructions}"
        )
        system_messages.append({"text": formatted_instructions})

    # Add recipe guidance
    system_messages.append(
        {
            "text": get_recipe_guidance()
        }  # Recipes as separate message (machine-constructible)
    )

    response = bedrock_client.converse(
        modelId=PLANNER_MODEL.value,
        system=system_messages,  # type: ignore[arg-type]
        messages=[{"role": "user", "content": [{"text": prompt}]}],
        inferenceConfig={
            "temperature": PLANNER_TEMPERATURE,
            "maxTokens": 4096,
        },
    )

    # Extract text from response
    output = response["output"]
    message = output.get("message")
    if not message:
        raise ValueError("No message in planner LLM response")

    content = message["content"]
    if not content or "text" not in content[0]:
        raise ValueError("No text in planner LLM response")

    return content[0]["text"]


def create_plan(
    session: "ChatSession",
    task: str,
    context: Optional[str] = None,
    evidence: Optional[Dict[str, Any]] = None,
    max_steps: int = 10,
) -> Plan:
    """
    Create an execution plan for complex multi-step tasks that require coordination across multiple tools and steps.

    You MUST use create_plan for tasks needing 2+ operations, especially:
    - Impact/dependency analysis (e.g., "what breaks if I delete X?", "what depends on Y?")
    - Multiple searches with different strategies
    - Search + examination of results
    - Finding data by topic/classification

    STRONGLY RECOMMENDED for tasks with:
    - Multiple search strategies needed (tags, keywords, filters)
    - Iterative refinement (search → examine → search more)
    - Data aggregation across sources
    - Lineage traversal with analysis

    DO NOT use for simple single-step tasks like:
    - Basic search queries (one search, return results)
    - Single entity lookups (get one entity)
    - Straightforward lineage queries (one entity, one hop, no analysis)

    WHAT THIS TOOL DOES:
    1. Breaks down complex tasks into sequential steps with clear "done_when" criteria
    2. Selects appropriate tool recipes based on the task type
    3. Provides structure to track progress through multi-step workflows
    4. Returns a Plan with: plan_id, steps (each with description and done_when), constraints

    TYPICAL WORKFLOW:
    1. Call create_plan(task="Find Looker dashboards affected by deprecating orders dataset")
    2. Receive Plan with plan_id and steps (s0: find dataset, s1: get lineage, s2: filter Looker, etc.)
    3. Execute each step in order, checking against the done_when criteria
    4. Optionally call report_step_progress after completing each step
    5. If a step fails or plan needs adjustment, call revise_plan

    EXAMPLES OF GOOD TASKS FOR PLANNING:
    - "What downstream Looker dashboards would be affected if we deprecate the orders dataset?"
      → Steps: find orders, get downstream lineage, filter for Looker, fetch dashboard details
    - "Find all datasets with PII and identify their owners"
      → Steps: search for PII-tagged datasets, fetch ownership info, group by owner
    - "Compare the schema of dev.users vs prod.users"
      → Steps: find dev.users, find prod.users, get schemas, compare fields

    Args:
        task: Description of the multi-step task to create a plan for.
              Example: "Find Looker dashboards affected by deprecating orders dataset"
        context: Optional additional context about the task, current state, or constraints.
                 Example: "We previously identified 18 downstream assets across 7 platforms that would be affected"
        evidence: Optional structured data with concrete details about already-identified entities and findings.
                 This is especially important for follow-up questions where entities, URNs, and prior results are known.
                 The planner will use this to populate param_hints with specific URNs and details.
                 Examples:
                 - Follow-up about removing a column:
                   {"source_entity": {"urn": "urn:li:dataset:(urn:li:dataPlatform:kafka,account_transactions,PROD)",
                                      "name": "account_transactions", "platform": "kafka"},
                    "column_to_remove": "transaction_type",
                    "downstream_count": 18,
                    "affected_platforms": ["snowflake", "redshift", "looker", "tableau"],
                    "downstream_urns": ["urn:li:dataset:(...)", "urn:li:dashboard:(...)"]}
                 - Follow-up about already-found dataset:
                   {"dataset_urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,prod.analytics.users,PROD)",
                    "schema_fields_count": 42,
                    "owner": "data-platform-team"}
        max_steps: Maximum number of steps to include in the plan. Default: 10.
                   Planner will aim for fewer steps when possible.

    Returns:
        Plan: A Pydantic model containing:
            - plan_id: Unique identifier (e.g., "plan_a3f2c891") for tracking and revision
            - version: Plan version number (1 for new plans)
            - title: Human-readable plan title
            - goal: Overall objective of the plan
            - assumptions: List of assumptions made during planning
            - constraints: Execution constraints (tool_allowlist, max_tool_calls)
            - steps: Sequential list of Step objects, each with:
                * id: Step identifier (s0, s1, s2...)
                * description: What this step accomplishes
                * done_when: Natural language acceptance criteria
                * intent/tool: Capability needed or specific tool to use
                * param_hints: Optional guidance for parameters
            - expected_deliverable: What should be produced at the end

    Implementation Details:
        - Introspects MCP tool registry to get available tools
        - Matches task keywords against recipe templates
        - Uses low-temperature LLM (0.2-0.4) to generate structured plan
        - Validates plan structure (tool allowlist, budgets, sequential steps)
        - Stores plan in session-scoped cache (keyed by plan_id)
        - Cache persists for ChatSession lifetime only

    After receiving the plan, execute steps sequentially and check if each step's done_when criteria is satisfied.
    """
    # Generate unique plan ID
    plan_id = f"plan_{uuid.uuid4().hex[:8]}"

    # Get available tools from session (correct prefixed names)
    tool_allowlist = _get_available_tools(session)
    tool_descriptions = _get_tool_descriptions(session)

    logger.info(
        f"Creating plan for task: {task[:100]}... with {len(tool_allowlist)} available tools"
    )

    # Build prompt for planner LLM
    # Include FULL tool descriptions so planner understands complete capabilities (especially search OR syntax)
    tools_summary = "\n\n".join(
        [
            f"Tool: {name}\n{desc}"
            for name, desc in tool_descriptions.items()
            if name
            not in [
                "create_plan",
                "revise_plan",
                "report_step_progress",
            ]  # Exclude planning tools
        ]
    )

    # Format evidence if provided
    evidence_str = ""
    if evidence:
        evidence_str = f"\n\nEvidence (concrete details from prior findings):\n{json.dumps(evidence, indent=2)}"

    prompt = f"""Create an execution plan for this task:

Task: {task}
{f"Context: {context}" if context else ""}{evidence_str}

Available tools ({len(tool_allowlist)} total):
{tools_summary}

Requirements:
- Maximum {max_steps} steps
- Each step must have a "done_when" criteria
- If evidence contains URNs or specific entity details, use them in param_hints to guide execution

Return a JSON object with this structure:
{{
  "title": "Short plan title",
  "goal": "Overall objective", 
  "assumptions": ["assumption1", "assumption2"],
  "steps": [
    {{
      "id": "s0",
      "description": "What this step accomplishes",
      "done_when": "Natural language success criteria",
      "return_to_user_when": "When user input needed (optional)",
      "tool": "tool_name (optional)",
      "param_hints": {{"hint_key": "hint_value"}},
      "on_fail": {{"action": "abort"}} (optional, use 'abort' if user input needed)
    }}
  ],
  "expected_deliverable": "What should be delivered to user"
}}"""

    # Call planner LLM
    try:
        response_text = _call_planner_llm(prompt)

        # Parse JSON response
        # Try to extract JSON if wrapped in markdown code blocks
        if "```json" in response_text:
            json_start = response_text.find("```json") + 7
            json_end = response_text.find("```", json_start)
            response_text = response_text[json_start:json_end].strip()
        elif "```" in response_text:
            json_start = response_text.find("```") + 3
            json_end = response_text.find("```", json_start)
            response_text = response_text[json_start:json_end].strip()
        else:
            # Try to find JSON object boundaries if not in code blocks
            # Look for the first { and last }
            first_brace = response_text.find("{")
            last_brace = response_text.rfind("}")
            if first_brace != -1 and last_brace != -1 and last_brace > first_brace:
                response_text = response_text[first_brace : last_brace + 1].strip()

        try:
            plan_data = json.loads(response_text)
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON plan. Error: {e}")
            logger.error(f"Response text (first 500 chars): {response_text[:500]}")
            logger.error(f"Response text (last 500 chars): {response_text[-500:]}")
            raise ValueError(
                f"Invalid JSON in plan response: {e}. Check that the response contains only valid JSON without trailing text."
            ) from e

        # Build Plan object from LLM response
        plan = Plan(
            plan_id=plan_id,
            version=1,
            title=plan_data.get("title", f"Plan for: {task[:50]}"),
            goal=plan_data.get("goal", task),
            assumptions=plan_data.get("assumptions", []),
            constraints=Constraints(
                tool_allowlist=tool_allowlist,
                max_tool_calls=plan_data.get("max_tool_calls", max_steps * 3),
            ),
            steps=[
                Step(
                    id=step_data["id"],
                    description=step_data["description"],
                    done_when=step_data.get("done_when"),
                    failed_when=step_data.get("failed_when"),
                    return_to_user_when=step_data.get("return_to_user_when"),
                    tool=step_data.get("tool"),
                    param_hints=step_data.get("param_hints"),
                    on_fail=OnFail(**step_data["on_fail"])
                    if step_data.get("on_fail")
                    else None,
                )
                for step_data in plan_data.get("steps", [])
            ],
            expected_deliverable=plan_data.get("expected_deliverable", ""),
            metadata=plan_data.get("metadata", {}),
        )

    except Exception as e:
        logger.error(f"Failed to create plan: {e}")
        raise

    # Store in session cache
    session.plan_cache[plan_id] = {
        "plan": plan,
        "progress": {},  # Will store {step_id: {status, evidence, confidence, timestamp}}
    }

    logger.info(
        f"Created plan {plan_id} (v{plan.version}) with {len(plan.steps)} steps"
    )
    return plan


def revise_plan(
    session: "ChatSession",
    plan_id: str,
    completed_steps: List[str],
    current_step: str,
    issue: str,
    evidence: Optional[Dict] = None,
) -> Plan:
    """
    Revise an existing plan when execution encounters issues or needs adjustment.

    WHEN TO USE THIS TOOL:
    Call revise_plan when:
    - A step's approach isn't working (e.g., search found no results, need different strategy)
    - New information suggests a better path forward
    - A step failed and you need to adjust the remaining steps
    - The original plan's assumptions were incorrect

    DO NOT use if:
    - Step status='returned_to_user' (USER INPUT NEEDED - respond to user, don't revise!)
    - Step has on_fail.action='abort' (means user must clarify, not a planning issue)
    - User input needed (disambiguation, clarification) - call respond_to_user instead
    - Just one tool call failed (try alternative approach first)
    - Task is nearly complete (just finish remaining steps)

    CRITICAL: If step_status='returned_to_user', do NOT call revise_plan!
    Returned_to_user means user clarification needed. Call respond_to_user instead.

    WHAT THIS TOOL DOES:
    1. Retrieves the original plan from cache using plan_id
    2. Keeps completed steps intact
    3. Regenerates steps from current_step onward based on the issue
    4. Increments version number
    5. Returns updated Plan with same plan_id

    TYPICAL SCENARIOS:
    - Issue: "No datasets found for 'orders'"
      → Revised plan might suggest: expand search terms, check different platforms, search by tag
    - Issue: "Lineage too shallow, found no BI assets"
      → Revised plan might increase max_hops, add intermediate data marts to search
    - Issue: "Dataset is ambiguous, found 5 candidates"
      → Revised plan might add filtering steps, check environment/usage to narrow down

    EXAMPLE USAGE:
    ```
    revise_plan(
        plan_id="plan_abc123",
        completed_steps=["s0", "s1"],  # Already found dataset and got lineage
        current_step="s2",              # Currently on "Filter for Looker assets"
        issue="No Looker assets found in immediate downstream, only Snowflake tables",
        evidence={"downstream_count": 8, "platforms": ["snowflake"], "max_hops": 2}
    )
    ```
    Returns: Updated Plan with s0, s1 unchanged but new s2+ steps suggesting deeper traversal or different filtering.

    Args:
        plan_id: The plan_id returned from create_plan (e.g., "plan_abc123").
                 Must exist in session cache or this will raise an error.
        completed_steps: List of step IDs that were successfully completed.
                        Example: ["s0", "s1"] means steps s0 and s1 are done.
                        These steps will be preserved in the revised plan.
        current_step: The step ID where the issue occurred or revision is needed.
                     Example: "s2" means revise from step s2 onward.
        issue: Clear description of why replanning is needed.
               Examples:
               - "Search found no results for 'orders', need broader strategy"
               - "Lineage depth of 2 insufficient, no BI assets found"
               - "Found 10 datasets, need disambiguation step"
        evidence: Optional dict with execution details that inform replanning.
                 Examples:
                 - {"search_results": 0, "query": "orders", "platform": "snowflake"}
                 - {"downstream_count": 5, "max_hops": 2, "platforms": ["snowflake"]}
                 - {"candidates": 10, "filter_used": {"env": ["PROD"]}}

    Returns:
        Plan: Updated plan with:
            - Same plan_id (for continuity)
            - Incremented version (e.g., 1 → 2)
            - Preserved completed steps (s0, s1...)
            - New/adjusted steps from current_step onward
            - Updated assumptions if needed

    Raises:
        ValueError: If plan_id not found in session cache (plan expired or invalid ID)

    Implementation Details:
        - Retrieves original plan from session-scoped cache
        - Uses planner LLM to generate revised steps based on issue and evidence
        - Only regenerates steps from current_step onward (completed steps preserved)
        - The revised plan may have more, fewer, or different remaining steps
    """
    # Retrieve original plan from session cache
    if plan_id not in session.plan_cache:
        raise ValueError(f"Plan {plan_id} not found in session cache")

    cached_entry = session.plan_cache[plan_id]
    original_plan = cached_entry["plan"]

    logger.info(
        f"Revising plan {plan_id} (v{original_plan.version}): "
        f"completed={completed_steps}, current={current_step}, issue={issue[:100]}"
    )

    # Get tool descriptions for replanning (correct prefixed names)
    # Include FULL descriptions (same as create_plan)
    tool_descriptions = _get_tool_descriptions(session)
    tools_summary = "\n\n".join(
        [
            f"Tool: {name}\n{desc}"
            for name, desc in tool_descriptions.items()
            if name not in ["create_plan", "revise_plan", "report_step_progress"]
        ]
    )

    # Build prompt for replanning
    completed_steps_desc = "\n".join(
        [
            f"- {step.id}: {step.description} (COMPLETED)"
            for step in original_plan.steps
            if step.id in completed_steps
        ]
    )

    evidence_str = json.dumps(evidence, indent=2) if evidence else "None"

    prompt = f"""Revise this execution plan due to an issue:

Original Plan:
- Title: {original_plan.title}
- Goal: {original_plan.goal}

Completed Steps:
{completed_steps_desc or "None yet"}

Current Step Having Issues:
- Step ID: {current_step}
- Issue: {issue}
- Evidence: {evidence_str}

Available tools:
{tools_summary}

Please generate REVISED steps from {current_step} onward (keep completed steps as-is).
Address the issue and adjust the approach as needed.

Return JSON with:
{{
  "assumptions": ["updated assumptions"],
  "steps": [
    {{
      "id": "{current_step}",
      "description": "Revised approach",
      "done_when": "Success criteria",
      "tool": "tool_name (optional)",
      "param_hints": {{}}
    }}
  ],
  "expected_deliverable": "What should be delivered"
}}"""

    # Call planner LLM
    try:
        response_text = _call_planner_llm(prompt)

        # Parse JSON response (handle markdown code blocks)
        if "```json" in response_text:
            json_start = response_text.find("```json") + 7
            json_end = response_text.find("```", json_start)
            response_text = response_text[json_start:json_end].strip()
        elif "```" in response_text:
            json_start = response_text.find("```") + 3
            json_end = response_text.find("```", json_start)
            response_text = response_text[json_start:json_end].strip()

        revision_data = json.loads(response_text)

        # Build revised plan with completed steps + new steps
        completed_step_objs = [
            step for step in original_plan.steps if step.id in completed_steps
        ]
        new_steps = [
            Step(
                id=step_data["id"],
                description=step_data["description"],
                done_when=step_data.get("done_when"),
                failed_when=step_data.get("failed_when"),
                return_to_user_when=step_data.get("return_to_user_when"),
                tool=step_data.get("tool"),
                param_hints=step_data.get("param_hints"),
                on_fail=OnFail(**step_data["on_fail"])
                if step_data.get("on_fail")
                else None,
            )
            for step_data in revision_data.get("steps", [])
        ]

        revised_plan = Plan(
            plan_id=plan_id,  # Same ID
            version=original_plan.version + 1,  # Increment version
            title=original_plan.title,
            goal=original_plan.goal,
            assumptions=revision_data.get("assumptions", original_plan.assumptions),
            constraints=original_plan.constraints,
            steps=completed_step_objs + new_steps,  # Preserve completed + new
            expected_deliverable=revision_data.get(
                "expected_deliverable", original_plan.expected_deliverable
            ),
        )

    except Exception as e:
        logger.error(f"Failed to revise plan {plan_id}: {e}")
        raise

    # Update session cache
    session.plan_cache[plan_id]["plan"] = revised_plan

    logger.info(
        f"Revised plan {plan_id} to version {revised_plan.version} with {len(revised_plan.steps)} steps"
    )
    return revised_plan


def report_step_progress(
    session: "ChatSession",
    plan_id: str,
    step_id: str,
    status: Annotated[
        str,
        Field(
            description="Current status of this step. Common values: 'started', 'in_progress', 'completed', 'failed', 'returned_to_user'",
            json_schema_extra={
                "enum": [
                    "started",
                    "in_progress",
                    "completed",
                    "failed",
                    "returned_to_user",
                ]
            },
        ),
    ],
    done_criteria_met: Optional[bool] = None,
    failed_criteria_met: Optional[bool] = None,
    return_to_user_criteria_met: Optional[bool] = None,
    evidence: Optional[Dict] = None,
    confidence: Optional[float] = None,
) -> str:
    """
    Report progress on a plan step. Use this to track which steps have been completed and get guidance on what to do next.

    WHEN TO USE THIS TOOL:
    Call report_step_progress after completing or failing a step to:
    - Record that a step is done with evidence of what was accomplished
    - Get confirmation of progress (e.g., "1/5 complete")
    - Receive suggestions for next steps or warnings about issues
    - Track confidence in step completion

    OPTIONAL: You may choose to execute steps without explicitly reporting progress if you prefer.

    WHAT THIS TOOL DOES:
    1. Records the step's status (started, completed, failed) in the plan cache
    2. Stores evidence (URNs, counts, results) for later review
    3. Calculates overall plan progress
    4. Returns a message with progress update and optional suggestions

    EXAMPLE USAGE:
    ```
    # Success - found exactly 1 (done_when satisfied):
    report_step_progress(
        plan_id="plan_abc123",
        step_id="s0",  # done_when="Found exactly 1 matching entity"
        status="completed",
        done_criteria_met=True,  # Found 1 entity ✓
        failed_criteria_met=False,  # Did not find >1 ✓
        evidence={"total_matches": 1, "urn": "urn:li:dataset:(...)"},
        confidence=1.0
    )
    Returns: "Step s0 completed. Next: s1 (Get downstream lineage). 1/5 complete."

    # Returned to user - found 25, need user input (on_fail='abort'):
    report_step_progress(
        plan_id="plan_abc123",
        step_id="s0",  # return_to_user_when="Search returned more than 1 result", on_fail={'action': 'abort'}
        status="returned_to_user",  # USER INPUT NEEDED
        done_criteria_met=False,  # Search returned 25, not 1 ✗
        failed_criteria_met=True,  # More than 1 found ✗
        return_to_user_criteria_met=True,  # on_fail='abort' AND criteria not met ✗
        evidence={"total_matches": 25, "matches": [...]},
        confidence=1.0
    )
    Returns: "WARNING: Step s0 returned to user (user input needed). 0/5 complete, 1 returned_to_user."
    → Then: Call respond_to_user listing matches, ask user to pick, END plan
    → Do NOT call revise_plan!

    # Failed - technical issue, can retry (on_fail='retry'):
    report_step_progress(
        plan_id="plan_abc123",
        step_id="s2",  # done_when="Retrieved schema", on_fail={'action': 'retry'}
        status="failed",
        done_criteria_met=False,
        failed_criteria_met=True,
        return_to_user_criteria_met=False,  # on_fail is 'retry', not 'abort'
        evidence={"error": "Timeout"},
        confidence=1.0
    )
    Returns: "WARNING: Step s2 failed. 1/5 complete, 1 failed."
    → Then: Can call revise_plan or retry
    ```

    EVIDENCE EXAMPLES:
    - Search step: {"urns": [...], "selected_urn": "...", "results_count": 5}
    - Lineage step: {"downstream_count": 12, "bi_assets_found": 3, "max_depth": 2}
    - Filter step: {"total_items": 12, "filtered_count": 3, "filter_criteria": {...}}

    Args:
        plan_id: The plan_id from create_plan.
                 Must exist in session cache.
        step_id: ID of the step being reported.
                 Must match a step in the plan's steps array.
        status: Current status of this step:
                - "started": Just beginning work on this step
                - "in_progress": Actively working, may report again with updates
                - "completed": Step's done_when satisfied (done_criteria_met=true)
                - "failed": Technical failure, can retry/revise (failed_criteria_met=true but NOT return_to_user_criteria_met)
                - "returned_to_user": USER INPUT NEEDED, plan must stop (return_to_user_criteria_met=true)
        done_criteria_met: Did you satisfy done_when? true/false
                          Example: done_when="Search returned exactly 1 result", search total=1 → true, total=9 → false
        failed_criteria_met: Did you meet failed_when? true/false
                            Example: failed_when="Found 0 results", search total=0 → true, total=9 → false
        return_to_user_criteria_met: Does this require user input? true/false
                             Example: If step has on_fail.action='abort' AND (failed_criteria_met=true OR done_criteria_met=false) → true
                             When true: Call respond_to_user, do NOT call revise_plan, do NOT continue plan
        evidence: Concrete data proving what was accomplished. Should include:
                 - URNs of entities found/processed
                 - Counts of results/items
                 - Key decisions made (which entity selected, why)
                 - Tool results summary
        confidence: Optional confidence score (0.0-1.0) indicating how certain you are
                   that the step's done_when criteria was satisfied.
                   Example: 0.9 = high confidence, 0.5 = uncertain, 0.2 = likely incomplete

    Returns:
        str: A message with progress update and optional guidance. Examples:
            - "Step recorded. 1/5 complete."
            - "Step s0 completed. Next: s1 (Get downstream lineage). 1/5 complete."
            - "Step s2 in progress. 1/5 complete, 1 in progress."
            - "WARNING: Step s2 failed. Consider calling revise_plan. 2/5 complete, 1 failed."
            - "Plan complete! All 5 steps finished."

    Raises:
        ValueError: If plan_id not found in session cache
        ValueError: If step_id doesn't exist in the plan

    Implementation Details:
        - This tool can be called by the LLM or explicitly by code
        - Evidence is stored in full in the cache but not returned in response
        - Response message is kept concise to avoid bloating context
        - Multiple reports for the same step are allowed (updates status)
        - Useful for tracking but not required - plans can execute without progress reporting
    """
    # Retrieve plan from session cache
    if plan_id not in session.plan_cache:
        raise ValueError(f"Plan {plan_id} not found in session cache")

    cached_entry = session.plan_cache[plan_id]
    plan = cached_entry["plan"]
    progress = cached_entry["progress"]

    # Validate step_id exists in plan
    step_ids = [step.id for step in plan.steps]
    if step_id not in step_ids:
        raise ValueError(
            f"Step {step_id} not found in plan {plan_id}. Valid steps: {step_ids}"
        )

    # Store step report
    progress[step_id] = {
        "status": status,
        "evidence": evidence or {},
        "confidence": confidence,
        "timestamp": datetime.utcnow().isoformat(),
    }

    # Calculate progress
    completed_count = sum(1 for s in progress.values() if s["status"] == "completed")
    failed_count = sum(1 for s in progress.values() if s["status"] == "failed")
    total_steps = len(plan.steps)

    # Determine next step
    current_step_idx = step_ids.index(step_id)
    next_step_id = (
        step_ids[current_step_idx + 1] if current_step_idx + 1 < total_steps else None
    )

    # Generate response message
    if status == "completed" and next_step_id:
        next_step_desc = plan.steps[current_step_idx + 1].description
        message = f"Step {step_id} completed. Next: {next_step_id} ({next_step_desc}). {completed_count}/{total_steps} complete."
    elif status == "failed":
        message = f"WARNING: Step {step_id} failed. Consider calling revise_plan. {completed_count}/{total_steps} complete, {failed_count} failed."
    elif completed_count == total_steps:
        message = f"Plan complete! All {total_steps} steps finished."
    else:
        message = f"Step recorded. {completed_count}/{total_steps} complete."

    logger.info(
        f"Progress on {plan_id}: {step_id}={status}, {completed_count}/{total_steps} complete"
    )
    return message


def get_planning_tool_wrappers(session: "ChatSession") -> list[ToolWrapper]:
    """
    Get planning tools as ToolWrappers for internal ChatSession use only.

    These tools are NOT registered on the customer-facing MCP server.
    They are automatically added to ChatSession in chat_session.py.

    Args:
        session: The ChatSession instance to bind to planning tools

    Returns:
        List of ToolWrapper objects for create_plan, revise_plan, and report_step_progress
    """
    # Create wrapper functions that bind the session parameter
    # We can't use functools.partial because FastMCP's tool introspection
    # (using Pydantic) can't handle partial objects properly

    def _create_plan_wrapper(
        task: str,
        context: Optional[str] = None,
        evidence: Optional[Dict[str, Any]] = None,
        max_steps: int = 10,
    ) -> Plan:
        return create_plan(session, task, context, evidence, max_steps)

    def _revise_plan_wrapper(
        plan_id: str,
        completed_steps: List[str],
        current_step: str,
        issue: str,
        evidence: Optional[Dict] = None,
    ) -> Plan:
        return revise_plan(
            session, plan_id, completed_steps, current_step, issue, evidence
        )

    def _report_step_progress_wrapper(
        plan_id: str,
        step_id: str,
        status: Literal[
            "started", "in_progress", "completed", "failed", "returned_to_user"
        ],
        done_criteria_met: Optional[bool] = None,
        failed_criteria_met: Optional[bool] = None,
        return_to_user_criteria_met: Optional[bool] = None,
        evidence: Optional[Dict] = None,
        confidence: Optional[float] = None,
    ) -> str:
        return report_step_progress(
            session,
            plan_id,
            step_id,
            status,
            done_criteria_met,
            failed_criteria_met,
            return_to_user_criteria_met,
            evidence,
            confidence,
        )

    # Copy docstrings from original functions
    _create_plan_wrapper.__doc__ = create_plan.__doc__
    _revise_plan_wrapper.__doc__ = revise_plan.__doc__
    _report_step_progress_wrapper.__doc__ = report_step_progress.__doc__

    return [
        ToolWrapper.from_function(
            fn=_create_plan_wrapper,
            name="create_plan",
            description=create_plan.__doc__ or "Create an execution plan",
        ),
        ToolWrapper.from_function(
            fn=_revise_plan_wrapper,
            name="revise_plan",
            description=revise_plan.__doc__ or "Revise an execution plan",
        ),
        ToolWrapper.from_function(
            fn=_report_step_progress_wrapper,
            name="report_step_progress",
            description=report_step_progress.__doc__ or "Report plan step progress",
        ),
    ]
