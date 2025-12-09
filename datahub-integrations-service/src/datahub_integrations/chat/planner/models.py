"""
Data models for planning system.

Based on Rev 7 specification with natural-language acceptance criteria.

Plan Types:
-----------
1. Full Plan (create_execution_plan): Custom multi-step plan generated from scratch
2. Noop Plan (create_noop_plan): Signal to skip planning and execute directly
3. Templated Plan (create_templated_plan): Plan built from a reusable template

Noop Plans:
-----------
A noop plan has metadata["noop"] = True and indicates the task is simple enough
to execute directly without multi-step planning. The executor should check
plan.is_noop and handle accordingly.
"""

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

# =============================================================================
# Core Plan Models
# =============================================================================


class Binding(BaseModel):
    """Tool binding preferences and fallbacks."""

    preferred_tool: Optional[str] = None
    locked: bool = False  # Future use for write gating
    fallbacks: List[str] = Field(default_factory=list)


class OnFail(BaseModel):
    """Strategy for handling step failures."""

    action: str = Field(
        default="revise",
        description="Action to take on failure. Common values: 'revise' (trigger replanning), 'abort' (stop, user input needed), 'retry' (try again).",
        json_schema_extra={"enum": ["revise", "abort", "retry"]},
    )
    hint: Optional[str] = None
    max_retries: int = 0


class Step(BaseModel):
    """
    Individual step in a plan.

    Steps are objectives, not single tool calls. The LLM may make
    multiple tool calls to complete a single step.

    Steps are executed in array order (steps[0], steps[1], steps[2]...).
    """

    id: Optional[str] = Field(
        default=None, description="Step identifier (e.g., 's0', 's1')"
    )
    description: Optional[str] = Field(
        default=None, description="Human-readable step description"
    )

    # Step can be intent-level or tool-bound
    intent: Optional[str] = Field(default=None, description="Capability-level intent")
    tool: Optional[str] = Field(
        default=None, description="Concrete tool name (optional)"
    )

    binding: Optional[Binding] = Field(
        default=None,
        description=(
            "Tool preferences and fallbacks. "
            "Example: Binding(preferred_tool='datahub.search', fallbacks=['datahub.scroll_search'])"
        ),
    )
    param_hints: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "Advisory parameters to guide step execution. "
            "Examples: "
            "{'prefer_env': ['prod', 'production'], 'platforms': ['snowflake', 'bigquery']}, "
            "{'max_results': 10, 'include_deprecated': False}, "
            "{'search_depth': 3, 'include_columns': True}"
        ),
    )

    # Natural-language acceptance criteria
    done_when: Optional[str] = Field(
        default=None,
        description="Natural language description of what 'done' looks like for this step",
    )

    failed_when: Optional[str] = Field(
        default=None,
        description=(
            "Natural language description of technical failure conditions. "
            "Examples: 'API timeout', 'Lineage depth exceeded'"
        ),
    )

    return_to_user_when: Optional[str] = Field(
        default=None,
        description=(
            "Natural language description of when control should return to user (plan must stop). "
            "Examples: 'Search returned more than 1 result - user must choose which one'"
        ),
    )

    # Optional budgets per step
    budgets: Optional[Dict[str, int]] = Field(
        default=None, description="Per-step budgets (max_calls, max_seconds)"
    )

    on_fail: Optional[OnFail] = Field(
        default=None,
        description=(
            "Failure handling strategy for this step. "
            "Examples: "
            "OnFail(action='retry', max_retries=3) - retry up to 3 times; "
            "OnFail(action='revise', hint='Try broader search terms') - trigger replanning with hint; "
            "OnFail(action='abort') - stop execution immediately"
        ),
    )
    metadata: Dict[str, Any] = Field(
        default_factory=dict,
        description=(
            "Optional metadata for the step. "
            "Examples: {'priority': 'high', 'estimated_duration_ms': 5000, 'retries': 0}"
        ),
    )


class PlanTemplate(BaseModel):
    """
    Reusable plan template for common task patterns.

    Templates define the structure (steps with descriptions and done_when criteria)
    using regular Step objects with tool/param_hints left empty. When the LLM uses
    a template, it provides step_overrides with the tool bindings and param_hints.

    This allows ~50-60% reduction in LLM output tokens compared to full plan generation.

    Example templates:
    - simple-search: Single search with response (1-2 steps)
    - entity-details: Get entity by URN and present details
    - lineage-basic: Single-hop lineage query
    - search-then-examine: Search + get details pattern
    """

    id: str = Field(
        ..., description="Unique template identifier (e.g., 'simple-search')"
    )
    name: str = Field(..., description="Human-readable template name")
    description: str = Field(
        ...,
        description="When to use this template - helps LLM decide if template applies",
    )
    steps: List[Step] = Field(
        ...,
        description="Pre-defined step structure (tool/param_hints typically empty)",
    )
    expected_deliverable_template: str = Field(
        ...,
        description="Template for expected deliverable",
    )


class Constraints(BaseModel):
    """Global constraints for plan execution."""

    tool_allowlist: List[str] = Field(
        default_factory=list,
        description="Tools available for execution (populated by planner)",
    )
    max_tool_calls: Optional[int] = Field(
        None, description="Maximum tool calls allowed (optional)"
    )


class Plan(BaseModel):
    """
    Execution plan for a multi-step task.

    Contains intent-level steps with natural-language acceptance criteria.
    """

    plan_id: str = Field(..., description="Unique plan identifier")
    version: int = Field(1, description="Plan version (increments on revision)")
    title: str = Field(..., description="Human-readable plan title")
    goal: str = Field(..., description="Overall goal of the plan")
    assumptions: List[str] = Field(
        default_factory=list,
        description=(
            "Assumptions made when creating this plan. "
            "Examples: "
            "['Looker metadata is ingested', 'Prefer production assets over dev', "
            "'User has access to lineage data', 'Dataset name is unambiguous']"
        ),
    )
    constraints: Constraints = Field(..., description="Execution constraints")
    steps: List[Step] = Field(..., description="Steps to execute")
    expected_deliverable: str = Field(
        ...,
        description=(
            "What should be delivered at the end of plan execution. "
            "Examples: "
            "'List of Looker dashboards with titles, URLs, and owners', "
            "'Report of all datasets containing PII with confidence scores', "
            "'Comparison table showing schema differences between dev and prod'"
        ),
    )
    metadata: Dict[str, Any] = Field(
        default_factory=dict,
        description=(
            "Optional metadata for the plan. "
            "Examples: {'created_by': 'user_id', 'task_type': 'impact_analysis', "
            "'original_query': '...', 'recipe_used': 'deprecation-impact'}. "
            "For noop plans: {'noop': True, 'reason': 'Simple single-search task'}"
        ),
    )

    @property
    def is_noop(self) -> bool:
        """
        Check if this is a noop plan (task is simple, skip multi-step planning).

        Noop plans have metadata["noop"] = True. The executor should check this
        and execute the task directly without following the plan steps.

        Returns:
            True if this is a noop plan, False otherwise
        """
        return self.metadata.get("noop", False) is True

    @property
    def noop_reason(self) -> Optional[str]:
        """
        Get the reason why this was marked as a noop plan.

        Returns:
            The reason string if this is a noop plan, None otherwise
        """
        if self.is_noop:
            return self.metadata.get("reason")
        return None
