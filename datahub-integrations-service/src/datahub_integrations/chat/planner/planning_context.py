"""
PlanningContext for planning tools.

This module is separate from tools.py to avoid circular imports.
PlanningContext is created in AgentRunner and passed to planning tools.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Optional

from datahub_integrations.mcp_integration.tool import Tool

if TYPE_CHECKING:
    from datahub_integrations.chat.agent.history_snapshot import PlanCacheEntry
    from datahub_integrations.chat.agent.langgraph_agent import SnapshotHolder
    from datahub_integrations.chat.planner.models import Plan


class PlanningContext:
    """
    Context for planning tools providing plan access and plannable tools.

    Created at runner init with a reference to the SnapshotHolder (AgentGraphState).
    Planning tools use this interface instead of accessing AgentRunner directly.

    Attributes:
        _plannable_tools: Tools available for use in execution plans
        _holder: SnapshotHolder for plan state access
    """

    def __init__(
        self,
        plannable_tools: list[Tool],
        holder: SnapshotHolder,
    ):
        """
        Initialize planning context.

        Args:
            plannable_tools: Tools that can be used in plans
            holder: SnapshotHolder for reading/writing plan state
        """
        self._plannable_tools = plannable_tools
        self._holder = holder

    def get_plannable_tools(self) -> list[Tool]:
        """
        Get tools available for use in execution plans.

        Returns:
            List of Tool objects
        """
        return self._plannable_tools

    def get_plan(self, plan_id: str) -> Optional[PlanCacheEntry]:
        """
        Get a plan by ID from the plan cache.

        Args:
            plan_id: Unique identifier for the plan

        Returns:
            PlanCacheEntry or None if not found
        """
        return self._holder.get_plan(plan_id)

    def set_plan(self, plan_id: str, plan: Plan, internal: dict[str, Any]) -> None:
        """
        Store or update a plan in the cache.

        Args:
            plan_id: Unique identifier for the plan
            plan: The Plan object
            internal: Internal metadata (tool_used, template_id, etc.)
        """
        self._holder.set_plan(plan_id, plan, internal)
