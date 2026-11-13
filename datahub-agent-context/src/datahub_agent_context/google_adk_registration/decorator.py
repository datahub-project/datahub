from __future__ import annotations

import importlib.metadata
import logging
from typing import Any, Callable, List, Optional

from datahub_agent_context._registration_core import (
    AgentRegistrar,
    datahub_tool_decorator as datahub_tool,
)
from datahub_agent_context.google_adk_registration.plugin import _capture_tools

logger = logging.getLogger(__name__)

__all__ = ["datahub_tool", "register_google_adk_agent"]


def register_google_adk_agent(
    agent: Any,
    *,
    agent_id: str,
    agent_name: Optional[str] = None,
    description: Optional[str] = None,
    instructions: Optional[str] = None,
    emitter: Optional[Any] = None,
) -> None:
    """Snapshot the agent's structure and emit it to DataHub immediately.

    Use when you have a fully-built Google ADK Agent and want a one-shot
    registration without attaching a callback.
    """
    framework_version: Optional[str] = None
    try:
        framework_version = importlib.metadata.version("google-adk")
    except Exception:
        pass

    registrar = AgentRegistrar(
        framework="Google ADK",
        framework_version=framework_version,
        platform="google-adk",
        agent_id=agent_id,
        agent_name=agent_name,
        description=description,
        instructions=instructions,
        emitter=emitter,
    )

    raw_tools: List[Callable[..., Any]] = []
    try:
        raw_tools = list(
            getattr(agent, "tools", None) or getattr(agent, "_tools", None) or []
        )
    except Exception:
        pass

    model_id: Optional[str] = None
    try:
        model_id = getattr(agent, "model", None) or getattr(agent, "_model", None)
    except Exception:
        pass

    captures = _capture_tools(raw_tools)
    registrar.register_structure(captures, model_id)
