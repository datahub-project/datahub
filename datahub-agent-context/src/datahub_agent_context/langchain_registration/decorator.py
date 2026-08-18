from __future__ import annotations

import importlib.metadata
import logging
from typing import Any, List, Optional

from datahub_agent_context._registration_core import (
    AgentRegistrar,
    ToolCapture,
    datahub_tool_decorator as datahub_tool,
)

logger = logging.getLogger(__name__)

__all__ = ["datahub_tool", "register_langchain_agent"]


def _extract_tool_captures(tools: List[Any]) -> List[ToolCapture]:
    from datahub_agent_context.langchain_registration.handler import (
        _extract_tool_captures as _handler_extract,
    )

    return _handler_extract(tools)


def register_langchain_agent(
    agent_executor: Any,
    *,
    agent_id: str,
    agent_name: Optional[str] = None,
    description: Optional[str] = None,
    instructions: Optional[str] = None,
    emitter: Optional[Any] = None,
) -> None:
    """Snapshot the agent's structure and emit it to DataHub immediately.

    Use when you have a fully-built AgentExecutor but don't want to add a
    callback handler.  Inspects ``agent_executor.tools`` to extract tool info.
    """
    framework_version: Optional[str] = None
    try:
        framework_version = importlib.metadata.version("langchain")
    except Exception:
        pass

    registrar = AgentRegistrar(
        framework="LangChain",
        framework_version=framework_version,
        platform="langchain",
        agent_id=agent_id,
        agent_name=agent_name,
        description=description,
        instructions=instructions,
        emitter=emitter,
    )

    raw_tools: List[Any] = []
    try:
        raw_tools = list(getattr(agent_executor, "tools", None) or [])
    except Exception:
        pass

    model_id: Optional[str] = None
    try:
        llm = agent_executor.agent.llm
        model_id = getattr(llm, "model_name", None) or getattr(llm, "model", None)
    except Exception:
        pass

    captures = _extract_tool_captures(raw_tools)
    registrar.register_structure(captures, model_id)
