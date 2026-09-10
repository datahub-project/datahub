from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

try:
    from langchain_core.callbacks import BaseCallbackHandler
except ImportError as e:
    raise ImportError(
        "langchain_core is required for DataHubCallbackHandler. "
        "Install it with: pip install langchain-core"
    ) from e

from datahub_agent_context._registration_core import (
    AgentRegistrar,
    ToolCapture,
    read_tool_metadata,
)

logger = logging.getLogger(__name__)


def _extract_tool_captures(tools: List[Any]) -> List[ToolCapture]:
    captures: List[ToolCapture] = []
    for t in tools:
        name: str = getattr(t, "name", None) or type(t).__name__
        description: Optional[str] = getattr(t, "description", None)

        input_schema: Optional[Dict[str, Any]] = None
        args_schema = getattr(t, "args_schema", None)
        if args_schema is not None:
            try:
                input_schema = args_schema.model_json_schema()
            except Exception:
                try:
                    input_schema = args_schema.schema()
                except Exception:
                    pass

        meta = read_tool_metadata(t)
        if meta is None:
            meta = read_tool_metadata(getattr(t, "func", None))

        captures.append(
            ToolCapture(
                name=name,
                description=description,
                input_schema=input_schema,
                datasets=list(meta.datasets) if meta else [],
                skill=meta.skill if meta else None,
                external_url=meta.external_url if meta else None,
            )
        )
    return captures


class DataHubCallbackHandler(BaseCallbackHandler):
    """LangChain callback handler that auto-registers agents into DataHub.

    Attach this handler to any LangChain agent or LLM chain.  It captures tool
    shapes from the ``tools`` list at construction time, records the model name
    on the first LLM call, and emits the full agent graph to DataHub when the
    top-level chain finishes.
    """

    ignore_llm: bool = False
    ignore_chain: bool = False
    ignore_agent: bool = False

    def __init__(
        self,
        registrar: AgentRegistrar,
        *,
        tools: Optional[List[Any]] = None,
    ) -> None:
        super().__init__()
        self._registrar = registrar
        self._model_id: Optional[str] = None
        self._captured_tools: List[ToolCapture] = []

        if tools:
            self._captured_tools = _extract_tool_captures(tools)

    def _record_model(self, serialized: Dict[str, Any], kwargs: Dict[str, Any]) -> None:
        """Extract model name from LangChain callback kwargs and serialized dict."""
        if self._model_id is not None:
            return
        invocation_params: Dict[str, Any] = kwargs.get("invocation_params") or {}
        serialized_kwargs: Dict[str, Any] = serialized.get("kwargs") or {}
        # ls_model_name is the standardised field set by all LangChain LLM classes
        ls_metadata: Dict[str, Any] = kwargs.get("metadata") or {}
        model_name: Optional[str] = (
            ls_metadata.get("ls_model_name")
            or invocation_params.get("model_name")
            or invocation_params.get("model")
            or serialized_kwargs.get("model_name")
            or serialized_kwargs.get("model")
        )
        if model_name:
            self._model_id = model_name

    def on_llm_start(
        self,
        serialized: Dict[str, Any],
        prompts: List[str],
        **kwargs: Any,
    ) -> None:
        self._record_model(serialized, kwargs)

    def on_chat_model_start(
        self,
        serialized: Dict[str, Any],
        messages: List[Any],
        **kwargs: Any,
    ) -> None:
        # ChatOpenAI, ChatAnthropic, ChatVertexAI etc. call this hook, not on_llm_start
        self._record_model(serialized, kwargs)

    def on_chain_end(
        self,
        outputs: Dict[str, Any],
        **kwargs: Any,
    ) -> None:
        try:
            self._registrar.register_structure(self._captured_tools, self._model_id)
        except Exception as exc:
            logger.warning("DataHub agent registration failed (ignored): %s", exc)
