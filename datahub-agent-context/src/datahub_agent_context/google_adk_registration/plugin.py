from __future__ import annotations

import inspect
import logging
from typing import Any, Callable, Dict, List, Optional

from datahub_agent_context._registration_core import (
    AgentRegistrar,
    ToolCapture,
    read_tool_metadata,
)

logger = logging.getLogger(__name__)


def _proto_schema_to_json_schema(schema: Any) -> Dict[str, Any]:
    """Convert a google.ai.generativelanguage Schema proto to a JSON-Schema dict."""
    # Proto Type enum values: 1=STRING 2=NUMBER 3=INTEGER 4=BOOLEAN 5=ARRAY 6=OBJECT
    _TYPE_MAP: Dict[int, str] = {
        1: "string",
        2: "number",
        3: "integer",
        4: "boolean",
        5: "array",
        6: "object",
    }
    result: Dict[str, Any] = {}
    raw_type = getattr(schema, "type_", None) or getattr(schema, "type", None)
    if raw_type:
        result["type"] = _TYPE_MAP.get(int(raw_type), "string")
    desc = getattr(schema, "description", None)
    if desc:
        result["description"] = desc
    properties = getattr(schema, "properties", None)
    if properties:
        result["properties"] = {
            k: _proto_schema_to_json_schema(v) for k, v in properties.items()
        }
    required = getattr(schema, "required", None)
    if required:
        result["required"] = list(required)
    items = getattr(schema, "items", None)
    if items:
        result["items"] = _proto_schema_to_json_schema(items)
    return result


def _adk_tool_schema(t: Any) -> Optional[Dict[str, Any]]:
    """Extract the parameter schema for an ADK tool, preferring the ADK-native path.

    ADK wraps plain functions in FunctionTool, which builds a FunctionDeclaration
    internally. Reading that declaration is more accurate than re-parsing the
    signature because ADK handles pydantic models, Optional[X], list[T], etc.
    """
    # Primary path: ADK FunctionTool exposes _get_declaration()
    try:
        declaration = t._get_declaration()
        # google-adk >= 1.x may expose parameters_json_schema directly
        json_schema = getattr(declaration, "parameters_json_schema", None)
        if isinstance(json_schema, dict):
            return json_schema
        # Older ADK: declaration.parameters is a Schema proto
        params_proto = getattr(declaration, "parameters", None)
        if params_proto is not None:
            return _proto_schema_to_json_schema(params_proto)
    except Exception:
        pass

    # Fallback for plain callables not wrapped by ADK
    func: Optional[Callable[..., Any]] = getattr(t, "_func", None) or (
        t if callable(t) else None
    )
    if func is None:
        return None

    try:
        sig = inspect.signature(func)
    except (ValueError, TypeError):
        return None

    properties: Dict[str, Any] = {}
    required: List[str] = []
    # typing.get_type_hints() resolves PEP-563 string annotations (from __future__ import annotations)
    # back to actual types; fall back to raw __annotations__ if resolution fails.
    try:
        import typing as _typing

        annotations: Dict[str, Any] = _typing.get_type_hints(func)
    except Exception:
        annotations = getattr(func, "__annotations__", {})
    _TYPE_MAP: Dict[Any, str] = {
        int: "integer",
        "int": "integer",
        float: "number",
        "float": "number",
        bool: "boolean",
        "bool": "boolean",
        list: "array",
        "list": "array",
        dict: "object",
        "dict": "object",
    }
    for param_name, param in sig.parameters.items():
        if param_name in ("self", "return"):
            continue
        if param.kind in (
            inspect.Parameter.VAR_POSITIONAL,
            inspect.Parameter.VAR_KEYWORD,
        ):
            continue
        type_hint = annotations.get(param_name)
        properties[param_name] = {"type": _TYPE_MAP.get(type_hint, "string")}
        if param.default is inspect.Parameter.empty:
            required.append(param_name)

    if not properties:
        return None
    schema: Dict[str, Any] = {"type": "object", "properties": properties}
    if required:
        schema["required"] = required
    return schema


def _capture_tools(tools: List[Callable[..., Any]]) -> List[ToolCapture]:
    captures: List[ToolCapture] = []
    for t in tools:
        # Prefer .name from ADK FunctionTool; fall back to __name__ for plain callables
        name: str = (
            getattr(t, "name", None) or getattr(t, "__name__", None) or type(t).__name__
        )
        description: Optional[str] = inspect.getdoc(t)
        input_schema = _adk_tool_schema(t)

        meta = read_tool_metadata(t)
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


class DataHubBeforeModelCallback:
    """Callable passed as ``before_model_callback`` on a Google ADK Agent."""

    def __init__(
        self,
        registrar: AgentRegistrar,
        *,
        tools: Optional[List[Callable[..., Any]]] = None,
    ) -> None:
        self._registrar = registrar
        self._tools_captured: bool = False
        self._captured: List[ToolCapture] = []
        self._model_id: Optional[str] = None

        if tools:
            self._captured = _capture_tools(tools)
            self._tools_captured = True

    def __call__(self, callback_context: Any, llm_request: Any) -> None:
        """Called by Google ADK before each model invocation."""
        if not self._tools_captured:
            try:
                raw_tools = list(
                    getattr(getattr(callback_context, "agent", None), "tools", None)
                    or []
                )
                self._captured = _capture_tools(raw_tools)
            except Exception as exc:
                logger.debug("Could not capture tools from callback_context: %s", exc)
            self._tools_captured = True

        if self._model_id is None:
            try:
                raw_model = getattr(llm_request, "model", None)
                model: Optional[str] = raw_model if isinstance(raw_model, str) else None
                if not model:
                    config = getattr(llm_request, "config", None)
                    if isinstance(config, dict):
                        model = config.get("model")
                    elif config is not None:
                        raw_cfg_model = getattr(config, "model", None)
                        model = (
                            raw_cfg_model if isinstance(raw_cfg_model, str) else None
                        )
                self._model_id = model or None
            except Exception as exc:
                logger.debug("Could not capture model from llm_request: %s", exc)

        try:
            self._registrar.register_structure(self._captured, self._model_id)
        except Exception as exc:
            logger.warning("DataHub agent registration failed (ignored): %s", exc)
