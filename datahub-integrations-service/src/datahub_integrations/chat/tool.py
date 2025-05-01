from __future__ import annotations

from datahub_integrations.gen_ai.mlflow_init import MLFLOW_INITIALIZED

import dataclasses
import inspect
from typing import Any, Callable, Dict, List, TypeVar, get_type_hints

import mlflow
import mlflow.entities
from pydantic import BaseModel, Field, create_model

assert MLFLOW_INITIALIZED


class ToolRunError(Exception):
    """Error raised when a tool fails to execute."""

    pass


@dataclasses.dataclass
class Tool:
    fn: Callable
    name: str = Field(description="Name of the tool")
    description: str = Field(description="Description of what the tool does")

    _parameters_model: type[BaseModel] = dataclasses.field(
        init=False,
    )

    def __post_init__(self) -> None:
        """Initialize the parameters model from function signature."""
        self._parameters_model = self._create_parameters_model(self.fn)

    @classmethod
    def _create_parameters_model(cls, fn: Callable) -> type[BaseModel]:
        """Create a pydantic model from the function's type hints."""
        sig = inspect.signature(fn)
        type_hints = get_type_hints(fn)

        fields: Dict[str, tuple[type, Any]] = {}
        for param_name, param in sig.parameters.items():
            param_type = type_hints.get(param_name, Any)
            default = ... if param.default == inspect.Parameter.empty else param.default
            fields[param_name] = (param_type, default)

        return create_model("ToolParameters", **fields)  # type: ignore

    def parameters(self) -> type[BaseModel]:
        """Get the pydantic model representing the tool's parameters."""
        return self._parameters_model

    def to_bedrock_spec(self) -> dict:
        schema = self.parameters().schema()
        return {
            "toolSpec": {
                "name": self.name,
                "description": self.description,
                "inputSchema": {"json": schema},
            }
        }

    def run(self, arguments: dict) -> Any:
        """Run the tool with arguments."""
        with mlflow.start_span(
            f"run_tool_{self.name}",
            span_type=mlflow.entities.SpanType.TOOL,
            attributes={"tool_name": self.name},
        ):
            try:
                # Validate arguments using the parameters model
                validated_args = self.parameters()(**arguments)
                result = self.fn(**validated_args.__dict__)
                return result
            except Exception as e:
                raise ToolRunError(f"Error executing tool {self.name}: {e}") from e


_ToolFn = TypeVar("_ToolFn", bound=Callable)


class ToolRegistry:
    def __init__(self) -> None:
        self.tools: Dict[str, Tool] = {}

    def tool(self, *, description: str) -> Callable[[_ToolFn], _ToolFn]:
        # Decorator to register a tool with the registry.
        def decorator(fn: _ToolFn) -> _ToolFn:
            name = fn.__name__
            tool = Tool(fn=fn, name=name, description=description)
            assert name not in self.tools, f"Tool {name} already registered"
            self.tools[name] = tool
            return fn

        return decorator

    def get_all_tools(self) -> List[Tool]:
        return list(self.tools.values())
