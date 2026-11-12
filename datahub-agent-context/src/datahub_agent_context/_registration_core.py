"""Framework-agnostic core for auto-registering agents into DataHub's Agent Registry.

This module owns everything that is not tied to a specific agent framework:

* JSON-Schema -> :class:`ApiParam` conversion for typed tool signatures.
* Model-name -> ``mlModel`` urn resolution with provider inference.
* The ``@datahub_tool`` decorator that annotates a plain tool function with the
  datasets / skill / docs it relates to.
* :class:`ToolCapture` — the normalized, framework-neutral snapshot of one tool.
* :class:`AgentRegistrar` — accumulates captured structure (tools, model) and
  emits the ``aiAgent`` / ``api`` / ``mlModel`` graph exactly once per
  process, failing open so instrumentation never breaks the host application.

Framework adapters (LangChain callback handler, Google ADK plugin) capture the
runtime shapes their framework exposes and feed a single :class:`AgentRegistrar`.
"""

from __future__ import annotations

import functools
import logging
import re
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Set

from datahub.api.entities.agent.agent import Agent, StructuredPropertyValue
from datahub.api.entities.agent.api import API_SUBTYPE_FUNCTION, Api, ApiParam
from datahub.emitter.generic_emitter import Emitter
from datahub.emitter.mce_builder import make_ml_model_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import MLModelPropertiesClass

logger = logging.getLogger(__name__)

# Structured-property qualified names set on the aiAgent entity at registration time.
FRAMEWORK_PROPERTY = "io.acryl.agent.framework"
VERSION_PROPERTY = "io.acryl.agent.version"

_DEFAULT_ACTOR = "urn:li:corpuser:datahub"

# JSON-Schema type -> friendly ``data_type`` string understood by ApiParam.
_JSON_TYPE_TO_DATA_TYPE: Dict[str, str] = {
    "string": "string",
    "integer": "integer",
    "number": "number",
    "boolean": "boolean",
    "object": "object",
    "array": "array",
    "null": "string",
}

# Model-name prefix -> provider. Ordered longest-prefix-first so, e.g.,
# ``text-embedding-`` wins over a bare ``t``.
_MODEL_PREFIX_TO_PROVIDER: List[tuple[str, str]] = [
    ("text-embedding-", "openai"),
    ("gpt-", "openai"),
    ("o1", "openai"),
    ("o3", "openai"),
    ("claude", "anthropic"),
    ("gemini", "google"),
    ("mistral", "mistral"),
    ("mixtral", "mistral"),
    ("llama", "meta"),
    ("command", "cohere"),
]

# URN-unsafe characters: ':' is the urn delimiter and '(' ')' ',' are reserved
# for tuple keys. Sanitize arbitrary tool names to a safe charset.
_URN_UNSAFE_CHARS = re.compile(r"[^a-z0-9._/-]+")


def _slug(value: str) -> str:
    """Normalize a provider/name fragment to a lower-case, urn-safe slug."""
    slug = _URN_UNSAFE_CHARS.sub("-", value.strip().lower().replace(" ", "-"))
    return slug.strip("-") or "tool"


def _friendly_data_type(schema: Dict[str, Any]) -> str:
    """Map a JSON-Schema property node to a friendly ``data_type`` string."""
    any_of = schema.get("anyOf")
    if isinstance(any_of, list):
        non_null = [
            branch
            for branch in any_of
            if isinstance(branch, dict) and branch.get("type") != "null"
        ]
        if len(non_null) == 1:
            return _friendly_data_type(non_null[0])

    json_type = schema.get("type")
    if json_type == "array":
        items = schema.get("items")
        if isinstance(items, dict):
            return f"array<{_friendly_data_type(items)}>"
        return "array"
    if isinstance(json_type, str):
        return _JSON_TYPE_TO_DATA_TYPE.get(json_type, json_type)
    return "string"


def json_schema_to_tool_params(schema: Optional[Dict[str, Any]]) -> List[ApiParam]:
    """Convert a JSON-Schema object into a list of typed :class:`ApiParam`."""
    if not schema:
        return []
    properties = schema.get("properties")
    if not isinstance(properties, dict):
        return []
    required: Set[str] = set(schema.get("required") or [])
    params: List[ApiParam] = []
    for name, prop in properties.items():
        prop_schema = prop if isinstance(prop, dict) else {}
        params.append(
            ApiParam(
                name=name,
                data_type=_friendly_data_type(prop_schema),
                required=name in required,
                description=prop_schema.get("description"),
            )
        )
    return params


def model_urn_from_identifier(model_name: str, provider: Optional[str] = None) -> str:
    """Resolve a model name (and optional provider) to an ``mlModel`` urn."""
    resolved_provider = provider
    name = model_name.strip()

    if resolved_provider is None and "/" in name:
        prefix, _, remainder = name.partition("/")
        resolved_provider = prefix
        name = remainder

    if resolved_provider is None:
        lowered = name.lower()
        for prefix, candidate in _MODEL_PREFIX_TO_PROVIDER:
            if lowered.startswith(prefix):
                resolved_provider = candidate
                break

    if resolved_provider is None:
        resolved_provider = "unknown"

    return make_ml_model_urn(_slug(resolved_provider), name, "PROD")


@dataclass
class DataHubToolMetadata:
    """Metadata attached to a tool function by :func:`datahub_tool`."""

    datasets: List[str] = field(default_factory=list)
    skill: Optional[str] = None
    external_url: Optional[str] = None


def datahub_tool_decorator(
    datasets: Optional[List[str]] = None,
    skill: Optional[str] = None,
    external_url: Optional[str] = None,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Core of the ``@datahub_tool`` decorator."""
    metadata = DataHubToolMetadata(
        datasets=list(datasets or []),
        skill=skill,
        external_url=external_url,
    )

    def wrap(func: Callable[..., Any]) -> Callable[..., Any]:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            return func(*args, **kwargs)

        wrapper.__datahub__ = metadata  # type: ignore[attr-defined]
        func.__datahub__ = metadata  # type: ignore[attr-defined]
        return wrapper

    return wrap


datahub_tool = datahub_tool_decorator


def read_tool_metadata(obj: Any) -> Optional[DataHubToolMetadata]:
    """Read :class:`DataHubToolMetadata` off a tool object, if present."""
    metadata = getattr(obj, "__datahub__", None)
    if isinstance(metadata, DataHubToolMetadata):
        return metadata
    inner = getattr(obj, "func", None)
    if inner is not None:
        metadata = getattr(inner, "__datahub__", None)
        if isinstance(metadata, DataHubToolMetadata):
            return metadata
    return None


@dataclass
class ToolCapture:
    """A framework-neutral snapshot of a single captured tool."""

    name: str
    description: Optional[str] = None
    subtype: str = API_SUBTYPE_FUNCTION
    input_schema: Optional[Dict[str, Any]] = None
    datasets: List[str] = field(default_factory=list)
    skill: Optional[str] = None
    external_url: Optional[str] = None


def resolve_emitter(emitter: Optional[Emitter]) -> Optional[Emitter]:
    """Return a usable emitter or ``None`` (disabled) — never raise."""
    if emitter is not None:
        return emitter
    try:
        from datahub.ingestion.graph.client import get_default_graph

        return get_default_graph()
    except Exception as exc:
        logger.warning("DataHub emission disabled: could not resolve emitter: %s", exc)
        return None


class AgentRegistrar:
    """Accumulates captured agent structure and emits it to DataHub once.

    Args:
        framework: Human-readable framework name (e.g. ``"LangChain"``).
        framework_version: The installed framework version string.
        platform: The dataPlatform slug (``"langchain"`` / ``"google-adk"``).
        agent_id: Stable id of the agent (bare id or full ``urn:li:aiAgent:``).
        agent_name: Display name of the agent.
        description: What the agent does.
        instructions: Base instructions injected into the agent's system prompt.
        emitter: Emitter to use; defaults to the environment's default graph.
    """

    def __init__(
        self,
        *,
        framework: str,
        framework_version: Optional[str],
        platform: str,
        agent_id: str,
        agent_name: Optional[str] = None,
        description: Optional[str] = None,
        instructions: Optional[str] = None,
        emitter: Optional[Emitter] = None,
    ) -> None:
        self.framework = framework
        self.framework_version = framework_version
        self.platform = platform
        self.agent_id = agent_id
        self.agent_name = agent_name
        self.description = description
        self.instructions = instructions
        self._emitter = emitter

        self._registered = False
        self._emitted_urns: Set[str] = set()

        self._tools: Dict[str, ToolCapture] = {}
        self._model_identifier: Optional[str] = None
        self._model_provider: Optional[str] = None

    def fill_defaults(
        self, agent_name: Optional[str], agent_description: Optional[str]
    ) -> None:
        """Backfill name/description only if not already set."""
        if not self.agent_name and agent_name:
            self.agent_name = agent_name
        if not self.description and agent_description:
            self.description = agent_description

    def register_structure(
        self,
        tools: List[ToolCapture],
        model_identifier: Optional[str],
        model_provider: Optional[str] = None,
    ) -> None:
        """Record the agent's tools and model, then emit if not yet emitted."""
        for tool in tools:
            self._tools.setdefault(tool.name, tool)
        if model_identifier and self._model_identifier is None:
            self._model_identifier = model_identifier
            self._model_provider = model_provider
        self.flush()

    def flush(self) -> None:
        """Emit the full agent graph once, if not already emitted (fail-open)."""
        try:
            if self._registered:
                return
            if self._get_emitter() is None:
                return
            mcps = self._build_graph()
            if not mcps:
                return
            self._emit(mcps)
            self._registered = True
        except Exception as exc:
            logger.warning("DataHub agent registration failed (ignored): %s", exc)

    def _get_emitter(self) -> Optional[Emitter]:
        self._emitter = resolve_emitter(self._emitter)
        return self._emitter

    def _agent_urn(self) -> str:
        return Agent(id=self.agent_id, name=self.agent_name or self.agent_id).urn

    def _tool_id(self, tool_name: str) -> str:
        return f"{self.platform}.{_slug(tool_name)}"

    def _build_graph(self) -> List[MetadataChangeProposalWrapper]:
        mcps: List[MetadataChangeProposalWrapper] = []

        tool_urns: List[str] = []
        skills: Set[str] = set()
        datasets: Set[str] = set()
        for capture in self._tools.values():
            tool = Api(
                id=self._tool_id(capture.name),
                name=capture.name,
                subtypes=[capture.subtype],
                description=capture.description,
                parameters=json_schema_to_tool_params(capture.input_schema) or None,
                external_url=capture.external_url,
                platform=self.platform,
            )
            tool_urns.append(tool.urn)
            mcps.extend(tool.generate_mcp())
            if capture.skill:
                skills.add(capture.skill)
            datasets.update(capture.datasets)

        model_urns: List[str] = []
        if self._model_identifier and self._model_identifier.strip():
            model_urn = model_urn_from_identifier(
                self._model_identifier, self._model_provider
            )
            model_urns.append(model_urn)
            mcps.append(
                MetadataChangeProposalWrapper(
                    entityUrn=model_urn,
                    aspect=MLModelPropertiesClass(name=self._model_identifier),
                )
            )

        structured_properties: Dict[str, StructuredPropertyValue] = {
            FRAMEWORK_PROPERTY: self.framework
        }
        if self.framework_version:
            structured_properties[VERSION_PROPERTY] = self.framework_version

        agent = Agent(
            id=self.agent_id,
            name=self.agent_name or self.agent_id,
            description=self.description,
            instructions=self.instructions,
            source_type="EXTERNAL",
            platform=self.platform,
            tools=sorted(tool_urns) or None,
            skills=sorted(skills) or None,
            models=model_urns or None,
            consumes_datasets=sorted(datasets) or None,
            structured_properties=structured_properties,
        )
        mcps.extend(agent.generate_mcp())
        return mcps

    def _emit(self, mcps: List[MetadataChangeProposalWrapper]) -> None:
        emitter = self._get_emitter()
        if emitter is None:
            return
        for mcp in mcps:
            try:
                emitter.emit(mcp)
                if mcp.entityUrn is not None:
                    self._emitted_urns.add(mcp.entityUrn)
            except Exception as exc:
                logger.warning("DataHub emission failed for %s: %s", mcp.entityUrn, exc)
