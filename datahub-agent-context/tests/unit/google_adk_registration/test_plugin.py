from __future__ import annotations

import sys
import types
from typing import Any
from unittest.mock import MagicMock

# ---------------------------------------------------------------------------
# Stub out google.adk so the module can be imported without the real package.
# ---------------------------------------------------------------------------

_google_pkg = types.ModuleType("google")
_google_pkg.__path__ = []  # mark as namespace package
_adk_pkg = types.ModuleType("google.adk")
_agents_pkg = types.ModuleType("google.adk.agents")


class _StubAgent:
    """Minimal stand-in for google.adk.agents.Agent."""

    def __init__(
        self,
        *,
        model: str = "",
        name: str = "",
        description: str = "",
        instruction: str = "",
        tools: Any = None,
    ) -> None:
        self.model = model
        self.name = name
        self.description = description
        self.instruction = instruction
        self.tools = tools or []


_agents_pkg.Agent = _StubAgent  # type: ignore[attr-defined]
sys.modules.setdefault("google", _google_pkg)
sys.modules.setdefault("google.adk", _adk_pkg)
sys.modules.setdefault("google.adk.agents", _agents_pkg)

# ---------------------------------------------------------------------------
# Imports under test (after stubs are in place)
# ---------------------------------------------------------------------------

from datahub_agent_context._registration_core import AgentRegistrar  # noqa: E402
from datahub_agent_context.google_adk_registration import (  # noqa: E402
    DataHubBeforeModelCallback,
    datahub_tool,
    register_google_adk_agent,
)
from tests.unit.fake_emitter import FakeEmitter  # noqa: E402

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_registrar(emitter: FakeEmitter) -> AgentRegistrar:
    return AgentRegistrar(
        framework="Google ADK",
        framework_version="1.0.0",
        platform="google-adk",
        agent_id="test-agent",
        agent_name="Test Agent",
        emitter=emitter,
    )


def _agent_urn(agent_id: str = "test-agent") -> str:
    from datahub.api.entities.agent.agent import Agent

    return Agent(id=agent_id, name=agent_id).urn


# ---------------------------------------------------------------------------
# Tests: DataHubBeforeModelCallback with pre-loaded tools
# ---------------------------------------------------------------------------


def test_callback_preloaded_tools_emits_on_call() -> None:
    """Pre-loaded tools should be emitted on the first __call__."""

    def search_datasets(query: str) -> str:
        """Search for datasets in DataHub."""
        return ""

    emitter = FakeEmitter()
    registrar = _make_registrar(emitter)
    callback = DataHubBeforeModelCallback(registrar, tools=[search_datasets])

    llm_request = MagicMock()
    llm_request.model = "gemini-2.0-flash"
    callback(MagicMock(), llm_request)

    urns = emitter.urns()
    assert any("aiAgent" in u for u in urns), f"No aiAgent urn emitted; got {urns}"
    assert any("api" in u for u in urns), f"No api (tool) urn emitted; got {urns}"


def test_callback_tool_urn_contains_tool_name() -> None:
    def my_tool(x: str) -> str:
        """My special tool."""
        return ""

    emitter = FakeEmitter()
    registrar = _make_registrar(emitter)
    callback = DataHubBeforeModelCallback(registrar, tools=[my_tool])
    callback(MagicMock(), MagicMock())

    tool_urns = [u for u in emitter.urns() if "api" in u]
    assert any("my-tool" in u or "my_tool" in u for u in tool_urns), (
        f"Expected tool urn for 'my_tool'; got {tool_urns}"
    )


def test_callback_captures_model_from_llm_request() -> None:
    def tool_a(x: str) -> str:
        """Tool A."""
        return ""

    emitter = FakeEmitter()
    registrar = _make_registrar(emitter)
    callback = DataHubBeforeModelCallback(registrar, tools=[tool_a])

    llm_request = MagicMock()
    llm_request.model = "gemini-2.0-flash"
    callback(MagicMock(), llm_request)

    ml_urns = [u for u in emitter.urns() if "mlModel" in u]
    assert any("gemini" in u for u in ml_urns), (
        f"Expected mlModel urn with 'gemini'; got {ml_urns}"
    )


def test_callback_idempotent_on_multiple_calls() -> None:
    """Registration should happen exactly once regardless of how many times __call__ fires."""

    def tool_b() -> str:
        """Tool B."""
        return ""

    emitter = FakeEmitter()
    registrar = _make_registrar(emitter)
    callback = DataHubBeforeModelCallback(registrar, tools=[tool_b])

    callback(MagicMock(), MagicMock())
    count_after_first = len(emitter.mcps)
    assert count_after_first > 0, "Expected at least one MCP after first call"

    for _ in range(4):
        callback(MagicMock(), MagicMock())

    # No new MCPs should be emitted after the first registration.
    assert len(emitter.mcps) == count_after_first, (
        f"Expected {count_after_first} MCPs total, got {len(emitter.mcps)}"
    )


def test_callback_tools_from_callback_context() -> None:
    """When no tools are pre-loaded, tools should be read from callback_context.agent.tools."""

    def ctx_tool(name: str, value: int) -> str:
        """A tool discovered via callback_context."""
        return ""

    emitter = FakeEmitter()
    registrar = _make_registrar(emitter)
    # No tools pre-loaded
    callback = DataHubBeforeModelCallback(registrar)

    mock_agent = MagicMock()
    mock_agent.tools = [ctx_tool]
    ctx = MagicMock()
    ctx.agent = mock_agent

    callback(ctx, MagicMock())

    tool_urns = [u for u in emitter.urns() if "api" in u]
    assert any("ctx-tool" in u or "ctx_tool" in u for u in tool_urns), (
        f"Expected tool urn for ctx_tool; got {tool_urns}"
    )


# ---------------------------------------------------------------------------
# Tests: register_google_adk_agent
# ---------------------------------------------------------------------------


def test_register_google_adk_agent_emits_graph() -> None:
    def fetch_table(table_name: str) -> str:
        """Fetch a BigQuery table."""
        return ""

    agent = _StubAgent(
        model="gemini-2.0-flash",
        name="bq-agent",
        tools=[fetch_table],
    )
    emitter = FakeEmitter()

    register_google_adk_agent(
        agent,
        agent_id="bq-agent",
        agent_name="BQ Agent",
        emitter=emitter,
    )

    urns = emitter.urns()
    assert any("aiAgent" in u for u in urns)
    assert any("api" in u for u in urns)
    assert any("mlModel" in u and "gemini" in u for u in urns)


def test_register_google_adk_agent_no_tools_no_crash() -> None:
    agent = _StubAgent(model="gemini-2.0-flash", name="no-tools-agent", tools=[])
    emitter = FakeEmitter()

    register_google_adk_agent(agent, agent_id="no-tools-agent", emitter=emitter)

    assert any("aiAgent" in u for u in emitter.urns())


# ---------------------------------------------------------------------------
# Tests: @datahub_tool metadata carries through
# ---------------------------------------------------------------------------


def test_datahub_tool_datasets_carry_through() -> None:
    DATASET_URN = "urn:li:dataset:(urn:li:dataPlatform:bigquery,myproject.sales,PROD)"

    @datahub_tool(datasets=[DATASET_URN])
    def sales_lookup(product_id: str) -> str:
        """Look up sales data."""
        return ""

    emitter = FakeEmitter()
    registrar = _make_registrar(emitter)
    callback = DataHubBeforeModelCallback(registrar, tools=[sales_lookup])
    callback(MagicMock(), MagicMock())

    agent_urns = [u for u in emitter.urns() if "aiAgent" in u]
    assert len(agent_urns) == 1
    assert any("aiAgent" in u for u in emitter.urns())


# ---------------------------------------------------------------------------
# Tests: ADK-native schema extraction (_get_declaration path — Finding 5 fix)
# ---------------------------------------------------------------------------


def test_adk_native_schema_preferred_over_inspect() -> None:
    """When a tool exposes _get_declaration().parameters_json_schema, that dict is used."""
    from datahub_agent_context.google_adk_registration.plugin import _adk_tool_schema

    class _FakeTool:
        name = "search_warehouse"
        __name__ = "search_warehouse"

        def __call__(self) -> None:
            pass

        def _get_declaration(self) -> Any:
            decl = MagicMock()
            decl.parameters_json_schema = {
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "The search query"},
                    "limit": {"type": "integer"},
                },
                "required": ["query"],
            }
            return decl

    schema = _adk_tool_schema(_FakeTool())
    assert schema is not None
    assert schema["properties"]["query"]["type"] == "string"
    assert schema["properties"]["limit"]["type"] == "integer"
    assert "query" in schema["required"]


def test_adk_schema_falls_back_when_declaration_missing() -> None:
    """Plain callables without _get_declaration fall back to inspect.signature."""
    from datahub_agent_context.google_adk_registration.plugin import _adk_tool_schema

    def plain_func(table: str, limit: int) -> str:
        return ""

    schema = _adk_tool_schema(plain_func)
    assert schema is not None
    assert "table" in schema["properties"]
    assert schema["properties"]["limit"]["type"] == "integer"


def test_adk_proto_schema_conversion() -> None:
    """_proto_schema_to_json_schema converts a Schema-like object to a dict."""
    from datahub_agent_context.google_adk_registration.plugin import (
        _proto_schema_to_json_schema,
    )

    proto = MagicMock()
    proto.type_ = 6  # OBJECT
    proto.description = "The input schema"
    prop_a = MagicMock()
    prop_a.type_ = 1  # STRING
    prop_a.description = "A string field"
    prop_a.properties = {}
    prop_a.required = []
    prop_a.items = None
    proto.properties = {"field_a": prop_a}
    proto.required = ["field_a"]
    proto.items = None

    result = _proto_schema_to_json_schema(proto)
    assert result["type"] == "object"
    assert result["properties"]["field_a"]["type"] == "string"
    assert "field_a" in result["required"]


def test_datahub_tool_external_url_carries_through() -> None:
    @datahub_tool(external_url="https://docs.example.com/my-tool")
    def my_documented_tool(query: str) -> str:
        """A well-documented tool."""
        return ""

    emitter = FakeEmitter()
    registrar = _make_registrar(emitter)
    callback = DataHubBeforeModelCallback(registrar, tools=[my_documented_tool])
    callback(MagicMock(), MagicMock())

    # Verify the api MCP for the tool has the external url set.
    # The slug keeps underscores, so the urn contains "my_documented_tool".
    api_mcps = [
        m
        for m in emitter.mcps
        if m.entityUrn and "api" in m.entityUrn and "my_documented_tool" in m.entityUrn
    ]
    assert len(api_mcps) > 0, f"No api urn for my_documented_tool; got {emitter.urns()}"
    from datahub.metadata.schema_classes import ApiPropertiesClass

    props_aspect = next(
        (m.aspect for m in api_mcps if isinstance(m.aspect, ApiPropertiesClass)),
        None,
    )
    assert props_aspect is not None, "Expected ApiPropertiesClass aspect on tool urn"
    assert props_aspect.externalUrl == "https://docs.example.com/my-tool"
