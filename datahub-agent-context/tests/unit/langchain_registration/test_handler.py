from __future__ import annotations

from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock

from datahub_agent_context._registration_core import (
    AgentRegistrar,
    datahub_tool_decorator as datahub_tool,
)
from datahub_agent_context.langchain_registration.decorator import (
    register_langchain_agent,
)
from datahub_agent_context.langchain_registration.handler import (
    DataHubCallbackHandler,
    _extract_tool_captures,
)
from tests.unit.fake_emitter import FakeEmitter

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_registrar(emitter: FakeEmitter) -> AgentRegistrar:
    return AgentRegistrar(
        framework="LangChain",
        framework_version="0.3.0",
        platform="langchain",
        agent_id="test-agent",
        agent_name="Test Agent",
        description="A test agent",
        emitter=emitter,
    )


def _fake_tool(
    name: str,
    description: str = "A test tool",
    schema: Optional[Dict[str, Any]] = None,
    func: Any = None,
) -> MagicMock:
    tool = MagicMock()
    tool.name = name
    tool.description = description
    tool.func = func

    if schema is not None:
        _schema = schema

        class _FakeSchema:
            @staticmethod
            def model_json_schema() -> Dict[str, Any]:
                assert _schema is not None
                return _schema

        tool.args_schema = _FakeSchema
    else:
        tool.args_schema = None

    return tool


# ---------------------------------------------------------------------------
# _extract_tool_captures
# ---------------------------------------------------------------------------


def test_extract_tool_captures_basic():
    tool = _fake_tool("search", "Search for things")
    captures = _extract_tool_captures([tool])

    assert len(captures) == 1
    assert captures[0].name == "search"
    assert captures[0].description == "Search for things"
    assert captures[0].datasets == []
    assert captures[0].skill is None
    assert captures[0].external_url is None


def test_extract_tool_captures_with_schema():
    schema = {
        "properties": {
            "query": {"type": "string", "description": "search query"},
            "limit": {"type": "integer"},
        },
        "required": ["query"],
    }
    tool = _fake_tool("search", schema=schema)
    captures = _extract_tool_captures([tool])

    assert captures[0].input_schema == schema


def test_extract_tool_captures_schema_fallback_to_v1():
    tool = _fake_tool("search")

    class _V1Schema:
        @staticmethod
        def model_json_schema() -> Dict[str, Any]:
            raise AttributeError("no v2")

        @staticmethod
        def schema() -> Dict[str, Any]:
            return {"properties": {"q": {"type": "string"}}}

    tool.args_schema = _V1Schema
    captures = _extract_tool_captures([tool])
    assert captures[0].input_schema == {"properties": {"q": {"type": "string"}}}


def test_extract_tool_captures_schema_errors_silenced():
    tool = _fake_tool("search")

    class _BrokenSchema:
        @staticmethod
        def model_json_schema() -> Dict[str, Any]:
            raise RuntimeError("boom v2")

        @staticmethod
        def schema() -> Dict[str, Any]:
            raise RuntimeError("boom v1")

    tool.args_schema = _BrokenSchema
    captures = _extract_tool_captures([tool])
    assert captures[0].input_schema is None


def test_extract_tool_captures_datahub_metadata_on_tool():
    @datahub_tool(datasets=["urn:li:dataset:(urn:li:dataPlatform:s3,raw.events,PROD)"])
    def my_func(q: str) -> str:
        return q

    tool = _fake_tool("my_tool")
    tool.__datahub__ = my_func.__datahub__

    captures = _extract_tool_captures([tool])
    assert captures[0].datasets == [
        "urn:li:dataset:(urn:li:dataPlatform:s3,raw.events,PROD)"
    ]


def test_extract_tool_captures_datahub_metadata_on_func():
    @datahub_tool(
        datasets=["urn:li:dataset:(urn:li:dataPlatform:snowflake,db.t,PROD)"],
        skill="search",
        external_url="https://example.com/docs",
    )
    def annotated_fn(q: str) -> str:
        return q

    tool = _fake_tool("my_tool", func=annotated_fn)
    captures = _extract_tool_captures([tool])

    assert captures[0].datasets == [
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.t,PROD)"
    ]
    assert captures[0].skill == "search"
    assert captures[0].external_url == "https://example.com/docs"


# ---------------------------------------------------------------------------
# DataHubCallbackHandler
# ---------------------------------------------------------------------------


def test_handler_with_preloaded_tools_emits_on_chain_end():
    emitter = FakeEmitter()
    registrar = _make_registrar(emitter)
    tool = _fake_tool("lookup", "Lookup a record")

    handler = DataHubCallbackHandler(registrar, tools=[tool])
    handler.on_chain_end(outputs={})

    agent_urns = {u for u in emitter.urns() if "aiAgent" in u}
    api_urns = {u for u in emitter.urns() if "api" in u.lower() or "API" in u}
    assert agent_urns, "Expected at least one aiAgent urn"
    assert api_urns, "Expected at least one api/tool urn"


def test_handler_without_tools_still_emits_agent():
    emitter = FakeEmitter()
    registrar = _make_registrar(emitter)
    handler = DataHubCallbackHandler(registrar)
    handler.on_chain_end(outputs={})

    agent_urns = {u for u in emitter.urns() if "aiAgent" in u}
    assert agent_urns


def test_on_llm_start_captures_model_from_invocation_params():
    emitter = FakeEmitter()
    registrar = _make_registrar(emitter)
    handler = DataHubCallbackHandler(registrar)

    handler.on_llm_start(
        serialized={},
        prompts=["hello"],
        invocation_params={"model_name": "gpt-4o"},
    )
    assert handler._model_id == "gpt-4o"


def test_on_llm_start_captures_model_from_serialized_kwargs():
    emitter = FakeEmitter()
    registrar = _make_registrar(emitter)
    handler = DataHubCallbackHandler(registrar)

    handler.on_llm_start(
        serialized={"kwargs": {"model": "claude-3-5-sonnet"}},
        prompts=[],
    )
    assert handler._model_id == "claude-3-5-sonnet"


def test_on_llm_start_prefers_invocation_params():
    emitter = FakeEmitter()
    registrar = _make_registrar(emitter)
    handler = DataHubCallbackHandler(registrar)

    handler.on_llm_start(
        serialized={"kwargs": {"model": "fallback-model"}},
        prompts=[],
        invocation_params={"model_name": "primary-model"},
    )
    assert handler._model_id == "primary-model"


def test_on_llm_start_does_not_overwrite_existing_model():
    emitter = FakeEmitter()
    registrar = _make_registrar(emitter)
    handler = DataHubCallbackHandler(registrar)
    handler._model_id = "already-set"

    handler.on_llm_start(
        serialized={},
        prompts=[],
        invocation_params={"model_name": "new-model"},
    )
    assert handler._model_id == "already-set"


def test_model_urn_emitted_when_model_captured():
    emitter = FakeEmitter()
    registrar = _make_registrar(emitter)
    handler = DataHubCallbackHandler(registrar)

    handler.on_llm_start(
        serialized={},
        prompts=[],
        invocation_params={"model_name": "gpt-4o"},
    )
    handler.on_chain_end(outputs={})

    model_urns = {u for u in emitter.urns() if "mlModel" in u}
    assert model_urns, "Expected an mlModel urn for the captured model"


def test_datahub_tool_datasets_flow_through_handler():
    @datahub_tool(
        datasets=["urn:li:dataset:(urn:li:dataPlatform:redshift,events,PROD)"]
    )
    def get_events(q: str) -> str:
        return q

    tool = _fake_tool("get_events", func=get_events)

    emitter = FakeEmitter()
    registrar = _make_registrar(emitter)
    handler = DataHubCallbackHandler(registrar, tools=[tool])
    handler.on_chain_end(outputs={})

    agent_urns = {u for u in emitter.urns() if "aiAgent" in u}
    assert agent_urns
    agent_urn = next(iter(agent_urns))
    aspects = emitter.aspects_for(agent_urn)

    # consumesDatasets is emitted as UpstreamLineageClass on the agent urn
    from datahub.metadata.schema_classes import UpstreamLineageClass

    lineage = next((a for a in aspects if isinstance(a, UpstreamLineageClass)), None)
    assert lineage is not None, (
        "Expected UpstreamLineageClass aspect for consumesDatasets"
    )
    upstream_datasets = [u.dataset for u in (lineage.upstreams or [])]
    assert any("redshift" in str(d) for d in upstream_datasets), (
        f"Expected redshift dataset in upstreams, got {upstream_datasets}"
    )


# ---------------------------------------------------------------------------
# register_langchain_agent
# ---------------------------------------------------------------------------


def test_register_langchain_agent_emits_graph():
    emitter = FakeEmitter()

    tool = _fake_tool("query_db", "Run a database query")

    executor = MagicMock()
    executor.tools = [tool]
    executor.agent.llm.model_name = "gpt-4o-mini"

    register_langchain_agent(
        executor,
        agent_id="my-db-agent",
        agent_name="DB Agent",
        description="Queries a database",
        emitter=emitter,
    )

    agent_urns = {u for u in emitter.urns() if "aiAgent" in u}
    api_urns = {u for u in emitter.urns() if "api" in u.lower() or "API" in u}
    model_urns = {u for u in emitter.urns() if "mlModel" in u}

    assert agent_urns, "Expected aiAgent urn"
    assert api_urns, "Expected tool api urn"
    assert model_urns, "Expected mlModel urn"


def test_register_langchain_agent_tolerates_missing_llm():
    emitter = FakeEmitter()

    class _NoLlmAgent:
        @property
        def llm(self) -> None:
            raise AttributeError("no llm")

    class _NoLlmExecutor:
        tools: List[Any] = []
        agent = _NoLlmAgent()

    executor = _NoLlmExecutor()

    register_langchain_agent(
        executor,
        agent_id="no-llm-agent",
        emitter=emitter,
    )

    agent_urns = {u for u in emitter.urns() if "aiAgent" in u}
    assert agent_urns


def test_register_langchain_agent_tolerates_missing_tools_attr():
    emitter = FakeEmitter()

    executor = MagicMock(spec=[])  # no attributes at all

    register_langchain_agent(
        executor,
        agent_id="no-tools-agent",
        emitter=emitter,
    )

    agent_urns = {u for u in emitter.urns() if "aiAgent" in u}
    assert agent_urns


# ---------------------------------------------------------------------------
# on_chat_model_start (ChatOpenAI / ChatAnthropic path — Finding 2 fix)
# ---------------------------------------------------------------------------


def test_on_chat_model_start_captures_ls_model_name():
    """ls_model_name is the standardised LangChain metadata field for chat models."""
    emitter = FakeEmitter()
    registrar = _make_registrar(emitter)
    handler = DataHubCallbackHandler(registrar)

    handler.on_chat_model_start(
        serialized={},
        messages=[],
        metadata={"ls_model_name": "gpt-4o", "ls_provider": "openai"},
    )
    assert handler._model_id == "gpt-4o"


def test_on_chat_model_start_falls_back_to_invocation_params():
    """Older LangChain builds may not set ls_model_name; fall back to invocation_params."""
    emitter = FakeEmitter()
    registrar = _make_registrar(emitter)
    handler = DataHubCallbackHandler(registrar)

    handler.on_chat_model_start(
        serialized={},
        messages=[],
        invocation_params={"model": "claude-3-5-sonnet-20241022"},
    )
    assert handler._model_id == "claude-3-5-sonnet-20241022"


def test_on_chat_model_start_model_urn_emitted():
    """Full path: chat model hook → chain end → mlModel urn in graph."""
    emitter = FakeEmitter()
    registrar = _make_registrar(emitter)
    handler = DataHubCallbackHandler(registrar)

    handler.on_chat_model_start(
        serialized={},
        messages=[],
        metadata={"ls_model_name": "gemini-2.0-flash"},
    )
    handler.on_chain_end(outputs={})

    model_urns = {u for u in emitter.urns() if "mlModel" in u}
    assert model_urns, "Expected mlModel urn from on_chat_model_start path"


def test_on_chat_model_start_does_not_overwrite_existing():
    emitter = FakeEmitter()
    registrar = _make_registrar(emitter)
    handler = DataHubCallbackHandler(registrar)
    handler._model_id = "already-set"

    handler.on_chat_model_start(
        serialized={},
        messages=[],
        metadata={"ls_model_name": "new-model"},
    )
    assert handler._model_id == "already-set"
