import asyncio
import threading
from typing import Any, Optional

import pytest
import requests
from datahub.configuration.common import GraphError
from helpers import rejected_token_error

from datahub_ai_service.mcp.settings import (
    DEFAULT_POLICY,
    LEGACY_POLICY,
    MCP_SETTINGS_QUERY,
    MalformedSettingsError,
    McpPolicy,
    McpPolicyCache,
    PolicySource,
    SettingsDeniedError,
    SettingsState,
    SettingsUnavailableError,
    parse_mcp_settings,
    read_mcp_policy,
)
from datahub_core_mcp_tools.mcp_instructions import InstructionSize
from datahub_core_mcp_tools.tool_groups import ToolRef

FIELD_UNDEFINED = {
    "message": "Validation error (FieldUndefined@[mcpSettings]) : Field "
    "'mcpSettings' in type 'Query' is undefined",
    "extensions": {"classification": "ValidationError"},
}


def _overrides(**fields: Any) -> dict[str, Any]:
    return {
        "mcpSettings": {
            "enabled": True,
            "defaultMcpServerOverrides": {
                "enabled": True,
                "instructionSize": None,
                "customInstructions": None,
                "tools": None,
                **fields,
            },
        }
    }


def test_parse_selection_and_instructions() -> None:
    policy = parse_mcp_settings(
        _overrides(
            instructionSize="SMALL",
            customInstructions="Prefer the gold tables.",
            tools=[
                {"refType": "GROUP", "toolId": "read_metadata", "enabled": True},
                {"refType": "TOOL", "toolId": "add_tags", "enabled": True},
            ],
        )
    )

    assert policy == McpPolicy(
        enabled=True,
        tool_refs=(
            ToolRef("GROUP", "read_metadata", True),
            ToolRef("TOOL", "add_tags", True),
        ),
        instruction_size=InstructionSize.SMALL,
        custom_instructions="Prefer the gold tables.",
        source=PolicySource.SETTINGS,
    )


@pytest.mark.parametrize(
    "data",
    [
        {"mcpSettings": {"enabled": False, "defaultMcpServerOverrides": None}},
        _overrides(enabled=False),
        {
            "mcpSettings": {
                "enabled": False,
                "defaultMcpServerOverrides": _overrides()["mcpSettings"][
                    "defaultMcpServerOverrides"
                ],
            }
        },
    ],
)
def test_master_switch_or_default_row_off_disables(data: dict[str, Any]) -> None:
    assert parse_mcp_settings(data).enabled is False


def test_no_settings_and_empty_selection_mean_defaults() -> None:
    assert parse_mcp_settings({"mcpSettings": None}) is DEFAULT_POLICY
    policy = parse_mcp_settings(_overrides(tools=[], instructionSize="HUGE"))
    assert policy.tool_refs is None
    assert policy.instruction_size is InstructionSize.LARGE


@pytest.mark.parametrize(
    "data",
    [
        None,
        {},
        {"mcpSettings": {"enabled": "false"}},
        {"mcpSettings": []},
        _overrides(enabled=None),
        _overrides(tools="read_metadata"),
        _overrides(tools=[{"refType": "TOOL", "toolId": None, "enabled": True}]),
        _overrides(tools=[{"refType": "BUNDLE", "toolId": "x", "enabled": True}]),
        _overrides(customInstructions=7),
    ],
)
def test_malformed_settings_are_rejected(data: Any) -> None:
    with pytest.raises(MalformedSettingsError):
        parse_mcp_settings(data)


class _Graph:
    def __init__(self, outcome: Any) -> None:
        self._outcome = outcome
        self.queries: list[str] = []

    def execute_graphql(self, query: str, **_: Any) -> Any:
        self.queries.append(query)
        if isinstance(self._outcome, Exception):
            raise self._outcome
        return self._outcome


def _read(outcome: Any) -> McpPolicy:
    client = type("Client", (), {"_graph": _Graph(outcome)})()
    return read_mcp_policy(client)


def test_read_answers() -> None:
    assert _read(_overrides(enabled=False)).enabled is False
    assert _read({"mcpSettings": None}) is DEFAULT_POLICY
    # A GMS from before the settings API: positively identified, so defaults.
    assert _read(GraphError([FIELD_UNDEFINED])) is LEGACY_POLICY


@pytest.mark.parametrize(
    "outcome",
    [
        requests.Timeout("read timed out"),
        requests.ConnectionError("refused"),
        rejected_token_error(500),
        GraphError([{"message": "Failed to retrieve MCP settings"}]),
        # Only part of the query is unknown: not the old schema.
        GraphError(
            [
                FIELD_UNDEFINED,
                {"message": "Field 'customInstructions' in type 'X' is undefined"},
            ]
        ),
        {"mcpSettings": {"enabled": None}},
    ],
)
def test_read_failures_never_become_defaults(outcome: Any) -> None:
    with pytest.raises(SettingsUnavailableError):
        _read(outcome)


@pytest.mark.parametrize(
    ("outcome", "status"),
    [
        (rejected_token_error(401), 401),
        (rejected_token_error(403), 403),
        (GraphError([{"message": "Unauthorized", "extensions": {"code": 403}}]), 403),
    ],
)
def test_read_denials_are_the_callers(outcome: Exception, status: int) -> None:
    with pytest.raises(SettingsDeniedError) as raised:
        _read(outcome)
    assert raised.value.status == status


def test_query_asks_only_for_what_the_policy_needs() -> None:
    graph = _Graph({"mcpSettings": None})
    read_mcp_policy(type("Client", (), {"_graph": graph})())
    assert graph.queries == [MCP_SETTINGS_QUERY]
    assert "servers" not in MCP_SETTINGS_QUERY


class _Clock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


ON = DEFAULT_POLICY
OFF = McpPolicy(
    enabled=False,
    tool_refs=None,
    instruction_size=InstructionSize.LARGE,
    custom_instructions=None,
    source=PolicySource.SETTINGS,
)
KEY = ("http://gms", "v1.9.0", "core")


def _reader(*outcomes: Any) -> Any:
    remaining = list(outcomes)
    calls: list[int] = []

    def read() -> McpPolicy:
        calls.append(1)
        outcome = remaining.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    read.calls = calls  # type: ignore[attr-defined]
    return read


async def test_cache_shares_a_policy_until_it_expires() -> None:
    clock = _Clock()
    cache = McpPolicyCache(clock=clock)
    read = _reader(ON, OFF)

    assert cache.state is SettingsState.UNKNOWN
    assert await cache.get(KEY, read) is ON
    clock.now += 59
    assert await cache.get(KEY, read) is ON
    assert len(read.calls) == 1
    clock.now += 1
    assert await cache.get(KEY, read) is OFF
    assert cache.state is SettingsState.DISABLED


async def test_a_new_gms_version_reads_again() -> None:
    cache = McpPolicyCache(clock=_Clock())
    read = _reader(LEGACY_POLICY, ON)

    assert await cache.get(("http://gms", "v1.3.0", "core"), read) is LEGACY_POLICY
    assert await cache.get(KEY, read) is ON


async def test_stale_policy_is_never_served_after_a_failed_refresh() -> None:
    clock = _Clock()
    cache = McpPolicyCache(clock=clock)
    read = _reader(OFF, SettingsUnavailableError("timeout"), ON)

    assert await cache.get(KEY, read) is OFF
    clock.now += 61
    with pytest.raises(SettingsUnavailableError):
        await cache.get(KEY, read)
    assert cache.state is SettingsState.UNAVAILABLE
    # The next successful read recovers.
    assert await cache.get(KEY, read) is ON
    assert cache.state is SettingsState.ENABLED


async def test_unexpected_errors_fail_closed() -> None:
    cache = McpPolicyCache(clock=_Clock())
    with pytest.raises(SettingsUnavailableError):
        await cache.get(KEY, _reader(KeyError("boom")))
    assert cache.state is SettingsState.UNAVAILABLE


async def test_a_denied_caller_changes_nothing_for_others() -> None:
    clock = _Clock()
    cache = McpPolicyCache(clock=clock)
    assert await cache.get(KEY, _reader(OFF)) is OFF
    clock.now += 61

    with pytest.raises(SettingsDeniedError):
        await cache.get(KEY, _reader(SettingsDeniedError(403)))
    # Neither the stale policy nor a failure was recorded for the next caller.
    assert cache.state is SettingsState.DISABLED
    assert await cache.get(KEY, _reader(ON)) is ON


async def test_concurrent_callers_share_one_read() -> None:
    cache = McpPolicyCache(clock=_Clock())
    started, release = threading.Event(), threading.Event()
    calls: list[int] = []

    def slow_read() -> McpPolicy:
        calls.append(1)
        started.set()
        release.wait(5)
        return OFF

    first = asyncio.create_task(cache.get(KEY, slow_read))
    await asyncio.to_thread(started.wait, 5)
    others = [asyncio.create_task(cache.get(KEY, slow_read)) for _ in range(3)]
    await asyncio.sleep(0.05)
    release.set()

    assert await asyncio.gather(first, *others) == [OFF] * 4
    assert len(calls) == 1


async def test_callers_queued_behind_a_failed_read_share_its_failure() -> None:
    cache = McpPolicyCache(clock=_Clock())
    started, release = threading.Event(), threading.Event()
    calls: list[int] = []

    def failing_read() -> McpPolicy:
        calls.append(1)
        started.set()
        release.wait(5)
        raise SettingsUnavailableError("GMS timed out")

    first = asyncio.create_task(cache.get(KEY, failing_read))
    await asyncio.to_thread(started.wait, 5)
    queued = asyncio.create_task(cache.get(KEY, failing_read))
    await asyncio.sleep(0.05)
    release.set()

    for task in (first, queued):
        with pytest.raises(SettingsUnavailableError):
            await task
    assert len(calls) == 1


async def test_a_late_read_never_replaces_a_newer_policy() -> None:
    cache = McpPolicyCache(clock=_Clock())
    started, release = threading.Event(), threading.Event()
    result: Optional[McpPolicy] = None

    def stale_read() -> McpPolicy:
        started.set()
        release.wait(5)
        return ON

    # The first reader gives up (its client went away) while GMS is still answering.
    first = asyncio.create_task(cache.get(KEY, stale_read))
    await asyncio.to_thread(started.wait, 5)
    first.cancel()
    with pytest.raises(asyncio.CancelledError):
        await first

    result = await cache.get(KEY, _reader(OFF))
    release.set()
    await asyncio.sleep(0.05)

    assert result is OFF
    assert await cache.get(KEY, _reader(ON)) is OFF
