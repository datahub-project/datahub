"""The MCP policy an admin sets on DataHub's settings page, read from GMS.

GraphQL ``mcpSettings`` holds the master switch and the overrides for the
default server at /mcp: whether it is on, which tools it serves, and its
instructions. The service reads it with the caller's own token (it holds no
credential of its own), validates it, and shares the result with every caller
for up to 60 seconds. The cache is keyed by the GMS and its version, never by
caller.

Fail closed: a read that fails (timeout, 5xx, a GraphQL error, a payload that
doesn't validate) never turns into defaults, because the stored settings may
have turned MCP or a tool off. Only two answers mean defaults: GMS returning no
settings, and a GMS so old its schema has no ``mcpSettings`` query.
"""

import asyncio
import dataclasses
import enum
import time
from typing import Any, Callable, Hashable, Optional

from datahub.configuration.common import GraphError
from datahub.sdk.main_client import DataHubClient
from loguru import logger

from datahub_core_mcp_tools.graphql_helpers import http_status
from datahub_core_mcp_tools.mcp_instructions import InstructionSize
from datahub_core_mcp_tools.tool_groups import (
    TOOL_REF_TYPE_GROUP,
    TOOL_REF_TYPE_TOOL,
    ToolRef,
)

SETTINGS_TTL_SECONDS = 60.0

MCP_SETTINGS_QUERY = """
query getMcpSettings {
  mcpSettings {
    enabled
    defaultMcpServerOverrides {
      enabled
      instructionSize
      customInstructions
      tools { refType toolId enabled }
    }
  }
}
"""

# AGENT refs are DataHub Cloud's; Core has no agents, so they select nothing.
_REF_TYPES = frozenset({TOOL_REF_TYPE_TOOL, TOOL_REF_TYPE_GROUP, "AGENT"})


class PolicySource(enum.Enum):
    SETTINGS = "settings"
    DEFAULTS = "defaults"
    """GMS has the settings API but returned no settings."""
    LEGACY = "legacy"
    """GMS predates the settings API."""


@dataclasses.dataclass(frozen=True)
class McpPolicy:
    enabled: bool
    """False when the master switch or the Default server is off: /mcp is 404."""

    tool_refs: Optional[tuple[ToolRef, ...]]
    """The Default server's tool selection; None serves every tool."""

    instruction_size: InstructionSize
    custom_instructions: Optional[str]
    source: PolicySource


DEFAULT_POLICY = McpPolicy(
    enabled=True,
    tool_refs=None,
    instruction_size=InstructionSize.LARGE,
    custom_instructions=None,
    source=PolicySource.DEFAULTS,
)
LEGACY_POLICY = dataclasses.replace(DEFAULT_POLICY, source=PolicySource.LEGACY)


class SettingsUnavailableError(Exception):
    """The settings couldn't be read or didn't validate; nobody's request may run."""


class SettingsDeniedError(Exception):
    """GMS refused this caller the settings. Says nothing about other callers."""

    def __init__(self, status: int) -> None:
        super().__init__(f"GMS refused the MCP settings (HTTP {status})")
        self.status = status


class MalformedSettingsError(ValueError):
    pass


def _expect(value: Any, kind: type, what: str, *, optional: bool = False) -> Any:
    if value is None and optional:
        return None
    if not isinstance(value, kind):
        raise MalformedSettingsError(f"{what} is {type(value).__name__}")
    return value


def _tool_ref(raw: Any) -> ToolRef:
    raw = _expect(raw, dict, "a tool ref")
    ref_type = _expect(raw.get("refType"), str, "refType")
    if ref_type not in _REF_TYPES:
        raise MalformedSettingsError(f"unknown refType {ref_type!r}")
    tool_id = _expect(raw.get("toolId"), str, "toolId")
    return ToolRef(
        ref_type=ref_type,
        tool_id=tool_id,
        enabled=_expect(raw.get("enabled"), bool, "a tool ref's enabled"),
    )


def _instruction_size(raw: Any) -> InstructionSize:
    raw = _expect(raw, str, "instructionSize", optional=True)
    if raw is None:
        return InstructionSize.LARGE
    try:
        return InstructionSize(raw.lower())
    except ValueError:
        # A size from a newer GMS: the size only shapes the instructions, so the
        # default is safe, unlike guessing at a switch or a tool selection.
        logger.warning(f"Unknown MCP instructionSize {raw!r}; using LARGE")
        return InstructionSize.LARGE


def parse_mcp_settings(data: Any) -> McpPolicy:
    """The policy in a ``getMcpSettings`` response's data; raises if malformed."""
    data = _expect(data, dict, "the response")
    if "mcpSettings" not in data:
        raise MalformedSettingsError("the response has no mcpSettings")
    settings = _expect(data["mcpSettings"], dict, "mcpSettings", optional=True)
    if settings is None:
        return DEFAULT_POLICY
    master = _expect(settings.get("enabled"), bool, "mcpSettings.enabled")
    overrides = _expect(
        settings.get("defaultMcpServerOverrides"),
        dict,
        "defaultMcpServerOverrides",
        optional=True,
    )
    if overrides is None:
        return dataclasses.replace(
            DEFAULT_POLICY, enabled=master, source=PolicySource.SETTINGS
        )
    tools = _expect(overrides.get("tools"), list, "tools", optional=True)
    return McpPolicy(
        enabled=master
        and _expect(
            overrides.get("enabled"), bool, "defaultMcpServerOverrides.enabled"
        ),
        tool_refs=tuple(_tool_ref(ref) for ref in tools) if tools else None,
        instruction_size=_instruction_size(overrides.get("instructionSize")),
        custom_instructions=_expect(
            overrides.get("customInstructions"),
            str,
            "customInstructions",
            optional=True,
        ),
        source=PolicySource.SETTINGS,
    )


def _is_missing_settings_query(errors: Any) -> bool:
    """Whether GraphQL errors say only that Query has no mcpSettings field.

    That is how a GMS from before the settings API answers. Any other error,
    including a missing nested field, is not proof of an old schema.
    """
    if not isinstance(errors, list) or not errors:
        return False
    for error in errors:
        message = error.get("message") if isinstance(error, dict) else None
        if not isinstance(message, str) or not (
            "FieldUndefined" in message
            and "Field 'mcpSettings' in type 'Query' is undefined" in message
        ):
            return False
    return True


def _denied_status(errors: Any) -> Optional[int]:
    """401 or 403 when GMS refused the read itself (it says so in each error's code)."""
    if not isinstance(errors, list) or not errors:
        return None
    codes = {
        (error.get("extensions") or {}).get("code") if isinstance(error, dict) else None
        for error in errors
    }
    if len(codes) == 1 and (code := next(iter(codes))) in (401, 403):
        return code
    return None


def read_mcp_policy(client: DataHubClient) -> McpPolicy:
    """Read and validate the policy with ``client``. Blocks."""
    try:
        data = client._graph.execute_graphql(
            MCP_SETTINGS_QUERY,
            operation_name="getMcpSettings",
            format_exception=False,
        )
    except Exception as e:
        status = http_status(e)
        if status in (401, 403):
            raise SettingsDeniedError(status) from e
        # The SDK raises its HTTP failures as GraphError subclasses too; only an
        # answered query carries a list of GraphQL errors.
        errors = e.args[0] if isinstance(e, GraphError) and e.args else None
        if status is None and isinstance(errors, list):
            if _is_missing_settings_query(errors):
                return LEGACY_POLICY
            denied = _denied_status(errors)
            if denied is not None:
                raise SettingsDeniedError(denied) from e
            raise SettingsUnavailableError(f"GraphQL errors: {errors!r}") from e
        raise SettingsUnavailableError(f"{type(e).__name__}: {e}") from e
    try:
        return parse_mcp_settings(data)
    except MalformedSettingsError as e:
        raise SettingsUnavailableError(f"Malformed MCP settings: {e}") from e


class SettingsState(enum.Enum):
    """What /health reports. Never the settings themselves."""

    UNKNOWN = "unknown"
    """Not read yet: the first caller's request reads them."""
    ENABLED = "enabled"
    DISABLED = "disabled"
    UNAVAILABLE = "unavailable"
    """The last read failed."""


@dataclasses.dataclass(frozen=True)
class _Entry:
    key: Hashable
    read_at: float
    policy: McpPolicy


class McpPolicyCache:
    """The validated policy, shared by every caller, for at most ``ttl_seconds``.

    One refresh runs at a time, so a slow, late read can't replace a newer one,
    and callers queued behind a failed read share its failure instead of each
    waiting on GMS in turn. A caller GMS refuses the settings to gets an error of
    their own; the shared entry and state are left as they were.

    The last good policy is kept for /health, but a request is only ever served
    a policy younger than the TTL.
    """

    def __init__(
        self,
        *,
        ttl_seconds: float = SETTINGS_TTL_SECONDS,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._ttl_seconds = ttl_seconds
        self._clock = clock
        self._entry: Optional[_Entry] = None
        self._failed = False
        self._failures = 0
        self._lock = asyncio.Lock()

    @property
    def state(self) -> SettingsState:
        if self._failed:
            return SettingsState.UNAVAILABLE
        if self._entry is None:
            return SettingsState.UNKNOWN
        return (
            SettingsState.ENABLED
            if self._entry.policy.enabled
            else SettingsState.DISABLED
        )

    def _fresh(self, key: Hashable) -> Optional[McpPolicy]:
        entry = self._entry
        if (
            entry is not None
            and entry.key == key
            and self._clock() - entry.read_at < self._ttl_seconds
        ):
            return entry.policy
        return None

    async def get(self, key: Hashable, read: Callable[[], McpPolicy]) -> McpPolicy:
        """The policy for ``key``, calling ``read`` (in a thread) when none is fresh.

        Raises SettingsUnavailableError or SettingsDeniedError.
        """
        policy = self._fresh(key)
        if policy is not None:
            return policy
        failures = self._failures
        async with self._lock:
            policy = self._fresh(key)
            if policy is not None:
                return policy
            if self._failures != failures:
                # A read failed while this caller waited: share its outcome.
                raise SettingsUnavailableError("The MCP settings could not be read")
            read_at = self._clock()
            try:
                policy = await asyncio.to_thread(read)
            except SettingsDeniedError:
                raise
            except SettingsUnavailableError as e:
                self._failed = True
                self._failures += 1
                logger.warning(f"Could not read the MCP settings: {e}")
                raise
            except Exception as e:
                self._failed = True
                self._failures += 1
                logger.warning(f"Could not read the MCP settings: {e!r}")
                raise SettingsUnavailableError(str(e)) from e
            if self._failed or self.state is SettingsState.UNKNOWN:
                logger.info(f"MCP settings read ({policy.source.value})")
            self._entry = _Entry(key=key, read_at=read_at, policy=policy)
            self._failed = False
            return policy
