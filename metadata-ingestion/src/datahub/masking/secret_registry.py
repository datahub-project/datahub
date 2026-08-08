"""Thread-safe registry for secrets, scoped per execution.

Secrets are grouped by the execution that registered them so overlapping
in-process executions don't interfere: one execution ending drops only its
own secrets. The masking filter always sees the union of all groups (via
``snapshot``), so masking is process-global and always-on; it can over-mask
during overlap (safe) but never under-mask.

Registration stores raw values only — no key expansion, no pattern work.
``snapshot`` expands keys (raw + repr + URL-quoted + JSON-escaped) on demand
from the union of all groups, so the regex carries the working set of the
concurrent executions, not the process-lifetime union.
"""

import json
import os
import threading
import uuid
from contextvars import ContextVar
from typing import Dict, Optional, Tuple

from datahub.masking.constants import REDACTED_FORMAT
from datahub.masking.logging_utils import get_masking_safe_logger

logger = get_masking_safe_logger(__name__)

# Marker prefix used to refuse previously-redacted values pasted back into a
# recipe. Derived from REDACTED_FORMAT so the registry and the filter share
# one definition — a second hardcoded copy would let them drift.
_MARKER_PREFIX = REDACTED_FORMAT.split("{", 1)[0]

# Secrets registered outside any execution scope land here. Dropped when no
# real executions remain active, so the catch-all does not grow unbounded in a
# long-lived worker.
_GLOBAL_GROUP = "__global__"

# Identifies the execution owning the ambient context's scope. Set by
# begin_execution, read by register_secret when no explicit exec_id is passed,
# cleared by end_execution when it ends the ambient scope.
_current_exec: ContextVar[Optional[str]] = ContextVar("masking_exec_id", default=None)


def is_masking_enabled() -> bool:
    """True unless DATAHUB_DISABLE_SECRET_MASKING is set to a truthy value."""
    return os.getenv("DATAHUB_DISABLE_SECRET_MASKING", "").lower() not in ("true", "1")


def _expand_keys(raw_value: str) -> Dict[str, None]:
    """All string forms of a secret that should be masked.

    Besides the raw value, covers repr-escaped forms, SQLAlchemy-style URL
    encoding of ``:@/``, and JSON-escaped forms (the inner content of
    ``json.dumps(v)``). The JSON form matters for the stdout/stderr wrapper
    path and for JSON formatters that serialize before the filter sees the
    record.
    """
    keys: Dict[str, None] = {raw_value: None}
    if any(c in raw_value for c in ["\n", "\r", "\t", "\\", '"', "'"]):
        repr_value = repr(raw_value)[1:-1]
        if repr_value != raw_value:
            keys[repr_value] = None
    sqlalchemy_encoded = (
        raw_value.replace(":", "%3A").replace("@", "%40").replace("/", "%2F")
    )
    if sqlalchemy_encoded != raw_value:
        keys[sqlalchemy_encoded] = None
    json_inner = json.dumps(raw_value)[1:-1]
    if json_inner != raw_value:
        keys[json_inner] = None
    return keys


class SecretRegistry:
    """Thread-safe registry for secrets, scoped per execution."""

    _instance: Optional["SecretRegistry"] = None
    _lock = threading.Lock()

    # Upper bound on raw secret count; the regex carries ~4x MAX_SECRETS
    # value alternatives plus per-name markers.
    MAX_SECRETS = 2500

    def __init__(self) -> None:
        self._groups: Dict[str, Dict[str, str]] = {}
        self._version: int = 0
        self._registry_lock = threading.Lock()
        self._capacity_warned = False
        self._global_fallthrough_warned = False

    @classmethod
    def get_instance(cls) -> "SecretRegistry":
        with cls._lock:
            if cls._instance is None:
                cls._instance = cls()
            return cls._instance

    @classmethod
    def reset_instance(cls) -> None:
        """Reset the singleton (test-only)."""
        with cls._lock:
            cls._instance = None
        _current_exec.set(None)

    # --- Execution scoping ------------------------------------------------

    def begin_execution(self) -> str:
        """Open a new secret scope and return its token.

        Each call opens a distinct scope; two calls on the same context
        return different tokens and own different groups, so ending one
        never drops another's secrets. The ambient contextvar is set under
        ``_registry_lock`` so the contextvar and the ``_groups`` entry
        publish atomically w.r.t. a concurrent ``register_secret``.
        """
        exec_id = uuid.uuid4().hex
        with self._registry_lock:
            self._groups[exec_id] = {}
            _current_exec.set(exec_id)
            self._global_fallthrough_warned = False
            self._capacity_warned = False
        return exec_id

    def end_execution(self, execution_id: Optional[str] = None) -> None:
        # Drop an execution's secrets. With an explicit token, drops that
        # group (cross-thread); without it, drops the ambient scope. A dead
        # id is a no-op. When the last real scope ends, __global__ is dropped
        # too. A completely no-op call changes neither groups nor version.
        ambient = _current_exec.get()
        if execution_id is None:
            execution_id = ambient
        if ambient is not None and execution_id == ambient:
            _current_exec.set(None)
        with self._registry_lock:
            popped = self._groups.pop(execution_id, None) if execution_id else None
            if popped is not None:
                if not any(g != _GLOBAL_GROUP for g in self._groups):
                    self._groups.pop(_GLOBAL_GROUP, None)
                self._version += 1
            has_live = any(g != _GLOBAL_GROUP for g in self._groups) or any(
                self._groups.values()
            )
        if popped is not None:
            return
        if has_live:
            logger.warning(
                "end_execution resolved to no group but the registry still "
                "holds live executions or secrets; a scope may be leaking"
            )
        else:
            logger.debug("end_execution called with no scope to drop")

    # --- Registration -----------------------------------------------------

    def _admit(
        self,
        variable_name: str,
        raw_value: str,
        group: Dict[str, str],
    ) -> bool:
        """Validate and store one secret in ``group``. Returns True if stored.

        Single validation path shared by ``register_secret`` and
        ``register_secrets_batch``. Stores the raw value (no expansion) and
        bumps ``_version`` once per call (the batch path bumps once for the
        whole batch, outside this helper).
        """
        if not isinstance(raw_value, str) or not raw_value:
            return False
        if len(raw_value) < 3:
            logger.warning(
                "Refusing to register secret %s: value is below the "
                "3-character minimum floor (%d chars).",
                variable_name,
                len(raw_value),
            )
            return False
        if _MARKER_PREFIX in raw_value:
            # Refusal is fail-open for this one secret: the value will NOT be
            # masked in logs. State that plainly so an operator sees it.
            logger.warning(
                "Refusing to register secret %s: value contains the "
                "redaction marker prefix %r. This value will NOT be masked "
                "in logs; it looks like previously-redacted output pasted "
                "back into a recipe. Rename the variable or change the value.",
                variable_name,
                _MARKER_PREFIX,
            )
            return False
        if raw_value in group:
            return False
        # Capacity is on raw secret count, not expanded keys.
        total = sum(len(g) for g in self._groups.values())
        if total >= self.MAX_SECRETS:
            if not self._capacity_warned:
                logger.warning(
                    "Secret registry at capacity (%d raw secrets). "
                    "Skipping registration of %s; this value will NOT be "
                    "masked in logs.",
                    self.MAX_SECRETS,
                    variable_name,
                )
                self._capacity_warned = True
            return False
        group[raw_value] = variable_name
        return True

    def _resolve_group(self, exec_id: Optional[str]) -> Dict[str, str]:
        """Return the group for ``exec_id`` (or the ambient/global fallback).

        An unknown explicit exec_id falls through to ``__global__`` (dead
        groups are not revived). When the fall-through lands in
        ``__global__`` while live execution scopes exist, warn once per
        execution — the flag is reset by ``begin_execution`` and ``clear``.
        """
        if exec_id is not None:
            group = self._groups.get(exec_id)
            if group is not None:
                return group
            # Unknown explicit id: fall through to __global__ without reviving.
            active = [g for g in self._groups if g != _GLOBAL_GROUP]
            if active and not self._global_fallthrough_warned:
                self._global_fallthrough_warned = True
                logger.warning(
                    "Secret registered under an unknown execution id while "
                    "live scope(s) are active; landing in __global__. If the "
                    "id is stale the secret outlives its intended scope.",
                )
            return self._groups.setdefault(_GLOBAL_GROUP, {})
        ambient = _current_exec.get()
        if ambient is not None and ambient in self._groups:
            return self._groups[ambient]
        global_group = self._groups.setdefault(_GLOBAL_GROUP, {})
        active = [g for g in self._groups if g != _GLOBAL_GROUP]
        if active and not self._global_fallthrough_warned:
            self._global_fallthrough_warned = True
            logger.warning(
                "Secret registered outside any execution scope while %d "
                "execution scope(s) are active; landing in __global__. If "
                "the ambient contextvar does not propagate from "
                "begin_execution to the registration sites, all secrets "
                "pool in __global__ and per-execution scoping does nothing.",
                len(active),
            )
        return global_group

    def register_secret(
        self,
        variable_name: str,
        raw_value: str,
        exec_id: Optional[str] = None,
    ) -> None:
        """Register a secret for masking.

        With ``exec_id`` (the token from ``begin_execution``), the secret
        lands in that scope — this is how a worker thread registers into a
        parent execution's scope, since ContextVars do not cross thread
        boundaries. Without ``exec_id``, the secret lands in the ambient
        context's scope, or ``__global__`` if the ambient context opened no
        scope.
        """
        with self._registry_lock:
            group = self._resolve_group(exec_id)
            if self._admit(variable_name, raw_value, group):
                self._version += 1

    def register_secrets_batch(
        self,
        secrets: Dict[str, str],
        exec_id: Optional[str] = None,
    ) -> None:
        """Register multiple secrets atomically under one execution scope."""
        if not secrets:
            return
        with self._registry_lock:
            group = self._resolve_group(exec_id)
            admitted = False
            for name, value in secrets.items():
                if self._admit(name, value, group):
                    admitted = True
            if admitted:
                self._version += 1

    # --- Reads -------------------------------------------------------------

    def snapshot(self) -> Tuple[int, Dict[str, str]]:
        # Return (version, expanded_key -> name) for the union of groups.
        # Deep-copy under the lock, expand outside it so concurrent mask_text
        # isn't blocked on O(N) string work. First registration wins a name.
        with self._registry_lock:
            version = self._version
            groups_copy = {k: dict(v) for k, v in self._groups.items()}
        expanded: Dict[str, str] = {}
        for group in groups_copy.values():
            for raw_value, name in group.items():
                for key in _expand_keys(raw_value):
                    expanded.setdefault(key, name)
        return version, expanded

    def get_version(self) -> int:
        # Int read is atomic under the GIL; bumps happen under _registry_lock.
        return self._version

    def get_count(self) -> int:
        """Number of raw secrets currently registered (the union)."""
        with self._registry_lock:
            return sum(len(g) for g in self._groups.values())

    def has_active_executions(self) -> bool:
        """True if any per-execution scope is currently open.

        ``__global__`` (the catch-all for ambient registrations with no
        exec_id) does not count as an active execution — it is the residual
        bucket, not a scope.
        """
        with self._registry_lock:
            return any(g != _GLOBAL_GROUP for g in self._groups)

    def has_secret(self, variable_name: str) -> bool:
        """True if a secret was registered under ``variable_name``."""
        with self._registry_lock:
            return any(variable_name in g.values() for g in self._groups.values())

    def get_secret_value(self, variable_name: str) -> Optional[str]:
        """Return the raw value for ``variable_name`` (first-wins across scopes)."""
        with self._registry_lock:
            for group in self._groups.values():
                for raw_value, name in group.items():
                    if name == variable_name:
                        return raw_value
            return None

    def clear(self) -> None:
        """Drop all secrets from all executions (primarily for tests)."""
        with self._registry_lock:
            self._groups = {}
            self._version += 1
            self._capacity_warned = False
            self._global_fallthrough_warned = False
        _current_exec.set(None)
