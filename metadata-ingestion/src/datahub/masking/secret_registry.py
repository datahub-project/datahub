"""
Thread-safe registry for managing secrets used in masking.

Secrets are grouped by the execution that registered them, so overlapping
in-process executions don't interfere: one execution ending removes only its own
secrets, never another's. The masking filter always sees the *union* of all
active executions' secrets (via get_all_secrets/get_version), so masking is
process-global and always-on — it can over-mask during overlap (safe) but never
under-mask. See datahub.masking.bootstrap for the lifecycle.

Secrets registered outside any execution scope land in a catch-all ``__global__``
group; it is dropped once the last real execution ends (so it lives at most as
long as concurrent execution activity, not for the whole process).

Concurrency: copy-on-write. Writers (register/begin/end/clear) hold the lock and
atomically swap in freshly-built dicts; readers (the masking hot path) read those
immutable snapshots without a lock.
"""

import json
import os
import threading
import uuid
from contextvars import ContextVar
from enum import Enum, auto
from typing import Dict, Optional

from datahub.masking.logging_utils import get_masking_safe_logger

logger = get_masking_safe_logger(__name__)

# Secrets registered outside an explicit execution scope land here. Dropped when
# no real executions remain active.
_GLOBAL_GROUP = "__global__"

# Identifies the execution registering/owning secrets in the current context.
# Set by begin_execution(), read by register_secret() and end_execution().
_current_exec: ContextVar[Optional[str]] = ContextVar("masking_exec_id", default=None)


class _Admit(Enum):
    """Tri-state result of admitting one secret against the registry.

    A bool (``admitted``) would conflate "duplicate" with "at capacity" —
    both are non-admitting, but only "at capacity" is a warning condition.
    Conflating them makes duplicate registrations (common: the same value
    read twice, or the same recipe's secrets re-registered per execution)
    emit spurious "at capacity" warnings and permanently suppress the real
    one via _capacity_warned. Count and warn only on REJECTED.
    """

    ADMITTED = auto()
    DUPLICATE = auto()
    REJECTED = auto()


def is_masking_enabled() -> bool:
    """Check if masking is enabled."""
    return os.getenv("DATAHUB_DISABLE_SECRET_MASKING", "").lower() not in (
        "true",
        "1",
    )


def _expand_keys(raw_value: str) -> Dict[str, None]:
    """Return all string forms of a secret that should be masked.

    Besides the raw value, this covers:
    - repr-escaped forms (for values with escape sequences)
    - SQLAlchemy-style URL encoding of ``:@/``
    - JSON-escaped forms (``json.dumps(v)[1:-1]``) — for secrets printed via
      ``print(json.dumps(...))`` or emitted by a JSON formatter that serializes
      before the masking filter sees the record. Extras on a LogRecord are
      masked at filter time before formatting, so this is mainly for the
      stdout/stderr wrapper path.
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
    # JSON-escaped form: json.dumps wraps in quotes and escapes inner chars.
    # [1:-1] strips the surrounding quotes so we match the value as it appears
    # inside a serialized object/string field.
    json_inner = json.dumps(raw_value)[1:-1]
    if json_inner != raw_value:
        keys[json_inner] = None
    return keys


class SecretRegistry:
    """Thread-safe registry for secrets, scoped per execution (copy-on-write)."""

    _instance: Optional["SecretRegistry"] = None
    _lock = threading.RLock()

    # Upper bound on the *expanded* key count (raw + repr + sqlalchemy + json
    # variants per secret), not the number of registered secrets. Adding
    # _expand_keys variants reduces how many distinct secrets fit; keep this
    # bound generous because the masking regex is built from these keys.
    MAX_SECRETS = 10000

    def __init__(self):
        # Source of truth: execution_id -> {raw_value: variable_name}.
        self._groups: Dict[str, Dict[str, str]] = {}
        # Derived, immutable snapshots for the masking hot path: the union of all
        # groups (key -> name) and a reverse index (name -> value). Never mutated
        # in place — rebuilt and atomically reassigned by writers (COW).
        self._secrets: Dict[str, str] = {}
        self._name_to_value: Dict[str, str] = {}
        self._version = 0
        # Serializes writers only; readers are lock-free.
        self._registry_lock = threading.RLock()
        # True once we've warned about being at capacity; reset by
        # _rebuild_locked on the condition (count >= MAX) so a capacity
        # warning fires once per rise to capacity, not once per call, and
        # can fire again after executions end and free room.
        self._capacity_warned = False

    @classmethod
    def get_instance(cls) -> "SecretRegistry":
        """Get singleton instance (thread-safe)."""
        with cls._lock:
            if cls._instance is None:
                cls._instance = cls()
            return cls._instance

    @classmethod
    def reset_instance(cls) -> None:
        """Reset singleton instance (and current execution scope).

        Also tears down the installed masking filter (lazily) so a
        subsequent ``install_masking_filter(new_registry)`` picks up the
        new registry. Without this, the filter survives
        ``reset_instance()`` and keeps masking with the old (now-stale)
        registry — masking silently stops working for every secret
        registered after the reset. Tearing down (rather than just
        clearing the ``_installed_filter`` global) also removes the old
        filter from handlers/root, so the next install doesn't attach a
        second filter alongside the stale one. The lazy import avoids a
        circular dependency (masking_filter imports this module).

        ``_bootstrap_lock`` is deliberately NOT held here. ``shutdown_secret_masking()``
        establishes the lock order ``_bootstrap_lock`` -> ``SecretRegistry._lock``
        (it takes ``_bootstrap_lock`` first, then calls into the registry). Taking
        ``_bootstrap_lock`` here would invert that order against a concurrent
        ``shutdown_secret_masking()`` that already holds it and is waiting on
        ``SecretRegistry._lock`` — a classic lock-order-inversion deadlock.
        The teardown below is safe to run without ``_bootstrap_lock`` because
        ``reset_instance`` is a test/dev seam, not a production teardown path,
        and ``uninstall_masking_filter()`` is idempotent and self-guarding.
        """
        with cls._lock:
            cls._instance = None
        _current_exec.set(None)
        try:
            from datahub.masking import masking_filter as _mf

            if _mf._installed_filter is not None:
                _mf.uninstall_masking_filter()
            # Clear the bootstrap-completed latch so the next
            # initialize_secret_masking() re-installs instead of
            # short-circuiting on a stranded True. The lazy import avoids a
            # circular dependency (bootstrap imports this module).
            from datahub.masking import bootstrap as _boot

            _boot.reset_bootstrap_state()
        except Exception as e:
            # Don't let a teardown failure prevent the singleton reset, but
            # don't swallow it silently either — masking would stay installed
            # against a dead registry with no signal.
            logger.debug("reset_instance: filter teardown failed: %r", e)

    # --- Execution scoping (writers) ---------------------------------------

    def begin_execution(self) -> str:
        """Open a secret scope for the current execution; returns its id.

        Secrets registered after this (in the same context) are owned by this
        execution and dropped by the matching end_execution().
        """
        exec_id = uuid.uuid4().hex
        with self._registry_lock:
            self._groups.setdefault(exec_id, {})
        _current_exec.set(exec_id)
        return exec_id

    def ensure_execution(self) -> str:
        """Open a secret scope for the current context only if it doesn't already
        have one; returns the active execution id. Idempotent within a context,
        so a repeated initialize_secret_masking() won't start a second scope.

        Revalidates the ambient id against ``_groups``: a token-based
        ``end_execution`` from another thread drops the group but leaves this
        context's contextvar pointing at the dead id (end_execution only clears
        the ambient contextvar when it matches the *ending* context's). Without
        revalidation, this would return the stale id and recreate a scope under
        a dead id, so a later token holder would target the wrong scope.
        """
        exec_id = _current_exec.get()
        if exec_id is not None:
            with self._registry_lock:
                if exec_id in self._groups:
                    return exec_id
            # Ambient id names no live group — fall through to open a fresh scope.
        return self.begin_execution()

    def end_execution(self, exec_id: Optional[str] = None) -> bool:
        """Drop the current execution's secrets. Returns True if other
        executions are still active (so the caller should NOT fully tear down).

        If ``exec_id`` is provided, drop that specific execution's group —
        this allows ``initialize_secret_masking`` and ``shutdown_secret_masking``
        to be called from different threads/contexts (e.g. a dispatcher that
        starts an execution on one thread and tears it down on another).
        Without it, the ambient context's execution is dropped; if the
        ambient context has no scope, this is a no-op and a debug is logged
        (the signature of the cross-thread hole — a teardown caller that
        forgot to pass the token).
        """
        ambient_exec_id = _current_exec.get()
        if exec_id is None:
            exec_id = ambient_exec_id
        # Clear the ambient context's scope only if it matches the one we're
        # ending; a token-based call from another thread must not clobber this
        # thread's contextvar.
        if ambient_exec_id is not None and exec_id == ambient_exec_id:
            _current_exec.set(None)
        with self._registry_lock:
            if exec_id is not None:
                self._groups.pop(exec_id, None)
            elif ambient_exec_id is None:
                logger.debug(
                    "end_execution called with no ambient execution scope and "
                    "no explicit exec_id; nothing to drop. If this is a "
                    "shutdown call, the caller likely started the execution on "
                    "another thread and should pass the token returned by "
                    "initialize_secret_masking()."
                )
            active = [g for g in self._groups if g != _GLOBAL_GROUP]
            if not active:
                # No real executions left — drop the catch-all bucket too.
                self._groups.pop(_GLOBAL_GROUP, None)
            self._rebuild_locked()
            return bool(active)

    def _current_group_locked(self) -> Dict[str, str]:
        exec_id = _current_exec.get() or _GLOBAL_GROUP
        return self._groups.setdefault(exec_id, {})

    def _rebuild_locked(self) -> None:
        """Recompute the union + reverse index from all groups and publish them
        atomically. Called only by writers holding the lock."""
        secrets: Dict[str, str] = {}
        name_to_value: Dict[str, str] = {}
        for group in self._groups.values():
            for raw_value, name in group.items():
                name_to_value[name] = raw_value
                for key in _expand_keys(raw_value):
                    secrets.setdefault(key, name)
        self._secrets = secrets
        self._name_to_value = name_to_value
        self._version += 1
        # Reset _capacity_warned on the condition (at capacity), not the
        # event (a warning fired). When executions end and free room, this
        # self-corrects so the next capacity rise can warn again; without
        # it, _capacity_warned would stay True forever after the first
        # warning and the real warning would be permanently suppressed.
        # With the >= admit check the union stays under MAX_SECRETS, so
        # this is False after admits; _warn_capacity sets the flag True
        # between rebuilds, suppressing repeats until the next rebuild
        # (admit or end_execution).
        self._capacity_warned = len(self._secrets) >= self.MAX_SECRETS

    # --- Registration (writers) --------------------------------------------

    def _warn_capacity(self, message: str) -> None:
        """Emit a capacity warning once per episode.

        Single enforcement point for the ``_capacity_warned`` gate: this is
        the only place that reads the flag to decide whether to warn and sets
        it after warning. ``_rebuild_locked`` clears the flag when the
        registry drops below capacity so a later rise can warn again; nothing
        else touches it. Centralising the gate here avoids the duplicated-
        guard structure that caused the original capacity bug (two call sites
        each getting the accounting right and drifting).
        """
        if not self._capacity_warned:
            logger.warning(message)
            self._capacity_warned = True

    def _admit_locked(
        self,
        variable_name: str,
        raw_value: str,
        group: Dict[str, str],
        pending_keys: set,
    ) -> _Admit:
        """Admit one secret against the registry, capacity, and batch state.

        Called by register_secret / register_secrets_batch with the
        registry lock held. Returns:

        - DUPLICATE: the raw value is already in this execution's group, or
          already pending in the current batch (dedup across the batch).
          Not a warning condition — duplicate registration is common.
        - REJECTED: admitting would push the *expanded* key count
          (``len(self._secrets)``) to >= MAX_SECRETS. The caller is
          responsible for warning via ``_warn_capacity``; this method does
          not touch ``_capacity_warned`` so the gate stays in one place.
          The capacity bound is on expanded keys (raw + repr + sqlalchemy +
          json variants), not on the number of registered secrets —
          counting secrets instead (the old behavior) let a batch of
          multi-key-expanding values overshoot MAX_SECRETS, which is the
          unit mismatch this fixes.
        - ADMITTED: the secret was added to ``group`` and its truly-new
          expanded keys to ``pending_keys``.

        Mutates ``pending_keys``: unioned with the truly-new keys so the
        batch loop dedup-counts within the batch. The single path passes a
        throwaway ``set()``, so the mutation is harmless there; the batch
        path passes a persistent set so successive calls see prior admits.
        A helper that mutates an argument is easy to misuse later — keep
        this contract in the docstring.
        """
        if raw_value in group or raw_value in pending_keys:
            return _Admit.DUPLICATE
        keys = _expand_keys(raw_value)
        truly_new = set(keys) - self._secrets.keys() - pending_keys
        if len(self._secrets) + len(pending_keys) + len(truly_new) >= self.MAX_SECRETS:
            return _Admit.REJECTED
        group[raw_value] = variable_name
        pending_keys |= truly_new
        return _Admit.ADMITTED

    def register_secret(self, variable_name: str, raw_value: str) -> None:
        """Register a secret for masking under the current execution.

        The secret is owned by the ambient execution scope (set by
        ``begin_execution``/``ensure_execution``). Secrets registered from a
        thread/context that never opened an execution scope land in the
        ``__global__`` catch-all bucket, which is dropped only when no real
        executions remain — so in a long-lived worker that registers from a
        non-execution context, those secrets accumulate for the process
        lifetime (fails safe: over-masking, never leaking). Register from the
        execution's own context to get per-execution cleanup.
        """
        if not raw_value or not isinstance(raw_value, str):
            return
        if len(raw_value) < 3:
            return

        with self._registry_lock:
            group = self._current_group_locked()
            result = self._admit_locked(variable_name, raw_value, group, set())
            if result is _Admit.ADMITTED:
                self._rebuild_locked()
                logger.debug(
                    f"Registered secret: {variable_name[:8]}*** (version {self._version})"
                )
            elif result is _Admit.REJECTED:
                self._warn_capacity(
                    f"Secret registry at capacity ({self.MAX_SECRETS}). "
                    f"Skipping registration of {variable_name}"
                )

    def register_secrets_batch(self, secrets: Dict[str, str]) -> None:
        """Register multiple secrets atomically under the current execution."""
        if not secrets:
            return
        valid_secrets = {
            name: value
            for name, value in secrets.items()
            if value and isinstance(value, str) and len(value) >= 3
        }
        if not valid_secrets:
            return

        with self._registry_lock:
            group = self._current_group_locked()
            pending_keys: set = set()
            admitted = 0
            duplicates = 0
            rejected = 0
            for variable_name, raw_value in valid_secrets.items():
                result = self._admit_locked(
                    variable_name, raw_value, group, pending_keys
                )
                if result is _Admit.ADMITTED:
                    admitted += 1
                elif result is _Admit.DUPLICATE:
                    duplicates += 1
                else:
                    rejected += 1

            if rejected > 0:
                # One summary warning with the rejected count so an operator
                # sees the blast radius, not just one variable name.
                # _warn_capacity owns the _capacity_warned gate, so this is
                # the only place the batch path reads/sets the flag.
                self._warn_capacity(
                    f"Secret registry at capacity ({self.MAX_SECRETS}). "
                    f"Skipped {rejected} of {len(valid_secrets)} secret(s) in batch."
                )

            if admitted > 0:
                self._rebuild_locked()
            logger.debug(
                f"Batch result: admitted {admitted}, duplicates {duplicates}, "
                f"rejected {rejected} (version {self._version})"
            )

    # --- Reads (lock-free; the masking hot path) ---------------------------

    def get_all_secrets(self) -> Dict[str, str]:
        """Union of all active executions' secrets (value -> name)."""
        # COW: self._secrets is an immutable snapshot, never mutated in place.
        return self._secrets.copy()

    def get_version(self) -> int:
        """Current version (bumps whenever the union changes)."""
        return self._version

    def get_count(self) -> int:
        """Number of distinct secret keys currently masked (the union)."""
        return len(self._secrets)

    def clear(self) -> None:
        """Drop all secrets from all executions (primarily for tests)."""
        with self._registry_lock:
            self._groups = {}
            self._rebuild_locked()
            logger.debug("Cleared all secrets from registry")

    def has_secret(self, variable_name: str) -> bool:
        """Check if secret is registered."""
        return variable_name in self._name_to_value

    def get_secret_value(self, variable_name: str) -> Optional[str]:
        """Get secret value by name."""
        return self._name_to_value.get(variable_name)
