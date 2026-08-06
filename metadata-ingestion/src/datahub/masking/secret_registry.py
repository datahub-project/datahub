"""
Thread-safe registry for managing secrets used in masking.

Secrets are grouped by the execution that registered them, so overlapping
in-process executions don't interfere: one execution ending removes only its own
secrets, never another's. The masking filter always sees the *union* of all
active executions' secrets (via get_all_secrets/get_version), so masking is
process-global and always-on — it can over-mask during overlap (safe) but never
under-mask. See datahub.masking.bootstrap for the lifecycle.

Each ``initialize_secret_masking()`` opens a *distinct* scope and returns its
token; the caller owns nesting and passes the token to ``shutdown_secret_masking``.
A second initialize on the same context does NOT alias onto the first — both
scopes stay live until each is ended by its own token. Secrets registered with
an explicit ``exec_id`` land in that scope (so a worker thread can register into
a parent execution's scope without context propagation); without one they land
in the ambient context's scope, or the ``__global__`` catch-all if the ambient
context opened no scope.

Concurrency: copy-on-write. Writers (register/begin/end/clear) hold the lock and
atomically swap in freshly-built dicts; readers (the masking hot path) read those
immutable snapshots without a lock. Registration publishes incrementally (merges
only the new secret's expanded keys into a new dict) so n admits are O(n) at C
speed rather than O(n^2) at Python speed; the full rebuild is reserved for
removals (end_execution/clear), which are rare.
"""

import json
import os
import threading
import uuid
from contextvars import ContextVar
from dataclasses import dataclass
from enum import Enum, auto
from typing import Dict, FrozenSet, Optional, Tuple

from datahub.masking.logging_utils import get_masking_safe_logger

logger = get_masking_safe_logger(__name__)

# Secrets registered outside an explicit execution scope land here. Dropped when
# no real executions remain active.
_GLOBAL_GROUP = "__global__"

# Identifies the execution owning the current context's ambient scope.
# Set by begin_execution(), read by register_secret() when no explicit exec_id
# is passed, and cleared by end_execution() when it ends the ambient scope.
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


@dataclass
class _GroupEntry:
    """One secret within an execution's group.

    ``expanded`` caches ``_expand_keys(raw_value)`` computed once at admit
    time so removals (which rebuild from scratch) and the incremental publish
    path never re-expand. Tying the cache to the entry (not a process-wide
    cache) bounds it to the secret's lifetime and keeps the registry's
    memory reclaimable when executions end.
    """

    name: str
    expanded: Dict[str, None]


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
    # variants per secret), not the number of registered secrets. The admit
    # check rejects when ``len(_secrets) + pending + truly_new >= MAX_SECRETS``,
    # so the effective capacity is MAX_SECRETS - 1 (the >= leaves no room for
    # the last admit). Keep this bound generous because the masking regex is
    # built from these keys.
    MAX_SECRETS = 10000

    def __init__(self):
        # Source of truth: execution_id -> {raw_value: _GroupEntry}.
        self._groups: Dict[str, Dict[str, _GroupEntry]] = {}
        # Derived, immutable snapshots for the masking hot path: the union of all
        # groups (key -> name) and a reverse index (name -> value). Never mutated
        # in place — rebuilt and atomically reassigned by writers (COW).
        self._secrets: Dict[str, str] = {}
        self._name_to_value: Dict[str, str] = {}
        self._version = 0
        # Serializes writers only; readers are lock-free.
        self._registry_lock = threading.RLock()
        # True once we've warned about being at capacity; cleared by _rebuild_locked
        # when the registry drops below capacity so a later rise can warn again.
        # _warn_capacity is the single reader/setter; nothing else touches it.
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

        ``_bootstrap_lock`` is an ``RLock``. ``shutdown_secret_masking()``
        establishes the lock order ``_bootstrap_lock`` -> ``SecretRegistry._lock``
        (it takes ``_bootstrap_lock`` first, then calls into the registry).
        ``reset_instance`` calls ``reset_bootstrap_state()`` (which acquires
        ``_bootstrap_lock``) *after* releasing ``cls._lock``, so it does not
        invert that order. Using an ``RLock`` here means a future path that
        reaches ``reset_instance`` from inside a ``_bootstrap_lock`` region
        re-enters the same lock rather than self-deadlocking; a plain
        ``Lock`` would deadlock in that position. The ``cls._lock`` region
        is exited before the ``_bootstrap_lock`` call so the two locks are
        never held simultaneously here.
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
        """Open a new secret scope and return its id.

        Always opens a *distinct* scope — two ``initialize_secret_masking()``
        calls on the same context return different tokens and own different
        groups, so ending one never drops the other's secrets. The ambient
        contextvar is set to this scope; a later ``begin_execution`` on the
        same context overwrites it (the latest scope owns ambient
        registrations), but the earlier token stays valid for its own
        token-based ``end_execution``.
        """
        exec_id = uuid.uuid4().hex
        with self._registry_lock:
            self._groups.setdefault(exec_id, {})
        _current_exec.set(exec_id)
        return exec_id

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

    def _current_group_locked(self, exec_id: Optional[str]) -> Dict[str, _GroupEntry]:
        if exec_id is not None:
            return self._groups.setdefault(exec_id, {})
        ambient = _current_exec.get()
        if ambient is not None and ambient in self._groups:
            return self._groups[ambient]
        return self._groups.setdefault(_GLOBAL_GROUP, {})

    def _rebuild_locked(self) -> None:
        """Recompute the union + reverse index from all groups and publish them
        atomically. Called only by writers holding the lock, on removals
        (end_execution/clear). Admits use the incremental _publish_incremental
        path instead, so this O(n) walk runs only when secrets actually leave
        the registry — not on every admit.
        """
        secrets: Dict[str, str] = {}
        name_to_value: Dict[str, str] = {}
        for group in self._groups.values():
            for raw_value, entry in group.items():
                name_to_value.setdefault(entry.name, raw_value)
                for key in entry.expanded:
                    secrets.setdefault(key, entry.name)
        self._secrets = secrets
        self._name_to_value = name_to_value
        self._version += 1
        # The admit check rejects on >= MAX_SECRETS, so the union never reaches
        # MAX_SECRETS — this is always False after a rebuild. Clearing here is
        # belt-and-braces: if a future path sets _capacity_warned without going
        # through _warn_capacity, dropping below capacity still self-corrects.
        # Effective capacity is MAX_SECRETS - 1 (the >= leaves no room for the
        # last admit); a batch that fills to exactly MAX_SECRETS - 1 does not
        # trip the warning, which is correct (it fit).
        self._capacity_warned = len(self._secrets) >= self.MAX_SECRETS

    def _publish_incremental(
        self,
        new_keys: Dict[str, str],
        new_name_to_value: Dict[str, str],
    ) -> None:
        """Publish only the newly-admitted keys by merging into fresh dicts.

        COW contract: readers see an immutable snapshot, so we build a new
        dict (``{**old, **new}``) and swap it in atomically (pointer
        reassignment under the GIL). The merge is O(len(old) + len(new)) at C
        speed, so n admits are O(n) total — matching the fork-point
        implementation. The full O(n) walk (_rebuild_locked) is reserved for
        removals, which are rare. ``new_keys`` are only the truly-new expanded
        keys (already deduped against ``_secrets`` by _admit_locked), so the
        merge never overwrites an existing key — first registration wins the
        name, matching _rebuild_locked's setdefault semantics.
        """
        self._secrets = {**self._secrets, **new_keys}
        self._name_to_value = {**self._name_to_value, **new_name_to_value}
        self._version += 1

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
        group: Dict[str, _GroupEntry],
        pending_keys: set,
    ) -> Tuple[_Admit, FrozenSet[str]]:
        """Admit one secret against the registry, capacity, and batch state.

        Called by register_secret / register_secrets_batch with the
        registry lock held. Returns ``(result, truly_new_keys)``:

        - DUPLICATE: the raw value is already in this execution's group, or
          already pending in the current batch (dedup across the batch).
          Not a warning condition — duplicate registration is common.
        - REJECTED: admitting would push the *expanded* key count
          (``len(self._secrets)``) to >= MAX_SECRETS. The caller is
          responsible for warning via ``_warn_capacity``; this method does
          not touch ``_capacity_warned`` so the gate stays in one place.
          The capacity bound is on expanded keys (raw + repr + sqlalchemy +
          json variants), not on the number of registered secrets — counting
          secrets instead let a batch of multi-key-expanding values overshoot
          MAX_SECRETS.
        - ADMITTED: the secret was added to ``group``; ``truly_new_keys`` is
          the set of expanded keys not already in ``_secrets`` or
          ``pending_keys`` (the caller publishes them).

        ``pending_keys`` is mutated (unioned with truly_new) so the batch
        loop dedup-counts within the batch. The single path passes a
        throwaway ``set()``, so the mutation is harmless; the batch path
        passes a persistent set so successive calls see prior admits.
        """
        if raw_value in group or raw_value in pending_keys:
            return _Admit.DUPLICATE, frozenset()
        keys = _expand_keys(raw_value)
        truly_new = frozenset(set(keys) - self._secrets.keys() - pending_keys)
        if len(self._secrets) + len(pending_keys) + len(truly_new) >= self.MAX_SECRETS:
            return _Admit.REJECTED, frozenset()
        group[raw_value] = _GroupEntry(name=variable_name, expanded=keys)
        pending_keys |= truly_new
        return _Admit.ADMITTED, truly_new

    def register_secret(
        self,
        variable_name: str,
        raw_value: str,
        exec_id: Optional[str] = None,
    ) -> None:
        """Register a secret for masking.

        With ``exec_id`` (the token returned by ``initialize_secret_masking``),
        the secret lands in that execution's scope — this is how a worker
        thread registers into a parent execution's scope, since ContextVars
        do not cross thread boundaries. Without ``exec_id``, the secret lands
        in the ambient context's scope (set by ``begin_execution``); if the
        ambient context opened no scope it lands in the ``__global__`` catch-all
        bucket, which is dropped only when no real executions remain — so in
        a long-lived worker that registers from a non-execution context, those
        secrets accumulate for the process lifetime (fails safe: over-masking,
        never leaking). Pass the parent execution's ``exec_id`` to get
        per-execution cleanup across threads.
        """
        if not raw_value or not isinstance(raw_value, str):
            return
        if len(raw_value) < 3:
            return

        with self._registry_lock:
            group = self._current_group_locked(exec_id)
            result, truly_new = self._admit_locked(
                variable_name, raw_value, group, set()
            )
            if result is _Admit.ADMITTED:
                self._publish_incremental(
                    {k: variable_name for k in truly_new},
                    {variable_name: raw_value},
                )
                logger.debug(
                    f"Registered secret: {variable_name[:8]}*** (version {self._version})"
                )
            elif result is _Admit.REJECTED:
                self._warn_capacity(
                    f"Secret registry at capacity ({self.MAX_SECRETS}). "
                    f"Skipping registration of {variable_name}"
                )

    def register_secrets_batch(
        self,
        secrets: Dict[str, str],
        exec_id: Optional[str] = None,
    ) -> None:
        """Register multiple secrets atomically under one execution scope."""
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
            group = self._current_group_locked(exec_id)
            pending_keys: set = set()
            admitted = 0
            duplicates = 0
            rejected = 0
            new_keys: Dict[str, str] = {}
            new_name_to_value: Dict[str, str] = {}
            for variable_name, raw_value in valid_secrets.items():
                result, truly_new = self._admit_locked(
                    variable_name, raw_value, group, pending_keys
                )
                if result is _Admit.ADMITTED:
                    admitted += 1
                    new_keys.update({k: variable_name for k in truly_new})
                    new_name_to_value[variable_name] = raw_value
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
                self._publish_incremental(new_keys, new_name_to_value)
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
