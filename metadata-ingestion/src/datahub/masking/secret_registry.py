"""
Thread-safe registry for managing secrets used in masking.

Secrets are grouped by the execution that registered them, so overlapping
in-process executions don't interfere: one execution ending removes only its own
secrets, never another's. The masking filter always sees the *union* of all
active executions' secrets (via get_all_secrets/get_version), so masking is
process-global and always-on — it can over-mask during overlap (safe) but never
under-mask. See datahub.masking.bootstrap for the lifecycle.

Concurrency: copy-on-write. Writers (register/begin/end/clear) hold the lock and
atomically swap in freshly-built dicts; readers (the masking hot path) read those
immutable snapshots without a lock.
"""

import os
import threading
import uuid
from contextvars import ContextVar
from typing import Dict, Optional

from datahub.masking.logging_utils import get_masking_safe_logger

logger = get_masking_safe_logger(__name__)

# Secrets registered outside an explicit execution scope land here. Dropped when
# no real executions remain active.
_GLOBAL_GROUP = "__global__"

# Identifies the execution registering/owning secrets in the current context.
# Set by begin_execution(), read by register_secret() and end_execution().
_current_exec: ContextVar[Optional[str]] = ContextVar("masking_exec_id", default=None)


def is_masking_enabled() -> bool:
    """Check if masking is enabled."""
    return os.getenv("DATAHUB_DISABLE_SECRET_MASKING", "").lower() not in (
        "true",
        "1",
    )


def _expand_keys(raw_value: str) -> Dict[str, None]:
    """Return all string forms of a secret that should be masked.

    Besides the raw value, this covers repr-escaped forms (for values with
    escape sequences) and SQLAlchemy-style URL encoding of ``:@/``.
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
    return keys


class SecretRegistry:
    """Thread-safe registry for secrets, scoped per execution (copy-on-write)."""

    _instance: Optional["SecretRegistry"] = None
    _lock = threading.RLock()

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

    @classmethod
    def get_instance(cls) -> "SecretRegistry":
        """Get singleton instance (thread-safe)."""
        with cls._lock:
            if cls._instance is None:
                cls._instance = cls()
            return cls._instance

    @classmethod
    def reset_instance(cls) -> None:
        """Reset singleton instance (and current execution scope)."""
        with cls._lock:
            cls._instance = None
        _current_exec.set(None)

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
        so a repeated initialize_secret_masking() won't start a second scope."""
        exec_id = _current_exec.get()
        if exec_id is not None:
            return exec_id
        return self.begin_execution()

    def end_execution(self) -> bool:
        """Drop the current execution's secrets. Returns True if other
        executions are still active (so the caller should NOT fully tear down)."""
        exec_id = _current_exec.get()
        _current_exec.set(None)
        with self._registry_lock:
            if exec_id is not None:
                self._groups.pop(exec_id, None)
            active = [g for g in self._groups if g != _GLOBAL_GROUP]
            if not active:
                # No real executions left — drop the catch-all bucket too.
                self._groups.pop(_GLOBAL_GROUP, None)
            self._rebuild_locked()
            return bool(active)

    def has_active_executions(self) -> bool:
        with self._registry_lock:
            return any(g != _GLOBAL_GROUP for g in self._groups)

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

    # --- Registration (writers) --------------------------------------------

    def register_secret(self, variable_name: str, raw_value: str) -> None:
        """Register a secret for masking under the current execution."""
        if not raw_value or not isinstance(raw_value, str):
            return
        if len(raw_value) < 3:
            return

        with self._registry_lock:
            group = self._current_group_locked()
            if raw_value in group:
                return
            if len(self._secrets) >= self.MAX_SECRETS:
                logger.warning(
                    f"Secret registry at capacity ({self.MAX_SECRETS}). "
                    f"Skipping registration of {variable_name}"
                )
                return
            group[raw_value] = variable_name
            self._rebuild_locked()
            logger.debug(
                f"Registered secret: {variable_name[:8]}*** (version {self._version})"
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
            added_count = 0
            for variable_name, raw_value in valid_secrets.items():
                if raw_value in group:
                    continue
                if len(self._secrets) + added_count >= self.MAX_SECRETS:
                    logger.warning(
                        f"Secret registry at capacity ({self.MAX_SECRETS}). "
                        f"Skipping registration of {variable_name}"
                    )
                    break
                group[raw_value] = variable_name
                added_count += 1

            if added_count > 0:
                self._rebuild_locked()
                logger.debug(
                    f"Batch registered {added_count} secrets (version {self._version})"
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
