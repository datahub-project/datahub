"""
Thread-safe registry for secrets used in masking.

Uses copy-on-write pattern:
- Secrets dict is replaced atomically (not modified in-place)
- Readers don't need to copy (just get immutable reference)
- Version tracking for efficient change detection
"""

import threading
from typing import Dict, Optional

from datahub.ingestion.masking.logging_utils import get_masking_safe_logger

logger = get_masking_safe_logger(__name__)


class SecretRegistry:
    """
    Thread-safe registry for secrets.

    Uses copy-on-write pattern:
    - Secrets dict is replaced atomically (not modified in-place)
    - Readers don't need to copy (just get immutable reference)
    - Version tracking for efficient change detection

    API Design Philosophy:
        This registry intentionally provides a minimal API focused on
        registration and lookup. Secrets are meant to be registered once
        at initialization and remain immutable during execution.

        For advanced use cases (rotation, batch operations), these can
        be implemented at the application layer using the core primitives.

    Core Operations:
        - register_secret(name, value): Register a single secret
        - has_secret(name): Check if secret exists
        - get_secret_value(name): Retrieve secret value
        - get_all_secrets(): Get all registered secrets
        - clear(): Remove all secrets

    Thread Safety:
        All methods are thread-safe and can be called concurrently
        from multiple threads without external synchronization.
    """

    _instance: Optional["SecretRegistry"] = None
    _lock = threading.RLock()

    def __init__(self):
        """Initialize registry."""
        self._secrets: Dict[str, str] = {}  # value -> name mapping
        self._name_to_value: Dict[
            str, str
        ] = {}  # name -> value mapping (reverse index)
        self._version = 0
        self._registry_lock = threading.RLock()

    @classmethod
    def get_instance(cls) -> "SecretRegistry":
        """
        Get singleton instance (thread-safe).

        Uses simple locking pattern. Lock acquisition is fast enough
        that double-checked locking optimization is unnecessary.

        Returns:
            Singleton SecretRegistry instance
        """
        with cls._lock:
            if cls._instance is None:
                cls._instance = cls()
            return cls._instance

    @classmethod
    def reset_instance(cls) -> None:
        """
        Reset singleton instance.

        Used primarily for testing.
        """
        with cls._lock:
            cls._instance = None

    def register_secret(self, variable_name: str, raw_value: str) -> None:
        """
        Register a secret for masking.

        Uses copy-on-write with double-checked locking optimization.
        Fast path: Check outside lock avoids contention for duplicates.
        Slow path: Check again inside lock before modifying.

        Policy: First registration wins - if value already registered under
        any name, subsequent registrations are ignored.

        Args:
            variable_name: Name for the secret (e.g., "SNOWFLAKE_PASSWORD")
            raw_value: The actual secret value to mask
        """
        # Validation BEFORE any lock operations
        if not raw_value or not isinstance(raw_value, str):
            return

        # Skip very short values (likely not secrets)
        if len(raw_value) < 3:
            return

        # FAST PATH: Check outside lock (no contention!)
        # Safe because:
        # 1. Reading dict reference is atomic in Python
        # 2. COW ensures the dict is immutable
        # 3. We double-check inside the lock anyway
        if raw_value in self._secrets:
            return

        # SLOW PATH: Not found outside lock, need to register
        with self._registry_lock:
            # COPY FIRST (create snapshot)
            new_secrets = self._secrets.copy()
            new_name_to_value = self._name_to_value.copy()

            # CHECK SECOND (on copy) - another thread might have added it
            # while we were waiting for the lock
            if raw_value in new_secrets:
                return

            # MODIFY - actually add the secret
            new_secrets[raw_value] = variable_name
            new_name_to_value[variable_name] = raw_value

            # Also register Python repr() escaped version for traceback masking
            # Example: "pass!!" -> "pass\\!\\!" in tracebacks
            repr_value = repr(raw_value)[1:-1]  # Remove surrounding quotes
            if repr_value != raw_value and repr_value not in new_secrets:
                new_secrets[repr_value] = variable_name
                logger.debug(
                    f"Also registered repr version: {variable_name[:8]}*** (repr)"
                )

            # Atomic swaps
            self._secrets = new_secrets
            self._name_to_value = new_name_to_value
            self._version += 1

            logger.debug(
                f"Registered secret: {variable_name[:8]}*** (version {self._version})"
            )

    def get_all_secrets(self) -> Dict[str, str]:
        """
        Get all registered secrets.

        Returns a copy to prevent external modifications from
        corrupting the registry.

        Returns:
            Dict mapping secret values to variable names (copy)
        """
        with self._registry_lock:
            return self._secrets.copy()

    def get_version(self) -> int:
        """
        Get current version number.

        Version increments whenever secrets are added/removed.
        Used by filters to detect when pattern needs rebuilding.

        Returns:
            Current version number
        """
        with self._registry_lock:
            return self._version

    def get_count(self) -> int:
        """
        Get number of registered secrets.

        No lock needed - COW ensures immutable snapshot.

        Returns:
            Number of secrets in registry
        """
        # No lock needed! Dict reference read is atomic
        return len(self._secrets)

    def clear(self) -> None:
        """
        Clear all secrets.

        Used primarily for testing.
        """
        with self._registry_lock:
            self._secrets = {}
            self._name_to_value = {}
            self._version += 1
            logger.debug("Cleared all secrets from registry")

    def has_secret(self, variable_name: str) -> bool:
        """
        Check if a secret with given name is registered.

        Uses reverse index for O(1) lookup.

        Args:
            variable_name: Name of the secret to check

        Returns:
            True if secret is registered
        """
        with self._registry_lock:
            return variable_name in self._name_to_value

    def get_secret_value(self, variable_name: str) -> Optional[str]:
        """
        Get the actual value of a registered secret by name.

        O(1) lookup using reverse index.
        No lock needed - COW ensures immutable snapshot.

        Args:
            variable_name: Name of the secret

        Returns:
            Secret value if found, None otherwise
        """
        # No lock needed! Dict reference read is atomic
        return self._name_to_value.get(variable_name)
