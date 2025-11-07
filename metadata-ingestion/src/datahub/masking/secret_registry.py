"""
Thread-safe registry for managing secrets used in masking.

This module provides a singleton registry for storing and managing secrets.
Secrets are automatically registered from ConfigModel fields and environment
variables, then used by logging filters to mask sensitive data.

"""

import os
import re
import threading
from typing import Dict, Optional, Set

from datahub.masking.logging_utils import get_masking_safe_logger

logger = get_masking_safe_logger(__name__)

# Environment variables that should NOT be masked (common system variables)
#
# Maintenance Guidelines:
#   - This list is intentionally hardcoded for common cross-platform variables
#   - For custom variables, users should use DATAHUB_MASKING_ENV_VARS_SKIP_LIST
#     or DATAHUB_MASKING_ENV_VARS_SKIP_PATTERN (see should_mask_env_var below)
#   - Only add variables that are:
#     * Non-secret (paths, IDs, flags)
#     * Widely used across environments
#     * Documented in shell/OS documentation
#
# Security Policy:
#   - NEVER add variables containing secrets, passwords, keys, or tokens
#   - Examples to NEVER add: AWS_ACCESS_KEY_ID, DATABASE_PASSWORD, API_KEY
#   - If unsure, don't add it - users can customize via skip list/pattern
#   - Better to mask too much than too little
SYSTEM_ENV_VARS: Set[str] = {
    # Paths
    "HOME",
    "PWD",
    "OLDPWD",
    "PATH",
    "TMPDIR",
    "TEMP",
    "TMP",
    "VIRTUAL_ENV",
    # Shell and Terminal
    "SHELL",
    "TERM",
    "TERM_PROGRAM",
    "TERM_PROGRAM_VERSION",
    "TERM_SESSION_ID",
    "COLORTERM",
    "CLICOLOR",
    "CLICOLOR_FORCE",
    # Locale and Language
    "LANG",
    "LANGUAGE",
    "LC_ALL",
    "LC_CTYPE",
    "LC_MESSAGES",
    # Display and Graphics
    "DISPLAY",
    "WAYLAND_DISPLAY",
    # User Info
    "USER",
    "USERNAME",
    "LOGNAME",
    "UID",
    # System Info
    "HOSTNAME",
    "HOSTTYPE",
    "OSTYPE",
    "MACHTYPE",
    # Development Tools
    "EDITOR",
    "VISUAL",
    "PAGER",
    "LESS",
    "GIT_EDITOR",
    # Python-specific
    "PYTHONPATH",
    "PYTHONHOME",
    "PYTHONIOENCODING",
    "PYTHONUNBUFFERED",
    "PYTHONDONTWRITEBYTECODE",
    # Java
    "JAVA_HOME",
    "JAVA_OPTS",
    # Node/NPM
    "NODE_ENV",
    "NVM_DIR",
    "NPM_CONFIG_PREFIX",
    # Build Tools
    "GRADLE_HOME",
    "MAVEN_HOME",
    "M2_HOME",
    # Package Managers
    "HOMEBREW_PREFIX",
    "HOMEBREW_CELLAR",
    "HOMEBREW_REPOSITORY",
    # Version Managers
    "ASDF_DIR",
    "ASDF_DATA_DIR",
    "PYENV_ROOT",
    "RBENV_ROOT",
    # Color/Display Settings
    "LSCOLORS",
    "LS_COLORS",
    "GREP_COLOR",
    "GREP_COLORS",
    # Misc System
    "SHLVL",
    "PS1",
    "PS2",
    "IFS",
    "MANPATH",
    "INFOPATH",
    # macOS Specific
    "__CF_USER_TEXT_ENCODING",
    "__CFBundleIdentifier",
    "XPC_FLAGS",
    "XPC_SERVICE_NAME",
    # SSH (paths only, not keys)
    "SSH_AUTH_SOCK",  # Socket path, not a secret
    # ZSH
    "ZSH",
    "ZSH_VERSION",
    # DataHub Debug (not secrets)
    "DATAHUB_DEBUG",
    "DATAHUB_LOG_LEVEL",
    "DATAHUB_DISABLE_SECRET_MASKING",
    "DATAHUB_MASKING_ENV_VARS_SKIP_LIST",
    "DATAHUB_MASKING_ENV_VARS_SKIP_PATTERN",
    # Claude Code
    "CLAUDE_CODE_ENTRYPOINT",
    # Telemetry
    "OTEL_EXPORTER_OTLP_ENDPOINT",
}


def _parse_skip_list() -> Set[str]:
    """Parse comma-separated skip list from DATAHUB_MASKING_ENV_VARS_SKIP_LIST."""
    skip_list_env = os.getenv("DATAHUB_MASKING_ENV_VARS_SKIP_LIST", "")
    if not skip_list_env:
        return set()
    return {v.strip() for v in skip_list_env.split(",") if v.strip()}


def _parse_skip_pattern() -> Optional[re.Pattern]:
    """Parse regex pattern from DATAHUB_MASKING_ENV_VARS_SKIP_PATTERN."""
    skip_pattern_env = os.getenv("DATAHUB_MASKING_ENV_VARS_SKIP_PATTERN", "")
    if not skip_pattern_env:
        return None
    try:
        return re.compile(skip_pattern_env)
    except re.error as e:
        logger.warning(
            f"Invalid regex in DATAHUB_MASKING_ENV_VARS_SKIP_PATTERN: {e}. "
            f"Pattern will be ignored."
        )
        return None


# Cached at module load for performance
_USER_SKIP_LIST: Set[str] = _parse_skip_list()
_USER_SKIP_PATTERN: Optional[re.Pattern] = _parse_skip_pattern()


def _refresh_env_var_filters() -> None:
    """Refresh cached environment variable filters."""
    global _USER_SKIP_LIST, _USER_SKIP_PATTERN
    _USER_SKIP_LIST = _parse_skip_list()
    _USER_SKIP_PATTERN = _parse_skip_pattern()


def should_mask_env_var(var_name: str) -> bool:
    """Check if environment variable should be masked."""
    if var_name in SYSTEM_ENV_VARS:
        return False

    if var_name in _USER_SKIP_LIST:
        return False

    if _USER_SKIP_PATTERN and _USER_SKIP_PATTERN.match(var_name):
        return False

    return True


def is_masking_enabled() -> bool:
    """Check if masking is enabled."""
    return os.getenv("DATAHUB_DISABLE_SECRET_MASKING", "").lower() not in (
        "true",
        "1",
    )


class SecretRegistry:
    """Thread-safe registry for secrets."""

    _instance: Optional["SecretRegistry"] = None
    _lock = threading.RLock()

    MAX_SECRETS = 10000

    def __init__(self):
        self._secrets: Dict[str, str] = {}  # value -> name mapping
        self._name_to_value: Dict[
            str, str
        ] = {}  # name -> value mapping (reverse index)
        self._version = 0
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
        """Reset singleton instance."""
        with cls._lock:
            cls._instance = None

    def register_secret(self, variable_name: str, raw_value: str) -> None:
        """Register a secret for masking."""
        if not raw_value or not isinstance(raw_value, str):
            return

        if len(raw_value) < 3:
            return

        # Fast path: check without lock
        if raw_value in self._secrets:
            return

        with self._registry_lock:
            # Copy-on-write
            new_secrets = self._secrets.copy()
            new_name_to_value = self._name_to_value.copy()

            # Double-check after acquiring lock
            if raw_value in new_secrets:
                return

            # Check memory limit
            if len(new_secrets) >= self.MAX_SECRETS:
                logger.warning(
                    f"Secret registry at capacity ({self.MAX_SECRETS}). "
                    f"Skipping registration of {variable_name}"
                )
                return

            new_secrets[raw_value] = variable_name
            new_name_to_value[variable_name] = raw_value

            # Register repr() version if secret contains escape sequences
            if any(c in raw_value for c in ["\n", "\r", "\t", "\\", '"', "'"]):
                repr_value = repr(raw_value)[1:-1]
                if repr_value != raw_value and repr_value not in new_secrets:
                    new_secrets[repr_value] = variable_name

            self._secrets = new_secrets
            self._name_to_value = new_name_to_value
            self._version += 1

            logger.debug(
                f"Registered secret: {variable_name[:8]}*** (version {self._version})"
            )

    def register_secrets_batch(self, secrets: Dict[str, str]) -> None:
        """Register multiple secrets atomically."""
        if not secrets:
            return

        # Pre-validate all secrets
        valid_secrets = {
            name: value
            for name, value in secrets.items()
            if value and isinstance(value, str) and len(value) >= 3
        }

        if not valid_secrets:
            return

        # Check fast path - if all secrets already registered, skip
        all_present = all(value in self._secrets for value in valid_secrets.values())
        if all_present:
            return

        # Slow path - batch register
        with self._registry_lock:
            # Copy-on-write
            new_secrets = self._secrets.copy()
            new_name_to_value = self._name_to_value.copy()

            added_count = 0
            for variable_name, raw_value in valid_secrets.items():
                # Skip if already registered
                if raw_value in new_secrets:
                    continue

                # Check memory limit
                if len(new_secrets) >= self.MAX_SECRETS:
                    logger.warning(
                        f"Secret registry at capacity ({self.MAX_SECRETS}). "
                        f"Skipping registration of {variable_name}"
                    )
                    break

                # Register main value
                new_secrets[raw_value] = variable_name
                new_name_to_value[variable_name] = raw_value
                added_count += 1

                # Register repr version if needed
                if any(c in raw_value for c in ["\n", "\r", "\t", "\\", '"', "'"]):
                    repr_value = repr(raw_value)[1:-1]
                    if repr_value != raw_value and repr_value not in new_secrets:
                        new_secrets[repr_value] = variable_name

            if added_count > 0:
                # Atomic swaps - single version increment for entire batch
                self._secrets = new_secrets
                self._name_to_value = new_name_to_value
                self._version += 1

                logger.debug(
                    f"Batch registered {added_count} secrets (version {self._version})"
                )

    def get_all_secrets(self) -> Dict[str, str]:
        """Get all registered secrets."""
        with self._registry_lock:
            return self._secrets.copy()

    def get_version(self) -> int:
        """Get current version."""
        with self._registry_lock:
            return self._version

    def get_count(self) -> int:
        """Get number of registered secrets."""
        return len(self._secrets)

    def clear(self) -> None:
        """Clear all secrets."""
        with self._registry_lock:
            self._secrets = {}
            self._name_to_value = {}
            self._version += 1
            logger.debug("Cleared all secrets from registry")

    def has_secret(self, variable_name: str) -> bool:
        """Check if secret is registered."""
        with self._registry_lock:
            return variable_name in self._name_to_value

    def get_secret_value(self, variable_name: str) -> Optional[str]:
        """Get secret value by name."""
        return self._name_to_value.get(variable_name)
