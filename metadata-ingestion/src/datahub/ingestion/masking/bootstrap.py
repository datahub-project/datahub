"""
Bootstrap module for secret masking initialization.
"""

import logging
import os
import sys
import threading
from enum import Enum
from typing import List, Optional

from datahub.ingestion.masking.logging_utils import get_masking_safe_logger
from datahub.ingestion.masking.masking_filter import (
    SecretMaskingFilter,
    install_masking_filter,
    uninstall_masking_filter,
)
from datahub.ingestion.masking.secret_registry import SecretRegistry

logger = get_masking_safe_logger(__name__)

# Bootstrap state tracking
_bootstrap_completed = False
_bootstrap_error: Optional[Exception] = None
_original_excepthook = None  # Track original exception hook for restoration
_bootstrap_lock = threading.Lock()  # Thread safety for concurrent initialization


class ExecutionContext(Enum):
    """Execution context for DataHub ingestion."""

    CLI = "cli"
    UI_BACKEND = "ui_backend"
    REMOTE_EXECUTOR = "remote"
    SCHEDULED = "scheduled"
    UNKNOWN = "unknown"


def is_bootstrapped() -> bool:
    """
    Check if secret masking bootstrap has completed.

    Returns:
        True if bootstrap completed successfully
    """
    return _bootstrap_completed


def get_bootstrap_error() -> Optional[Exception]:
    """
    Get bootstrap error if bootstrap failed.

    Returns:
        Exception if bootstrap failed, None otherwise
    """
    return _bootstrap_error


def detect_execution_context() -> ExecutionContext:
    """
    Detect how DataHub is being executed.

    Returns:
        ExecutionContext enum value
    """
    # Check for explicit context marker
    context_env = os.environ.get("DATAHUB_EXECUTION_CONTEXT")
    if context_env:
        try:
            return ExecutionContext(context_env)
        except ValueError:
            pass

    # Detect UI backend
    if os.environ.get("DATAHUB_UI_INGESTION") == "1":
        return ExecutionContext.UI_BACKEND

    # Detect remote executor
    if os.environ.get("DATAHUB_EXECUTOR_ID"):
        return ExecutionContext.REMOTE_EXECUTOR

    # Detect scheduled job
    if os.environ.get("DATAHUB_SCHEDULED_JOB") == "1":
        return ExecutionContext.SCHEDULED

    # Default to CLI
    return ExecutionContext.CLI


def _install_exception_hook(registry: SecretRegistry) -> None:
    """
    Install custom exception hook to mask secrets in unhandled exceptions.

    This ensures that if an exception with secrets reaches sys.excepthook,
    it gets masked before being printed to stderr.

    Args:
        registry: SecretRegistry instance for masking
    """
    global _original_excepthook

    # Store original exception hook for later restoration
    if _original_excepthook is None:
        _original_excepthook = sys.excepthook

    original_excepthook = _original_excepthook

    masking_filter = SecretMaskingFilter(registry)

    def masking_excepthook(exc_type, exc_value, exc_traceback):
        """
        Custom exception hook that masks secrets in exception messages.
        """
        try:
            # Mask the exception args
            if exc_value and hasattr(exc_value, "args") and exc_value.args:
                # Mask each arg
                masked_args = tuple(
                    masking_filter._mask_text(arg) if isinstance(arg, str) else arg
                    for arg in exc_value.args
                )

                # Create new exception with masked args
                exc_value = type(exc_value)(*masked_args)
        except Exception as e:
            # If masking fails, log error but continue with original exception
            logger.error(f"Failed to mask exception: {e}")

        # Call original exception hook with (potentially) masked exception
        original_excepthook(exc_type, exc_value, exc_traceback)

    # Install the custom hook
    sys.excepthook = masking_excepthook
    logger.debug("Installed custom exception hook for secret masking")


def initialize_secret_masking(
    context: Optional[ExecutionContext] = None,
    secret_sources: Optional[List[str]] = None,
    max_message_size: int = 5000,
    force: bool = False,
) -> None:
    """
    Initialize secret masking for any execution context.

    This function:
    1. Detects execution context (or uses provided)
    2. Loads secrets from appropriate sources
    3. Installs logging filter
    4. Installs stdout wrapper as backup

    Thread-safe: Uses lock to prevent concurrent initialization.

    Args:
        context: Execution context (auto-detected if None)
        secret_sources: List of sources to load (auto-detected if None)
        max_message_size: Maximum log message size before truncation
        force: Force re-initialization even if already initialized
    """
    global _bootstrap_completed, _bootstrap_error

    # Thread-safe initialization: acquire lock for entire operation
    with _bootstrap_lock:
        # Prevent double initialization
        if _bootstrap_completed and not force:
            logger.debug("Secret masking already initialized")
            return

        try:
            # Detect context
            if context is None:
                context = detect_execution_context()

            logger.info(f"Initializing secret masking for context: {context.value}")

            # Get registry
            registry = SecretRegistry.get_instance()

            # Load secrets from appropriate sources
            _load_secrets_for_context(context, registry, secret_sources)

            # Install logging filter + stdout wrapper
            install_masking_filter(
                secret_registry=registry,
                max_message_size=max_message_size,
                install_stdout_wrapper=True,
            )

            # Install custom exception hook to mask unhandled exceptions
            _install_exception_hook(registry)

            # Configure warnings to use logging
            logging.captureWarnings(True)

            # Disable HTTP debug output (prevent deadlock)
            try:
                import http.client

                http.client.HTTPConnection.debuglevel = 0
            except Exception:
                pass

            # Set HTTP-related loggers to INFO (not DEBUG)
            for logger_name in [
                "urllib3",
                "urllib3.connectionpool",
                "urllib3.util.retry",
                "requests",
            ]:
                try:
                    logging.getLogger(logger_name).setLevel(logging.INFO)
                except Exception:
                    pass

            _bootstrap_completed = True
            _bootstrap_error = None
            secret_count = registry.get_count()
            logger.info(
                f"Secret masking initialized successfully for {context.value} "
                f"with {secret_count} secrets registered"
            )

        except Exception as e:
            _bootstrap_error = e
            logger.error(f"Failed to initialize secret masking: {e}", exc_info=True)
            # Don't raise - graceful degradation


def _load_secrets_for_context(
    context: ExecutionContext,
    registry: SecretRegistry,
    explicit_sources: Optional[List[str]] = None,
) -> None:
    """
    Load secrets from appropriate sources based on context.

    Args:
        context: Execution context
        registry: SecretRegistry instance
        explicit_sources: Optional list of sources to load
    """
    # Determine sources
    sources = explicit_sources if explicit_sources else _get_default_sources(context)

    logger.debug(f"Loading secrets from sources: {sources}")

    for source in sources:
        try:
            if source == "environment":
                _load_environment_secrets(registry)
            elif source == "datahub_secrets_store":
                # TODO: Implement when backend API available
                logger.debug("DataHub secrets store not yet implemented")
            else:
                logger.warning(f"Unknown secret source: {source}")
        except Exception as e:
            logger.warning(f"Failed to load secrets from {source}: {e}")


def _get_default_sources(context: ExecutionContext) -> List[str]:
    """
    Determine secret sources based on execution context.

    All contexts use environment variables as the base, since DataHub
    substitutes ${VAR} with os.environ['VAR'] in recipe files.

    Args:
        context: Execution context

    Returns:
        List of source names to load
    """
    # Base: All contexts mask environment variables
    sources = ["environment"]

    # Additional: UI and remote may use DataHub secrets store in future
    if context in [ExecutionContext.UI_BACKEND, ExecutionContext.REMOTE_EXECUTOR]:
        # TODO: Implement datahub_secrets_store loading when backend API is available
        # sources.append("datahub_secrets_store")
        pass

    return sources


# Environment variables that should NOT be masked (common system variables)
#
# Note: Never add variables containing secrets, passwords, keys, or tokens.
#       Examples to NEVER add: AWS_ACCESS_KEY_ID, DATABASE_PASSWORD, API_KEY,
#       SECRET_KEY. If unsure, don't add it.
_SYSTEM_ENV_VARS = {
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
    # Claude Code
    "CLAUDE_CODE_ENTRYPOINT",
    # Telemetry
    "OTEL_EXPORTER_OTLP_ENDPOINT",
}


def _load_environment_secrets(registry: SecretRegistry) -> None:
    """
    Load environment variables for masking, excluding common system variables.

    Security approach: Mask environment variables that might contain secrets,
    but exclude common system variables (paths, shell config, etc.) to keep
    error messages and tracebacks readable.

    Rationale:
    - DataHub substitutes ${VAR} with os.environ['VAR'] in recipes
    - We mask env vars that might contain secrets (tokens, passwords, keys)
    - We exclude common system vars (HOME, PATH, PWD, etc.) for usability
    - Users can still use ${PASSWORD}, ${API_KEY}, etc. safely

    Args:
        registry: SecretRegistry instance to register secrets
    """
    secrets_loaded = 0
    skipped_count = 0

    for key, value in os.environ.items():
        # Skip system environment variables
        if key in _SYSTEM_ENV_VARS:
            skipped_count += 1
            continue

        # Validate value
        if not value or not isinstance(value, str):
            continue

        # Skip very short values (likely not secrets)
        if len(value) < 3:
            continue

        # Register for masking
        registry.register_secret(key, value)
        secrets_loaded += 1

    logger.info(
        f"Registered {secrets_loaded} environment variables for masking "
        f"(skipped {skipped_count} system variables)"
    )


def shutdown_secret_masking() -> None:
    """
    Shutdown secret masking system.

    Used primarily for testing or cleanup.
    """
    global _bootstrap_completed, _bootstrap_error, _original_excepthook

    try:
        uninstall_masking_filter()

        # Restore original exception hook
        if _original_excepthook is not None:
            sys.excepthook = _original_excepthook
            _original_excepthook = None

        # Clear registry
        registry = SecretRegistry.get_instance()
        registry.clear()

        _bootstrap_completed = False
        _bootstrap_error = None

        logger.info("Secret masking shutdown completed")
    except Exception as e:
        logger.error(f"Error during secret masking shutdown: {e}")
