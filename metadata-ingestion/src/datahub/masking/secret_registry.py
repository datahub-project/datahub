import os
import threading
from typing import Any, Dict, List, Optional, Set, Tuple

from datahub.masking.constants import REDACTED_PREFIX
from datahub.masking.logging_utils import get_masking_safe_logger

logger = get_masking_safe_logger(__name__)

MIN_SECRET_LENGTH = 3
MIN_FRAGMENT_LENGTH = 8
MAX_SECRET_VERSIONS = 3
LARGE_SECRET_RENDERING_COUNT = 200

_UNMASKABLE_LITERALS = frozenset({"true", "false", "yes", "no", "none", "null"})
_ESCAPABLE_CHARACTERS = ("\n", "\r", "\t", "\\", '"', "'")


def is_masking_enabled() -> bool:
    return os.getenv("DATAHUB_DISABLE_SECRET_MASKING", "").lower() not in (
        "true",
        "1",
    )


def _is_maskable_type(value: Any) -> bool:
    return bool(value) and isinstance(value, str) and len(value) >= MIN_SECRET_LENGTH


def _unprotectable_reason(value: str) -> Optional[str]:
    if value.strip().lower() in _UNMASKABLE_LITERALS:
        return "a common literal; masking it would redact ordinary log text"
    if REDACTED_PREFIX in value:
        return "shaped like a redaction marker; masking it would corrupt masked output"
    return None


def _escaped_rendering(value: str) -> Optional[str]:
    if not any(c in value for c in _ESCAPABLE_CHARACTERS):
        return None
    escaped = repr(value)[1:-1]
    return escaped if escaped != value else None


def _url_encoded_rendering(value: str) -> Optional[str]:
    encoded = value.replace(":", "%3A").replace("@", "%40").replace("/", "%2F")
    return encoded if encoded != value else None


def _line_fragments(value: str) -> List[str]:
    lines = value.splitlines()
    if lines == [value]:
        return []
    fragments = []
    for line in lines:
        fragment = line.strip()
        if (
            len(fragment) >= MIN_FRAGMENT_LENGTH
            and _unprotectable_reason(fragment) is None
        ):
            fragments.append(fragment)
    return fragments


def maskable_renderings(value: str) -> List[str]:
    """All forms of a secret worth matching: the value itself, each substantial
    line of a multi-line value, and escaped / URL-encoded variants of each."""
    renderings: List[str] = []
    for text in [value, *_line_fragments(value)]:
        for rendering in (
            text,
            _escaped_rendering(text),
            _url_encoded_rendering(text),
        ):
            if rendering is not None and rendering not in renderings:
                renderings.append(rendering)
    return renderings


def _evict_renderings(
    secrets: Dict[str, str],
    history: Dict[str, List[str]],
    evicted_values: List[str],
) -> int:
    """Remove renderings of evicted values, sparing any rendering still
    produced by a value retained under some name."""
    if not evicted_values:
        return 0
    retained: Set[str] = set()
    for values in history.values():
        for value in values:
            retained.update(maskable_renderings(value))
    removed = 0
    for value in evicted_values:
        for rendering in maskable_renderings(value):
            if rendering not in retained and rendering in secrets:
                del secrets[rendering]
                removed += 1
    return removed


class SecretRegistry:
    """Thread-safe store of secret values to mask.

    Keyed by value: re-registering a name with a new value keeps the last
    MAX_SECRET_VERSIONS values maskable, so recently rotated secrets stay
    covered while the registry stays bounded. Exceeding MAX_SECRETS total
    renderings fails closed: filters suppress all output instead of letting
    an unregistered secret through.
    """

    _instance: Optional["SecretRegistry"] = None
    _lock = threading.RLock()

    MAX_SECRETS = 10000

    def __init__(self) -> None:
        self._secrets: Dict[str, str] = {}
        self._name_history: Dict[str, List[str]] = {}
        self._version = 0
        self._capacity_exceeded = False
        self._registry_lock = threading.RLock()

    @classmethod
    def get_instance(cls) -> "SecretRegistry":
        with cls._lock:
            if cls._instance is None:
                cls._instance = cls()
            return cls._instance

    @classmethod
    def reset_instance(cls) -> None:
        with cls._lock:
            cls._instance = None

    def register_secret(self, variable_name: str, raw_value: str) -> None:
        self.register_secrets_batch({variable_name: raw_value})

    def register_secrets_batch(self, secrets: Dict[str, str]) -> None:
        if not is_masking_enabled():
            return

        accepted: Dict[str, str] = {}
        for name, value in secrets.items():
            if not _is_maskable_type(value):
                continue
            reason = _unprotectable_reason(value)
            if reason is not None:
                logger.warning(f"Secret '{name}' is {reason}; it will NOT be masked")
                continue
            accepted[name] = value

        if not accepted:
            return

        if all(self._is_current(name, value) for name, value in accepted.items()):
            return

        with self._registry_lock:
            new_secrets = self._secrets.copy()
            new_history = {
                name: list(values) for name, values in self._name_history.items()
            }

            new_values: List[Tuple[str, str]] = []
            evicted_values: List[str] = []
            for name, value in accepted.items():
                history = new_history.setdefault(name, [])
                if value in history:
                    if history[-1] != value:
                        history.remove(value)
                        history.append(value)
                    continue
                history.append(value)
                evicted_values.extend(history[:-MAX_SECRET_VERSIONS])
                del history[:-MAX_SECRET_VERSIONS]
                new_values.append((name, value))

            removed_count = _evict_renderings(new_secrets, new_history, evicted_values)
            added_count = self._add_renderings(new_secrets, new_values)

            if added_count or removed_count:
                self._secrets = new_secrets
                self._version += 1
                logger.debug(
                    f"Registered {added_count} and evicted {removed_count} "
                    f"maskable value(s) (version {self._version})"
                )
            if new_history != self._name_history:
                self._name_history = new_history

    def _is_current(self, name: str, value: str) -> bool:
        history = self._name_history.get(name)
        return history is not None and history[-1] == value and value in self._secrets

    def _add_renderings(
        self, secrets: Dict[str, str], new_values: List[Tuple[str, str]]
    ) -> int:
        added = 0
        for name, value in new_values:
            renderings = maskable_renderings(value)
            if len(renderings) >= LARGE_SECRET_RENDERING_COUNT:
                logger.warning(
                    f"Secret '{name}' is unusually large: {len(value)} characters "
                    f"producing {len(renderings)} maskable renderings "
                    f"(registry at {len(secrets)}/{self.MAX_SECRETS})"
                )
            for rendering in renderings:
                if rendering in secrets:
                    continue
                if len(secrets) >= self.MAX_SECRETS:
                    self._declare_capacity_exceeded(name)
                    return added
                secrets[rendering] = name
                added += 1
        return added

    def _declare_capacity_exceeded(self, name: str) -> None:
        if self._capacity_exceeded:
            return
        self._capacity_exceeded = True
        logger.critical(
            f"CRITICAL: Secret registry capacity ({self.MAX_SECRETS}) exceeded "
            f"while registering '{name}'. All maskable output will be suppressed "
            f"to avoid leaking unprotected secrets; reduce the number or size of "
            f"configured secrets and restart the process to recover."
        )

    def is_capacity_exceeded(self) -> bool:
        return self._capacity_exceeded

    def get_all_secrets(self) -> Dict[str, str]:
        with self._registry_lock:
            return self._secrets.copy()

    def get_registered_secrets(self) -> Dict[str, str]:
        with self._registry_lock:
            return {name: values[-1] for name, values in self._name_history.items()}

    def get_version(self) -> int:
        with self._registry_lock:
            return self._version

    def get_count(self) -> int:
        return len(self._secrets)

    def clear(self) -> None:
        with self._registry_lock:
            self._secrets = {}
            self._name_history = {}
            self._capacity_exceeded = False
            self._version += 1
            logger.debug("Cleared all secrets from registry")

    def has_secret(self, variable_name: str) -> bool:
        with self._registry_lock:
            return variable_name in self._name_history

    def get_secret_value(self, variable_name: str) -> Optional[str]:
        history = self._name_history.get(variable_name)
        return history[-1] if history else None
