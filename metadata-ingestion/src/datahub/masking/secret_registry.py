import os
import threading
from typing import Any, Dict, List, Optional

from datahub.masking.constants import REDACTED_PREFIX
from datahub.masking.logging_utils import get_masking_safe_logger

logger = get_masking_safe_logger(__name__)

MIN_SECRET_LENGTH = 3
MIN_FRAGMENT_LENGTH = 8

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
    if "\n" not in value:
        return []
    fragments = []
    for line in value.splitlines():
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


class SecretRegistry:
    """Thread-safe, append-only store of secret values to mask.

    Keyed by value: re-registering a name with a new value keeps the old
    value maskable, so rotated secrets stay covered for the process lifetime.
    """

    _instance: Optional["SecretRegistry"] = None
    _lock = threading.RLock()

    MAX_SECRETS = 10000

    def __init__(self) -> None:
        self._secrets: Dict[str, str] = {}
        self._name_to_value: Dict[str, str] = {}
        self._version = 0
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

        with self._registry_lock:
            new_secrets = self._secrets.copy()
            new_name_to_value = self._name_to_value.copy()

            added_count = 0
            for name, value in accepted.items():
                if len(new_secrets) >= self.MAX_SECRETS:
                    logger.warning(
                        f"Secret registry at capacity ({self.MAX_SECRETS}). "
                        f"Skipping registration of {name}"
                    )
                    break
                for rendering in maskable_renderings(value):
                    if rendering in new_secrets:
                        continue
                    if len(new_secrets) >= self.MAX_SECRETS:
                        logger.warning(
                            f"Secret registry at capacity ({self.MAX_SECRETS}). "
                            f"Some renderings of {name} were not registered"
                        )
                        break
                    new_secrets[rendering] = name
                    added_count += 1
                new_name_to_value[name] = value

            if added_count > 0:
                self._secrets = new_secrets
                self._version += 1
                logger.debug(
                    f"Registered {added_count} maskable value(s) "
                    f"(version {self._version})"
                )
            if new_name_to_value != self._name_to_value:
                self._name_to_value = new_name_to_value

    def get_all_secrets(self) -> Dict[str, str]:
        with self._registry_lock:
            return self._secrets.copy()

    def get_registered_secrets(self) -> Dict[str, str]:
        with self._registry_lock:
            return self._name_to_value.copy()

    def get_version(self) -> int:
        with self._registry_lock:
            return self._version

    def get_count(self) -> int:
        return len(self._secrets)

    def clear(self) -> None:
        with self._registry_lock:
            self._secrets = {}
            self._name_to_value = {}
            self._version += 1
            logger.debug("Cleared all secrets from registry")

    def has_secret(self, variable_name: str) -> bool:
        with self._registry_lock:
            return variable_name in self._name_to_value

    def get_secret_value(self, variable_name: str) -> Optional[str]:
        return self._name_to_value.get(variable_name)
