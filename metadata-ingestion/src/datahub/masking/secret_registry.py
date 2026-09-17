import contextlib
import contextvars
import os
import re
import threading
from typing import Any, Dict, Iterator, List, Optional, Set, Tuple

from datahub.masking.constants import (
    CAPACITY_EXCEEDED_MESSAGE,
    CIRCUIT_OPEN_MESSAGE,
    REDACTED_PREFIX,
    REDACTED_SUFFIX,
    SENTINEL_MESSAGES,
)
from datahub.masking.logging_utils import get_masking_safe_logger

logger = get_masking_safe_logger(__name__)

MIN_SECRET_LENGTH = 3
MIN_FRAGMENT_LENGTH = 8
MAX_SECRET_VERSIONS = 3
LARGE_SECRET_RENDERING_COUNT = 200

_UNMASKABLE_LITERALS = frozenset({"true", "false", "yes", "no", "none", "null"})

# Key fragments that mark a config value as a credential.
SENSITIVE_KEY_HINTS: Tuple[str, ...] = (
    "password",
    "sasl",
    "secret",
    "token",
    "basic.auth.user.info",
    "ssl.key",
    # Key-pair auth (Snowflake) and service-account JSON (GCP) both carry the
    # key under this name, nested one level down (`credential.private_key`), so
    # a top-level SecretStr sweep misses it even though the field is typed.
    "private_key",
    # Names that carry a credential and match none of the above. Each was
    # treated as NON-sensitive, which for the disclosure exemption means
    # "stated in the clear", which means exempt from masking -- so an
    # `api_key` written inline in a recipe reached the caller's output.
    #
    # "passwd" is not a substring of "password", and "api_key" is not a
    # substring of "apikey", so both spellings are listed. Bare "key" is
    # deliberately absent: it would match partition_key, primary_key,
    # key_path and every other structural field.
    #
    # Checked against every registered connector's config before adding:
    # exactly five fields become sensitive that were not -- api_key,
    # aws_access_key_id, cloud_api_key, credential, kafka_api_key -- and all
    # five are credential material. No ordinary field is caught.
    "passwd",
    "api_key",
    "apikey",
    "access_key",
    # NOT "credential". It names a mixed object rather than a scalar secret:
    # BigQuery's `credential` holds private_key -- already matched above --
    # beside project_id, and collect_nested_secret_values has no suffix
    # guard, so the hint swept the project id into the masked set. A project
    # id appears in almost every line of BigQuery output, and masking it
    # corrupts the answer rather than protecting anything.
    # test_a_nested_private_key_is_collected pins this.
)

# A note on why this is a name heuristic at all, since replacing it with
# ConfigModel._collect_secrets' SecretStr set has been suggested twice:
#
#   - These hints run over the RAW recipe dict, before any config class is
#     built, so no field is typed yet. That is the whole point: the window
#     this closes is the one before validation succeeds.
#   - _collect_secrets returns only isinstance(value, SecretStr). None of
#     the three fields that motivated this change -- elasticsearch's
#     api_key, dynamodb's and glue's aws_access_key_id -- is SecretStr
#     typed, so it would not have caught them.
#   - Replacing rather than widening would also LOSE the plain-`str` fields
#     named `password` that the hints catch today.
#
# The typed set is a good second source and ConfigModel already registers
# from it; the two are complementary, not alternatives.


def plain_config_values(
    obj: object, hints: Tuple[str, ...] = SENSITIVE_KEY_HINTS
) -> Set[str]:
    """String values a recipe states in the clear under a non-sensitive key.

    A secret whose resolved value equals one of these cannot be protected by
    masking. The recipe already states the value, consumers legitimately have
    to print it -- a probe verdict's `target` is a qualified identifier, a log
    line is full of ordinary words -- and blanking it is itself what tells a
    reader that the secret equals the identifier they can already see. A
    password of "my_db" otherwise rewrites `my_db_executor.coordinator` into
    `***REDACTED:PW***_executor.coordinator`.

    Same family as _UNMASKABLE_LITERALS and MIN_SECRET_LENGTH above: masking
    that cannot protect anything only corrupts output.

    Callers must read the RAW recipe, not a resolved config. A resolved config
    holds ${ref}-sourced secrets, including under keys no hint matches
    (`options.some_odd_key: ${PW}`), and treating those as disclosed would
    exempt the very values the ${ref} sweep exists to catch. A raw value still
    containing `${` is skipped for the same reason.

    Callers must also subtract what the recipe carries as an inline secret
    literal: a recipe with `password: p` and `database: p` discloses the
    credential itself, and a report travels further than a recipe does.
    """
    found: Set[str] = set()
    if isinstance(obj, dict):
        for k, v in obj.items():
            sensitive = any(h in str(k).lower() for h in hints)
            if isinstance(v, str):
                if v and not sensitive and "${" not in v:
                    found.add(v)
            elif not sensitive:
                # The subtree under a sensitive key is skipped whole. Recursing
                # into it dropped `sensitive`, so the decision was re-made from
                # the CHILD key -- and `token: {access: ...}` was judged by
                # `access`, which no hint matches, so the credential came back
                # "already disclosed" and was exempted from masking everywhere.
                # Nothing under a sensitive key is public, whatever its
                # children are called.
                found |= plain_config_values(v, hints)
    elif isinstance(obj, str):
        # Reached only through the list branch below -- the dict branch
        # handles its own string values inline, and a sensitive subtree is
        # never descended into -- so a string arriving here is one the recipe
        # states in the clear under a plain key.
        #
        # Without this, `schemas: [public]` disclosed nothing while
        # `schema: public` disclosed "public", and the list form is the common
        # one: project_ids, databases, schemas are all lists. A password equal
        # to a project named there was masked everywhere, which is exactly the
        # corruption this exemption exists to prevent.
        if obj and "${" not in obj:
            found.add(obj)
    elif isinstance(obj, list):
        for item in obj:
            found |= plain_config_values(item, hints)
    return found


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


def _compiles(pattern_str: str) -> bool:
    try:
        re.compile(pattern_str)
        return True
    except Exception:
        return False


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

    def __init__(self, _parent: Optional["SecretRegistry"] = None) -> None:
        # Set only for a task-scoped registry (see task_secret_scope), and
        # READ-ONLY: this registry masks against its own secrets plus the
        # parent's, and never writes into it.
        #
        # The first version of the scope had this the other way round -- a
        # write mirror into the global. That put every task's secrets in one
        # shared place, which is precisely why the global could not then be
        # read as a floor: doing so would have shown task B everything task
        # A registered. Reading up and writing down are not interchangeable.
        self._parent = _parent
        self._secrets: Dict[str, str] = {}
        self._name_history: Dict[str, List[str]] = {}
        self._version = 0
        self._capacity_exceeded = False
        self._compile_failed = False
        self._pattern: Optional[re.Pattern] = None
        self._pattern_replacements: Dict[str, str] = {}
        self._pattern_version = -1
        # Combined-with-parent cache; see _combined_with_parent.
        self._combined: Optional[re.Pattern] = None
        self._combined_replacements: Dict[str, str] = {}
        self._combined_key: Optional[Tuple[int, int]] = None
        self._registry_lock = threading.RLock()

    @classmethod
    def get_instance(cls) -> "SecretRegistry":
        """The registry this caller should read and write.

        A task-scoped registry when one is active on this context, otherwise
        the process-global one. See task_secret_scope for why both exist.
        """
        scoped = _active_registry.get()
        if scoped is not None:
            return scoped
        return cls.global_instance()

    @classmethod
    def global_instance(cls) -> "SecretRegistry":
        """The process-global registry, ignoring any active task scope.

        The fail-safe floor: everything registered anywhere reaches this one,
        so a caller that resolves to it can over-mask but never under-mask.
        """
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

    def suppression_message(self) -> Optional[str]:
        """Non-None when masking must fail closed: the fixed message that
        replaces all output."""
        if self._capacity_exceeded:
            return CAPACITY_EXCEEDED_MESSAGE
        if self._compile_failed:
            return CIRCUIT_OPEN_MESSAGE
        return None

    def get_pattern_and_replacements(
        self,
    ) -> Tuple[Optional[re.Pattern], Dict[str, str]]:
        """Compiled masking pattern and rendering-to-name map, rebuilt when
        the registry has changed since the last build. (None, {}) when the
        registry is empty or the pattern is uncompilable."""
        with self._registry_lock:
            if self._pattern_version != self._version:
                self._rebuild_pattern()
            own, replacements = self._pattern, self._pattern_replacements

        if self._parent is None:
            return own, replacements
        return self._combined_with_parent(own, replacements)

    def _combined_with_parent(
        self, own: Optional[re.Pattern], replacements: Dict[str, str]
    ) -> Tuple[Optional[re.Pattern], Dict[str, str]]:
        """This task's secrets plus the process-level ones.

        A task masks against what it was given AND what was registered
        before any task existed -- the envelope secrets load_config_file
        registers, a ConfigModel's own SecretStr fields, the executor's
        startup config. Without the parent those were invisible the moment a
        scope opened, which is a leak rather than an inconvenience.

        Cached on (own version, parent version) so a change on either side
        rebuilds and neither is rebuilt on an unchanged call. Measured at
        1.0-1.1x a single registry for realistic secret counts.
        """
        parent = self._parent
        assert parent is not None
        parent_pattern, parent_replacements = parent.get_pattern_and_replacements()
        if parent_pattern is None:
            return own, replacements
        if own is None:
            return parent_pattern, parent_replacements

        key = (self._version, parent._version)
        with self._registry_lock:
            if self._combined_key != key:
                # Longest-first across BOTH, for the reason _rebuild_pattern
                # sorts: two registered secrets can overlap, and masking the
                # shorter first strands the longer one's tail in the output.
                merged = dict(parent_replacements)
                merged.update(replacements)
                sources = sorted(merged, key=len, reverse=True)
                try:
                    self._combined = re.compile("|".join(re.escape(v) for v in sources))
                except re.error:
                    # Fail closed the way _rebuild_pattern does: mask with
                    # whatever this scope alone can, rather than nothing.
                    self._combined = own
                    merged = replacements
                self._combined_replacements = merged
                self._combined_key = key
            return self._combined, self._combined_replacements

    def _rebuild_pattern(self) -> None:
        self._pattern_version = self._version
        self._pattern = None
        self._pattern_replacements = {}
        if not self._secrets or self._compile_failed:
            return

        sorted_secrets = sorted(
            self._secrets.items(), key=lambda x: len(x[0]), reverse=True
        )

        # CRITICAL: re.escape() ensures secrets with regex metacharacters
        # (e.g., ".*", "a+b", "test|prod") are matched literally, not as regex.
        # The marker alternative comes first so that already-masked spans are
        # consumed whole and never re-matched - this is what makes masking
        # idempotent even when a secret value collides with marker text. Only
        # markers bearing a name the filters could have produced are consumed;
        # a wildcard would let marker-shaped delimiters arriving in untrusted
        # text smuggle a secret through unmasked.
        names = sorted(
            {name for _, name in sorted_secrets} | {"UNKNOWN"},
            key=len,
            reverse=True,
        )
        marker_regex = (
            re.escape(REDACTED_PREFIX)
            + "(?:"
            + "|".join(re.escape(name) for name in names)
            + ")"
            + re.escape(REDACTED_SUFFIX)
        )
        escaped_values = [re.escape(value) for value, _ in sorted_secrets]
        pattern_str = "|".join(
            [
                marker_regex,
                *(re.escape(message) for message in SENTINEL_MESSAGES),
                *escaped_values,
            ]
        )

        try:
            self._pattern = re.compile(pattern_str)
        except Exception as e:
            self._declare_compile_failed(type(e).__name__, sorted_secrets)
            return

        self._pattern_replacements = dict(sorted_secrets)
        if len(sorted_secrets) >= 100:
            logger.warning(
                f"Large number of secrets registered ({len(sorted_secrets)}). "
                f"This may impact masking performance."
            )
        logger.debug(
            f"Rebuilt masking pattern with {len(sorted_secrets)} secrets "
            f"(version {self._version})"
        )

    def _declare_compile_failed(
        self, exception_type: str, sorted_secrets: List[Tuple[str, str]]
    ) -> None:
        self._compile_failed = True
        offending = sorted(
            {name for value, name in sorted_secrets if not _compiles(re.escape(value))}
        )
        if offending:
            detail = f"offending secret(s): {', '.join(offending)}"
        else:
            total_length = sum(len(value) for value, _ in sorted_secrets)
            detail = (
                f"no single secret at fault; combined pattern too large "
                f"({len(sorted_secrets)} renderings, {total_length} characters)"
            )
        logger.error(
            f"Masking pattern failed to compile ({exception_type}); {detail}. "
            f"All output will be suppressed; restart the process to recover."
        )

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
            self._compile_failed = False
            self._version += 1
            self._pattern = None
            self._pattern_replacements = {}
            self._pattern_version = self._version
            logger.debug("Cleared all secrets from registry")

    def has_secret(self, variable_name: str) -> bool:
        with self._registry_lock:
            return variable_name in self._name_history

    def get_secret_value(self, variable_name: str) -> Optional[str]:
        history = self._name_history.get(variable_name)
        return history[-1] if history else None


# The registry the current context should use. A ContextVar rather than a
# thread-local because it is the same mechanism asyncio uses, and because a
# new thread starting from the default is exactly the behaviour the floor
# above is written for.
_active_registry: contextvars.ContextVar[Optional["SecretRegistry"]] = (
    contextvars.ContextVar("datahub_active_secret_registry", default=None)
)


@contextlib.contextmanager
def task_secret_scope() -> Iterator["SecretRegistry"]:
    """Give this task its own view of the registry.

    The executor runs tasks in concurrent threads and registers every task's
    secrets into one process-global registry that is never cleared, so each
    task inherited every earlier task's secrets. The visible harm is a later
    task's own output being redacted against an unrelated task's password --
    and the marker names that other task's variable, which on a shared
    executor is one tenant's recipe leaking into another's output.

    Clearing between tasks is not the fix: tasks overlap, so a clear during
    one disarms masking for another running beside it. Scoping is, because
    it needs no coordination between tasks.

    A task's secrets stay in its scope and never reach the global, which is
    what lets the global be read as a floor: a scope masks against its own
    secrets PLUS the process-level ones, and never against another task's.

    Residual, measured rather than assumed: a RAW thread started inside a
    task does not inherit this ContextVar, so it masks process-level secrets
    only. asyncio.create_task and asyncio.to_thread do inherit, which covers
    what the executor actually uses for subprocess output and progress; a
    raw thread that needs the scope can carry it with
    contextvars.copy_context().
    """
    scoped = SecretRegistry(_parent=SecretRegistry.global_instance())
    token = _active_registry.set(scoped)
    try:
        yield scoped
    finally:
        _active_registry.reset(token)
