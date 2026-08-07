"""
Logging filter for masking secrets in log messages and streams.

This module provides a Python logging.Filter that automatically masks
registered secrets in all log output. Secrets are replaced with
***REDACTED:VARIABLE_NAME*** for debugging while preventing leaks.

Key Features:
- Automatic masking of messages, arguments, and exceptions
- Deferred pattern rebuild (only during masking, not registration)
- Circuit breaker for graceful degradation
- Message truncation (5KB default) for performance
- Stream wrappers for stdout/stderr coverage

Performance:
- Pattern rebuilt only when needed during masking operations
- Lock-free masking with COW snapshots
- Truncation before masking avoids regex on huge strings
- Performance warnings at 100/500 secrets
"""

import contextlib
import io
import logging
import os
import re
import sys
import threading
import uuid
import weakref
from typing import Any, Dict, List, Optional, TextIO, Tuple

from datahub.masking.constants import REDACTED_FORMAT
from datahub.masking.logging_utils import get_masking_safe_logger
from datahub.masking.secret_registry import SecretRegistry, is_masking_enabled

logger = get_masking_safe_logger(__name__)

# Cached is_masking_enabled() result. Refreshed when the env var changes
# (one os.getenv per call, no .lower() per call) so filter() doesn't
# re-import is_masking_enabled or re-read the env per record. Tests that
# toggle DATAHUB_DISABLE_SECRET_MASKING via os.environ/monkeypatch are
# picked up on the next record (the refresh compares the raw env string).
_masking_enabled_cached: bool = is_masking_enabled()
_masking_enabled_env_raw: Optional[str] = os.getenv("DATAHUB_DISABLE_SECRET_MASKING")


def _refresh_masking_enabled_cached() -> None:
    """Recompute the cached is_masking_enabled() if the env var changed."""
    global _masking_enabled_cached, _masking_enabled_env_raw
    raw = os.getenv("DATAHUB_DISABLE_SECRET_MASKING")
    if raw != _masking_enabled_env_raw:
        _masking_enabled_env_raw = raw
        _masking_enabled_cached = is_masking_enabled()


# The single installed filter instance for this process (identity-based
# install/uninstall tracking). See install_masking_filter. Forward-annotated
# because SecretMaskingFilter is defined below.
_installed_filter: Optional["SecretMaskingFilter"] = None

# Original ``logging.Handler.__init__`` saved while the __init__ hook is
# installed, so uninstall can restore it. ``None`` means the hook is not
# active. The hook auto-attaches the installed filter to every *new* handler
# (a handler created after install — basicConfig, a library's lazy logging
# config, a per-task FileHandler — otherwise gets no filter). Idempotent
# (re-install is a no-op) and reversible on uninstall.
_original_handler_init: Optional[Any] = None
# The patched ``__init__`` we installed, kept so uninstall can verify the
# current ``Handler.__init__`` is still ours before restoring. If another
# library wrapped our patch, we leave the chain alone (see
# _uninstall_handler_init_hook).
_patched_handler_init: Optional[Any] = None

# Serializes install/uninstall mutations of the module globals
# (_installed_filter, _original_handler_init, _patched_handler_init) and the
# root-logger / handler filter attachments. Bootstrap holds _bootstrap_lock
# across install/uninstall, but install_masking_filter is also public and
# called directly by tests; without this lock, two concurrent direct calls
# attach two filter instances and leak one on teardown. An RLock so a
# holder can re-enter (e.g. bootstrap's _bootstrap_lock path calling install
# then uninstall within the same thread).
_install_lock = threading.RLock()

# Leaf lock for ``_covered_handlers``. Acquired LAST (after ``_install_lock``
# and ``logging._lock``) and never held while acquiring either of those, so it
# cannot be the outer lock in any cycle. Without this, ``_cover_handler`` (called
# from the ``Handler.__init__`` patch, which runs while CPython holds
# ``logging._lock`` for ``basicConfig`` / ``dictConfig`` / ``fileConfig``) would
# take ``_install_lock`` and invert the order against install/uninstall, which
# take ``_install_lock`` then ``logging._lock`` via ``_snapshot_handler_pairs``.
# Two threads then deadlock with the logging lock held, so there is no output.
# A plain ``Lock`` is enough: nothing holding ``_covered_lock`` re-enters it.
_covered_lock = threading.Lock()

# Weak set of every handler the filter was attached to (via the one-shot
# scan or the Handler.__init__ hook). Uninstall iterates this in addition to
# _snapshot_handler_pairs so handlers not on any logger (held by a
# QueueListener, or nested inside another handler) still get the filter
# removed — _snapshot_handler_pairs only sees handlers attached to a
# logger, so without this set those handlers would retain the filter after
# uninstall and keep masking records dispatched through them. Weak so
# a GC'd handler leaves the set without manual cleanup.
_covered_handlers: "weakref.WeakSet[logging.Handler]" = weakref.WeakSet()

# Constants
REDACTED_MASKING_NAMESPACE = "datahub.masking."

MASKING_ERROR_MESSAGE = "[MASKING_ERROR - OUTPUT_SUPPRESSED_FOR_SECURITY]"


def _is_masking_namespace_name(name: str) -> bool:
    """True for loggers in the masking framework's own namespace.

    Single predicate for the two call sites that need to skip masking-internal
    loggers: ``filter()``'s record-name bypass and
    ``_add_filter_to_existing_handlers``'s attach-time skip. They must agree,
    or a record from the exact-name ``datahub.masking`` logger is masked by
    ``filter()`` (whose ``startswith(REDACTED_MASKING_NAMESPACE)`` check
    misses the dotless name) while its handler was skipped at attach time —
    an inconsistency that over-masks internal logs. The trailing dot in
    ``REDACTED_MASKING_NAMESPACE`` avoids matching a hypothetical
    "datahub.maskingfoo" logger; the explicit ``== "datahub.masking"`` covers
    the dotless name itself.
    """
    return name == "datahub.masking" or name.startswith(REDACTED_MASKING_NAMESPACE)


# Idempotency token stamped onto records after masking. A random string
# (not ``object()``) so structured formatters that serialize ``record.__dict__``
# emit a valid line instead of raising ``TypeError: Object of type object is
# not JSON serializable`` and dropping the record. Compared by value (``==``)
# so the guard survives cross-thread ``QueueHandler``/``QueueListener``
# within a process, where ``is`` would fail on the un-pickled copy.
# Cross-process the child's token differs (generated at its own import),
# so the child re-masks — safe because ``mask_text`` is idempotent (see the
# marker-alternatives comment in ``_check_and_rebuild_pattern``); the
# second pass is not merely redundant, it is required for correctness
# there. Randomness prevents a caller from forging
# ``extra={'_datahub_masked': ...}`` to disable masking for a record.
_MASKED = f"_datahub_masked_{uuid.uuid4().hex}"
CIRCUIT_OPEN_MESSAGE = "[REDACTED: Masking Circuit Open]"

# Truncation suffix marker. Used by ``_truncate_message`` and by the
# idempotency check in ``_truncate_message`` (B2): if the message already
# ends with this marker, it is returned unchanged so a second masking pass
# (when the sentinel guard fails) does not re-truncate and corrupt the
# byte count.
_TRUNCATION_MARKER_RE = re.compile(r"\n\.\.\. \[\d+ bytes truncated for performance\]$")

# LogRecord standard attributes (per logging.LogRecord.__init__ + attributes set
# by the framework during formatting). Extras added via ``extra={...}`` are
# everything NOT in this set, and we mask those recursively. Iterating the whole
# __dict__ and masking pathname/funcName/module would waste regex on the hot path
# and could corrupt those fields.
#
# ``_datahub_masked`` is included so ``_mask_extras`` skips it (it is the
# idempotency token, not user data). This set only governs ``_mask_extras``'s
# own skip loop — a third-party formatter that serializes ``record.__dict__``
# directly still sees ``_datahub_masked`` and emits it as a key. That is the
# accepted cost of making the token a string (B1): the alternative
# ``object()`` sentinel raised ``TypeError`` in JSON formatters and dropped
# the record entirely.
_STANDARD_RECORD_ATTRS = frozenset(
    {
        "name",
        "msg",
        "args",
        "levelname",
        "levelno",
        "pathname",
        "filename",
        "module",
        "exc_info",
        "exc_text",
        "stack_info",
        "lineno",
        "funcName",
        "created",
        "msecs",
        "relativeCreated",
        "thread",
        "threadName",
        "process",
        "processName",
        "taskName",
        "message",
        "asctime",
        "_datahub_masked",
    }
)


class SecretMaskingFilter(logging.Filter):
    """Logging filter that masks secrets in log records."""

    def __init__(
        self,
        secret_registry: Optional[SecretRegistry] = None,
        max_message_size: int = 5000,
    ):
        """Initialize the masking filter."""
        super().__init__()

        self._registry = secret_registry or SecretRegistry.get_instance()
        self._max_message_size = max_message_size

        # Thread safety: RLock for pattern access
        self._pattern_lock = threading.RLock()

        # Pattern state (immutable references - copy-on-write)
        self._pattern: Optional[re.Pattern] = None
        self._replacements: Dict[str, str] = {}
        self._last_version: int = 0
        # Length of the longest alternative in the compiled pattern (the
        # longest expanded value or exact marker), used by two-stage
        # truncation in ``mask_text`` to pre-truncate to
        # ``max_message_size + longest_pattern`` before masking, so a secret
        # starting within the first ``max_message_size`` chars is fully
        # contained in the pre-truncated region and gets masked before the
        # final cut. The bound is the longest expanded key (repr-escaped /
        # JSON-escaped / SQLAlchemy-encoded variants can be longer than the
        # raw value) and the longest exact marker (``15 + len(name)``),
        # since markers are alternatives too and severing one at the
        # pre-cut would un-protect the secret substring inside the fragment.
        # The generic marker fallback is EXCLUDED — see the bound-exclusion
        # comment in ``_check_and_rebuild_pattern``.
        self._longest_pattern_length: int = 0

        # Circuit breaker to prevent cascading failures
        self._failure_count = 0
        self._max_failures = 10
        self._circuit_open = False

    def _check_and_rebuild_pattern(self) -> None:
        """Check if pattern needs rebuilding and rebuild if necessary."""
        MAX_REBUILD_ATTEMPTS = 10  # Prevent infinite loops

        # Track last successfully built pattern for emergency fallback
        last_built_pattern: Optional[re.Pattern] = None
        last_built_replacements: Dict[str, str] = {}
        last_built_version: int = 0

        for attempt in range(MAX_REBUILD_ATTEMPTS):
            # Quick check WITHOUT lock (fast path)
            current_version = self._registry.get_version()

            with self._pattern_lock:
                if current_version == self._last_version:
                    return  # Pattern is up to date

            # Build pattern OUTSIDE lock (expensive operations)
            secrets = self._registry.get_all_secrets()

            if not secrets:
                with self._pattern_lock:
                    self._pattern = None
                    self._replacements = {}
                    self._last_version = current_version
                    self._longest_pattern_length = 0
                return

            # Sort by value length (longest first) - NOT under lock. ``re``
            # is leftmost-then-first: at a given position it tries alternatives
            # in listing order, so listing longer values first means a longer
            # secret wins ties against a shorter one at the same position.
            sorted_secrets = sorted(
                secrets.items(), key=lambda x: len(x[0]), reverse=True
            )

            # Build pattern - NOT under lock.
            #
            # ORDERING IS LOAD-BEARING, not a preference. Register
            # ``***REDACTED:A***`` under name ``B`` and ``***REDACTED:B***``
            # under name ``A`` (both reachable by pasting redacted output
            # back into a recipe). With values first, masking oscillates
            # with period 2 and has NO fixed point; with markers first it
            # terminates on the first pass. The ordering is what makes
            # ``mask_text`` well-defined.
            #
            # The ordering claim is FALSE without named-group dispatch:
            # when a value is marker-shaped the marker and the value are
            # the *same string*, so no test on the matched text can tell
            # which alternative won, and a naive ``replacements.get(
            # match.group(0))`` callback oscillates even with markers
            # first. The callback dispatches on ``match.lastgroup`` (which
            # alternative won), not on the matched text, so a marker
            # alternative passes through untouched and a value alternative
            # is replaced.
            #
            # The generic fallback goes LAST. A marker at position 0 beats
            # a value matching at position 12 regardless of order
            # (leftmost wins), so generic-last still protects
            # dropped-execution markers (whose names are gone from the
            # registry, so no exact marker alternative exists for them),
            # AND lets a marker-shaped registered value be masked by the
            # generic alternative at its own position.
            #
            # One accepted fail-open ("route A"): a registered value that
            # exactly equals a marker for a *registered name* passes through
            # in cleartext (both alternatives match at the same position
            # and the marker must win for termination). It is refused at
            # registration instead — see ``_validate_secret`` in
            # secret_registry. A per-process nonce in the marker would
            # close route A but make cross-process idempotency depend on
            # the only best-effort alternative (the generic fallback), so
            # it is not adopted.
            marker_alts = [
                f"(?P<m{i}>{re.escape(REDACTED_FORMAT.format(name=n))})"
                for i, (_, n) in enumerate(sorted_secrets)
            ]
            # CRITICAL: re.escape() ensures secrets with regex metacharacters
            # (e.g., ".*", "a+b", "test|prod") are matched literally, not as regex.
            value_alts = [
                f"(?P<v{i}>{re.escape(v)})" for i, (v, _) in enumerate(sorted_secrets)
            ]
            generic = [r"(?P<g>\*\*\*REDACTED:[^*]{0,256}?\*\*\*)"]
            pattern_str = "|".join(marker_alts + value_alts + generic)

            # Bound for two-stage truncation: the longest of (longest value,
            # longest exact marker). The generic alternative is EXCLUDED:
            # it is forced, not chosen — you cannot compute the length of a
            # marker whose name is gone from the registry (the
            # dropped-execution case the fallback exists for). It is safe
            # because a marker carries a NAME, not a value, so severing
            # any marker can never leak a secret substring. (This holds
            # once the configuration/common.py name-hygiene change lands;
            # until then a name could carry a value, which is why the N
            # exact marker alternatives are kept rather than collapsing to
            # generic-only — they bound the marker length for names that
            # ARE registered.)
            longest_value = len(sorted_secrets[0][0])
            longest_marker = max(
                (len(REDACTED_FORMAT.format(name=n)) for _, n in sorted_secrets),
                default=0,
            )
            new_longest_pattern_length = max(longest_value, longest_marker)

            # Compile regex - NOT under lock (this is the expensive part!)
            try:
                new_pattern = re.compile(pattern_str)
                new_replacements = {value: name for value, name in sorted_secrets}

                # Save this for emergency fallback
                last_built_pattern = new_pattern
                last_built_replacements = new_replacements
                last_built_version = current_version
            except Exception as e:
                logger.error(f"Failed to compile masking pattern: {e}")
                return  # Keep using old pattern

            # Warn about performance impact with large secret counts
            secret_count = len(secrets)
            if secret_count > 500:
                logger.warning(
                    f"Very large secret count ({secret_count})! "
                    f"This may impact masking performance. "
                    f"Consider using Aho-Corasick algorithm for better performance."
                )
            elif secret_count >= 100:
                logger.warning(
                    f"Large number of secrets registered ({secret_count}). "
                    f"This may impact masking performance."
                )

            # Atomic swap under lock (fast!)
            with self._pattern_lock:
                # Check version again - secrets might have changed while building
                registry_version = self._registry.get_version()

                if registry_version == current_version:
                    # Version is stable, safe to swap in the new pattern
                    self._pattern = new_pattern
                    self._replacements = new_replacements
                    self._last_version = current_version
                    self._longest_pattern_length = new_longest_pattern_length

                    if attempt > 0:
                        logger.debug(
                            f"Rebuilt masking pattern with {secret_count} secrets "
                            f"(version {current_version}) after {attempt + 1} attempts"
                        )
                    else:
                        logger.debug(
                            f"Rebuilt masking pattern with {secret_count} secrets "
                            f"(version {current_version})"
                        )
                    return  # Success!

                # Version changed during build, loop will retry
                logger.debug(
                    f"Pattern version changed during build "
                    f"(expected {current_version}, got {registry_version}). "
                    f"Retrying... (attempt {attempt + 1}/{MAX_REBUILD_ATTEMPTS})"
                )
                # Continue to next iteration of the loop

        # If we get here, we failed after MAX_REBUILD_ATTEMPTS
        # Emergency fallback: Use the last pattern we built if we have no pattern at all
        # Better to have a slightly stale pattern than no masking at all
        with self._pattern_lock:
            if self._pattern is None and last_built_pattern is not None:
                self._pattern = last_built_pattern
                self._replacements = last_built_replacements
                self._last_version = last_built_version
                logger.warning(
                    f"Emergency fallback: Using potentially stale pattern (version {last_built_version}) "
                    f"because no pattern was previously available and registry is changing too rapidly."
                )
            else:
                logger.error(
                    f"CRITICAL: Failed to rebuild masking pattern after {MAX_REBUILD_ATTEMPTS} attempts. "
                    f"Secrets are being modified too rapidly. "
                    f"Continuing with potentially stale pattern (version {self._last_version}). "
                    f"Some newly added secrets may not be masked until rate of changes decreases."
                )
        # Keep using the old pattern rather than crashing - graceful degradation

    def mask_text(
        self,
        text: str,
        pre_truncate_budget: Optional[int] = None,
    ) -> str:
        """Mask secrets in text string.

        Thread-safe, with automatic pattern rebuilding, circuit breaker
        protection, and error handling.

        Args:
            text: Text content to mask.
            pre_truncate_budget: Optional final-output budget (e.g.
                ``max_message_size``). When provided AND the pattern has
                secrets, the input is pre-truncated to
                ``budget + longest_pattern_length`` under ``_pattern_lock``
                *after* the rebuild that refreshes ``_longest_pattern_length``,
                so the pre-cut and the regex use one snapshot by
                construction — no ordering hazard where the pre-cut could
                use a stale length and sever a secret that the regex then
                misses. When ``None`` or no secrets are registered, no
                pre-truncation happens (existing behaviour for the non-msg
                call sites: args, exc_text, stack_info, extras, stream
                wrapper output).

        Returns:
            Text with secrets replaced by ***REDACTED:VARIABLE_NAME***
        """
        if not isinstance(text, str) or not text:
            return text

        # Snapshot pattern, replacements, and longest-pattern length under
        # one lock, and pre-truncate under the SAME lock. Doing the pre-cut
        # here (rather than in filter() before mask_text) means the cut uses
        # the freshly-rebuilt length, not a stale one read before the
        # rebuild — closing the deterministic leak where a 3000-char PEM
        # key registered after a 20-char env-var secret would be severed by
        # a 5020-char pre-cut sized from the stale 20.
        with self._pattern_lock:
            self._check_and_rebuild_pattern()
            pattern = self._pattern
            replacements = self._replacements  # No .copy() needed!
            longest = self._longest_pattern_length
            if (
                pre_truncate_budget is not None
                and longest > 0
                and len(text) > pre_truncate_budget + longest
            ):
                text = text[: pre_truncate_budget + longest]

        # Pattern might be None if no secrets registered
        if pattern is None:
            return text

        # Circuit breaker - if too many failures, stop trying
        if self._circuit_open:
            return CIRCUIT_OPEN_MESSAGE

        # Mask secrets (outside lock - safe because immutable references)
        try:
            # Dispatch on which alternative won (``match.lastgroup``), not on
            # the matched text. Marker alternatives (groups named ``m*`` and
            # the generic ``g``) pass through untouched so masking terminates
            # on the first pass; value alternatives (``v*``) are replaced.
            # A text-based dispatch would oscillate when a value is
            # marker-shaped (the marker and the value are the same string) —
            # see the ordering comment in ``_check_and_rebuild_pattern``.
            def replace_with_variable_name(match):
                last = match.lastgroup
                if last is not None and not last.startswith("v"):
                    return match.group(0)  # marker: pass through untouched
                return REDACTED_FORMAT.format(
                    name=replacements.get(match.group(0), "UNKNOWN")
                )

            masked = pattern.sub(replace_with_variable_name, text)

            # Success - reset failure count
            if self._failure_count > 0:
                self._failure_count = 0

            return masked

        except re.error as e:
            self._failure_count += 1
            logger.error(
                f"CRITICAL: Secret masking failed due to regex error "
                f"(failure {self._failure_count}/{self._max_failures}). "
                f"Message redacted for safety. Error: {e}"
            )
            if self._failure_count >= self._max_failures:
                self._circuit_open = True
                logger.critical(
                    "CRITICAL: Masking circuit breaker OPEN. All messages will be redacted."
                )
            return "[REDACTED: Masking Regex Error]"

        except MemoryError:
            self._failure_count += 1
            logger.error(
                f"CRITICAL: Secret masking failed due to memory error "
                f"(failure {self._failure_count}/{self._max_failures}). "
                f"Message redacted for safety."
            )
            if self._failure_count >= self._max_failures:
                self._circuit_open = True
                logger.critical(
                    "CRITICAL: Masking circuit breaker OPEN. All messages will be redacted."
                )
            return "[REDACTED: Masking Memory Error]"

        except Exception as e:
            self._failure_count += 1
            logger.error(
                f"CRITICAL: Secret masking failed with unexpected error "
                f"(failure {self._failure_count}/{self._max_failures}). "
                f"Message redacted for safety. Error type: {type(e).__name__}"
            )
            if self._failure_count >= self._max_failures:
                self._circuit_open = True
                logger.critical(
                    "CRITICAL: Masking circuit breaker OPEN. All messages will be redacted."
                )
            return "[REDACTED: Masking Error]"

    def rebind_registry(self, registry: "SecretRegistry") -> None:
        """Rebind the registry this filter reads, and force a pattern
        rebuild on the next mask.

        Used by ``install_masking_filter``'s refresh path when the caller
        hands in a different registry on a repeat install. Encapsulating
        the rebind here keeps the invariant (rebind implies version reset)
        in one place where it can't drift from the install path.

        Uses ``_last_version = -1`` (not 0): a fresh registry's version is 0,
        so ``0 == 0`` would make ``_check_and_rebuild_pattern``'s fast path
        skip the rebuild and the old pattern would persist (over-masking the
        old registry's values). ``-1`` never equals a real version, so the
        next mask always rebuilds. The pattern and replacements are cleared
        too so a stale pattern can't be served before the rebuild runs.

        The whole body runs under ``_pattern_lock`` (an ``RLock``, so nesting
        is safe). Every other write to ``_registry`` / ``_last_version`` /
        ``_pattern`` / ``_replacements`` happens under that lock, and
        ``mask_text`` snapshots them together under it — an unlocked
        rebind could land between the snapshot of ``_pattern`` and
        ``_replacements`` and emit cleartext (or a ``***REDACTED:UNKNOWN***``
        mismatch from a cleared ``_replacements`` paired with an old
        ``_pattern``). Clearing under the lock also makes the four writes
        atomic relative to each other.
        """
        with self._pattern_lock:
            self._registry = registry
            self._last_version = -1
            self._pattern = None
            self._replacements = {}

    def _mask_args(self, args: Any) -> Any:
        """Mask secrets in log arguments."""
        if not args:
            return args

        try:
            if isinstance(args, dict):
                return {
                    k: self.mask_text(v) if isinstance(v, str) else v
                    for k, v in args.items()
                }
            elif isinstance(args, tuple):
                return tuple(
                    self.mask_text(arg) if isinstance(arg, str) else arg for arg in args
                )
            else:
                return args
        except Exception as e:
            # Fail-secure: never return unmasked args on error
            logger.error(f"CRITICAL: Secret masking failed in args: {e}", exc_info=True)
            return (MASKING_ERROR_MESSAGE,)

    def _mask_exception(self, exc_info: Optional[Tuple]) -> Optional[str]:
        """Materialize traceback text from ``exc_info`` so filter() can mask it.

        Returns the unmasked traceback text (or a sentinel on error). filter()
        assigns the result to ``record.exc_text`` and masks it alongside any
        pre-existing ``exc_text``. Formatters reuse ``exc_text`` when set, so
        the masked text is what gets emitted.

        ``exc_info`` itself is left untouched: handler-based error reporters
        (Sentry, Datadog, anything reading ``record.exc_info``) keep the real
        exception with its chain and traceback.

        Stated trade-off: the previous implementation rebuilt the exception via
        ``type(exc_value)(*masked_args)``, which attempted to mask exception
        args for downstream error-reporter consumers — badly (it raised for
        any exception whose ``__init__`` signature didn't match its ``args``,
        common in DB drivers and source connectors, and the fallback returned
        a bare ``RuntimeError`` that destroyed the traceback, message, class,
        and ``__cause__`` chain). This PR trades masked-args-for-error-reporters
        for correctness of the *printed* output. That is the right trade
        because the printed output is what ships to log files and is the
        primary leak surface; error reporters are a secondary surface and
        their consumers can be separately addressed (e.g. by masking at the
        reporter integration). It is a stated decision, not a silent
        consequence.

        A middle option (mutating ``exc_value.args`` in place) was considered
        and rejected: it mutates a live exception the caller may re-raise or
        inspect after logging.

        Benefit that comes free: ``Formatter.formatException`` follows
        ``__cause__`` / ``__context__``, so the masked text covers the whole
        chain, including chained exception messages that the old rebuild
        dropped.

        Residual: pre-filling ``exc_text`` means stdlib ``Formatter.format``
        skips its ``formatException`` call entirely (it only calls
        ``formatException`` when ``exc_text`` is falsy). So a handler whose
        formatter overrides ``formatException`` — to redact frames, add
        context, or route to a structured traceback renderer — has that
        override bypassed: the stock masked traceback is emitted instead.
        Not pre-filling ``exc_text`` would restore the custom
        ``formatException`` but leave tracebacks unmasked, which is the
        primary leak surface; the trade runs in the direction of masking the
        printed output. A handler that wants both a custom traceback format
        and masking must mask inside its own ``formatException`` (or read
        ``record.exc_text`` when set).
        """
        if not exc_info:
            return None

        try:
            return logging.Formatter().formatException(exc_info)
        except Exception as e:
            # Fail-secure: return a sentinel string rather than fabricating a
            # RuntimeError, so the original exc_info (and error reporters)
            # survive. filter() will mask this (no-op since it has no secrets).
            # Note: do NOT log with exc_info=True here — that would call
            # formatException again and re-raise under the same failure.
            logger.error(
                f"CRITICAL: Secret masking failed materializing exception: {e}"
            )
            return MASKING_ERROR_MESSAGE

    def _truncate_message(
        self, message: str, original_len: Optional[int] = None
    ) -> str:
        """Truncate large messages after masking.

        Idempotent: if the message already ends with the truncation marker,
        return it unchanged. This is defense-in-depth so that when the
        idempotency token guard in ``filter()`` fails (e.g. a third-party
        formatter that strips unknown attributes, or a record that round-
        tripped through pickle and lost the token), a second masking pass
        does not re-truncate the already-truncated text, eat the first
        suffix, and report a wrong byte count. The marker is a trailing
        ``\\n... [N bytes truncated for performance]`` line.

        ``original_len`` is the pre-truncation length of the message (before
        the two-stage pre-truncate). When provided, the byte count in the
        marker reflects the original overrun (``original_len -
        max_message_size``), not the post-pre-truncate overrun, so the user
        sees how much of the original was dropped rather than how much of the
        already-truncated region was dropped.
        """
        if not isinstance(message, str):
            return message

        if len(message) <= self._max_message_size:
            return message

        if _TRUNCATION_MARKER_RE.search(message):
            return message

        base_len = original_len if original_len is not None else len(message)
        # ``max(0, ...)``: masking can EXPAND the text (a 3-char ``***`` secret
        # becomes ``***REDACTED:STARS***`` = 18 chars), so ``original_len -
        # max_message_size`` can be negative when the original fit but the masked
        # output did not. A negative byte count in the marker is wrong (it implies
        # the output grew, which is not "truncation"), so clamp to 0 — the user
        # sees "0 bytes truncated" rather than "-4997 bytes truncated".
        truncated_bytes = max(0, base_len - self._max_message_size)
        return (
            f"{message[: self._max_message_size]}\n"
            f"... [{truncated_bytes} bytes truncated for performance]"
        )

    # Depth cap and cycle guard for _mask_value_recursive. extra= can carry
    # self-referential or very large structures onto the logging hot path; an
    # unbounded recursion would hang the logger. The cap is generous (real
    # config is rarely >10 deep); beyond it we return a placeholder string
    # (not the raw value, not an exception) so logging proceeds and any
    # secret in the elided subtree is not emitted in cleartext.
    _MAX_EXTRA_DEPTH = 10

    def _mask_value_recursive(
        self, value: Any, _depth: int = 0, _seen: Optional[set] = None
    ) -> Any:
        """Recursively mask secrets in a value's string leaves.

        Covers dict/list/tuple/set containers and string leaves. Non-string,
        non-container values pass through unchanged. Arbitrary objects are NOT
        stringified-and-masked (that would change behavior and risk calling
        ``__str__`` side effects); if a formatter later serializes such an
        object via ``%s``, the serialized form is masked at the formatter-output
        stage only if that output flows through the stdout/stderr wrapper —
        otherwise it is a residual gap, documented in install_masking_filter.

        Containers are *copied*, not mutated in place: ``record.__dict__["cfg"]``
        is the caller's live dict, so masking leaves in place would silently
        turn a config dict into ***REDACTED:X*** after the first log line — a
        nasty bug to chase. We build a masked copy and assign that to the record.

        A depth cap (``_MAX_EXTRA_DEPTH``) and an identity-based cycle guard
        (``_seen``) bound the recursion: extra= can carry self-referential or
        very large structures onto the hot path. Beyond the cap or on a cycle
        we return a placeholder string (``"<not masked: ...>"``) rather
        than the raw value — returning the raw subtree would emit any secret
        it contains in cleartext, and everything else in this module fails
        closed. The placeholder also keeps this docstring honest: "return
        the value unchanged" reads as "we mask less here" when it actually
        means "we emit the secret."

        Accepted limitation — namedtuple retyping: the container branch
        rebuilds via ``type(value)(masked)``, which for a ``namedtuple``
        produces a plain ``tuple`` (the ``type(seq)`` call bypasses the
        named constructor) and for a ``defaultdict`` would need
        ``default_factory`` positionally (a second fix). This means
        ``extra=`` containers may be retyped by masking. We document rather
        than fix it: the third-party ``extra=`` content that now reaches the
        filter is the surface area, and a generic ``type(value)(masked)``
        does not yield for these. If fixed, note that ``defaultdict`` needs
        ``default_factory`` positionally, so the dict branch is two fixes.
        """
        if _depth >= self._MAX_EXTRA_DEPTH:
            return "<not masked: depth limit>"
        if isinstance(value, str):
            return self.mask_text(value)
        if isinstance(value, dict):
            if _seen is None:
                _seen = set()
            if id(value) in _seen:
                return "<not masked: cycle>"
            _seen.add(id(value))
            try:
                return {
                    k: self._mask_value_recursive(v, _depth + 1, _seen)
                    for k, v in value.items()
                }
            finally:
                _seen.discard(id(value))
        if isinstance(value, (list, tuple, set, frozenset)):
            if _seen is None:
                _seen = set()
            if id(value) in _seen:
                return "<not masked: cycle>"
            _seen.add(id(value))
            try:
                masked = [
                    self._mask_value_recursive(v, _depth + 1, _seen) for v in value
                ]
            finally:
                _seen.discard(id(value))
            if isinstance(value, tuple):
                return tuple(masked)
            if isinstance(value, set):
                return set(masked)
            if isinstance(value, frozenset):
                return frozenset(masked)
            return masked
        return value

    def _mask_extras(self, record: logging.LogRecord) -> None:
        """Mask secrets in non-standard LogRecord attributes (``extra={...}``).

        Formatters pull fields from ``record.__dict__``; without this, a
        ``Formatter("%(dsn)s")`` with ``extra={"dsn": "db://u:sekret@h"}`` would
        emit the secret unmasked. Recurses into dict/list/tuple/set so nested
        containers (``extra={"cfg": {"password": "sekret"}}``) are covered.

        Standard LogRecord attributes are skipped: masking ``pathname`` /
        ``funcName`` / ``module`` would waste regex on the hot path and risks
        corrupting framework-set fields. Third-party libraries (structlog,
        gunicorn, ddtrace) inject their own attributes; this loop must not
        raise on unexpected types — it runs inside filter()'s try/except, and
        _mask_value_recursive returns non-container values unchanged.
        """
        for key, value in list(record.__dict__.items()):
            if key in _STANDARD_RECORD_ATTRS:
                continue
            if isinstance(value, (str, dict, list, tuple, set, frozenset)):
                record.__dict__[key] = self._mask_value_recursive(value)

    def _mask_record_msg(self, record: logging.LogRecord) -> None:
        """Step 1 of ``filter()``: mask ``record.msg`` regardless of type.

        ``str`` msg: pre-truncate + mask + truncate (see ``mask_text`` for the
        bound). dict/list/tuple/set/frozenset msg: route through
        ``_mask_value_recursive`` so the structlog / JSON-logging idiom
        (``logger.info({"password": "s3cret"})``) is masked — previously
        such a msg was emitted in cleartext by ``getMessage()`` because
        ``filter()`` masked msg only when it was a ``str`` and ``_mask_extras``
        skipped it (msg is a standard attribute). Arbitrary objects: compute
        ``str()``, mask it, and replace only if the masked text differs from
        the plain ``str()`` — type-preserving when there is no secret,
        sanitizing when there is. We do NOT normalize with
        ``record.msg = record.getMessage(); record.args = None``: that
        destroys the format string for every downstream handler in the
        process (aggregators group on the template; python-json-logger /
        structlog read msg and args separately) — the same category of
        invasive global logging mutation this PR exists to correct.

        When ``record.args`` is present, ``record.msg`` is a format string
        (e.g. ``"Connecting to %s"``), not the final text. Pre-truncating or
        final-truncating the format string before ``%`` formatting can sever a
        ``%s`` placeholder, producing ``"Connecting to %*** [N bytes
        truncated...]"`` — a corrupted format string that then raises
        ``ValueError`` during ``msg % args``. Mask the format string (a secret
        could be in it) but skip both truncation stages when args are present;
        the formatted message is truncated downstream if it flows through the
        stdout/stderr wrapper, and a format string is rarely long enough to
        need truncation anyway.
        """
        if isinstance(record.msg, str):
            if record.args:
                record.msg = self.mask_text(record.msg)
            else:
                original_len = len(record.msg)
                record.msg = self.mask_text(
                    record.msg, pre_truncate_budget=self._max_message_size
                )
                record.msg = self._truncate_message(
                    record.msg, original_len=original_len
                )
        elif isinstance(record.msg, (dict, list, tuple, set, frozenset)):
            record.msg = self._mask_value_recursive(record.msg)
        elif record.msg is not None:
            plain = str(record.msg)
            masked = self.mask_text(plain)
            if masked != plain:
                record.msg = masked

    def filter(self, record: logging.LogRecord) -> bool:
        """Filter and mask a log record.

        Idempotent per record: a token (``record._datahub_masked``) is set
        after masking so the same record flowing through multiple handlers
        (each carrying this filter) is not re-masked / re-truncated. The
        token is a random string compared by value so the guard survives
        cross-thread ``QueueHandler``/``QueueListener`` within a process,
        and so structured formatters that serialize ``record.__dict__``
        emit a valid line instead of raising on a non-serializable sentinel.

        Defense-in-depth: ``_truncate_message`` is independently idempotent
        (it detects its own suffix marker), so a second masking pass when the
        token guard fails does not re-truncate and corrupt the byte count.

        Fail closed per field: each of the seven numbered steps runs in its
        own try; a field that raises is substituted with
        ``MASKING_ERROR_MESSAGE`` and the next step still runs. The
        idempotency sentinel is set in ``finally`` so it guards the failure
        path too — a record that raised in step 3 used to get a full
        unmasked pass per handler (the sentinel was the last statement inside
        the try), compounding corruption and the N x str() cost on the same
        record.
        """
        # Re-entrancy / idempotency guard. Records from the masking framework's
        # own loggers bypass masking entirely (their handlers are skipped at
        # install time, but they may propagate to root handlers after
        # reset_masking_safe_loggers flips propagate=True on teardown — see
        # bootstrap.shutdown_secret_masking). The check is on the record name,
        # not the logger we attached to, so it catches propagation. Uses the
        # shared _is_masking_namespace_name predicate so the bypass and the
        # attach-time skip agree: a record from the exact-name
        # ``datahub.masking`` logger is bypassed here, not masked.
        if _is_masking_namespace_name(record.name):
            return True

        if getattr(record, "_datahub_masked", None) == _MASKED:
            return True

        # Check if masking is disabled for debugging. is_masking_enabled is
        # imported at module load (no per-record import) and the result is
        # cached, refreshed when the env var changes (one getenv per call).
        _refresh_masking_enabled_cached()
        if not _masking_enabled_cached:
            return True  # Skip all masking and truncation for debugging

        # Each of the seven steps below runs in its own try. If a step raises,
        # the field it was masking is substituted with ``MASKING_ERROR_MESSAGE``
        # (fail closed — never an unmasked value) and the next step still runs.
        # The idempotency sentinel is set in ``finally`` so it guards the
        # failure path too: a record that failed one step used to get a full
        # unmasked pass per handler (the sentinel was the last statement
        # inside the try), compounding corruption and the N x str() cost on
        # the same record. "Marked as done" is now truthful — every field is
        # either masked or suppressed.
        try:
            # 1. Mask record.msg (any type — see _mask_record_msg).
            try:
                self._mask_record_msg(record)
            except Exception:
                record.msg = MASKING_ERROR_MESSAGE

            # 2. Mask arguments (for formatting)
            try:
                if record.args:
                    record.args = self._mask_args(record.args)
            except Exception:
                record.args = (MASKING_ERROR_MESSAGE,)

            # 3. Mask pre-formatted message if it exists
            try:
                if hasattr(record, "message") and record.message:
                    record.message = self.mask_text(record.message)
            except Exception:
                record.message = MASKING_ERROR_MESSAGE

            # 4. Materialize traceback text from exc_info so it can be masked.
            #    exc_info itself is left untouched (error reporters read it).
            #    See _mask_exception for why we do not rebuild the exception.
            try:
                if record.exc_info and not record.exc_text:
                    record.exc_text = self._mask_exception(record.exc_info)
            except Exception:
                record.exc_text = MASKING_ERROR_MESSAGE

            # 5. Mask formatted exception text (pre-existing or just materialized)
            try:
                if record.exc_text:
                    record.exc_text = self.mask_text(record.exc_text)
            except Exception:
                record.exc_text = MASKING_ERROR_MESSAGE

            # 6. Mask stack_info if present (Python 3.2+)
            try:
                if hasattr(record, "stack_info") and record.stack_info:
                    record.stack_info = self.mask_text(record.stack_info)
            except Exception:
                record.stack_info = MASKING_ERROR_MESSAGE

            # 7. Mask non-standard attributes (extra={...}). Must run after msg/args
            #    masking so a formatter that reads msg/args sees the masked values.
            try:
                self._mask_extras(record)
            except Exception:
                # _mask_extras already masks per-field; if the whole loop
                # raises (e.g. __dict__ iteration races), there is no single
                # field to substitute. Leave extras as they are — fail-closed
                # is preserved by the per-field sentinel below.
                pass

        except Exception as e:
            # NEVER let masking break logging. The per-step tries above catch
            # their own exceptions, so reaching here means something truly
            # unexpected (a non-Exception BaseException, or an error in the
            # sentinel-setting itself). Log to stderr and continue.
            try:
                sys.stderr.write(f"WARNING: Secret masking filter failed: {e}\n")
                sys.stderr.flush()
            except Exception:
                pass  # Even error reporting failed, continue silently
        finally:
            # Set in finally so it guards the failure path: a record that
            # raised in step 3 still gets the sentinel, so a second handler
            # does not re-run steps 1-2 (which already succeeded) and
            # re-mask what was already masked. "Marked as done" is truthful
            # because every field is either masked or suppressed.
            record._datahub_masked = _MASKED

        return True  # Always let record through


class StreamMaskingWrapper:
    """Lightweight wrapper for stdout/stderr that masks secrets."""

    def __init__(self, original_stream: TextIO, masking_filter: SecretMaskingFilter):
        """Initialize stream wrapper."""
        self._original = original_stream
        self._filter = masking_filter

    def write(self, text: str) -> int:
        """Write text to stream with secrets masked."""
        # Type validation - text streams require strings
        if not isinstance(text, str):
            raise TypeError(f"write() argument must be str, not {type(text).__name__}")

        try:
            # Mask text (filter handles locking internally)
            masked = self._filter.mask_text(text)

            # Write WITHOUT holding any locks (prevents deadlock)
            self._original.write(masked)

            # Return length of MASKED text (contract compliance)
            return len(masked)

        except TypeError:
            # Re-raise type errors
            raise

        except Exception:
            # Graceful degradation for masking failures
            try:
                self._original.write(text)
                return len(text)
            except Exception:
                return 0

    def flush(self):
        """Flush the underlying stream."""
        try:
            if hasattr(self._original, "flush"):
                self._original.flush()
        except Exception:
            pass

    def writelines(self, lines) -> None:
        """Mask each line then write. Without this, ``writelines`` falls
        through ``__getattr__`` to the unwrapped stream and bypasses masking
        (a ``print(..., file=wrapped_stream)`` path that uses ``writelines``
        would leak). Mask each line via ``write`` so the masking + graceful-
        degradation behavior is identical to the single-line path."""
        for line in lines:
            self.write(line)

    def __getattr__(self, name):
        """Delegate all other attributes to original stream."""
        return getattr(self._original, name)


def _acquire_logging_lock():
    """Context manager for the logging module lock.

    ``logging._lock`` is private and already changed shape once (3.13 dropped
    ``_acquireLock`` / ``_releaseLock``); it remains an ``RLock`` usable as a
    context manager on 3.10–3.14+. Fall back to a no-op rather than crashing
    if a future CPython rename removes ``_lock`` — the snapshot is best-effort
    against concurrent ``addHandler`` / ``removeHandler``, and a no-op only
    risks a ``RuntimeError`` on concurrent mutation, which the caller already
    handles by snapshotting ``list(...)`` before iterating.
    """
    lock = getattr(logging, "_lock", None)
    if lock is None:
        return contextlib.nullcontext()
    return lock


def _snapshot_handler_pairs() -> List[Tuple[logging.Logger, logging.Handler]]:
    """Snapshot ``(logger, handler)`` pairs under the logging module lock.

    Holding the logging module lock around the snapshot prevents
    ``RuntimeError`` from a concurrent ``addHandler`` / ``removeHandler``
    mutating ``logger.handlers`` or ``loggerDict`` while we iterate.
    The lock is released before we return, so callers can ``addFilter`` /
    ``removeFilter`` and log without holding it — holding it across a log
    call would deadlock (the logging path acquires the same lock).

    ``loggerDict`` is snapshotted as ``list(items())``: indexing by
    key after a keys-only snapshot can raise ``KeyError`` if a logger is
    GC'd between the two steps; ``items()`` holds the value reference too.
    ``PlaceHolder`` entries (non-Logger) are filtered out.
    """
    with _acquire_logging_lock():
        root = logging.getLogger()
        pairs: List[Tuple[logging.Logger, logging.Handler]] = [
            (root, h) for h in list(root.handlers)
        ]
        for _name, obj in list(logging.root.manager.loggerDict.items()):
            if isinstance(obj, logging.Logger):
                for h in list(obj.handlers):
                    pairs.append((obj, h))
        return pairs


def _install_handler_init_hook() -> None:
    """Patch ``logging.Handler.__init__`` to auto-attach the installed filter
    to every new handler. Idempotent: a no-op if already installed.
    Reversed by ``_uninstall_handler_init_hook``.

    ``Handler.__init__`` is the construction seam: standard handlers
    (StreamHandler, FileHandler, QueueHandler, ...) all call it, so one
    patch covers handlers created by ``basicConfig``, library lazy config,
    and per-task handler construction after install. Subclasses that do
    NOT call ``Handler.__init__`` (rare) won't be covered — documented
    residual. The patched ``__init__`` reads ``_installed_filter`` (a
    pointer read, atomic under the GIL); if it is None (uninstalled) it
    skips, so a handler constructed during the uninstall window gets no
    filter and ``_remove_filter_from_existing_handlers`` cleans up any that
    did.

    The original ``__init__`` is captured in a closure (not re-read from the
    global) so a handler being constructed when uninstall runs can still
    finish initializing — without this, uninstall clearing
    ``_original_handler_init`` to None mid-call would leave the handler
    half-constructed.
    """
    global _original_handler_init, _patched_handler_init
    if _original_handler_init is not None:
        return  # idempotent
    original = logging.Handler.__init__
    _original_handler_init = original

    def patched_handler_init(self: Any, level: int = logging.NOTSET) -> None:
        original(self, level)  # closure-captured, race-safe vs uninstall
        f = _installed_filter
        if f is not None and f not in self.filters:
            self.addFilter(f)
            _cover_handler(self)

    _patched_handler_init = patched_handler_init
    logging.Handler.__init__ = patched_handler_init  # type: ignore[assignment]


def _uninstall_handler_init_hook() -> None:
    """Restore ``logging.Handler.__init__`` if (and only if) it is still our
    patched function.

    If another library patched ``Handler.__init__`` *after* us, the current
    attribute is no longer ``_patched_handler_init``. Restoring
    ``_original_handler_init`` unconditionally would discard that library's
    patch — the classic monkeypatch stacking hazard. When the current
    attribute is not ours, leave the chain alone and log at debug: we
    cannot safely unwind a patch someone else wrapped, and the residual
    (their patch stays in place, ours is no longer reachable as
    ``Handler.__init__``) is the safe choice. No-op if the hook was never
    installed.

    On decline, the saved globals are NOT cleared. This keeps re-install
    idempotent (``_original_handler_init is not None`` short-circuits
    ``_install_handler_init_hook``) and prevents wrapper stacking across
    repeated install/uninstall cycles: the dead ``patched_v1`` stays in
    lib X's chain, but on re-install it reads the re-set ``_installed_filter``
    and reactivates, so the hook is live again without stacking a new
    wrapper. Clearing the globals on decline would force re-install to
    capture lib X's wrapper as the new "original" and nest a new patch on
    every cycle — permanent stack growth in a long-lived worker.
    """
    global _original_handler_init, _patched_handler_init
    if _original_handler_init is None:
        return
    if logging.Handler.__init__ is _patched_handler_init:
        logging.Handler.__init__ = _original_handler_init  # type: ignore[assignment]
        _original_handler_init = None
        _patched_handler_init = None
    else:
        # Someone wrapped our patch. Restoring the original would clobber
        # their wrapper; leave it in place. Our filter is still attached
        # to handlers constructed while our patch was active, and
        # _remove_filter_from_existing_handlers cleans those up. Keep the
        # saved globals so the next install is idempotent (the dead patch
        # in lib X's chain reactivates when _installed_filter is set again).
        logger.debug(
            "Skipping Handler.__init__ restore: another library patched "
            "it after we did; leaving their patch in place and keeping our "
            "saved globals for idempotent re-install"
        )


def _cover_handler(handler: logging.Handler) -> None:
    """Track a handler we attached the filter to, for uninstall cleanup.

    Handlers not on any logger (held by a QueueListener, nested inside
    another handler) are unreachable from _snapshot_handler_pairs; without
    tracking, uninstall would miss them and they'd retain the filter.
    The weak set auto-drops GC'd handlers.

    Acquires ``_covered_lock`` (a leaf lock — see its definition) rather than
    ``_install_lock``. This is called from the ``Handler.__init__`` patch, which
    CPython invokes while already holding ``logging._lock`` (for ``basicConfig``
    / ``dictConfig`` / ``fileConfig``). Taking ``_install_lock`` here would
    invert the lock order against install/uninstall (which take
    ``_install_lock`` then ``logging._lock``) and deadlock two threads with the
    logging lock held. The leaf lock is acquired last by every caller and never
    held while acquiring either of the others, so it cannot close a cycle.
    """
    with _covered_lock:
        try:
            _covered_handlers.add(handler)
        except Exception:
            # WeakSet can raise ReferenceError on a dying ref; not worth
            # failing the install for.
            pass


def _add_filter_to_existing_handlers(masking_filter: SecretMaskingFilter) -> None:
    """Attach the masking filter to all existing handlers.

    Masking lives on handlers, not the logger: Python skips logger-level filters
    for records propagated from child loggers, so a root-logger filter would miss
    almost everything. A handler filter sees every record reaching that output and
    masks it in place, without touching the handler's stream. (Repointing
    streams instead drops every record under celery -- see
    install_masking_filter.)

    Covers handlers present at install time. Handlers created later are
    covered by the ``Handler.__init__`` hook, so masking does not depend
    on install ordering.

    Skip datahub.masking.* loggers: they log to the original stderr by design and
    carry no secrets, so filtering them only risks re-entrancy. A handler
    *shared* between a datahub.masking.* logger and another logger still gets
    the filter via the other logger — that is safe because ``filter()``'s
    record-name bypass (using the same _is_masking_namespace_name predicate)
    short-circuits masking for records whose name is in the masking
    namespace, so there is no re-entrancy and no masking of internal logs. The
    per-logger skip is a best-effort attach-time optimization; the record-name
    bypass is the real safety net, and the two share one predicate.
    """
    pairs = _snapshot_handler_pairs()
    added = 0
    for log, handler in pairs:
        if _is_masking_namespace_name(log.name):
            continue
        # Identity check: don't skip attaching because some OTHER
        # SecretMaskingFilter is present — that would let us miss handlers
        # we should cover, and on teardown we'd strip a filter we never added.
        # We only skip our own instance (re-install / refresh path).
        if masking_filter in handler.filters:
            continue
        handler.addFilter(masking_filter)
        _cover_handler(handler)
        added += 1
    if added:
        logger.debug(f"Installed SecretMaskingFilter on {added} handler(s)")


def _remove_filter_from_existing_handlers(masking_filter: SecretMaskingFilter) -> None:
    """Remove *our* masking filter instance from every handler it's on.

    Identity-based (not isinstance) so we never strip a different
    SecretMaskingFilter that someone else installed. Uses removeFilter (the
    logging API) rather than reassigning handler.filters. Iterates
    _snapshot_handler_pairs (handlers on loggers) AND _covered_handlers
    (handlers we attached to but which may not be on any logger — held by a
    QueueListener, nested inside another handler). Snapshots under
    the logging module lock so a concurrent addHandler/removeHandler can't
    mutate the lists under us.
    """
    pairs = _snapshot_handler_pairs()
    for _log, handler in pairs:
        while masking_filter in handler.filters:
            handler.removeFilter(masking_filter)
    # Handlers we covered but that aren't on any logger. Snapshot the weak
    # set under the leaf lock (same lock ``_cover_handler`` adds under) so a
    # concurrent add doesn't mutate it; the handlers themselves are alive
    # (we hold them via the snapshot).
    with _covered_lock:
        covered = list(_covered_handlers)
    for handler in covered:
        while masking_filter in handler.filters:
            handler.removeFilter(masking_filter)


def _is_real_stream(stream: object) -> bool:
    """True if ``stream`` is a real OS-backed stream safe to wrap.

    Fail-closed: if ``fileno()`` raises (pytest capture, celery
    ``LoggingProxy``, structlog, IPython, journald), we treat it as a
    non-real stream and skip wrapping. celery's ``LoggingProxy`` defines
    no ``fileno`` (it proxies to ``sys.stderr``/``sys.stdout``), so this
    guard refuses to wrap it. That matters because ``LoggingProxy.write``
    has a thread-local ``recurse_protection`` flag that returns 0 and
    drops the write on re-entry: wrapping the proxy would re-enter logging
    via the proxy, and the proxy would drop every masked write — the
    symptom is silently dropped output, not infinite recursion. Skipping
    the wrap costs nothing under a proxy: raw ``print()``/``stderr`` writes
    are converted to log records by the proxy and flow through handlers
    that carry the masking filter.
    """
    try:
        stream.fileno()  # type: ignore[attr-defined]
        return True
    except (io.UnsupportedOperation, OSError, ValueError, AttributeError):
        return False


def install_masking_filter(
    secret_registry: Optional[SecretRegistry] = None,
    max_message_size: int = 5000,
    install_stdout_wrapper: bool = True,
) -> SecretMaskingFilter:
    """Enable secret masking: install the filter on existing handlers (+ root
    logger), hook ``Handler.__init__`` so handlers created later are also
    covered, and, optionally, wrap sys.stdout/stderr for raw writes.

    Masking happens at the handler level (see _add_filter_to_existing_handlers).
    The one-shot covers handlers present at install time; the
    ``Handler.__init__`` hook covers handlers created later — ``basicConfig``
    after install, a library's lazy logging config, a per-task
    FileHandler/QueueHandler — so masking does not depend on install
    ordering. Streams are not repointed: under celery, ``sys.stderr`` is a
    ``LoggingProxy`` whose ``write`` has a thread-local ``recurse_protection``
    flag that returns 0 and drops the write on re-entry, so a handler pointed
    at it would have every record dropped (silently, no exception).

    Residual gaps (fail-open limitations of the security envelope):
    - A handler attached by direct ``logger.handlers.append(h)`` (bypassing
      ``addHandler``) where ``h`` was constructed before install and is not
      on any logger at install time: neither the one-shot nor the ``__init__``
      hook sees it. Rare pattern; not used by the executor or stdlib logging.
    - A handler whose ``__init__`` does not call ``logging.Handler.__init__``
      (rare third-party override): the hook doesn't fire for it.
    - Arbitrary objects in extras are not stringified-and-masked at filter
      time (would risk __str__ side effects); a formatter that serializes them
      via %s may emit them unmasked unless the output flows through the
      stdout/stderr wrapper.
    - A custom ``Formatter.formatException`` override is bypassed: filter()
      pre-fills ``record.exc_text``, and stdlib ``Formatter.format`` only
      calls ``formatException`` when ``exc_text`` is falsy. See
      ``_mask_exception`` for the trade.

    Note: the "already installed → refresh" path re-scans handlers, re-adds
    the root-logger filter if something removed it, and rebinds the
    registry if the caller passed a different one (with a warning).
    ``max_message_size`` on a repeat call is still ignored (the filter
    instance is reused); call ``SecretRegistry.reset_instance()`` first
    for a full re-install with new args.
    """
    global _installed_filter

    with _install_lock:
        root_logger = logging.getLogger()

        # Identity-based "already installed?" check. The previous isinstance check
        # could skip attaching because someone else's SecretMaskingFilter was
        # present, then strip that filter on teardown. Track our own instance.
        if _installed_filter is not None:
            # Already installed: re-scan to cover handlers added since (fail-open).
            _add_filter_to_existing_handlers(_installed_filter)

            # Re-add the root-logger filter if something removed it (partial
            # teardown state). Without this, a partially-torn-down state stays
            # partial: the handler filters are re-scanned but the root-logger
            # sentinel is gone, so the "already installed?" check on a later call
            # would re-install from scratch and attach a second filter.
            if _installed_filter not in root_logger.filters:
                root_logger.addFilter(_installed_filter)

            # Rebind the registry if the caller passed a different one. Without
            # this, install(r1) → reset_instance() → install(r2) leaves the
            # filter masking with r1 (now stale), so r2's secrets leak. The
            # filter is process-lifetime, but the registry it reads is not —
            # rebind so the same filter instance reads the new registry, and
            # reset _last_version to force a pattern rebuild on the next mask.
            if (
                secret_registry is not None
                and secret_registry is not _installed_filter._registry
            ):
                logger.warning(
                    "Rebinding SecretMaskingFilter registry on repeat install; "
                    "the previous registry is no longer active. "
                    "Use SecretRegistry.reset_instance() between installs to "
                    "fully tear down first."
                )
                _installed_filter.rebind_registry(secret_registry)

            # Honour install_stdout_wrapper on refresh. The wrapper block sits after
            # this path's early return, so a repeat install with
            # install_stdout_wrapper=True after an earlier install with False would
            # silently skip wrapping. The helper is idempotent and guarded by the
            # fileno() real-stream check, so calling it here is safe. We do NOT
            # unwrap when False is passed — teardown owns unwrapping.
            if install_stdout_wrapper:
                _wrap_std_streams(_installed_filter)

            logger.debug("SecretMaskingFilter already installed; refreshed handlers")
            return _installed_filter

        masking_filter = SecretMaskingFilter(
            secret_registry=secret_registry, max_message_size=max_message_size
        )

        # The root-logger filter is the "already installed?" sentinel (and masks
        # records logged directly on root). The real masking is the handler
        # filters below, since logger-level filters don't see propagated
        # child-logger records.
        root_logger.addFilter(masking_filter)
        _installed_filter = masking_filter
        # Hook Handler.__init__ before the handler scan so a handler
        # constructed between the two is covered by the hook rather than
        # missed by both. The scan's identity check prevents a double
        # attach for handlers the hook has already covered.
        _install_handler_init_hook()
        _add_filter_to_existing_handlers(masking_filter)
        logger.info(
            "Installed SecretMaskingFilter on root logger and existing handlers"
        )

        # Wrap stdout/stderr only to mask raw writes (print(), C-extension output).
        # We do NOT repoint handler streams here: under celery, sys.stderr is a
        # LoggingProxy whose write() has a thread-local recurse_protection
        # flag that returns 0 and drops the write on re-entry, so a handler
        # pointed at it would have every record dropped silently.
        # Skip wrapping when the stream is not a real OS-backed stream (proxy / pytest
        # capture / structlog / etc.) — wrapping a proxy re-enters logging. Under a
        # proxy, raw writes become log records and flow through handlers that carry
        # the masking filter, so coverage doesn't suffer.
        if install_stdout_wrapper:
            _wrap_std_streams(masking_filter)

        return masking_filter


def _wrap_std_streams(masking_filter: "SecretMaskingFilter") -> None:
    """Wrap sys.stdout/stderr with StreamMaskingWrapper if they are real
    OS-backed streams and not already wrapped. Idempotent and guarded by the
    ``fileno()`` real-stream check, so it is safe to call on the refresh path
    (a repeat install with ``install_stdout_wrapper=True`` after an earlier
    install with ``False``). Does NOT unwrap when ``install_stdout_wrapper``
    is False — teardown owns unwrapping."""
    if not isinstance(sys.stdout, StreamMaskingWrapper) and _is_real_stream(sys.stdout):
        sys.stdout = StreamMaskingWrapper(sys.stdout, masking_filter)
        logger.debug("Wrapped sys.stdout with StreamMaskingWrapper")
    else:
        # A skipped security control warrants at least info — debug is easy
        # to miss in production logs, and an operator who sees this knows
        # raw print()/C-extension output on stdout is not being masked by
        # the wrapper (it is still masked if it flows through a handler
        # that carries the filter, e.g. under a proxy).
        logger.info(
            "Skipped wrapping sys.stdout (not a real stream / already wrapped); "
            "raw writes are not masked by the stdout wrapper"
        )

    if not isinstance(sys.stderr, StreamMaskingWrapper) and _is_real_stream(sys.stderr):
        sys.stderr = StreamMaskingWrapper(sys.stderr, masking_filter)
        logger.debug("Wrapped sys.stderr with StreamMaskingWrapper")
    else:
        logger.info(
            "Skipped wrapping sys.stderr (not a real stream / already wrapped); "
            "raw writes are not masked by the stderr wrapper"
        )


def _uninstall_masking_filter() -> None:
    """Remove secret masking: detach the filter from the root logger and every
    handler it was attached to, unwrap stdout/stderr, and revert the
    ``Handler.__init__`` hook so future handlers don't auto-attach.

    Identity-based: only *our* installed instance is removed, so a different
    ``SecretMaskingFilter`` someone else installed survives. Symmetric with
    ``install_masking_filter`` (which attaches the filter, wraps streams, and
    installs the ``Handler.__init__`` hook).

    Step-wise tolerant: each mutation runs in its own try so a failure in step
    two does not skip three through five. A teardown that leaves masking
    *on* is fail-safe (no leak), so we swallow per-step errors and log them
    with ``exc_info=True`` rather than raising — raising from cleanup (often
    reached via ``finally`` or ``atexit``) destroys the original exception.
    The caller (``shutdown_secret_masking``) clears the bootstrap latch in
    its own ``finally`` so a partial teardown does not strand the latch
    ``True`` over a half-torn state.

    Module-private: this is the low-level teardown primitive that disarms
    masking process-wide. Callers should go through
    ``shutdown_secret_masking`` (the refcounted lifecycle), which only reaches
    this path when the last execution scope has ended. Calling this directly
    while execution scopes are active would uninstall the filter out from
    under live executions and silently unmask their logs.
    """
    global _installed_filter

    with _install_lock:
        root_logger = logging.getLogger()

        # 1. Remove our instance (identity-based) from the root logger.
        if _installed_filter is not None:
            try:
                while _installed_filter in root_logger.filters:
                    root_logger.removeFilter(_installed_filter)
            except Exception as e:
                logger.error(
                    "Failed to remove SecretMaskingFilter from root logger "
                    "during teardown: %r",
                    e,
                    exc_info=True,
                )

            # 2. Remove from every handler it was attached to.
            try:
                _remove_filter_from_existing_handlers(_installed_filter)
            except Exception as e:
                logger.error(
                    "Failed to remove SecretMaskingFilter from handlers "
                    "during teardown: %r",
                    e,
                    exc_info=True,
                )

            # 3. Drop the installed-filter global so the Handler.__init__ hook
            # stops attaching to new handlers.
            _installed_filter = None

        # 4. Revert the Handler.__init__ hook so handlers constructed after
        #    uninstall don't auto-attach a (now-None) filter. Done after
        #    _installed_filter is cleared so the hook's pointer read sees None
        #    during teardown.
        try:
            _uninstall_handler_init_hook()
        except Exception as e:
            logger.error(
                "Failed to revert Handler.__init__ hook during teardown: %r",
                e,
                exc_info=True,
            )

        # 5. Drop the covered-handler tracking set so a re-install starts clean.
        # ``_covered_lock`` (leaf) — see _cover_handler for the lock-order
        # invariant.
        try:
            with _covered_lock:
                _covered_handlers.clear()
        except Exception as e:
            logger.error(
                "Failed to clear covered-handler set during teardown: %r",
                e,
                exc_info=True,
            )

        # 6. Unwrap stdout/stderr (only if we wrapped them).
        try:
            if isinstance(sys.stdout, StreamMaskingWrapper):
                sys.stdout = sys.stdout._original
        except Exception as e:
            logger.error(
                "Failed to unwrap sys.stdout during teardown: %r", e, exc_info=True
            )

        try:
            if isinstance(sys.stderr, StreamMaskingWrapper):
                sys.stderr = sys.stderr._original
        except Exception as e:
            logger.error(
                "Failed to unwrap sys.stderr during teardown: %r", e, exc_info=True
            )

        logger.info("Uninstalled SecretMaskingFilter")


def uninstall_masking_filter() -> None:
    """Public teardown that refuses while execution scopes are active.

    The refcounted lifecycle is ``shutdown_secret_masking``; this primitive
    exists for direct callers (tests, integrations that bypass the
    refcount). A direct call disarms masking process-wide, so it refuses
    (and logs) when per-execution scopes are still open — otherwise it
    would uninstall the filter out from under live executions and
    silently unmask their logs. Internal callers that have already done
    the refcount check (``shutdown_secret_masking``,
    ``SecretRegistry.reset_instance``) call ``_uninstall_masking_filter``
    directly to bypass this guard.
    """
    registry = SecretRegistry.get_instance()
    if registry.has_active_executions():
        logger.warning(
            "uninstall_masking_filter() called while execution scopes are "
            "still active; refusing to disarm masking process-wide. Use "
            "shutdown_secret_masking(token) to end an execution, or call "
            "_uninstall_masking_filter() directly if you have already "
            "verified no executions are live."
        )
        return
    _uninstall_masking_filter()
