import datetime
import logging
import threading
import time
from typing import List, Optional, Tuple, TypedDict, TypeVar

import requests
import urllib3
from datahub.ingestion.graph.client import DataHubGraph
from tenacity import (
    RetryCallState,
    retry,
    wait_exponential,
)
from tenacity.stop import stop_base

from datahub_executor.common.client.config.graphql.query import (
    GRAPHQL_FETCH_EXECUTOR_CONFIGS,
)
from datahub_executor.common.helpers import create_datahub_graph
from datahub_executor.common.monitoring.base import METRIC
from datahub_executor.common.types import ExecutorConfig
from datahub_executor.config import (
    DATAHUB_EXECUTOR_CONFIG_FETCHER_MAX_RETRIES,
    DATAHUB_EXECUTOR_CONFIG_FETCHER_MAX_RETRY_DURATION_SECONDS,
    DATAHUB_EXECUTOR_MODE,
)

CREDENTIAL_EXPIRY_DELTA = 5

logger = logging.getLogger(__name__)


T = TypeVar("T")


class _RetrySequenceState(TypedDict):
    """Type definition for retry sequence state stored in _retry_sequences."""

    category: str
    start_time: float
    attempt_at_category_start: int


# Exception type tuple for retry logic - used by _categorize_exception
# These are the long-term retryable exceptions (timeouts, connection errors, urllib3 errors)
_LONG_TERM_RETRY_EXCEPTIONS = (
    requests.exceptions.Timeout,
    urllib3.exceptions.ReadTimeoutError,
    urllib3.exceptions.ConnectTimeoutError,
    TimeoutError,
    requests.exceptions.ConnectionError,
    ConnectionError,
    urllib3.exceptions.MaxRetryError,
    urllib3.exceptions.NewConnectionError,
    urllib3.exceptions.ProtocolError,
)


def _collect_exception_chain(exc: BaseException, seen: set) -> list[BaseException]:
    """
    Recursively collect all exceptions in the chain (including __cause__ and __context__).

    Args:
        exc: The exception to start collecting from
        seen: Set of exception IDs already seen (to prevent cycles)

    Returns:
        List of all exceptions in the chain
    """
    if exc is None or id(exc) in seen:
        return []
    seen.add(id(exc))
    chain: list[BaseException] = [exc]
    if exc.__cause__:
        chain.extend(_collect_exception_chain(exc.__cause__, seen))
    if exc.__context__:
        chain.extend(_collect_exception_chain(exc.__context__, seen))
    return chain


def _categorize_exception(exception: BaseException | None) -> str:
    """
    Categorize exception into retry policy categories.

    Returns:
        "long_term" - for timeouts and connection errors (use duration-based retry)
        "short_term" - for other retryable exceptions (use attempt-based retry)
    """
    if exception is None:
        return "short_term"

    exceptions_to_check = _collect_exception_chain(exception, set())

    for exc in exceptions_to_check:
        # Long-term retry: timeouts and connection errors
        if isinstance(exc, _LONG_TERM_RETRY_EXCEPTIONS):
            return "long_term"

    # Short-term retry: other retryable exceptions
    return "short_term"


class ExceptionTypeBasedStop(stop_base):
    """
    Stop condition that applies different retry policies based on exception type.

    - Long-term exceptions (timeouts, connection errors): Use duration-based limit (24 hours)
    - Short-term exceptions: Use attempt-based limit (max retries)

    When exception type changes, the retry policy switches and counters reset.

    Note: This class maintains instance-level mutable state (_retry_sequences) that is
    keyed by retry_state object IDs. When an instance is created at module scope and
    shared across requests, this state persists across requests. However, since each
    retry sequence is keyed by a unique retry_state object ID, state isolation is
    maintained per retry sequence. Stale entries are cleaned up periodically to prevent
    memory growth.
    """

    def __init__(self, max_delay: float, max_attempts: int):
        self.max_delay = max_delay
        self.max_attempts = max_attempts
        # Track state per retry sequence: {retry_id: {"category": str, "start_time": float, "attempt_at_category_start": int}}
        # Protected by _retry_sequences_lock for thread-safe access
        # Note: This is instance-level mutable state that persists across requests when the instance
        # is shared. State is isolated per retry sequence via unique retry_state object IDs.
        self._retry_sequences: dict[int, _RetrySequenceState] = {}
        self._retry_sequences_lock = threading.Lock()
        # Cleanup threshold: remove entries older than max_delay + buffer
        # This ensures we only clean up truly stale entries (from successful retries)
        # and don't delete active retry sequences that are still within their retry window
        # Buffer of 1 hour to account for any timing edge cases
        self._cleanup_threshold_seconds = max_delay + 3600.0

    def __call__(self, retry_state) -> bool:
        retry_id = id(retry_state)

        # If the retry succeeded, clean up the sequence entry
        # Note: tenacity may not call stop condition on success, but we handle it if it does
        if retry_state.outcome and not retry_state.outcome.failed:
            with self._retry_sequences_lock:
                if retry_id in self._retry_sequences:
                    del self._retry_sequences[retry_id]
            return False

        # Only process failures
        if not retry_state.outcome or not retry_state.outcome.failed:
            return False

        # Periodic cleanup: remove stale entries (from successful retries that weren't cleaned up)
        # This prevents memory growth if tenacity doesn't call stop condition on success
        # Note: Cleanup happens before checking if entry exists, but cleanup threshold is
        # set to max_delay + buffer, so active retry sequences won't be deleted
        self._cleanup_stale_entries()

        exception = retry_state.outcome.exception()
        # exception() can return BaseException | None, but we handle None in _categorize_exception
        exception_category = _categorize_exception(exception)  # type: ignore[arg-type]

        # Thread-safe access to retry sequence state
        category_changed = False
        old_category = None
        should_stop = False
        elapsed = 0.0
        attempts_for_category = 0

        with self._retry_sequences_lock:
            # Initialize or get retry sequence state
            if retry_id not in self._retry_sequences:
                self._retry_sequences[retry_id] = {
                    "category": exception_category,
                    "start_time": time.time(),
                    "attempt_at_category_start": retry_state.attempt_number,
                }
            else:
                sequence = self._retry_sequences[retry_id]
                # If exception category changed, reset counters for new policy
                if sequence["category"] != exception_category:
                    old_category = sequence["category"]
                    category_changed = True
                    sequence["category"] = exception_category
                    sequence["start_time"] = time.time()
                    sequence["attempt_at_category_start"] = retry_state.attempt_number

            sequence = self._retry_sequences[retry_id]
            current_time = time.time()
            elapsed = current_time - sequence["start_time"]
            # Calculate attempts for current category: current attempt - attempt when category started + 1
            attempts_for_category = (
                retry_state.attempt_number - sequence["attempt_at_category_start"] + 1
            )

            # Apply policy based on exception category
            if sequence["category"] == "long_term":
                # Long-term: stop after duration limit
                should_stop = elapsed >= self.max_delay
            else:
                # Short-term: stop after attempt limit
                should_stop = attempts_for_category >= self.max_attempts

            # Clean up when stopping
            if should_stop and retry_id in self._retry_sequences:
                del self._retry_sequences[retry_id]

        # Log outside lock to reduce contention
        if category_changed:
            logger.info(
                f"Exception category changed from {old_category} to {exception_category}, "
                f"resetting retry policy and counters"
            )

        if should_stop:
            if exception_category == "long_term":
                logger.warning(
                    f"Stopping retries after {elapsed:.1f} seconds (duration limit reached) "
                    f"for long-term exception category"
                )
            else:
                logger.warning(
                    f"Stopping retries after {attempts_for_category} attempts (attempt limit reached) "
                    f"for short-term exception category"
                )

        return should_stop

    def _cleanup_stale_entries(self) -> None:
        """
        Remove stale entries from _retry_sequences that are older than cleanup threshold.

        This prevents memory growth from successful retries that weren't cleaned up
        (since tenacity may not call stop condition on success).
        """
        current_time = time.time()
        with self._retry_sequences_lock:
            stale_ids = [
                retry_id
                for retry_id, sequence in self._retry_sequences.items()
                if (current_time - sequence["start_time"])
                > self._cleanup_threshold_seconds
            ]
            for retry_id in stale_ids:
                del self._retry_sequences[retry_id]
                logger.debug(f"Cleaned up stale retry sequence entry: {retry_id}")


def _build_retry_decorator(resolver: "ExecutorConfigResolver"):
    """
    Build retry decorator with exponential backoff and exception-type-based retry policies.

    Only retries on transient errors (timeouts, connection issues) based on exception types.
    Does NOT retry on HTTP error codes (4xx, 5xx).

    Retry policy is determined by the last exception type:
    - Long-term exceptions (timeouts, connection errors): Stop after max duration (default: 24 hours)
    - Short-term exceptions (other retryable): Stop after max attempts (default: 100 attempts)

    When exception type changes, the retry policy switches and counters reset.

    Args:
        resolver: The ExecutorConfigResolver instance to use for retry state tracking
    """

    def _before_sleep_callback(retry_state: RetryCallState) -> None:
        """
        Custom before_sleep callback that logs warnings and updates retry state.

        This ensures that:
        1. We log warnings (not errors) during retries
        2. We track retry activity so health checks know we're actively retrying
        3. We track exception category changes to update retry policy

        Important: This callback is ONLY called when a retry is about to happen.
        All exceptions are retried (tenacity's default behavior), and the retry policy
        (long_term vs short_term) is determined by the exception category:
        - Long-term exceptions (timeouts, connection errors) → this callback runs → last_retry_attempt updated
        - Short-term exceptions (HTTP errors, others) → this callback runs → last_retry_attempt NOT updated
        """
        if retry_state.outcome and retry_state.outcome.failed:
            exception = retry_state.outcome.exception()
            attempt_number = retry_state.attempt_number
            exception_category = _categorize_exception(exception)

            # Thread-safe update of retry state
            with resolver._retry_state_lock:
                # Get current category before updating
                current_category = resolver._retry_state.get(
                    "current_exception_category"
                )

                # Check if exception category changed
                # Only consider it a change if:
                # 1. current_category is not None (was previously set)
                # 2. current_category != exception_category (actually different)
                # This prevents false positives when current_category is None (first attempt or after reset)
                category_changed = (
                    current_category is not None
                    and current_category != exception_category
                )

                # Update retry state - only long-term exceptions update last_retry_attempt
                # Short-term exceptions don't update it since they'll stop after limited attempts
                resolver._retry_state["current_exception_category"] = exception_category
                if exception_category == "long_term":
                    resolver._retry_state["last_retry_attempt"] = time.time()
                    resolver._retry_state["is_retrying"] = True
                else:
                    # Short-term exceptions don't update last_retry_attempt
                    # They'll stop after max attempts, so we don't want to keep service "alive" for health checks
                    resolver._retry_state["is_retrying"] = False

            # Log category change outside lock to reduce contention
            if category_changed:
                logger.info(
                    f"Exception category changed to {exception_category}, retry policy updated accordingly"
                )

            # Log warning with retry information based on category
            wait_time = retry_state.next_action.sleep if retry_state.next_action else 0
            if exception_category == "long_term":
                max_duration_hours = (
                    DATAHUB_EXECUTOR_CONFIG_FETCHER_MAX_RETRY_DURATION_SECONDS / 3600
                )
                logger.warning(
                    f"Retrying datahub_executor.common.client.config.resolver.ExecutorConfigResolver._fetch_executor_configs "
                    f"in {wait_time:.1f} seconds (attempt {attempt_number}, category: long_term, "
                    f"max duration: {max_duration_hours:.1f} hours) as it raised {type(exception).__name__}: {exception}"
                )
            else:
                logger.warning(
                    f"Retrying datahub_executor.common.client.config.resolver.ExecutorConfigResolver._fetch_executor_configs "
                    f"in {wait_time:.1f} seconds (attempt {attempt_number}/{DATAHUB_EXECUTOR_CONFIG_FETCHER_MAX_RETRIES}, "
                    f"category: short_term) as it raised {type(exception).__name__}: {exception}"
                )

    # Create ExceptionTypeBasedStop instance at function scope (not module scope)
    # This ensures each call to _build_retry_decorator() gets a fresh instance, though
    # in practice this function is only called once at class definition time.
    # The instance maintains mutable state (_retry_sequences) that is keyed by
    # retry_state object IDs, providing isolation per retry sequence.
    stop_condition = ExceptionTypeBasedStop(
        DATAHUB_EXECUTOR_CONFIG_FETCHER_MAX_RETRY_DURATION_SECONDS,
        DATAHUB_EXECUTOR_CONFIG_FETCHER_MAX_RETRIES,
    )

    def _retry_error_callback(retry_state: RetryCallState) -> None:
        """Called when retries are exhausted. Log metric and re-raise the exception."""
        METRIC("WORKER_CONFIG_FETCHER_ERRORS", exception="Retry").inc()
        # Re-raise the last exception instead of returning None
        if retry_state.outcome is not None:
            exc = retry_state.outcome.exception()
            if exc is not None:
                raise exc
        raise Exception("Retry exhausted with no exception captured")

    # No "retry" condition specified - tenacity will retry on all exceptions by default
    # The stop condition (ExceptionTypeBasedStop) determines when to stop retrying
    return retry(
        wait=wait_exponential(multiplier=2, min=4, max=60),
        before_sleep=_before_sleep_callback,
        stop=stop_condition,
        retry_error_callback=_retry_error_callback,
    )


class ExecutorConfigResolver:
    """Resolver class responsible for resolving/fetching executor configuration"""

    graph: DataHubGraph
    executor_configs: List[ExecutorConfig]
    # Instance-level state to track retry activity for health checks
    # Protected by _retry_state_lock for thread-safe access
    _retry_state: dict
    _retry_state_lock: threading.Lock
    # Lock for thread-safe access to executor_configs
    _executor_configs_lock: threading.Lock

    def __init__(self, graph: Optional[DataHubGraph] = None) -> None:
        if graph:
            self.graph = graph
        else:
            self.graph = create_datahub_graph()
        self.executor_configs = []
        # Initialize retry state as instance-level state
        self._retry_state = {
            "last_retry_attempt": None,
            "is_retrying": False,
            "current_exception_category": None,
        }
        self._retry_state_lock = threading.Lock()
        self._executor_configs_lock = threading.Lock()
        # Build retry decorator for this instance and apply it to _fetch_executor_configs
        retry_decorator = _build_retry_decorator(self)
        # Get the original function from the class (it's already a function, not a method)
        original_function = self.__class__._fetch_executor_configs
        # Apply the decorator to create a wrapped function
        wrapped_function = retry_decorator(original_function)
        # Rebind the wrapped function as a method on this instance
        import types

        self._fetch_executor_configs = types.MethodType(wrapped_function, self)

    def is_actively_retrying(self, max_staleness_seconds: int = 300) -> bool:
        """
        Check if the resolver is actively retrying (for health check purposes).

        Returns True if a retry attempt occurred within the last max_staleness_seconds.
        This allows health checks to consider the service as "alive" even when
        retrying, preventing premature pod restarts.

        Args:
            max_staleness_seconds: Maximum seconds since last retry attempt to consider as "actively retrying"

        Returns:
            True if actively retrying, False otherwise
        """
        # Thread-safe read of last_retry_attempt
        with self._retry_state_lock:
            last_retry_attempt = self._retry_state["last_retry_attempt"]

        # Compute comparison outside lock to reduce contention
        if last_retry_attempt is None:
            return False
        return (time.time() - last_retry_attempt) < max_staleness_seconds

    def get_executor_configs(self) -> List[ExecutorConfig]:
        """
        Get executor configs, fetching if not already cached.

        Thread-safe: uses lock to prevent race conditions when checking and setting.
        """
        with self._executor_configs_lock:
            if self.executor_configs:
                return list(self.executor_configs)  # Return copy for thread safety

        # Fetch outside lock to avoid deadlock, then acquire lock to write
        # Note: fetch_executor_configs() will acquire the lock internally when writing
        fetched_configs = self.fetch_executor_configs()

        # Double-check pattern: another thread might have set it while we were fetching
        with self._executor_configs_lock:
            if not self.executor_configs:
                self.executor_configs = fetched_configs
            return list(self.executor_configs)  # Return copy for thread safety

    def refresh_executor_configs(self) -> Tuple[bool, List[ExecutorConfig]]:
        """
        Refresh executor configs if they're expiring or missing.

        Thread-safe: uses lock to prevent concurrent modification during iteration and update.
        """
        with self._executor_configs_lock:
            # Make a copy of the list to iterate over while holding the lock
            # This prevents concurrent modification during iteration
            current_configs = list(self.executor_configs)
            needs_refresh = not self.executor_configs

        # Check expiration outside lock to reduce lock contention
        expiring = False
        for creds in current_configs:
            if (
                creds.expiration
                and datetime.datetime.now(datetime.timezone.utc)
                + datetime.timedelta(minutes=CREDENTIAL_EXPIRY_DELTA)
                > creds.expiration
            ):
                expiring = True
                needs_refresh = True

        if not needs_refresh:
            # Return current configs (re-acquire lock for thread-safe read)
            with self._executor_configs_lock:
                return expiring, list(self.executor_configs)

        # Fetch outside lock to avoid deadlock
        # Note: fetch_executor_configs() will acquire the lock internally when writing
        logger.info("Refreshing executor_configs - fetching new from GMS")
        fetched_configs = self.fetch_executor_configs()

        # Acquire lock to update
        with self._executor_configs_lock:
            # Double-check: another thread might have refreshed while we were fetching
            if not self.executor_configs or expiring:
                self.executor_configs = fetched_configs
            return True, list(self.executor_configs)

    def fetch_executor_configs(self) -> List[ExecutorConfig]:
        return self._fetch_executor_configs()

    @METRIC("WORKER_CONFIG_FETCHER_REQUESTS").time()  # type: ignore
    def _fetch_executor_configs(self) -> List[ExecutorConfig]:
        # Reset is_retrying flag at the start of each attempt (including retries)
        # Note: We do NOT reset current_exception_category here because tenacity
        # re-executes this method on retries, which would cause the category change
        # detection in _before_sleep_callback to always trigger. The category is
        # only reset on successful completion or when explicitly changed.
        with self._retry_state_lock:
            self._retry_state["is_retrying"] = False

        result = self.graph.execute_graphql(GRAPHQL_FETCH_EXECUTOR_CONFIGS)

        # Validate result before clearing retry state
        # If validation fails, an exception will be raised and retried, so we should
        # preserve the retry state until we're certain the fetch was successful
        if "error" in result and result["error"] is not None:
            METRIC("WORKER_CONFIG_FETCHER_ERRORS", exception="GmsError").inc()
            raise Exception(
                f"Received error while fetching executor_configs from GMS! {result.get('error')}"
            )

        if (
            "listExecutorConfigs" not in result
            or "executorConfigs" not in result["listExecutorConfigs"]
        ):
            METRIC("WORKER_CONFIG_FETCHER_ERRORS", exception="IncompleteResults").inc()
            raise Exception(
                "Found incomplete search results when fetching executor_configs from GMS!"
            )

        # In worker mode, having no queues means that worker never has work to do, so we have to retry until we get any
        if (
            DATAHUB_EXECUTOR_MODE != "coordinator"
            and len(result["listExecutorConfigs"]["executorConfigs"]) == 0
        ):
            METRIC("WORKER_CONFIG_FETCHER_ERRORS", exception="EmptyConfig").inc()
            raise Exception("GMS returned no executor configs, unable to proceed.")

        executor_configs = []
        for credential in result["listExecutorConfigs"]["executorConfigs"]:
            try:
                executor_configs.append(ExecutorConfig.model_validate(credential))
            except Exception:
                METRIC("WORKER_CONFIG_FETCHER_ERRORS", exception="ParseError").inc()
                raise Exception(
                    f"Failed to convert ExecutorConfig object to Python object. {credential}"
                )

        # Thread-safe write to executor_configs
        with self._executor_configs_lock:
            self.executor_configs = executor_configs
            # Return copy while holding lock for thread safety
            configs_copy = list(self.executor_configs)

        # Successfully fetched and validated - clear retry state to indicate we're no longer retrying
        # This must happen AFTER all validations pass, otherwise if validation fails and an exception
        # is raised, the retry state would have been prematurely cleared, causing is_actively_retrying()
        # to return False even though retries are ongoing.
        with self._retry_state_lock:
            self._retry_state["is_retrying"] = False
            self._retry_state["last_retry_attempt"] = None
            self._retry_state["current_exception_category"] = None

        return configs_copy
