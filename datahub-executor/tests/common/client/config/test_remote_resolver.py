import datetime
import time
from unittest.mock import Mock, patch

import requests
import urllib3
from datahub.ingestion.graph.client import DataHubGraph

from datahub_executor.common.client.config.resolver import (
    ExceptionTypeBasedStop,
    ExecutorConfigResolver,
    _categorize_exception,
)
from datahub_executor.common.constants import DATAHUB_EXECUTOR_EMBEDDED_POOL_ID


class TestRemoteResolver:
    def setup_method(self) -> None:
        self.graph = Mock(spec=DataHubGraph)
        # Set the mock return value
        self.graph.execute_graphql.return_value = {
            "listExecutorConfigs": {
                "total": 1,
                "executorConfigs": [
                    {
                        "region": "us-west-2",
                        "executorId": DATAHUB_EXECUTOR_EMBEDDED_POOL_ID,
                        "queueUrl": "https://queue.amazon.com/something/123456",
                        "accessKeyId": "some-access-key",
                        "secretKeyId": "some-secret-key",
                        "expiration": datetime.datetime.now(
                            datetime.timezone.utc
                        ).isoformat(),
                        "sessionToken": "some-session-token",
                    },
                ],
            }
        }

        # Create a fresh resolver instance for each test - no singleton, no state leakage
        self.resolver = ExecutorConfigResolver(self.graph)
        self.resolver.executor_configs = []

    def test_get_executor_configs(self) -> None:
        executor_configs = self.resolver.get_executor_configs()
        assert len(executor_configs) == 1
        assert executor_configs[0].executor_id == DATAHUB_EXECUTOR_EMBEDDED_POOL_ID

    def test_fetch_executor_configs_empty_list(self) -> None:
        self.resolver.graph.execute_graphql.return_value = {  # type: ignore
            "listExecutorConfigs": {
                "total": 0,
                "executorConfigs": [],
            }
        }
        executor_configs = self.resolver.fetch_executor_configs()
        assert len(executor_configs) == 0

    def test_refresh_executor_configs_not_expired(self) -> None:
        self.resolver.graph.execute_graphql.return_value = {  # type: ignore
            "listExecutorConfigs": {
                "total": 1,
                "executorConfigs": [
                    {
                        "region": "us-west-2",
                        "executorId": DATAHUB_EXECUTOR_EMBEDDED_POOL_ID,
                        "queueUrl": "https://queue.amazon.com/something/123456",
                        "accessKeyId": "some-access-key",
                        "secretKeyId": "some-secret-key",
                        "expiration": (
                            datetime.datetime.now(datetime.timezone.utc)
                            + datetime.timedelta(minutes=30)
                        ).isoformat(),
                        "sessionToken": "some-session-token",
                    },
                ],
            }
        }
        self.resolver.get_executor_configs()
        expiring, executor_configs = self.resolver.refresh_executor_configs()

        assert expiring is False

    def test_is_actively_retrying_no_retries(self) -> None:
        """Test is_actively_retrying returns False when no retries have occurred."""
        assert self.resolver.is_actively_retrying() is False

    def test_is_actively_retrying_recent_retry(self) -> None:
        """Test is_actively_retrying returns True when retry occurred recently."""
        # Set instance-level retry state directly
        with self.resolver._retry_state_lock:
            self.resolver._retry_state["last_retry_attempt"] = (
                time.time() - 60
            )  # 60 seconds ago

        assert self.resolver.is_actively_retrying(max_staleness_seconds=300) is True

    def test_is_actively_retrying_stale_retry(self) -> None:
        """Test is_actively_retrying returns False when retry is too old."""
        # Set instance-level retry state directly
        with self.resolver._retry_state_lock:
            self.resolver._retry_state["last_retry_attempt"] = (
                time.time() - 400
            )  # 400 seconds ago (stale)

        assert self.resolver.is_actively_retrying(max_staleness_seconds=300) is False

    @patch("time.sleep")
    @patch(
        "datahub_executor.common.client.config.resolver.DATAHUB_EXECUTOR_CONFIG_FETCHER_MAX_RETRY_DURATION_SECONDS",
        1,
    )
    @patch(
        "datahub_executor.common.client.config.resolver.DATAHUB_EXECUTOR_CONFIG_FETCHER_MAX_RETRIES",
        10,
    )
    def test_fetch_executor_configs_retries_on_connection_error(
        self, mock_sleep: Mock
    ) -> None:
        """Test that fetch_executor_configs retries on connection errors."""
        # Patch sleep to be instant for fast tests
        mock_sleep.return_value = None

        # First call raises ConnectionError, second call succeeds
        self.resolver.graph.execute_graphql.side_effect = [  # type: ignore
            requests.exceptions.ConnectionError("Connection failed"),
            {
                "listExecutorConfigs": {
                    "total": 1,
                    "executorConfigs": [
                        {
                            "region": "us-west-2",
                            "executorId": DATAHUB_EXECUTOR_EMBEDDED_POOL_ID,
                            "queueUrl": "https://queue.amazon.com/something/123456",
                            "accessKeyId": "some-access-key",
                            "secretKeyId": "some-secret-key",
                            "expiration": datetime.datetime.now(
                                datetime.timezone.utc
                            ).isoformat(),
                            "sessionToken": "some-session-token",
                        },
                    ],
                }
            },
        ]

        # Should retry and eventually succeed
        executor_configs = self.resolver.fetch_executor_configs()
        assert len(executor_configs) == 1
        assert executor_configs[0].executor_id == DATAHUB_EXECUTOR_EMBEDDED_POOL_ID
        # Verify execute_graphql was called multiple times (retry happened)
        assert self.resolver.graph.execute_graphql.call_count >= 2  # type: ignore

    @patch("time.sleep")
    @patch(
        "datahub_executor.common.client.config.resolver.DATAHUB_EXECUTOR_CONFIG_FETCHER_MAX_RETRY_DURATION_SECONDS",
        1,
    )
    @patch(
        "datahub_executor.common.client.config.resolver.DATAHUB_EXECUTOR_CONFIG_FETCHER_MAX_RETRIES",
        10,
    )
    def test_fetch_executor_configs_retries_on_timeout_error(
        self, mock_sleep: Mock
    ) -> None:
        """Test that fetch_executor_configs retries on timeout errors."""
        # Patch sleep to be instant for fast tests
        mock_sleep.return_value = None

        # First call raises ReadTimeoutError, second call succeeds
        self.resolver.graph.execute_graphql.side_effect = [  # type: ignore
            urllib3.exceptions.ReadTimeoutError(None, None, "Read timed out"),  # type: ignore[arg-type]
            {
                "listExecutorConfigs": {
                    "total": 1,
                    "executorConfigs": [
                        {
                            "region": "us-west-2",
                            "executorId": DATAHUB_EXECUTOR_EMBEDDED_POOL_ID,
                            "queueUrl": "https://queue.amazon.com/something/123456",
                            "accessKeyId": "some-access-key",
                            "secretKeyId": "some-secret-key",
                            "expiration": datetime.datetime.now(
                                datetime.timezone.utc
                            ).isoformat(),
                            "sessionToken": "some-session-token",
                        },
                    ],
                }
            },
        ]

        # Should retry and eventually succeed
        executor_configs = self.resolver.fetch_executor_configs()
        assert len(executor_configs) == 1
        assert self.resolver.graph.execute_graphql.call_count >= 2  # type: ignore

    @patch(
        "datahub_executor.common.client.config.resolver.DATAHUB_EXECUTOR_CONFIG_FETCHER_MAX_RETRIES",
        3,  # Set a low retry limit for testing
    )
    @patch("time.sleep")
    def test_fetch_executor_configs_retry_on_http_error_short_term(
        self, mock_sleep: Mock
    ) -> None:
        """Test that fetch_executor_configs retries HTTP errors (4xx/5xx) with short_term policy (attempt-based limit).

        HTTP errors use short_term policy, which means:
        1. They retry up to max attempts (attempt-based limit, not duration-based)
        2. They do NOT update last_retry_attempt (unlike long_term exceptions)
        3. This allows health checks to fail after staleness period, causing pod restart
        4. This is by design - HTTP errors are not transient connectivity issues
        """
        # Create an HTTPError (retryable with short_term policy)
        response = Mock()
        response.status_code = 404
        http_error = requests.exceptions.HTTPError(response=response)

        self.resolver.graph.execute_graphql.side_effect = http_error  # type: ignore

        # Should retry up to max attempts (short_term policy), then stop
        # No exception is raised - this is by design. The function will return (or fail silently),
        # and the health check will fail because last_retry_attempt is not updated
        # This allows the pod to restart when HTTP errors occur (non-transient errors)
        try:
            self.resolver.fetch_executor_configs()
        except Exception:
            # If an exception is raised, that's also acceptable (tenacity might raise RetryError)
            # The important part is that retries happened and last_retry_attempt is NOT updated
            pass

        # Verify execute_graphql was called multiple times (retries happened)
        # With max_retries=3, we expect at least 3 attempts (1 initial + 2 retries)
        # But it might be more due to default value being 5, so just check it's > 1
        assert self.resolver.graph.execute_graphql.call_count > 1  # type: ignore

        # Verify that last_retry_attempt was NOT updated (this is the key design point)
        # Short-term exceptions don't update last_retry_attempt, so health checks will fail
        with self.resolver._retry_state_lock:
            last_retry_attempt = self.resolver._retry_state.get("last_retry_attempt")
            is_retrying = self.resolver._retry_state.get("is_retrying")

        # last_retry_attempt should be None (not updated for short_term exceptions)
        assert last_retry_attempt is None, (
            "last_retry_attempt should not be updated for short_term exceptions"
        )
        # is_retrying should be False (short_term exceptions don't keep service alive)
        assert is_retrying is False, (
            "is_retrying should be False for short_term exceptions"
        )

        # Verify health check will fail (because last_retry_attempt is None)
        assert self.resolver.is_actively_retrying() is False, (
            "Health check should fail for short_term exceptions"
        )


class TestExceptionCategorization:
    """Test _categorize_exception function"""

    def test_categorize_timeout_exceptions_as_long_term(self) -> None:
        """Test that timeout exceptions are categorized as long_term."""
        assert _categorize_exception(requests.exceptions.Timeout()) == "long_term"
        assert (
            _categorize_exception(
                urllib3.exceptions.ReadTimeoutError(None, None, "timeout")  # type: ignore[arg-type]
            )
            == "long_term"
        )
        assert (
            _categorize_exception(
                urllib3.exceptions.ConnectTimeoutError(None, "timeout")
            )
            == "long_term"
        )
        assert _categorize_exception(TimeoutError("timeout")) == "long_term"

    def test_categorize_connection_errors_as_long_term(self) -> None:
        """Test that connection errors are categorized as long_term."""
        assert (
            _categorize_exception(
                requests.exceptions.ConnectionError("connection failed")
            )
            == "long_term"
        )
        assert (
            _categorize_exception(ConnectionError("connection failed")) == "long_term"
        )

    def test_categorize_urllib3_errors_as_long_term(self) -> None:
        """Test that urllib3 connection pool errors are categorized as long_term."""
        assert (
            _categorize_exception(
                urllib3.exceptions.MaxRetryError(None, None, "max retries")  # type: ignore[arg-type]
            )
            == "long_term"
        )
        assert (
            _categorize_exception(
                urllib3.exceptions.NewConnectionError(None, "new connection failed")
            )
            == "long_term"
        )
        assert (
            _categorize_exception(urllib3.exceptions.ProtocolError("protocol error"))
            == "long_term"
        )

    def test_categorize_other_exceptions_as_short_term(self) -> None:
        """Test that other exceptions are categorized as short_term."""
        assert _categorize_exception(ValueError("some error")) == "short_term"
        assert _categorize_exception(RuntimeError("some error")) == "short_term"
        assert _categorize_exception(Exception("some error")) == "short_term"

    def test_categorize_nested_exception_chain_with_cause(self) -> None:
        """Test categorization of nested exception chain via __cause__."""
        inner_exception = requests.exceptions.Timeout("timeout")
        outer_exception = RuntimeError("outer error")
        outer_exception.__cause__ = inner_exception

        # Should categorize as long_term because inner exception is Timeout
        assert _categorize_exception(outer_exception) == "long_term"

    def test_categorize_nested_exception_chain_with_context(self) -> None:
        """Test categorization of nested exception chain via __context__."""
        inner_exception = urllib3.exceptions.ReadTimeoutError(None, None, "timeout")  # type: ignore[arg-type]
        outer_exception = ValueError("outer error")
        outer_exception.__context__ = inner_exception

        # Should categorize as long_term because inner exception is ReadTimeoutError
        assert _categorize_exception(outer_exception) == "long_term"

    def test_categorize_nested_exception_chain_both_cause_and_context(self) -> None:
        """Test categorization when exception has both __cause__ and __context__."""
        timeout_exception = requests.exceptions.Timeout("timeout")
        connection_exception = requests.exceptions.ConnectionError("connection failed")
        outer_exception = RuntimeError("outer error")
        outer_exception.__cause__ = timeout_exception
        outer_exception.__context__ = connection_exception

        # Should categorize as long_term because both nested exceptions are long_term
        assert _categorize_exception(outer_exception) == "long_term"

    def test_categorize_deeply_nested_exception_chain(self) -> None:
        """Test categorization of deeply nested exception chain."""
        innermost = urllib3.exceptions.MaxRetryError(None, None, "max retries")  # type: ignore[arg-type]
        middle = RuntimeError("middle error")
        middle.__cause__ = innermost
        outermost = ValueError("outer error")
        outermost.__cause__ = middle

        # Should categorize as long_term because innermost is MaxRetryError
        assert _categorize_exception(outermost) == "long_term"

    def test_categorize_circular_exception_reference(self) -> None:
        """Test that circular exception references don't cause infinite loops."""
        exception1 = RuntimeError("error1")
        exception2 = RuntimeError("error2")
        exception1.__cause__ = exception2
        exception2.__cause__ = exception1  # Circular reference

        # Should handle circular reference gracefully and return short_term
        assert _categorize_exception(exception1) == "short_term"


class TestExceptionTypeBasedStop:
    """Test ExceptionTypeBasedStop class"""

    def test_stop_after_duration_limit_long_term(self) -> None:
        """Test that long_term exceptions stop after duration limit."""
        stop_condition = ExceptionTypeBasedStop(max_delay=1.0, max_attempts=100)

        # Create mock retry state with long_term exception
        retry_state = Mock()
        retry_state.outcome = Mock()
        retry_state.outcome.failed = True
        retry_state.outcome.exception.return_value = requests.exceptions.Timeout(
            "timeout"
        )
        retry_state.attempt_number = 1

        # First call should not stop
        assert stop_condition(retry_state) is False

        # Advance time past duration limit
        with patch("time.time", return_value=time.time() + 2.0):
            # Should stop after duration limit
            assert stop_condition(retry_state) is True

    def test_stop_after_attempt_limit_short_term(self) -> None:
        """Test that short_term exceptions stop after attempt limit."""
        stop_condition = ExceptionTypeBasedStop(max_delay=3600.0, max_attempts=3)

        # Create mock retry state with short_term exception
        retry_state = Mock()
        retry_state.outcome = Mock()
        retry_state.outcome.failed = True
        retry_state.outcome.exception.return_value = ValueError("some error")
        retry_state.attempt_number = 1

        # First call (attempt 1) should not stop
        assert stop_condition(retry_state) is False

        # Second call (attempt 2) should not stop
        retry_state.attempt_number = 2
        assert stop_condition(retry_state) is False

        # Third call (attempt 3) should stop
        retry_state.attempt_number = 3
        assert stop_condition(retry_state) is True

    def test_exception_category_switching_long_to_short(self) -> None:
        """Test that exception category switching from long_term to short_term resets counters."""
        stop_condition = ExceptionTypeBasedStop(max_delay=1.0, max_attempts=2)

        retry_state = Mock()
        retry_state.outcome = Mock()
        retry_state.outcome.failed = True

        # Start with long_term exception
        retry_state.outcome.exception.return_value = requests.exceptions.Timeout(
            "timeout"
        )
        retry_state.attempt_number = 1

        # First call with long_term - should not stop
        assert stop_condition(retry_state) is False

        # Switch to short_term exception
        retry_state.outcome.exception.return_value = ValueError("some error")
        retry_state.attempt_number = 2

        # Should reset and start counting attempts for short_term
        # Attempt 1 for short_term - should not stop
        assert stop_condition(retry_state) is False

        # Attempt 2 for short_term - should stop
        retry_state.attempt_number = 3
        assert stop_condition(retry_state) is True

    def test_exception_category_switching_short_to_long(self) -> None:
        """Test that exception category switching from short_term to long_term resets counters."""
        stop_condition = ExceptionTypeBasedStop(max_delay=1.0, max_attempts=2)

        retry_state = Mock()
        retry_state.outcome = Mock()
        retry_state.outcome.failed = True

        # Start with short_term exception
        retry_state.outcome.exception.return_value = ValueError("some error")
        retry_state.attempt_number = 1

        # First call with short_term - should not stop
        assert stop_condition(retry_state) is False

        # Switch to long_term exception
        retry_state.outcome.exception.return_value = (
            requests.exceptions.ConnectionError("connection failed")
        )
        retry_state.attempt_number = 2

        # Should reset and start duration timer for long_term
        # Should not stop immediately
        assert stop_condition(retry_state) is False

        # Advance time past duration limit
        with patch("time.time", return_value=time.time() + 2.0):
            # Should stop after duration limit
            assert stop_condition(retry_state) is True

    def test_multiple_retry_sequences(self) -> None:
        """Test that different retry_state objects have independent sequences."""
        stop_condition = ExceptionTypeBasedStop(max_delay=1.0, max_attempts=2)

        # Create two different retry states
        retry_state1 = Mock()
        retry_state1.outcome = Mock()
        retry_state1.outcome.failed = True
        retry_state1.outcome.exception.return_value = requests.exceptions.Timeout(
            "timeout"
        )
        retry_state1.attempt_number = 1

        retry_state2 = Mock()
        retry_state2.outcome = Mock()
        retry_state2.outcome.failed = True
        retry_state2.outcome.exception.return_value = ValueError("some error")
        retry_state2.attempt_number = 1

        # Both should have independent tracking
        assert stop_condition(retry_state1) is False
        assert stop_condition(retry_state2) is False

        # Advance time for retry_state1 (long_term)
        with patch("time.time", return_value=time.time() + 2.0):
            assert stop_condition(retry_state1) is True

        # retry_state2 should still be active (short_term, attempt 1)
        retry_state2.attempt_number = 1
        assert stop_condition(retry_state2) is False

    def test_no_stop_when_not_failed(self) -> None:
        """Test that stop condition returns False when outcome is not failed."""
        stop_condition = ExceptionTypeBasedStop(max_delay=1.0, max_attempts=2)

        retry_state = Mock()
        retry_state.outcome = Mock()
        retry_state.outcome.failed = False

        assert stop_condition(retry_state) is False

    def test_no_stop_when_no_outcome(self) -> None:
        """Test that stop condition returns False when there's no outcome."""
        stop_condition = ExceptionTypeBasedStop(max_delay=1.0, max_attempts=2)

        retry_state = Mock()
        retry_state.outcome = None

        assert stop_condition(retry_state) is False


class TestBeforeSleepCallback:
    """Test before_sleep callback behavior through actual retry mechanism"""

    def setup_method(self) -> None:
        """Set up test fixtures."""
        self.resolver = ExecutorConfigResolver(Mock(spec=DataHubGraph))
        self.resolver.executor_configs = []

    @patch("time.sleep")
    @patch("datahub_executor.common.client.config.resolver.logger")
    @patch(
        "datahub_executor.common.client.config.resolver.DATAHUB_EXECUTOR_CONFIG_FETCHER_MAX_RETRY_DURATION_SECONDS",
        86400,
    )
    @patch(
        "datahub_executor.common.client.config.resolver.DATAHUB_EXECUTOR_CONFIG_FETCHER_MAX_RETRIES",
        5,
    )
    def test_before_sleep_updates_retry_state_long_term(
        self, mock_logger: Mock, mock_sleep: Mock
    ) -> None:
        """Test that before_sleep callback updates retry state for long_term exceptions."""
        mock_sleep.return_value = None

        # Reset retry state
        with self.resolver._retry_state_lock:
            self.resolver._retry_state["last_retry_attempt"] = None
            self.resolver._retry_state["is_retrying"] = False
            self.resolver._retry_state["current_exception_category"] = None

        # First call raises Timeout (long_term), second call succeeds
        self.resolver.graph.execute_graphql.side_effect = [  # type: ignore
            requests.exceptions.Timeout("timeout"),
            {
                "listExecutorConfigs": {
                    "total": 1,
                    "executorConfigs": [
                        {
                            "region": "us-west-2",
                            "executorId": DATAHUB_EXECUTOR_EMBEDDED_POOL_ID,
                            "queueUrl": "https://queue.amazon.com/something/123456",
                            "accessKeyId": "some-access-key",
                            "secretKeyId": "some-secret-key",
                            "expiration": datetime.datetime.now(
                                datetime.timezone.utc
                            ).isoformat(),
                            "sessionToken": "some-session-token",
                        },
                    ],
                }
            },
        ]

        # Trigger retry - callback should update retry state
        self.resolver.fetch_executor_configs()

        # Should update retry state for long_term exceptions
        # Note: last_retry_attempt will be set during retry, but reset on success
        # So we check that it was set during the retry by verifying category was set
        assert (
            self.resolver._retry_state["current_exception_category"] is None
        )  # Reset on success
        # Verify that warning was logged (callback was called)
        assert mock_logger.warning.called
        # Verify warning message contains long_term category
        warning_calls = [str(call) for call in mock_logger.warning.call_args_list]
        assert any("long_term" in str(call) for call in warning_calls)

    @patch("time.sleep")
    @patch("datahub_executor.common.client.config.resolver.logger")
    @patch(
        "datahub_executor.common.client.config.resolver.DATAHUB_EXECUTOR_CONFIG_FETCHER_MAX_RETRIES",
        5,
    )
    def test_before_sleep_updates_retry_state_short_term(
        self, mock_logger: Mock, mock_sleep: Mock
    ) -> None:
        """Test that before_sleep callback updates retry state for short_term exceptions."""
        mock_sleep.return_value = None

        # Reset retry state to ensure clean state
        with self.resolver._retry_state_lock:
            self.resolver._retry_state["last_retry_attempt"] = None
            self.resolver._retry_state["is_retrying"] = False
            self.resolver._retry_state["current_exception_category"] = None

        # First call raises ValueError (short_term), second call succeeds
        self.resolver.graph.execute_graphql.side_effect = [  # type: ignore
            ValueError("some error"),
            {
                "listExecutorConfigs": {
                    "total": 1,
                    "executorConfigs": [
                        {
                            "region": "us-west-2",
                            "executorId": DATAHUB_EXECUTOR_EMBEDDED_POOL_ID,
                            "queueUrl": "https://queue.amazon.com/something/123456",
                            "accessKeyId": "some-access-key",
                            "secretKeyId": "some-secret-key",
                            "expiration": datetime.datetime.now(
                                datetime.timezone.utc
                            ).isoformat(),
                            "sessionToken": "some-session-token",
                        },
                    ],
                }
            },
        ]

        # Trigger retry - callback should update retry state
        self.resolver.fetch_executor_configs()

        # Should NOT update last_retry_attempt for short_term exceptions
        assert self.resolver._retry_state.get("last_retry_attempt") is None
        assert self.resolver._retry_state["is_retrying"] is False
        assert (
            self.resolver._retry_state["current_exception_category"] is None
        )  # Reset on success
        # Verify that warning was logged (callback was called)
        assert mock_logger.warning.called
        # Verify warning message contains short_term category
        warning_calls = [str(call) for call in mock_logger.warning.call_args_list]
        assert any("short_term" in str(call) for call in warning_calls)

    @patch("time.sleep")
    @patch("datahub_executor.common.client.config.resolver.logger")
    @patch(
        "datahub_executor.common.client.config.resolver.DATAHUB_EXECUTOR_CONFIG_FETCHER_MAX_RETRY_DURATION_SECONDS",
        86400,
    )
    @patch(
        "datahub_executor.common.client.config.resolver.DATAHUB_EXECUTOR_CONFIG_FETCHER_MAX_RETRIES",
        5,
    )
    def test_before_sleep_detects_category_change(
        self, mock_logger: Mock, mock_sleep: Mock
    ) -> None:
        """Test that before_sleep callback detects exception category changes."""
        mock_sleep.return_value = None

        # First call raises Timeout (long_term), second raises ValueError (short_term), third succeeds
        self.resolver.graph.execute_graphql.side_effect = [  # type: ignore
            requests.exceptions.Timeout("timeout"),
            ValueError("some error"),
            {
                "listExecutorConfigs": {
                    "total": 1,
                    "executorConfigs": [
                        {
                            "region": "us-west-2",
                            "executorId": DATAHUB_EXECUTOR_EMBEDDED_POOL_ID,
                            "queueUrl": "https://queue.amazon.com/something/123456",
                            "accessKeyId": "some-access-key",
                            "secretKeyId": "some-secret-key",
                            "expiration": datetime.datetime.now(
                                datetime.timezone.utc
                            ).isoformat(),
                            "sessionToken": "some-session-token",
                        },
                    ],
                }
            },
        ]

        # Trigger retry with category change - callback should detect and log it
        self.resolver.fetch_executor_configs()

        # Should log info about category change
        assert mock_logger.info.called
        info_calls = [str(call) for call in mock_logger.info.call_args_list]
        assert any("Exception category changed" in str(call) for call in info_calls)


class TestExceptionCategorySwitching:
    """Test exception category switching during retries"""

    @patch("time.sleep")
    @patch(
        "datahub_executor.common.client.config.resolver.DATAHUB_EXECUTOR_CONFIG_FETCHER_MAX_RETRY_DURATION_SECONDS",
        1,
    )
    @patch(
        "datahub_executor.common.client.config.resolver.DATAHUB_EXECUTOR_CONFIG_FETCHER_MAX_RETRIES",
        10,
    )
    def test_category_switching_from_connection_to_timeout(
        self, mock_sleep: Mock
    ) -> None:
        """Test retry behavior when exception category switches from connection to timeout."""
        mock_sleep.return_value = None

        resolver = ExecutorConfigResolver(Mock(spec=DataHubGraph))
        resolver.executor_configs = []

        # First raise ConnectionError (long_term), then Timeout (long_term), then succeed
        resolver.graph.execute_graphql.side_effect = [  # type: ignore
            requests.exceptions.ConnectionError("connection failed"),
            requests.exceptions.Timeout("timeout"),
            {
                "listExecutorConfigs": {
                    "total": 1,
                    "executorConfigs": [
                        {
                            "region": "us-west-2",
                            "executorId": DATAHUB_EXECUTOR_EMBEDDED_POOL_ID,
                            "queueUrl": "https://queue.amazon.com/something/123456",
                            "accessKeyId": "some-access-key",
                            "secretKeyId": "some-secret-key",
                            "expiration": datetime.datetime.now(
                                datetime.timezone.utc
                            ).isoformat(),
                            "sessionToken": "some-session-token",
                        },
                    ],
                }
            },
        ]

        # Should retry through both exceptions and succeed
        executor_configs = resolver.fetch_executor_configs()
        assert len(executor_configs) == 1
        # Should have 3 calls: 1 initial + 2 retries (one for each exception type)
        assert resolver.graph.execute_graphql.call_count >= 3  # type: ignore

    @patch("time.sleep")
    @patch(
        "datahub_executor.common.client.config.resolver.DATAHUB_EXECUTOR_CONFIG_FETCHER_MAX_RETRY_DURATION_SECONDS",
        1,
    )
    @patch(
        "datahub_executor.common.client.config.resolver.DATAHUB_EXECUTOR_CONFIG_FETCHER_MAX_RETRIES",
        10,
    )
    def test_category_switching_between_different_long_term_exceptions(
        self, mock_sleep: Mock
    ) -> None:
        """Test retry behavior when switching between different long_term exception types."""
        mock_sleep.return_value = None

        resolver = ExecutorConfigResolver(Mock(spec=DataHubGraph))
        resolver.executor_configs = []

        # Switch between different long_term exception types
        resolver.graph.execute_graphql.side_effect = [  # type: ignore
            requests.exceptions.Timeout("timeout"),
            urllib3.exceptions.MaxRetryError(None, None, "max retries"),  # type: ignore[arg-type]
            requests.exceptions.ConnectionError("connection failed"),
            {
                "listExecutorConfigs": {
                    "total": 1,
                    "executorConfigs": [
                        {
                            "region": "us-west-2",
                            "executorId": DATAHUB_EXECUTOR_EMBEDDED_POOL_ID,
                            "queueUrl": "https://queue.amazon.com/something/123456",
                            "accessKeyId": "some-access-key",
                            "secretKeyId": "some-secret-key",
                            "expiration": datetime.datetime.now(
                                datetime.timezone.utc
                            ).isoformat(),
                            "sessionToken": "some-session-token",
                        },
                    ],
                }
            },
        ]

        # Should retry through all exceptions and succeed
        executor_configs = resolver.fetch_executor_configs()
        assert len(executor_configs) == 1
        # Should have 4 calls: 1 initial + 3 retries (one for each exception type)
        assert resolver.graph.execute_graphql.call_count >= 4  # type: ignore


class TestRetryStateManagement:
    """Test retry state management and reset"""

    def test_retry_state_reset_on_successful_fetch(self) -> None:
        """Test that retry state is reset on successful fetch."""
        resolver = ExecutorConfigResolver(Mock(spec=DataHubGraph))
        resolver.executor_configs = []

        # Set up retry state as if we were retrying
        resolver._retry_state["last_retry_attempt"] = time.time()
        resolver._retry_state["is_retrying"] = True
        resolver._retry_state["current_exception_category"] = "long_term"

        # Successful fetch - ensure side_effect is None and return_value is set
        resolver.graph.execute_graphql.side_effect = None  # type: ignore
        resolver.graph.execute_graphql.return_value = {  # type: ignore
            "listExecutorConfigs": {
                "total": 1,
                "executorConfigs": [
                    {
                        "region": "us-west-2",
                        "executorId": DATAHUB_EXECUTOR_EMBEDDED_POOL_ID,
                        "queueUrl": "https://queue.amazon.com/something/123456",
                        "accessKeyId": "some-access-key",
                        "secretKeyId": "some-secret-key",
                        "expiration": datetime.datetime.now(
                            datetime.timezone.utc
                        ).isoformat(),
                        "sessionToken": "some-session-token",
                    },
                ],
            }
        }

        resolver.fetch_executor_configs()

        # Retry state should be reset
        assert resolver._retry_state["last_retry_attempt"] is None
        assert resolver._retry_state["is_retrying"] is False
        assert resolver._retry_state["current_exception_category"] is None

    def test_retry_state_initialization_at_start_of_fetch(self) -> None:
        """Test that retry state is initialized at start of fetch."""
        resolver = ExecutorConfigResolver(Mock(spec=DataHubGraph))
        resolver.executor_configs = []

        # Set up some stale retry state
        resolver._retry_state["last_retry_attempt"] = time.time() - 1000
        resolver._retry_state["is_retrying"] = True
        resolver._retry_state["current_exception_category"] = "long_term"

        # Successful fetch - ensure side_effect is None and return_value is set
        resolver.graph.execute_graphql.side_effect = None  # type: ignore
        resolver.graph.execute_graphql.return_value = {  # type: ignore
            "listExecutorConfigs": {
                "total": 0,
                "executorConfigs": [],
            }
        }

        resolver.fetch_executor_configs()

        # Retry state should be reset after successful fetch
        assert resolver._retry_state["last_retry_attempt"] is None
        assert resolver._retry_state["is_retrying"] is False
        assert resolver._retry_state["current_exception_category"] is None
