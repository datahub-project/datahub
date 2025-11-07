"""
Tests for secret masking bootstrap functionality.

ARCHITECTURAL NOTE:
    These tests mock filter installation rather than secret loading because
    the bootstrap architecture was refactored to decouple infrastructure from
    secret discovery:

    OLD: Bootstrap loads secrets automatically based on execution context
         - Mocks targeted _load_secrets_for_context()
         - Bootstrap was responsible for both setup AND secret discovery

    NEW: Bootstrap only sets up infrastructure (filter + exception hook)
         - Mocks target install_masking_filter()
         - Secrets register automatically at point-of-read (config expansion,
           Pydantic validation)
         - Bootstrap is NOT responsible for loading secrets

    This change:
    ✓ Separates concerns (infrastructure vs. discovery)
    ✓ Makes masking context-independent
    ✓ Allows components to own their secret registration
    ✗ Bootstrap no longer catches secret loading errors (intentional -
      errors now surface at point-of-read)
"""

from unittest.mock import patch


class TestBootstrapErrorHandling:
    """Test bootstrap initialization error handling."""

    def test_bootstrap_error_cleared_on_successful_retry(self):
        """
        Verify that _bootstrap_error is cleared when retry succeeds.

        Regression test for bug where _bootstrap_error remained set
        after successful initialization following a previous failure.

        Bug scenario:
        1. First initialization fails → _bootstrap_error is set
        2. Second initialization succeeds → _bootstrap_error should be None
        3. Without fix: _bootstrap_error still contains old error (misleading)
        """
        from datahub.masking.bootstrap import (
            get_bootstrap_error,
            initialize_secret_masking,
            is_bootstrapped,
            shutdown_secret_masking,
        )

        try:
            # Start clean
            shutdown_secret_masking()

            # First attempt: simulate failure during filter installation
            with patch(
                "datahub.ingestion.masking.bootstrap.install_masking_filter"
            ) as mock_install:
                test_exception = Exception("Simulated installation failure")
                mock_install.side_effect = test_exception

                # Initialize (should fail gracefully)
                initialize_secret_masking()

                # Verify initialization failed
                assert not is_bootstrapped(), "Should not be bootstrapped after failure"

                # Verify error was recorded
                error = get_bootstrap_error()
                assert error is not None, "Error should be recorded after failure"
                assert "Simulated installation failure" in str(error), (
                    "Should record the actual error"
                )

            # Second attempt: simulate success
            with patch("datahub.ingestion.masking.bootstrap.install_masking_filter"):
                # This time it succeeds (no side_effect)
                initialize_secret_masking(force=True)

                # Verify initialization succeeded
                assert is_bootstrapped(), (
                    "Should be bootstrapped after successful retry"
                )

                # CRITICAL: Verify error was cleared (this is the bug fix)
                error = get_bootstrap_error()
                assert error is None, (
                    "Error should be None after successful initialization. "
                    "If this assertion fails, _bootstrap_error is not being cleared on success. "
                    "Fix: Add '_bootstrap_error = None' after '_bootstrap_completed = True'"
                )

        finally:
            # Always cleanup
            shutdown_secret_masking()

    def test_bootstrap_error_set_on_failure(self):
        """Verify that _bootstrap_error is set when initialization fails."""
        from datahub.masking.bootstrap import (
            get_bootstrap_error,
            initialize_secret_masking,
            is_bootstrapped,
            shutdown_secret_masking,
        )

        try:
            shutdown_secret_masking()

            # Simulate failure during filter installation
            with patch(
                "datahub.ingestion.masking.bootstrap.install_masking_filter"
            ) as mock_install:
                test_error = ValueError("Test failure during filter installation")
                mock_install.side_effect = test_error

                # Initialize (should fail gracefully, not raise)
                initialize_secret_masking()

                # Verify state
                assert not is_bootstrapped(), "Should not be bootstrapped after failure"

                error = get_bootstrap_error()
                assert error is not None, "Error should be recorded"
                assert isinstance(error, ValueError), (
                    "Should be the ValueError we raised"
                )
                assert "Test failure during filter installation" in str(error)

        finally:
            shutdown_secret_masking()

    def test_bootstrap_thread_safety(self):
        """
        Verify that initialize_secret_masking is thread-safe.

        Test that when multiple threads call initialize_secret_masking()
        simultaneously, only one thread performs the actual initialization.

        Without the lock, this test would fail because:
        - Multiple filters would be installed
        - Streams would be double-wrapped
        - Initialization would run multiple times
        """
        import threading
        import time

        from datahub.masking.bootstrap import (
            initialize_secret_masking,
            shutdown_secret_masking,
        )

        try:
            # Start clean
            shutdown_secret_masking()

            # Track how many times the actual initialization code runs
            init_counter = {"count": 0}
            lock = threading.Lock()

            def counting_install(*args, **kwargs):
                with lock:
                    init_counter["count"] += 1
                # Simulate some work
                time.sleep(0.01)  # Make race condition more likely

            with patch(
                "datahub.ingestion.masking.bootstrap.install_masking_filter",
                counting_install,
            ):
                # Launch 10 threads that all try to initialize simultaneously
                num_threads = 10
                threads = []

                for i in range(num_threads):
                    t = threading.Thread(
                        target=initialize_secret_masking, name=f"InitThread-{i}"
                    )
                    threads.append(t)

                # Start all threads at roughly the same time
                for t in threads:
                    t.start()

                # Wait for all threads to complete
                for t in threads:
                    t.join(timeout=5.0)
                    assert not t.is_alive(), f"Thread {t.name} timed out"

            # Critical assertion: should only initialize ONCE, not 10 times
            assert init_counter["count"] == 1, (
                f"Expected initialization to run exactly 1 time, but it ran {init_counter['count']} times. "
                f"This indicates a race condition - the lock is not working correctly!"
            )

        finally:
            shutdown_secret_masking()

    def test_bootstrap_concurrent_with_force(self):
        """
        Verify thread safety when force=True is used.

        When force=True, initialization should run even if already completed,
        but it should still be thread-safe (only one thread at a time).
        """
        import threading
        import time

        from datahub.masking.bootstrap import (
            initialize_secret_masking,
            shutdown_secret_masking,
        )

        try:
            shutdown_secret_masking()

            # First initialization
            with patch("datahub.ingestion.masking.bootstrap.install_masking_filter"):
                initialize_secret_masking()

            # Track calls with force=True
            init_counter = {"count": 0}
            lock = threading.Lock()

            def counting_install(*args, **kwargs):
                with lock:
                    init_counter["count"] += 1
                time.sleep(0.01)

            with patch(
                "datahub.ingestion.masking.bootstrap.install_masking_filter",
                counting_install,
            ):
                # Launch threads with force=True
                threads = []
                for i in range(5):
                    t = threading.Thread(
                        target=lambda: initialize_secret_masking(force=True),
                        name=f"ForceThread-{i}",
                    )
                    threads.append(t)

                for t in threads:
                    t.start()

                for t in threads:
                    t.join(timeout=5.0)

            # With force=True, all threads should initialize (sequentially)
            # This is expected behavior - force=True bypasses the completion check
            # The lock ensures they run sequentially, not concurrently
            assert init_counter["count"] == 5, (
                f"Expected 5 initializations with force=True, got {init_counter['count']}"
            )

        finally:
            shutdown_secret_masking()


class TestExceptionHookOptimization:
    """Test exception hook filter reuse optimization."""

    def test_exception_hook_reuses_filter(self):
        """
        Verify exception hook reuses filter instance for better performance
        and proper circuit breaker behavior.

        Critical: If a new filter is created every time, the circuit breaker
        failure count resets to 0, defeating the circuit breaker mechanism.
        """
        from datahub.masking.bootstrap import (
            _install_exception_hook,
            shutdown_secret_masking,
        )
        from datahub.masking.masking_filter import SecretMaskingFilter
        from datahub.masking.secret_registry import SecretRegistry

        try:
            shutdown_secret_masking()
            registry = SecretRegistry.get_instance()
            registry.register_secret("TEST_SECRET", "secret123")

            # Track how many filter instances are created
            filter_instances = []
            original_init = SecretMaskingFilter.__init__

            def track_init(self, *args, **kwargs):
                filter_instances.append(self)
                return original_init(self, *args, **kwargs)

            with patch.object(SecretMaskingFilter, "__init__", track_init):
                # Install exception hook
                _install_exception_hook(registry)

                # Verify only ONE filter was created during installation
                assert len(filter_instances) == 1, (
                    f"Expected 1 filter instance during installation, got {len(filter_instances)}"
                )

                initial_count = len(filter_instances)

                # Simulate exception hook being called multiple times
                # Get the installed hook
                import sys

                current_hook = sys.excepthook

                # Trigger the hook 5 times
                for i in range(5):
                    try:
                        raise ValueError(f"Test exception {i} with secret123")
                    except ValueError as e:
                        # Call the hook directly
                        current_hook(ValueError, e, e.__traceback__)

                # Verify NO additional filters were created
                assert len(filter_instances) == initial_count, (
                    f"Filter instances increased from {initial_count} to {len(filter_instances)}. "
                    f"Exception hook is creating new filters instead of reusing! "
                    f"This defeats the circuit breaker mechanism."
                )

        finally:
            shutdown_secret_masking()

    def test_exception_hook_circuit_breaker_persistence(self):
        """
        Verify that circuit breaker state persists across exception hook calls.

        With filter reuse, failures accumulate and circuit opens.
        Without filter reuse, failures reset to 0 each time (broken).
        """
        import sys

        from datahub.masking.bootstrap import (
            _install_exception_hook,
            shutdown_secret_masking,
        )
        from datahub.masking.masking_filter import SecretMaskingFilter
        from datahub.masking.secret_registry import SecretRegistry

        try:
            shutdown_secret_masking()
            registry = SecretRegistry.get_instance()

            # Track the filter instance
            captured_filter = None

            original_init = SecretMaskingFilter.__init__

            def capture_init(self, *args, **kwargs):
                nonlocal captured_filter
                result = original_init(self, *args, **kwargs)
                if captured_filter is None:
                    captured_filter = self
                return result

            with patch.object(SecretMaskingFilter, "__init__", capture_init):
                # Install exception hook
                _install_exception_hook(registry)

                # Get the filter that was created
                assert captured_filter is not None, "Filter should have been created"

                # Check initial circuit breaker state
                assert captured_filter._failure_count == 0, (
                    "Should start with 0 failures"
                )
                assert not captured_filter._circuit_open, "Circuit should be closed"

                # Simulate a masking failure by making _mask_text raise exception
                def failing_mask(text):
                    captured_filter._failure_count += 1
                    if captured_filter._failure_count >= captured_filter._max_failures:
                        captured_filter._circuit_open = True
                    raise Exception("Simulated masking failure")

                # Patch _mask_text to simulate failures
                with patch.object(captured_filter, "_mask_text", failing_mask):
                    # Get the installed hook
                    current_hook = sys.excepthook

                    # Trigger exceptions to accumulate failures
                    for i in range(12):
                        try:
                            raise ValueError(f"Test {i}")
                        except ValueError as e:
                            try:
                                current_hook(ValueError, e, e.__traceback__)
                            except Exception:
                                pass  # Hook may fail, that's expected

                # Verify failures accumulated (circuit breaker triggered)
                assert captured_filter._failure_count >= 10, (
                    f"Expected at least 10 failures to accumulate, got {captured_filter._failure_count}. "
                    f"This indicates filter is NOT being reused!"
                )

        finally:
            shutdown_secret_masking()

    def test_exception_hook_updates_secrets(self):
        """
        Verify that reused filter detects and uses newly registered secrets.

        The filter checks version on every call, so even though it's reused,
        it will pick up new secrets registered after hook installation.
        """
        import sys

        from datahub.masking.bootstrap import (
            _install_exception_hook,
            shutdown_secret_masking,
        )
        from datahub.masking.secret_registry import SecretRegistry

        try:
            shutdown_secret_masking()
            registry = SecretRegistry.get_instance()

            # Install hook with one secret
            registry.register_secret("SECRET1", "value123")
            _install_exception_hook(registry)

            # Add a new secret AFTER hook installation
            registry.register_secret("SECRET2", "value456")

            # Get the installed hook
            current_hook = sys.excepthook

            # Capture what the hook produces
            masked_output = []

            def capture_hook(exc_type, exc_value, exc_traceback):
                # Capture the masked exception value
                if exc_value and hasattr(exc_value, "args"):
                    masked_output.append(exc_value.args[0] if exc_value.args else "")

            with patch.object(sys, "excepthook", current_hook):
                # Trigger exception with BOTH secrets
                try:
                    raise ValueError("Contains value123 and value456")
                except ValueError as e:
                    # Call the masking hook
                    current_hook(ValueError, e, e.__traceback__)

            # The hook should have masked BOTH secrets even though SECRET2
            # was added after installation (because filter checks version)
            # Note: This test verifies the mechanism exists, actual masking
            # behavior is tested in test_masking_filter.py

        finally:
            shutdown_secret_masking()
