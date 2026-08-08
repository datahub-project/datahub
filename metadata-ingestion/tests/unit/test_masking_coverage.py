"""Tests for handler coverage — invariants 1, 2, 12, 17."""

import logging
import queue
import sys
from io import StringIO
from logging.handlers import QueueHandler, QueueListener
from typing import List

import pytest

from datahub.masking import masking_filter
from datahub.masking.bootstrap import (
    initialize_secret_masking,
    shutdown_secret_masking,
)
from datahub.masking.masking_filter import (
    SecretMaskingFilter,
    install_masking_filter,
    uninstall_masking_filter,
)
from datahub.masking.secret_registry import SecretRegistry


class TestChildLoggerPropagation:
    """Invariant 1: a record logged on a child logger and reaching a root
    FileHandler via propagation is masked."""

    def setup_method(self):
        shutdown_secret_masking()
        SecretRegistry.reset_instance()
        from datahub.masking.bootstrap import reset_bootstrap_state

        reset_bootstrap_state()

    def teardown_method(self):
        shutdown_secret_masking()
        SecretRegistry.reset_instance()
        from datahub.masking.bootstrap import reset_bootstrap_state

        reset_bootstrap_state()

    def test_child_logger_record_masked_at_root_handler(self, tmp_path):
        log_file = tmp_path / "out.log"
        # Install a FileHandler on the root logger.
        initialize_secret_masking()
        SecretRegistry.get_instance().register_secret("PW", "propagation_secret_value")

        root = logging.getLogger()
        fh = logging.FileHandler(str(log_file))
        fh.setFormatter(logging.Formatter("%(message)s"))
        root.addHandler(fh)
        try:
            # Re-scan so the FileHandler gets the filter.
            install_masking_filter(install_stdout_wrapper=False)
            child = logging.getLogger("datahub.ingestion.some_source")
            child.setLevel(logging.INFO)
            child.info("leak propagation_secret_value in child log")
        finally:
            root.removeHandler(fh)
            fh.close()

        contents = log_file.read_text()
        assert "propagation_secret_value" not in contents
        assert "***REDACTED:PW***" in contents


class TestCeleryProxyNotWrapped:
    """Invariant 2: a celery-style stream proxy (no usable fileno()) is never
    wrapped; logging continues normally after initialize + shutdown (no
    feedback loop, no silenced logs)."""

    def setup_method(self):
        shutdown_secret_masking()
        SecretRegistry.reset_instance()
        from datahub.masking.bootstrap import reset_bootstrap_state

        reset_bootstrap_state()

    def teardown_method(self):
        shutdown_secret_masking()
        SecretRegistry.reset_instance()
        from datahub.masking.bootstrap import reset_bootstrap_state

        reset_bootstrap_state()

    def test_proxy_not_wrapped(self):
        class LoggingProxy:
            """Mimics celery's LoggingProxy: no fileno, recurse guard."""

            def __init__(self, target_stream):
                self._target = target_stream
                self._recurse = threading.local()

            def fileno(self):
                raise io.UnsupportedOperation("no fileno on proxy")

            def write(self, text):
                if getattr(self._recurse, "flag", False):
                    return 0
                self._recurse.flag = True
                try:
                    # Re-enters logging (like celery's proxy).
                    logging.getLogger("proxy.target").info(text.rstrip())
                finally:
                    self._recurse.flag = False
                return len(text)

            def flush(self):
                pass

        import io
        import threading

        proxy = LoggingProxy(StringIO())
        original_stdout = sys.stdout
        sys.stdout = proxy
        try:
            initialize_secret_masking()
            # The proxy must NOT be wrapped.
            assert sys.stdout is proxy
            SecretRegistry.get_instance().register_secret("PW", "proxy_secret_value")
            logging.getLogger("test.proxy").info("leak proxy_secret_value")
            shutdown_secret_masking()
            # After shutdown, stdout still the proxy (never wrapped).
            assert sys.stdout is proxy
            # Logging continues normally after initialize + shutdown: a record
            # logged on a fresh handler actually arrives (no feedback loop, no
            # silenced logs).
            arrived: List[logging.LogRecord] = []

            class _Capture(logging.Handler):
                def emit(self, record: logging.LogRecord) -> None:
                    arrived.append(record)

            post_log = logging.getLogger("test.proxy.post_shutdown")
            post_log.handlers.clear()
            post_log.propagate = False
            post_log.setLevel(logging.INFO)
            cap_handler = _Capture()
            post_log.addHandler(cap_handler)
            try:
                post_log.info("still logging after shutdown")
            finally:
                post_log.removeHandler(cap_handler)
            assert len(arrived) == 1, arrived
            assert arrived[0].getMessage() == "still logging after shutdown"
        finally:
            sys.stdout = original_stdout


class TestHandlerAddedAfterInstall:
    """Invariant 12: a handler added to a logger after install (via plain
    addHandler and via logging.basicConfig(force=True)) carries the filter
    and masks; a record routed through a logger-attached QueueHandler to a
    QueueListener whose target handler was never attached to any logger
    arrives at that target already masked, and the sentinel short-circuited
    the nested pass."""

    def setup_method(self):
        shutdown_secret_masking()
        SecretRegistry.reset_instance()
        from datahub.masking.bootstrap import reset_bootstrap_state

        reset_bootstrap_state()

    def teardown_method(self):
        shutdown_secret_masking()
        SecretRegistry.reset_instance()
        from datahub.masking.bootstrap import reset_bootstrap_state

        reset_bootstrap_state()

    def test_plain_addhandler_covers_new_handler(self):
        initialize_secret_masking()
        SecretRegistry.get_instance().register_secret("PW", "addhandler_secret_value")

        cap = StringIO()
        h = logging.StreamHandler(cap)
        h.setFormatter(logging.Formatter("%(message)s"))
        log = logging.getLogger("test.addhandler_after")
        log.handlers.clear()
        log.propagate = False
        log.setLevel(logging.INFO)
        log.addHandler(h)
        try:
            log.info("leak addhandler_secret_value here")
        finally:
            log.removeHandler(h)
        out = cap.getvalue()
        assert "addhandler_secret_value" not in out
        assert "***REDACTED:PW***" in out

    def test_basicconfig_force_covers_new_handler(self):
        initialize_secret_masking()
        SecretRegistry.get_instance().register_secret("PW", "basicconfig_secret_value")
        # basicConfig(force=True) replaces root handlers.
        cap = StringIO()
        root = logging.getLogger()
        saved_handlers = list(root.handlers)
        saved_level = root.level
        logging.basicConfig(force=True, stream=cap, level=logging.INFO)
        try:
            logging.getLogger("test.basicconfig").info(
                "leak basicconfig_secret_value here"
            )
        finally:
            # Restore the root handler setup and level so later tests in the
            # random order aren't left handler-less / re-leveled.
            root.handlers.clear()
            root.handlers.extend(saved_handlers)
            root.setLevel(saved_level)
        out = cap.getvalue()
        assert "basicconfig_secret_value" not in out
        assert "***REDACTED:PW***" in out

    def test_queuehandler_target_arrives_masked(self):
        initialize_secret_masking()
        SecretRegistry.get_instance().register_secret("PW", "queue_secret_value")

        # A target handler that is NEVER attached to a logger; it stores the
        # records it receives so the sentinel can be checked by value.
        captured: List[logging.LogRecord] = []

        class _Capture(logging.Handler):
            def emit(self, record: logging.LogRecord) -> None:
                captured.append(record)

        target = _Capture()

        # QueueHandler attached to a logger; QueueListener dispatches to target.
        q = queue.Queue()
        qh = QueueHandler(q)
        log = logging.getLogger("test.queuehandler")
        log.handlers.clear()
        log.propagate = False
        log.setLevel(logging.INFO)
        log.addHandler(qh)
        listener = QueueListener(q, target)
        listener.start()
        try:
            log.info("leak queue_secret_value here")
        finally:
            listener.stop()  # drains and joins the listener thread
            log.removeHandler(qh)
        assert len(captured) == 1, captured
        rec = captured[0]
        # Sentinel set by value against the module constant (invariant 12).
        assert getattr(rec, "_datahub_masked", None) is masking_filter._MASKED
        assert "queue_secret_value" not in rec.getMessage()
        assert "***REDACTED:PW***" in rec.getMessage()


class TestReinstallAfterUninstall:
    """Invariant 17: after a test-only uninstall, re-install re-covers a
    handler that was added while the wrap was inert."""

    def setup_method(self):
        shutdown_secret_masking()
        SecretRegistry.reset_instance()
        from datahub.masking.bootstrap import reset_bootstrap_state

        reset_bootstrap_state()

    def teardown_method(self):
        shutdown_secret_masking()
        SecretRegistry.reset_instance()
        from datahub.masking.bootstrap import reset_bootstrap_state

        reset_bootstrap_state()

    def test_reinstall_recovers_handler_added_while_inert(self):
        install_masking_filter(install_stdout_wrapper=False)
        SecretRegistry.get_instance().register_secret("PW", "first_secret_value")
        # Uninstall (test-only): wrap goes inert.
        uninstall_masking_filter()
        # Add a handler while the wrap is inert — it gets no filter.
        cap = StringIO()
        h = logging.StreamHandler(cap)
        h.setFormatter(logging.Formatter("%(message)s"))
        log = logging.getLogger("test.reinstall")
        log.handlers.clear()
        log.propagate = False
        log.addHandler(h)
        assert not any(isinstance(f, SecretMaskingFilter) for f in h.filters)
        # Re-install: the scan re-covers the handler added while inert.
        install_masking_filter(install_stdout_wrapper=False)
        SecretRegistry.get_instance().register_secret("PW", "second_secret_value")
        try:
            log.info("leak second_secret_value here")
        finally:
            log.removeHandler(h)
        out = cap.getvalue()
        assert "second_secret_value" not in out
        assert "***REDACTED:PW***" in out


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
