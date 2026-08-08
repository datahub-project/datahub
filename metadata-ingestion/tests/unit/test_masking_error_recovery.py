"""Tests for fail-closed behavior — invariants 7 (failure half), 10 (failure
half), 13."""

import logging
from io import StringIO
from typing import Any
from unittest.mock import patch

from datahub.masking.masking_filter import (
    MASKING_ERROR_MESSAGE,
    SecretMaskingFilter,
)
from datahub.masking.secret_registry import SecretRegistry


class TestTracebackFailureHalf:
    """Invariant 7 (failure half): when the exc_text masking step fails,
    exc_text becomes MASKING_ERROR_MESSAGE (never the unmasked traceback)."""

    def setup_method(self):
        SecretRegistry.reset_instance()

    def teardown_method(self):
        SecretRegistry.reset_instance()

    def test_exc_text_step_failure_becomes_masking_error(self):
        reg = SecretRegistry.get_instance()
        reg.clear()
        mf = SecretMaskingFilter(reg)
        reg.register_secret("PW", "leak_in_traceback_value")

        record = logging.LogRecord(
            name="test",
            level=logging.ERROR,
            pathname="",
            lineno=0,
            msg="error",
            args=(),
            exc_info=(ValueError, ValueError("embed leak_in_traceback_value"), None),
        )
        original_mask_bounded = mf._mask_bounded

        def failing_mask_bounded(text, budget, original_len=None):
            if "leak_in_traceback_value" in text:
                raise RuntimeError("forced exc_text masking failure")
            return original_mask_bounded(text, budget, original_len)

        with patch.object(mf, "_mask_bounded", side_effect=failing_mask_bounded):
            mf.filter(record)
        assert record.exc_text == MASKING_ERROR_MESSAGE
        assert record.exc_info is not None


class TestExtrasFailureHalf:
    """Invariant 10 (failure half): a field whose masking raises becomes
    MASKING_ERROR_MESSAGE while the other fields are still masked; depth cap
    and cycles produce a placeholder, never the raw subtree."""

    def setup_method(self):
        SecretRegistry.reset_instance()

    def teardown_method(self):
        SecretRegistry.reset_instance()

    def test_one_bad_field_becomes_error_others_still_masked(self):
        reg = SecretRegistry.get_instance()
        reg.clear()
        mf = SecretMaskingFilter(reg)
        reg.register_secret("PW", "field_secret_value")

        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="msg",
            args=(),
            exc_info=None,
        )
        record.__dict__["good"] = "field_secret_value here"
        original = mf._mask_value_recursive

        def selective_fail(value, _depth=0, _seen=None):
            if isinstance(value, str) and value == "POISON":
                raise RuntimeError("forced field failure")
            return original(value, _depth, _seen)

        record.__dict__["bad"] = "POISON"
        with patch.object(mf, "_mask_value_recursive", side_effect=selective_fail):
            mf.filter(record)
        assert record.__dict__["bad"] == MASKING_ERROR_MESSAGE
        assert "field_secret_value" not in record.__dict__["good"]
        assert "***REDACTED:PW***" in record.__dict__["good"]

    def test_depth_cap_produces_placeholder(self):
        reg = SecretRegistry.get_instance()
        reg.clear()
        mf = SecretMaskingFilter(reg)
        reg.register_secret("PW", "deep_secret_value")

        deep: Any = "deep_secret_value"
        for _ in range(mf._MAX_EXTRA_DEPTH + 2):
            deep = {"inner": deep}
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="msg",
            args=(),
            exc_info=None,
        )
        record.__dict__["deep"] = deep
        mf.filter(record)
        flat = str(record.__dict__["deep"])
        assert "deep_secret_value" not in flat
        assert "<not masked: depth limit>" in flat

    def test_cycle_produces_placeholder(self):
        reg = SecretRegistry.get_instance()
        reg.clear()
        mf = SecretMaskingFilter(reg)
        reg.register_secret("PW", "cycle_secret_value")

        d: dict[str, Any] = {"k": "cycle_secret_value"}
        d["self"] = d
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="msg",
            args=(),
            exc_info=None,
        )
        record.__dict__["cyc"] = d
        mf.filter(record)
        flat = str(record.__dict__["cyc"])
        assert "cycle_secret_value" not in flat
        assert "<not masked: cycle>" in flat


class TestFilterInternalException:
    """Invariant 13: a filter-internal exception never propagates to the
    logger.info() caller; the record goes out with MASKING_ERROR_MESSAGE,
    not unmasked and not raised."""

    def setup_method(self):
        SecretRegistry.reset_instance()

    def teardown_method(self):
        SecretRegistry.reset_instance()

    def test_filter_does_not_raise(self):
        reg = SecretRegistry.get_instance()
        reg.clear()
        mf = SecretMaskingFilter(reg)
        reg.register_secret("PW", "outer_secret_value")

        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="leak outer_secret_value",
            args=(),
            exc_info=None,
        )
        with patch.object(
            mf, "_mask_record_msg", side_effect=RuntimeError("forced outer failure")
        ):
            result = mf.filter(record)
        assert result is True
        assert record.msg == MASKING_ERROR_MESSAGE
        assert record.args == ()
        assert record.exc_info is None
        assert record.exc_text is None

    def test_filter_via_logger_info_does_not_raise(self):
        reg = SecretRegistry.get_instance()
        reg.clear()
        mf = SecretMaskingFilter(reg)
        reg.register_secret("PW", "logger_secret_value")

        test_logger = logging.getLogger("test_filter_no_raise")
        test_logger.handlers.clear()
        test_logger.addFilter(mf)
        cap = StringIO()
        handler = logging.StreamHandler(cap)
        handler.setFormatter(logging.Formatter("%(message)s"))
        test_logger.addHandler(handler)
        test_logger.setLevel(logging.INFO)

        try:
            with patch.object(
                mf, "_mask_record_msg", side_effect=RuntimeError("forced")
            ):
                test_logger.info("leak logger_secret_value")
            out = cap.getvalue()
            assert MASKING_ERROR_MESSAGE in out
            assert "logger_secret_value" not in out
        finally:
            test_logger.removeHandler(handler)
            test_logger.removeFilter(mf)
