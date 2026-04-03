import io
import logging
import sys
from typing import Generator
from unittest.mock import patch

import click
import pytest

from datahub_actions.entrypoints import _BelowErrorFilter, datahub_actions


@pytest.fixture
def datahub_actions_logger() -> Generator[logging.Logger, None, None]:
    log = logging.getLogger("datahub_actions")
    log.handlers.clear()
    ctx = click.Context(datahub_actions)
    ctx.ensure_object(dict)
    cb = datahub_actions.callback
    assert cb is not None
    with ctx.scope():
        cb(
            enable_monitoring=False,
            monitoring_port=8000,
            debug=False,
            detect_memory_leaks=False,
        )
    yield log
    log.handlers.clear()
    log.propagate = True


def test_console_handlers_target_stdout_and_stderr(
    datahub_actions_logger: logging.Logger,
) -> None:
    assert len(datahub_actions_logger.handlers) == 2
    stdout_h_raw, stderr_h_raw = datahub_actions_logger.handlers
    assert isinstance(stdout_h_raw, logging.StreamHandler)
    assert isinstance(stderr_h_raw, logging.StreamHandler)
    assert stdout_h_raw.stream is sys.stdout
    assert stderr_h_raw.stream is sys.stderr
    assert stderr_h_raw.level == logging.ERROR


def test_below_error_filter_on_stdout_handler(
    datahub_actions_logger: logging.Logger,
) -> None:
    stdout_h = datahub_actions_logger.handlers[0]
    assert len(stdout_h.filters) == 1
    filt = stdout_h.filters[0]
    assert isinstance(filt, _BelowErrorFilter)
    rec_info = logging.LogRecord("x", logging.INFO, __file__, 1, "m", (), None)
    rec_err = logging.LogRecord("x", logging.ERROR, __file__, 1, "m", (), None)
    assert filt.filter(rec_info) is True
    assert filt.filter(rec_err) is False


def test_below_error_filter_class() -> None:
    filt = _BelowErrorFilter()
    rec_warn = logging.LogRecord("x", logging.WARNING, __file__, 1, "m", (), None)
    rec_crit = logging.LogRecord("x", logging.CRITICAL, __file__, 1, "m", (), None)
    assert filt.filter(rec_warn) is True
    assert filt.filter(rec_crit) is False


def test_info_to_stdout_error_to_stderr() -> None:
    out_buf = io.StringIO()
    err_buf = io.StringIO()
    log = logging.getLogger("datahub_actions")
    log.handlers.clear()
    try:
        with patch.object(sys, "stdout", out_buf), patch.object(sys, "stderr", err_buf):
            ctx = click.Context(datahub_actions)
            ctx.ensure_object(dict)
            cb = datahub_actions.callback
            assert cb is not None
            with ctx.scope():
                cb(
                    enable_monitoring=False,
                    monitoring_port=8000,
                    debug=False,
                    detect_memory_leaks=False,
                )
            log.info("info-msg-actions-test")
            log.error("err-msg-actions-test")
        out_val, err_val = out_buf.getvalue(), err_buf.getvalue()
        assert "info-msg-actions-test" in out_val
        assert "info-msg-actions-test" not in err_val
        assert "err-msg-actions-test" in err_val
        assert "err-msg-actions-test" not in out_val
    finally:
        log.handlers.clear()
        log.propagate = True
