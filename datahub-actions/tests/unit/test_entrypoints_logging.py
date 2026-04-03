import logging
import sys
from typing import Generator

import pytest

from datahub_actions.entrypoints import (
    _attach_datahub_actions_console_handlers,
    _BelowErrorFilter,
)


@pytest.fixture
def isolated_logger() -> Generator[logging.Logger, None, None]:
    name = "datahub_actions.test_entrypoints_logging"
    log = logging.getLogger(name)
    log.handlers.clear()
    log.setLevel(logging.DEBUG)
    log.propagate = False
    yield log
    log.handlers.clear()
    log.propagate = True


def test_console_handlers_target_stdout_and_stderr(
    isolated_logger: logging.Logger,
) -> None:
    _attach_datahub_actions_console_handlers(isolated_logger)
    assert len(isolated_logger.handlers) == 2
    stdout_h_raw, stderr_h_raw = isolated_logger.handlers
    assert isinstance(stdout_h_raw, logging.StreamHandler)
    assert isinstance(stderr_h_raw, logging.StreamHandler)
    assert stdout_h_raw.stream is sys.stdout
    assert stderr_h_raw.stream is sys.stderr
    assert stderr_h_raw.level == logging.ERROR


def test_below_error_filter_on_stdout_handler(isolated_logger: logging.Logger) -> None:
    _attach_datahub_actions_console_handlers(isolated_logger)
    stdout_h = isolated_logger.handlers[0]
    assert len(stdout_h.filters) == 1
    filt = stdout_h.filters[0]
    assert isinstance(filt, _BelowErrorFilter)
    rec_info = logging.LogRecord("x", logging.INFO, __file__, 1, "m", (), None)
    rec_err = logging.LogRecord("x", logging.ERROR, __file__, 1, "m", (), None)
    assert filt.filter(rec_info) is True
    assert filt.filter(rec_err) is False


def test_info_to_stdout_error_to_stderr(
    isolated_logger: logging.Logger, capsys: pytest.CaptureFixture[str]
) -> None:
    _attach_datahub_actions_console_handlers(isolated_logger)
    isolated_logger.info("info-msg-actions-test")
    isolated_logger.error("err-msg-actions-test")
    captured = capsys.readouterr()
    assert "info-msg-actions-test" in captured.out
    assert "info-msg-actions-test" not in captured.err
    assert "err-msg-actions-test" in captured.err
    assert "err-msg-actions-test" not in captured.out
