import io
import logging
import sys
from unittest.mock import patch

import click

from datahub_actions.entrypoints import datahub_actions


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
            log.info("info-sentinel")
            log.error("error-sentinel")
        assert "info-sentinel" in out_buf.getvalue()
        assert "info-sentinel" not in err_buf.getvalue()
        assert "error-sentinel" in err_buf.getvalue()
        assert "error-sentinel" not in out_buf.getvalue()
    finally:
        log.handlers.clear()
        log.propagate = True
