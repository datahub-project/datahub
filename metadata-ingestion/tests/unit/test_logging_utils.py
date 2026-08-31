"""Test logging utilities for masking framework."""

import logging

from datahub.masking.logging_utils import get_masking_safe_logger


class TestGetMaskingSafeLogger:
    """Test get_masking_safe_logger function."""

    def test_get_masking_safe_logger_returns_logger(self):
        """Should return a configured logger."""
        logger = get_masking_safe_logger("test.logger")

        assert isinstance(logger, logging.Logger)
        assert logger.name == "test.logger"

    def test_masking_safe_logger_does_not_propagate(self):
        """Masking-safe loggers should not propagate to avoid masking filters."""
        logger = get_masking_safe_logger("datahub.masking.test")

        assert logger.propagate is False

    def test_masking_safe_logger_has_handler(self):
        """Masking-safe logger should have a handler configured."""
        logger = get_masking_safe_logger("datahub.masking.test2")

        assert len(logger.handlers) > 0

    def test_masking_safe_logger_idempotent(self):
        """Calling get_masking_safe_logger twice should not add duplicate handlers."""
        logger1 = get_masking_safe_logger("datahub.masking.test3")
        handler_count_1 = len(logger1.handlers)

        logger2 = get_masking_safe_logger("datahub.masking.test3")
        handler_count_2 = len(logger2.handlers)

        assert logger1 is logger2
        assert handler_count_1 == handler_count_2
