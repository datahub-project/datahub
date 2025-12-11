# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

"""Test logging utilities for masking framework."""

import logging

from datahub.masking.logging_utils import (
    get_masking_safe_logger,
    reset_masking_safe_loggers,
)


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


class TestResetMaskingSafeLoggers:
    """Test reset_masking_safe_loggers function."""

    def test_reset_clears_masking_logger_handlers(self):
        """reset_masking_safe_loggers should remove handlers from masking loggers."""
        # Create a masking-safe logger
        logger = get_masking_safe_logger("datahub.masking.test_reset")

        assert len(logger.handlers) > 0
        assert logger.propagate is False

        # Reset
        reset_masking_safe_loggers()

        # Handlers should be removed
        assert len(logger.handlers) == 0

    def test_reset_restores_propagate_flag(self):
        """reset_masking_safe_loggers should restore propagate flag."""
        # Create a masking-safe logger
        logger = get_masking_safe_logger("datahub.masking.test_propagate")

        assert logger.propagate is False

        # Reset
        reset_masking_safe_loggers()

        # Propagate should be restored
        assert logger.propagate is True

    def test_reset_only_affects_masking_namespace(self):
        """reset should only affect loggers in datahub.masking namespace."""
        # Create a logger outside masking namespace
        other_logger = logging.getLogger("datahub.other.test")
        other_logger.addHandler(logging.StreamHandler())
        original_handler_count = len(other_logger.handlers)

        # Create a masking logger
        masking_logger = get_masking_safe_logger("datahub.masking.test_namespace")

        # Reset
        reset_masking_safe_loggers()

        # Other logger should not be affected
        assert len(other_logger.handlers) == original_handler_count

        # Masking logger should be reset
        assert len(masking_logger.handlers) == 0

        # Clean up
        other_logger.handlers.clear()
