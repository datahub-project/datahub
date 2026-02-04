# Copyright 2021 Acryl Data, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Logging configuration helpers."""

import logging
import logging.config
import os
from pathlib import Path


def reset_logging() -> None:
    """
    Iterates over all currently configured loggers and resets their configured
    set of handlers so that we can later override them.
    """
    manager = logging.root.manager
    manager.disable = logging.NOTSET
    for sub_logger in manager.loggerDict.values():
        if isinstance(sub_logger, logging.Logger):
            sub_logger.setLevel(logging.NOTSET)
            sub_logger.propagate = True
            sub_logger.disabled = False
            sub_logger.filters.clear()
            handlers = sub_logger.handlers.copy()
            for handler in handlers:
                # Copied from `logging.shutdown`.
                try:
                    handler.acquire()
                    handler.flush()
                    handler.close()
                except (OSError, ValueError):
                    pass
                finally:
                    handler.release()
                sub_logger.removeHandler(handler)


def configure_logging() -> None:
    """Configure global logging based on environment variables.

    Environment variables (checked in order of precedence):
    - DATAHUB_LOG_CONFIG_FILE: Path to an external logging config file (INI format).
      If set, this file is used directly.
    - DATAHUB_LOG_FORMAT: If set to "json", uses logger_json.ini for JSON output.
      Otherwise, uses logger.ini for standard text format.
    """
    config_dir = Path(__file__).parent

    reset_logging()

    # Check for external config file first
    external_config = os.environ.get("DATAHUB_LOG_CONFIG_FILE")
    if external_config:
        config_file = Path(external_config)
    else:
        # Fall back to built-in configs based on format
        log_format = os.environ.get("DATAHUB_LOG_FORMAT", "").lower()
        if log_format == "json":
            config_file = config_dir / "logger_json.ini"
        else:
            config_file = config_dir / "logger.ini"

    logging.config.fileConfig(config_file, disable_existing_loggers=False)
