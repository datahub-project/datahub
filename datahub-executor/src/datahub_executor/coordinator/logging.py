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
    # Check for external config file first and use that (useful for running in k8s, etc).
    external_config = os.environ.get("DATAHUB_LOG_CONFIG_FILE")
    if external_config:
        reset_logging()

        # Route warnings.warn() calls through the logging system.
        logging.captureWarnings(True)

        config_file = Path(external_config)
        logging.config.fileConfig(config_file, disable_existing_loggers=False)

        return

    # Configure Logging using PY directly as a fallback
    BASE_LOGGING_FORMAT = (
        "[%(asctime)s] %(levelname)-8s {%(name)s:%(lineno)d} - %(message)s"
    )
    logging.basicConfig(format=BASE_LOGGING_FORMAT)

    # 1. Create 'datahub' parent logger.
    datahub_logger = logging.getLogger("datahub_executor")

    # 2. Setup the stream handler with formatter.
    stream_handler = logging.StreamHandler()
    formatter = logging.Formatter(BASE_LOGGING_FORMAT)
    stream_handler.setFormatter(formatter)
    datahub_logger.addHandler(stream_handler)

    # 3. Turn off propagation to the root handler.
    datahub_logger.propagate = False

    # 4. Adjust log-levels based on presence of environment variable.
    if os.getenv("DATAHUB_DEBUG", False):
        logging.getLogger().setLevel(logging.INFO)
        datahub_logger.setLevel(logging.DEBUG)
    else:
        logging.getLogger().setLevel(logging.WARNING)
        datahub_logger.setLevel(logging.INFO)
