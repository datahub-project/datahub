import inspect
import logging

from datahub.utilities.logging_manager import DATAHUB_PACKAGES
from loguru import logger


class InterceptHandler(logging.Handler):
    # Copied from the loguru documentation:
    # https://loguru.readthedocs.io/en/stable/overview.html#entirely-compatible-with-standard-logging

    def emit(self, record: logging.LogRecord) -> None:
        # Get corresponding Loguru level if it exists.
        level: str | int
        try:
            level = logger.level(record.levelname).name
        except ValueError:
            level = record.levelno

        # Find caller from where originated the logged message.
        frame, depth = inspect.currentframe(), 0
        while frame and (depth == 0 or frame.f_code.co_filename == logging.__file__):
            frame = frame.f_back
            depth += 1

        logger.opt(depth=depth, exception=record.exc_info).log(
            level, record.getMessage()
        )


logging.basicConfig(handlers=[InterceptHandler()], level=logging.INFO, force=True)
for package in [*DATAHUB_PACKAGES, "datahub_integrations"]:
    logging.getLogger(package).setLevel(logging.INFO)

# When in asyncio debug mode, we do want to see warnings related to blocking the main thread.
logging.getLogger("asyncio").setLevel(logging.WARNING)


LOGGING_SETUP_COMPLETE = True
