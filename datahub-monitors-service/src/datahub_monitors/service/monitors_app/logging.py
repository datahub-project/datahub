import logging
import os


def configure_logging() -> None:
    # Configure Logging
    BASE_LOGGING_FORMAT = (
        "[%(asctime)s] %(levelname)-8s {%(name)s:%(lineno)d} - %(message)s"
    )
    logging.basicConfig(format=BASE_LOGGING_FORMAT)

    # 1. Create 'datahub' parent logger.
    datahub_logger = logging.getLogger("datahub_monitors")

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
