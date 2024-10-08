"""
This is a small utility script that runs a single datahub action pipeline locally or remotely. 
"""

import argparse
import contextlib
import logging

from datahub.configuration.config_loader import load_config_file
from datahub.telemetry.telemetry import telemetry_instance
from datahub.utilities import logging_manager
from loguru import logger as loguru_logger

from datahub_integrations.actions.local_action_runner import run_action_locally
from datahub_integrations.actions.remote_action_runner import run_action_remotely
from datahub_integrations.actions.stats_util import Stage

# We force load the telemetry client because it has a side-effect of loading Sentry.
assert telemetry_instance

DEFAULT_EXECUTOR_ID = "default"

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run a single datahub action pipeline."
    )
    parser.add_argument(
        "--action_urn",
        help="Urn of the action being started.",
    )
    parser.add_argument("config_file", type=str, help="Path to the config file.")
    parser.add_argument("--port", type=int, help="Port to run the webserver on.")
    parser.add_argument(
        "--rollback", action="store_true", default=False, help="Rollback the pipeline."
    )
    parser.add_argument(
        "--bootstrap",
        action="store_true",
        default=False,
        help="Bootstrap the pipeline.",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        default=False,
        help="Enable debug logging.",
    )
    parser.add_argument(
        "--executor_id",
        default=DEFAULT_EXECUTOR_ID,
        help="Executor id to use when running the action.",
    )

    # Parse the CLI arguments.
    args = parser.parse_args()
    action_urn = args.action_urn
    config_file = args.config_file
    executor_id = args.executor_id
    port = args.port
    debug = args.debug

    stage = Stage.LIVE
    if args.rollback:
        stage = Stage.ROLLBACK
    elif args.bootstrap:
        stage = Stage.BOOTSTRAP

    with contextlib.ExitStack() as stack:
        # Configure log capturing.
        logging_manager.DATAHUB_PACKAGES.append("datahub_integrations")
        stack.enter_context(logging_manager.configure_logging(debug=debug))
        loguru_logger.add(
            logging_manager._BufferLogHandler(logging_manager.get_log_buffer()),
            format="{message}",
        )

        # Load the config file.
        recipe = load_config_file(
            config_file,
            allow_remote=False,
            resolve_env_vars=True,
        )

        if executor_id == DEFAULT_EXECUTOR_ID:
            logger.info("Starting automation with id {action_urn} locally...")
            run_action_locally(recipe, port, stage)
        else:
            logger.info(
                f"Starting automation with id {action_urn} and executor id {executor_id} remotely..."
            )
            # Remote Executor Pathway.
            run_action_remotely(action_urn, recipe, executor_id, stage)


if __name__ == "__main__":
    main()
