"""
This is a small utility script that runs a single datahub action pipeline.

It handles loading a config, resolving environment variables, and running the pipeline.
It also captures signals to stop the pipeline gracefully.
"""

import argparse
import contextlib
import json
import logging
import signal
import sys
import threading
from datetime import datetime, timezone

import fastapi
import fastapi.responses
import uvicorn
from datahub.configuration.config_loader import load_config_file
from datahub.telemetry.telemetry import telemetry_instance
from datahub.utilities import logging_manager
from datahub_actions.pipeline.pipeline import Pipeline
from loguru import logger as loguru_logger

from datahub_integrations.actions.action_extended import ExtendedAction
from datahub_integrations.actions.oss.stats_util import ReportingAction
from datahub_integrations.actions.reporter import ActionStatsReporter
from datahub_integrations.actions.stats_util import Stage

# We force load the telemetry client because it has a side-effect of loading Sentry.
assert telemetry_instance

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

REPORTING_FREQ_SEC = 10


def make_api(pipeline: Pipeline) -> fastapi.FastAPI:
    app = fastapi.FastAPI()

    @app.get("/ping")
    def ping() -> str:
        return "pong"

    @app.get("/", include_in_schema=False)
    def redirect_to_docs() -> fastapi.Response:
        return fastapi.responses.RedirectResponse(url="/docs")

    @app.get("/stats")
    def stats() -> dict:
        pipeline_stats = pipeline.stats()

        main_stats_obj = json.loads(pipeline_stats.as_string())
        main_stats_obj["stats_generated_at"] = datetime.now(tz=timezone.utc).isoformat()

        # If we have an action report, merge that in.
        if isinstance(pipeline.action, ReportingAction):
            main_stats_obj["action"] = pipeline.action.get_report().as_obj()

        return main_stats_obj

    return app


def setup_server(app: fastapi.FastAPI, port: int) -> uvicorn.Server:
    logger.info(f"Starting introspection server on port {port}")
    server = uvicorn.Server(uvicorn.Config(app, port=port, workers=1))
    threading.Thread(target=server.run, args=(), daemon=True).start()
    return server


def setup_reporter(
    reporter: ActionStatsReporter,
    report_interval_secs: int,
) -> None:
    threading.Thread(
        target=reporter.run_action_stats_reporter,
        args=(report_interval_secs,),
        daemon=True,
    ).start()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run a single datahub action pipeline."
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

    # Parse the CLI arguments.
    args = parser.parse_args()
    config_file = args.config_file
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
        # logger.info(f"Loaded config: {recipe}")

        # Initialize the pipeline.
        pipeline: Pipeline = Pipeline.create(recipe)

        # Run the webserver.
        server = None
        if port is not None:
            api = make_api(pipeline)
            server = setup_server(api, port)

        # Run the reporter.
        reporter = None
        if isinstance(pipeline.action, ReportingAction):
            reporter = ActionStatsReporter(
                pipeline, graph=pipeline.action.ctx.graph.graph, stage=stage
            )
            setup_reporter(reporter, report_interval_secs=REPORTING_FREQ_SEC)

        # Register signal handlers to stop the pipeline gracefully.
        def stop_handler(signum, frame):  # type: ignore[no-untyped-def]
            logger.info(f"Received signal {signum}. Stopping pipeline gracefully...")

            if server:
                server.handle_exit(signum, frame)
            pipeline.stop()
            sys.exit(0)

        signal.signal(signal.SIGINT, stop_handler)
        signal.signal(signal.SIGTERM, stop_handler)

        if stage == Stage.ROLLBACK:
            if isinstance(pipeline.action, ExtendedAction):
                logger.info("Rolling back pipeline")
                pipeline.action.rollback()

                assert reporter is not None
                reporter.report()
            else:
                logger.error("Action does not support rollback")
                sys.exit(1)
        elif stage == Stage.BOOTSTRAP:
            logger.info("Bootstrapping pipeline")
            if isinstance(pipeline.action, ExtendedAction):
                pipeline.action.bootstrap()

                assert reporter is not None
                reporter.report()
            else:
                logger.error("Action does not support bootstrap")
                sys.exit(1)
            logger.info("Pipeline bootstrapped successfully")
        else:
            # Run the pipeline.
            logger.info("Running pipeline")
            try:
                pipeline.run()

                logger.info(
                    "Pipeline has stopped unexpectedly, without raising an exception."
                )
                sys.exit(2)
            except Exception as e:
                logger.exception(f"Caught exception while running pipeline: {e}")
                pipeline.stop()
                sys.exit(1)


if __name__ == "__main__":
    main()
