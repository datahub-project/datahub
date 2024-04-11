"""
This is a small utility script that runs a single datahub action pipeline.

It handles loading a config, resolving environment variables, and running the pipeline.
It also captures signals to stop the pipeline gracefully.
"""

import argparse
import json
import logging
import signal
import sys
import threading
from datetime import datetime

import fastapi
import fastapi.responses
import uvicorn
from datahub.configuration.config_loader import load_config_file
from datahub_actions.pipeline.pipeline import Pipeline

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


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
        stats_obj = pipeline.stats()

        # Hacky was to convert stats_obj to a dict.
        # TODO: Change datahub-actions to use reports properly.
        stats = {
            "stats_generated_at": datetime.now().isoformat(),
            "main": json.loads(stats_obj.as_string()),
            "transformers": {
                key: json.loads(transformer_stats.as_string())
                for key, transformer_stats in stats_obj.transformer_stats.items()
            },
            "action": json.loads(stats_obj.action_stats.as_string()),
        }

        return stats

    return app


def setup_server(app: fastapi.FastAPI, port: int) -> uvicorn.Server:
    logger.info(f"Starting introspection server on port {port}")
    server = uvicorn.Server(uvicorn.Config(app, port=port, workers=1))
    threading.Thread(target=server.run, args=(), daemon=True).start()
    return server


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run a single datahub action pipeline."
    )
    parser.add_argument("config_file", type=str, help="Path to the config file.")
    parser.add_argument(
        "--port", type=int, default=9012, help="Port to run the webserver on."
    )

    # Parse the CLI arguments.
    args = parser.parse_args()
    config_file = args.config_file
    port = args.port

    # Load the config file.
    recipe = load_config_file(
        config_file,
        allow_remote=False,
        resolve_env_vars=True,
    )

    # Initialize the pipeline.
    pipeline: Pipeline = Pipeline.create(recipe)

    # Run the webserver.
    api = make_api(pipeline)
    server = setup_server(api, port)

    # Register signal handlers to stop the pipeline gracefully.
    def stop_handler(signum, frame):  # type: ignore[no-untyped-def]
        logger.info(f"Received signal {signum}. Stopping pipeline gracefully...")

        server.handle_exit(signum, frame)
        pipeline.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, stop_handler)
    signal.signal(signal.SIGTERM, stop_handler)

    # Run the pipeline.
    logger.info("Running pipeline")
    try:
        pipeline.run()
    except Exception as e:
        logger.exception(f"Caught exception while running pipeline: {e}")
        pipeline.stop()
        sys.exit(1)

    logger.info("Pipeline has stopped unexpectedly, without raising an exception.")
    sys.exit(2)


if __name__ == "__main__":
    main()
