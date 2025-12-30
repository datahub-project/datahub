"""
Runs an action locally.
"""

import json
import logging
import signal
import sys
import threading
from datetime import datetime, timezone

import fastapi
import fastapi.responses
import uvicorn
from datahub.telemetry.telemetry import telemetry_instance
from datahub_actions.pipeline.pipeline import Pipeline
from opentelemetry.exporter.prometheus import PrometheusMetricReader

from datahub_integrations.actions.bulk_bootstrap_action import BulkBootstrapAction
from datahub_integrations.actions.oss.stats_util import ReportingAction
from datahub_integrations.actions.remote_action_runner import run_action_remotely
from datahub_integrations.actions.reporter import ActionStatsReporter
from datahub_integrations.actions.stats_util import Stage
from datahub_integrations.observability.otel_config import (
    ObservabilityConfig,
    setup_meter_provider,
)

# We force load the telemetry client because it has a side-effect of loading Sentry.
assert telemetry_instance

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

REPORTING_FREQ_SEC = 10


def make_api(
    pipeline: Pipeline, prometheus_reader: PrometheusMetricReader
) -> fastapi.FastAPI:
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

    @app.get("/metrics")
    def metrics() -> fastapi.Response:
        """Expose OpenTelemetry metrics in Prometheus format.

        This allows the main service to scrape subprocess metrics,
        and also enables direct OTLP export to collectors.
        """
        # PrometheusMetricReader has a built-in HTTP handler that generates
        # the Prometheus exposition format. We'll use its internal method.
        try:
            # Get metrics in Prometheus format from the reader
            from prometheus_client import REGISTRY, generate_latest

            # The PrometheusMetricReader registers metrics with prometheus_client's REGISTRY
            # We can use generate_latest to get the formatted output
            metrics_output = generate_latest(REGISTRY)

            return fastapi.Response(
                content=metrics_output,
                media_type="text/plain; version=0.0.4; charset=utf-8",
            )
        except Exception as e:
            logger.error(f"Error generating metrics: {e}")
            return fastapi.Response(
                content=f"# Error generating metrics: {e}\n",
                media_type="text/plain",
                status_code=500,
            )

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


def run_action_locally(recipe: dict, port: int, stage: Stage) -> None:
    # Initialize OpenTelemetry for this subprocess
    # This enables both pull-based (Prometheus scraping) and push-based (OTLP export)
    logger.info("Initializing OpenTelemetry observability in action subprocess...")
    config = ObservabilityConfig()
    meter_provider = setup_meter_provider(config)

    # Get the PrometheusMetricReader for exposing /metrics endpoint
    prometheus_reader = None
    for reader in meter_provider._sdk_config.metric_readers:
        if isinstance(reader, PrometheusMetricReader):
            prometheus_reader = reader
            break

    if not prometheus_reader:
        logger.warning(
            "PrometheusMetricReader not found - /metrics endpoint will not work"
        )
        # Create a dummy reader to avoid errors
        prometheus_reader = PrometheusMetricReader()

    # Initialize the pipeline.
    pipeline: Pipeline = Pipeline.create(recipe)

    # Set stage on action if it's an ExtendedAction
    # This ensures metrics are tagged with the correct stage (LIVE/BOOTSTRAP/ROLLBACK)
    from datahub_integrations.actions.action_extended import ExtendedAction

    if isinstance(pipeline.action, ExtendedAction):
        # Use model_copy to update the stage (Pydantic v2 doesn't allow direct field assignment)
        pipeline.action._stats = pipeline.action._stats.model_copy(
            update={"stage": stage.value}
        )

    # Run the webserver.
    server = None
    if port is not None:
        api = make_api(pipeline, prometheus_reader)
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
        if isinstance(pipeline.action, (ExtendedAction, BulkBootstrapAction)):
            logger.info("Rolling back pipeline")
            pipeline.action.rollback()

            assert reporter is not None
            reporter.report()
            logger.info("Pipeline rolled back successfully")
        else:
            logger.error("Action does not support rollback")
            sys.exit(1)
    elif stage == Stage.BOOTSTRAP:
        logger.info("Bootstrapping pipeline")
        if isinstance(pipeline.action, ExtendedAction):
            pipeline.action.bootstrap()
        elif isinstance(pipeline.action, BulkBootstrapAction):
            if (
                pipeline.action.is_monitoring_process()
                and pipeline.action.config.bootstrap_executor_id
            ):
                for slice_id in range(pipeline.action.num_slices):
                    new_recipe = {
                        **recipe,
                        "action": {
                            **((recipe.get("action") or {}).get("config") or {}),
                            "slice": slice_id,
                        },
                    }
                    run_action_remotely(
                        action_urn=pipeline.action.action_urn,
                        action_recipe=new_recipe,
                        executor_id=pipeline.action.config.bootstrap_executor_id,
                        stage=stage,
                    )
                pipeline.action.monitor_bootstrap()
            else:
                pipeline.action.bootstrap()
        elif hasattr(pipeline.action, "bootstrap") and callable(
            pipeline.action.bootstrap
        ):
            # Support actions that have a bootstrap method but don't inherit from ExtendedAction
            pipeline.action.bootstrap()
        else:
            logger.error("Action does not support bootstrap")
            sys.exit(1)

        if reporter:
            reporter.report()
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
