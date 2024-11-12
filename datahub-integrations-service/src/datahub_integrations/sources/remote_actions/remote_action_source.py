import logging
import signal
import sys
from dataclasses import dataclass
from typing import Any, Iterable, Optional

import yaml
from datahub.configuration import ConfigModel
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SupportStatus,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub_actions.pipeline.pipeline import Pipeline
from jinja2 import Environment

logger = logging.getLogger(__name__)

SYSTEM_ACTOR = "urn:li:corpuser:__datahub_system"


class RemoteActionSourceConfig(ConfigModel):
    # The unique URN for the action or automation
    action_urn: str

    # The configuration for the action itself, nothing else.
    action_spec: Any

    # On first run, do we want the action to look back a certain number of days?
    # This is useful for backfilling data.
    lookback_days: Optional[int] = None

    # On this run, do we want the action to discard previous state and start
    # fresh?
    force_full_refresh: Optional[bool] = False


@dataclass
class RemoteActionSourceReport(SourceReport):
    events_processed: int = 0


@platform_name(id="datahub", platform_name="DataHub")
@config_class(RemoteActionSourceConfig)
@support_status(SupportStatus.INCUBATING)
class RemoteActionSource(Source):
    platform = "datahub"
    pipeline_recipe_template = """
name: {{ action_urn }}
datahub: 
  server: {{ datahub_server }}
  {% if datahub_token is not none %}
  token: {{ datahub_token }}
  {% endif %}
source:
  type: 'datahub_integrations.sources.remote_actions.events.datahub_events_source.DataHubEventSource'
  config:
    consumer_id: {{ action_urn }}
    topic: PlatformEvent_v1
    {% if lookback_days is not none %}
    lookback_days: {{ lookback_days }}
    {% endif %}
    {% if force_full_refresh is not none %}
    force_full_refresh: {{ force_full_refresh }}
    {% endif %}

action:
  {{ action_spec | to_yaml | indent(2) }}
"""

    def __init__(self, config: RemoteActionSourceConfig, ctx: PipelineContext):
        super().__init__(ctx)
        self.config: RemoteActionSourceConfig = config
        self.report = RemoteActionSourceReport()
        self.graph = ctx.graph

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        # Simply start an action pipeline up with a specific last offset processed offset.
        # Initialize the pipeline.
        pipeline: Pipeline = Pipeline.create(
            yaml.safe_load(self.create_pipeline_recipe())
        )

        # Register signal handlers to stop the pipeline gracefully.
        def stop_handler(signum, frame):  # type: ignore[no-untyped-def]
            logger.info(f"Received signal {signum}. Stopping pipeline gracefully...")
            pipeline.stop()
            sys.exit(0)

        signal.signal(signal.SIGINT, stop_handler)
        signal.signal(signal.SIGTERM, stop_handler)

        # TODO: Support Remote Bootstrap and Rollback
        # Run the pipeline.
        logger.info("Running pipeline")
        try:
            pipeline.run()
            logger.info("Pipeline has stopped without raising an exception.")
            pipeline.stop()
            yield from []
        except Exception as e:
            logger.exception(f"Caught exception while running pipeline: {e}")
            pipeline.stop()
            sys.exit(1)

    def create_pipeline_recipe(self) -> str:
        def to_yaml_filter(value: Any) -> str:
            return yaml.dump(value, default_flow_style=False)

        # Inject the Custom Event Source Config
        env = Environment()
        env.filters["to_yaml"] = to_yaml_filter
        template = env.from_string(self.pipeline_recipe_template)

        return template.render(
            action_urn=self.config.action_urn,
            action_spec=self.config.action_spec,
            datahub_server=(
                self.graph.config.server if self.graph else None
            ),  # Link to GMS provided by the Ingestion Source context. Do not require additional config.
            lookback_days=self.config.lookback_days,
            force_full_refresh=self.config.force_full_refresh,
            datahub_token=self.graph.config.token if self.graph else None,
        )

    def get_report(self) -> SourceReport:
        return self.report

    def close(self) -> None:
        return super().close()
