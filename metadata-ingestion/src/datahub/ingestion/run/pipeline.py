import importlib
import logging
import time

import click

from datahub.configuration.common import (
    ConfigModel,
    DynamicTypedConfig,
    PipelineExecutionError,
)
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.sink import Sink, WriteCallback
from datahub.ingestion.api.source import Extractor, Source
from datahub.ingestion.sink import sink_class_mapping
from datahub.ingestion.source import source_class_mapping

logger = logging.getLogger(__name__)


class SourceConfig(DynamicTypedConfig):
    extractor: str = "datahub.ingestion.extractor.generic.WorkUnitMCEExtractor"


class PipelineConfig(ConfigModel):
    source: SourceConfig
    sink: DynamicTypedConfig
    run_id: str = str(int(time.time()) * 1000)


class LoggingCallback(WriteCallback):
    def on_success(self, record_envelope, success_meta):
        logger.debug("sink called success callback")

    def on_failure(self, record_envelope, exception, failure_meta):
        # breakpoint()
        logger.exception(
            f"failed to write {record_envelope.record}"
            " with {exception} and info {failure_meta}"
        )


class Pipeline:
    config: PipelineConfig
    ctx: PipelineContext
    source: Source
    sink: Sink

    def get_class_from_name(self, class_string: str):
        module_name, class_name = class_string.rsplit(".", 1)
        MyClass = getattr(importlib.import_module(module_name), class_name)
        return MyClass

    def __init__(self, config: PipelineConfig):
        self.config = config
        self.ctx = PipelineContext(run_id=self.config.run_id)

        source_type = self.config.source.type
        try:
            source_class = source_class_mapping[source_type]
        except KeyError as e:
            raise ValueError(
                f"Did not find a registered source class for {source_type}"
            ) from e
        self.source: Source = source_class.create(
            self.config.source.dict().get("config", {}), self.ctx
        )
        logger.debug(f"Source type:{source_type},{source_class} configured")

        sink_type = self.config.sink.type
        try:
            sink_class = sink_class_mapping[sink_type]
        except KeyError as e:
            raise ValueError(
                f"Did not find a registered sink class for {sink_type}"
            ) from e
        sink_config = self.config.sink.dict().get("config", {})
        self.sink: Sink = sink_class.create(sink_config, self.ctx)
        logger.debug(f"Sink type:{self.config.sink.type},{sink_class} configured")

        # Ensure extractor can be constructed, even though we use them later
        self.extractor_class = self.get_class_from_name(self.config.source.extractor)

    @classmethod
    def create(cls, config_dict: dict) -> "Pipeline":
        config = PipelineConfig.parse_obj(config_dict)
        return cls(config)

    def run(self):
        callback = LoggingCallback()
        extractor: Extractor = self.extractor_class()
        for wu in self.source.get_workunits():
            # TODO: change extractor interface
            extractor.configure({}, self.ctx)

            self.sink.handle_work_unit_start(wu)
            for record_envelope in extractor.get_records(wu):
                self.sink.write_record_async(record_envelope, callback)
            extractor.close()
            self.sink.handle_work_unit_end(wu)
        self.source.close()
        self.sink.close()

    def raise_from_status(self, raise_warnings=False):
        if self.source.get_report().failures:
            raise PipelineExecutionError(
                "Source reported errors", self.source.get_report()
            )
        if self.sink.get_report().failures:
            raise PipelineExecutionError("Sink reported errors", self.sink.get_report())
        if raise_warnings and (
            self.source.get_report().warnings or self.sink.get_report().warnings
        ):
            raise PipelineExecutionError(
                "Source reported warnings", self.source.get_report()
            )

    def pretty_print_summary(self):
        click.echo()
        click.secho("Source report:", bold=True)
        click.echo(self.source.get_report().as_string())
        click.secho("Sink report:", bold=True)
        click.echo(self.sink.get_report().as_string())
        click.echo()
        if self.source.get_report().failures or self.sink.get_report().failures:
            click.secho("Pipeline finished with failures", fg="bright_red", bold=True)
        elif self.source.get_report().warnings or self.sink.get_report().warnings:
            click.secho("Pipeline finished with warnings", fg="yellow", bold=True)
        else:
            click.secho("Pipeline finished successfully", fg="green", bold=True)
