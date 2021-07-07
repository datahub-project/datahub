import logging
import uuid
from typing import Iterable, List, Optional

import click
from pydantic import Field

from datahub.configuration.common import (
    ConfigModel,
    DynamicTypedConfig,
    PipelineExecutionError,
)
from datahub.ingestion.api.common import PipelineContext, RecordEnvelope
from datahub.ingestion.api.sink import Sink, WriteCallback
from datahub.ingestion.api.source import Extractor, Source
from datahub.ingestion.api.transform import Transformer
from datahub.ingestion.extractor.extractor_registry import extractor_registry
from datahub.ingestion.sink.sink_registry import sink_registry
from datahub.ingestion.source.source_registry import source_registry
from datahub.ingestion.transformer.transform_registry import transform_registry

logger = logging.getLogger(__name__)


class SourceConfig(DynamicTypedConfig):
    extractor: str = "generic"


class PipelineConfig(ConfigModel):
    # Once support for discriminated unions gets merged into Pydantic, we can
    # simplify this configuration and validation.
    # See https://github.com/samuelcolvin/pydantic/pull/2336.

    run_id: str = Field(default_factory=lambda: str(uuid.uuid1()))
    source: SourceConfig
    sink: DynamicTypedConfig
    transformers: Optional[List[DynamicTypedConfig]]


class LoggingCallback(WriteCallback):
    def on_success(
        self, record_envelope: RecordEnvelope, success_metadata: dict
    ) -> None:
        logger.info(f"sink wrote workunit {record_envelope.metadata['workunit_id']}")

    def on_failure(
        self,
        record_envelope: RecordEnvelope,
        failure_exception: Exception,
        failure_metadata: dict,
    ) -> None:
        logger.error(
            f"failed to write record with workunit {record_envelope.metadata['workunit_id']}"
            f" with {failure_exception} and info {failure_metadata}"
        )


class Pipeline:
    config: PipelineConfig
    ctx: PipelineContext
    source: Source
    sink: Sink
    transformers: List[Transformer]

    def __init__(self, config: PipelineConfig):
        self.config = config
        self.ctx = PipelineContext(run_id=self.config.run_id)

        source_type = self.config.source.type
        source_class = source_registry.get(source_type)
        self.source: Source = source_class.create(
            self.config.source.dict().get("config", {}), self.ctx
        )
        logger.debug(f"Source type:{source_type},{source_class} configured")

        sink_type = self.config.sink.type
        sink_class = sink_registry.get(sink_type)
        sink_config = self.config.sink.dict().get("config", {})
        self.sink: Sink = sink_class.create(sink_config, self.ctx)
        logger.debug(f"Sink type:{self.config.sink.type},{sink_class} configured")

        self.extractor_class = extractor_registry.get(self.config.source.extractor)

        self._configure_transforms()

    def _configure_transforms(self):
        self.transformers = []
        if self.config.transformers is not None:
            for transformer in self.config.transformers:
                transformer_type = transformer.type
                transformer_class = transform_registry.get(transformer_type)
                transformer_config = transformer.dict().get("config", {})
                self.transformers.append(
                    transformer_class.create(transformer_config, self.ctx)
                )
                logger.debug(
                    f"Transformer type:{transformer_type},{transformer_class} configured"
                )

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
            record_envelopes = extractor.get_records(wu)
            for record_envelope in self.transform(record_envelopes):
                self.sink.write_record_async(record_envelope, callback)

            extractor.close()
            self.sink.handle_work_unit_end(wu)
        self.source.close()
        self.sink.close()

    def transform(self, records: Iterable[RecordEnvelope]) -> Iterable[RecordEnvelope]:
        """
        Transforms the given sequence of records by passing the records through the transformers
        :param records: the records to transform
        :return: the transformed records
        """
        for transformer in self.transformers:
            records = transformer.transform(records)

        return records

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

    def pretty_print_summary(self) -> int:
        click.echo()
        click.secho(f"Source ({self.config.source.type}) report:", bold=True)
        click.echo(self.source.get_report().as_string())
        click.secho(f"Sink ({self.config.sink.type}) report:", bold=True)
        click.echo(self.sink.get_report().as_string())
        click.echo()
        if self.source.get_report().failures or self.sink.get_report().failures:
            click.secho("Pipeline finished with failures", fg="bright_red", bold=True)
            return 1
        elif self.source.get_report().warnings or self.sink.get_report().warnings:
            click.secho("Pipeline finished with warnings", fg="yellow", bold=True)
            return 0
        else:
            click.secho("Pipeline finished successfully", fg="green", bold=True)
            return 0
