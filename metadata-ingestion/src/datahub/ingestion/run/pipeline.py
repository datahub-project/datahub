import itertools
import logging
import os
import platform
import sys
import time
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, NoReturn, Optional, cast

import click
import humanfriendly
import psutil

import datahub
from datahub.configuration.common import IgnorableError, PipelineExecutionError
from datahub.ingestion.api.committable import CommitPolicy
from datahub.ingestion.api.common import EndOfStream, PipelineContext, RecordEnvelope
from datahub.ingestion.api.pipeline_run_listener import PipelineRunListener
from datahub.ingestion.api.report import Report
from datahub.ingestion.api.sink import Sink, WriteCallback
from datahub.ingestion.api.source import Extractor, Source
from datahub.ingestion.api.transform import Transformer
from datahub.ingestion.extractor.extractor_registry import extractor_registry
from datahub.ingestion.reporting.reporting_provider_registry import (
    reporting_provider_registry,
)
from datahub.ingestion.run.pipeline_config import PipelineConfig, ReporterConfig
from datahub.ingestion.sink.file import FileSink, FileSinkConfig
from datahub.ingestion.sink.sink_registry import sink_registry
from datahub.ingestion.source.source_registry import source_registry
from datahub.ingestion.transformer.transform_registry import transform_registry
from datahub.metadata.schema_classes import MetadataChangeProposalClass
from datahub.telemetry import stats, telemetry
from datahub.utilities.lossy_collections import LossyDict, LossyList

logger = logging.getLogger(__name__)


class LoggingCallback(WriteCallback):
    def __init__(self, name: str = "") -> None:
        super().__init__()
        self.name = name

    def on_success(
        self, record_envelope: RecordEnvelope, success_metadata: dict
    ) -> None:
        logger.debug(
            f"{self.name} sink wrote workunit {record_envelope.metadata['workunit_id']}"
        )

    def on_failure(
        self,
        record_envelope: RecordEnvelope,
        failure_exception: Exception,
        failure_metadata: dict,
    ) -> None:
        logger.error(
            f"{self.name} failed to write record with workunit {record_envelope.metadata['workunit_id']}"
            f" with {failure_exception} and info {failure_metadata}"
        )


class DeadLetterQueueCallback(WriteCallback):
    def __init__(self, ctx: PipelineContext, config: Optional[FileSinkConfig]) -> None:
        if not config:
            config = FileSinkConfig.parse_obj({"filename": "failed_events.json"})
        self.file_sink: FileSink = FileSink(ctx, config)
        self.logging_callback = LoggingCallback(name="failure-queue")
        logger.info(f"Failure logging enabled. Will log to {config.filename}.")

    def on_success(
        self, record_envelope: RecordEnvelope, success_metadata: dict
    ) -> None:
        pass

    def on_failure(
        self,
        record_envelope: RecordEnvelope,
        failure_exception: Exception,
        failure_metadata: dict,
    ) -> None:
        if "workunit_id" in record_envelope.metadata:
            if isinstance(record_envelope.record, MetadataChangeProposalClass):
                mcp = cast(MetadataChangeProposalClass, record_envelope.record)
                if mcp.systemMetadata:
                    if not mcp.systemMetadata.properties:
                        mcp.systemMetadata.properties = {}
                    if "workunit_id" not in mcp.systemMetadata.properties:
                        # update the workunit id
                        mcp.systemMetadata.properties[
                            "workunit_id"
                        ] = record_envelope.metadata["workunit_id"]
                record_envelope.record = mcp
        self.file_sink.write_record_async(record_envelope, self.logging_callback)

    def close(self) -> None:
        self.file_sink.close()


class PipelineInitError(Exception):
    pass


@dataclass
class CliReport(Report):
    cli_version: str = datahub.nice_version_name()
    cli_entry_location: str = datahub.__file__
    py_version: str = sys.version
    py_exec_path: str = sys.executable
    os_details: str = platform.platform()

    def compute_stats(self) -> None:

        self.mem_info = humanfriendly.format_size(
            psutil.Process(os.getpid()).memory_info().rss
        )
        return super().compute_stats()


class Pipeline:
    config: PipelineConfig
    ctx: PipelineContext
    source: Source
    extractor: Extractor
    sink: Sink
    transformers: List[Transformer]

    def _raise_initialization_error(self, e: Exception, msg: str) -> NoReturn:
        raise PipelineInitError(f"{msg}: {e}") from e

    def __init__(
        self,
        config: PipelineConfig,
        dry_run: bool = False,
        preview_mode: bool = False,
        preview_workunits: int = 10,
        report_to: Optional[str] = None,
        no_default_report: bool = False,
    ):
        self.config = config
        self.dry_run = dry_run
        self.preview_mode = preview_mode
        self.preview_workunits = preview_workunits
        self.report_to = report_to
        self.reporters: List[PipelineRunListener] = []
        self.num_intermediate_workunits = 0
        self.last_time_printed = int(time.time())
        self.cli_report = CliReport()

        try:
            self.ctx = PipelineContext(
                run_id=self.config.run_id,
                datahub_api=self.config.datahub_api,
                pipeline_name=self.config.pipeline_name,
                dry_run=dry_run,
                preview_mode=preview_mode,
                pipeline_config=self.config,
            )
        except Exception as e:
            self._raise_initialization_error(e, "Failed to set up framework context")

        sink_type = self.config.sink.type
        try:
            sink_class = sink_registry.get(sink_type)
        except Exception as e:
            self._raise_initialization_error(
                e, f"Failed to find a registered sink for type {sink_type}"
            )

        try:
            sink_config = self.config.sink.dict().get("config") or {}
            self.sink: Sink = sink_class.create(sink_config, self.ctx)
            logger.debug(f"Sink type:{self.config.sink.type},{sink_class} configured")
            logger.info(f"Sink configured successfully. {self.sink.configured()}")
        except Exception as e:
            self._raise_initialization_error(
                e, f"Failed to configure sink ({sink_type})"
            )

        # once a sink is configured, we can configure reporting immediately to get observability
        try:
            self._configure_reporting(report_to, no_default_report)
        except Exception as e:
            self._raise_initialization_error(e, "Failed to configure reporters")

        try:
            source_type = self.config.source.type
            source_class = source_registry.get(source_type)
        except Exception as e:
            self._raise_initialization_error(e, "Failed to create source")

        try:
            self.source: Source = source_class.create(
                self.config.source.dict().get("config", {}), self.ctx
            )
            logger.debug(f"Source type:{source_type},{source_class} configured")
            logger.info("Source configured successfully.")
        except Exception as e:
            self._raise_initialization_error(
                e, f"Failed to configure source ({source_type})"
            )

        try:
            extractor_class = extractor_registry.get(self.config.source.extractor)
            self.extractor = extractor_class(
                self.config.source.extractor_config, self.ctx
            )
        except Exception as e:
            self._raise_initialization_error(
                e, f"Failed to configure extractor ({self.config.source.extractor})"
            )

        try:
            self._configure_transforms()
        except ValueError as e:
            self._raise_initialization_error(e, "Failed to configure transformers")

    def _configure_transforms(self) -> None:
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

    def _configure_reporting(
        self, report_to: Optional[str], no_default_report: bool
    ) -> None:
        if report_to == "datahub":
            # we add the default datahub reporter unless a datahub reporter is already configured
            if not no_default_report and (
                not self.config.reporting
                or "datahub" not in [x.type for x in self.config.reporting]
            ):
                self.config.reporting.append(
                    ReporterConfig.parse_obj({"type": "datahub"})
                )
        elif report_to:
            # we assume this is a file name, and add the file reporter
            self.config.reporting.append(
                ReporterConfig.parse_obj(
                    {"type": "file", "config": {"filename": report_to}}
                )
            )

        for reporter in self.config.reporting:
            reporter_type = reporter.type
            reporter_class = reporting_provider_registry.get(reporter_type)
            reporter_config_dict = reporter.dict().get("config", {})
            try:
                self.reporters.append(
                    reporter_class.create(
                        config_dict=reporter_config_dict,
                        ctx=self.ctx,
                    )
                )
                logger.debug(
                    f"Reporter type:{reporter_type},{reporter_class} configured."
                )
            except Exception as e:
                if reporter.required:
                    raise
                elif isinstance(e, IgnorableError):
                    logger.debug(f"Reporter type {reporter_type} is disabled: {e}")
                else:
                    logger.warning(
                        f"Failed to configure reporter: {reporter_type}", exc_info=e
                    )

    def _notify_reporters_on_ingestion_start(self) -> None:
        for reporter in self.reporters:
            try:
                reporter.on_start(ctx=self.ctx)
            except Exception as e:
                logger.warning("Reporting failed on start", exc_info=e)

    def _notify_reporters_on_ingestion_completion(self) -> None:
        for reporter in self.reporters:
            try:
                reporter.on_completion(
                    status="CANCELLED"
                    if self.final_status == "cancelled"
                    else "FAILURE"
                    if self.source.get_report().failures
                    or self.sink.get_report().failures
                    else "SUCCESS"
                    if self.final_status == "completed"
                    else "UNKNOWN",
                    report=self._get_structured_report(),
                    ctx=self.ctx,
                )
            except Exception as e:
                logger.warning("Reporting failed on completion", exc_info=e)

    @classmethod
    def create(
        cls,
        config_dict: dict,
        dry_run: bool = False,
        preview_mode: bool = False,
        preview_workunits: int = 10,
        report_to: Optional[str] = None,
        no_default_report: bool = False,
        raw_config: Optional[dict] = None,
    ) -> "Pipeline":
        config = PipelineConfig.from_dict(config_dict, raw_config)
        return cls(
            config,
            dry_run=dry_run,
            preview_mode=preview_mode,
            preview_workunits=preview_workunits,
            report_to=report_to,
            no_default_report=no_default_report,
        )

    def _time_to_print(self) -> bool:
        self.num_intermediate_workunits += 1
        current_time = int(time.time())
        if current_time - self.last_time_printed > 10:
            # we print
            self.num_intermediate_workunits = 0
            self.last_time_printed = current_time
            return True
        return False

    def run(self) -> None:
        self.final_status = "unknown"
        self._notify_reporters_on_ingestion_start()
        callback = None
        try:
            callback = (
                LoggingCallback()
                if not self.config.failure_log.enabled
                else DeadLetterQueueCallback(
                    self.ctx, self.config.failure_log.log_config
                )
            )
            for wu in itertools.islice(
                self.source.get_workunits(),
                self.preview_workunits if self.preview_mode else None,
            ):
                try:
                    if self._time_to_print():
                        self.pretty_print_summary(currently_running=True)
                except Exception as e:
                    logger.warning(f"Failed to print summary {e}")

                if not self.dry_run:
                    self.sink.handle_work_unit_start(wu)
                try:
                    record_envelopes = self.extractor.get_records(wu)
                    for record_envelope in self.transform(record_envelopes):
                        if not self.dry_run:
                            self.sink.write_record_async(record_envelope, callback)

                except RuntimeError:
                    raise
                except SystemExit:
                    raise
                except Exception as e:
                    logger.error(
                        "Failed to process some records. Continuing.", exc_info=e
                    )

                self.extractor.close()
                if not self.dry_run:
                    self.sink.handle_work_unit_end(wu)
            self.source.close()
            # no more data is coming, we need to let the transformers produce any additional records if they are holding on to state
            for record_envelope in self.transform(
                [
                    RecordEnvelope(
                        record=EndOfStream(), metadata={"workunit_id": "end-of-stream"}
                    )
                ]
            ):
                if not self.dry_run and not isinstance(
                    record_envelope.record, EndOfStream
                ):
                    # TODO: propagate EndOfStream and other control events to sinks, to allow them to flush etc.
                    self.sink.write_record_async(record_envelope, callback)

            self.sink.close()
            self.process_commits()
            self.final_status = "completed"
        except (SystemExit, RuntimeError) as e:
            self.final_status = "cancelled"
            logger.error("Caught error", exc_info=e)
            raise
        finally:
            if callback and hasattr(callback, "close"):
                callback.close()  # type: ignore

            self._notify_reporters_on_ingestion_completion()

    def transform(self, records: Iterable[RecordEnvelope]) -> Iterable[RecordEnvelope]:
        """
        Transforms the given sequence of records by passing the records through the transformers
        :param records: the records to transform
        :return: the transformed records
        """
        for transformer in self.transformers:
            records = transformer.transform(records)

        return records

    def process_commits(self) -> None:
        """
        Evaluates the commit_policy for each committable in the context and triggers the commit operation
        on the committable if its required commit policies are satisfied.
        """
        has_errors: bool = (
            True
            if self.source.get_report().failures or self.sink.get_report().failures
            else False
        )
        has_warnings: bool = bool(
            self.source.get_report().warnings or self.sink.get_report().warnings
        )

        for name, committable in self.ctx.get_committables():
            commit_policy: CommitPolicy = committable.commit_policy

            logger.info(
                f"Processing commit request for {name}. Commit policy = {commit_policy},"
                f" has_errors={has_errors}, has_warnings={has_warnings}"
            )

            if (
                commit_policy == CommitPolicy.ON_NO_ERRORS_AND_NO_WARNINGS
                and (has_errors or has_warnings)
            ) or (commit_policy == CommitPolicy.ON_NO_ERRORS and has_errors):
                logger.warning(
                    f"Skipping commit request for {name} since policy requirements are not met."
                )
                continue

            try:
                committable.commit()
            except Exception as e:
                logger.error(f"Failed to commit changes for {name}.", e)
                raise e
            else:
                logger.info(f"Successfully committed changes for {name}.")

    def raise_from_status(self, raise_warnings: bool = False) -> None:
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

    def log_ingestion_stats(self) -> None:
        telemetry.telemetry_instance.ping(
            "ingest_stats",
            {
                "source_type": self.config.source.type,
                "sink_type": self.config.sink.type,
                "records_written": stats.discretize(
                    self.sink.get_report().total_records_written
                ),
            },
            self.ctx.graph,
        )

    def _approx_all_vals(self, d: LossyDict[str, LossyList]) -> int:
        result = d.dropped_keys_count()
        for k in d:
            result += len(d[k])
        return result

    def _get_text_color(self, running: bool, failures: bool, warnings: bool) -> str:
        if running:
            return "cyan"
        else:
            if failures:
                return "bright_red"
            elif warnings:
                return "bright_yellow"
            else:
                return "bright_green"

    def pretty_print_summary(
        self, warnings_as_failure: bool = False, currently_running: bool = False
    ) -> int:
        click.echo()
        click.secho("Cli report:", bold=True)
        click.secho(self.cli_report.as_string())
        click.secho(f"Source ({self.config.source.type}) report:", bold=True)
        click.echo(self.source.get_report().as_string())
        click.secho(f"Sink ({self.config.sink.type}) report:", bold=True)
        click.echo(self.sink.get_report().as_string())
        click.echo()
        workunits_produced = self.source.get_report().events_produced
        duration_message = f"in {Report.to_str(self.source.get_report().running_time)}."

        if self.source.get_report().failures or self.sink.get_report().failures:
            num_failures_source = self._approx_all_vals(
                self.source.get_report().failures
            )
            num_failures_sink = len(self.sink.get_report().failures)
            click.secho(
                f"{'⏳' if currently_running else ''} Pipeline {'running' if currently_running else 'finished'} with at least {num_failures_source+num_failures_sink} failures {'so far' if currently_running else ''}; produced {workunits_produced} events {duration_message}",
                fg=self._get_text_color(
                    running=currently_running,
                    failures=True,
                    warnings=False,
                ),
                bold=True,
            )
            return 1
        elif self.source.get_report().warnings or self.sink.get_report().warnings:
            num_warn_source = self._approx_all_vals(self.source.get_report().warnings)
            num_warn_sink = len(self.sink.get_report().warnings)
            click.secho(
                f"{'⏳' if currently_running else ''} Pipeline {'running' if currently_running else 'finished'} with at least {num_warn_source+num_warn_sink} warnings{' so far' if currently_running else ''}; produced {workunits_produced} events {duration_message}",
                fg=self._get_text_color(
                    running=currently_running, failures=False, warnings=True
                ),
                bold=True,
            )
            return 1 if warnings_as_failure else 0
        else:
            click.secho(
                f"{'⏳' if currently_running else ''} Pipeline {'running' if currently_running else 'finished'} successfully{' so far' if currently_running else ''}; produced {workunits_produced} events {duration_message}",
                fg=self._get_text_color(
                    running=currently_running, failures=False, warnings=False
                ),
                bold=True,
            )
            return 0

    def _get_structured_report(self) -> Dict[str, Any]:
        return {
            "source": {
                "type": self.config.source.type,
                "report": self.source.get_report().as_obj(),
            },
            "sink": {
                "type": self.config.sink.type,
                "report": self.sink.get_report().as_obj(),
            },
        }
