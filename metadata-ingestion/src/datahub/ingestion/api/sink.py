import datetime
from abc import ABCMeta, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Generic, Optional, Type, TypeVar, cast

from datahub.configuration.common import ConfigModel
from datahub.ingestion.api.closeable import Closeable
from datahub.ingestion.api.common import PipelineContext, RecordEnvelope, WorkUnit
from datahub.ingestion.api.report import Report
from datahub.utilities.lossy_collections import LossyList
from datahub.utilities.type_annotations import get_class_from_annotation


@dataclass
class SinkReport(Report):
    total_records_written: int = 0
    records_written_per_second: int = 0
    warnings: LossyList[Any] = field(default_factory=LossyList)
    failures: LossyList[Any] = field(default_factory=LossyList)
    start_time: datetime.datetime = field(default_factory=datetime.datetime.now)
    current_time: Optional[datetime.datetime] = None
    total_duration_in_seconds: Optional[float] = None

    def report_record_written(self, record_envelope: RecordEnvelope) -> None:
        self.total_records_written += 1

    def report_warning(self, info: Any) -> None:
        self.warnings.append(info)

    def report_failure(self, info: Any) -> None:
        self.failures.append(info)

    def compute_stats(self) -> None:
        super().compute_stats()
        self.current_time = datetime.datetime.now()
        if self.start_time:
            self.total_duration_in_seconds = round(
                (self.current_time - self.start_time).total_seconds(), 2
            )
            if self.total_duration_in_seconds > 0:
                self.records_written_per_second = int(
                    self.total_records_written / self.total_duration_in_seconds
                )


class WriteCallback(metaclass=ABCMeta):
    @abstractmethod
    def on_success(
        self, record_envelope: RecordEnvelope, success_metadata: dict
    ) -> None:
        pass

    @abstractmethod
    def on_failure(
        self,
        record_envelope: RecordEnvelope,
        failure_exception: Exception,
        failure_metadata: dict,
    ) -> None:
        pass


class NoopWriteCallback(WriteCallback):
    """Convenience WriteCallback class to support noop"""

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
        pass


SinkReportType = TypeVar("SinkReportType", bound=SinkReport)
SinkConfig = TypeVar("SinkConfig", bound=ConfigModel)
Self = TypeVar("Self", bound="Sink")


class Sink(Generic[SinkConfig, SinkReportType], Closeable, metaclass=ABCMeta):
    """All Sinks must inherit this base class."""

    ctx: PipelineContext
    config: SinkConfig
    report: SinkReportType

    @classmethod
    def get_config_class(cls) -> Type[SinkConfig]:
        config_class = get_class_from_annotation(cls, Sink, ConfigModel)
        assert config_class, "Sink subclasses must define a config class"
        return cast(Type[SinkConfig], config_class)

    @classmethod
    def get_report_class(cls) -> Type[SinkReportType]:
        report_class = get_class_from_annotation(cls, Sink, SinkReport)
        assert report_class, "Sink subclasses must define a report class"
        return cast(Type[SinkReportType], report_class)

    def __init__(self, ctx: PipelineContext, config: SinkConfig):
        self.ctx = ctx
        self.config = config
        self.report = self.get_report_class()()

        self.__post_init__()

    def __post_init__(self) -> None:
        pass

    @classmethod
    def create(cls: Type[Self], config_dict: dict, ctx: PipelineContext) -> "Self":
        return cls(ctx, cls.get_config_class().parse_obj(config_dict))

    def handle_work_unit_start(self, workunit: WorkUnit) -> None:
        pass

    def handle_work_unit_end(self, workunit: WorkUnit) -> None:
        pass

    @abstractmethod
    def write_record_async(
        self, record_envelope: RecordEnvelope, callback: WriteCallback
    ) -> None:
        # must call callback when done.
        pass

    def get_report(self) -> SinkReportType:
        return self.report

    def close(self) -> None:
        pass

    def configured(self) -> str:
        """Override this method to output a human-readable and scrubbed version of the configured sink"""
        return ""
