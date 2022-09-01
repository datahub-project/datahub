import datetime
from abc import ABCMeta, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Optional

from datahub.ingestion.api.closeable import Closeable
from datahub.ingestion.api.common import PipelineContext, RecordEnvelope, WorkUnit
from datahub.ingestion.api.report import Report
from datahub.utilities.lossy_collections import LossyList


@dataclass
class SinkReport(Report):
    total_records_written: int = 0
    records_written_per_second: int = 0
    warnings: LossyList[Any] = field(default_factory=LossyList)
    failures: LossyList[Any] = field(default_factory=LossyList)
    start_time: datetime.datetime = datetime.datetime.now()
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


# See https://github.com/python/mypy/issues/5374 for why we suppress this mypy error.
@dataclass  # type: ignore[misc]
class Sink(Closeable, metaclass=ABCMeta):
    """All Sinks must inherit this base class."""

    ctx: PipelineContext

    @classmethod
    @abstractmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "Sink":
        pass

    @abstractmethod
    def handle_work_unit_start(self, workunit: WorkUnit) -> None:
        pass

    @abstractmethod
    def handle_work_unit_end(self, workunit: WorkUnit) -> None:
        pass

    @abstractmethod
    def write_record_async(
        self, record_envelope: RecordEnvelope, callback: WriteCallback
    ) -> None:
        # must call callback when done.
        pass

    @abstractmethod
    def get_report(self) -> SinkReport:
        pass

    @abstractmethod
    def close(self) -> None:
        pass

    def configured(self) -> str:
        """Override this method to output a human-readable and scrubbed version of the configured sink"""
        return ""
