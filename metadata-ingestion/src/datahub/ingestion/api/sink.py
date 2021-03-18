from abc import ABCMeta, abstractmethod
from dataclasses import dataclass, field
from typing import Any, List

from datahub.ingestion.api.closeable import Closeable
from datahub.ingestion.api.common import PipelineContext, RecordEnvelope, WorkUnit
from datahub.ingestion.api.report import Report


@dataclass
class SinkReport(Report):
    records_written = 0
    warnings: List[Any] = field(default_factory=list)
    failures: List[Any] = field(default_factory=list)

    def report_record_written(self, record_envelope: RecordEnvelope):
        self.records_written += 1

    def report_warning(self, info: Any) -> None:
        self.warnings.append(info)

    def report_failure(self, info: Any) -> None:
        self.failures.append(info)


class WriteCallback(metaclass=ABCMeta):
    @abstractmethod
    def on_success(self, record_envelope: RecordEnvelope, success_metadata: dict):
        pass

    @abstractmethod
    def on_failure(
        self,
        record_envelope: RecordEnvelope,
        failure_exception: Exception,
        failure_metadata: dict,
    ):
        pass


class NoopWriteCallback(WriteCallback):
    """Convenience class to support noop"""

    def on_success(self, re, sm):
        pass

    def on_failure(self, re, fe, fm):
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
