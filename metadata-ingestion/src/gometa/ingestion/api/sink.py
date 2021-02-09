from abc import abstractmethod, ABCMeta
from dataclasses import dataclass, field
from typing import List, Any

from gometa.ingestion.api.closeable import Closeable
from gometa.ingestion.api.common import RecordEnvelope, WorkUnit, PipelineContext
from gometa.ingestion.api.report import Report

@dataclass
class SinkReport(Report):
    # workunits_processed = 0
    records_written = 0
    failures: List[Any] = field(default_factory=list)

    def report_record_written(self, record: RecordEnvelope):
        self.records_written += 1

    def report_failure(self, info: Any) -> None:
        self.failures.append(info)


class WriteCallback(metaclass=ABCMeta):

    @abstractmethod
    def on_success(self, record_envelope: RecordEnvelope, success_metadata: dict):
        pass

    @abstractmethod
    def on_failure(self, record_envelope: RecordEnvelope, failure_exception: Exception, failure_metadata: dict):
        pass

class NoopWriteCallback(WriteCallback):
    """Convenience class to support noop"""
    def on_success(self, re, sm):
        pass
    
    def on_failure(self, re, fe, fm):
        pass


# See https://github.com/python/mypy/issues/5374 for why we suppress this mypy error.
@dataclass  # type: ignore[misc]
class Sink(Closeable, metaclass = ABCMeta):
    """All Sinks must inherit this base class."""

    ctx: PipelineContext

    @classmethod
    @abstractmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> 'Sink':
        pass

    @abstractmethod
    def handle_work_unit_start(self, workunit: WorkUnit) -> None:
        pass

    @abstractmethod
    def handle_work_unit_end(self, workunit: WorkUnit) -> None:
        pass

    @abstractmethod
    def write_record_async(self, record_envelope: RecordEnvelope, callback: WriteCallback):
        # must call callback when done.
        pass

    @abstractmethod
    def get_report(self) -> SinkReport:
        pass

    @abstractmethod
    def close(self) -> None:
        pass
