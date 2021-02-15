from abc import ABCMeta, abstractmethod
from dataclasses import dataclass, field
from typing import Iterable, List

from .closeable import Closeable
from .common import PipelineContext, RecordEnvelope, WorkUnit
from .report import Report


@dataclass
class SourceReport(Report):
    workunits_produced = 0
    workunit_ids: List[str] = field(default_factory=list)

    def report_workunit(self, wu: WorkUnit):
        self.workunits_produced += 1
        self.workunit_ids.append(wu.id)


class Extractor(Closeable, metaclass=ABCMeta):
    @abstractmethod
    def configure(self, config_dict: dict, ctx: PipelineContext):
        pass

    @abstractmethod
    def get_records(self, workunit: WorkUnit) -> Iterable[RecordEnvelope]:
        pass


# See https://github.com/python/mypy/issues/5374 for why we suppress this mypy error.
@dataclass  # type: ignore[misc]
class Source(Closeable, metaclass=ABCMeta):
    ctx: PipelineContext

    @classmethod
    @abstractmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> 'Source':
        pass

    @abstractmethod
    def get_workunits(self) -> Iterable[WorkUnit]:
        pass

    @abstractmethod
    def get_report(self) -> SourceReport:
        pass
