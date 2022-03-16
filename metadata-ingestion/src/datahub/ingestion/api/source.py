import logging
import platform
import sys
from abc import ABCMeta, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, Generic, Iterable, List, TypeVar

import datahub
from datahub.ingestion.api.closeable import Closeable
from datahub.ingestion.api.common import PipelineContext, RecordEnvelope, WorkUnit
from datahub.ingestion.api.report import Report


@dataclass
class SourceReport(Report):
    workunits_produced: int = 0
    workunit_ids: List[str] = field(default_factory=list)

    warnings: Dict[str, List[str]] = field(default_factory=dict)
    failures: Dict[str, List[str]] = field(default_factory=dict)
    cli_version: str = datahub.nice_version_name()
    cli_entry_location: str = datahub.__file__
    py_version: str = sys.version
    py_exec_path: str = sys.executable
    os_details: str = platform.platform()

    def report_workunit(self, wu: WorkUnit) -> None:
        self.workunits_produced += 1
        self.workunit_ids.append(wu.id)

    def report_warning(self, key: str, reason: str) -> None:
        if key not in self.warnings:
            self.warnings[key] = []
        self.warnings[key].append(reason)

    def report_failure(self, key: str, reason: str) -> None:
        if key not in self.failures:
            self.failures[key] = []
        self.failures[key].append(reason)


WorkUnitType = TypeVar("WorkUnitType", bound=WorkUnit)


class Extractor(Generic[WorkUnitType], Closeable, metaclass=ABCMeta):
    @abstractmethod
    def configure(self, config_dict: dict, ctx: PipelineContext) -> None:
        pass

    @abstractmethod
    def get_records(self, workunit: WorkUnitType) -> Iterable[RecordEnvelope]:
        pass


# See https://github.com/python/mypy/issues/5374 for why we suppress this mypy error.
@dataclass  # type: ignore[misc]
class Source(Closeable, metaclass=ABCMeta):
    def __init__(self, ctx: PipelineContext) -> None:
        self.report: SourceReport
        self.ctx: PipelineContext = ctx

    @classmethod
    @abstractmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "Source":
        pass

    @abstractmethod
    def get_workunits(self) -> Iterable[WorkUnit]:
        pass

    @abstractmethod
    def get_report(self) -> SourceReport:
        pass

    def error(self, log: logging.Logger, key: str, reason: str) -> Any:
        self.report.report_failure(key, reason)
        log.error(f"{key} => {reason}")

    def warn(self, log: logging.Logger, key: str, reason: str) -> Any:
        self.report.report_warning(key, reason)
        log.warning(reason)
