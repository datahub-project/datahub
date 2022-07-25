import platform
import sys
from abc import ABCMeta, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, Generic, Iterable, List, Optional, TypeVar, Union

from pydantic import BaseModel

import datahub
from datahub.ingestion.api.closeable import Closeable
from datahub.ingestion.api.common import PipelineContext, RecordEnvelope, WorkUnit
from datahub.ingestion.api.report import Report


class SourceCapability(Enum):
    PLATFORM_INSTANCE = "Platform Instance"
    DOMAINS = "Domains"
    DATA_PROFILING = "Data Profiling"
    USAGE_STATS = "Dataset Usage"
    PARTITION_SUPPORT = "Partition Support"
    DESCRIPTIONS = "Descriptions"
    LINEAGE_COARSE = "Table-Level Lineage"
    LINEAGE_FINE = "Column-level Lineage"
    OWNERSHIP = "Extract Ownership"
    DELETION_DETECTION = "Detect Deleted Entities"
    TAGS = "Extract Tags"


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


class CapabilityReport(BaseModel):
    """A report capturing the result of any capability evaluation"""

    capable: bool
    failure_reason: Optional[str] = None
    mitigation_message: Optional[str] = None


@dataclass
class TestConnectionReport(Report):
    internal_failure: Optional[bool] = None
    internal_failure_reason: Optional[str] = None
    basic_connectivity: Optional[CapabilityReport] = None
    capability_report: Optional[
        Dict[Union[SourceCapability, str], CapabilityReport]
    ] = None


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
    ctx: PipelineContext

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "Source":
        pass

    @abstractmethod
    def get_workunits(self) -> Iterable[WorkUnit]:
        pass

    @abstractmethod
    def get_report(self) -> SourceReport:
        pass


class TestableSource(Source):
    @staticmethod
    @abstractmethod
    def test_connection(config_dict: dict) -> TestConnectionReport:
        raise NotImplementedError("This class does not implement this method")
