import collections
import datetime
from abc import ABCMeta, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import (
    Deque,
    Dict,
    Generic,
    Iterable,
    Iterator,
    List,
    Optional,
    TypeVar,
    Union,
)

from pydantic import BaseModel

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
    SCHEMA_METADATA = "Schema Metadata"
    CONTAINERS = "Asset Containers"


T = TypeVar("T")


class LossyList(List[T]):
    """A list that only preserves the head and tail of lists longer than a certain number"""

    def __init__(
        self, max_elements: int = 10, section_breaker: Optional[str] = "..."
    ) -> None:
        super().__init__()
        self.max_elements = max_elements
        self.list_head: List[T] = []
        self.list_tail: Deque[T] = collections.deque([], maxlen=int(max_elements / 2))
        self.head_full = False
        self.total_elements = 0
        self.section_breaker = section_breaker

    def __iter__(self) -> Iterator[T]:
        yield from self.list_head
        if self.section_breaker and len(self.list_tail):
            yield f"{self.section_breaker} {self.total_elements - len(self.list_head) - len(self.list_tail)} more elements"  # type: ignore
        yield from self.list_tail

    def append(self, __object: T) -> None:
        if self.head_full:
            self.list_tail.append(__object)
        else:
            self.list_head.append(__object)
            if len(self.list_head) > int(self.max_elements / 2):
                self.head_full = True
        self.total_elements += 1

    def __len__(self) -> int:
        return self.total_elements

    def __repr__(self) -> str:
        return repr(list(self.__iter__()))

    def __str__(self) -> str:
        return str(list(self.__iter__()))


@dataclass
class SourceReport(Report):
    events_produced: int = 0
    events_produced_per_sec: int = 0
    event_ids: List[str] = field(default_factory=LossyList)

    warnings: Dict[str, List[str]] = field(default_factory=dict)
    failures: Dict[str, List[str]] = field(default_factory=dict)

    def report_workunit(self, wu: WorkUnit) -> None:
        self.events_produced += 1
        self.event_ids.append(wu.id)

    def report_warning(self, key: str, reason: str) -> None:
        if key not in self.warnings:
            self.warnings[key] = []
        self.warnings[key].append(reason)

    def report_failure(self, key: str, reason: str) -> None:
        if key not in self.failures:
            self.failures[key] = []
        self.failures[key].append(reason)

    def __post_init__(self) -> None:
        self.start_time = datetime.datetime.now()
        self.running_time_in_seconds = 0

    def compute_stats(self) -> None:
        duration = int((datetime.datetime.now() - self.start_time).total_seconds())
        workunits_produced = self.events_produced
        if duration > 0:
            self.events_produced_per_sec: int = int(workunits_produced / duration)
            self.running_time_in_seconds = duration
        else:
            self.read_rate = 0


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
