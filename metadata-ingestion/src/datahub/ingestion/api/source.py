import platform
import sys
from abc import ABCMeta, abstractmethod
from dataclasses import dataclass, field
from typing import Callable, Dict, Generic, Iterable, List, Type, TypeVar

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
    ctx: PipelineContext

    # @classmethod
    # @abstractmethod
    # def create(cls, config_dict: dict, ctx: PipelineContext) -> "Source":
    #    pass

    @abstractmethod
    def get_workunits(self) -> Iterable[WorkUnit]:
        pass

    @abstractmethod
    def get_report(self) -> SourceReport:
        pass


def config_class(config_cls: Type) -> Callable[[Type], Type]:
    """Adds a get_config_class method to the decorated class"""

    def default_create(cls: Type, config_dict: Dict, ctx: PipelineContext) -> Type:
        config = config_cls.parse_obj(config_dict)
        return cls(config, ctx)

    def wrapper(cls: Type) -> Type:
        # add a get_config_class method
        setattr(cls, "get_config_class", lambda: config_cls)
        # if the class does not define the create method, auto-define it
        if not hasattr(cls, "create"):
            setattr(cls, "create", classmethod(default_create))
        return cls

    return wrapper


def platform_name(platform_name: str) -> Callable[[Type], Type]:
    """Adds a get_platform_name method to the decorated class"""

    def wrapper(cls: Type) -> Type:
        setattr(cls, "get_platform_name", lambda: platform_name)
        return cls

    return wrapper
