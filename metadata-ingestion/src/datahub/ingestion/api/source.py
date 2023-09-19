import datetime
from abc import ABCMeta, abstractmethod
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum
from functools import partial
from typing import (
    Callable,
    Dict,
    Generic,
    Iterable,
    List,
    Optional,
    Sequence,
    Set,
    Type,
    TypeVar,
    Union,
    cast,
)

from pydantic import BaseModel

from datahub.configuration.common import ConfigModel
from datahub.configuration.source_common import PlatformInstanceConfigMixin
from datahub.emitter.mcp_builder import mcps_from_mce
from datahub.ingestion.api.closeable import Closeable
from datahub.ingestion.api.common import PipelineContext, RecordEnvelope, WorkUnit
from datahub.ingestion.api.report import Report
from datahub.ingestion.api.source_helpers import (
    auto_browse_path_v2,
    auto_materialize_referenced_tags,
    auto_status_aspect,
    auto_workunit_reporter,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.utilities.lossy_collections import LossyDict, LossyList
from datahub.utilities.type_annotations import get_class_from_annotation


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


@dataclass
class SourceReport(Report):
    events_produced: int = 0
    events_produced_per_sec: int = 0

    _urns_seen: Set[str] = field(default_factory=set)
    entities: Dict[str, list] = field(default_factory=lambda: defaultdict(LossyList))
    aspects: Dict[str, Dict[str, int]] = field(
        default_factory=lambda: defaultdict(lambda: defaultdict(int))
    )

    warnings: LossyDict[str, LossyList[str]] = field(default_factory=LossyDict)
    failures: LossyDict[str, LossyList[str]] = field(default_factory=LossyDict)

    def report_workunit(self, wu: WorkUnit) -> None:
        self.events_produced += 1

        if isinstance(wu, MetadataWorkUnit):
            urn = wu.get_urn()

            # Specialized entity reporting.
            if not isinstance(wu.metadata, MetadataChangeEvent):
                mcps = [wu.metadata]
            else:
                mcps = list(mcps_from_mce(wu.metadata))

            for mcp in mcps:
                entityType = mcp.entityType
                aspectName = mcp.aspectName

                if urn not in self._urns_seen:
                    self._urns_seen.add(urn)
                    self.entities[entityType].append(urn)

                if aspectName is not None:  # usually true
                    self.aspects[entityType][aspectName] += 1

    def report_warning(self, key: str, reason: str) -> None:
        warnings = self.warnings.get(key, LossyList())
        warnings.append(reason)
        self.warnings[key] = warnings

    def report_failure(self, key: str, reason: str) -> None:
        failures = self.failures.get(key, LossyList())
        failures.append(reason)
        self.failures[key] = failures

    def __post_init__(self) -> None:
        self.start_time = datetime.datetime.now()
        self.running_time: datetime.timedelta = datetime.timedelta(seconds=0)

    def compute_stats(self) -> None:
        duration = datetime.datetime.now() - self.start_time
        workunits_produced = self.events_produced
        if duration.total_seconds() > 0:
            self.events_produced_per_sec: int = int(
                workunits_produced / duration.total_seconds()
            )
            self.running_time = duration
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
ExtractorConfig = TypeVar("ExtractorConfig", bound=ConfigModel)

WorkUnitProcessor = Callable[[Iterable[WorkUnitType]], Iterable[WorkUnitType]]
MetadataWorkUnitProcessor = WorkUnitProcessor[MetadataWorkUnit]


class Extractor(Generic[WorkUnitType, ExtractorConfig], Closeable, metaclass=ABCMeta):
    ctx: PipelineContext
    config: ExtractorConfig

    @classmethod
    def get_config_class(cls) -> Type[ExtractorConfig]:
        config_class = get_class_from_annotation(cls, Extractor, ConfigModel)
        assert config_class, "Extractor subclasses must define a config class"
        return cast(Type[ExtractorConfig], config_class)

    def __init__(self, config_dict: dict, ctx: PipelineContext) -> None:
        super().__init__()

        config_class = self.get_config_class()

        self.ctx = ctx
        self.config = config_class.parse_obj(config_dict)

    @abstractmethod
    def get_records(self, workunit: WorkUnitType) -> Iterable[RecordEnvelope]:
        pass


@dataclass
class Source(Closeable, metaclass=ABCMeta):
    ctx: PipelineContext

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "Source":
        # Technically, this method should be abstract. However, the @config_class
        # decorator automatically generates a create method at runtime if one is
        # not defined. Python still treats the class as abstract because it thinks
        # the create method is missing. To avoid the class becoming abstract, we
        # can't make this method abstract.
        raise NotImplementedError('sources must implement "create"')

    def get_workunit_processors(self) -> List[Optional[MetadataWorkUnitProcessor]]:
        """A list of functions that transforms the workunits produced by this source.
        Run in order, first in list is applied first. Be careful with order when overriding.
        """
        browse_path_processor: Optional[MetadataWorkUnitProcessor] = None
        if (
            self.ctx.pipeline_config
            and self.ctx.pipeline_config.flags.generate_browse_path_v2
        ):
            browse_path_processor = self._get_browse_path_processor(
                self.ctx.pipeline_config.flags.generate_browse_path_v2_dry_run
            )

        return [
            auto_status_aspect,
            auto_materialize_referenced_tags,
            browse_path_processor,
            partial(auto_workunit_reporter, self.get_report()),
        ]

    @staticmethod
    def _apply_workunit_processors(
        workunit_processors: Sequence[Optional[MetadataWorkUnitProcessor]],
        stream: Iterable[MetadataWorkUnit],
    ) -> Iterable[MetadataWorkUnit]:
        for processor in workunit_processors:
            if processor is not None:
                stream = processor(stream)
        return stream

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        return self._apply_workunit_processors(
            self.get_workunit_processors(), self.get_workunits_internal()
        )

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        raise NotImplementedError(
            "get_workunits_internal must be implemented if get_workunits is not overriden."
        )

    def get_config(self) -> Optional[ConfigModel]:
        """Overridable method to return the config object for this source.

        Enables defining workunit processors in this class, rather than per source.
        More broadly, this method contributes to the standardization of sources,
        to promote more source-generic functionality.

        Eventually, would like to replace this call with a Protocol that requires
        a config object to be defined on each source.
        """
        return getattr(self, "config", None) or getattr(self, "source_config", None)

    @abstractmethod
    def get_report(self) -> SourceReport:
        pass

    def close(self) -> None:
        pass

    def _get_browse_path_processor(self, dry_run: bool) -> MetadataWorkUnitProcessor:
        config = self.get_config()
        platform = getattr(self, "platform", None) or getattr(config, "platform", None)
        env = getattr(config, "env", None)
        browse_path_drop_dirs = [
            platform,
            platform and platform.lower(),
            env,
            env and env.lower(),
        ]

        platform_instance: Optional[str] = None
        if isinstance(config, PlatformInstanceConfigMixin) and config.platform_instance:
            platform_instance = config.platform_instance

        return partial(
            auto_browse_path_v2,
            platform=platform,
            platform_instance=platform_instance,
            drop_dirs=[s for s in browse_path_drop_dirs if s is not None],
            dry_run=dry_run,
        )


class TestableSource(Source):
    @staticmethod
    @abstractmethod
    def test_connection(config_dict: dict) -> TestConnectionReport:
        raise NotImplementedError("This class does not implement this method")
