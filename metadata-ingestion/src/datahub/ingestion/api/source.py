import contextlib
import datetime
import logging
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
    Iterator,
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
from typing_extensions import LiteralString

from datahub.configuration.common import ConfigModel
from datahub.configuration.source_common import PlatformInstanceConfigMixin
from datahub.emitter.mcp_builder import mcps_from_mce
from datahub.ingestion.api.closeable import Closeable
from datahub.ingestion.api.common import PipelineContext, RecordEnvelope, WorkUnit
from datahub.ingestion.api.report import Report
from datahub.ingestion.api.source_helpers import (
    auto_browse_path_v2,
    auto_fix_duplicate_schema_field_paths,
    auto_lowercase_urns,
    auto_materialize_referenced_tags_terms,
    auto_status_aspect,
    auto_workunit_reporter,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.schema_classes import UpstreamLineageClass
from datahub.utilities.lossy_collections import LossyDict, LossyList
from datahub.utilities.type_annotations import get_class_from_annotation

logger = logging.getLogger(__name__)

_MAX_CONTEXT_STRING_LENGTH = 300


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
    CLASSIFICATION = "Classification"


class StructuredLogLevel(Enum):
    INFO = logging.INFO
    WARN = logging.WARN
    ERROR = logging.ERROR


@dataclass
class StructuredLogEntry(Report):
    title: Optional[str]
    message: str
    context: LossyList[str]


@dataclass
class StructuredLogs(Report):
    # Underlying Lossy Dicts to Capture Errors, Warnings, and Infos.
    _entries: Dict[StructuredLogLevel, LossyDict[str, StructuredLogEntry]] = field(
        default_factory=lambda: {
            StructuredLogLevel.ERROR: LossyDict(10),
            StructuredLogLevel.WARN: LossyDict(10),
            StructuredLogLevel.INFO: LossyDict(10),
        }
    )

    def report_log(
        self,
        level: StructuredLogLevel,
        message: LiteralString,
        title: Optional[LiteralString] = None,
        context: Optional[str] = None,
        exc: Optional[BaseException] = None,
        log: bool = False,
        stacklevel: int = 1,
    ) -> None:
        """
        Report a user-facing warning for the ingestion run.

        Args:
            level: The level of the log entry.
            message: The main message associated with the report entry. This should be a human-readable message.
            title: The category / heading to present on for this message in the UI.
            context: Additional context (e.g. where, how) for the log entry.
            exc: The exception associated with the event. We'll show the stack trace when in debug mode.
        """

        # One for this method, and one for the containing report_* call.
        stacklevel = stacklevel + 2

        log_key = f"{title}-{message}"
        entries = self._entries[level]

        if context and len(context) > _MAX_CONTEXT_STRING_LENGTH:
            context = f"{context[:_MAX_CONTEXT_STRING_LENGTH]} ..."

        log_content = f"{message} => {context}" if context else message
        if title:
            log_content = f"{title}: {log_content}"
        if exc:
            log_content += f"{log_content}: {exc}"

            if log:
                logger.log(level=level.value, msg=log_content, stacklevel=stacklevel)
                logger.log(
                    level=logging.DEBUG,
                    msg="Full stack trace:",
                    stacklevel=stacklevel,
                    exc_info=exc,
                )

            # Add the simple exception details to the context.
            context = f"{context}: {exc}"
        elif log:
            logger.log(level=level.value, msg=log_content, stacklevel=stacklevel)

        if log_key not in entries:
            context_list: LossyList[str] = LossyList()
            if context is not None:
                context_list.append(context)
            entries[log_key] = StructuredLogEntry(
                title=title,
                message=message,
                context=context_list,
            )
        else:
            if context is not None:
                entries[log_key].context.append(context)

    def _get_of_type(self, level: StructuredLogLevel) -> LossyList[StructuredLogEntry]:
        entries = self._entries[level]
        result: LossyList[StructuredLogEntry] = LossyList()
        for log in entries.values():
            result.append(log)
        result.set_total(entries.total_key_count())
        return result

    @property
    def warnings(self) -> LossyList[StructuredLogEntry]:
        return self._get_of_type(StructuredLogLevel.WARN)

    @property
    def failures(self) -> LossyList[StructuredLogEntry]:
        return self._get_of_type(StructuredLogLevel.ERROR)

    @property
    def infos(self) -> LossyList[StructuredLogEntry]:
        return self._get_of_type(StructuredLogLevel.INFO)


@dataclass
class SourceReport(Report):
    events_produced: int = 0
    events_produced_per_sec: int = 0

    _urns_seen: Set[str] = field(default_factory=set)
    entities: Dict[str, list] = field(default_factory=lambda: defaultdict(LossyList))
    aspects: Dict[str, Dict[str, int]] = field(
        default_factory=lambda: defaultdict(lambda: defaultdict(int))
    )
    aspect_urn_samples: Dict[str, Dict[str, LossyList[str]]] = field(
        default_factory=lambda: defaultdict(lambda: defaultdict(LossyList))
    )

    _structured_logs: StructuredLogs = field(default_factory=StructuredLogs)

    @property
    def warnings(self) -> LossyList[StructuredLogEntry]:
        return self._structured_logs.warnings

    @property
    def failures(self) -> LossyList[StructuredLogEntry]:
        return self._structured_logs.failures

    @property
    def infos(self) -> LossyList[StructuredLogEntry]:
        return self._structured_logs.infos

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
                    self.aspect_urn_samples[entityType][aspectName].append(urn)
                    if isinstance(mcp.aspect, UpstreamLineageClass):
                        upstream_lineage = cast(UpstreamLineageClass, mcp.aspect)
                        if upstream_lineage.fineGrainedLineages:
                            self.aspect_urn_samples[entityType][
                                "fineGrainedLineages"
                            ].append(urn)

    def report_warning(
        self,
        message: LiteralString,
        context: Optional[str] = None,
        title: Optional[LiteralString] = None,
        exc: Optional[BaseException] = None,
    ) -> None:
        self._structured_logs.report_log(
            StructuredLogLevel.WARN, message, title, context, exc, log=False
        )

    def warning(
        self,
        message: LiteralString,
        context: Optional[str] = None,
        title: Optional[LiteralString] = None,
        exc: Optional[BaseException] = None,
    ) -> None:
        self._structured_logs.report_log(
            StructuredLogLevel.WARN, message, title, context, exc, log=True
        )

    def report_failure(
        self,
        message: LiteralString,
        context: Optional[str] = None,
        title: Optional[LiteralString] = None,
        exc: Optional[BaseException] = None,
        log: bool = True,
    ) -> None:
        self._structured_logs.report_log(
            StructuredLogLevel.ERROR, message, title, context, exc, log=log
        )

    def failure(
        self,
        message: LiteralString,
        context: Optional[str] = None,
        title: Optional[LiteralString] = None,
        exc: Optional[BaseException] = None,
        log: bool = True,
    ) -> None:
        self._structured_logs.report_log(
            StructuredLogLevel.ERROR, message, title, context, exc, log=log
        )

    def info(
        self,
        message: LiteralString,
        context: Optional[str] = None,
        title: Optional[LiteralString] = None,
        exc: Optional[BaseException] = None,
        log: bool = True,
    ) -> None:
        self._structured_logs.report_log(
            StructuredLogLevel.INFO, message, title, context, exc, log=log
        )

    @contextlib.contextmanager
    def report_exc(
        self,
        message: LiteralString,
        title: Optional[LiteralString] = None,
        context: Optional[str] = None,
        level: StructuredLogLevel = StructuredLogLevel.ERROR,
    ) -> Iterator[None]:
        # Convenience method that helps avoid boilerplate try/except blocks.
        # TODO: I'm not super happy with the naming here - it's not obvious that this
        # suppresses the exception in addition to reporting it.
        try:
            yield
        except Exception as exc:
            self._structured_logs.report_log(
                level, message=message, title=title, context=context, exc=exc
            )

    def __post_init__(self) -> None:
        self.start_time = datetime.datetime.now()
        self.running_time: datetime.timedelta = datetime.timedelta(seconds=0)

    def as_obj(self) -> dict:
        return {
            **super().as_obj(),
            # To reduce the amount of nesting, we pull these fields out of the structured log.
            "failures": Report.to_pure_python_obj(self.failures),
            "warnings": Report.to_pure_python_obj(self.warnings),
            "infos": Report.to_pure_python_obj(self.infos),
        }

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

        auto_lowercase_dataset_urns: Optional[MetadataWorkUnitProcessor] = None
        if (
            self.ctx.pipeline_config
            and self.ctx.pipeline_config.source
            and self.ctx.pipeline_config.source.config
            and (
                (
                    hasattr(
                        self.ctx.pipeline_config.source.config,
                        "convert_urns_to_lowercase",
                    )
                    and self.ctx.pipeline_config.source.config.convert_urns_to_lowercase
                )
                or (
                    hasattr(self.ctx.pipeline_config.source.config, "get")
                    and self.ctx.pipeline_config.source.config.get(
                        "convert_urns_to_lowercase"
                    )
                )
            )
        ):
            auto_lowercase_dataset_urns = auto_lowercase_urns

        return [
            auto_lowercase_dataset_urns,
            auto_status_aspect,
            auto_materialize_referenced_tags_terms,
            partial(
                auto_fix_duplicate_schema_field_paths, platform=self._infer_platform()
            ),
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

    def _infer_platform(self) -> Optional[str]:
        config = self.get_config()
        return (
            getattr(config, "platform_name", None)
            or getattr(self, "platform", None)
            or getattr(config, "platform", None)
        )

    def _get_browse_path_processor(self, dry_run: bool) -> MetadataWorkUnitProcessor:
        config = self.get_config()

        platform = self._infer_platform()
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

        browse_path_processor = partial(
            auto_browse_path_v2,
            platform=platform,
            platform_instance=platform_instance,
            drop_dirs=[s for s in browse_path_drop_dirs if s is not None],
            dry_run=dry_run,
        )
        return lambda stream: browse_path_processor(stream)


class TestableSource(Source):
    @staticmethod
    @abstractmethod
    def test_connection(config_dict: dict) -> TestConnectionReport:
        raise NotImplementedError("This class does not implement this method")
