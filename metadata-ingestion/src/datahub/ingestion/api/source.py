import contextlib
import datetime
import logging
from abc import ABCMeta, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import (
    TYPE_CHECKING,
    Callable,
    Dict,
    Generic,
    Iterable,
    Iterator,
    List,
    Optional,
    Sequence,
    Type,
    TypeVar,
    Union,
    cast,
)

from pydantic import BaseModel
from typing_extensions import LiteralString, Self

from datahub.configuration.common import ConfigModel
from datahub.configuration.env_vars import (
    get_report_failure_sample_size,
    get_report_info_sample_size,
    get_report_warning_sample_size,
)
from datahub.ingestion.api.closeable import Closeable
from datahub.ingestion.api.common import PipelineContext, RecordEnvelope, WorkUnit
from datahub.ingestion.api.report import ExamplesReport, Report
from datahub.ingestion.api.source_helpers import (
    AutoSystemMetadata,
    auto_workunit,
)
from datahub.ingestion.api.source_protocols import (
    MetadataWorkUnitIterable,
    ProfilingCapable,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.api.workunit_processor import WorkunitProcessorReport
from datahub.ingestion.source_report.ingestion_stage import (
    IngestionHighStage,
    IngestionStageReport,
)
from datahub.telemetry import stats
from datahub.utilities.lossy_collections import LossyDict, LossyList
from datahub.utilities.type_annotations import get_class_from_annotation

if TYPE_CHECKING:
    from datahub.ingestion.api.workunit_processor import WorkunitProcessor
    from datahub.ingestion.source.state.entity_removal_state import (
        GenericCheckpointState,
    )

logger = logging.getLogger(__name__)

_MAX_CONTEXT_STRING_LENGTH = 1000


class SourceCapability(Enum):
    PLATFORM_INSTANCE = "Platform Instance"
    DOMAINS = "Domains"
    DATA_PROFILING = "Data Profiling"
    USAGE_STATS = "Dataset Usage"
    OPERATION_CAPTURE = "Operation Capture"
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
    TEST_CONNECTION = "Test Connection"
    GLOSSARY_TERMS = "Glossary Terms"


class StructuredLogLevel(Enum):
    INFO = logging.INFO
    WARN = logging.WARN
    ERROR = logging.ERROR


class StructuredLogCategory(Enum):
    """
    This is used to categorise the errors mainly based on the biggest impact area
    This is to be used to help in self-serve understand the impact of any log entry
    More enums to be added as logs are updated to be self-serve
    """

    LINEAGE = "LINEAGE"
    USAGE = "USAGE"
    PROFILING = "PROFILING"


@dataclass
class StructuredLogEntry(Report):
    title: Optional[str]
    message: str
    context: LossyList[str]
    log_category: Optional[StructuredLogCategory] = None


@dataclass
class StructuredLogs(Report):
    # Underlying Lossy Dicts to Capture Errors, Warnings, and Infos.
    _entries: Dict[StructuredLogLevel, LossyDict[str, StructuredLogEntry]] = field(
        default_factory=lambda: {
            StructuredLogLevel.ERROR: LossyDict(get_report_failure_sample_size()),
            StructuredLogLevel.WARN: LossyDict(get_report_warning_sample_size()),
            StructuredLogLevel.INFO: LossyDict(get_report_info_sample_size()),
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
        log_category: Optional[StructuredLogCategory] = None,
    ) -> None:
        """
        Report a user-facing log for the ingestion run.

        Args:
            level: The level of the log entry.
            message: The main message associated with the report entry. This should be a human-readable message.
            title: The category / heading to present on for this message in the UI.
            context: Additional context (e.g. where, how) for the log entry.
            exc: The exception associated with the event. We'll show the stack trace when in debug mode.
            log_category: The type of the log entry. This is used to categorise the log entry.
            log: Whether to log the entry to the console.
            stacklevel: The stack level to use for the log entry.
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
            if context:
                context = f"{context} {type(exc)}: {exc}"
            else:
                context = f"{type(exc)}: {exc}"
        elif log:
            logger.log(level=level.value, msg=log_content, stacklevel=stacklevel)

        if log_key not in entries:
            # Size the per-entry context list to match the level's configured
            # sample size, so DATAHUB_REPORT_*_SAMPLE_SIZE controls both the
            # number of distinct entries and the number of grouped contexts
            # under each entry.
            context_list: LossyList[str] = LossyList(max_elements=entries.max_elements)
            if context is not None:
                context_list.append(context)
            entries[log_key] = StructuredLogEntry(
                title=title,
                message=message,
                context=context_list,
                log_category=log_category,
            )
        else:
            if context is not None:
                entries[log_key].context.append(context)

    def set_sample_sizes(
        self,
        failure_size: Optional[int] = None,
        warning_size: Optional[int] = None,
        info_size: Optional[int] = None,
    ) -> None:
        """Override the max_elements on the underlying LossyDicts.

        Should be called early (right after source construction). Sources may
        log warnings/errors during __init__, so existing entries are pruned
        if they exceed the new limit.
        """
        for level, size in (
            (StructuredLogLevel.ERROR, failure_size),
            (StructuredLogLevel.WARN, warning_size),
            (StructuredLogLevel.INFO, info_size),
        ):
            if size is None:
                continue
            entries = self._entries[level]
            entries.resize(size)
            # Also resize the nested context list on any entries that were
            # already recorded (e.g. during source __init__ before pipeline
            # applied the configured sample size).
            for entry in entries.values():
                entry.context.resize(size)

    def _get_of_type(self, level: StructuredLogLevel) -> LossyList[StructuredLogEntry]:
        entries = self._entries[level]
        result: LossyList[StructuredLogEntry] = LossyList(
            max_elements=entries.max_elements
        )
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
class SourceReport(ExamplesReport, IngestionStageReport):
    event_not_produced_warn: bool = True
    events_produced: int = 0
    events_produced_per_sec: int = 0
    workunit_processor_reports: Dict[str, WorkunitProcessorReport] = field(
        default_factory=dict
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

    def set_sample_sizes(
        self,
        failure_size: Optional[int] = None,
        warning_size: Optional[int] = None,
        info_size: Optional[int] = None,
    ) -> None:
        self._structured_logs.set_sample_sizes(
            failure_size=failure_size,
            warning_size=warning_size,
            info_size=info_size,
        )

    def report_workunit(self, wu: WorkUnit) -> None:
        self.events_produced += 1
        if not isinstance(wu, MetadataWorkUnit):
            return

        super()._store_workunit_data(wu)

    def report_warning(
        self,
        message: LiteralString,
        context: Optional[str] = None,
        title: Optional[LiteralString] = None,
        exc: Optional[BaseException] = None,
        log_category: Optional[StructuredLogCategory] = None,
    ) -> None:
        """
        See docs of StructuredLogs.report_log for details of args
        """
        self._structured_logs.report_log(
            StructuredLogLevel.WARN,
            message,
            title,
            context,
            exc,
            log=False,
            log_category=log_category,
        )

    def warning(
        self,
        message: LiteralString,
        context: Optional[str] = None,
        title: Optional[LiteralString] = None,
        exc: Optional[BaseException] = None,
        log: bool = True,
        log_category: Optional[StructuredLogCategory] = None,
    ) -> None:
        """
        See docs of StructuredLogs.report_log for details of args
        """
        self._structured_logs.report_log(
            StructuredLogLevel.WARN,
            message,
            title,
            context,
            exc,
            log=log,
            log_category=log_category,
        )

    def report_failure(
        self,
        message: LiteralString,
        context: Optional[str] = None,
        title: Optional[LiteralString] = None,
        exc: Optional[BaseException] = None,
        log: bool = True,
        log_category: Optional[StructuredLogCategory] = None,
    ) -> None:
        """
        See docs of StructuredLogs.report_log for details of args
        """
        self._structured_logs.report_log(
            StructuredLogLevel.ERROR,
            message,
            title,
            context,
            exc,
            log=log,
            log_category=log_category,
        )

    def failure(
        self,
        message: LiteralString,
        context: Optional[str] = None,
        title: Optional[LiteralString] = None,
        exc: Optional[BaseException] = None,
        log: bool = True,
        log_category: Optional[StructuredLogCategory] = None,
    ) -> None:
        """
        See docs of StructuredLogs.report_log for details of args
        """
        self._structured_logs.report_log(
            StructuredLogLevel.ERROR,
            message,
            title,
            context,
            exc,
            log=log,
            log_category=log_category,
        )

    def info(
        self,
        message: LiteralString,
        context: Optional[str] = None,
        title: Optional[LiteralString] = None,
        exc: Optional[BaseException] = None,
        log: bool = True,
        log_category: Optional[StructuredLogCategory] = None,
    ) -> None:
        """
        See docs of StructuredLogs.report_log for details of args
        """
        self._structured_logs.report_log(
            StructuredLogLevel.INFO,
            message,
            title,
            context,
            exc,
            log=log,
            log_category=log_category,
        )

    @contextlib.contextmanager
    def report_exc(
        self,
        message: LiteralString,
        title: Optional[LiteralString] = None,
        context: Optional[str] = None,
        level: StructuredLogLevel = StructuredLogLevel.ERROR,
        log_category: Optional[StructuredLogCategory] = None,
    ) -> Iterator[None]:
        # Convenience method that helps avoid boilerplate try/except blocks.
        # TODO: I'm not super happy with the naming here - it's not obvious that this
        # suppresses the exception in addition to reporting it.
        try:
            yield
        except Exception as exc:
            self._structured_logs.report_log(
                level,
                message=message,
                title=title,
                context=context,
                exc=exc,
                log_category=log_category,
            )

    def __post_init__(self) -> None:
        super().__post_init__()
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

    @staticmethod
    def _discretize_dict_values(
        nested_dict: Dict[str, Dict[str, int]],
    ) -> Dict[str, Dict[str, int]]:
        """Helper method to discretize values in a nested dictionary structure."""
        result = {}
        for outer_key, inner_dict in nested_dict.items():
            discretized_dict: Dict[str, int] = {}
            for inner_key, count in inner_dict.items():
                discretized_dict[inner_key] = stats.discretize(count)
            result[outer_key] = discretized_dict
        return result

    def get_aspects_dict(self) -> Dict[str, Dict[str, int]]:
        """Convert the nested defaultdict aspects to a regular dict for serialization."""
        return self._discretize_dict_values(self.aspects)

    def get_aspects_by_subtypes_dict(self) -> Dict[str, Dict[str, Dict[str, int]]]:
        """Get aspect counts grouped by entity type and subtype."""
        return self._discretize_dict_values_nested(self.aspects_by_subtypes)

    @staticmethod
    def _discretize_dict_values_nested(
        nested_dict: Dict[str, Dict[str, Dict[str, int]]],
    ) -> Dict[str, Dict[str, Dict[str, int]]]:
        """Helper method to discretize values in a nested dictionary structure with three levels."""
        result = {}
        for outer_key, middle_dict in nested_dict.items():
            discretized_middle_dict: Dict[str, Dict[str, int]] = {}
            for middle_key, inner_dict in middle_dict.items():
                discretized_inner_dict: Dict[str, int] = {}
                for inner_key, count in inner_dict.items():
                    discretized_inner_dict[inner_key] = stats.discretize(count)
                discretized_middle_dict[middle_key] = discretized_inner_dict
            result[outer_key] = discretized_middle_dict
        return result

    def compute_stats(self) -> None:
        super().compute_stats()

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
        self.config = config_class.model_validate(config_dict)

    @abstractmethod
    def get_records(self, workunit: WorkUnitType) -> Iterable[RecordEnvelope]:
        pass


@dataclass
class Source(Closeable, metaclass=ABCMeta):
    ctx: PipelineContext

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> Self:
        # Technically, this method should be abstract. However, the @config_class
        # decorator automatically generates a create method at runtime if one is
        # not defined. Python still treats the class as abstract because it thinks
        # the create method is missing.
        #
        # Once we're on Python 3.10, we can use the abc.update_abstractmethods(cls)
        # method in the config_class decorator. That would allow us to make this
        # method abstract.
        raise NotImplementedError('sources must implement "create"')

    def get_excluded_workunit_processors(
        self,
    ) -> "List[Union[str, Type[WorkunitProcessor]]]":
        """Processor classes or names to exclude from automatic discovery.

        Override ONLY when specific processors are architecturally unsafe for this source
        (e.g. parallelism incompatibilities). Default: empty list (no exclusions).

        Pass processor classes directly for type safety and IDE autocomplete:
            return [AutoStatusAspectProcessor, AutoBrowsePathV2Processor]

        The order in the master processor list is preserved.
        """
        return []

    def get_allowed_workunit_processors(
        self,
    ) -> "Optional[List[Union[str, Type[WorkunitProcessor]]]]":
        """Whitelist of processor classes or names to use. If None, all are considered.

        Override when your source should use ONLY a specific small set of
        processors (e.g. utility/cleanup sources). Default: None (use all).

        Pass processor classes directly for type safety and IDE autocomplete:
            return [AutoLowercaseUrnsProcessor, EnsureAspectSizeProcessor]

        The order in the master processor list is preserved.
        """
        return None

    def get_stale_entity_state_type(self) -> "Optional[Type[GenericCheckpointState]]":
        """Override to provide a custom checkpoint state type for stale entity removal."""
        return None

    def get_workunit_processors(self) -> List[Optional[MetadataWorkUnitProcessor]]:
        """A list of functions that transforms the workunits produced by this source.
        Run in order, first in list is applied first. Be careful with order when overriding.
        """
        # Deferred imports to avoid circular dependency:
        # source.py → workunit_processors → stale_entity_removal → stateful_ingestion_base → source.py
        from datahub.ingestion.api.workunit_processor import (
            StaleEntityRemovalContext,
            WorkunitProcessor,
            WorkunitProcessorContext,
        )
        from datahub.ingestion.source.state.entity_removal_state import (
            GenericCheckpointState,
        )
        from datahub.ingestion.workunit_processors import (
            AutoBrowsePathV2Processor,
            AutoIncrementalLineageProcessor,
            AutoIncrementalOwnershipProcessor,
            AutoIncrementalPropertiesProcessor,
            AutoLowercaseUrnsProcessor,
            AutoMaterializeReferencedTagsTermsProcessor,
            AutoPatchLastModifiedProcessor,
            AutoStaleEntityRemovalProcessor,
            AutoStatusAspectProcessor,
            AutoWorkunitsReporterProcessor,
            EnsureAspectSizeProcessor,
            ValidateDuplicateSchemaFieldPathsProcessor,
            ValidateEmptySchemaFieldPathsProcessor,
            ValidateInputFieldsProcessor,
        )

        # Build stale entity removal context for stateful sources.
        stale_entity_removal_ctx = None
        state_provider = getattr(self, "state_provider", None)
        if state_provider is not None:
            state_type_class = (
                self.get_stale_entity_state_type() or GenericCheckpointState
            )
            stale_entity_removal_ctx = StaleEntityRemovalContext(
                state_provider=state_provider,
                state_type_class=state_type_class,
            )

        ctx = WorkunitProcessorContext(
            source_report=self.get_report(),
            pipeline_context=self.ctx,
            source_config=self.get_config(),
            # Use the raw platform instance attribute to preserve backward-compatible
            # job IDs. Sources without self.platform fall back to "default" in
            # StaleEntityRemovalHandler._init_job_id(), matching the pre-refactor
            # behavior of getattr(source, "platform", "default").
            platform=getattr(self, "platform", None),
            # Fully inferred platform (includes @platform_name decorator fallback)
            # for processors like browse path that need the complete platform value.
            source_platform=self.infer_platform(),
            stale_entity_removal_context=stale_entity_removal_ctx,
        )

        # ORDER IS CRITICAL for deterministic output and golden file validation.
        # Do NOT reorder without understanding the impact on all sources.
        _ALL_PROCESSOR_CLASSES: List[Type[WorkunitProcessor]] = [
            AutoLowercaseUrnsProcessor,
            AutoStatusAspectProcessor,
            AutoMaterializeReferencedTagsTermsProcessor,
            ValidateDuplicateSchemaFieldPathsProcessor,
            ValidateEmptySchemaFieldPathsProcessor,
            AutoBrowsePathV2Processor,
            AutoIncrementalLineageProcessor,
            AutoIncrementalPropertiesProcessor,
            AutoIncrementalOwnershipProcessor,
            AutoWorkunitsReporterProcessor,
            AutoPatchLastModifiedProcessor,
            ValidateInputFieldsProcessor,
            EnsureAspectSizeProcessor,
            AutoStaleEntityRemovalProcessor,
        ]

        # Convert processor classes to names for comparison
        def _to_name(p: "Union[str, Type[WorkunitProcessor]]") -> str:
            return p.__name__ if isinstance(p, type) else p

        excluded = set(_to_name(p) for p in self.get_excluded_workunit_processors())
        allowed = self.get_allowed_workunit_processors()
        allowed_set = set(_to_name(p) for p in allowed) if allowed is not None else None

        processors: List[WorkunitProcessor] = []
        for processor_class in _ALL_PROCESSOR_CLASSES:
            name = processor_class.__name__
            if name in excluded:
                logger.info(f"Workunit processor '{name}' excluded by source")
                continue
            if allowed_set is not None and name not in allowed_set:
                logger.info(f"Workunit processor '{name}' not in allowed list")
                continue
            if processor_class.should_enable(ctx):
                logger.info(f"Workunit processor '{name}' enabled")
                try:
                    processors.append(processor_class.create(ctx))
                except Exception as e:
                    logger.error(
                        f"Failed to create workunit processor '{name}'", exc_info=True
                    )
                    raise RuntimeError(
                        f"Failed to initialize workunit processor '{name}': {e}"
                    ) from e
            else:
                logger.info(f"Workunit processor '{name}' disabled by should_enable()")

        return [p.process for p in processors]

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
        workunit_processors = self.get_workunit_processors()
        workunit_processors.append(AutoSystemMetadata(self.ctx).stamp)
        # Process main workunits
        yield from self._apply_workunit_processors(
            workunit_processors, auto_workunit(self.get_workunits_internal())
        )
        # Process profiling workunits
        yield from self._process_profiling_stage(workunit_processors)

    def _process_profiling_stage(
        self, processors: List[Optional[MetadataWorkUnitProcessor]]
    ) -> Iterable[MetadataWorkUnit]:
        """Process profiling stage if source supports it."""
        if (
            not isinstance(self, ProfilingCapable)
            or not self.is_profiling_enabled_internal()
        ):
            return
        with self.get_report().new_high_stage(IngestionHighStage.PROFILING):
            profiling_stream = self._apply_workunit_processors(
                processors, auto_workunit(self.get_profiling_internal())
            )
            yield from profiling_stream

    def get_workunits_internal(
        self,
    ) -> MetadataWorkUnitIterable:
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
        self.get_report().close()

    def infer_platform(self) -> Optional[str]:
        config = self.get_config()
        platform = (
            getattr(config, "platform_name", None)
            or getattr(self, "platform", None)
            or getattr(config, "platform", None)
        )
        if platform is None and hasattr(self, "get_platform_id"):
            platform = type(self).get_platform_id()

        return platform


class TestableSource(Source):
    @staticmethod
    @abstractmethod
    def test_connection(config_dict: dict) -> TestConnectionReport:
        raise NotImplementedError("This class does not implement this method")
