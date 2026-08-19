import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Dict, Generic, Iterable, Optional, Type, TypeVar, cast

from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.utilities.type_annotations import get_class_from_annotation

if TYPE_CHECKING:
    from datahub.configuration.common import ConfigModel
    from datahub.ingestion.api.common import PipelineContext
    from datahub.ingestion.api.source import SourceReport
    from datahub.ingestion.source.state.entity_removal_state import (
        GenericCheckpointState,
    )
    from datahub.ingestion.source.state.stateful_ingestion_base import (
        StateProviderWrapper,
    )

logger = logging.getLogger(__name__)

_WorkunitProcessorT = TypeVar("_WorkunitProcessorT", bound="WorkunitProcessor")
_ReportT = TypeVar("_ReportT", bound="WorkunitProcessorReport")


@dataclass
class WorkunitProcessorReport:
    """Base report class for workunit processor metrics."""

    def as_obj(self) -> Dict[str, object]:
        return {
            key: value
            for key, value in self.__dict__.items()
            if not key.startswith("_")
        }


@dataclass
class StaleEntityRemovalContext:
    """Context for stale entity removal processor."""

    state_provider: "StateProviderWrapper"
    state_type_class: "Type[GenericCheckpointState]"


@dataclass
class WorkunitProcessorContext:
    """Context passed to all workunit processors."""

    source_report: "SourceReport"
    pipeline_context: "PipelineContext"
    source_config: "Optional[ConfigModel]"
    platform: Optional[str]
    # Fully inferred platform (includes @platform_name decorator fallback).
    # Use for browse path generation. Separate from `platform` (raw attribute)
    # which is used for backward-compatible stale removal job IDs.
    source_platform: Optional[str] = None
    stale_entity_removal_context: Optional[StaleEntityRemovalContext] = None

    def infer_platform(self) -> Optional[str]:
        return self.platform or getattr(self.source_config, "platform", None)


class WorkunitProcessor(ABC, Generic[_ReportT]):
    """Base class for all workunit processors.

    Workunit processors are stream transformers that apply common logic to
    metadata workunits across all sources.

    Naming Convention:
    - Auto*Processor: Processors that automatically enrich metadata by adding new data
      (e.g., AutoStatusAspectProcessor, AutoBrowsePathV2Processor)
    - Validate*Processor: Processors that validate and cleanup data by removing invalid entries
      (e.g., ValidateInputFieldsProcessor, ValidateDuplicateSchemaFieldPathsProcessor)
    - Ensure*Processor: Processors that enforce constraints by modifying data to fit limits
      (e.g., EnsureAspectSizeProcessor)

    Type Parameters:
    - _ReportT: The report type for this processor (subclass of WorkunitProcessorReport).
      The report class is automatically extracted from the generic parameter at runtime.

    Example:
        @dataclass
        class MyProcessorReport(WorkunitProcessorReport):
            num_processed: int = 0

        class MyProcessor(WorkunitProcessor[MyProcessorReport]):
            def process(self, stream):
                for wu in stream:
                    self.report.num_processed += 1  # Properly typed!
                    yield wu
    """

    def __init__(self, ctx: WorkunitProcessorContext) -> None:
        self.ctx = ctx
        self.report: _ReportT

    @classmethod
    def create(
        cls: Type[_WorkunitProcessorT], ctx: WorkunitProcessorContext
    ) -> _WorkunitProcessorT:
        """Instantiate processor and register its report with the source report."""
        processor = cls(ctx)
        report_class = get_class_from_annotation(
            cls, WorkunitProcessor, WorkunitProcessorReport
        )
        assert report_class, f"{cls.__name__} must specify a report type parameter"
        processor.report = cast(_ReportT, report_class())
        ctx.source_report.workunit_processor_reports[cls.__name__] = processor.report
        return processor

    @classmethod
    def should_enable(cls, ctx: WorkunitProcessorContext) -> bool:
        """Return True if this processor should be enabled for the given context."""
        return True

    @abstractmethod
    def process(self, stream: Iterable[MetadataWorkUnit]) -> Iterable[MetadataWorkUnit]:
        """Transform a stream of metadata workunits."""
