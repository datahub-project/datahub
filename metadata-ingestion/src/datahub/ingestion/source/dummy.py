import logging
from dataclasses import dataclass, field as dataclass_field
from typing import Iterable, List, Optional

import pydantic
from pydantic import Field

from datahub.configuration.common import AllowDenyPattern
from datahub.configuration.source_common import DEFAULT_ENV, DatasetSourceConfigMixin
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import MetadataWorkUnitProcessor, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
    StaleEntityRemovalSourceReport,
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
    StatefulIngestionSourceBase,
)
from datahub.metadata.schema_classes import StatusClass
from datahub.utilities.urns.dataset_urn import DatasetUrn

# Logger instance
logger = logging.getLogger(__name__)

dummy_datasets: List = ["dummy_dataset1", "dummy_dataset2", "dummy_dataset3"]


@dataclass
class DummySourceReport(StaleEntityRemovalSourceReport):
    datasets_scanned: int = 0
    filtered_datasets: List[str] = dataclass_field(default_factory=list)

    def report_datasets_scanned(self, count: int = 1) -> None:
        self.datasets_scanned += count

    def report_datasets_dropped(self, model: str) -> None:
        self.filtered_datasets.append(model)


class DummySourceConfig(StatefulIngestionConfigBase, DatasetSourceConfigMixin):
    dataset_patterns: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for datasets to filter in ingestion.",
    )
    # Configuration for stateful ingestion
    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = pydantic.Field(
        default=None, description="Dummy source Ingestion Config."
    )


class DummySource(StatefulIngestionSourceBase):
    """
    This is dummy source which only extract dummy datasets
    """

    source_config: DummySourceConfig
    reporter: DummySourceReport
    platform: str = "dummy"

    def __init__(self, config: DummySourceConfig, ctx: PipelineContext):
        super(DummySource, self).__init__(config, ctx)
        self.source_config = config
        self.reporter = DummySourceReport()
        # Create and register the stateful ingestion use-case handler.
        self.stale_entity_removal_handler = StaleEntityRemovalHandler.create(
            self, self.source_config, self.ctx
        )

    @classmethod
    def create(cls, config_dict, ctx):
        config = DummySourceConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def get_workunit_processors(self) -> List[Optional[MetadataWorkUnitProcessor]]:
        return [
            *super().get_workunit_processors(),
            self.stale_entity_removal_handler.workunit_processor,
        ]

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        """
        Datahub Ingestion framework invoke this method
        """
        logger.info("Dummy plugin execution is started")

        for dataset in dummy_datasets:
            if not self.source_config.dataset_patterns.allowed(dataset):
                self.reporter.report_datasets_dropped(dataset)
                continue
            else:
                self.reporter.report_datasets_scanned()
            dataset_urn = DatasetUrn.create_from_ids(
                platform_id="postgres",
                table_name=dataset,
                env=DEFAULT_ENV,
            )
            yield MetadataChangeProposalWrapper(
                entityUrn=str(dataset_urn),
                aspect=StatusClass(removed=False),
            ).as_workunit()

    def get_report(self) -> SourceReport:
        return self.reporter
