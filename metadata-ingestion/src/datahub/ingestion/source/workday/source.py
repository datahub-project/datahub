import logging
from typing import Dict, Iterable, List, Optional, Tuple

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import (
    CapabilityReport,
    TestableSource,
    TestConnectionReport,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)
from datahub.ingestion.source.workday.client import (
    WorkdayAuthError,
    WorkdayClient,
)
from datahub.ingestion.source.workday.config import WorkdayConfig
from datahub.ingestion.source.workday.constants import (
    PRISM_SOURCE_TYPE_EXTERNAL,
    PRISM_SOURCE_TYPE_WORKDAY,
    WORKDAY_PLATFORM,
)
from datahub.ingestion.source.workday.mapper import WorkdayMapper
from datahub.ingestion.source.workday.models import (
    PrismDataset,
    PrismDataSource,
    PrismTable,
)
from datahub.ingestion.source.workday.report import WorkdayReport
from datahub.ingestion.source_report.ingestion_stage import METADATA_EXTRACTION

logger = logging.getLogger(__name__)


@platform_name("Workday")
@config_class(WorkdayConfig)
@support_status(SupportStatus.INCUBATING)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(SourceCapability.CONTAINERS, "Tenant emits as a container")
@capability(SourceCapability.DESCRIPTIONS, "Enabled by default")
@capability(SourceCapability.SCHEMA_METADATA, "Prism table and dataset schemas")
@capability(SourceCapability.TAGS, "Primary-key field tags")
@capability(SourceCapability.OWNERSHIP, "Enabled by default via `ingest_owner`")
@capability(
    SourceCapability.LINEAGE_COARSE,
    "Table-to-source lineage, plus external upstreams via "
    "`data_source_platform_mapping`",
)
@capability(SourceCapability.DELETION_DETECTION, "Enabled via stateful ingestion")
@capability(SourceCapability.TEST_CONNECTION, "Enabled by default")
class WorkdaySource(StatefulIngestionSourceBase, TestableSource):
    config: WorkdayConfig
    report: WorkdayReport
    platform: str = WORKDAY_PLATFORM

    def __init__(self, config: WorkdayConfig, ctx: PipelineContext):
        super().__init__(config, ctx)
        self.config = config
        self.report = WorkdayReport()
        self.client = WorkdayClient(config, self.report)
        self.mapper = WorkdayMapper(config, self.report)

    @classmethod
    def create(
        cls, config_dict: Dict[str, object], ctx: PipelineContext
    ) -> "WorkdaySource":
        config = WorkdayConfig.model_validate(config_dict)
        return cls(config, ctx)

    @staticmethod
    def test_connection(config_dict: Dict[str, object]) -> TestConnectionReport:
        test_report = TestConnectionReport()
        client: Optional[WorkdayClient] = None
        try:
            config = WorkdayConfig.parse_obj_allow_extras(config_dict)
            client = WorkdayClient(config, WorkdayReport())
            client.list_tables()
            test_report.basic_connectivity = CapabilityReport(capable=True)
        except Exception as error:
            test_report.basic_connectivity = CapabilityReport(
                capable=False, failure_reason=str(error)
            )
        finally:
            if client is not None:
                client.close()
        return test_report

    def get_report(self) -> WorkdayReport:
        return self.report

    def _include_external(self, source_type: Optional[str]) -> bool:
        if source_type != PRISM_SOURCE_TYPE_EXTERNAL:
            return True
        return self.config.include_external_tables

    def _discover(
        self,
    ) -> Tuple[List[PrismTable], List[PrismDataset], List[PrismDataSource]]:
        tables: List[PrismTable] = []
        datasets: List[PrismDataset] = []
        data_sources: List[PrismDataSource] = []

        if self.config.extract_tables:
            for table in self.client.list_tables():
                if not self.config.table_pattern.allowed(table.name):
                    self.report.filtered_tables.append(table.name)
                    continue
                if not self._include_external(table.source_type):
                    continue
                tables.append(table)

        if self.config.extract_datasets:
            for dataset in self.client.list_datasets():
                if not self.config.dataset_pattern.allowed(dataset.name):
                    self.report.filtered_datasets.append(dataset.name)
                    continue
                datasets.append(dataset)

        if self.config.extract_data_sources or self.config.extract_reports:
            for data_source in self.client.list_data_sources():
                is_report = data_source.source_type == PRISM_SOURCE_TYPE_WORKDAY
                if is_report:
                    if not self.config.extract_reports:
                        continue
                    if not self.config.report_pattern.allowed(data_source.name):
                        self.report.filtered_reports.append(data_source.name)
                        continue
                else:
                    if not self.config.extract_data_sources:
                        continue
                    if not self.config.data_source_pattern.allowed(data_source.name):
                        self.report.filtered_data_sources.append(data_source.name)
                        continue
                    if not self._include_external(data_source.source_type):
                        continue
                data_sources.append(data_source)

        return tables, datasets, data_sources

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        try:
            with self.report.new_stage(METADATA_EXTRACTION):
                tables, datasets, data_sources = self._discover()
        except WorkdayAuthError as error:
            self.report.failure(
                title="Workday Authentication Failed",
                message="Could not obtain an OAuth token; aborting the run.",
                exc=error,
            )
            return

        # id -> URN across every emitted object so lineage edges resolve; data
        # source names feed external-warehouse upstream resolution.
        id_to_urn: Dict[str, str] = {}
        for table in tables:
            id_to_urn[table.id] = self.mapper.dataset_urn("table", table.id)
        for dataset in datasets:
            id_to_urn[dataset.id] = self.mapper.dataset_urn("dataset", dataset.id)
        for data_source in data_sources:
            id_to_urn[data_source.id] = self.mapper.dataset_urn(
                "source", data_source.id
            )
        data_source_names = {ds.id: ds.name for ds in data_sources}

        yield from self.mapper.gen_tenant_container()

        for table in tables:
            yield from self.mapper.map_table(table, id_to_urn, data_source_names)
        for dataset in datasets:
            yield from self.mapper.map_dataset(dataset, id_to_urn, data_source_names)
        for data_source in data_sources:
            yield from self.mapper.map_data_source(data_source, id_to_urn)

        self.client.close()
