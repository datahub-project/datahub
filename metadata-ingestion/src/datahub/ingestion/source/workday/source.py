import logging
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional

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
    CustomReport,
    PrismBucket,
    PrismDataset,
    PrismDataSource,
    PrismTable,
    WqlDataSource,
)
from datahub.ingestion.source.workday.report import WorkdayReport
from datahub.ingestion.source_report.ingestion_stage import METADATA_EXTRACTION

logger = logging.getLogger(__name__)


@dataclass
class _Discovered:
    """The Workday objects selected for ingestion after filtering."""

    tables: List[PrismTable] = field(default_factory=list)
    datasets: List[PrismDataset] = field(default_factory=list)
    data_sources: List[PrismDataSource] = field(default_factory=list)
    buckets: List[PrismBucket] = field(default_factory=list)
    business_objects: List[WqlDataSource] = field(default_factory=list)
    custom_reports: List[CustomReport] = field(default_factory=list)


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
@capability(
    SourceCapability.LINEAGE_FINE,
    "Column-level lineage from Prism dataset field mappings via "
    "`extract_column_level_lineage`",
)
@capability(
    SourceCapability.DATA_PROFILING,
    "Prism table row counts via `include_row_counts`",
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

    def _discover(self) -> _Discovered:
        discovered = _Discovered()

        if self.config.extract_tables:
            for table in self.client.list_tables():
                if not self.config.table_pattern.allowed(table.name):
                    self.report.filtered_tables.append(table.name)
                    continue
                if not self._include_external(table.source_type):
                    continue
                discovered.tables.append(table)

        if self.config.extract_datasets:
            for dataset in self.client.list_datasets():
                if not self.config.dataset_pattern.allowed(dataset.name):
                    self.report.filtered_datasets.append(dataset.name)
                    continue
                discovered.datasets.append(dataset)

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
                discovered.data_sources.append(data_source)

        if self.config.extract_buckets:
            for bucket in self.client.list_buckets():
                if not self.config.bucket_pattern.allowed(bucket.name):
                    self.report.filtered_buckets.append(bucket.name)
                    continue
                discovered.buckets.append(bucket)

        self._discover_wql(discovered)
        return discovered

    def _discover_wql(self, discovered: _Discovered) -> None:
        """Discover WQL business objects and custom reports (beyond-Prism, opt-in)."""
        if self.config.extract_business_objects:
            for business_object in self.client.list_business_objects():
                if not self.config.business_object_pattern.allowed(
                    business_object.name
                ):
                    self.report.filtered_business_objects.append(business_object.name)
                    continue
                # Fields are a separate call; hydrate only the ones we keep.
                if not business_object.fields:
                    business_object.fields = self.client.get_business_object_fields(
                        business_object.id
                    )
                discovered.business_objects.append(business_object)

        if self.config.extract_custom_reports:
            for report in self.client.list_custom_reports():
                if not self.config.custom_report_pattern.allowed(report.name):
                    self.report.filtered_custom_reports.append(report.name)
                    continue
                discovered.custom_reports.append(report)

    def _hydrate(self, discovered: _Discovered) -> None:
        """Replace summary list objects with full detail via GET-by-id.

        Prism list endpoints usually omit schema fields, lineage relationships,
        timestamps, and transformation logic; the detail endpoint carries them.
        """
        if not self.config.extract_schema_details:
            return
        for table_index, table in enumerate(discovered.tables):
            if table.has_full_detail():
                continue
            detailed_table = self.client.get_table(table.id)
            if detailed_table is not None:
                discovered.tables[table_index] = detailed_table
                self.report.report_table_hydrated()
        for dataset_index, dataset in enumerate(discovered.datasets):
            detailed_dataset = self.client.get_dataset(dataset.id)
            if detailed_dataset is not None:
                discovered.datasets[dataset_index] = detailed_dataset
                self.report.report_dataset_hydrated()

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        try:
            with self.report.new_stage(METADATA_EXTRACTION):
                discovered = self._discover()
                self._hydrate(discovered)
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
        for table in discovered.tables:
            id_to_urn[table.id] = self.mapper.dataset_urn("table", table.id)
        for dataset in discovered.datasets:
            id_to_urn[dataset.id] = self.mapper.dataset_urn("dataset", dataset.id)
        for data_source in discovered.data_sources:
            id_to_urn[data_source.id] = self.mapper.dataset_urn(
                "source", data_source.id
            )
        for bucket in discovered.buckets:
            id_to_urn[bucket.id] = self.mapper.dataset_urn("bucket", bucket.id)
        for business_object in discovered.business_objects:
            id_to_urn[business_object.id] = self.mapper.dataset_urn(
                "businessObject", business_object.id
            )
        for report in discovered.custom_reports:
            id_to_urn[report.id] = self.mapper.dataset_urn("report", report.id)

        data_source_names = {ds.id: ds.name for ds in discovered.data_sources}
        # A table is produced by the dataset whose output_table_id points at it;
        # that dataset carries the field mappings that drive column-level lineage.
        dataset_by_output_table: Dict[str, PrismDataset] = {
            dataset.output_table_id: dataset
            for dataset in discovered.datasets
            if dataset.output_table_id
        }
        # Buckets feeding each table become extra upstreams of that table.
        bucket_urns_by_target_table: Dict[str, List[str]] = {}
        for bucket in discovered.buckets:
            if bucket.target_table_id:
                bucket_urns_by_target_table.setdefault(
                    bucket.target_table_id, []
                ).append(id_to_urn[bucket.id])
        business_object_by_name = {bo.name: bo for bo in discovered.business_objects}

        yield from self.mapper.gen_tenant_container()
        yield from self._gen_functional_area_containers(discovered)

        for table in discovered.tables:
            yield from self.mapper.map_table(
                table,
                id_to_urn,
                data_source_names,
                producing_dataset=dataset_by_output_table.get(table.id),
                extra_upstream_urns=bucket_urns_by_target_table.get(table.id),
            )
        for dataset in discovered.datasets:
            yield from self.mapper.map_dataset(dataset, id_to_urn, data_source_names)
        for data_source in discovered.data_sources:
            yield from self.mapper.map_data_source(data_source, id_to_urn)
        for bucket in discovered.buckets:
            yield from self.mapper.map_bucket(bucket, id_to_urn)
        for business_object in discovered.business_objects:
            yield from self.mapper.map_business_object(business_object, id_to_urn)
        for report in discovered.custom_reports:
            yield from self.mapper.map_custom_report(
                report, id_to_urn, business_object_by_name
            )

        self.client.close()

    def _gen_functional_area_containers(
        self, discovered: _Discovered
    ) -> Iterable[MetadataWorkUnit]:
        """Emit one sub-container per distinct functional area used by an object."""
        if not self.config.use_functional_area_containers:
            return
        areas = {
            obj.category
            for obj in (*discovered.business_objects, *discovered.custom_reports)
            if obj.category
        }
        for area in sorted(areas):
            yield from self.mapper.gen_functional_area_container(area)
