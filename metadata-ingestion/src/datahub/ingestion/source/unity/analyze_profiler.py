import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Callable, Collection, Iterable, Optional

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.unity.config import UnityCatalogAnalyzeProfilerConfig
from datahub.ingestion.source.unity.proxy import UnityCatalogApiProxy
from datahub.ingestion.source.unity.proxy_types import (
    ColumnProfile,
    TableProfile,
    TableReference,
)
from datahub.ingestion.source.unity.report import UnityCatalogReport
from datahub.metadata.schema_classes import (
    DatasetFieldProfileClass,
    DatasetProfileClass,
)

logger = logging.getLogger(__name__)


@dataclass
class UnityCatalogAnalyzeProfiler:
    config: UnityCatalogAnalyzeProfilerConfig
    report: UnityCatalogReport
    proxy: UnityCatalogApiProxy
    dataset_urn_builder: Callable[[TableReference], str]

    def get_workunits(
        self, table_refs: Collection[TableReference]
    ) -> Iterable[MetadataWorkUnit]:
        try:
            tables = self._filter_tables(table_refs)
            with ThreadPoolExecutor(max_workers=self.config.max_workers) as executor:
                futures = [executor.submit(self.process_table, ref) for ref in tables]
                for future in as_completed(futures):
                    wu: Optional[MetadataWorkUnit] = future.result()
                    if wu:
                        yield wu
        except Exception as e:
            self.report.report_warning("profiling", str(e))
            logger.warning(f"Unexpected error during profiling: {e}", exc_info=True)
            return

    def _filter_tables(
        self, table_refs: Collection[TableReference]
    ) -> Collection[TableReference]:
        return [
            ref
            for ref in table_refs
            if self.config.pattern.allowed(ref.qualified_table_name)
        ]

    def process_table(self, ref: TableReference) -> Optional[MetadataWorkUnit]:
        try:
            table_profile = self.proxy.get_table_stats(
                ref,
                max_wait_secs=self.config.max_wait_secs,
                call_analyze=self.config.call_analyze,
                include_columns=self.config.include_columns,
            )
            if table_profile:
                return self.gen_dataset_profile_workunit(ref, table_profile)
            elif table_profile is not None:  # table_profile is Falsy == empty
                self.report.profile_table_empty.append(str(ref))
        except Exception as e:
            self.report.report_warning("profiling", str(e))
            logger.warning(
                f"Unexpected error during profiling table {ref}: {e}", exc_info=True
            )
        return None

    def gen_dataset_profile_workunit(
        self, ref: TableReference, table_profile: TableProfile
    ) -> MetadataWorkUnit:
        row_count = table_profile.num_rows
        aspect = DatasetProfileClass(
            timestampMillis=int(time.time() * 1000),
            rowCount=row_count,
            columnCount=table_profile.num_columns,
            sizeInBytes=table_profile.total_size,
            fieldProfiles=(
                [
                    self._gen_dataset_field_profile(row_count, column_profile)
                    for column_profile in table_profile.column_profiles
                    if column_profile  # Drop column profiles with no data
                ]
                if self.config.include_columns
                else None
            ),
        )
        return MetadataChangeProposalWrapper(
            entityUrn=self.dataset_urn_builder(ref),
            aspect=aspect,
        ).as_workunit()

    @staticmethod
    def _gen_dataset_field_profile(
        num_rows: Optional[int], column_profile: ColumnProfile
    ) -> DatasetFieldProfileClass:
        unique_proportion: Optional[float] = None
        null_proportion: Optional[float] = None
        if num_rows:
            if column_profile.distinct_count is not None:
                unique_proportion = min(1.0, column_profile.distinct_count / num_rows)
            if column_profile.null_count is not None:
                null_proportion = min(1.0, column_profile.null_count / num_rows)

        return DatasetFieldProfileClass(
            fieldPath=column_profile.name,
            uniqueCount=column_profile.distinct_count,
            uniqueProportion=unique_proportion,
            nullCount=column_profile.null_count,
            nullProportion=null_proportion,
            min=column_profile.min,
            max=column_profile.max,
        )
