import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Callable, Collection, Iterable, Optional

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.unity.config import UnityCatalogProfilerConfig
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
class UnityCatalogProfiler:
    config: UnityCatalogProfilerConfig
    report: UnityCatalogReport
    proxy: UnityCatalogApiProxy
    dataset_urn_builder: Callable[[TableReference], str]

    def get_workunits(
        self, table_refs: Collection[TableReference]
    ) -> Iterable[MetadataWorkUnit]:
        try:
            with ThreadPoolExecutor(max_workers=self.config.max_workers) as executor:
                futures = [
                    executor.submit(self.process_table, ref) for ref in table_refs
                ]
                for future in as_completed(futures):
                    wu: MetadataWorkUnit = future.result()
                    if wu:
                        self.report.num_profile_workunits_emitted += 1
                        logger.info(f"SUCCESS {wu.metadata.entityUrn}")
                        yield wu
        except Exception as e:
            self.report.report_warning("profiling", str(e))
            logger.warning(f"Unexpected error during profiling: {e}", exc_info=True)
            return

    def process_table(self, ref: TableReference) -> Optional[MetadataWorkUnit]:
        table_profile = self.proxy.get_table_stats(ref, self.config.max_wait_secs)
        if table_profile:
            return self.gen_dataset_profile_workunit(ref, table_profile)
        elif table_profile is not None:
            self.report.profile_table_empty.append(str(ref))
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
            fieldProfiles=[
                self._gen_dataset_field_profile(row_count, column_profile)
                for column_profile in table_profile.column_profiles
                if column_profile  # Drop column profiles with no data
            ],
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
