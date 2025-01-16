import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional

import numpy as np
from cassandra.util import OrderedMapSerializedKey, SortedSet

from datahub.emitter.mce_builder import make_dataset_urn_with_platform_instance
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.cassandra.cassandra_api import (
    CassandraAPI,
    CassandraColumn,
    CassandraEntities,
    CassandraQueries,
)
from datahub.ingestion.source.cassandra.cassandra_config import CassandraSourceConfig
from datahub.ingestion.source.cassandra.cassandra_utils import CassandraSourceReport
from datahub.ingestion.source_report.ingestion_stage import PROFILING
from datahub.metadata.schema_classes import (
    DatasetFieldProfileClass,
    DatasetProfileClass,
    QuantileClass,
)

logger = logging.getLogger(__name__)


@dataclass
class ColumnMetric:
    col_type: str = ""
    values: List[Any] = field(default_factory=list)
    null_count: int = 0
    total_count: int = 0
    distinct_count: Optional[int] = None
    min: Optional[Any] = None
    max: Optional[Any] = None
    mean: Optional[float] = None
    stdev: Optional[float] = None
    median: Optional[float] = None
    quantiles: Optional[List[float]] = None
    sample_values: Optional[Any] = None


@dataclass
class ProfileData:
    row_count: Optional[int] = None
    column_count: Optional[int] = None
    column_metrics: Dict[str, ColumnMetric] = field(default_factory=dict)


class CassandraProfiler:
    config: CassandraSourceConfig
    report: CassandraSourceReport

    def __init__(
        self,
        config: CassandraSourceConfig,
        report: CassandraSourceReport,
        api: CassandraAPI,
    ) -> None:
        self.api = api
        self.config = config
        self.report = report

    def get_workunits(
        self, cassandra_data: CassandraEntities
    ) -> Iterable[MetadataWorkUnit]:
        for keyspace_name in cassandra_data.keyspaces:
            tables = cassandra_data.tables.get(keyspace_name, [])
            with self.report.new_stage(f"{keyspace_name}: {PROFILING}"):
                with ThreadPoolExecutor(
                    max_workers=self.config.profiling.max_workers
                ) as executor:
                    future_to_dataset = {
                        executor.submit(
                            self.generate_profile,
                            keyspace_name,
                            table_name,
                            cassandra_data.columns.get(table_name, []),
                        ): table_name
                        for table_name in tables
                    }
                    for future in as_completed(future_to_dataset):
                        table_name = future_to_dataset[future]
                        try:
                            yield from future.result()
                        except Exception as exc:
                            self.report.profiling_skipped_other[table_name] += 1
                            self.report.failure(
                                message="Failed to profile for table",
                                context=f"{keyspace_name}.{table_name}",
                                exc=exc,
                            )

    def generate_profile(
        self,
        keyspace_name: str,
        table_name: str,
        columns: List[CassandraColumn],
    ) -> Iterable[MetadataWorkUnit]:
        dataset_name: str = f"{keyspace_name}.{table_name}"
        dataset_urn = make_dataset_urn_with_platform_instance(
            platform="cassandra",
            name=dataset_name,
            env=self.config.env,
            platform_instance=self.config.platform_instance,
        )

        if not columns:
            self.report.warning(
                message="Skipping profiling as no columns found for table",
                context=f"{keyspace_name}.{table_name}",
            )
            self.report.profiling_skipped_other[table_name] += 1
            return

        if not self.config.profile_pattern.allowed(f"{keyspace_name}.{table_name}"):
            self.report.profiling_skipped_table_profile_pattern[keyspace_name] += 1
            logger.info(
                f"Table {table_name} in {keyspace_name}, not allowed for profiling"
            )
            return

        try:
            profile_data = self.profile_table(keyspace_name, table_name, columns)
        except Exception as e:
            self.report.warning(
                message="Profiling Failed",
                context=f"{keyspace_name}.{table_name}",
                exc=e,
            )
            return

        profile_aspect = self.populate_profile_aspect(profile_data)

        if profile_aspect:
            self.report.report_entity_profiled(table_name)
            mcp = MetadataChangeProposalWrapper(
                entityUrn=dataset_urn, aspect=profile_aspect
            )
            yield mcp.as_workunit()

    def populate_profile_aspect(self, profile_data: ProfileData) -> DatasetProfileClass:
        field_profiles = [
            self._create_field_profile(column_name, column_metrics)
            for column_name, column_metrics in profile_data.column_metrics.items()
        ]
        return DatasetProfileClass(
            timestampMillis=round(time.time() * 1000),
            rowCount=profile_data.row_count,
            columnCount=profile_data.column_count,
            fieldProfiles=field_profiles,
        )

    def _create_field_profile(
        self, field_name: str, field_stats: ColumnMetric
    ) -> DatasetFieldProfileClass:
        quantiles = field_stats.quantiles
        return DatasetFieldProfileClass(
            fieldPath=field_name,
            uniqueCount=field_stats.distinct_count,
            nullCount=field_stats.null_count,
            min=str(field_stats.min) if field_stats.min else None,
            max=str(field_stats.max) if field_stats.max else None,
            mean=str(field_stats.mean) if field_stats.mean else None,
            median=str(field_stats.median) if field_stats.median else None,
            stdev=str(field_stats.stdev) if field_stats.stdev else None,
            quantiles=[
                QuantileClass(quantile=str(0.25), value=str(quantiles[0])),
                QuantileClass(quantile=str(0.75), value=str(quantiles[1])),
            ]
            if quantiles
            else None,
            sampleValues=field_stats.sample_values
            if field_stats.sample_values
            else None,
        )

    def profile_table(
        self, keyspace_name: str, table_name: str, columns: List[CassandraColumn]
    ) -> ProfileData:
        profile_data = ProfileData()

        resp = self.api.execute(
            CassandraQueries.ROW_COUNT.format(keyspace_name, table_name)
        )
        if resp:
            profile_data.row_count = resp[0].row_count

        profile_data.column_count = len(columns)

        if not self.config.profiling.profile_table_level_only:
            resp = self.api.execute(
                f'SELECT {", ".join([col.column_name for col in columns])} FROM {keyspace_name}."{table_name}"'
            )
            profile_data.column_metrics = self._collect_column_data(resp, columns)

        return self._parse_profile_results(profile_data)

    def _parse_profile_results(self, profile_data: ProfileData) -> ProfileData:
        for cl_name, column_metrics in profile_data.column_metrics.items():
            if column_metrics.values:
                try:
                    self._compute_field_statistics(column_metrics)
                except Exception as e:
                    self.report.warning(
                        message="Profiling Failed For Column Stats",
                        context=cl_name,
                        exc=e,
                    )
                    raise e
        return profile_data

    def _collect_column_data(
        self, rows: List[Any], columns: List[CassandraColumn]
    ) -> Dict[str, ColumnMetric]:
        metrics = {column.column_name: ColumnMetric() for column in columns}

        for row in rows:
            for column in columns:
                if self._is_skippable_type(column.type):
                    continue

                value: Any = getattr(row, column.column_name, None)
                metric = metrics[column.column_name]
                metric.col_type = column.type

                metric.total_count += 1
                if value is None:
                    metric.null_count += 1
                else:
                    metric.values.extend(self._parse_value(value))

        return metrics

    def _is_skippable_type(self, data_type: str) -> bool:
        return data_type.lower() in ["timeuuid", "blob", "frozen<tuple<tinyint, text>>"]

    def _parse_value(self, value: Any) -> List[Any]:
        if isinstance(value, SortedSet):
            return list(value)
        elif isinstance(value, OrderedMapSerializedKey):
            return list(dict(value).values())
        elif isinstance(value, list):
            return value
        return [value]

    def _compute_field_statistics(self, column_metrics: ColumnMetric) -> None:
        values = column_metrics.values
        if not values:
            return

        # ByDefault Null count is added
        if not self.config.profiling.include_field_null_count:
            column_metrics.null_count = 0

        if self.config.profiling.include_field_distinct_count:
            column_metrics.distinct_count = len(set(values))

        if self.config.profiling.include_field_min_value:
            column_metrics.min = min(values)

        if self.config.profiling.include_field_max_value:
            column_metrics.max = max(values)

        if values and self._is_numeric_type(column_metrics.col_type):
            if self.config.profiling.include_field_mean_value:
                column_metrics.mean = round(float(np.mean(values)), 2)
            if self.config.profiling.include_field_stddev_value:
                column_metrics.stdev = round(float(np.std(values)), 2)
            if self.config.profiling.include_field_median_value:
                column_metrics.median = round(float(np.median(values)), 2)
            if self.config.profiling.include_field_quantiles:
                column_metrics.quantiles = [
                    float(np.percentile(values, 25)),
                    float(np.percentile(values, 75)),
                ]

        if values and self.config.profiling.include_field_sample_values:
            column_metrics.sample_values = [str(v) for v in values[:5]]

    def _is_numeric_type(self, data_type: str) -> bool:
        return data_type.lower() in [
            "int",
            "counter",
            "bigint",
            "float",
            "double",
            "decimal",
            "smallint",
            "tinyint",
            "varint",
        ]
