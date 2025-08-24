import logging
import re
import time
from typing import Any, Dict, Iterable, List, Optional, Tuple

from pydantic import BaseModel, Field

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.dremio.dremio_api import (
    DremioAPIException,
    DremioAPIOperations,
)
from datahub.ingestion.source.dremio.dremio_config import DremioSourceConfig
from datahub.ingestion.source.dremio.dremio_entities import DremioDataset
from datahub.ingestion.source.dremio.dremio_reporting import DremioSourceReport
from datahub.metadata.schema_classes import (
    DatasetFieldProfileClass,
    DatasetProfileClass,
    QuantileClass,
)

logger = logging.getLogger(__name__)


class ColumnProfileStats(BaseModel):
    """Statistics for a single column profile."""

    distinct_count: Optional[int] = None
    null_count: Optional[int] = None
    min_value: Optional[Any] = Field(None, alias="min")
    max_value: Optional[Any] = Field(None, alias="max")
    mean: Optional[float] = None
    median: Optional[float] = None
    stdev: Optional[float] = None
    quantiles: Optional[List[float]] = None
    sample_values: Optional[List[str]] = None

    class Config:
        allow_population_by_field_name = True


class TableProfileData(BaseModel):
    """Complete profile data for a table."""

    row_count: int = 0
    column_count: int = 0
    column_stats: Dict[str, ColumnProfileStats] = Field(default_factory=dict)

    def add_column_stats(self, column_name: str, stats: ColumnProfileStats) -> None:
        """Add column statistics to the profile."""
        self.column_stats[column_name] = stats

    def get_column_stats(self, column_name: str) -> Optional[ColumnProfileStats]:
        """Get statistics for a specific column."""
        return self.column_stats.get(column_name)


class ProfileChunkResult(BaseModel):
    """Result from profiling a chunk of columns."""

    row_count: int = 0
    column_count: int = 0
    column_stats: Dict[str, ColumnProfileStats] = Field(default_factory=dict)

    @classmethod
    def from_query_result(
        cls,
        result: Dict[str, Any],
        columns: List[Tuple[str, str]],
        config: Optional["DremioSourceConfig"] = None,
    ) -> "ProfileChunkResult":
        """Create ProfileChunkResult from raw query results."""
        chunk_result = cls(
            row_count=int(result.get("row_count", 0)),
            column_count=int(result.get("column_count", 0)),
        )

        for column_name, data_type in columns:
            safe_column_name = re.sub(r"\W|^(?=\d)", "_", column_name)

            # Extract column statistics from result based on config
            stats = ColumnProfileStats()

            if config is None or config.profiling.include_field_distinct_count:
                stats.distinct_count = cls._safe_int(
                    result.get(f"{safe_column_name}_distinct_count")
                )

            if config is None or config.profiling.include_field_null_count:
                stats.null_count = cls._safe_int(
                    result.get(f"{safe_column_name}_null_count")
                )

            if config is None or config.profiling.include_field_min_value:
                stats.min_value = result.get(f"{safe_column_name}_min")

            if config is None or config.profiling.include_field_max_value:
                stats.max_value = result.get(f"{safe_column_name}_max")

            # Handle numeric type statistics
            if data_type.lower() in [
                "int",
                "integer",
                "bigint",
                "float",
                "double",
                "decimal",
            ]:
                if config is None or config.profiling.include_field_mean_value:
                    stats.mean = cls._safe_float(result.get(f"{safe_column_name}_mean"))

                if config is None or config.profiling.include_field_stddev_value:
                    stats.stdev = cls._safe_float(
                        result.get(f"{safe_column_name}_stdev")
                    )

                if config is None or config.profiling.include_field_median_value:
                    stats.median = cls._safe_float(
                        result.get(f"{safe_column_name}_median")
                    )

                if config is None or config.profiling.include_field_quantiles:
                    q25 = cls._safe_float(
                        result.get(f"{safe_column_name}_25th_percentile")
                    )
                    q75 = cls._safe_float(
                        result.get(f"{safe_column_name}_75th_percentile")
                    )
                    if q25 is not None and q75 is not None:
                        stats.quantiles = [q25, q75]

            chunk_result.column_stats[column_name] = stats

        return chunk_result

    @staticmethod
    def _safe_int(value: Any) -> Optional[int]:
        """Safely convert value to int."""
        if value is None:
            return None
        try:
            return int(value)
        except (ValueError, TypeError):
            return None

    @staticmethod
    def _safe_float(value: Any) -> Optional[float]:
        """Safely convert value to float."""
        if value is None:
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None


class DremioProfiler:
    config: DremioSourceConfig
    report: DremioSourceReport
    MAX_COLUMNS_PER_QUERY = 800

    def __init__(
        self,
        config: DremioSourceConfig,
        report: DremioSourceReport,
        api_operations: DremioAPIOperations,
    ) -> None:
        self.api_operations = api_operations
        self.config = config
        self.report = report
        self.QUERY_TIMEOUT = (
            config.profiling.query_timeout
        )  # 5 minutes timeout for each query

    def get_workunits(
        self, dataset: DremioDataset, dataset_urn: str
    ) -> Iterable[MetadataWorkUnit]:
        if not dataset.columns:
            self.report.warning(
                message="Skipping profiling as no columns found for table",
                context=f"{dataset.resource_name}",
            )
            self.report.profiling_skipped_other[dataset.resource_name] += 1
            return

        full_table_name = (
            '"' + '"."'.join(dataset.path) + '"."' + dataset.resource_name + '"'
        )

        columns = [(col.name, col.data_type) for col in dataset.columns]

        if not self.config.profile_pattern.allowed(full_table_name):
            self.report.profiling_skipped_table_profile_pattern[full_table_name] += 1
            self.report.warning(
                message="Profiling is restricted due to the specified profile pattern.",
                context=f"{full_table_name}",
            )
            return

        profile_data = self.profile_table(full_table_name, columns)
        profile_aspect = self.populate_profile_aspect(profile_data)

        if profile_aspect:
            self.report.report_entity_profiled(dataset.resource_name)
            mcp = MetadataChangeProposalWrapper(
                entityUrn=dataset_urn, aspect=profile_aspect
            )
            yield mcp.as_workunit()

    def populate_profile_aspect(
        self, profile_data: TableProfileData
    ) -> DatasetProfileClass:
        field_profiles = [
            self._create_field_profile(field_name, field_stats)
            for field_name, field_stats in profile_data.column_stats.items()
        ]
        return DatasetProfileClass(
            timestampMillis=round(time.time() * 1000),
            rowCount=profile_data.row_count,
            columnCount=profile_data.column_count,
            fieldProfiles=field_profiles,
        )

    def _create_field_profile(
        self, field_name: str, field_stats: ColumnProfileStats
    ) -> DatasetFieldProfileClass:
        return DatasetFieldProfileClass(
            fieldPath=field_name,
            uniqueCount=field_stats.distinct_count,
            nullCount=field_stats.null_count,
            min=str(field_stats.min_value)
            if field_stats.min_value is not None
            else None,
            max=str(field_stats.max_value)
            if field_stats.max_value is not None
            else None,
            mean=str(field_stats.mean) if field_stats.mean is not None else None,
            median=str(field_stats.median) if field_stats.median is not None else None,
            stdev=str(field_stats.stdev) if field_stats.stdev is not None else None,
            quantiles=[
                QuantileClass(quantile=str(0.25), value=str(field_stats.quantiles[0])),
                QuantileClass(quantile=str(0.75), value=str(field_stats.quantiles[1])),
            ]
            if field_stats.quantiles and len(field_stats.quantiles) >= 2
            else None,
            sampleValues=field_stats.sample_values,
        )

    def profile_table(
        self, table_name: str, columns: List[Tuple[str, str]]
    ) -> TableProfileData:
        chunked_columns = self._chunk_columns(columns)
        profile_results = []

        for chunk in chunked_columns:
            try:
                chunk_result = self._profile_chunk(table_name, chunk)
                profile_results.append(chunk_result)
            except DremioAPIException as e:
                self.report.profiling_skipped_other[table_name] += 1
                profile_results.append(self._create_empty_profile_result(chunk))
                self.report.warning(
                    message="Error in profiling for table",
                    context=f"{table_name}",
                    exc=e,
                )
        return self._combine_profile_results(profile_results)

    def _profile_chunk(
        self, table_name: str, columns: List[Tuple[str, str]]
    ) -> ProfileChunkResult:
        profile_sql = self._build_profile_sql(table_name, columns)
        try:
            results = self.api_operations.execute_query(profile_sql)
            if results and len(results) > 0:
                return ProfileChunkResult.from_query_result(
                    results[0], columns, self.config
                )
            else:
                return self._create_empty_profile_result(columns)
        except DremioAPIException as e:
            raise e

    def _chunk_columns(
        self, columns: List[Tuple[str, str]]
    ) -> List[List[Tuple[str, str]]]:
        return [
            columns[i : i + self.MAX_COLUMNS_PER_QUERY]
            for i in range(0, len(columns), self.MAX_COLUMNS_PER_QUERY)
        ]

    def _build_profile_sql(
        self, table_name: str, columns: List[Tuple[str, str]]
    ) -> str:
        metrics = []

        metrics.append("COUNT(*) AS row_count")
        metrics.append(f"{len(columns)} AS column_count")

        if not self.config.profiling.profile_table_level_only:
            for column_name, data_type in columns:
                try:
                    metrics.extend(self._get_column_metrics(column_name, data_type))
                except Exception as e:
                    self.report.warning(
                        message="Error building metrics for column",
                        context=f"Table: {table_name}, Column: {column_name}",
                        exc=e,
                    )
                    # Skip this column and continue with others

        if not metrics:
            raise ValueError("No valid metrics could be generated")

        main_query = f"SELECT {', '.join(metrics)} FROM {table_name}"

        if self.config.profiling.limit:
            main_query += f" LIMIT {self.config.profiling.limit}"
        if self.config.profiling.offset:
            main_query += f" OFFSET {self.config.profiling.offset}"

        return main_query

    def _get_column_metrics(self, column_name: str, data_type: str) -> List[str]:
        metrics = []

        # Wrap column name in quotes to handle special characters
        quoted_column_name = f'"{column_name}"'
        safe_column_name = re.sub(r"\W|^(?=\d)", "_", column_name)

        if self.config.profiling.include_field_distinct_count:
            metrics.append(
                f"COUNT(DISTINCT {quoted_column_name}) AS {safe_column_name}_distinct_count"
            )

        if self.config.profiling.include_field_null_count:
            metrics.append(
                f"SUM(CASE WHEN {quoted_column_name} IS NULL THEN 1 ELSE 0 END) AS {safe_column_name}_null_count"
            )

        if self.config.profiling.include_field_min_value:
            metrics.append(f"MIN({quoted_column_name}) AS {safe_column_name}_min")

        if self.config.profiling.include_field_max_value:
            metrics.append(f"MAX({quoted_column_name}) AS {safe_column_name}_max")

        if data_type.lower() in [
            "int",
            "integer",
            "bigint",
            "float",
            "double",
            "decimal",
        ]:
            if self.config.profiling.include_field_mean_value:
                metrics.append(
                    f"AVG(CAST({quoted_column_name} AS DOUBLE)) AS {safe_column_name}_mean"
                )

            if self.config.profiling.include_field_stddev_value:
                metrics.append(
                    f"STDDEV(CAST({quoted_column_name} AS DOUBLE)) AS {safe_column_name}_stdev"
                )

            if self.config.profiling.include_field_median_value:
                metrics.append(
                    f"MEDIAN({quoted_column_name}) AS {safe_column_name}_median"
                )

            if self.config.profiling.include_field_quantiles:
                metrics.append(
                    f"PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY {quoted_column_name}) AS {safe_column_name}_25th_percentile"
                )
                metrics.append(
                    f"PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY {quoted_column_name}) AS {safe_column_name}_75th_percentile"
                )

        return metrics

    def _create_empty_profile_result(
        self, columns: List[Tuple[str, str]]
    ) -> ProfileChunkResult:
        result = ProfileChunkResult()
        for column_name, _ in columns:
            result.column_stats[column_name] = ColumnProfileStats()
        return result

    def _combine_profile_results(
        self, profile_results: List[ProfileChunkResult]
    ) -> TableProfileData:
        combined_profile = TableProfileData()

        # Sum up row counts and column counts
        combined_profile.row_count = sum(
            profile.row_count for profile in profile_results
        )
        combined_profile.column_count = sum(
            profile.column_count for profile in profile_results
        )

        # Combine column statistics from all chunks
        for profile in profile_results:
            combined_profile.column_stats.update(profile.column_stats)

        return combined_profile
