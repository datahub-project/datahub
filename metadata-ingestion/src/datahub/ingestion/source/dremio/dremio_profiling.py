import logging
import re
import time
from typing import Any, Dict, Iterable, List, Tuple

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

    def populate_profile_aspect(self, profile_data: Dict) -> DatasetProfileClass:
        field_profiles = [
            self._create_field_profile(field_name, field_stats)
            for field_name, field_stats in profile_data.get("column_stats", {}).items()
        ]
        return DatasetProfileClass(
            timestampMillis=round(time.time() * 1000),
            rowCount=profile_data.get("row_count"),
            columnCount=profile_data.get("column_count"),
            fieldProfiles=field_profiles,
        )

    def _create_field_profile(
        self, field_name: str, field_stats: Dict
    ) -> DatasetFieldProfileClass:
        quantiles = field_stats.get("quantiles")
        return DatasetFieldProfileClass(
            fieldPath=field_name,
            uniqueCount=field_stats.get("distinct_count"),
            nullCount=field_stats.get("null_count"),
            min=str(field_stats.get("min")) if field_stats.get("min") else None,
            max=str(field_stats.get("max")) if field_stats.get("max") else None,
            mean=str(field_stats.get("mean")) if field_stats.get("mean") else None,
            median=str(field_stats.get("median"))
            if field_stats.get("median")
            else None,
            stdev=str(field_stats.get("stdev")) if field_stats.get("stdev") else None,
            quantiles=[
                QuantileClass(quantile=str(0.25), value=str(quantiles[0])),
                QuantileClass(quantile=str(0.75), value=str(quantiles[1])),
            ]
            if quantiles
            else None,
            sampleValues=field_stats.get("sample_values"),
        )

    def profile_table(self, table_name: str, columns: List[Tuple[str, str]]) -> Dict:
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

    def _profile_chunk(self, table_name: str, columns: List[Tuple[str, str]]) -> Dict:
        profile_sql = self._build_profile_sql(table_name, columns)
        try:
            results = self.api_operations.execute_query(profile_sql)
            return self._parse_profile_results(results, columns)
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
                    logger.warning(
                        f"Error building metrics for column {column_name}: {str(e)}"
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

    def _parse_profile_results(
        self, results: List[Dict], columns: List[Tuple[str, str]]
    ) -> Dict:
        profile: Dict[str, Any] = {"column_stats": {}}
        result = results[0] if results else {}  # We expect only one row of results

        profile["row_count"] = int(result.get("row_count", 0))

        profile["column_count"] = int(result.get("column_count", 0))

        for column_name, data_type in columns:
            safe_column_name = re.sub(r"\W|^(?=\d)", "_", column_name)
            column_stats: Dict[str, Any] = {}
            if self.config.profiling.include_field_distinct_count:
                null_distinct = result.get(f"{safe_column_name}_distinct_count", 0)
                null_distinct = int(null_distinct) if null_distinct is not None else 0
                column_stats["distinct_count"] = null_distinct

            if self.config.profiling.include_field_null_count:
                null_count_value = result.get(f"{safe_column_name}_null_count", 0)
                null_count = (
                    int(null_count_value) if null_count_value is not None else 0
                )
                column_stats["null_count"] = null_count

            if self.config.profiling.include_field_min_value:
                column_stats["min"] = result.get(f"{safe_column_name}_min")
            if self.config.profiling.include_field_max_value:
                column_stats["max"] = result.get(f"{safe_column_name}_max")

            if data_type.lower() in [
                "int",
                "integer",
                "bigint",
                "float",
                "double",
                "decimal",
            ]:
                if self.config.profiling.include_field_mean_value:
                    column_stats["mean"] = result.get(f"{safe_column_name}_mean")
                if self.config.profiling.include_field_stddev_value:
                    column_stats["stdev"] = result.get(f"{safe_column_name}_stdev")
                if self.config.profiling.include_field_median_value:
                    column_stats["median"] = result.get(f"{safe_column_name}_median")
                if self.config.profiling.include_field_quantiles:
                    column_stats["quantiles"] = [
                        result.get(f"{safe_column_name}_25th_percentile"),
                        result.get(f"{safe_column_name}_75th_percentile"),
                    ]

            profile["column_stats"][column_name] = column_stats

        return profile

    def _create_empty_profile_result(self, columns: List[Tuple[str, str]]) -> Dict:
        profile: Dict[str, Any] = {"column_stats": {}}
        for column_name, _ in columns:
            profile["column_stats"][column_name] = {}
        return profile

    def _combine_profile_results(self, profile_results: List[Dict]) -> Dict:
        combined_profile = {}
        combined_profile["row_count"] = sum(
            profile.get("row_count", 0) for profile in profile_results
        )
        combined_profile["column_count"] = sum(
            profile.get("column_count", 0) for profile in profile_results
        )
        combined_profile["column_stats"] = {}

        for profile in profile_results:
            combined_profile["column_stats"].update(profile.get("column_stats", {}))

        return combined_profile
