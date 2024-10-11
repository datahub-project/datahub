import logging
import re
from typing import Any, Dict, List, Tuple

from datahub.ingestion.source.dremio.dremio_api import DremioAPIOperations
from datahub.ingestion.source.dremio.dremio_config import ProfileConfig

logger = logging.getLogger(__name__)


class DremioProfiler:
    def __init__(self, api_operations: DremioAPIOperations, config: ProfileConfig):
        self.api_operations = api_operations
        self.config = config
        self.MAX_COLUMNS_PER_QUERY = 800
        self.QUERY_TIMEOUT = config.query_timeout  # 5 minutes timeout for each query

    def profile_table(self, table_name: str, columns: List[Tuple[str, str]]) -> Dict:
        chunked_columns = self._chunk_columns(columns)
        profile_results = []

        for chunk in chunked_columns:
            try:
                chunk_result = self._profile_chunk(table_name, chunk)
                profile_results.append(chunk_result)
            except Exception as e:
                logger.error(f"Error profiling chunk of {table_name}: {str(e)}")
                profile_results.append(self._create_empty_profile_result(chunk))

        return self._combine_profile_results(profile_results)

    def _profile_chunk(self, table_name: str, columns: List[Tuple[str, str]]) -> Dict:
        profile_sql = self._build_profile_sql(table_name, columns)
        try:
            results = self.api_operations.execute_query(profile_sql)
            return self._parse_profile_results(results, columns)
        except Exception as e:
            logger.error(f"Error profiling {table_name}: {str(e)}")
            raise

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

        if self.config.row_count:
            metrics.append("COUNT(*) AS row_count")

        if self.config.column_count:
            metrics.append(f"{len(columns)} AS column_count")

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

        return main_query

    def _get_column_metrics(self, column_name: str, data_type: str) -> List[str]:
        metrics = []

        # Wrap column name in quotes to handle special characters
        quoted_column_name = f'"{column_name}"'
        safe_column_name = re.sub(r"\W|^(?=\d)", "_", column_name)

        if self.config.include_field_distinct_count:
            metrics.append(
                f"COUNT(DISTINCT {quoted_column_name}) AS {safe_column_name}_distinct_count"
            )

        if self.config.include_field_null_count:
            metrics.append(
                f"SUM(CASE WHEN {quoted_column_name} IS NULL THEN 1 ELSE 0 END) AS {safe_column_name}_null_count"
            )

        if self.config.include_field_min_value:
            metrics.append(f"MIN({quoted_column_name}) AS {safe_column_name}_min")

        if self.config.include_field_max_value:
            metrics.append(f"MAX({quoted_column_name}) AS {safe_column_name}_max")

        if data_type.lower() in ["int", "bigint", "float", "double", "decimal"]:
            if self.config.include_field_mean_value:
                metrics.append(
                    f"AVG(CAST({quoted_column_name} AS DOUBLE)) AS {safe_column_name}_mean"
                )

            if self.config.include_field_stddev_value:
                metrics.append(
                    f"STDDEV(CAST({quoted_column_name} AS DOUBLE)) AS {safe_column_name}_stdev"
                )

            if self.config.include_field_median_value:
                metrics.append(
                    f"MEDIAN({quoted_column_name}) AS {safe_column_name}_median"
                )

            if self.config.include_field_quantiles:
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

        if self.config.row_count:
            profile["row_count"] = int(result.get("row_count", 0))

        if self.config.column_count:
            profile["column_count"] = int(result.get("column_count", 0))

        for column_name, data_type in columns:
            safe_column_name = re.sub(r"\W|^(?=\d)", "_", column_name)
            column_stats: Dict[str, Any] = {}
            if self.config.include_field_distinct_count:
                column_stats["distinct_count"] = int(
                    result.get(f"{safe_column_name}_distinct_count", 0)
                )
            if self.config.include_field_null_count:
                column_stats["null_count"] = int(
                    result.get(f"{safe_column_name}_null_count", 0)
                )
            if self.config.include_field_min_value:
                column_stats["min"] = result.get(f"{safe_column_name}_min")
            if self.config.include_field_max_value:
                column_stats["max"] = result.get(f"{safe_column_name}_max")

            if data_type.lower() in ["int", "bigint", "float", "double", "decimal"]:
                if self.config.include_field_mean_value:
                    column_stats["mean"] = result.get(f"{safe_column_name}_mean")
                if self.config.include_field_stddev_value:
                    column_stats["stdev"] = result.get(f"{safe_column_name}_stdev")
                if self.config.include_field_median_value:
                    column_stats["median"] = result.get(f"{safe_column_name}_median")
                if self.config.include_field_quantiles:
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
