import logging
import time
from typing import Any, Dict, Iterable, List, Tuple

import numpy as np
from cassandra.util import OrderedMapSerializedKey, SortedSet

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.cassandra.cassandra_api import CassandraAPIInterface
from datahub.ingestion.source.cassandra.cassandra_config import CassandraSourceConfig
from datahub.ingestion.source.cassandra.cassandra_utils import CassandraQueries
from datahub.ingestion.source.sql.sql_generic_profiler import ProfilingSqlReport
from datahub.metadata.schema_classes import (
    DatasetFieldProfileClass,
    DatasetProfileClass,
    QuantileClass,
)

logger = logging.getLogger(__name__)


class CassandraProfiler:
    config: CassandraSourceConfig
    report: ProfilingSqlReport

    def __init__(
        self,
        config: CassandraSourceConfig,
        report: ProfilingSqlReport,
        api: CassandraAPIInterface,
    ) -> None:
        self.api = api
        self.config = config
        self.report = report

    def get_workunits(
        self, dataset_urn: str, keyspace_name: str, table_name: str
    ) -> Iterable[MetadataWorkUnit]:
        columns = self.api.get_columns(keyspace_name, table_name)

        if not columns:
            self.report.warning(
                message="Skipping profiling as no columns found for table",
                context=f"{keyspace_name}.{table_name}",
            )
            self.report.profiling_skipped_other[table_name] += 1
            return

        columns = [(col.column_name, col.type) for col in columns]

        if not self.config.profile_pattern.allowed(f"{keyspace_name}.{table_name}"):
            self.report.profiling_skipped_table_profile_pattern[table_name] += 1
            self.report.warning(
                message="Profiling is restricted due to the specified profile pattern.",
                context=f"{keyspace_name}.{table_name}",
            )
            return

        profile_data = self.profile_table(keyspace_name, table_name, columns)
        profile_aspect = self.populate_profile_aspect(profile_data)

        if profile_aspect:
            self.report.report_entity_profiled(table_name)
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

    def profile_table(
        self, keyspace_name: str, table_name: str, columns: List[Tuple[str, str]]
    ) -> Dict:

        results: Dict[str, Any] = {}

        limit = None
        if self.config.profiling.limit:
            limit = self.config.profiling.limit

        if self.config.profiling.row_count:
            resp = self.api.execute(
                CassandraQueries.ROW_COUNT.format(keyspace_name, table_name), limit
            )
            if resp:
                results["row_count"] = resp[0].row_count

        if self.config.profiling.column_count:
            resp = self.api.execute(
                CassandraQueries.COLUMN_COUNT.format(keyspace_name, table_name), limit
            )
            if resp:
                results["column_count"] = resp[0].column_count

        if not self.config.profiling.profile_table_level_only:
            resp = self.api.execute(
                f'SELECT {", ".join([col[0] for col in columns])} FROM {keyspace_name}."{table_name}"',
                limit,
            )
            results["column_metrics"] = resp

        return self._parse_profile_results(results, columns)

    def _parse_profile_results(
        self, results: Dict[str, Any], columns: List[Tuple[str, str]]
    ) -> Dict:
        profile: Dict[str, Any] = {"column_stats": {}}

        # Step 1: Parse overall profile metrics
        self._parse_overall_metrics(results, profile)

        # Step 2: Process and parse each column
        if results.get("column_metrics"):
            metrics = self._initialize_metrics(columns)
            self._collect_column_data(results, metrics, columns)
            self._calculate_statistics(metrics, columns, profile)

        return profile

    def _parse_overall_metrics(
        self, results: Dict[str, Any], profile: Dict[str, Any]
    ) -> None:
        if self.config.profiling.row_count:
            profile["row_count"] = int(results.get("row_count", 0))

        if self.config.profiling.column_count:
            profile["column_count"] = int(results.get("column_count", 0))

    def _initialize_metrics(
        self, columns: List[Tuple[str, str]]
    ) -> Dict[str, Dict[str, Any]]:
        return {
            column: {"values": [], "null_count": 0, "total_count": 0}
            for column, _ in columns
        }

    def _collect_column_data(
        self,
        results: Dict[str, Any],
        metrics: Dict[str, Dict[str, Any]],
        columns: List[Tuple[str, str]],
    ) -> None:
        for row in results.get("column_metrics", []):
            for cl_name, col_type in columns:
                if self._is_skippable_type(col_type):
                    continue
                value: Any = getattr(row, cl_name, None)
                metrics[cl_name]["total_count"] += 1
                if not value:
                    metrics[cl_name]["null_count"] += 1
                else:
                    metrics[cl_name]["values"].extend(self._parse_value(value))

    def _is_skippable_type(self, data_type: str) -> bool:
        return data_type.lower() in ["timeuuid", "blob", "frozen<tuple<tinyint, text>>"]

    def _parse_value(self, value: Any) -> List[Any]:
        # NOTE for astra db column need to check type
        if isinstance(value, SortedSet):
            return list(value)
        elif isinstance(value, OrderedMapSerializedKey):
            return list(dict(value).values())
        elif isinstance(value, list):
            return value
        return [value]

    def _calculate_statistics(
        self,
        metrics: Dict[str, Dict[str, Any]],
        columns: List[Tuple[str, str]],
        profile: Dict[str, Any],
    ) -> None:
        for column_name, data_type in columns:
            if column_name not in metrics:
                continue

            data = metrics[column_name]
            if not data:
                continue

            values: List[Any] = data.get("values", [])
            column_stats: Dict[str, Any] = {}

            if self.config.profiling.include_field_null_count:
                column_stats["null_count"] = data.get("null_count", 0)

            if values:
                self._compute_field_statistics(values, data_type, column_stats)

            profile["column_stats"][column_name] = column_stats

    def _compute_field_statistics(
        self, values: List[Any], data_type: str, column_stats: Dict[str, Any]
    ) -> None:
        if self.config.profiling.include_field_distinct_count:
            column_stats["distinct_count"] = len(set(values))

        if self.config.profiling.include_field_min_value:
            column_stats["min"] = min(values)

        if self.config.profiling.include_field_max_value:
            column_stats["max"] = max(values)

        if self._is_numeric_type(data_type):
            if self.config.profiling.include_field_mean_value:
                column_stats["mean"] = str(np.mean(values))
            if self.config.profiling.include_field_stddev_value:
                column_stats["stdev"] = str(np.std(values))
            if self.config.profiling.include_field_median_value:
                column_stats["median"] = str(np.median(values))
            if self.config.profiling.include_field_quantiles:
                column_stats["quantiles"] = [
                    str(np.percentile(values, 25)),
                    str(np.percentile(values, 75)),
                ]

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
