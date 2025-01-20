import asyncio
import logging
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Union

import numpy as np

from datahub.emitter.mce_builder import make_dataset_urn_with_platform_instance
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.couchbase.couchbase_aggregate import CouchbaseAggregate
from datahub.ingestion.source.couchbase.couchbase_common import (
    CouchbaseDBConfig,
    CouchbaseDBSourceReport,
    flatten,
)
from datahub.ingestion.source.couchbase.couchbase_connect import CouchbaseConnect
from datahub.ingestion.source.couchbase.couchbase_schema_reader import (
    CouchbaseCollectionItemsReader,
)
from datahub.metadata.schema_classes import (
    DatasetFieldProfileClass,
    DatasetProfileClass,
    QuantileClass,
)

logger = logging.getLogger(__name__)


@dataclass
class ColumnMetric:
    col_type: Union[str, None] = None
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
    row_count: Optional[int] = 0
    column_count: Optional[int] = 0
    column_metrics: Dict[str, ColumnMetric] = field(default_factory=dict)


class CouchbaseProfiler:
    config: CouchbaseDBConfig
    report: CouchbaseDBSourceReport
    client: CouchbaseConnect

    def __init__(
        self,
        config: CouchbaseDBConfig,
        report: CouchbaseDBSourceReport,
        client: CouchbaseConnect,
    ) -> None:
        self.config = config
        self.report = report
        self.client = client

        try:
            self.loop = asyncio.get_running_loop()
        except RuntimeError:
            self.loop = asyncio.new_event_loop()

    def get_workunits(self, datasets: List[str]) -> Iterable[MetadataWorkUnit]:
        logger.info(f"Profiling {len(datasets)} keyspaces")
        for keyspace in datasets:
            logger.info(f"Profiling Keyspace {keyspace}")

            try:
                yield from self.generate_profile(keyspace)
            except Exception as exc:
                self.report.profiling_skipped_other[keyspace] += 1
                self.report.failure(
                    message="Failed to profile keyspace",
                    context=f"{keyspace}",
                    exc=exc,
                )

    def generate_profile(self, keyspace: str) -> Iterable[MetadataWorkUnit]:
        dataset_urn = make_dataset_urn_with_platform_instance(
            platform="couchbase",
            name=keyspace,
            env=self.config.env,
            platform_instance=self.config.cluster_name,
        )

        if not self.config.profile_pattern.allowed(keyspace):
            self.report.profiling_skipped_table_profile_pattern[keyspace] += 1
            logger.info(f"Profiling not allowed for Keyspace {keyspace}")
            return

        try:
            profile_data = self.profile_table(keyspace)
        except Exception as exc:
            self.report.warning(
                message="Profiling Failed",
                context=f"{keyspace}",
                exc=exc,
            )
            return

        profile_aspect = self.populate_profile_aspect(profile_data)

        if profile_aspect:
            self.report.report_entity_profiled()
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

    @staticmethod
    def _create_field_profile(
        field_name: str, field_stats: ColumnMetric
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

    def profile_table(self, keyspace: str) -> ProfileData:
        profile_data = ProfileData()

        if not self.config.profiling.profile_table_level_only:
            return self.loop.run_until_complete(
                self._collect_column_data(keyspace, profile_data)
            )
        else:
            return self._collect_keyspace_data(keyspace, profile_data)

    def _collect_keyspace_data(
        self, keyspace: str, profile_data: ProfileData
    ) -> ProfileData:
        collection_count: int = self.client.collection_count(keyspace)

        value_sample_size: int = self.config.classification.sample_size
        schema_sample_size: int = (
            self.config.schema_sample_size if self.config.schema_sample_size else 10000
        )
        schema: dict = self.client.collection_infer(
            schema_sample_size, value_sample_size, keyspace
        )

        schema_reader = CouchbaseCollectionItemsReader.create(schema)
        schema_data: dict = schema_reader.get_sample_data_for_table(
            keyspace.split("."), value_sample_size
        )

        profile_data.row_count = collection_count
        profile_data.column_count = len(list(schema_data.keys()))

        return profile_data

    async def _collect_column_data(
        self, keyspace: str, profile_data: ProfileData
    ) -> ProfileData:
        document_total_count: int = 0

        aggregator = CouchbaseAggregate(self.client, keyspace)

        async for chunk in aggregator.get_documents():
            for document in chunk:
                column_values: Dict[str, list] = defaultdict(list)
                document_total_count += 1

                for _field, data in flatten([], document):
                    column_values[_field].append(data)

                for field_name, values in column_values.items():
                    if field_name not in profile_data.column_metrics:
                        profile_data.column_metrics[field_name] = ColumnMetric()
                        if not profile_data.column_count:
                            profile_data.column_count = 1
                        else:
                            profile_data.column_count += 1
                    for value in values:
                        col_type = type(value).__name__
                        if not profile_data.column_metrics[field_name].col_type:
                            profile_data.column_metrics[field_name].col_type = col_type
                        else:
                            if (
                                profile_data.column_metrics[field_name].col_type
                                != col_type
                            ):
                                profile_data.column_metrics[
                                    field_name
                                ].col_type = "mixed"
                        profile_data.column_metrics[field_name].total_count += 1
                        if value is None:
                            profile_data.column_metrics[field_name].null_count += 1
                        else:
                            profile_data.column_metrics[field_name].values.append(value)

        profile_data.row_count = document_total_count

        for field_name, column_metrics in profile_data.column_metrics.items():
            if column_metrics.values:
                try:
                    self._compute_field_statistics(column_metrics)
                except Exception as exc:
                    self.report.warning(
                        message="Profiling Failed For Column Stats",
                        context=field_name,
                        exc=exc,
                    )
                    raise exc

        return profile_data

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

    @staticmethod
    def _is_numeric_type(data_type: Union[str, None]) -> bool:
        if not data_type:
            return False
        else:
            return data_type.lower() in [
                "int",
                "float",
            ]
