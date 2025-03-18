import logging
import time
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Union

import h5py
import numpy as np

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.hdf5.config import HDF5SourceConfig
from datahub.ingestion.source.hdf5.report import HDF5SourceReport
from datahub.ingestion.source.hdf5.util import decode_type, numpy_value_to_string
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


class HDF5Profiler:
    config: HDF5SourceConfig
    report: HDF5SourceReport
    dataset: h5py.Dataset
    dataset_urn: str
    path: str

    def __init__(
        self,
        config: HDF5SourceConfig,
        report: HDF5SourceReport,
        dataset: h5py.Dataset,
        dataset_urn: str,
        path: str,
    ) -> None:
        self.config = config
        self.report = report
        self.dataset = dataset
        self.dataset_urn = dataset_urn
        self.path = path

        if self.config.profiling.use_sampling:
            self.sample_size = self.config.profiling.sample_size
        else:
            self.sample_size = 0

        self.field_sample_count = self.config.profiling.field_sample_values_limit

        if self.config.profiling.max_number_of_fields_to_profile:
            self.sample_fields = self.config.profiling.max_number_of_fields_to_profile
        else:
            self.sample_fields = 0

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        logger.info(f"Profiling dataset {self.dataset.name}")

        try:
            yield from self.generate_profile()
        except Exception as exc:
            self.report.profiling_skipped_other[self.dataset.name] += 1
            self.report.failure(
                message="Failed to profile dataset",
                context=f"{self.dataset.name}",
                exc=exc,
            )

    def generate_profile(self) -> Iterable[MetadataWorkUnit]:
        if (
            not self.config.profile_pattern.allowed(self.path)
            and self.config.profiling.report_dropped_profiles
        ):
            self.report.profiling_skipped_table_profile_pattern[self.dataset.name] += 1
            logger.info(f"Profiling not allowed for dataset {self.dataset.name}")
            return

        try:
            profile_data = self.profile_dataset()
        except Exception as exc:
            self.report.warning(
                message="Profiling Failed",
                context=f"{self.dataset.name}",
                exc=exc,
            )
            return

        profile_aspect = self.populate_profile_aspect(profile_data)

        if profile_aspect:
            self.report.report_entity_profiled()
            mcp = MetadataChangeProposalWrapper(
                entityUrn=self.dataset_urn, aspect=profile_aspect
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

    def profile_dataset(self) -> ProfileData:
        profile_data = ProfileData()

        if not self.config.profiling.profile_table_level_only:
            return self.collect_column_data(profile_data)
        else:
            return self.collect_dataset_data(profile_data)

    def collect_dataset_data(self, profile_data: ProfileData) -> ProfileData:
        profile_data.row_count = self.dataset.shape[0]
        if self.dataset.dtype.kind == "V":
            field_names = self.dataset.dtype.names
            field_count = len(field_names)
            profile_data.column_count = field_count
        elif len(self.dataset.shape) > 1:
            profile_data.column_count = self.dataset.shape[1]
        else:
            profile_data.column_count = 1

        return profile_data

    def collect_column_data(self, profile_data: ProfileData) -> ProfileData:
        dropped_fields = set()
        dataset_name = self.dataset.name

        if len(self.dataset.shape) == 1:
            logger.info(f"Attempting to profile 1-dimensional dataset {dataset_name}")
            if self.dataset.dtype.names is not None:
                for n, (f_name, f_type) in enumerate(self.dataset.dtype.descr):
                    if 0 < self.sample_fields <= n:
                        dropped_fields.add(f_name)
                        continue
                    profile_data.column_metrics[f_name] = ColumnMetric()
                    profile_data.column_metrics[f_name].values.extend(
                        self.dataset[f_name].tolist()
                    )
                    profile_data.column_metrics[f_name].col_type = decode_type(f_type)
            else:
                f_name = "col0"
                profile_data.column_metrics[f_name] = ColumnMetric()
                profile_data.column_metrics[f_name].values.extend(
                    self.dataset[:].tolist()
                )
                profile_data.column_metrics[f_name].col_type = decode_type(
                    self.dataset.dtype
                )
        else:
            logger.info(
                f"Attempting to profile multidimensional dataset {dataset_name} type {self.dataset.dtype}"
            )
            rows = self.dataset.shape[0]
            columns = self.dataset.shape[1]
            for n in range(rows):
                f_name = f"row_{n + 1}_with_{columns}_values"
                if 0 < self.sample_fields <= n:
                    dropped_fields.add(f_name)
                    continue
                row_values = self.dataset[n, :].tolist()
                profile_data.column_metrics[f_name] = ColumnMetric()
                profile_data.column_metrics[f_name].values.extend(row_values)
                profile_data.column_metrics[f_name].col_type = decode_type(
                    self.dataset.dtype
                )

        if len(dropped_fields) > 0:
            if self.config.profiling.report_dropped_profiles:
                self.report.report_dropped(
                    f"The max_number_of_fields_to_profile={self.sample_fields} reached. Dropped fields for {dataset_name} ({', '.join(sorted(dropped_fields))})"
                )

        profile_data.row_count = self.dataset.shape[0]
        profile_data.column_count = len(profile_data.column_metrics)

        return self.add_field_statistics(profile_data)

    def add_field_statistics(self, profile_data: ProfileData) -> ProfileData:
        for field_name, column_metrics in profile_data.column_metrics.items():
            if column_metrics.values:
                try:
                    self.compute_field_statistics(column_metrics)
                except Exception as exc:
                    self.report.warning(
                        message="Profiling Failed For Column Stats",
                        context=field_name,
                        exc=exc,
                    )
                    raise exc

        return profile_data

    def compute_field_statistics(self, column_metrics: ColumnMetric) -> None:
        values = column_metrics.values
        if not values:
            return

        logger.debug(
            f"Computing statistics for column of type {column_metrics.col_type}"
        )

        column_metrics.total_count = len(values)

        # ByDefault Null count is added
        if not self.config.profiling.include_field_null_count:
            column_metrics.null_count = 0

        if self.config.profiling.include_field_distinct_count:
            column_metrics.distinct_count = len(set(values))

        if values and self._is_numeric_type(column_metrics.col_type):
            if self.config.profiling.include_field_min_value:
                column_metrics.min = min(values)
            if self.config.profiling.include_field_max_value:
                column_metrics.max = max(values)
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
            column_metrics.sample_values = [
                numpy_value_to_string(v) for v in values[: self.field_sample_count]
            ]

    @staticmethod
    def _is_numeric_type(data_type: Union[str, None]) -> bool:
        if not data_type:
            return False
        else:
            return data_type.lower() in [
                "int8",
                "int16",
                "int32",
                "int64",
                "uint8",
                "uint16",
                "uint32",
                "uint64",
                "intp",
                "uintp",
                "float16",
                "float32",
                "float64",
                "float128",
            ]
