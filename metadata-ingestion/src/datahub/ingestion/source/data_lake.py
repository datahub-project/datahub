import dataclasses
import os
from dataclasses import field as dataclass_field
from typing import Any, Dict, List, Optional
from enum import Enum
from datahub.ingestion.api.common import PipelineContext

from pyspark.sql import SparkSession, Row
import pydeequ

import pydantic
from pydeequ.analyzers import (
    AnalysisRunner,
    ApproxCountDistinct,
    ApproxQuantiles,
    Completeness,
    CountDistinct,
    Distinctness,
    Histogram,
    Maximum,
    Mean,
    Minimum,
    StandardDeviation,
    UniqueValueRatio,
    Size,
)

from datahub.configuration.common import AllowDenyPattern, ConfigModel
from datahub.ingestion.api.source import Source, SourceReport


class DataLakeSourceConfig(ConfigModel):

    file: str
    file_type: Optional[str] = None

    limit: Optional[int] = None
    offset: Optional[int] = None

    # These settings will override the ones below.
    turn_off_expensive_profiling_metrics: bool = False
    profile_table_level_only: bool = False

    include_field_null_count: bool = True
    include_field_min_value: bool = True
    include_field_max_value: bool = True
    include_field_mean_value: bool = True
    include_field_median_value: bool = True
    include_field_stddev_value: bool = True
    include_field_quantiles: bool = True
    include_field_distinct_value_frequencies: bool = True
    include_field_histogram: bool = True
    include_field_sample_values: bool = True

    allow_deny_patterns: AllowDenyPattern = AllowDenyPattern.allow_all()
    max_number_of_fields_to_profile: Optional[pydantic.PositiveInt] = None

    # The default of (5 * cpu_count) is adopted from the default max_workers
    # parameter of ThreadPoolExecutor. Given that profiling is often an I/O-bound
    # task, it may make sense to increase this default value in the future.
    # https://docs.python.org/3/library/concurrent.futures.html#concurrent.futures.ThreadPoolExecutor
    max_workers: int = 5 * (os.cpu_count() or 4)


@dataclasses.dataclass
class DataLakeSourceReport(SourceReport):
    files_scanned = 0
    filtered: List[str] = dataclass_field(default_factory=list)

    def report_file_scanned(self) -> None:
        self.file_scanned += 1

    def report_file_dropped(self, file: str) -> None:
        self.filtered.append(file)


class FileType(Enum):
    PARQUET = "parquet"
    CSV = "csv"

class DataLakeSource(Source):
    source_config: DataLakeSourceConfig
    report = DataLakeSourceReport()

    def __init__(self, config: DataLakeSourceConfig, ctx: PipelineContext):
        super().__init__(config, ctx)
        self.source_config = config

        self.spark = (SparkSession
            .builder
            .config("spark.jars.packages", pydeequ.deequ_maven_coord)
            .config("spark.jars.excludes", pydeequ.f2j_maven_coord)
            .getOrCreate())

    def read_file(self, file: str, file_type:FileType):
        
        if file_type == FileType.PARQUET:
            df = self.spark.read.parquet(file)
        elif file_type == FileType.CSV:
            df = self.spark.read.csv(file)

        return df

    def generate_profiles(self):
        
        df = self.read_file(self.source_config.file, self.source_config.file_type)

        columns = df.columns
            

        analyzer = AnalysisRunner(self.spark) \
            .onData(df) \
            .addAnalyzer(Size())

        for column in columns:
            analyzer.addAnalyzer(Completeness(column))

        analyzer_result = analyzer.run()