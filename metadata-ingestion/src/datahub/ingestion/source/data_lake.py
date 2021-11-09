import dataclasses
import os
from dataclasses import field as dataclass_field
from enum import Enum
from typing import Any, Dict, Iterable, List, Optional

import pydantic
import pydeequ
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
    Size,
    StandardDeviation,
    UniqueValueRatio,
)
from pyspark.sql import Row, SparkSession

from datahub.configuration.common import AllowDenyPattern, ConfigModel
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit


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
        super().__init__(ctx)
        self.source_config = config

        self.spark = (SparkSession
            .builder
            .config("spark.jars.packages", pydeequ.deequ_maven_coord)
            .config("spark.jars.excludes", pydeequ.f2j_maven_coord)
            .getOrCreate())
        
    @classmethod
    def create(cls, config_dict, ctx):
        config = DataLakeSourceConfig.parse_obj(config_dict)
        return cls(config, ctx)

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

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        return []

    def get_report(self):
        return self.report

    def close(self):
        pass
