import logging
import os
from typing import IO, TYPE_CHECKING, Any, Iterable, List, Optional, Union

from smart_open import open as smart_open

from datahub.emitter.mce_builder import get_sys_time
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.aws.aws_common import AwsConnectionConfig
from datahub.ingestion.source.aws.s3_util import (
    get_bucket_name,
    get_bucket_relative_path,
    is_s3_uri,
)
from datahub.ingestion.source.data_lake_common.profiling.accumulators import (
    ColumnStats,
    TableAccumulator,
)
from datahub.ingestion.source.data_lake_common.profiling.readers import (
    AvroSource,
    ColumnarSource,
    read_avro,
    read_csv,
    read_json,
    read_parquet,
)
from datahub.ingestion.source.s3.datalake_profiler_config import DataLakeProfilerConfig
from datahub.ingestion.source.s3.report import DataLakeSourceReport
from datahub.metadata.schema_classes import (
    DatasetFieldProfileClass,
    DatasetProfileClass,
    HistogramClass,
    QuantileClass,
    ValueFrequencyClass,
)
from datahub.telemetry import stats, telemetry
from datahub.utilities.perf_timer import PerfTimer

if TYPE_CHECKING:
    # Imported for typing only; a runtime import would be circular because
    # s3.source lazily imports FileProfiler from this module.
    from datahub.ingestion.source.s3.source import TableData

logger: logging.Logger = logging.getLogger(__name__)

NUM_SAMPLE_ROWS = 20


def null_str(value: Any) -> Optional[str]:
    return str(value) if value is not None else None


class FileProfiler:
    """
    Profiles S3/GCS/local files (parquet/csv/tsv/avro/json) via streaming Arrow/fastavro
    readers and Apache DataSketches.

    Cardinality (see `accumulators.Cardinality`) decides which fields a
    column gets, mirroring the old Spark/Deequ profiler: low-cardinality
    columns of any type get an exact distinctValueFrequencies table;
    high-cardinality numeric/temporal columns get min/max/mean/median/stdev
    plus approximate quantiles and a continuous histogram.
    """

    def __init__(
        self,
        aws_config: Optional[AwsConnectionConfig],
        verify_ssl: Optional[Union[bool, str]],
        report: DataLakeSourceReport,
        profiling_times_taken: List[float],
        profiling_config: DataLakeProfilerConfig,
    ):
        self.aws_config = aws_config
        self.verify_ssl = verify_ssl
        self.report = report
        self.profiling_times_taken = profiling_times_taken
        self.profiling_config = profiling_config

    def _open_file(self, path: str) -> IO[bytes]:
        # S3 and GCS both go through this branch: GCSSource rewrites gs:// paths
        # to s3:// and supplies an AwsConnectionConfig pointed at GCS's
        # S3-interoperability endpoint, so GCS is handled as S3 here. Local files
        # are not S3 URIs, so they use plain smart_open and need no aws_config
        # (which is why it is Optional).
        if is_s3_uri(path):
            if self.aws_config is None:
                raise ValueError("AWS config is required to profile S3/GCS files")
            s3_client = self.aws_config.get_s3_client(self.verify_ssl)
            normalized = (
                f"s3://{get_bucket_name(path)}/{get_bucket_relative_path(path)}"
            )
            return smart_open(normalized, "rb", transport_params={"client": s3_client})
        return smart_open(path, "rb")

    def _iter_table_paths(self, table_data: "TableData") -> Iterable[str]:
        """Enumerate every file under a (possibly partitioned) table path.

        `table_data.table_path` is a directory when the table spans multiple
        partition files; Spark used to glob it internally, so we do the
        equivalent listing ourselves.
        """
        if not table_data.partitions:
            yield table_data.full_path
            return

        extension = os.path.splitext(table_data.full_path)[1]
        table_path = table_data.table_path

        if is_s3_uri(table_path):
            if self.aws_config is None:
                raise ValueError("AWS config is required to profile S3/GCS files")
            s3_client = self.aws_config.get_s3_client(self.verify_ssl)
            bucket = get_bucket_name(table_path)
            prefix = get_bucket_relative_path(table_path)
            paginator = s3_client.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                for obj in page.get("Contents", []):
                    if obj["Key"].endswith(extension):
                        yield f"s3://{bucket}/{obj['Key']}"
        else:
            for root, _dirs, files in os.walk(table_path):
                for name in files:
                    if name.endswith(extension):
                        yield os.path.join(root, name)

    def _read_source(
        self, file_obj: IO[bytes], extension: str
    ) -> Optional[Union[ColumnarSource, AvroSource]]:
        if extension.endswith(".parquet"):
            return read_parquet(file_obj)
        elif extension.endswith(".csv"):
            return read_csv(file_obj, delimiter=",")
        elif extension.endswith(".tsv"):
            return read_csv(file_obj, delimiter="\t")
        elif extension.endswith(".avro"):
            return read_avro(file_obj)
        elif extension.endswith(".json") or extension.endswith(".jsonl"):
            return read_json(file_obj)
        return None

    def _build_field_profile(
        self, column_stats: ColumnStats
    ) -> DatasetFieldProfileClass:
        config = self.profiling_config
        field_profile = DatasetFieldProfileClass(fieldPath=column_stats.column)

        field_profile.uniqueCount = column_stats.unique_count
        if column_stats.unique_count is not None and column_stats.non_null_count > 0:
            field_profile.uniqueProportion = (
                column_stats.unique_count / column_stats.non_null_count
            )

        if config.include_field_null_count:
            field_profile.nullCount = column_stats.null_count
            row_count = column_stats.non_null_count + column_stats.null_count
            if row_count > 0:
                field_profile.nullProportion = column_stats.null_count / row_count

        if config.include_field_min_value:
            field_profile.min = null_str(column_stats.min_value)
        if config.include_field_max_value:
            field_profile.max = null_str(column_stats.max_value)
        if config.include_field_mean_value:
            field_profile.mean = null_str(column_stats.mean)
        if config.include_field_median_value:
            field_profile.median = null_str(column_stats.median)
        if config.include_field_stddev_value:
            field_profile.stdev = null_str(column_stats.stdev)
        if config.include_field_sample_values:
            field_profile.sampleValues = column_stats.sample_values

        if config.include_field_quantiles and column_stats.quantiles is not None:
            field_profile.quantiles = [
                QuantileClass(quantile=str(q), value=str(value))
                for q, value in column_stats.quantiles
            ]
        if config.include_field_histogram and column_stats.histogram is not None:
            boundaries, counts = column_stats.histogram
            field_profile.histogram = HistogramClass(boundaries, counts)
        if (
            config.include_field_distinct_value_frequencies
            and column_stats.distinct_value_frequencies is not None
        ):
            field_profile.distinctValueFrequencies = [
                ValueFrequencyClass(value=value, frequency=frequency)
                for value, frequency in column_stats.distinct_value_frequencies
            ]

        return field_profile

    def get_table_profile(
        self, table_data: "TableData", dataset_urn: str
    ) -> Iterable[MetadataWorkUnit]:
        config = self.profiling_config
        extension = os.path.splitext(table_data.full_path)[1]

        telemetry.telemetry_instance.ping("data_lake_file", {"extension": extension})

        allowed_columns: Optional[List[str]] = None
        accumulator: Optional[TableAccumulator] = None

        with PerfTimer() as timer:
            for path in self._iter_table_paths(table_data):
                try:
                    with self._open_file(path) as file_obj:
                        source = self._read_source(file_obj, extension)
                        if source is None:
                            self.report.report_warning(
                                table_data.display_name,
                                f"file {path} has unsupported extension",
                            )
                            continue

                        if allowed_columns is None:
                            allowed_columns = [
                                column
                                for column in source.columns
                                if config._allow_deny_patterns.allowed(column)
                            ]
                            if config.max_number_of_fields_to_profile is not None:
                                max_fields = config.max_number_of_fields_to_profile
                                if len(allowed_columns) > max_fields:
                                    dropped = allowed_columns[max_fields:]
                                    allowed_columns = allowed_columns[:max_fields]
                                    self.report.report_file_dropped(
                                        f"The max_number_of_fields_to_profile={max_fields} "
                                        f"reached. Profile of columns {table_data.full_path}"
                                        f"({', '.join(sorted(dropped))})"
                                    )
                            column_kinds = {
                                column: kind
                                for column, kind in source.column_kinds.items()
                                if column in allowed_columns
                            }
                            sample_size = (
                                NUM_SAMPLE_ROWS
                                if config.include_field_sample_values
                                else None
                            )
                            accumulator = TableAccumulator(
                                columns=allowed_columns,
                                column_kinds=column_kinds,
                                sample_size=sample_size,
                            )

                        assert accumulator is not None
                        if isinstance(source, ColumnarSource):
                            for batch in source.batches:
                                accumulator.add_batch(batch)
                        else:
                            for row in source.rows:
                                accumulator.add_row(row)
                except Exception as e:
                    logger.error(e)
                    self.report.report_warning(
                        table_data.display_name,
                        f"unable to read table {table_data.display_name} from file {path}: {e}",
                    )
                    continue

            if accumulator is None:
                self.report.report_warning(
                    table_data.display_name,
                    f"unable to read table {table_data.display_name} from file "
                    f"{table_data.full_path}",
                )
                return

            table_stats = accumulator.finalize()

            telemetry.telemetry_instance.ping(
                "profile_data_lake_table",
                {"rows_profiled": stats.discretize(table_stats.row_count)},
            )

            profile = DatasetProfileClass(timestampMillis=get_sys_time())
            profile.rowCount = table_stats.row_count
            profile.columnCount = table_stats.column_count

            if not config.profile_table_level_only:
                profile.fieldProfiles = [
                    self._build_field_profile(column_stats)
                    for column_stats in table_stats.columns
                ]

            time_taken = timer.elapsed_seconds()
            logger.info(
                f"Finished profiling {table_data.full_path}; took {time_taken:.3f} seconds"
            )
            self.profiling_times_taken.append(time_taken)

        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=profile,
        ).as_workunit()
