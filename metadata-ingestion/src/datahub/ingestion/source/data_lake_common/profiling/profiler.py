import logging
import os
from typing import (
    IO,
    TYPE_CHECKING,
    Any,
    Collection,
    Iterable,
    List,
    Optional,
    Protocol,
    Union,
)

from smart_open import open as smart_open

from datahub.configuration.common import AllowDenyPattern
from datahub.emitter.mce_builder import get_sys_time
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.aws.aws_common import AwsConnectionConfig
from datahub.ingestion.source.aws.s3_boto_utils import list_objects_recursive
from datahub.ingestion.source.aws.s3_util import (
    get_bucket_name,
    get_bucket_relative_path,
    is_s3_uri,
)

# abs_utils is dependency-light (no Azure SDK), so it is safe to import here
# without pulling azure packages into the s3/gcs profiling path.
from datahub.ingestion.source.azure.abs_utils import (
    get_abs_prefix,
    get_container_name,
    get_container_relative_path,
    is_abs_uri,
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
    # Type-only import: AzureConnectionConfig pulls the Azure SDK, which is not
    # installed for s3/gcs. The instance is supplied at runtime by the abs
    # source (which does install it), so we never import the SDK here.
    from datahub.ingestion.source.azure.azure_common import AzureConnectionConfig

logger: logging.Logger = logging.getLogger(__name__)

NUM_SAMPLE_ROWS = 20


class TableDataLike(Protocol):
    """The subset of a source's table descriptor that the profiler needs.

    Declared structurally (rather than importing s3's ``TableData``) so the
    profiler stays decoupled from the file-based data-access layer and can be
    driven by any source that exposes these fields. Also avoids a circular
    import, since ``s3.source`` imports ``FileProfiler`` from this module.
    """

    @property
    def display_name(self) -> str: ...
    @property
    def full_path(self) -> str: ...
    @property
    def table_path(self) -> str: ...
    @property
    def partitions(self) -> Optional[Collection[Any]]: ...


class ProfilingReport(Protocol):
    """The report surface the profiler needs.

    Declared structurally so the profiler doesn't import a specific connector's
    report class; the s3 and abs ``DataLakeSourceReport`` variants both satisfy it.
    """

    def report_file_dropped(self, file: str) -> None: ...
    def warning(
        self,
        message: Any,
        context: Optional[str] = ...,
        title: Optional[Any] = ...,
        exc: Optional[BaseException] = ...,
        log: bool = ...,
        log_category: Optional[Any] = ...,
    ) -> None: ...


class ProfilingConfig(Protocol):
    """The profiling-config surface the profiler reads.

    Structural, for the same reason as ``ProfilingReport``: the s3 and abs
    ``DataLakeProfilerConfig`` variants are twins and both satisfy it.
    """

    max_number_of_fields_to_profile: Optional[int]
    profile_table_level_only: bool
    include_field_sample_values: bool
    include_field_null_count: bool
    include_field_min_value: bool
    include_field_max_value: bool
    include_field_mean_value: bool
    include_field_median_value: bool
    include_field_stddev_value: bool
    include_field_quantiles: bool
    include_field_histogram: bool
    include_field_distinct_value_frequencies: bool

    @property
    def _allow_deny_patterns(self) -> AllowDenyPattern: ...


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
        report: ProfilingReport,
        profiling_times_taken: List[float],
        profiling_config: ProfilingConfig,
        azure_config: Optional["AzureConnectionConfig"] = None,
    ):
        self.aws_config = aws_config
        self.verify_ssl = verify_ssl
        self.report = report
        self.profiling_times_taken = profiling_times_taken
        self.profiling_config = profiling_config
        # Set only by the abs source; s3/gcs leave it None and never touch the
        # Azure branches below.
        self.azure_config = azure_config

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
        if is_abs_uri(path):
            if self.azure_config is None:
                raise ValueError("Azure config is required to profile ABS files")
            blob_client = self.azure_config.get_blob_service_client()
            container = get_container_name(path)
            rel_path = get_container_relative_path(path)
            return smart_open(
                f"azure://{container}/{rel_path}",
                "rb",
                transport_params={"client": blob_client},
            )
        return smart_open(path, "rb")

    def _iter_table_paths(self, table_data: TableDataLike) -> Iterable[str]:
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
            bucket = get_bucket_name(table_path)
            prefix = get_bucket_relative_path(table_path)
            # Reuse the shared lister (paged, structured, GCS-cursor aware)
            # rather than hand-rolling list_objects_v2 pagination here.
            for obj in list_objects_recursive(bucket, prefix, self.aws_config):
                if obj.key.endswith(extension):
                    yield f"s3://{obj.bucket_name}/{obj.key}"
        elif is_abs_uri(table_path):
            if self.azure_config is None:
                raise ValueError("Azure config is required to profile ABS files")
            container = get_container_name(table_path)
            prefix = get_container_relative_path(table_path)
            abs_prefix = get_abs_prefix(table_path)  # https://<account>.../
            container_client = (
                self.azure_config.get_blob_service_client().get_container_client(
                    container
                )
            )
            for blob in container_client.list_blobs(name_starts_with=prefix):
                if blob.name.endswith(extension):
                    yield f"{abs_prefix}{container}/{blob.name}"
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
                QuantileClass(quantile=str(q.quantile), value=str(q.value))
                for q in column_stats.quantiles
            ]
        if config.include_field_histogram and column_stats.histogram is not None:
            field_profile.histogram = HistogramClass(
                column_stats.histogram.boundaries, column_stats.histogram.counts
            )
        if (
            config.include_field_distinct_value_frequencies
            and column_stats.distinct_value_frequencies is not None
        ):
            field_profile.distinctValueFrequencies = [
                ValueFrequencyClass(value=vf.value, frequency=vf.frequency)
                for vf in column_stats.distinct_value_frequencies
            ]

        return field_profile

    def get_table_profile(
        self, table_data: TableDataLike, dataset_urn: str
    ) -> Iterable[MetadataWorkUnit]:
        config = self.profiling_config
        extension = os.path.splitext(table_data.full_path)[1]

        telemetry.telemetry_instance.ping("data_lake_file", {"extension": extension})

        allowed_columns: Optional[List[str]] = None
        accumulator: Optional[TableAccumulator] = None

        table_read_failed = False
        with PerfTimer() as timer:
            try:
                # `_iter_table_paths` is a generator whose S3 listing runs as it
                # is consumed; materialize it here so a listing failure
                # (throttling / AccessDenied) warns and skips this table like a
                # read failure, instead of propagating out and aborting the
                # whole source run.
                paths = list(self._iter_table_paths(table_data))
            except Exception as e:
                self.report.warning(
                    title="Failed to list files during profiling",
                    message="Table could not be profiled because its files could not be listed",
                    context=table_data.full_path,
                    exc=e,
                )
                return

            for path in paths:
                try:
                    with self._open_file(path) as file_obj:
                        source = self._read_source(file_obj, extension)
                        if source is None:
                            self.report.warning(
                                title="Skipped file with unsupported extension during profiling",
                                message="File type is not supported by the profiler",
                                context=path,
                            )
                            continue

                        if isinstance(source, ColumnarSource) and source.reflowed_rows:
                            self.report.warning(
                                title="Profiled a CSV/TSV file with ragged rows",
                                message=(
                                    "Some rows did not match the header width and were "
                                    "truncated/padded so the file could be parsed; the "
                                    "profile is computed over this altered data"
                                ),
                                context=f"{source.reflowed_rows} row(s) altered in {path}",
                            )

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
                    # The accumulator may already hold partial rows from this
                    # file, so the whole table profile is now untrustworthy;
                    # record the failure and stop rather than emit polluted stats.
                    # This also catches profiling errors (e.g. a value that
                    # doesn't match a column's Avro-declared type), not just I/O
                    # failures, so the wording covers both.
                    self.report.warning(
                        title="Failed to read or profile a file",
                        message="Table profile was skipped because a file could not be read or profiled",
                        context=path,
                        exc=e,
                    )
                    table_read_failed = True
                    break

            if accumulator is None:
                self.report.warning(
                    title="Failed to read or profile a file",
                    message="Table could not be profiled because no file could be read or profiled",
                    context=table_data.full_path,
                )
                return

            if table_read_failed:
                # A file failed partway through streaming, leaving the
                # accumulator with partial rows. Emitting now would report a
                # wrong rowCount and stats built on partial data, so skip it.
                return

            try:
                table_stats = accumulator.finalize()

                profile = DatasetProfileClass(timestampMillis=get_sys_time())
                profile.rowCount = table_stats.row_count
                profile.columnCount = table_stats.column_count

                if not config.profile_table_level_only:
                    profile.fieldProfiles = [
                        self._build_field_profile(column_stats)
                        for column_stats in table_stats.columns
                    ]
            except Exception as e:
                # finalize()/profile assembly (sketch math, histogram edges, stat
                # conversions) runs outside the per-file guard above. Contain any
                # failure here so one table can't abort profiling (and ingestion)
                # for the rest.
                self.report.warning(
                    title="Failed to compute profile",
                    message="Profile could not be finalized for the table",
                    context=table_data.full_path,
                    exc=e,
                )
                return

            # Emitted after the profile is assembled and outside the guard
            # above, so a telemetry failure can never suppress a valid profile.
            telemetry.telemetry_instance.ping(
                "profile_data_lake_table",
                {"rows_profiled": stats.discretize(table_stats.row_count)},
            )

            time_taken = timer.elapsed_seconds()
            logger.info(
                f"Finished profiling {table_data.full_path}; took {time_taken:.3f} seconds"
            )
            self.profiling_times_taken.append(time_taken)

        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=profile,
        ).as_workunit()
