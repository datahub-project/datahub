from datahub.utilities._markupsafe_compat import MARKUPSAFE_PATCHED

import collections
import concurrent.futures
import contextlib
import dataclasses
import functools
import logging
import threading
import traceback
import unittest.mock
import uuid
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
    Tuple,
    Union,
    cast,
)

import sqlalchemy as sa
from great_expectations.core.util import convert_to_json_serializable
from great_expectations.data_context import BaseDataContext
from great_expectations.data_context.types.base import (
    DataContextConfig,
    DatasourceConfig,
    InMemoryStoreBackendDefaults,
    datasourceConfigSchema,
)
from great_expectations.dataset.dataset import Dataset
from great_expectations.datasource.sqlalchemy_datasource import SqlAlchemyDatasource
from great_expectations.profile.base import ProfilerDataType
from great_expectations.profile.basic_dataset_profiler import BasicDatasetProfilerBase
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.exc import ProgrammingError
from typing_extensions import Concatenate, ParamSpec

from datahub.emitter.mce_builder import get_sys_time
from datahub.ingestion.source.ge_profiling_config import GEProfilingConfig
from datahub.ingestion.source.profiling.common import (
    Cardinality,
    convert_to_cardinality,
)
from datahub.ingestion.source.sql.sql_common import SQLSourceReport
from datahub.metadata.schema_classes import (
    DatasetFieldProfileClass,
    DatasetProfileClass,
    HistogramClass,
    PartitionSpecClass,
    QuantileClass,
    ValueFrequencyClass,
)
from datahub.telemetry import stats, telemetry
from datahub.utilities.perf_timer import PerfTimer
from datahub.utilities.sqlalchemy_query_combiner import (
    SQLAlchemyQueryCombiner,
    get_query_columns,
)

assert MARKUPSAFE_PATCHED
logger: logging.Logger = logging.getLogger(__name__)

P = ParamSpec("P")

# The reason for this wacky structure is quite fun. GE basically assumes that
# the config structures were generated directly from YML and further assumes that
# they can be `deepcopy`'d without issue. The SQLAlchemy engine and connection
# objects, however, cannot be copied. Despite the fact that the SqlAlchemyDatasource
# class accepts an `engine` argument (which can actually be an Engine or Connection
# object), we cannot use it because of the config loading system. As such, we instead
# pass a "dummy" config into the DatasourceConfig, but then dynamically add the
# engine parameter when the SqlAlchemyDatasource is actually set up, and then remove
# it from the cached config object to avoid those same copying mechanisms.
#
# We need to wrap this mechanism with a lock, since having multiple threads
# simultaneously patching the same methods will cause concurrency issues.

_datasource_connection_injection_lock = threading.Lock()


@contextlib.contextmanager
def _inject_connection_into_datasource(conn: Connection) -> Iterator[None]:
    with _datasource_connection_injection_lock:
        underlying_datasource_init = SqlAlchemyDatasource.__init__

        def sqlalchemy_datasource_init(
            self: SqlAlchemyDatasource, *args: Any, **kwargs: Any
        ) -> None:
            underlying_datasource_init(self, *args, **kwargs, engine=conn)
            self.drivername = conn.dialect.name
            del self._datasource_config["engine"]

        with unittest.mock.patch(
            "great_expectations.datasource.sqlalchemy_datasource.SqlAlchemyDatasource.__init__",
            sqlalchemy_datasource_init,
        ):
            yield


@dataclasses.dataclass
class GEProfilerRequest:
    pretty_name: str
    batch_kwargs: dict


def get_column_unique_count_patch(self, column):
    if self.engine.dialect.name.lower() == "redshift":
        element_values = self.engine.execute(
            sa.select(
                [sa.text(f'APPROXIMATE count(distinct "{column}")')]  # type:ignore
            ).select_from(self._table)
        )
        return convert_to_json_serializable(element_values.fetchone()[0])
    elif self.engine.dialect.name.lower() in {"bigquery", "snowflake"}:
        element_values = self.engine.execute(
            sa.select(
                [sa.text(f"APPROX_COUNT_DISTINCT ({sa.column(column)})")]  # type:ignore
            ).select_from(self._table)
        )
        return convert_to_json_serializable(element_values.fetchone()[0])
    return convert_to_json_serializable(
        self.engine.execute(
            sa.select([sa.func.count(sa.func.distinct(sa.column(column)))]).select_from(
                self._table
            )
        ).scalar()
    )


def _get_column_quantiles_bigquery_patch(  # type:ignore
    self, column: str, quantiles: Iterable
) -> list:
    quantile_queries = list()
    for quantile in quantiles:
        quantile_queries.append(
            sa.text(f"approx_quantiles({column}, 100) OFFSET [{round(quantile * 100)}]")
        )

    quantiles_query = sa.select(quantile_queries).select_from(  # type:ignore
        self._table
    )
    try:
        quantiles_results = self.engine.execute(quantiles_query).fetchone()
        return list(quantiles_results)

    except ProgrammingError as pe:
        # This treat quantile exception will raise a formatted exception and there won't be any return value here
        self._treat_quantiles_exception(pe)
        return list()


def _is_single_row_query_method(query: Any) -> bool:
    SINGLE_ROW_QUERY_FILES = {
        # "great_expectations/dataset/dataset.py",
        "great_expectations/dataset/sqlalchemy_dataset.py",
    }
    SINGLE_ROW_QUERY_METHODS = {
        "get_row_count",
        "get_column_min",
        "get_column_max",
        "get_column_mean",
        "get_column_stdev",
        "get_column_stdev",
        "get_column_nonnull_count",
        "get_column_unique_count",
    }
    CONSTANT_ROW_QUERY_METHODS = {
        # This actually returns two rows instead of a single row.
        "get_column_median",
    }
    UNPREDICTABLE_ROW_QUERY_METHODS = {
        "get_column_value_counts",
    }
    UNHANDLEABLE_ROW_QUERY_METHODS = {
        "expect_column_kl_divergence_to_be_less_than",
        "get_column_quantiles",  # this is here for now since SQLAlchemy anonymous columns need investigation
        "get_column_hist",  # this requires additional investigation
    }
    COLUMN_MAP_QUERY_METHOD = "inner_wrapper"
    COLUMN_MAP_QUERY_SINGLE_ROW_COLUMNS = [
        "element_count",
        "null_count",
        "unexpected_count",
    ]

    # We'll do this the inefficient way since the arrays are pretty small.
    stack = traceback.extract_stack()
    for frame in reversed(stack):
        if not any(frame.filename.endswith(file) for file in SINGLE_ROW_QUERY_FILES):
            continue

        if frame.name in UNPREDICTABLE_ROW_QUERY_METHODS:
            return False
        if frame.name in UNHANDLEABLE_ROW_QUERY_METHODS:
            return False
        if frame.name in SINGLE_ROW_QUERY_METHODS:
            return True
        if frame.name in CONSTANT_ROW_QUERY_METHODS:
            # TODO: figure out how to handle these.
            # A cross join will return (`constant` ** `queries`) rows rather
            # than `constant` rows with `queries` columns.
            # See https://stackoverflow.com/questions/35638753/create-query-to-join-2-tables-1-on-1-with-nothing-in-common.
            return False

        if frame.name == COLUMN_MAP_QUERY_METHOD:
            # Some column map expectations are single-row.
            # We can disambiguate by checking the column names.
            query_columns = get_query_columns(query)
            column_names = [column.name for column in query_columns]

            if column_names == COLUMN_MAP_QUERY_SINGLE_ROW_COLUMNS:
                return True

    return False


def _run_with_query_combiner(
    method: Callable[Concatenate["_SingleDatasetProfiler", P], None]
) -> Callable[Concatenate["_SingleDatasetProfiler", P], None]:
    @functools.wraps(method)
    def inner(
        self: "_SingleDatasetProfiler", *args: P.args, **kwargs: P.kwargs
    ) -> None:
        return self.query_combiner.run(lambda: method(self, *args, **kwargs))

    return inner


@dataclasses.dataclass
class _SingleColumnSpec:
    column: str
    column_profile: DatasetFieldProfileClass

    type_: ProfilerDataType = ProfilerDataType.UNKNOWN

    unique_count: Optional[int] = None
    nonnull_count: Optional[int] = None
    cardinality: Optional[Cardinality] = None


@dataclasses.dataclass
class _SingleDatasetProfiler(BasicDatasetProfilerBase):
    dataset: Dataset
    dataset_name: str
    partition: Optional[str]
    config: GEProfilingConfig
    report: SQLSourceReport

    query_combiner: SQLAlchemyQueryCombiner

    def _get_columns_to_profile(self) -> List[str]:
        if not self.config.any_field_level_metrics_enabled():
            return []

        # Compute columns to profile
        columns_to_profile: List[str] = []
        # Compute ignored columns
        ignored_columns: List[str] = []
        for col in self.dataset.get_table_columns():
            # We expect the allow/deny patterns to specify '<table_pattern>.<column_pattern>'
            if not self.config._allow_deny_patterns.allowed(
                f"{self.dataset_name}.{col}"
            ):
                ignored_columns.append(col)
            else:
                columns_to_profile.append(col)
        if ignored_columns:
            self.report.report_dropped(
                f"The profile of columns by pattern {self.dataset_name}({', '.join(sorted(ignored_columns))})"
            )

        if self.config.max_number_of_fields_to_profile is not None:
            if len(columns_to_profile) > self.config.max_number_of_fields_to_profile:
                columns_being_dropped = columns_to_profile[
                    self.config.max_number_of_fields_to_profile :
                ]
                columns_to_profile = columns_to_profile[
                    : self.config.max_number_of_fields_to_profile
                ]
                if self.config.report_dropped_profiles:
                    self.report.report_dropped(
                        f"The max_number_of_fields_to_profile={self.config.max_number_of_fields_to_profile} reached. Profile of columns {self.dataset_name}({', '.join(sorted(columns_being_dropped))})"
                    )
        return columns_to_profile

    @_run_with_query_combiner
    def _get_column_type(self, column_spec: _SingleColumnSpec, column: str) -> None:
        column_spec.type_ = BasicDatasetProfilerBase._get_column_type(
            self.dataset, column
        )

    @_run_with_query_combiner
    def _get_column_cardinality(
        self, column_spec: _SingleColumnSpec, column: str
    ) -> None:
        try:
            nonnull_count = self.dataset.get_column_nonnull_count(column)
            column_spec.nonnull_count = nonnull_count
        except Exception as e:
            logger.debug(
                f"Caught exception while attempting to get column cardinality for column {column}. {e}"
            )
            self.report.report_warning(
                "Profiling - Unable to get column cardinality",
                f"{self.dataset_name}.{column}",
            )
            return

        unique_count = None
        pct_unique = None
        try:
            unique_count = self.dataset.get_column_unique_count(column)
            if nonnull_count > 0:
                pct_unique = float(unique_count) / nonnull_count
        except Exception:
            logger.exception(
                f"Failed to get unique count for column {self.dataset_name}.{column}"
            )

        column_spec.unique_count = unique_count

        column_spec.cardinality = convert_to_cardinality(unique_count, pct_unique)

    @_run_with_query_combiner
    def _get_dataset_rows(self, dataset_profile: DatasetProfileClass) -> None:
        dataset_profile.rowCount = self.dataset.get_row_count()

    @_run_with_query_combiner
    def _get_dataset_column_min(
        self, column_profile: DatasetFieldProfileClass, column: str
    ) -> None:
        if self.config.include_field_min_value:
            column_profile.min = str(self.dataset.get_column_min(column))

    @_run_with_query_combiner
    def _get_dataset_column_max(
        self, column_profile: DatasetFieldProfileClass, column: str
    ) -> None:
        if self.config.include_field_max_value:
            column_profile.max = str(self.dataset.get_column_max(column))

    @_run_with_query_combiner
    def _get_dataset_column_mean(
        self, column_profile: DatasetFieldProfileClass, column: str
    ) -> None:
        if self.config.include_field_mean_value:
            column_profile.mean = str(self.dataset.get_column_mean(column))

    @_run_with_query_combiner
    def _get_dataset_column_median(
        self, column_profile: DatasetFieldProfileClass, column: str
    ) -> None:
        if not self.config.include_field_median_value:
            return
        try:
            column_profile.median = str(self.dataset.get_column_median(column))
        except Exception as e:
            logger.debug(
                f"Caught exception while attempting to get column median for column {column}. {e}"
            )
            self.report.report_warning(
                "Profiling - Unable to get column medians",
                f"{self.dataset_name}.{column}",
            )

    @_run_with_query_combiner
    def _get_dataset_column_stdev(
        self, column_profile: DatasetFieldProfileClass, column: str
    ) -> None:
        if not self.config.include_field_stddev_value:
            return
        try:
            column_profile.stdev = str(self.dataset.get_column_stdev(column))
        except Exception as e:
            logger.debug(
                f"Caught exception while attempting to get column stddev for column {column}. {e}"
            )
            self.report.report_warning(
                "Profiling - Unable to get column stddev",
                f"{self.dataset_name}.{column}",
            )

    @_run_with_query_combiner
    def _get_dataset_column_quantiles(
        self, column_profile: DatasetFieldProfileClass, column: str
    ) -> None:
        if not self.config.include_field_quantiles:
            return
        try:
            # FIXME: Eventually we'd like to switch to using the quantile method directly.
            # However, that method seems to be throwing an error in some cases whereas
            # this does not.
            # values = dataset.get_column_quantiles(column, tuple(quantiles))

            self.dataset.set_config_value("interactive_evaluation", True)
            quantiles = [0.05, 0.25, 0.5, 0.75, 0.95]

            res = self.dataset.expect_column_quantile_values_to_be_between(
                column,
                allow_relative_error=True,
                quantile_ranges={
                    "quantiles": quantiles,
                    "value_ranges": [[None, None]] * len(quantiles),
                },
            ).result
            if "observed_value" in res:
                column_profile.quantiles = [
                    QuantileClass(quantile=str(quantile), value=str(value))
                    for quantile, value in zip(
                        res["observed_value"]["quantiles"],
                        res["observed_value"]["values"],
                    )
                ]
        except Exception as e:
            logger.debug(
                f"Caught exception while attempting to get column quantiles for column {column}. {e}"
            )
            self.report.report_warning(
                "Profiling - Unable to get column quantiles",
                f"{self.dataset_name}.{column}",
            )

    @_run_with_query_combiner
    def _get_dataset_column_distinct_value_frequencies(
        self, column_profile: DatasetFieldProfileClass, column: str
    ) -> None:
        if self.config.include_field_distinct_value_frequencies:
            column_profile.distinctValueFrequencies = [
                ValueFrequencyClass(value=str(value), frequency=count)
                for value, count in self.dataset.get_column_value_counts(column).items()
            ]

    @_run_with_query_combiner
    def _get_dataset_column_histogram(
        self, column_profile: DatasetFieldProfileClass, column: str
    ) -> None:
        if not self.config.include_field_histogram:
            return
        try:
            self.dataset.set_config_value("interactive_evaluation", True)

            res = self.dataset.expect_column_kl_divergence_to_be_less_than(
                column,
                partition_object=None,
                threshold=None,
                result_format="COMPLETE",
            ).result
            if "details" in res and "observed_partition" in res["details"]:
                partition = res["details"]["observed_partition"]
                column_profile.histogram = HistogramClass(
                    [str(v) for v in partition["bins"]],
                    [
                        partition["tail_weights"][0],
                        *partition["weights"],
                        partition["tail_weights"][1],
                    ],
                )
        except Exception as e:
            logger.debug(
                f"Caught exception while attempting to get column histogram for column {column}. {e}"
            )
            self.report.report_warning(
                "Profiling - Unable to get column histogram",
                f"{self.dataset_name}.{column}",
            )

    @_run_with_query_combiner
    def _get_dataset_column_sample_values(
        self, column_profile: DatasetFieldProfileClass, column: str
    ) -> None:
        if not self.config.include_field_sample_values:
            return

        try:
            # TODO do this without GE
            self.dataset.set_config_value("interactive_evaluation", True)

            res = self.dataset.expect_column_values_to_be_in_set(
                column, [], result_format="SUMMARY"
            ).result

            column_profile.sampleValues = [
                str(v) for v in res["partial_unexpected_list"]
            ]
        except Exception as e:
            logger.debug(
                f"Caught exception while attempting to get sample values for column {column}. {e}"
            )
            self.report.report_warning(
                "Profiling - Unable to get column sample values",
                f"{self.dataset_name}.{column}",
            )

    def generate_dataset_profile(  # noqa: C901 (complexity)
        self,
    ) -> DatasetProfileClass:
        self.dataset.set_default_expectation_argument(
            "catch_exceptions", self.config.catch_exceptions
        )

        profile = DatasetProfileClass(timestampMillis=get_sys_time())
        if self.partition:
            profile.partitionSpec = PartitionSpecClass(partition=self.partition)
        profile.fieldProfiles = []
        self._get_dataset_rows(profile)

        all_columns = self.dataset.get_table_columns()
        profile.columnCount = len(all_columns)
        columns_to_profile = set(self._get_columns_to_profile())

        logger.debug(f"profiling {self.dataset_name}: flushing stage 1 queries")
        self.query_combiner.flush()

        columns_profiling_queue: List[_SingleColumnSpec] = []
        for column in all_columns:
            column_profile = DatasetFieldProfileClass(fieldPath=column)
            profile.fieldProfiles.append(column_profile)

            if column in columns_to_profile:
                column_spec = _SingleColumnSpec(column, column_profile)
                columns_profiling_queue.append(column_spec)

                self._get_column_type(column_spec, column)
                self._get_column_cardinality(column_spec, column)

        logger.debug(f"profiling {self.dataset_name}: flushing stage 2 queries")
        self.query_combiner.flush()

        assert profile.rowCount is not None
        row_count: int = profile.rowCount

        for column_spec in columns_profiling_queue:
            column = column_spec.column
            column_profile = column_spec.column_profile
            type_ = column_spec.type_
            cardinality = column_spec.cardinality

            non_null_count = column_spec.nonnull_count
            unique_count = column_spec.unique_count

            if non_null_count is not None:
                null_count = max(0, row_count - non_null_count)

                if self.config.include_field_null_count:
                    column_profile.nullCount = null_count
                    if row_count > 0:
                        # Sometimes this value is bigger than 1 because of the approx queries
                        column_profile.nullProportion = min(1, null_count / row_count)

            if unique_count is not None:
                if self.config.include_field_distinct_count:
                    column_profile.uniqueCount = unique_count
                    if non_null_count is not None and non_null_count > 0:
                        # Sometimes this value is bigger than 1 because of the approx queries
                        column_profile.uniqueProportion = min(
                            1, unique_count / non_null_count
                        )

            self._get_dataset_column_sample_values(column_profile, column)

            if (
                type_ == ProfilerDataType.INT
                or type_ == ProfilerDataType.FLOAT
                or type_ == ProfilerDataType.NUMERIC
            ):
                if cardinality == Cardinality.UNIQUE:
                    pass
                elif cardinality in [
                    Cardinality.ONE,
                    Cardinality.TWO,
                    Cardinality.VERY_FEW,
                    Cardinality.FEW,
                    Cardinality.MANY,
                    Cardinality.VERY_MANY,
                    Cardinality.UNIQUE,
                ]:
                    self._get_dataset_column_min(column_profile, column)
                    self._get_dataset_column_max(column_profile, column)
                    self._get_dataset_column_mean(column_profile, column)
                    self._get_dataset_column_median(column_profile, column)

                    if type_ == ProfilerDataType.INT:
                        self._get_dataset_column_stdev(column_profile, column)

                    self._get_dataset_column_quantiles(column_profile, column)
                    self._get_dataset_column_histogram(column_profile, column)
                    if cardinality in [
                        Cardinality.ONE,
                        Cardinality.TWO,
                        Cardinality.VERY_FEW,
                        Cardinality.FEW,
                    ]:
                        self._get_dataset_column_distinct_value_frequencies(
                            column_profile,
                            column,
                        )
                else:  # unknown cardinality - skip
                    pass

            elif type_ == ProfilerDataType.STRING:
                if cardinality in [
                    Cardinality.ONE,
                    Cardinality.TWO,
                    Cardinality.VERY_FEW,
                    Cardinality.FEW,
                ]:
                    self._get_dataset_column_distinct_value_frequencies(
                        column_profile,
                        column,
                    )

            elif type_ == ProfilerDataType.DATETIME:
                self._get_dataset_column_min(column_profile, column)
                self._get_dataset_column_max(column_profile, column)

                # FIXME: Re-add histogram once kl_divergence has been modified to support datetimes

                if cardinality in [
                    Cardinality.ONE,
                    Cardinality.TWO,
                    Cardinality.VERY_FEW,
                    Cardinality.FEW,
                ]:
                    self._get_dataset_column_distinct_value_frequencies(
                        column_profile,
                        column,
                    )

            else:
                if cardinality in [
                    Cardinality.ONE,
                    Cardinality.TWO,
                    Cardinality.VERY_FEW,
                    Cardinality.FEW,
                ]:
                    self._get_dataset_column_distinct_value_frequencies(
                        column_profile,
                        column,
                    )

        logger.debug(f"profiling {self.dataset_name}: flushing stage 3 queries")
        self.query_combiner.flush()
        return profile


@dataclasses.dataclass
class GEContext:
    data_context: BaseDataContext
    datasource_name: str


@dataclasses.dataclass
class DatahubGEProfiler:
    report: SQLSourceReport
    config: GEProfilingConfig
    times_taken: List[float]
    total_row_count: int

    base_engine: Engine
    platform: str  # passed from parent source config

    # The actual value doesn't matter, it just matters that we use it consistently throughout.
    _datasource_name_base: str = "my_sqlalchemy_datasource"

    def __init__(
        self,
        conn: Union[Engine, Connection],
        report: SQLSourceReport,
        config: GEProfilingConfig,
        platform: str,
    ):
        self.report = report
        self.config = config
        self.times_taken = []
        self.total_row_count = 0

        # TRICKY: The call to `.engine` is quite important here. Connection.connect()
        # returns a "branched" connection, which does not actually use a new underlying
        # DB-API object from the connection pool. Engine.connect() does what we want to
        # make the threading code work correctly. As such, we need to make sure we've
        # got an engine here.
        self.base_engine = conn.engine
        self.platform = platform

    @contextlib.contextmanager
    def _ge_context(self) -> Iterator[GEContext]:
        with self.base_engine.connect() as conn:
            data_context = BaseDataContext(
                project_config=DataContextConfig(
                    # The datasource will be added via add_datasource().
                    datasources={},
                    store_backend_defaults=InMemoryStoreBackendDefaults(),
                    anonymous_usage_statistics={
                        "enabled": False,
                        # "data_context_id": <not set>,
                    },
                )
            )

            datasource_name = f"{self._datasource_name_base}-{uuid.uuid4()}"
            datasource_config = DatasourceConfig(
                class_name="SqlAlchemyDatasource",
                credentials={
                    # This isn't actually used since we pass the connection directly,
                    # but GE parses it to change some of its behavior so it's useful
                    # to emulate that here.
                    "url": conn.engine.url,
                },
            )
            with _inject_connection_into_datasource(conn):
                # Using the add_datasource method ensures that the datasource is added to
                # GE-internal cache, which avoids problems when calling GE methods later on.
                assert data_context.add_datasource(
                    datasource_name,
                    initialize=True,
                    **dict(datasourceConfigSchema.dump(datasource_config)),
                )
            assert data_context.get_datasource(datasource_name)

            yield GEContext(data_context, datasource_name)

    def generate_profiles(
        self,
        requests: List[GEProfilerRequest],
        max_workers: int,
        platform: Optional[str] = None,
        profiler_args: Optional[Dict] = None,
    ) -> Iterable[Tuple[GEProfilerRequest, Optional[DatasetProfileClass]]]:
        with PerfTimer() as timer, concurrent.futures.ThreadPoolExecutor(
            max_workers=max_workers
        ) as async_executor, SQLAlchemyQueryCombiner(
            enabled=self.config.query_combiner_enabled,
            catch_exceptions=self.config.catch_exceptions,
            is_single_row_query_method=_is_single_row_query_method,
            serial_execution_fallback_enabled=True,
        ).activate() as query_combiner:
            max_workers = min(max_workers, len(requests))
            logger.info(
                f"Will profile {len(requests)} table(s) with {max_workers} worker(s) - this may take a while"
            )
            with unittest.mock.patch(
                "great_expectations.dataset.sqlalchemy_dataset.SqlAlchemyDataset.get_column_unique_count",
                get_column_unique_count_patch,
            ):
                with unittest.mock.patch(
                    "great_expectations.dataset.sqlalchemy_dataset.SqlAlchemyDataset._get_column_quantiles_bigquery",
                    _get_column_quantiles_bigquery_patch,
                ):
                    async_profiles = collections.deque(
                        async_executor.submit(
                            self._generate_profile_from_request,
                            query_combiner,
                            request,
                            platform=platform,
                            profiler_args=profiler_args,
                        )
                        for request in requests
                    )

                    # Avoid using as_completed so that the results are yielded in the
                    # same order as the requests.
                    # for async_profile in concurrent.futures.as_completed(async_profiles):
                    while len(async_profiles) > 0:
                        async_profile = async_profiles.popleft()
                        yield async_profile.result()

                    total_time_taken = timer.elapsed_seconds()

                    logger.info(
                        f"Profiling {len(requests)} table(s) finished in {total_time_taken:.3f} seconds"
                    )

                    time_percentiles: Dict[str, float] = {}

                    if len(self.times_taken) > 0:
                        percentiles = [50, 75, 95, 99]
                        percentile_values = stats.calculate_percentiles(
                            self.times_taken, percentiles
                        )

                        time_percentiles = {
                            f"table_time_taken_p{percentile}": stats.discretize(
                                percentile_values[percentile]
                            )
                            for percentile in percentiles
                        }

                    telemetry.telemetry_instance.ping(
                        "sql_profiling_summary",
                        # bucket by taking floor of log of time taken
                        {
                            "total_time_taken": stats.discretize(total_time_taken),
                            "count": stats.discretize(len(self.times_taken)),
                            "total_row_count": stats.discretize(self.total_row_count),
                            "platform": self.platform,
                            **time_percentiles,
                        },
                    )

                    self.report.report_from_query_combiner(query_combiner.report)

    def _generate_profile_from_request(
        self,
        query_combiner: SQLAlchemyQueryCombiner,
        request: GEProfilerRequest,
        platform: Optional[str] = None,
        profiler_args: Optional[Dict] = None,
    ) -> Tuple[GEProfilerRequest, Optional[DatasetProfileClass]]:
        return request, self._generate_single_profile(
            query_combiner=query_combiner,
            pretty_name=request.pretty_name,
            platform=platform,
            profiler_args=profiler_args,
            **request.batch_kwargs,
        )

    def _drop_trino_temp_table(self, temp_dataset: Dataset) -> None:
        schema = temp_dataset._table.schema
        table = temp_dataset._table.name
        try:
            with self.base_engine.connect() as connection:
                connection.execute(f"drop view if exists {schema}.{table}")
                logger.debug(f"View {schema}.{table} was dropped.")
        except Exception:
            logger.warning(f"Unable to delete trino temporary table: {schema}.{table}")

    def _generate_single_profile(
        self,
        query_combiner: SQLAlchemyQueryCombiner,
        pretty_name: str,
        schema: str = None,
        table: str = None,
        partition: Optional[str] = None,
        custom_sql: Optional[str] = None,
        platform: Optional[str] = None,
        profiler_args: Optional[Dict] = None,
        **kwargs: Any,
    ) -> Optional[DatasetProfileClass]:
        logger.debug(
            f"Received single profile request for {pretty_name} for {schema}, {table}, {custom_sql}"
        )

        ge_config = {
            "schema": schema,
            "table": table,
            "limit": self.config.limit,
            "offset": self.config.offset,
            **kwargs,
        }

        bigquery_temp_table: Optional[str] = None
        if platform == "bigquery" and (
            custom_sql or self.config.limit or self.config.offset
        ):
            # On BigQuery, we need to bypass GE's mechanism for creating temporary tables because
            # it requires create/delete table permissions.
            import google.cloud.bigquery.job.query
            from google.cloud.bigquery.dbapi.cursor import Cursor as BigQueryCursor

            raw_connection = self.base_engine.raw_connection()
            try:
                cursor: "BigQueryCursor" = cast(
                    "BigQueryCursor", raw_connection.cursor()
                )
                if custom_sql is not None:
                    # Note that limit and offset are not supported for custom SQL.
                    bq_sql = custom_sql
                else:
                    bq_sql = f"SELECT * FROM `{table}`"
                    if self.config.limit:
                        bq_sql += f" LIMIT {self.config.limit}"
                    if self.config.offset:
                        bq_sql += f" OFFSET {self.config.offset}"

                cursor.execute(bq_sql)

                # Great Expectations batch v2 API, which is the one we're using, requires
                # a concrete table name against which profiling is executed. Normally, GE
                # creates a table with an expiry time of 24 hours. However, we don't want the
                # temporary tables to stick around that long, so we'd also have to delete them
                # ourselves. As such, the profiler required create and delete table permissions
                # on BigQuery.
                #
                # It turns out that we can (ab)use the BigQuery cached results feature
                # to avoid creating temporary tables ourselves. For almost all queries, BigQuery
                # will store the results in a temporary, cached results table when an explicit
                # destination table is not provided. These tables are pretty easy to identify
                # because they live in "anonymous datasets" and have a name that looks like
                # "project-id._d60e97aec7f471046a960419adb6d44e98300db7.anon10774d0ea85fd20fe9671456c5c53d5f1b85e1b17bedb232dfce91661a219ee3"
                # These tables are per-user and per-project, so there's no risk of permissions escalation.
                # As per the docs, the cached results tables typically have a lifetime of 24 hours,
                # which should be plenty for our purposes.
                # See https://cloud.google.com/bigquery/docs/cached-results for more details.
                #
                # The code below extracts the name of the cached results table from the query job
                # and points GE to that table for profiling.
                #
                # Risks:
                # 1. If the query results are larger than the maximum response size, BigQuery will
                #    not cache the results. According to the docs https://cloud.google.com/bigquery/quotas,
                #    the maximum response size is 10 GB compressed.
                # 2. The cache lifetime of 24 hours is "best-effort" and hence not guaranteed.
                # 3. Tables with column-level security may not be cached, and tables with row-level
                #    security will not be cached.
                # 4. BigQuery "discourages" using cached results directly, but notes that
                #    the current semantics do allow it.
                #
                # The better long-term solution would be to use a subquery avoid this whole
                # temporary table dance. However, that would require either a) upgrading to
                # use GE's batch v3 API or b) bypassing GE altogether.

                query_job: Optional[
                    "google.cloud.bigquery.job.query.QueryJob"
                ] = cursor._query_job
                assert query_job
                temp_destination_table = query_job.destination
                bigquery_temp_table = f"{temp_destination_table.project}.{temp_destination_table.dataset_id}.{temp_destination_table.table_id}"
            finally:
                raw_connection.close()

        if platform == "bigquery":
            if bigquery_temp_table:
                ge_config["table"] = bigquery_temp_table
                ge_config["schema"] = None
                ge_config["limit"] = None
                ge_config["offset"] = None

                bigquery_temp_table = None

            assert not ge_config["limit"]
            assert not ge_config["offset"]
        else:
            if custom_sql is not None:
                ge_config["query"] = custom_sql

        with self._ge_context() as ge_context, PerfTimer() as timer:
            try:
                logger.info(f"Profiling {pretty_name}")

                batch = self._get_ge_dataset(
                    ge_context,
                    ge_config,
                    pretty_name=pretty_name,
                    platform=platform,
                )

                profile = _SingleDatasetProfiler(
                    batch,
                    pretty_name,
                    partition,
                    self.config,
                    self.report,
                    query_combiner,
                ).generate_dataset_profile()

                time_taken = timer.elapsed_seconds()
                logger.info(
                    f"Finished profiling {pretty_name}; took {time_taken:.3f} seconds"
                )
                self.times_taken.append(time_taken)
                if profile.rowCount is not None:
                    self.total_row_count += profile.rowCount

                return profile
            except Exception as e:
                if not self.config.catch_exceptions:
                    raise e
                logger.exception(f"Encountered exception while profiling {pretty_name}")
                self.report.report_failure(pretty_name, f"Profiling exception {e}")
                return None
            finally:
                if self.base_engine.engine.name == "trino":
                    self._drop_trino_temp_table(batch)

    def _get_ge_dataset(
        self,
        ge_context: GEContext,
        batch_kwargs: dict,
        pretty_name: str,
        platform: Optional[str] = None,
    ) -> Dataset:
        # This is effectively emulating the beginning of the process that
        # is followed by GE itself. In particular, we simply want to construct
        # a Dataset object.

        # profile_results = ge_context.data_context.profile_data_asset(
        #     ge_context.datasource_name,
        #     batch_kwargs={
        #         "datasource": ge_context.datasource_name,
        #         **batch_kwargs,
        #     },
        # )

        logger.debug(f"Got pretty_name={pretty_name}, kwargs={batch_kwargs}")
        expectation_suite_name = ge_context.datasource_name + "." + pretty_name

        ge_context.data_context.create_expectation_suite(
            expectation_suite_name=expectation_suite_name,
            overwrite_existing=True,
        )

        batch = ge_context.data_context.get_batch(
            expectation_suite_name=expectation_suite_name,
            batch_kwargs={
                "datasource": ge_context.datasource_name,
                **batch_kwargs,
            },
        )
        if platform is not None and platform == "bigquery":
            # This is done as GE makes the name as DATASET.TABLE
            # but we want it to be PROJECT.DATASET.TABLE instead for multi-project setups
            name_parts = pretty_name.split(".")
            if len(name_parts) != 3:
                logger.error(
                    f"Unexpected {pretty_name} while profiling. Should have 3 parts but has {len(name_parts)} parts."
                )
            # If we only have two parts that means the project_id is missing from the table name and we add it
            # Temp tables has 3 parts while normal tables only has 2 parts
            if len(str(batch._table).split(".")) == 2:
                batch._table = sa.text(f"{name_parts[0]}.{str(batch._table)}")
                logger.debug(f"Setting table name to be {batch._table}")

        return batch
