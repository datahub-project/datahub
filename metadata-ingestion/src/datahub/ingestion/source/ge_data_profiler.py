import concurrent.futures
import contextlib
import dataclasses
import itertools
import logging
import os
import threading
import traceback
import unittest.mock
import uuid
from typing import Any, ClassVar, Dict, Iterable, Iterator, List, Optional, Tuple, Union

import greenlet
import pydantic
import sqlalchemy.engine
from great_expectations.data_context import BaseDataContext
from great_expectations.data_context.types.base import (
    DataContextConfig,
    DatasourceConfig,
    InMemoryStoreBackendDefaults,
    datasourceConfigSchema,
)
from great_expectations.dataset.dataset import Dataset
from great_expectations.datasource.sqlalchemy_datasource import SqlAlchemyDatasource
from great_expectations.profile.base import ProfilerCardinality, ProfilerDataType
from great_expectations.profile.basic_dataset_profiler import BasicDatasetProfilerBase
from sqlalchemy.engine import Connection, Engine

from datahub.configuration.common import AllowDenyPattern, ConfigModel
from datahub.emitter.mce_builder import get_sys_time
from datahub.ingestion.source.sql.sql_common import SQLSourceReport
from datahub.metadata.schema_classes import (
    DatasetFieldProfileClass,
    DatasetProfileClass,
    HistogramClass,
    QuantileClass,
    ValueFrequencyClass,
)
from datahub.utilities.perf_timer import PerfTimer

logger: logging.Logger = logging.getLogger(__name__)

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


class GEProfilingConfig(ConfigModel):
    enabled: bool = False
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

    # Hidden option - used for debugging purposes.
    catch_exceptions: bool = True

    @pydantic.root_validator()
    def ensure_field_level_settings_are_normalized(
        cls: "GEProfilingConfig", values: Dict[str, Any]
    ) -> Dict[str, Any]:
        max_num_fields_to_profile_key = "max_number_of_fields_to_profile"
        table_level_profiling_only_key = "profile_table_level_only"
        max_num_fields_to_profile = values.get(max_num_fields_to_profile_key)
        if values.get(table_level_profiling_only_key):
            all_field_level_metrics: List[str] = [
                "include_field_null_count",
                "include_field_min_value",
                "include_field_max_value",
                "include_field_mean_value",
                "include_field_median_value",
                "include_field_stddev_value",
                "include_field_quantiles",
                "include_field_distinct_value_frequencies",
                "include_field_histogram",
                "include_field_sample_values",
            ]
            # Suppress all field-level metrics
            for field_level_metric in all_field_level_metrics:
                values[field_level_metric] = False
            assert (
                max_num_fields_to_profile is None
            ), f"{max_num_fields_to_profile_key} should be set to None"

        if values.get("turn_off_expensive_profiling_metrics"):
            if not values.get(table_level_profiling_only_key):
                expensive_field_level_metrics: List[str] = [
                    "include_field_quantiles",
                    "include_field_distinct_value_frequencies",
                    "include_field_histogram",
                    "include_field_sample_values",
                ]
                for expensive_field_metric in expensive_field_level_metrics:
                    values[expensive_field_metric] = False
            if max_num_fields_to_profile is None:
                # We currently profile up to 10 non-filtered columns in this mode by default.
                values[max_num_fields_to_profile_key] = 10

        return values


@dataclasses.dataclass
class _QueryFuture:
    conn: Connection
    query: Any
    multiparams: Any
    params: Any

    done: bool = False
    res: Any = None


@dataclasses.dataclass
class _SQLAlchemyQueryCoalescenceManager:
    # TODO this must be a singleton, should not be set up per-thread

    # Without the staticmethod decorator, Python thinks that this is an instance
    # method and attempts to bind self to it.
    _underlying_sa_execute_method: ClassVar = staticmethod(
        sqlalchemy.engine.Connection.execute
    )

    _allowed_single_row_query_methods: ClassVar = [
        (
            "great_expectations/dataset/sqlalchemy_dataset.py",
            {
                "get_row_count",
                "get_column_min",
                "get_column_max",
                "get_column_mean",
                "get_column_median",
                "get_column_stdev",
                "get_column_stdev",
                "get_column_nonnull_count",
                "get_column_unique_count",
            },
        ),
    ]

    catch_exceptions: bool

    # There will be one main greenlet per thread. As such, queries will be
    # queued according to the main greenlet's thread ID.
    _queries_by_thread: Dict[greenlet.greenlet, List[_QueryFuture]] = {}

    def _is_single_row_query_method(
        self, stack: traceback.StackSummary, query: Any
    ) -> bool:
        # We'll do this the inefficient way since the arrays are pretty small.
        for frame in stack:
            for file_suffix, allowed_methods in self._allowed_single_row_query_methods:
                if not frame.filename.endswith(file_suffix):
                    continue
                if frame.name in allowed_methods:
                    return True
        return False

    def _get_main_greenlet(self) -> greenlet.greenlet:
        let = greenlet.getcurrent()
        while let.parent is not None:
            let = let.parent
        return let

    def _get_queue(self, main_greenlet: greenlet.greenlet) -> List[_QueryFuture]:
        assert main_greenlet.parent is None

        # Because of the GIL, this operation is thread-safe. Hence, we can
        # just add the main greenlet here without any special consideration.
        # https://stackoverflow.com/a/6953515/5004662
        # https://docs.python.org/3/glossary.html#term-global-interpreter-lock

        return self._queries_by_thread.setdefault(main_greenlet, [])

    def _handle_execute(
        self, conn: Connection, query: Any, multiparams: Any, params: Any
    ) -> Tuple[bool, Any]:
        # Returns True with result if the query was handled, False if it
        # should be executed normally using the fallback method.

        # Must handle synchronously if the query was issued from the main greenlet.
        main_greenlet = self._get_main_greenlet()
        if greenlet.getcurrent() == main_greenlet:
            breakpoint()
            return False, None

        # Don't attempt to handle if these are set.
        if multiparams or params:
            return False, None

        # Attempt to match against the known single-row query methods.
        stack = traceback.extract_stack()
        if not self._is_single_row_query_method(stack, query):
            return False, None

        # Figure out how many columns this query returns.
        # TODO add escape hatch
        if not hasattr(query, "columns"):
            return False, None
        columns = list(query.columns)
        assert len(columns) > 0

        # Add query to the queue.
        queue = self._get_queue(main_greenlet)
        query_future = _QueryFuture(conn, query, multiparams, params)
        queue.append(query_future)

        # Yield control back to the main greenlet until the query is done.
        # We assume that the main greenlet will be the one that actually executes the query.
        while not query_future.done:
            main_greenlet.switch()

        return True, query_future.res

    @contextlib.contextmanager
    def activate(self) -> Iterator["_SQLAlchemyQueryCoalescenceManager"]:
        def _sa_execute_fake(conn, query, *args, **kwargs):
            try:
                handled, result = self._handle_execute(conn, query, args, kwargs)
            except Exception as e:
                if not self.catch_exceptions:
                    raise e
                logger.exception(
                    f"Failed to execute query normally, using fallback: {str(query)}"
                )
                return self._underlying_sa_execute_method(conn, query, *args, **kwargs)
            else:
                if handled:
                    logger.info(f"Query was handled: {str(query)}")
                    return result
                else:
                    logger.info(f"Executing query normally: {str(query)}")
                    return self._underlying_sa_execute_method(
                        conn, query, *args, **kwargs
                    )

        with unittest.mock.patch(
            "sqlalchemy.engine.Connection.execute", _sa_execute_fake
        ):
            yield self


@dataclasses.dataclass
class _SingleDatasetProfiler(BasicDatasetProfilerBase):
    dataset: Dataset
    dataset_name: str
    config: GEProfilingConfig
    report: SQLSourceReport

    def _get_columns_to_profile(self) -> List[str]:
        if self.config.profile_table_level_only:
            return []

        # Compute columns to profile
        columns_to_profile: List[str] = []
        # Compute ignored columns
        ignored_columns: List[str] = []
        for col in self.dataset.get_table_columns():
            # We expect the allow/deny patterns to specify '<table_pattern>.<column_pattern>'
            if not self.config.allow_deny_patterns.allowed(
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
            columns_being_dropped: List[str] = list(
                itertools.islice(
                    columns_to_profile,
                    self.config.max_number_of_fields_to_profile,
                    None,
                )
            )
            columns_to_profile = list(
                itertools.islice(
                    columns_to_profile, self.config.max_number_of_fields_to_profile
                )
            )
            if columns_being_dropped:
                self.report.report_dropped(
                    f"The max_number_of_fields_to_profile={self.config.max_number_of_fields_to_profile} reached. Profile of columns {self.dataset_name}({', '.join(sorted(columns_being_dropped))})"
                )
        return columns_to_profile

    def _get_column_type(self, column: str) -> ProfilerDataType:
        return BasicDatasetProfilerBase._get_column_type(self.dataset, column)

    def _get_column_cardinality(self, column: str) -> ProfilerCardinality:
        return BasicDatasetProfilerBase._get_column_cardinality(self.dataset, column)

    def _get_dataset_column_min(
        self, column_profile: DatasetFieldProfileClass, column: str
    ) -> None:
        if self.config.include_field_min_value:
            column_profile.min = str(self.dataset.get_column_min(column))

    def _get_dataset_column_max(
        self, column_profile: DatasetFieldProfileClass, column: str
    ) -> None:
        if self.config.include_field_max_value:
            column_profile.max = str(self.dataset.get_column_max(column))

    def _get_dataset_column_mean(
        self, column_profile: DatasetFieldProfileClass, column: str
    ) -> None:
        if self.config.include_field_mean_value:
            column_profile.mean = str(self.dataset.get_column_mean(column))

    def _get_dataset_column_median(
        self, column_profile: DatasetFieldProfileClass, column: str
    ) -> None:
        if self.config.include_field_median_value:
            column_profile.median = str(self.dataset.get_column_median(column))

    def _get_dataset_column_stdev(
        self, column_profile: DatasetFieldProfileClass, column: str
    ) -> None:
        if self.config.include_field_stddev_value:
            column_profile.stdev = str(self.dataset.get_column_stdev(column))

    def _get_dataset_column_quantiles(
        self, column_profile: DatasetFieldProfileClass, column: str
    ) -> None:
        if self.config.include_field_quantiles:
            # FIXME: Eventually we'd like to switch to using the quantile method directly.
            # values = dataset.get_column_quantiles(column, tuple(quantiles))

            self.dataset.set_config_value("interactive_evaluation", True)
            quantiles = [0.05, 0.25, 0.5, 0.75, 0.95]

            res = self.dataset.expect_column_quantile_values_to_be_between(
                column,
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

    def _get_dataset_column_distinct_value_frequencies(
        self, column_profile: DatasetFieldProfileClass, column: str
    ) -> None:
        if self.config.include_field_distinct_value_frequencies:
            column_profile.distinctValueFrequencies = [
                ValueFrequencyClass(value=str(value), frequency=count)
                for value, count in self.dataset.get_column_value_counts(column).items()
            ]

    def _get_dataset_column_histogram(
        self, column_profile: DatasetFieldProfileClass, column: str
    ) -> None:
        if self.config.include_field_histogram:
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

    def _get_dataset_column_sample_values(
        self, column_profile: DatasetFieldProfileClass, column: str
    ) -> None:
        if self.config.include_field_sample_values:
            self.dataset.set_config_value("interactive_evaluation", True)

            res = self.dataset.expect_column_values_to_be_in_set(
                column, [], result_format="SUMMARY"
            ).result
            column_profile.sampleValues = [
                str(v) for v in res["partial_unexpected_list"]
            ]

    def generate_dataset_profile(  # noqa: C901 (complexity)
        self,
    ) -> DatasetProfileClass:
        self.dataset.set_default_expectation_argument(
            "catch_exceptions", self.config.catch_exceptions
        )

        profile = DatasetProfileClass(timestampMillis=get_sys_time())
        profile.fieldProfiles = []

        all_columns = self.dataset.get_table_columns()
        profile.columnCount = len(all_columns)
        columns_to_profile = set(self._get_columns_to_profile())

        columns_profiling_queue: List[
            Tuple[str, DatasetFieldProfileClass, ProfilerDataType, ProfilerCardinality]
        ] = []
        for column in all_columns:
            column_profile = DatasetFieldProfileClass(fieldPath=column)
            profile.fieldProfiles.append(column_profile)

            if column in columns_to_profile:
                type_ = self._get_column_type(column)
                cardinality = self._get_column_cardinality(column)

                columns_profiling_queue.append(
                    (column, column_profile, type_, cardinality)
                )

        row_count = self.dataset.get_row_count()
        profile.rowCount = row_count

        for column, column_profile, type_, cardinality in columns_profiling_queue:
            if self.config.include_field_null_count:
                non_null_count = self.dataset.get_column_nonnull_count(column)
                null_count = row_count - non_null_count
                assert null_count >= 0
                column_profile.nullCount = null_count
                if row_count > 0:
                    column_profile.nullProportion = null_count / row_count
            else:
                non_null_count = None

            try:
                unique_count = self.dataset.get_column_unique_count(column)
                column_profile.uniqueCount = unique_count
                if non_null_count is not None and non_null_count > 0:
                    column_profile.uniqueProportion = unique_count / non_null_count
            except Exception:
                logger.exception(
                    f"Failed to get unique count for column {self.dataset_name}.{column}"
                )

            self._get_dataset_column_sample_values(column_profile, column)

            if type_ == ProfilerDataType.INT or type_ == ProfilerDataType.FLOAT:
                if cardinality == ProfilerCardinality.UNIQUE:
                    pass
                elif cardinality in [
                    ProfilerCardinality.ONE,
                    ProfilerCardinality.TWO,
                    ProfilerCardinality.VERY_FEW,
                    ProfilerCardinality.FEW,
                ]:
                    self._get_dataset_column_distinct_value_frequencies(
                        column_profile,
                        column,
                    )
                elif cardinality in [
                    ProfilerCardinality.MANY,
                    ProfilerCardinality.VERY_MANY,
                    ProfilerCardinality.UNIQUE,
                ]:
                    self._get_dataset_column_min(column_profile, column)
                    self._get_dataset_column_max(column_profile, column)
                    self._get_dataset_column_mean(column_profile, column)
                    self._get_dataset_column_median(column_profile, column)
                    if type_ == ProfilerDataType.INT:
                        self._get_dataset_column_stdev(column_profile, column)

                    self._get_dataset_column_quantiles(column_profile, column)
                    self._get_dataset_column_histogram(column_profile, column)
                else:  # unknown cardinality - skip
                    pass

            elif type_ == ProfilerDataType.STRING:
                if cardinality in [
                    ProfilerCardinality.ONE,
                    ProfilerCardinality.TWO,
                    ProfilerCardinality.VERY_FEW,
                    ProfilerCardinality.FEW,
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
                    ProfilerCardinality.ONE,
                    ProfilerCardinality.TWO,
                    ProfilerCardinality.VERY_FEW,
                    ProfilerCardinality.FEW,
                ]:
                    self._get_dataset_column_distinct_value_frequencies(
                        column_profile,
                        column,
                    )

            else:
                if cardinality in [
                    ProfilerCardinality.ONE,
                    ProfilerCardinality.TWO,
                    ProfilerCardinality.VERY_FEW,
                    ProfilerCardinality.FEW,
                ]:
                    self._get_dataset_column_distinct_value_frequencies(
                        column_profile,
                        column,
                    )

        return profile


@dataclasses.dataclass
class GEContext:
    data_context: BaseDataContext
    datasource_name: str


@dataclasses.dataclass
class DatahubGEProfiler:
    report: SQLSourceReport
    config: GEProfilingConfig

    base_engine: Engine

    # The actual value doesn't matter, it just matters that we use it consistently throughout.
    _datasource_name_base: str = "my_sqlalchemy_datasource"

    def __init__(
        self,
        conn: Union[Engine, Connection],
        report: SQLSourceReport,
        config: GEProfilingConfig,
    ):
        self.report = report
        self.config = config

        # TRICKY: The call to `.engine` is quite important here. Connection.connect()
        # returns a "branched" connection, which does not actually use a new underlying
        # DB-API object from the connection pool. Engine.connect() does what we want to
        # make the threading code work correctly. As such, we need to make sure we've
        # got an engine here.
        self.base_engine = conn.engine

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
        self, requests: List[GEProfilerRequest], max_workers: int
    ) -> Iterable[Tuple[GEProfilerRequest, Optional[DatasetProfileClass]]]:
        with PerfTimer() as timer, _SQLAlchemyQueryCoalescenceManager(
            catch_exceptions=self.config.catch_exceptions
        ).activate() as query_combiner:
            max_workers = min(max_workers, len(requests))
            logger.info(
                f"Will profile {len(requests)} table(s) with {max_workers} worker(s) - this may take a while"
            )
            with concurrent.futures.ThreadPoolExecutor(
                max_workers=max_workers
            ) as async_executor:
                async_profiles = [
                    async_executor.submit(
                        self._generate_profile_from_request,
                        query_combiner,
                        request,
                    )
                    for request in requests
                ]

                # Avoid using as_completed so that the results are yielded in the
                # same order as the requests.
                # for async_profile in concurrent.futures.as_completed(async_profiles):
                for async_profile in async_profiles:
                    yield async_profile.result()

            logger.info(
                f"Profiling {len(requests)} table(s) finished in {(timer.elapsed_seconds()):.3f} seconds"
            )

    def _generate_profile_from_request(
        self,
        query_combiner: _SQLAlchemyQueryCoalescenceManager,
        request: GEProfilerRequest,
    ) -> Tuple[GEProfilerRequest, Optional[DatasetProfileClass]]:
        return request, self._generate_single_profile(
            query_combiner,
            request.pretty_name,
            **request.batch_kwargs,
        )

    def _generate_single_profile(
        self,
        query_combiner: _SQLAlchemyQueryCoalescenceManager,
        pretty_name: str,
        schema: str = None,
        table: str = None,
        **kwargs: Any,
    ) -> Optional[DatasetProfileClass]:
        with self._ge_context() as ge_context, PerfTimer() as timer:
            try:
                logger.info(f"Profiling {pretty_name}")

                batch = self._get_ge_dataset(
                    ge_context,
                    {
                        "schema": schema,
                        "table": table,
                        "limit": self.config.limit,
                        "offset": self.config.offset,
                        **kwargs,
                    },
                    pretty_name=pretty_name,
                )
                # TODO add query_combiner
                profile = _SingleDatasetProfiler(
                    batch, pretty_name, self.config, self.report
                ).generate_dataset_profile()

                logger.info(
                    f"Finished profiling {pretty_name}; took {(timer.elapsed_seconds()):.3f} seconds"
                )
                return profile
            except Exception as e:
                if not self.config.catch_exceptions:
                    raise e
                logger.exception(f"Encountered exception while profiling {pretty_name}")
                self.report.report_failure(pretty_name, f"Profiling exception {e}")
                return None

    def _get_ge_dataset(
        self,
        ge_context: GEContext,
        batch_kwargs: dict,
        pretty_name: str,
    ) -> Dataset:
        # This is effectively emulating the beginning of the process that
        # is followed by GE itself. In particular, we simply want to construct
        # a Dataset object.

        # profile_results = ge_context.data_context.profile_data_asset(
        #     ge_context.datasource_name,
        #     profiler=DatahubConfigurableProfiler,
        #     profiler_configuration={
        #         "config": self.config,
        #         "dataset_name": pretty_name,
        #         "report": self.report,
        #     },
        #     batch_kwargs={
        #         "datasource": ge_context.datasource_name,
        #         **batch_kwargs,
        #     },
        # )

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
        return batch
