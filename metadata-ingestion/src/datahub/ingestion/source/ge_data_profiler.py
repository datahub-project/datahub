import concurrent.futures
import contextlib
import dataclasses
import itertools
import logging
import os
import threading
import unittest.mock
import uuid
from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple, Union

import pydantic
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
        ), unittest.mock.patch(
            "great_expectations.data_context.store.validations_store.ValidationsStore.set"
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


class _DatasetProfiler(BasicDatasetProfilerBase):
    @classmethod
    def _get_columns_to_profile(
        cls,
        dataset: Dataset,
        dataset_name: str,
        config: GEProfilingConfig,
        report: SQLSourceReport,
    ) -> List[str]:
        if config.profile_table_level_only:
            return []

        # Compute columns to profile
        columns_to_profile: List[str] = []
        # Compute ignored columns
        ignored_columns: List[str] = []
        for col in dataset.get_table_columns():
            # We expect the allow/deny patterns to specify '<table_pattern>.<column_pattern>'
            if not config.allow_deny_patterns.allowed(f"{dataset_name}.{col}"):
                ignored_columns.append(col)
            else:
                columns_to_profile.append(col)
        if ignored_columns:
            report.report_dropped(
                f"The profile of columns by pattern {dataset_name}({', '.join(sorted(ignored_columns))})"
            )

        if config.max_number_of_fields_to_profile is not None:
            columns_being_dropped: List[str] = list(
                itertools.islice(
                    columns_to_profile, config.max_number_of_fields_to_profile, None
                )
            )
            columns_to_profile = list(
                itertools.islice(
                    columns_to_profile, config.max_number_of_fields_to_profile
                )
            )
            if columns_being_dropped:
                report.report_dropped(
                    f"The max_number_of_fields_to_profile={config.max_number_of_fields_to_profile} reached. Profile of columns {dataset_name}({', '.join(sorted(columns_being_dropped))})"
                )
        return columns_to_profile

    @classmethod
    def _get_dataset_column_quantiles(
        cls, dataset: Dataset, column: str
    ) -> Optional[List[QuantileClass]]:
        # FIXME: Eventually we'd like to switch to using the quantile method directly.
        # values = dataset.get_column_quantiles(column, tuple(quantiles))

        dataset.set_config_value("interactive_evaluation", True)

        res = dataset.expect_column_quantile_values_to_be_between(
            column,
            quantile_ranges={
                "quantiles": [0.05, 0.25, 0.5, 0.75, 0.95],
                "value_ranges": [
                    [None, None],
                    [None, None],
                    [None, None],
                    [None, None],
                    [None, None],
                ],
            },
        ).result
        if "observed_value" in res:
            return [
                QuantileClass(quantile=str(quantile), value=str(value))
                for quantile, value in zip(
                    res["observed_value"]["quantiles"],
                    res["observed_value"]["values"],
                )
            ]
        return None

    @classmethod
    def _get_dataset_column_distinct_value_frequencies(
        cls, dataset: Dataset, column: str
    ) -> List[ValueFrequencyClass]:
        return [
            ValueFrequencyClass(value=str(value), frequency=count)
            for value, count in dataset.get_column_value_counts(column).items()
        ]

    @classmethod
    def _get_dataset_column_histogram(
        cls, dataset: Dataset, column: str
    ) -> Optional[HistogramClass]:
        dataset.set_config_value("interactive_evaluation", True)

        res = dataset.expect_column_kl_divergence_to_be_less_than(
            column,
            partition_object=None,
            threshold=None,
            result_format="COMPLETE",
        ).result
        if "details" in res and "observed_partition" in res["details"]:
            partition = res["details"]["observed_partition"]
            return HistogramClass(
                [str(v) for v in partition["bins"]],
                [
                    partition["tail_weights"][0],
                    *partition["weights"],
                    partition["tail_weights"][1],
                ],
            )
        return None

    @classmethod
    def _get_dataset_column_sample_values(
        cls, dataset: Dataset, column: str
    ) -> Optional[List[str]]:
        dataset.set_config_value("interactive_evaluation", True)

        res = dataset.expect_column_values_to_be_in_set(
            column, [], result_format="SUMMARY"
        ).result
        return [str(v) for v in res["partial_unexpected_list"]]

    # For some reason Flake8 really wants the complexity annotation on both lines.
    @classmethod  # noqa: C901 (complexity)
    def generate_dataset_profile(  # noqa: C901 (complexity)
        cls,
        dataset: Dataset,
        dataset_name: str,
        config: GEProfilingConfig,
        report: SQLSourceReport,
    ) -> DatasetProfileClass:
        dataset.set_default_expectation_argument(
            "catch_exceptions", config.catch_exceptions
        )

        profile = DatasetProfileClass(timestampMillis=get_sys_time())

        all_columns = dataset.get_table_columns()
        columns_to_profile = cls._get_columns_to_profile(
            dataset, dataset_name, config, report
        )

        row_count = dataset.get_row_count()
        profile.rowCount = row_count
        profile.columnCount = len(all_columns)

        profile.fieldProfiles = []
        for column in all_columns:
            column_profile = DatasetFieldProfileClass(fieldPath=column)
            profile.fieldProfiles.append(column_profile)

            if column not in columns_to_profile:
                continue

            type_ = cls._get_column_type(dataset, column)
            cardinality = cls._get_column_cardinality(dataset, column)

            if config.include_field_null_count:
                non_null_count = dataset.get_column_nonnull_count(column)
                null_count = row_count - non_null_count
                assert null_count >= 0
                column_profile.nullCount = null_count
                if row_count > 0:
                    column_profile.nullProportion = null_count / row_count
            else:
                non_null_count = None

            try:
                unique_count = dataset.get_column_unique_count(column)
                column_profile.uniqueCount = unique_count
                if non_null_count is not None and non_null_count > 0:
                    column_profile.uniqueProportion = unique_count / non_null_count
            except Exception:
                logger.exception(
                    f"Failed to get unique count for column {dataset_name}.{column}"
                )

            if config.include_field_sample_values:
                column_profile.sampleValues = cls._get_dataset_column_sample_values(
                    dataset, column
                )

            if type_ == ProfilerDataType.INT or type_ == ProfilerDataType.FLOAT:
                if cardinality == ProfilerCardinality.UNIQUE:
                    pass
                elif cardinality in [
                    ProfilerCardinality.ONE,
                    ProfilerCardinality.TWO,
                    ProfilerCardinality.VERY_FEW,
                    ProfilerCardinality.FEW,
                ]:
                    if config.include_field_distinct_value_frequencies:
                        column_profile.distinctValueFrequencies = (
                            cls._get_dataset_column_distinct_value_frequencies(
                                dataset, column
                            )
                        )
                elif cardinality in [
                    ProfilerCardinality.MANY,
                    ProfilerCardinality.VERY_MANY,
                    ProfilerCardinality.UNIQUE,
                ]:
                    if config.include_field_min_value:
                        column_profile.min = str(dataset.get_column_min(column))
                    if config.include_field_max_value:
                        column_profile.max = str(dataset.get_column_max(column))
                    if config.include_field_mean_value:
                        column_profile.mean = str(dataset.get_column_mean(column))
                    if config.include_field_median_value:
                        column_profile.median = str(dataset.get_column_median(column))
                    if type_ == ProfilerDataType.INT:
                        if config.include_field_stddev_value:
                            column_profile.stdev = str(dataset.get_column_stdev(column))

                    if config.include_field_quantiles:
                        column_profile.quantiles = cls._get_dataset_column_quantiles(
                            dataset, column
                        )
                    if config.include_field_histogram:
                        column_profile.histogram = cls._get_dataset_column_histogram(
                            dataset, column
                        )
                else:  # unknown cardinality - skip
                    pass

            elif type_ == ProfilerDataType.STRING:
                if cardinality in [
                    ProfilerCardinality.ONE,
                    ProfilerCardinality.TWO,
                    ProfilerCardinality.VERY_FEW,
                    ProfilerCardinality.FEW,
                ]:
                    if config.include_field_distinct_value_frequencies:
                        column_profile.distinctValueFrequencies = (
                            cls._get_dataset_column_distinct_value_frequencies(
                                dataset, column
                            )
                        )

            elif type_ == ProfilerDataType.DATETIME:
                if config.include_field_min_value:
                    column_profile.min = str(dataset.get_column_min(column))
                if config.include_field_max_value:
                    column_profile.max = str(dataset.get_column_max(column))

                # FIXME: Re-add histogram once kl_divergence has been modified to support datetimes

                if cardinality in [
                    ProfilerCardinality.ONE,
                    ProfilerCardinality.TWO,
                    ProfilerCardinality.VERY_FEW,
                    ProfilerCardinality.FEW,
                ]:
                    if config.include_field_distinct_value_frequencies:
                        column_profile.distinctValueFrequencies = (
                            cls._get_dataset_column_distinct_value_frequencies(
                                dataset, column
                            )
                        )

            else:
                if cardinality in [
                    ProfilerCardinality.ONE,
                    ProfilerCardinality.TWO,
                    ProfilerCardinality.VERY_FEW,
                    ProfilerCardinality.FEW,
                ]:
                    if config.include_field_distinct_value_frequencies:
                        column_profile.distinctValueFrequencies = (
                            cls._get_dataset_column_distinct_value_frequencies(
                                dataset, column
                            )
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
        with PerfTimer() as timer:
            max_workers = min(max_workers, len(requests))
            logger.info(
                f"Will profile {len(requests)} table(s) with {max_workers} worker(s) - this may take a while"
            )
            with concurrent.futures.ThreadPoolExecutor(
                max_workers=max_workers
            ) as async_executor:
                async_profiles = [
                    async_executor.submit(
                        self.generate_profile_from_request,
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

    def generate_profile_from_request(
        self, request: GEProfilerRequest
    ) -> Tuple[GEProfilerRequest, Optional[DatasetProfileClass]]:
        return request, self.generate_profile(
            request.pretty_name,
            **request.batch_kwargs,
        )

    def generate_profile(
        self,
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
                profile = _DatasetProfiler.generate_dataset_profile(
                    batch, pretty_name, self.config, self.report
                )

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
