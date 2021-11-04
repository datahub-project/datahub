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

# from great_expectations.core.expectation_validation_result import (
#     ExpectationSuiteValidationResult,
#     ExpectationValidationResult,
# )
from great_expectations.data_context import BaseDataContext
from great_expectations.data_context.types.base import (
    DataContextConfig,
    DatasourceConfig,
    InMemoryStoreBackendDefaults,
    datasourceConfigSchema,
)
from great_expectations.dataset.dataset import Dataset
from great_expectations.datasource.sqlalchemy_datasource import SqlAlchemyDatasource
from great_expectations.profile.base import (
    DatasetProfiler,
    ProfilerCardinality,
    ProfilerDataType,
)
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

# from datahub.utilities.groupby import groupby_unsorted
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


class DatahubConfigurableProfiler(DatasetProfiler):
    """
    DatahubConfigurableProfiler is a wrapper on top of DatahubGECustomProfiler that essentially translates the
    GEProfilingConfig into a proper GEProfiler's interface and delegates actual profiling to DatahubGECustomProfiler.
    Column filtering based on our Allow/Deny patterns requires us to intercept the _profile call
    and compute the list of the columns to profile.
    """

    # @staticmethod
    # def _get_excluded_expectations(config: GEProfilingConfig) -> List[str]:
    #     # Compute excluded expectations
    #     excluded_expectations: List[str] = []
    #     if not config.include_field_null_count:
    #         excluded_expectations.append("expect_column_values_to_not_be_null")
    #     if not config.include_field_min_value:
    #         excluded_expectations.append("expect_column_min_to_be_between")
    #     if not config.include_field_max_value:
    #         excluded_expectations.append("expect_column_max_to_be_between")
    #     if not config.include_field_mean_value:
    #         excluded_expectations.append("expect_column_mean_to_be_between")
    #     if not config.include_field_median_value:
    #         excluded_expectations.append("expect_column_median_to_be_between")
    #     if not config.include_field_stddev_value:
    #         excluded_expectations.append("expect_column_stdev_to_be_between")
    #     if not config.include_field_quantiles:
    #         excluded_expectations.append("expect_column_quantile_values_to_be_between")
    #     if not config.include_field_distinct_value_frequencies:
    #         excluded_expectations.append("expect_column_distinct_values_to_be_in_set")
    #     if not config.include_field_histogram:
    #         excluded_expectations.append("expect_column_kl_divergence_to_be_less_than")
    #     if not config.include_field_sample_values:
    #         excluded_expectations.append("expect_column_values_to_be_in_set")
    #     return excluded_expectations

    @staticmethod
    def _get_columns_to_profile(
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

    # @staticmethod
    # def datahub_config_to_ge_config(
    #     dataset: Dataset,
    #     dataset_name: str,
    #     config: GEProfilingConfig,
    #     report: SQLSourceReport,
    # ) -> Dict[str, Any]:
    #     excluded_expectations: List[
    #         str
    #     ] = DatahubConfigurableProfiler._get_excluded_expectations(config)
    #     columns_to_profile: List[
    #         str
    #     ] = DatahubConfigurableProfiler._get_columns_to_profile(
    #         dataset, dataset_name, config, report
    #     )
    #     return {
    #         "excluded_expectations": excluded_expectations,
    #         "columns_to_profile": columns_to_profile,
    #     }

    # @classmethod
    # def _profile(
    #     cls, dataset: Dataset, configuration: Dict[str, Any]
    # ) -> ExpectationSuite:
    #     """
    #     Override method, which returns the expectation suite using the UserConfigurable Profiler.
    #     """
    #     profiler_configuration = cls.datahub_config_to_ge_config(
    #         dataset,
    #         configuration["dataset_name"],
    #         configuration["config"],
    #         configuration["report"],
    #     )
    #     return DatahubGECustomProfiler._profile(dataset, profiler_configuration)


def _get_dataset_column_quantiles(dataset: Dataset, column: str) -> List[QuantileClass]:
    quantiles = [0.05, 0.25, 0.5, 0.75, 0.95]
    values = dataset.get_column_quantiles(column, tuple(quantiles))
    return [
        QuantileClass(quantile=str(quantile), value=str(value))
        for quantile, value in zip(quantiles, values)
    ]


def _get_dataset_column_distinct_value_frequencies(
    dataset: Dataset, column: str
) -> List[ValueFrequencyClass]:
    return [
        ValueFrequencyClass(value=str(value), frequency=count)
        for value, count in dataset.get_column_value_counts(column).items()
    ]


def _get_dataset_column_histogram(
    dataset: Dataset, column: str
) -> Optional[HistogramClass]:
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


def _get_dataset_column_sample_values(
    dataset: Dataset, column: str
) -> Optional[List[str]]:
    res = dataset.expect_column_values_to_be_in_set(
        column, [], result_format="SUMMARY"
    ).result
    if not res:
        breakpoint()
    return [str(v) for v in res["partial_unexpected_list"]]


def _generate_dataset_profile(
    dataset: Dataset,
    dataset_name: str,
    config: GEProfilingConfig,
    report: SQLSourceReport,
) -> DatasetProfileClass:
    dataset.set_default_expectation_argument("catch_exceptions", True)
    dataset.set_config_value("interactive_evaluation", True)

    profile = DatasetProfileClass(timestampMillis=get_sys_time())

    all_columns = dataset.get_table_columns()
    columns_to_profile = DatahubConfigurableProfiler._get_columns_to_profile(
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

        type_ = BasicDatasetProfilerBase._get_column_type(dataset, column)
        cardinality = BasicDatasetProfilerBase._get_column_cardinality(dataset, column)
        dataset.set_config_value("interactive_evaluation", True)

        if config.include_field_null_count:
            non_null_count = dataset.get_column_nonnull_count(column)
            null_count = row_count - non_null_count
            assert null_count >= 0
            column_profile.nullCount = null_count
            if row_count > 0:
                column_profile.nullProportion = null_count / row_count
        else:
            non_null_count = None

        unique_count = dataset.get_column_unique_count(column)
        column_profile.uniqueCount = unique_count
        if non_null_count is not None and non_null_count > 0:
            column_profile.uniqueProportion = unique_count / non_null_count

        if config.include_field_sample_values:
            column_profile.sampleValues = _get_dataset_column_sample_values(
                dataset, column
            )

        if type_ == ProfilerDataType.INT:
            if cardinality == ProfilerCardinality.UNIQUE:
                # df.expect_column_values_to_be_unique(column)
                pass
            elif cardinality in [
                ProfilerCardinality.ONE,
                ProfilerCardinality.TWO,
                ProfilerCardinality.VERY_FEW,
                ProfilerCardinality.FEW,
            ]:
                if config.include_field_distinct_value_frequencies:
                    column_profile.distinctValueFrequencies = (
                        _get_dataset_column_distinct_value_frequencies(dataset, column)
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
                if config.include_field_stddev_value:
                    column_profile.stdev = str(dataset.get_column_stdev(column))

                if config.include_field_quantiles:
                    column_profile.quantiles = _get_dataset_column_quantiles(
                        dataset, column
                    )
                if config.include_field_histogram:
                    column_profile.histogram = _get_dataset_column_histogram(
                        dataset, column
                    )
            else:  # unknown cardinality - skip
                pass
        elif type_ == ProfilerDataType.FLOAT:
            if cardinality == ProfilerCardinality.UNIQUE:
                # df.expect_column_values_to_be_unique(column)
                pass

            elif cardinality in [
                ProfilerCardinality.ONE,
                ProfilerCardinality.TWO,
                ProfilerCardinality.VERY_FEW,
                ProfilerCardinality.FEW,
            ]:
                if config.include_field_distinct_value_frequencies:
                    column_profile.distinctValueFrequencies = (
                        _get_dataset_column_distinct_value_frequencies(dataset, column)
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
                if config.include_field_quantiles:
                    column_profile.quantiles = _get_dataset_column_quantiles(
                        dataset, column
                    )
                if config.include_field_histogram:
                    column_profile.histogram = _get_dataset_column_histogram(
                        dataset, column
                    )
            else:  # unknown cardinality - skip
                pass

        elif type_ == ProfilerDataType.STRING:
            if cardinality == ProfilerCardinality.UNIQUE:
                # df.expect_column_values_to_be_unique(column)
                pass

            elif cardinality in [
                ProfilerCardinality.ONE,
                ProfilerCardinality.TWO,
                ProfilerCardinality.VERY_FEW,
                ProfilerCardinality.FEW,
            ]:
                if config.include_field_distinct_value_frequencies:
                    column_profile.distinctValueFrequencies = (
                        _get_dataset_column_distinct_value_frequencies(dataset, column)
                    )
            else:
                pass

        elif type_ == ProfilerDataType.DATETIME:
            if config.include_field_min_value:
                column_profile.min = str(dataset.get_column_min(column))

            if config.include_field_max_value:
                column_profile.max = str(dataset.get_column_max(column))

            # Re-add once kl_divergence has been modified to support datetimes
            # df.expect_column_kl_divergence_to_be_less_than(column, partition_object=None,
            #                                            threshold=None, result_format='COMPLETE')

            if cardinality in [
                ProfilerCardinality.ONE,
                ProfilerCardinality.TWO,
                ProfilerCardinality.VERY_FEW,
                ProfilerCardinality.FEW,
            ]:
                if config.include_field_distinct_value_frequencies:
                    column_profile.distinctValueFrequencies = (
                        _get_dataset_column_distinct_value_frequencies(dataset, column)
                    )

        else:
            if cardinality == ProfilerCardinality.UNIQUE:
                # df.expect_column_values_to_be_unique(column)
                pass

            elif cardinality in [
                ProfilerCardinality.ONE,
                ProfilerCardinality.TWO,
                ProfilerCardinality.VERY_FEW,
                ProfilerCardinality.FEW,
            ]:
                if config.include_field_distinct_value_frequencies:
                    column_profile.distinctValueFrequencies = (
                        _get_dataset_column_distinct_value_frequencies(dataset, column)
                    )
            else:
                pass

    return profile


@dataclasses.dataclass
class GEContext:
    data_context: BaseDataContext
    datasource_name: str


@dataclasses.dataclass
class DatahubGEProfiler:
    report: SQLSourceReport
    config: GEProfilingConfig

    # The actual value doesn't matter, it just matters that we use it consistently throughout.
    _datasource_name_base: str = "my_sqlalchemy_datasource"

    def __init__(
        self,
        conn: Union[Engine, Connection],
        report: SQLSourceReport,
        config: GEProfilingConfig,
    ):
        self.base_engine = conn
        self.report = report
        self.config = config

    @contextlib.contextmanager
    def _ge_context(self) -> Iterator[GEContext]:
        # TRICKY: The call to `.engine` is quite important here. Connection.connect()
        # returns a "branched" connection, which does not actually use a new underlying
        # DBAPI object from the connection pool. Engine.connect() does what we want to
        # make the threading code work correctly.
        with self.base_engine.engine.connect() as conn:
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

                for async_profile in concurrent.futures.as_completed(async_profiles):
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

                profile = _generate_dataset_profile(
                    batch, pretty_name, self.config, self.report
                )
                # expectation_suite = DatahubConfigurableProfiler._profile(
                #     batch,
                #     {
                #         "config": self.config,
                #         "dataset_name": pretty_name,
                #         "report": self.report,
                #     },
                # )
                # run_id = RunIdentifier(run_name="profiling", run_time=None)
                # evrs = batch.validate(
                #     expectation_suite, run_id=run_id, result_format="SUMMARY"
                # )

                # profile = (
                #     self._convert_evrs_to_profile(evrs, pretty_name=pretty_name)
                #     if evrs is not None
                #     else None
                # )

                logger.info(
                    f"Finished profiling {pretty_name}; took {(timer.elapsed_seconds()):.3f} seconds"
                )
                return profile
            except Exception as e:
                # TODO change this
                raise e
                logger.warning(
                    f"Encountered exception {e}\nwhile profiling {pretty_name}"
                )
                self.report.report_warning(pretty_name, f"Exception {e}")
                return None

    def _get_ge_dataset(
        self,
        ge_context: GEContext,
        batch_kwargs: dict,
        pretty_name: str,
    ) -> Dataset:
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

    # @staticmethod
    # def _get_column_from_evr(evr: ExpectationValidationResult) -> Optional[str]:
    #     return evr.expectation_config.kwargs.get("column")

    # # The list of handled expectations has been created by referencing these files:
    # # - https://github.com/great-expectations/great_expectations/blob/71e9c1eae433a31416a38de1688e2793e9778299/great_expectations/render/renderer/profiling_results_overview_section_renderer.py
    # # - https://github.com/great-expectations/great_expectations/blob/71e9c1eae433a31416a38de1688e2793e9778299/great_expectations/render/renderer/column_section_renderer.py
    # # - https://github.com/great-expectations/great_expectations/blob/71e9c1eae433a31416a38de1688e2793e9778299/great_expectations/profile/basic_dataset_profiler.py

    # def _convert_evrs_to_profile(
    #     self,
    #     evrs: ExpectationSuiteValidationResult,
    #     pretty_name: str,
    # ) -> DatasetProfileClass:
    #     profile = DatasetProfileClass(timestampMillis=get_sys_time())

    #     for col, evrs_for_col in groupby_unsorted(
    #         evrs.results, key=self._get_column_from_evr
    #     ):
    #         if col is None:
    #             self._handle_convert_table_evrs(
    #                 profile, evrs_for_col, pretty_name=pretty_name
    #             )
    #         else:
    #             self._handle_convert_column_evrs(
    #                 profile,
    #                 col,
    #                 evrs_for_col,
    #                 pretty_name=pretty_name,
    #             )

    #     return profile

    # def _handle_convert_table_evrs(
    #     self,
    #     profile: DatasetProfileClass,
    #     table_evrs: Iterable[ExpectationValidationResult],
    #     pretty_name: str,
    # ) -> None:
    #     # TRICKY: This method mutates the profile directly.

    #     for evr in table_evrs:
    #         exp: str = evr.expectation_config.expectation_type
    #         res: dict = evr.result

    #         if exp == "expect_table_row_count_to_be_between":
    #             profile.rowCount = res["observed_value"]
    #         elif exp == "expect_table_columns_to_match_ordered_list":
    #             profile.columnCount = len(res["observed_value"])
    #         else:
    #             self.report.report_warning(
    #                 f"profile of {pretty_name}", f"unknown table mapper {exp}"
    #             )

    # def _handle_convert_column_evrs(  # noqa: C901 (complexity)
    #     self,
    #     profile: DatasetProfileClass,
    #     column: str,
    #     col_evrs: Iterable[ExpectationValidationResult],
    #     pretty_name: str,
    # ) -> None:
    #     # TRICKY: This method mutates the profile directly.

    #     column_profile = DatasetFieldProfileClass(fieldPath=column)

    #     profile.fieldProfiles = profile.fieldProfiles or []
    #     profile.fieldProfiles.append(column_profile)

    #     for evr in col_evrs:
    #         exp: str = evr.expectation_config.expectation_type
    #         res: dict = evr.result
    #         if not res:
    #             self.report.report_warning(
    #                 f"profile of {pretty_name}", f"{exp} did not yield any results"
    #             )
    #             continue

    #         if exp == "expect_column_unique_value_count_to_be_between":
    #             column_profile.uniqueCount = res["observed_value"]
    #         elif exp == "expect_column_proportion_of_unique_values_to_be_between":
    #             column_profile.uniqueProportion = res["observed_value"]
    #         elif exp == "expect_column_values_to_not_be_null":
    #             column_profile.nullCount = res["unexpected_count"]
    #             if (
    #                 "unexpected_percent" in res
    #                 and res["unexpected_percent"] is not None
    #             ):
    #                 column_profile.nullProportion = res["unexpected_percent"] / 100
    #         elif exp == "expect_column_values_to_not_match_regex":
    #             # ignore; generally used for whitespace checks using regex r"^\s+|\s+$"
    #             pass
    #         elif exp == "expect_column_mean_to_be_between":
    #             column_profile.mean = str(res["observed_value"])
    #         elif exp == "expect_column_min_to_be_between":
    #             column_profile.min = str(res["observed_value"])
    #         elif exp == "expect_column_max_to_be_between":
    #             column_profile.max = str(res["observed_value"])
    #         elif exp == "expect_column_median_to_be_between":
    #             column_profile.median = str(res["observed_value"])
    #         elif exp == "expect_column_stdev_to_be_between":
    #             column_profile.stdev = str(res["observed_value"])
    #         elif exp == "expect_column_quantile_values_to_be_between":
    #             if "observed_value" in res:
    #                 column_profile.quantiles = [
    #                     QuantileClass(quantile=str(quantile), value=str(value))
    #                     for quantile, value in zip(
    #                         res["observed_value"]["quantiles"],
    #                         res["observed_value"]["values"],
    #                     )
    #                 ]
    #         elif exp == "expect_column_values_to_be_in_set":
    #             column_profile.sampleValues = [
    #                 str(v) for v in res["partial_unexpected_list"]
    #             ]
    #         elif exp == "expect_column_kl_divergence_to_be_less_than":
    #             if "details" in res and "observed_partition" in res["details"]:
    #                 partition = res["details"]["observed_partition"]
    #                 column_profile.histogram = HistogramClass(
    #                     [str(v) for v in partition["bins"]],
    #                     [
    #                         partition["tail_weights"][0],
    #                         *partition["weights"],
    #                         partition["tail_weights"][1],
    #                     ],
    #                 )
    #         elif exp == "expect_column_distinct_values_to_be_in_set":
    #             if "details" in res and "value_counts" in res["details"]:
    #                 # This can be used to produce a bar chart since it includes values and frequencies.
    #                 # As such, it is handled differently from expect_column_values_to_be_in_set, which
    #                 # is nonexhaustive.
    #                 column_profile.distinctValueFrequencies = [
    #                     ValueFrequencyClass(value=str(value), frequency=count)
    #                     for value, count in res["details"]["value_counts"].items()
    #                 ]
    #         elif exp == "expect_column_values_to_be_in_type_list":
    #             # ignore; we already know the types for each column via ingestion
    #             pass
    #         elif exp == "expect_column_values_to_be_unique":
    #             # ignore; this is generally covered by the unique value count test
    #             pass
    #         else:
    #             self.report.report_warning(
    #                 f"profile of {pretty_name}",
    #                 f"warning: unknown column mapper {exp} in col {column}",
    #             )
