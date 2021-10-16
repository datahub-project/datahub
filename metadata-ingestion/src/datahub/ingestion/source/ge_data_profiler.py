import contextlib

# import cProfile
import dataclasses
import logging
import unittest.mock
from typing import Any, Dict, Iterable, List, Optional

# import great_expectations.core
from great_expectations.core import ExpectationSuite

# from great_expectations.core.batch import Batch
from great_expectations.core.expectation_validation_result import (
    ExpectationSuiteValidationResult,
    ExpectationValidationResult,
)
from great_expectations.data_context import BaseDataContext
from great_expectations.data_context.types.base import (
    DataContextConfig,
    DatasourceConfig,
    InMemoryStoreBackendDefaults,
)
from great_expectations.dataset.dataset import Dataset
from great_expectations.datasource.sqlalchemy_datasource import SqlAlchemyDatasource
from great_expectations.profile.base import DatasetProfiler
from great_expectations.profile.user_configurable_profiler import (
    UserConfigurableProfiler,
)

from datahub.configuration.common import ConfigModel
from datahub.emitter.mce_builder import get_sys_time
from datahub.ingestion.api.source import SourceReport
from datahub.metadata.schema_classes import (
    DatasetFieldProfileClass,
    DatasetProfileClass,
    HistogramClass,
    QuantileClass,
    ValueFrequencyClass,
)
from datahub.utilities.groupby import groupby_unsorted

logger: logging.Logger = logging.getLogger(__name__)

# The reason for this wacky structure is quite fun. GE basically assumes that
# the config structures were generated directly from YML and further assumes that
# they can be `deepcopy`'d without issue. The SQLAlchemy engine and connection
# objects, however, cannot be copied. Despite the fact that the SqlAlchemyDatasource
# class accepts an `engine` argument (which can actually be an Engine or Connection
# object), we cannot use it because of the config loading system. As such, we instead
# pass a "dummy" config into the DatasourceConfig, but then dynamically add the
# engine parameter when the SqlAlchemyDatasource is actually set up, and then remove
# it from the cached config object to avoid those same copying mechanisms. While
# you might expect that this is sufficient because GE caches the Datasource objects
# that it constructs, it actually occassionally bypasses this cache (likely a bug
# in GE), and so we need to wrap every call to GE with the below context manager.


@contextlib.contextmanager
def _properly_init_datasource(conn):
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


class GEProfilingConfig(ConfigModel):
    enabled: bool = False
    limit: Optional[int] = None
    offset: Optional[int] = None
    profile_table_level_only: bool = True
    # TODO: Translate what sub-knobs this translates to.
    turn_off_expensive_profiling_metrics: bool = True
    get_field_level_stats_if_not_null_only: bool = False
    get_field_unique_count: bool = True
    get_field_unique_proportion: bool = True
    get_field_null_count: bool = True
    get_field_min_value: bool = True
    get_field_max_value: bool = True
    get_field_mean_value: bool = True
    get_field_median_value: bool = True
    get_field_stddev_value: bool = True
    get_field_quantiles: bool = True
    get_field_distinct_value_frequencies: bool = True
    get_field_histogram: bool = True
    get_field_sample_values: bool = True


class DatahubConfigurableProfiler(DatasetProfiler):
    """
    This is simply a wrapper over the GE's UserConfigurableProfile, since it does not inherit from DatasetProfile.
    """

    @staticmethod
    def datahub_config_to_ge_config(config: GEProfilingConfig) -> Dict[str, Any]:
        ge_config: Dict[str, Any] = {}
        ge_config["table_expectations_only"] = config.profile_table_level_only
        ge_config["not_null_only"] = config.get_field_level_stats_if_not_null_only
        # TODO: This is tricky!
        ge_config["ignored_columns"] = []
        # TODO: Set excluded expectations
        excluded_expectations: List[str] = []
        if not config.profile_table_level_only:
            if not config.get_field_unique_count:
                excluded_expectations.append(
                    "expect_column_unique_value_count_to_be_between"
                )
            if not config.get_field_unique_proportion:
                excluded_expectations.append(
                    "expect_column_proportion_of_unique_values_to_be_between"
                )
            if not config.get_field_null_count:
                excluded_expectations.append("expect_column_values_to_not_be_null")
            if not config.get_field_min_value:
                excluded_expectations.append("expect_column_min_to_be_between")
            if not config.get_field_max_value:
                excluded_expectations.append("expect_column_max_to_be_between")
            if not config.get_field_mean_value:
                excluded_expectations.append("expect_column_mean_to_be_between")
            if not config.get_field_median_value:
                excluded_expectations.append("expect_column_median_to_be_between")
            if not config.get_field_stddev_value:
                excluded_expectations.append("expect_column_stdev_to_be_between")
            if not config.get_field_quantiles:
                excluded_expectations.append(
                    "expect_column_quantile_values_to_be_between"
                )
            if not config.get_field_distinct_value_frequencies:
                excluded_expectations.append(
                    "expect_column_distinct_values_to_be_in_set"
                )
            if not config.get_field_histogram:
                excluded_expectations.append(
                    "expect_column_kl_divergence_to_be_less_than"
                )
            if not config.get_field_sample_values:
                excluded_expectations.append("expect_column_values_to_be_in_set")

        ge_config["excluded_expectations"] = excluded_expectations
        return ge_config

    @classmethod
    def _profile(
        cls, dataset: Dataset, configuration: Optional[Dict[str, Any]] = None
    ) -> ExpectationSuite:
        """
        Override method, which returns the expectation suite using the UserConfigurable Profiler.
        """
        profiler = (
            UserConfigurableProfiler(profile_dataset=dataset)
            if configuration is None
            else UserConfigurableProfiler(profile_dataset=dataset, **configuration)
        )
        return profiler.build_suite()


@dataclasses.dataclass
class DatahubGEProfiler:
    data_context: BaseDataContext
    report: SourceReport
    config: GEProfilingConfig

    # The actual value doesn't matter, it just matters that we use it consistently throughout.
    datasource_name: str = "my_sqlalchemy_datasource"

    def __init__(self, conn, report, config):
        self.conn = conn
        self.report = report
        self.config = config

        data_context_config = DataContextConfig(
            datasources={
                self.datasource_name: DatasourceConfig(
                    class_name="SqlAlchemyDatasource",
                    credentials={
                        # This isn't actually used since we pass the connection directly,
                        # but GE parses it to change some of its behavior so it's useful
                        # to emulate that here.
                        "url": self.conn.engine.url,
                    },
                )
            },
            store_backend_defaults=InMemoryStoreBackendDefaults(),
            anonymous_usage_statistics={
                "enabled": False,
                # "data_context_id": <not set>,
            },
        )

        with _properly_init_datasource(self.conn):
            self.data_context = BaseDataContext(project_config=data_context_config)

    def generate_profile(
        self,
        pretty_name: str,
        schema: str = None,
        table: str = None,
        **kwargs: Any,
    ) -> Optional[DatasetProfileClass]:
        with _properly_init_datasource(self.conn):
            evrs = self._profile_data_asset(
                batch_kwargs={
                    "datasource": self.datasource_name,
                    "schema": schema,
                    "table": table,
                    "limit": self.config.limit,
                    "offset": self.config.offset,
                    **kwargs,
                },
                pretty_name=pretty_name,
            )

        if evrs is not None:
            return self._convert_evrs_to_profile(
                evrs,
                pretty_name=pretty_name,
                send_sample_values=self.config.get_field_sample_values,
            )

        return None

    def _profile_data_asset(
        self,
        batch_kwargs: dict,
        pretty_name: str,
    ) -> ExpectationSuiteValidationResult:
        try:
            profile_results = self.data_context.profile_data_asset(
                self.datasource_name,
                profiler=DatahubConfigurableProfiler,
                profiler_configuration=DatahubConfigurableProfiler.datahub_config_to_ge_config(
                    self.config
                ),
                batch_kwargs={
                    "datasource": self.datasource_name,
                    **batch_kwargs,
                },
            )

            assert profile_results["success"]

            assert len(profile_results["results"]) == 1
            _suite, evrs = profile_results["results"][0]
            return evrs
        except Exception as e:
            logger.warning(
                f"Encountered exception {e}\nwhile profiling {pretty_name}, {batch_kwargs}"
            )
            return None

    @staticmethod
    def _get_column_from_evr(evr: ExpectationValidationResult) -> Optional[str]:
        return evr.expectation_config.kwargs.get("column")

    # The list of handled expectations has been created by referencing these files:
    # - https://github.com/great-expectations/great_expectations/blob/71e9c1eae433a31416a38de1688e2793e9778299/great_expectations/render/renderer/profiling_results_overview_section_renderer.py
    # - https://github.com/great-expectations/great_expectations/blob/71e9c1eae433a31416a38de1688e2793e9778299/great_expectations/render/renderer/column_section_renderer.py
    # - https://github.com/great-expectations/great_expectations/blob/71e9c1eae433a31416a38de1688e2793e9778299/great_expectations/profile/user_configurable_profiler.py

    def _convert_evrs_to_profile(
        self,
        evrs: ExpectationSuiteValidationResult,
        pretty_name: str,
        send_sample_values: bool,
    ) -> DatasetProfileClass:
        profile = DatasetProfileClass(timestampMillis=get_sys_time())

        for col, evrs_for_col in groupby_unsorted(
            evrs.results, key=self._get_column_from_evr
        ):
            if col is None:
                self._handle_convert_table_evrs(
                    profile, evrs_for_col, pretty_name=pretty_name
                )
            else:
                self._handle_convert_column_evrs(
                    profile,
                    col,
                    evrs_for_col,
                    pretty_name=pretty_name,
                    send_sample_values=send_sample_values,
                )

        return profile

    def _handle_convert_table_evrs(
        self,
        profile: DatasetProfileClass,
        table_evrs: Iterable[ExpectationValidationResult],
        pretty_name: str,
    ) -> None:
        # TRICKY: This method mutates the profile directly.

        for evr in table_evrs:
            exp: str = evr.expectation_config.expectation_type
            res: dict = evr.result

            if exp == "expect_table_row_count_to_be_between":
                profile.rowCount = res["observed_value"]
            elif exp == "expect_table_columns_to_match_ordered_list":
                profile.columnCount = len(res["observed_value"])
            else:
                self.report.report_warning(
                    f"profile of {pretty_name}", f"unknown table mapper {exp}"
                )

    def _handle_convert_column_evrs(  # noqa: C901 (complexity)
        self,
        profile: DatasetProfileClass,
        column: str,
        col_evrs: Iterable[ExpectationValidationResult],
        pretty_name: str,
        send_sample_values: bool,
    ) -> None:
        # TRICKY: This method mutates the profile directly.

        column_profile = DatasetFieldProfileClass(fieldPath=column)

        profile.fieldProfiles = profile.fieldProfiles or []
        profile.fieldProfiles.append(column_profile)

        for evr in col_evrs:
            exp: str = evr.expectation_config.expectation_type
            res: dict = evr.result
            if not res:
                self.report.report_warning(
                    f"profile of {pretty_name}", f"{exp} did not yield any results"
                )
                continue

            if exp == "expect_column_unique_value_count_to_be_between":
                column_profile.uniqueCount = res["observed_value"]
            elif exp == "expect_column_proportion_of_unique_values_to_be_between":
                column_profile.uniqueProportion = res["observed_value"]
            elif exp == "expect_column_values_to_not_be_null":
                column_profile.nullCount = res["unexpected_count"]
                if (
                    "unexpected_percent" in res
                    and res["unexpected_percent"] is not None
                ):
                    column_profile.nullProportion = res["unexpected_percent"] / 100
            elif exp == "expect_column_values_to_not_match_regex":
                # ignore; generally used for whitespace checks using regex r"^\s+|\s+$"
                pass
            elif exp == "expect_column_mean_to_be_between":
                column_profile.mean = str(res["observed_value"])
            elif exp == "expect_column_min_to_be_between":
                column_profile.min = str(res["observed_value"])
            elif exp == "expect_column_max_to_be_between":
                column_profile.max = str(res["observed_value"])
            elif exp == "expect_column_median_to_be_between":
                column_profile.median = str(res["observed_value"])
            elif exp == "expect_column_stdev_to_be_between":
                column_profile.stdev = str(res["observed_value"])
            elif exp == "expect_column_quantile_values_to_be_between":
                if "observed_value" in res:
                    column_profile.quantiles = [
                        QuantileClass(quantile=str(quantile), value=str(value))
                        for quantile, value in zip(
                            res["observed_value"]["quantiles"],
                            res["observed_value"]["values"],
                        )
                    ]
            elif exp == "expect_column_values_to_be_in_set":
                column_profile.sampleValues = [
                    str(v) for v in res["partial_unexpected_list"]
                ]
                if not send_sample_values:
                    column_profile.sampleValues = []
            elif exp == "expect_column_kl_divergence_to_be_less_than":
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
            elif exp == "expect_column_distinct_values_to_be_in_set":
                if "details" in res and "value_counts" in res["details"]:
                    # This can be used to produce a bar chart since it includes values and frequencies.
                    # As such, it is handled differently from expect_column_values_to_be_in_set, which
                    # is nonexhaustive.
                    column_profile.distinctValueFrequencies = [
                        ValueFrequencyClass(value=str(value), frequency=count)
                        for value, count in res["details"]["value_counts"].items()
                    ]
                    if not send_sample_values:
                        column_profile.distinctValueFrequencies = []
            elif exp == "expect_column_values_to_be_in_type_list":
                # ignore; we already know the types for each column via ingestion
                pass
            elif exp == "expect_column_values_to_be_unique":
                # ignore; this is generally covered by the unique value count test
                pass
            else:
                self.report.report_warning(
                    f"profile of {pretty_name}",
                    f"warning: unknown column mapper {exp} in col {column}",
                )
