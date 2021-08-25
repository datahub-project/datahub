import contextlib
import dataclasses
import unittest.mock
from typing import Any, Iterable, Optional

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
from great_expectations.datasource.sqlalchemy_datasource import SqlAlchemyDatasource

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


@dataclasses.dataclass
class DatahubGEProfiler:
    data_context: BaseDataContext
    report: SourceReport

    # The actual value doesn't matter, it just matters that we use it consistently throughout.
    datasource_name: str = "my_sqlalchemy_datasource"

    def __init__(self, conn, report):
        self.conn = conn
        self.report = report

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
        limit: int = None,
        offset: int = None,
        **kwargs: Any,
    ) -> DatasetProfileClass:
        with _properly_init_datasource(self.conn):
            evrs = self._profile_data_asset(
                {
                    "schema": schema,
                    "table": table,
                    "limit": limit,
                    "offset": offset,
                    **kwargs,
                },
                pretty_name=pretty_name,
            )

        profile = self._convert_evrs_to_profile(evrs, pretty_name=pretty_name)

        return profile

    def _profile_data_asset(
        self,
        batch_kwargs: dict,
        pretty_name: str,
    ) -> ExpectationSuiteValidationResult:
        # Internally, this uses the GE dataset profiler:
        # great_expectations.profile.basic_dataset_profiler.BasicDatasetProfiler

        profile_results = self.data_context.profile_data_asset(
            self.datasource_name,
            batch_kwargs={
                "datasource": self.datasource_name,
                **batch_kwargs,
            },
        )
        assert profile_results["success"]

        assert len(profile_results["results"]) == 1
        _suite, evrs = profile_results["results"][0]
        return evrs

    @staticmethod
    def _get_column_from_evr(evr: ExpectationValidationResult) -> Optional[str]:
        return evr.expectation_config.kwargs.get("column")

    # The list of handled expectations has been created by referencing these files:
    # - https://github.com/great-expectations/great_expectations/blob/71e9c1eae433a31416a38de1688e2793e9778299/great_expectations/render/renderer/profiling_results_overview_section_renderer.py
    # - https://github.com/great-expectations/great_expectations/blob/71e9c1eae433a31416a38de1688e2793e9778299/great_expectations/render/renderer/column_section_renderer.py
    # - https://github.com/great-expectations/great_expectations/blob/71e9c1eae433a31416a38de1688e2793e9778299/great_expectations/profile/basic_dataset_profiler.py

    def _convert_evrs_to_profile(
        self, evrs: ExpectationSuiteValidationResult, pretty_name: str
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
                    profile, col, evrs_for_col, pretty_name=pretty_name
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
