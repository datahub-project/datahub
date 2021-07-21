import dataclasses
from typing import Iterable, List, Optional, Tuple, Union

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

from datahub.utilities.groupby import groupby_unsorted

NumericColumnValue = Union[int, float]
ColumnValue = Union[int, float, str]


@dataclasses.dataclass
class DatasetFieldProfile:
    fieldPath: str

    uniqueCount: Optional[int] = None
    uniqueProportion: Optional[float] = None

    nullCount: Optional[int] = None
    nullProportion: Optional[float] = None

    min: Optional[ColumnValue] = None
    max: Optional[ColumnValue] = None
    mean: Optional[NumericColumnValue] = None
    median: Optional[NumericColumnValue] = None
    stdev: Optional[NumericColumnValue] = None

    quantiles: Optional[
        List[Tuple[float, NumericColumnValue]]
    ] = None  # render as a table or boxplot: list of (quantile, value) e.g. (.95, 50403) for 95th percentile
    distinct_value_frequencies: Optional[
        List[Tuple[ColumnValue, int]]
    ] = None  # render as a bar chart
    histogram: Optional[
        Tuple[List[NumericColumnValue], List[float]]
    ] = None  # render as a histogram: (k bin boundaries, k+1 frequencies); first+last frequency values are tail weights; sum(frequencies) = 1
    partial_example_values: Optional[List[ColumnValue]] = None  # render as a list


@dataclasses.dataclass
class DatasetProfile:
    rowCount: Optional[int] = None
    columnCount: Optional[int] = None
    fieldProfiles: Optional[List[DatasetFieldProfile]] = None


@dataclasses.dataclass
class DatahubGEProfiler:
    sqlalchemy_uri: str
    sqlalchemy_options: dict

    data_context: BaseDataContext = dataclasses.field(init=False)

    # The actual value doesn't matter, it just matters that we use it consistently throughout.
    datasource_name: str = "my_sqlalchemy_datasource"

    def __post_init__(self):
        self.data_context = self._make_data_context(
            self.sqlalchemy_uri, self.sqlalchemy_options
        )

    def _make_data_context(
        self, sqlalchemy_uri: str, sqlalchemy_options: dict
    ) -> BaseDataContext:
        data_context_config = DataContextConfig(
            datasources={
                self.datasource_name: DatasourceConfig(
                    class_name="SqlAlchemyDatasource",
                    credentials={
                        "url": sqlalchemy_uri,
                        **sqlalchemy_options,
                    },
                )
            },
            store_backend_defaults=InMemoryStoreBackendDefaults(),
            anonymous_usage_statistics={
                "enabled": False,
                # "data_context_id": <not set>,
            },
        )

        context = BaseDataContext(project_config=data_context_config)
        return context

    def generate_profile(self, schema: str, table: str) -> DatasetProfile:
        evrs = self._profile_data_asset(
            {
                "schema": schema,
                "table": table,
            }
        )

        profile = self._convert_evrs_to_profile(evrs)

        return profile

    def _profile_data_asset(
        self, batch_kwargs: dict
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
        self, evrs: ExpectationSuiteValidationResult
    ) -> DatasetProfile:
        profile = DatasetProfile()

        for col, evrs_for_col in groupby_unsorted(
            evrs.results, key=self._get_column_from_evr
        ):
            if col is None:
                self._handle_convert_table_evrs(profile, evrs_for_col)
            else:
                self._handle_convert_column_evrs(profile, col, evrs_for_col)

        return profile

    def _handle_convert_table_evrs(
        self, profile: DatasetProfile, table_evrs: Iterable[ExpectationValidationResult]
    ) -> None:
        # This method mutates the profile directly.

        for evr in table_evrs:
            exp: str = evr.expectation_config.expectation_type
            res: dict = evr.result

            if exp == "expect_table_row_count_to_be_between":
                profile.rowCount = res["observed_value"]
            elif exp == "expect_table_columns_to_match_ordered_list":
                profile.columnCount = len(res["observed_value"])
            else:
                print(f"warning: unknown table mapper {exp}")

    def _handle_convert_column_evrs(  # noqa: C901 (complexity)
        self,
        profile: DatasetProfile,
        column: str,
        col_evrs: Iterable[ExpectationValidationResult],
    ) -> None:
        # This method mutates the profile directly.
        column_profile = DatasetFieldProfile(fieldPath=column)

        profile.fieldProfiles = profile.fieldProfiles or []
        profile.fieldProfiles.append(column_profile)

        for evr in col_evrs:
            exp: str = evr.expectation_config.expectation_type
            res: dict = evr.result

            if exp == "expect_column_unique_value_count_to_be_between":
                column_profile.uniqueCount = res["observed_value"]
            elif exp == "expect_column_proportion_of_unique_values_to_be_between":
                column_profile.uniqueProportion = res["observed_value"]
            elif exp == "expect_column_values_to_not_be_null":
                column_profile.nullCount = res["unexpected_count"]
                column_profile.nullProportion = res["unexpected_percent"]
            elif exp == "expect_column_values_to_not_match_regex":
                # ignore; generally used for whitespace checks using regex r"^\s+|\s+$"
                pass
            elif exp == "expect_column_mean_to_be_between":
                column_profile.mean = res["observed_value"]
            elif exp == "expect_column_min_to_be_between":
                column_profile.min = res["observed_value"]
            elif exp == "expect_column_max_to_be_between":
                column_profile.max = res["observed_value"]
            elif exp == "expect_column_median_to_be_between":
                column_profile.median = res["observed_value"]
            elif exp == "expect_column_stdev_to_be_between":
                column_profile.stdev = res["observed_value"]
            elif exp == "expect_column_quantile_values_to_be_between":
                column_profile.quantiles = list(
                    zip(
                        res["observed_value"]["quantiles"],
                        res["observed_value"]["values"],
                    )
                )
            elif exp == "expect_column_values_to_be_in_set":
                column_profile.partial_example_values = res["partial_unexpected_list"]
            elif exp == "expect_column_kl_divergence_to_be_less_than":
                partition = res["details"]["observed_partition"]
                column_profile.histogram = (
                    partition["bins"],
                    [
                        partition["tail_weights"][0],
                        *partition["weights"],
                        partition["tail_weights"][1],
                    ],
                )
            elif exp == "expect_column_distinct_values_to_be_in_set":
                # This can be used to produce a bar chart since it includes values and frequencies.
                # As such, it is handled differently from expect_column_values_to_be_in_set, which
                # is nonexhaustive.
                column_profile.distinct_value_frequencies = list(
                    res["details"]["value_counts"].items()
                )
            elif exp == "expect_column_values_to_be_in_type_list":
                # ignore; we already know the types for each column via ingestion
                pass
            elif exp == "expect_column_values_to_be_unique":
                # ignore; this is generally covered by the unique value count test
                pass
            else:
                print(f"warning: unknown column mapper {exp} in col {column}")
