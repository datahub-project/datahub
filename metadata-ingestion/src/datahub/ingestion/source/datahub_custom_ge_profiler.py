import logging
from typing import Any, Dict

from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.dataset.dataset import Dataset
from great_expectations.profile.base import ProfilerCardinality, ProfilerDataType
from great_expectations.profile.basic_dataset_profiler import BasicDatasetProfilerBase

logger = logging.getLogger(__name__)


class DatahubGECustomProfiler(BasicDatasetProfilerBase):
    """DatahubGECustomProfiler is the customizable version of of the BasicDatasetProfiler.

    The reason for going this route instead of using UserConfigurableProfiler is UserConfigurableProfiler
    does not compute all of the expectations the BasicDatasetProfiler such as the sample values etc.
    """

    # flake8: noqa: C901
    @classmethod
    def _profile(
        cls,
        dataset: Dataset,
        configuration: Dict[str, Any],
    ) -> ExpectationSuite:
        columns_to_profile = configuration["columns_to_profile"]
        excluded_expectations = configuration["excluded_expectations"]

        df = dataset

        df.set_default_expectation_argument("catch_exceptions", True)

        df.expect_table_row_count_to_be_between(min_value=0, max_value=None)
        df.expect_table_columns_to_match_ordered_list(None)
        df.set_config_value("interactive_evaluation", False)

        meta_columns = {}
        for column in columns_to_profile:
            meta_columns[column] = {"description": ""}

        number_of_columns = len(columns_to_profile)
        for i, column in enumerate(columns_to_profile):
            logger.debug(
                "            Preparing column {} of {}: {}".format(
                    i + 1, number_of_columns, column
                )
            )

            type_ = cls._get_column_type(df, column)
            cardinality = cls._get_column_cardinality(df, column)
            if "expect_column_values_to_not_be_null" not in excluded_expectations:
                df.expect_column_values_to_not_be_null(column, mostly=0.5)

            if "expect_column_values_to_be_in_set" not in excluded_expectations:
                df.expect_column_values_to_be_in_set(
                    column, [], result_format="SUMMARY"
                )

            if type_ == ProfilerDataType.INT:
                if cardinality == ProfilerCardinality.UNIQUE:
                    df.expect_column_values_to_be_unique(column)
                elif cardinality in [
                    ProfilerCardinality.ONE,
                    ProfilerCardinality.TWO,
                    ProfilerCardinality.VERY_FEW,
                    ProfilerCardinality.FEW,
                ] and (
                    "expect_column_distinct_values_to_be_in_set"
                    not in excluded_expectations
                ):
                    df.expect_column_distinct_values_to_be_in_set(
                        column, value_set=None, result_format="SUMMARY"
                    )
                elif cardinality in [
                    ProfilerCardinality.MANY,
                    ProfilerCardinality.VERY_MANY,
                    ProfilerCardinality.UNIQUE,
                ]:
                    if "expect_column_min_to_be_between" not in excluded_expectations:
                        df.expect_column_min_to_be_between(
                            column, min_value=None, max_value=None
                        )
                    if "expect_column_max_to_be_between" not in excluded_expectations:
                        df.expect_column_max_to_be_between(
                            column, min_value=None, max_value=None
                        )
                    if "expect_column_mean_to_be_between" not in excluded_expectations:
                        df.expect_column_mean_to_be_between(
                            column, min_value=None, max_value=None
                        )
                    if (
                        "expect_column_median_to_be_between"
                        not in excluded_expectations
                    ):
                        df.expect_column_median_to_be_between(
                            column, min_value=None, max_value=None
                        )
                    if "expect_column_stdev_to_be_between" not in excluded_expectations:
                        df.expect_column_stdev_to_be_between(
                            column, min_value=None, max_value=None
                        )
                    if (
                        "expect_column_quantile_values_to_be_between"
                        not in excluded_expectations
                    ):
                        df.expect_column_quantile_values_to_be_between(
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
                        )
                    if (
                        "expect_column_kl_divergence_to_be_less_than"
                        not in excluded_expectations
                    ):
                        df.expect_column_kl_divergence_to_be_less_than(
                            column,
                            partition_object=None,
                            threshold=None,
                            result_format="COMPLETE",
                        )
                else:  # unknown cardinality - skip
                    pass
            elif type_ == ProfilerDataType.FLOAT:
                if cardinality == ProfilerCardinality.UNIQUE:
                    df.expect_column_values_to_be_unique(column)

                elif cardinality in [
                    ProfilerCardinality.ONE,
                    ProfilerCardinality.TWO,
                    ProfilerCardinality.VERY_FEW,
                    ProfilerCardinality.FEW,
                ] and (
                    "expect_column_distinct_values_to_be_in_set"
                    not in excluded_expectations
                ):
                    df.expect_column_distinct_values_to_be_in_set(
                        column, value_set=None, result_format="SUMMARY"
                    )

                elif cardinality in [
                    ProfilerCardinality.MANY,
                    ProfilerCardinality.VERY_MANY,
                    ProfilerCardinality.UNIQUE,
                ]:
                    if "expect_column_min_to_be_between" not in excluded_expectations:
                        df.expect_column_min_to_be_between(
                            column, min_value=None, max_value=None
                        )
                    if "expect_column_max_to_be_between" not in excluded_expectations:
                        df.expect_column_max_to_be_between(
                            column, min_value=None, max_value=None
                        )
                    if "expect_column_mean_to_be_between" not in excluded_expectations:
                        df.expect_column_mean_to_be_between(
                            column, min_value=None, max_value=None
                        )
                    if (
                        "expect_column_median_to_be_between"
                        not in excluded_expectations
                    ):
                        df.expect_column_median_to_be_between(
                            column, min_value=None, max_value=None
                        )
                    if (
                        "expect_column_quantile_values_to_be_between"
                        not in excluded_expectations
                    ):
                        df.expect_column_quantile_values_to_be_between(
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
                        )
                    if (
                        "expect_column_kl_divergence_to_be_less_than"
                        not in excluded_expectations
                    ):
                        df.expect_column_kl_divergence_to_be_less_than(
                            column,
                            partition_object=None,
                            threshold=None,
                            result_format="COMPLETE",
                        )
                else:  # unknown cardinality - skip
                    pass

            elif type_ == ProfilerDataType.STRING:
                # Check for leading and trailing whitespace.
                df.expect_column_values_to_not_match_regex(column, r"^\s+|\s+$")

                if cardinality == ProfilerCardinality.UNIQUE:
                    df.expect_column_values_to_be_unique(column)

                elif cardinality in [
                    ProfilerCardinality.ONE,
                    ProfilerCardinality.TWO,
                    ProfilerCardinality.VERY_FEW,
                    ProfilerCardinality.FEW,
                ] and (
                    "expect_column_distinct_values_to_be_in_set"
                    not in excluded_expectations
                ):
                    df.expect_column_distinct_values_to_be_in_set(
                        column, value_set=None, result_format="SUMMARY"
                    )
                else:
                    pass

            elif type_ == ProfilerDataType.DATETIME:

                if "expect_column_min_to_be_between" not in excluded_expectations:
                    df.expect_column_min_to_be_between(
                        column, min_value=None, max_value=None
                    )

                if "expect_column_max_to_be_between" not in excluded_expectations:
                    df.expect_column_max_to_be_between(
                        column, min_value=None, max_value=None
                    )

                # Re-add once kl_divergence has been modified to support datetimes
                # df.expect_column_kl_divergence_to_be_less_than(column, partition_object=None,
                #                                            threshold=None, result_format='COMPLETE')

                if cardinality in [
                    ProfilerCardinality.ONE,
                    ProfilerCardinality.TWO,
                    ProfilerCardinality.VERY_FEW,
                    ProfilerCardinality.FEW,
                ] and (
                    "expect_column_distinct_values_to_be_in_set"
                    not in excluded_expectations
                ):
                    df.expect_column_distinct_values_to_be_in_set(
                        column, value_set=None, result_format="SUMMARY"
                    )

            else:
                if cardinality == ProfilerCardinality.UNIQUE:
                    df.expect_column_values_to_be_unique(column)

                elif cardinality in [
                    ProfilerCardinality.ONE,
                    ProfilerCardinality.TWO,
                    ProfilerCardinality.VERY_FEW,
                    ProfilerCardinality.FEW,
                ] and (
                    "expect_column_distinct_values_to_be_in_set"
                    not in excluded_expectations
                ):
                    df.expect_column_distinct_values_to_be_in_set(
                        column, value_set=None, result_format="SUMMARY"
                    )
                else:
                    pass

        df.set_config_value("interactive_evaluation", True)
        expectation_suite = df.get_expectation_suite(
            suppress_warnings=True, discard_failed_expectations=False
        )
        expectation_suite.meta["columns"] = meta_columns

        return expectation_suite
