from datahub.utilities._markupsafe_compat import MARKUPSAFE_PATCHED

import collections
import concurrent.futures
import contextlib
import dataclasses
import functools
import json
import logging
import re
import threading
import traceback
import unittest.mock
import uuid
from functools import lru_cache
from typing import (
    TYPE_CHECKING,
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
import sqlalchemy.sql.compiler
from great_expectations.core.profiler_types_mapping import ProfilerTypeMapping
from great_expectations.core.util import convert_to_json_serializable
from great_expectations.data_context import AbstractDataContext, BaseDataContext
from great_expectations.data_context.types.base import (
    DataContextConfig,
    DatasourceConfig,
    InMemoryStoreBackendDefaults,
    datasourceConfigSchema,
)
from great_expectations.dataset.dataset import Dataset
from great_expectations.dataset.sqlalchemy_dataset import SqlAlchemyDataset
from great_expectations.datasource.sqlalchemy_datasource import SqlAlchemyDatasource
from great_expectations.execution_engine.sqlalchemy_dialect import GXSqlDialect
from great_expectations.profile.base import ProfilerDataType
from great_expectations.profile.basic_dataset_profiler import BasicDatasetProfilerBase
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.exc import ProgrammingError
from typing_extensions import Concatenate, ParamSpec

from datahub.emitter import mce_builder
from datahub.emitter.mce_builder import get_sys_time
from datahub.ingestion.graph.client import get_default_graph
from datahub.ingestion.source.ge_profiling_config import GEProfilingConfig
from datahub.ingestion.source.profiling.common import (
    Cardinality,
    convert_to_cardinality,
)
from datahub.ingestion.source.sql.sql_report import SQLSourceReport
from datahub.ingestion.source.sql.sql_types import resolve_sql_type
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    EditableSchemaMetadata,
    NumberType,
)
from datahub.metadata.schema_classes import (
    DatasetFieldProfileClass,
    DatasetProfileClass,
    HistogramClass,
    PartitionSpecClass,
    PartitionTypeClass,
    QuantileClass,
    ValueFrequencyClass,
)
from datahub.telemetry import stats, telemetry
from datahub.utilities.perf_timer import PerfTimer
from datahub.utilities.sqlalchemy_query_combiner import (
    IS_SQLALCHEMY_1_4,
    SQLAlchemyQueryCombiner,
    get_query_columns,
)

if TYPE_CHECKING:
    from pyathena.cursor import Cursor

assert MARKUPSAFE_PATCHED
logger: logging.Logger = logging.getLogger(__name__)

_original_get_column_median = SqlAlchemyDataset.get_column_median

P = ParamSpec("P")
POSTGRESQL = "postgresql"
MYSQL = "mysql"
SNOWFLAKE = "snowflake"
BIGQUERY = "bigquery"
REDSHIFT = "redshift"
DATABRICKS = "databricks"
TRINO = "trino"

# Type names for Databricks, to match Title Case types in sqlalchemy
ProfilerTypeMapping.INT_TYPE_NAMES.append("Integer")
ProfilerTypeMapping.INT_TYPE_NAMES.append("SmallInteger")
ProfilerTypeMapping.INT_TYPE_NAMES.append("BigInteger")
ProfilerTypeMapping.FLOAT_TYPE_NAMES.append("Float")
ProfilerTypeMapping.FLOAT_TYPE_NAMES.append("Numeric")
ProfilerTypeMapping.STRING_TYPE_NAMES.append("String")
ProfilerTypeMapping.STRING_TYPE_NAMES.append("Text")
ProfilerTypeMapping.STRING_TYPE_NAMES.append("Unicode")
ProfilerTypeMapping.STRING_TYPE_NAMES.append("UnicodeText")
ProfilerTypeMapping.BOOLEAN_TYPE_NAMES.append("Boolean")
ProfilerTypeMapping.DATETIME_TYPE_NAMES.append("Date")
ProfilerTypeMapping.DATETIME_TYPE_NAMES.append("DateTime")
ProfilerTypeMapping.DATETIME_TYPE_NAMES.append("Time")
ProfilerTypeMapping.DATETIME_TYPE_NAMES.append("Interval")
ProfilerTypeMapping.BINARY_TYPE_NAMES.append("LargeBinary")

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

NORMALIZE_TYPE_PATTERN = re.compile(r"^(.*?)(?:[\[<(].*)?$")


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


def get_column_unique_count_dh_patch(self: SqlAlchemyDataset, column: str) -> int:
    if self.engine.dialect.name.lower() == REDSHIFT:
        element_values = self.engine.execute(
            sa.select(
                [
                    # We use coalesce here to force SQL Alchemy to see this
                    # as a column expression.
                    sa.func.coalesce(
                        sa.text(f'APPROXIMATE count(distinct "{column}")')
                    ),
                ]
            ).select_from(self._table)
        )
        return convert_to_json_serializable(element_values.fetchone()[0])
    elif self.engine.dialect.name.lower() == BIGQUERY:
        element_values = self.engine.execute(
            sa.select(sa.func.APPROX_COUNT_DISTINCT(sa.column(column))).select_from(
                self._table
            )
        )
        return convert_to_json_serializable(element_values.fetchone()[0])
    elif self.engine.dialect.name.lower() == SNOWFLAKE:
        element_values = self.engine.execute(
            sa.select(sa.func.APPROX_COUNT_DISTINCT(sa.column(column))).select_from(
                self._table
            )
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


def _get_column_quantiles_awsathena_patch(  # type:ignore
    self, column: str, quantiles: Iterable
) -> list:
    import ast

    table_name = ".".join(
        [f'"{table_part}"' for table_part in str(self._table).split(".")]
    )

    quantiles_list = list(quantiles)
    quantiles_query = (
        f"SELECT approx_percentile({column}, ARRAY{str(quantiles_list)}) as quantiles "
        f"from (SELECT {column} from {table_name})"
    )
    try:
        quantiles_results = self.engine.execute(quantiles_query).fetchone()[0]
        quantiles_results_list = ast.literal_eval(quantiles_results)
        return quantiles_results_list

    except ProgrammingError as pe:
        self._treat_quantiles_exception(pe)
        return []


def _get_column_median_patch(self, column):
    # AWS Athena and presto have an special function that can be used to retrieve the median
    if (
        self.sql_engine_dialect.name.lower() == GXSqlDialect.AWSATHENA
        or self.sql_engine_dialect.name.lower() == GXSqlDialect.TRINO
    ):
        table_name = ".".join(
            [f'"{table_part}"' for table_part in str(self._table).split(".")]
        )
        element_values = self.engine.execute(
            f"SELECT approx_percentile({column},  0.5) FROM {table_name}"
        )
        return convert_to_json_serializable(element_values.fetchone()[0])
    else:
        return _original_get_column_median(self, column)


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

    FIRST_PARTY_SINGLE_ROW_QUERY_METHODS = {
        "get_column_unique_count_dh_patch",
    }

    # We'll do this the inefficient way since the arrays are pretty small.
    stack = traceback.extract_stack()
    for frame in reversed(stack):
        if frame.name in FIRST_PARTY_SINGLE_ROW_QUERY_METHODS:
            return True

        if not any(frame.filename.endswith(file) for file in SINGLE_ROW_QUERY_FILES):
            continue

        if frame.name in UNPREDICTABLE_ROW_QUERY_METHODS:
            return False
        if frame.name in UNHANDLEABLE_ROW_QUERY_METHODS:
            return False
        if frame.name in SINGLE_ROW_QUERY_METHODS:
            return True
        if frame.name in CONSTANT_ROW_QUERY_METHODS:
            # TODO: figure out how to handle these. A cross join will return (`constant` ** `queries`) rows rather
            #  than `constant` rows with `queries` columns. See
            #  https://stackoverflow.com/questions/35638753/create-query-to-join-2-tables-1-on-1-with-nothing-in-common.
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
    method: Callable[Concatenate["_SingleDatasetProfiler", P], None],
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
    dataset: SqlAlchemyDataset
    dataset_name: str
    partition: Optional[str]
    config: GEProfilingConfig
    report: SQLSourceReport
    custom_sql: Optional[str]

    query_combiner: SQLAlchemyQueryCombiner

    platform: str
    env: str

    column_types: Dict[str, str] = dataclasses.field(default_factory=dict)

    def _get_columns_to_profile(self) -> List[str]:
        if not self.config.any_field_level_metrics_enabled():
            return []

        # Compute columns to profile
        columns_to_profile: List[str] = []

        # Compute ignored columns
        ignored_columns_by_pattern: List[str] = []
        ignored_columns_by_type: List[str] = []

        for col_dict in self.dataset.columns:
            col = col_dict["name"]
            self.column_types[col] = str(col_dict["type"])
            # We expect the allow/deny patterns to specify '<table_pattern>.<column_pattern>'
            if not self.config._allow_deny_patterns.allowed(
                f"{self.dataset_name}.{col}"
            ):
                ignored_columns_by_pattern.append(col)
            # We try to ignore nested columns as well
            elif not self.config.profile_nested_fields and "." in col:
                ignored_columns_by_pattern.append(col)
            elif col_dict.get("type") and self._should_ignore_column(col_dict["type"]):
                ignored_columns_by_type.append(col)
            else:
                columns_to_profile.append(col)

        if ignored_columns_by_pattern:
            self.report.report_dropped(
                f"The profile of columns by pattern {self.dataset_name}({', '.join(sorted(ignored_columns_by_pattern))})"
            )
        if ignored_columns_by_type:
            self.report.report_dropped(
                f"The profile of columns by type {self.dataset_name}({', '.join(sorted(ignored_columns_by_type))})"
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

    def _should_ignore_column(self, sqlalchemy_type: sa.types.TypeEngine) -> bool:
        # We don't profiles columns with None types
        if str(sqlalchemy_type) == "NULL":
            return True

        sql_type = str(sqlalchemy_type)

        match = re.match(NORMALIZE_TYPE_PATTERN, sql_type)

        if match:
            sql_type = match.group(1)

        return sql_type in _get_column_types_to_ignore(self.dataset.engine.dialect.name)

    @_run_with_query_combiner
    def _get_column_type(self, column_spec: _SingleColumnSpec, column: str) -> None:
        column_spec.type_ = BasicDatasetProfilerBase._get_column_type(
            self.dataset, column
        )

        if column_spec.type_ == ProfilerDataType.UNKNOWN:
            try:
                datahub_field_type = resolve_sql_type(
                    self.column_types[column], self.dataset.engine.dialect.name.lower()
                )
            except Exception as e:
                logger.debug(
                    f"Error resolving sql type {self.column_types[column]}: {e}"
                )
                datahub_field_type = None
            if datahub_field_type is None:
                return
            if isinstance(datahub_field_type, NumberType):
                column_spec.type_ = ProfilerDataType.NUMERIC

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
                title="Profiling: Unable to Calculate Cardinality",
                message="The cardinality for the column will not be accessible",
                context=f"{self.dataset_name}.{column}",
                exc=e,
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
        if self.config.profile_table_row_count_estimate_only:
            dialect_name = self.dataset.engine.dialect.name.lower()
            if dialect_name == POSTGRESQL:
                schema_name = self.dataset_name.split(".")[1]
                table_name = self.dataset_name.split(".")[2]
                logger.debug(
                    f"Getting estimated rowcounts for table:{self.dataset_name}, schema:{schema_name}, table:{table_name}"
                )
                get_estimate_script = sa.text(
                    f"SELECT c.reltuples AS estimate FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE  c.relname = '{table_name}' AND n.nspname = '{schema_name}'"
                )
            elif dialect_name == MYSQL:
                schema_name = self.dataset_name.split(".")[0]
                table_name = self.dataset_name.split(".")[1]
                logger.debug(
                    f"Getting estimated rowcounts for table:{self.dataset_name}, schema:{schema_name}, table:{table_name}"
                )
                get_estimate_script = sa.text(
                    f"SELECT table_rows AS estimate FROM information_schema.tables WHERE table_schema = '{schema_name}' AND table_name = '{table_name}'"
                )
            else:
                logger.debug(
                    f"Dialect {dialect_name} not supported for feature "
                    f"profile_table_row_count_estimate_only. Proceeding with full row count."
                )
                dataset_profile.rowCount = self.dataset.get_row_count()
                return

            dataset_profile.rowCount = int(
                self.dataset.engine.execute(get_estimate_script).scalar()
            )
        else:
            # If the configuration is not set to 'estimate only' mode, we directly obtain the row count from the
            # dataset. However, if an offset or limit is set, we need to adjust how we calculate the row count. This
            # is because applying a limit or offset could potentially skew the row count. For instance, if a limit is
            # set and the actual row count exceeds this limit, the returned row count would incorrectly be the limit
            # value.
            #
            # To address this, if a limit is set, we use the original table name when calculating the row count. This
            # ensures that the row count is based on the original table, not on a view which have limit or offset
            # applied.
            if (self.config.limit or self.config.offset) and not self.custom_sql:
                # We don't want limit and offset to get applied to the row count
                # This is kinda hacky way to do it, but every other way would require major refactoring
                dataset_profile.rowCount = self.dataset.get_row_count(
                    self.dataset_name.split(".")[-1]
                )
            else:
                dataset_profile.rowCount = self.dataset.get_row_count()

    @_run_with_query_combiner
    def _get_dataset_column_min(
        self, column_profile: DatasetFieldProfileClass, column: str
    ) -> None:
        if not self.config.include_field_min_value:
            return
        try:
            column_profile.min = str(self.dataset.get_column_min(column))
        except Exception as e:
            logger.debug(
                f"Caught exception while attempting to get column min for column {column}. {e}"
            )

            self.report.report_warning(
                title="Profiling: Unable to Calculate Min",
                message="The min for the column will not be accessible",
                context=f"{self.dataset_name}.{column}",
                exc=e,
            )

    @_run_with_query_combiner
    def _get_dataset_column_max(
        self, column_profile: DatasetFieldProfileClass, column: str
    ) -> None:
        if not self.config.include_field_max_value:
            return
        try:
            column_profile.max = str(self.dataset.get_column_max(column))
        except Exception as e:
            logger.debug(
                f"Caught exception while attempting to get column max for column {column}. {e}"
            )

            self.report.report_warning(
                title="Profiling: Unable to Calculate Max",
                message="The max for the column will not be accessible",
                context=f"{self.dataset_name}.{column}",
                exc=e,
            )

    @_run_with_query_combiner
    def _get_dataset_column_mean(
        self, column_profile: DatasetFieldProfileClass, column: str
    ) -> None:
        if not self.config.include_field_mean_value:
            return
        try:
            column_profile.mean = str(self.dataset.get_column_mean(column))
        except Exception as e:
            logger.debug(
                f"Caught exception while attempting to get column mean for column {column}. {e}"
            )

            self.report.report_warning(
                title="Profiling: Unable to Calculate Mean",
                message="The mean for the column will not be accessible",
                context=f"{self.dataset_name}.{column}",
                exc=e,
            )

    @_run_with_query_combiner
    def _get_dataset_column_median(
        self, column_profile: DatasetFieldProfileClass, column: str
    ) -> None:
        if not self.config.include_field_median_value:
            return
        try:
            if self.dataset.engine.dialect.name.lower() == SNOWFLAKE:
                column_profile.median = str(
                    self.dataset.engine.execute(
                        sa.select([sa.func.median(sa.column(column))]).select_from(
                            self.dataset._table
                        )
                    ).scalar()
                )
            elif self.dataset.engine.dialect.name.lower() == BIGQUERY:
                column_profile.median = str(
                    self.dataset.engine.execute(
                        sa.select(
                            sa.text(f"approx_quantiles(`{column}`, 2) [OFFSET (1)]")
                        ).select_from(self.dataset._table)
                    ).scalar()
                )
            else:
                column_profile.median = str(self.dataset.get_column_median(column))
        except Exception as e:
            logger.debug(
                f"Caught exception while attempting to get column median for column {column}. {e}"
            )

            self.report.report_warning(
                title="Profiling: Unable to Calculate Medians",
                message="The medians for the column will not be accessible",
                context=f"{self.dataset_name}.{column}",
                exc=e,
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
                title="Profiling: Unable to Calculate Standard Deviation",
                message="The standard deviation for the column will not be accessible",
                context=f"{self.dataset_name}.{column}",
                exc=e,
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
                title="Profiling: Unable to Calculate Quantiles",
                message="The quantiles for the column will not be accessible",
                context=f"{self.dataset_name}.{column}",
                exc=e,
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
                title="Profiling: Unable to Calculate Histogram",
                message="The histogram for the column will not be accessible",
                context=f"{self.dataset_name}.{column}",
                exc=e,
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
                column,
                [],
                result_format={
                    "result_format": "SUMMARY",
                    "partial_unexpected_count": self.config.field_sample_values_limit,
                },
            ).result

            column_profile.sampleValues = [
                str(v) for v in res["partial_unexpected_list"]
            ]
        except Exception as e:
            logger.debug(
                f"Caught exception while attempting to get sample values for column {column}. {e}"
            )

            self.report.report_warning(
                title="Profiling: Unable to Calculate Sample Values",
                message="The sample values for the column will not be accessible",
                context=f"{self.dataset_name}.{column}",
                exc=e,
            )

    def generate_dataset_profile(  # noqa: C901 (complexity)
        self,
    ) -> DatasetProfileClass:
        self.dataset.set_default_expectation_argument(
            "catch_exceptions", self.config.catch_exceptions
        )

        profile = self.init_profile()

        profile.fieldProfiles = []
        self._get_dataset_rows(profile)

        all_columns = self.dataset.get_table_columns()
        profile.columnCount = len(all_columns)
        columns_to_profile = set(self._get_columns_to_profile())

        (
            ignore_table_sampling,
            columns_list_to_ignore_sampling,
        ) = _get_columns_to_ignore_sampling(
            self.dataset_name,
            self.config.tags_to_ignore_sampling,
            self.platform,
            self.env,
        )

        logger.debug(f"profiling {self.dataset_name}: flushing stage 1 queries")
        self.query_combiner.flush()

        assert profile.rowCount is not None
        full_row_count = profile.rowCount

        if self.config.use_sampling and not self.config.limit:
            self.update_dataset_batch_use_sampling(profile)

        # Note that this row count may be different from the full_row_count if we are using sampling.
        row_count: int = profile.rowCount
        if profile.partitionSpec and "SAMPLE" in profile.partitionSpec.partition:
            # Querying exact row count of sample using `_get_dataset_rows`.
            # We are not using `self.config.sample_size` directly as the actual row count
            # in the sample may be different than configured `sample_size`. For BigQuery,
            # we've even seen 160k rows returned for a sample size of 10k.
            logger.debug("Recomputing row count for the sample")

            # Note that we can't just call `self._get_dataset_rows(profile)` here because
            # there's some sort of caching happening that will return the full table row count
            # instead of the sample row count.
            row_count = self.dataset.get_row_count(str(self.dataset._table))

            profile.partitionSpec.partition += f" (sample rows {row_count})"

        columns_profiling_queue: List[_SingleColumnSpec] = []
        if columns_to_profile:
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

            if not profile.rowCount:
                continue

            if (
                not ignore_table_sampling
                and column not in columns_list_to_ignore_sampling
            ):
                self._get_dataset_column_sample_values(column_profile, column)

                if (
                    type_ == ProfilerDataType.INT
                    or type_ == ProfilerDataType.FLOAT
                    or type_ == ProfilerDataType.NUMERIC
                ):
                    self._get_dataset_column_min(column_profile, column)
                    self._get_dataset_column_max(column_profile, column)
                    self._get_dataset_column_mean(column_profile, column)
                    self._get_dataset_column_median(column_profile, column)
                    self._get_dataset_column_stdev(column_profile, column)

                    if cardinality in [
                        Cardinality.ONE,
                        Cardinality.TWO,
                        Cardinality.VERY_FEW,
                    ]:
                        self._get_dataset_column_distinct_value_frequencies(
                            column_profile,
                            column,
                        )
                    if cardinality in {
                        Cardinality.FEW,
                        Cardinality.MANY,
                        Cardinality.VERY_MANY,
                    }:
                        self._get_dataset_column_quantiles(column_profile, column)
                        self._get_dataset_column_histogram(column_profile, column)

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

        # Reset the row count to the original value.
        profile.rowCount = full_row_count

        return profile

    def init_profile(self):
        profile = DatasetProfileClass(timestampMillis=get_sys_time())
        if self.partition:
            profile.partitionSpec = PartitionSpecClass(partition=self.partition)
        elif self.config.limit:
            profile.partitionSpec = PartitionSpecClass(
                type=PartitionTypeClass.QUERY,
                partition=json.dumps(
                    dict(limit=self.config.limit, offset=self.config.offset)
                ),
            )
        elif self.custom_sql:
            profile.partitionSpec = PartitionSpecClass(
                type=PartitionTypeClass.QUERY, partition="SAMPLE"
            )

        return profile

    def update_dataset_batch_use_sampling(self, profile: DatasetProfileClass) -> None:
        if (
            self.dataset.engine.dialect.name.lower() == BIGQUERY
            and profile.rowCount
            and profile.rowCount > self.config.sample_size
        ):
            """
            According to BigQuery Sampling Docs(https://cloud.google.com/bigquery/docs/table-sampling),
            BigQuery does not cache the results of a query that includes a TABLESAMPLE clause and the
            query may return different results every time. Calculating different column level metrics
            on different sampling results is possible however each query execution would incur the cost
            of reading data from storage. Also, using different table samples may create non-coherent
            representation of column level metrics, for example, minimum value of a column in one sample
            can be greater than maximum value of the column in another sample.

            It is observed that for a simple select * query with TABLESAMPLE, results are cached and
            stored in temporary table. This can be (ab)used and all column level profiling calculations
            can be performed against it.

            Risks:
                1. All the risks mentioned in notes of `create_bigquery_temp_table` are also
                applicable here.
                2. TABLESAMPLE query may read entire table for small tables that are written
                as single data block. This may incorrectly label datasetProfile's partition as
                "SAMPLE", although profile is for entire table.
                3. Table Sampling in BigQuery is a Pre-GA (Preview) feature.
            """
            sample_pc = 100 * self.config.sample_size / profile.rowCount
            sql = (
                f"SELECT * FROM {str(self.dataset._table)} "
                + f"TABLESAMPLE SYSTEM ({sample_pc:.8f} percent)"
            )
            temp_table_name = create_bigquery_temp_table(
                self,
                sql,
                self.dataset_name,
                self.dataset.engine.engine.raw_connection(),
            )
            if temp_table_name:
                self.dataset._table = sa.text(temp_table_name)
                logger.debug(f"Setting table name to be {self.dataset._table}")

                if (
                    profile.partitionSpec
                    and profile.partitionSpec.type == PartitionTypeClass.FULL_TABLE
                ):
                    profile.partitionSpec = PartitionSpecClass(
                        type=PartitionTypeClass.QUERY, partition="SAMPLE"
                    )
                elif (
                    profile.partitionSpec
                    and profile.partitionSpec.type == PartitionTypeClass.PARTITION
                ):
                    profile.partitionSpec.partition += " SAMPLE"


@dataclasses.dataclass
class GEContext:
    data_context: AbstractDataContext
    datasource_name: str


@dataclasses.dataclass
class DatahubGEProfiler:
    report: SQLSourceReport
    config: GEProfilingConfig
    times_taken: List[float]
    total_row_count: int

    base_engine: Engine
    platform: str  # passed from parent source config
    env: str

    # The actual value doesn't matter, it just matters that we use it consistently throughout.
    _datasource_name_base: str = "my_sqlalchemy_datasource"

    def __init__(
        self,
        conn: Union[Engine, Connection],
        report: SQLSourceReport,
        config: GEProfilingConfig,
        platform: str,
        env: str = "PROD",
    ):
        self.report = report
        self.config = config
        self.times_taken = []
        self.total_row_count = 0

        self.env = env

        # TRICKY: The call to `.engine` is quite important here. Connection.connect()
        # returns a "branched" connection, which does not actually use a new underlying
        # DB-API object from the connection pool. Engine.connect() does what we want to
        # make the threading code work correctly. As such, we need to make sure we've
        # got an engine here.
        self.base_engine = conn.engine

        if IS_SQLALCHEMY_1_4:
            # SQLAlchemy 1.4 added a statement "linter", which issues warnings about cartesian products in SELECT statements.
            # Changelog: https://docs.sqlalchemy.org/en/14/changelog/migration_14.html#change-4737.
            # Code: https://github.com/sqlalchemy/sqlalchemy/blob/2f91dd79310657814ad28b6ef64f91fff7a007c9/lib/sqlalchemy/sql/compiler.py#L549
            #
            # The query combiner does indeed produce queries with cartesian products, but they are
            # safe because each "FROM" clause only returns one row, so the cartesian product
            # is also always a single row. As such, we disable the linter here.

            # Modified from https://github.com/sqlalchemy/sqlalchemy/blob/2f91dd79310657814ad28b6ef64f91fff7a007c9/lib/sqlalchemy/engine/create.py#L612
            self.base_engine.dialect.compiler_linting &= (  # type: ignore[attr-defined]
                ~sqlalchemy.sql.compiler.COLLECT_CARTESIAN_PRODUCTS  # type: ignore[attr-defined]
            )

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
        max_workers = min(max_workers, len(requests))
        logger.info(
            f"Will profile {len(requests)} table(s) with {max_workers} worker(s) - this may take a while"
        )

        with PerfTimer() as timer, unittest.mock.patch(
            "great_expectations.dataset.sqlalchemy_dataset.SqlAlchemyDataset.get_column_unique_count",
            get_column_unique_count_dh_patch,
        ), unittest.mock.patch(
            "great_expectations.dataset.sqlalchemy_dataset.SqlAlchemyDataset._get_column_quantiles_bigquery",
            _get_column_quantiles_bigquery_patch,
        ), unittest.mock.patch(
            "great_expectations.dataset.sqlalchemy_dataset.SqlAlchemyDataset._get_column_quantiles_awsathena",
            _get_column_quantiles_awsathena_patch,
        ), unittest.mock.patch(
            "great_expectations.dataset.sqlalchemy_dataset.SqlAlchemyDataset.get_column_median",
            _get_column_median_patch,
        ), concurrent.futures.ThreadPoolExecutor(
            max_workers=max_workers
        ) as async_executor, SQLAlchemyQueryCombiner(
            enabled=self.config.query_combiner_enabled,
            catch_exceptions=self.config.catch_exceptions,
            is_single_row_query_method=_is_single_row_query_method,
            serial_execution_fallback_enabled=True,
        ).activate() as query_combiner:
            # Submit the profiling requests to the thread pool executor.
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

    def _drop_temp_table(self, temp_dataset: Dataset) -> None:
        schema = temp_dataset._table.schema
        table = temp_dataset._table.name
        table_name = f'"{schema}"."{table}"' if schema else f'"{table}"'
        try:
            with self.base_engine.connect() as connection:
                connection.execute(f"drop view if exists {table_name}")
                logger.debug(f"View {table_name} was dropped.")
        except Exception:
            logger.warning(f"Unable to delete temporary table: {table_name}")

    def _generate_single_profile(
        self,
        query_combiner: SQLAlchemyQueryCombiner,
        pretty_name: str,
        schema: Optional[str] = None,
        table: Optional[str] = None,
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
        temp_view: Optional[str] = None
        if platform and platform.upper() == "ATHENA" and (custom_sql):
            if custom_sql is not None:
                # Note that limit and offset are not supported for custom SQL.
                temp_view = create_athena_temp_table(
                    self, custom_sql, pretty_name, self.base_engine.raw_connection()
                )
                ge_config["table"] = temp_view
                ge_config["schema"] = None
                ge_config["limit"] = None
                ge_config["offset"] = None
                custom_sql = None

        if platform == BIGQUERY and (
            custom_sql or self.config.limit or self.config.offset
        ):
            if custom_sql is not None:
                # Note that limit and offset are not supported for custom SQL.
                bq_sql = custom_sql
            else:
                bq_sql = f"SELECT * FROM `{table}`"
                if self.config.limit:
                    bq_sql += f" LIMIT {self.config.limit}"
                if self.config.offset:
                    bq_sql += f" OFFSET {self.config.offset}"
            bigquery_temp_table = create_bigquery_temp_table(
                self, bq_sql, pretty_name, self.base_engine.raw_connection()
            )

        if platform == BIGQUERY:
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

        batch = None
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
                    custom_sql,
                    query_combiner,
                    self.platform,
                    self.env,
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

                error_message = str(e).lower()
                if "permission denied" in error_message:
                    self.report.warning(
                        title="Unauthorized to extract data profile statistics",
                        message="We were denied access while attempting to generate profiling statistics for some assets. Please ensure the provided user has permission to query these tables and views.",
                        context=f"Asset: {pretty_name}",
                        exc=e,
                    )
                else:
                    self.report.warning(
                        title="Failed to extract statistics for some assets",
                        message="Caught unexpected exception while attempting to extract profiling statistics for some assets.",
                        context=f"Asset: {pretty_name}",
                        exc=e,
                    )
                return None
            finally:
                if batch is not None and self.base_engine.engine.name.upper() in [
                    "TRINO",
                    "AWSATHENA",
                ]:
                    if (
                        self.base_engine.engine.name.upper() == "TRINO"
                        or temp_view is not None
                    ):
                        self._drop_temp_table(batch)
                    # if we are not on Trino then we only drop table if temp table variable was set

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

        if platform == BIGQUERY or platform == DATABRICKS:
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


# More dialect specific types to ignore can be added here
# Stringified types are used to avoid dialect specific import errors
@lru_cache(maxsize=1)
def _get_column_types_to_ignore(dialect_name: str) -> List[str]:
    if dialect_name.lower() == POSTGRESQL:
        return ["JSON"]
    elif dialect_name.lower() == BIGQUERY:
        return ["ARRAY", "STRUCT", "GEOGRAPHY", "JSON"]

    return []


def create_athena_temp_table(
    instance: Union[DatahubGEProfiler, _SingleDatasetProfiler],
    sql: str,
    table_pretty_name: str,
    raw_connection: Any,
) -> Optional[str]:
    try:
        cursor: "Cursor" = cast("Cursor", raw_connection.cursor())
        logger.debug(f"Creating view for {table_pretty_name}: {sql}")
        temp_view = f"ge_{uuid.uuid4()}"
        if "." in table_pretty_name:
            schema_part = table_pretty_name.split(".")[-1]
            schema_part_quoted = ".".join(
                [f'"{part}"' for part in str(schema_part).split(".")]
            )
            temp_view = f"{schema_part_quoted}_{temp_view}"

        temp_view = f"ge_{uuid.uuid4()}"
        cursor.execute(f'create or replace view "{temp_view}" as {sql}')
    except Exception as e:
        if not instance.config.catch_exceptions:
            raise e
        logger.exception(f"Encountered exception while profiling {table_pretty_name}")
        instance.report.report_warning(
            table_pretty_name,
            f"Profiling exception {e} when running custom sql {sql}",
        )
        return None
    finally:
        raw_connection.close()

    return temp_view


def create_bigquery_temp_table(
    instance: Union[DatahubGEProfiler, _SingleDatasetProfiler],
    bq_sql: str,
    table_pretty_name: str,
    raw_connection: Any,
) -> Optional[str]:
    # On BigQuery, we need to bypass GE's mechanism for creating temporary tables because
    # it requires create/delete table permissions.
    import google.cloud.bigquery.job.query
    from google.cloud.bigquery.dbapi.cursor import Cursor as BigQueryCursor

    try:
        cursor: "BigQueryCursor" = cast("BigQueryCursor", raw_connection.cursor())
        try:
            logger.debug(f"Creating temporary table for {table_pretty_name}: {bq_sql}")
            cursor.execute(bq_sql)
        except Exception as e:
            if not instance.config.catch_exceptions:
                raise e
            logger.exception(
                f"Encountered exception while profiling {table_pretty_name}"
            )
            instance.report.report_warning(
                table_pretty_name,
                f"Profiling exception {e} when running custom sql {bq_sql}",
            )
            return None

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

        query_job: Optional["google.cloud.bigquery.job.query.QueryJob"] = (
            # In google-cloud-bigquery 3.15.0, the _query_job attribute was
            # made public and renamed to query_job.
            cursor.query_job if hasattr(cursor, "query_job") else cursor._query_job  # type: ignore[attr-defined]
        )
        assert query_job
        temp_destination_table = query_job.destination
        bigquery_temp_table = f"{temp_destination_table.project}.{temp_destination_table.dataset_id}.{temp_destination_table.table_id}"
        return bigquery_temp_table
    finally:
        raw_connection.close()


def _get_columns_to_ignore_sampling(
    dataset_name: str, tags_to_ignore: Optional[List[str]], platform: str, env: str
) -> Tuple[bool, List[str]]:
    logger.debug("Collecting columns to ignore for sampling")

    ignore_table: bool = False
    columns_to_ignore: List[str] = []

    if not tags_to_ignore:
        return ignore_table, columns_to_ignore

    dataset_urn = mce_builder.make_dataset_urn(
        name=dataset_name, platform=platform, env=env
    )

    datahub_graph = get_default_graph()

    dataset_tags = datahub_graph.get_tags(dataset_urn)
    if dataset_tags:
        ignore_table = any(
            tag_association.tag.split("urn:li:tag:")[1] in tags_to_ignore
            for tag_association in dataset_tags.tags
        )

    if not ignore_table:
        metadata = datahub_graph.get_aspect(
            entity_urn=dataset_urn, aspect_type=EditableSchemaMetadata
        )

        if metadata:
            for schemaField in metadata.editableSchemaFieldInfo:
                if schemaField.globalTags:
                    columns_to_ignore.extend(
                        schemaField.fieldPath
                        for tag_association in schemaField.globalTags.tags
                        if tag_association.tag.split("urn:li:tag:")[1] in tags_to_ignore
                    )

    return ignore_table, columns_to_ignore
