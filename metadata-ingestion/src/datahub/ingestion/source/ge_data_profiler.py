import concurrent.futures
import contextlib
import dataclasses
import logging
import threading
import time
import unittest.mock
import uuid
from typing import Any, Iterable, Iterator, List, Optional, Tuple, Union

from great_expectations.core.expectation_validation_result import (
    ExpectationSuiteValidationResult,
    ExpectationValidationResult,
)
from great_expectations.data_context import BaseDataContext
from great_expectations.data_context.types.base import (
    DataContextConfig,
    DatasourceConfig,
    InMemoryStoreBackendDefaults,
    datasourceConfigSchema,
)
from great_expectations.datasource.sqlalchemy_datasource import SqlAlchemyDatasource
from sqlalchemy.engine import Connection, Engine

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
# it from the cached config object to avoid those same copying mechanisms.
#
# We need to wrap this mechanism with a lock, since having multiple methods
# simultaneously patching the same methods will cause concurrency issues.

_properly_init_datasource_lock = threading.Lock()


@contextlib.contextmanager
def _properly_init_datasource(conn):
    with _properly_init_datasource_lock:
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

    send_sample_values: bool


@dataclasses.dataclass
class GEContext:
    data_context: BaseDataContext
    datasource_name: str


@dataclasses.dataclass
class DatahubGEProfiler:
    report: SourceReport

    # The actual value doesn't matter, it just matters that we use it consistently throughout.
    _datasource_name_base: str = "my_sqlalchemy_datasource"

    def __init__(self, conn: Union[Engine, Connection], report: SourceReport):
        self.base_engine = conn
        self.report = report

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
            with _properly_init_datasource(conn):
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
    ) -> Iterable[Tuple[GEProfilerRequest, DatasetProfileClass]]:
        start_time = time.perf_counter()

        max_workers = min(max_workers, len(requests))
        logger.info(
            f"Will profile {len(requests)} table(s) with {max_workers} worker(s)"
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

        end_time = time.perf_counter()
        logger.info(
            f"Profiling {len(requests)} table(s) finished in {end_time - start_time} seconds"
        )

    def generate_profile_from_request(
        self, request: GEProfilerRequest
    ) -> Tuple[GEProfilerRequest, DatasetProfileClass]:
        return request, self.generate_profile(
            request.pretty_name,
            **request.batch_kwargs,
            send_sample_values=request.send_sample_values,
        )

    def generate_profile(
        self,
        pretty_name: str,
        schema: str = None,
        table: str = None,
        limit: int = None,
        offset: int = None,
        send_sample_values: bool = True,
        **kwargs: Any,
    ) -> DatasetProfileClass:
        with self._ge_context() as ge_context:
            logger.info(f"Profiling {pretty_name} (this may take a while)")

            evrs = self._profile_data_asset(
                ge_context,
                {
                    "schema": schema,
                    "table": table,
                    "limit": limit,
                    "offset": offset,
                    **kwargs,
                },
                pretty_name=pretty_name,
            )

            profile = self._convert_evrs_to_profile(
                evrs, pretty_name=pretty_name, send_sample_values=send_sample_values
            )
            logger.debug(f"Finished profiling {pretty_name}")

            return profile

    def _profile_data_asset(
        self,
        ge_context: GEContext,
        batch_kwargs: dict,
        pretty_name: str,
    ) -> ExpectationSuiteValidationResult:
        # Internally, this uses the GE dataset profiler:
        # great_expectations.profile.basic_dataset_profiler.BasicDatasetProfiler

        profile_results = ge_context.data_context.profile_data_asset(
            ge_context.datasource_name,
            batch_kwargs={
                "datasource": ge_context.datasource_name,
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
