import logging
from abc import abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Dict, Iterable, List, Optional, Union, cast

from sqlalchemy import create_engine, inspect
from sqlalchemy.engine.reflection import Inspector

from datahub.emitter.mce_builder import make_dataset_urn_with_platform_instance
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.ge_data_profiler import (
    DatahubGEProfiler,
    GEProfilerRequest,
)
from datahub.ingestion.source.sql.sql_common import SQLSourceReport
from datahub.ingestion.source.sql.sql_config import SQLCommonConfig
from datahub.ingestion.source.sql.sql_generic import BaseTable, BaseView
from datahub.ingestion.source.sql.sql_utils import check_table_with_profile_pattern
from datahub.ingestion.source.state.profiling_state_handler import ProfilingHandler
from datahub.metadata.com.linkedin.pegasus2avro.dataset import DatasetProfile
from datahub.metadata.com.linkedin.pegasus2avro.timeseries import PartitionType
from datahub.utilities.stats_collections import TopKDict, int_top_k_dict


@dataclass
class DetailedProfilerReportMixin:
    profiling_skipped_not_updated: TopKDict[str, int] = field(
        default_factory=int_top_k_dict
    )
    profiling_skipped_size_limit: TopKDict[str, int] = field(
        default_factory=int_top_k_dict
    )

    profiling_skipped_row_limit: TopKDict[str, int] = field(
        default_factory=int_top_k_dict
    )

    profiling_skipped_table_profile_pattern: TopKDict[str, int] = field(
        default_factory=int_top_k_dict
    )

    profiling_skipped_other: TopKDict[str, int] = field(default_factory=int_top_k_dict)

    num_tables_not_eligible_profiling: Dict[str, int] = field(
        default_factory=int_top_k_dict
    )


class ProfilingSqlReport(DetailedProfilerReportMixin, SQLSourceReport):
    pass


@dataclass
class TableProfilerRequest(GEProfilerRequest):
    table: Union[BaseTable, BaseView]
    profile_table_level_only: bool = False


logger = logging.getLogger(__name__)


class GenericProfiler:
    def __init__(
        self,
        config: SQLCommonConfig,
        report: ProfilingSqlReport,
        platform: str,
        state_handler: Optional[ProfilingHandler] = None,
    ) -> None:
        self.config = config
        self.report = report
        self.platform = platform
        self.state_handler = state_handler

    def generate_profile_workunits(
        self,
        requests: List[TableProfilerRequest],
        *,
        max_workers: int,
        db_name: Optional[str] = None,
        platform: Optional[str] = None,
        profiler_args: Optional[Dict] = None,
    ) -> Iterable[MetadataWorkUnit]:
        ge_profile_requests: List[GEProfilerRequest] = [
            cast(GEProfilerRequest, request)
            for request in requests
            if not request.profile_table_level_only
        ]
        table_level_profile_requests: List[TableProfilerRequest] = [
            request for request in requests if request.profile_table_level_only
        ]
        for request in table_level_profile_requests:
            table_level_profile = DatasetProfile(
                timestampMillis=int(datetime.now().timestamp() * 1000),
                columnCount=request.table.column_count,
                rowCount=request.table.rows_count,
                sizeInBytes=request.table.size_in_bytes,
            )
            dataset_urn = self.dataset_urn_builder(request.pretty_name)
            yield MetadataChangeProposalWrapper(
                entityUrn=dataset_urn, aspect=table_level_profile
            ).as_workunit()

        if not ge_profile_requests:
            return

        # Otherwise, if column level profiling is enabled, use  GE profiler.
        ge_profiler = self.get_profiler_instance(db_name)

        for ge_profiler_request, profile in ge_profiler.generate_profiles(
            ge_profile_requests, max_workers, platform, profiler_args
        ):
            if profile is None:
                continue

            request = cast(TableProfilerRequest, ge_profiler_request)
            profile.sizeInBytes = request.table.size_in_bytes

            # If table is partitioned we profile only one partition (if nothing set then the last one)
            # but for table level we can use the rows_count from the table metadata
            # This way even though column statistics only reflects one partition data but the rows count
            # shows the proper count.
            if (
                profile.partitionSpec
                and profile.partitionSpec.type != PartitionType.FULL_TABLE
            ):
                profile.rowCount = request.table.rows_count

            dataset_urn = self.dataset_urn_builder(request.pretty_name)

            # We don't add to the profiler state if we only do table level profiling as it always happens
            if self.state_handler:
                self.state_handler.add_to_state(
                    dataset_urn, int(datetime.now().timestamp() * 1000)
                )
            yield MetadataChangeProposalWrapper(
                entityUrn=dataset_urn, aspect=profile
            ).as_workunit()

    def dataset_urn_builder(self, dataset_name: str) -> str:
        return make_dataset_urn_with_platform_instance(
            self.platform,
            dataset_name,
            self.config.platform_instance,
            self.config.env,
        )

    @abstractmethod
    def get_dataset_name(self, table_name: str, schema_name: str, db_name: str) -> str:
        pass

    def get_profile_request(
        self, table: BaseTable, schema_name: str, db_name: str
    ) -> Optional[TableProfilerRequest]:
        skip_profiling = False
        profile_table_level_only = self.config.profiling.profile_table_level_only
        dataset_name = self.get_dataset_name(table.name, schema_name, db_name)
        if not self.is_dataset_eligible_for_profiling(
            dataset_name,
            last_altered=table.last_altered,
            size_in_bytes=table.size_in_bytes,
            rows_count=table.rows_count,
        ):
            logger.debug(
                f"Dataset {dataset_name} was not eligible for profiling due to last_altered, size in bytes or count of rows limit"
            )
            # Profile only table level if dataset is filtered from profiling
            # due to size limits alone
            if self.is_dataset_eligible_for_profiling(
                dataset_name,
                last_altered=table.last_altered,
                size_in_bytes=None,
                rows_count=None,
            ):
                profile_table_level_only = True
            else:
                skip_profiling = True
                self.report.num_tables_not_eligible_profiling[
                    f"{db_name}.{schema_name}"
                ] += 1

        if table.column_count == 0:
            skip_profiling = True

        if skip_profiling:
            if self.config.profiling.report_dropped_profiles:
                self.report.report_dropped(f"profile of {dataset_name}")
            return None

        logger.debug(f"Preparing profiling request for {dataset_name}")
        profile_request = TableProfilerRequest(
            pretty_name=dataset_name,
            batch_kwargs=self.get_batch_kwargs(table, schema_name, db_name),
            table=table,
            profile_table_level_only=profile_table_level_only,
        )
        return profile_request

    def get_batch_kwargs(
        self, table: BaseTable, schema_name: str, db_name: str
    ) -> dict:
        return dict(
            schema=schema_name,
            table=table.name,
        )

    def get_inspectors(self) -> Iterable[Inspector]:
        # This method can be overridden in the case that you want to dynamically
        # run on multiple databases.

        url = self.config.get_sql_alchemy_url()
        logger.debug(f"sql_alchemy_url={url}")
        engine = create_engine(url, **self.config.options)
        with engine.connect() as conn:
            inspector = inspect(conn)
            yield inspector

    def get_profiler_instance(
        self, db_name: Optional[str] = None
    ) -> "DatahubGEProfiler":
        logger.debug(f"Getting profiler instance from {self.platform}")
        url = self.config.get_sql_alchemy_url()

        logger.debug(f"sql_alchemy_url={url}")

        engine = create_engine(url, **self.config.options)
        with engine.connect() as conn:
            inspector = inspect(conn)

        return DatahubGEProfiler(
            conn=inspector.bind,
            report=self.report,
            config=self.config.profiling,
            platform=self.platform,
            env=self.config.env,
        )

    def is_dataset_eligible_for_profiling(
        self,
        dataset_name: str,
        *,
        last_altered: Optional[datetime] = None,
        size_in_bytes: Optional[int] = None,
        rows_count: Optional[int] = None,
    ) -> bool:
        dataset_urn = make_dataset_urn_with_platform_instance(
            self.platform,
            dataset_name,
            self.config.platform_instance,
            self.config.env,
        )

        if not self.config.table_pattern.allowed(dataset_name):
            logger.debug(
                f"Table {dataset_name} is not allowed for profiling due to table pattern"
            )
            return False

        last_profiled: Optional[int] = None
        if self.state_handler:
            last_profiled = self.state_handler.get_last_profiled(dataset_urn)
            if last_profiled:
                # If profiling state exists we have to carry over to the new state
                self.state_handler.add_to_state(dataset_urn, last_profiled)

        threshold_time: Optional[datetime] = (
            datetime.fromtimestamp(last_profiled / 1000, timezone.utc)
            if last_profiled
            else None
        )
        if (
            not threshold_time
            and self.config.profiling.profile_if_updated_since_days is not None
        ):
            threshold_time = datetime.now(timezone.utc) - timedelta(
                self.config.profiling.profile_if_updated_since_days
            )
        schema_name = dataset_name.rsplit(".", 1)[0]

        if not check_table_with_profile_pattern(
            self.config.profile_pattern, dataset_name
        ):
            self.report.profiling_skipped_table_profile_pattern[schema_name] += 1
            logger.debug(
                f"Table {dataset_name} is not allowed for profiling due to profile pattern"
            )
            return False

        if (threshold_time is not None) and (
            last_altered is not None and last_altered < threshold_time
        ):
            self.report.profiling_skipped_not_updated[schema_name] += 1
            logger.debug(
                f"Table {dataset_name} was skipped because it was not updated recently enough"
            )
            return False

        if self.config.profiling.profile_table_size_limit is not None and (
            size_in_bytes is not None
            and size_in_bytes / (2**30)
            > self.config.profiling.profile_table_size_limit
        ):
            self.report.profiling_skipped_size_limit[schema_name] += 1
            logger.debug(
                f"Table {dataset_name} is not allowed for profiling due to size limit"
            )
            return False

        if self.config.profiling.profile_table_row_limit is not None and (
            rows_count is not None
            and rows_count > self.config.profiling.profile_table_row_limit
        ):
            self.report.profiling_skipped_row_limit[schema_name] += 1
            logger.debug(
                f"Table {dataset_name} is not allowed for profiling due to row limit"
            )
            return False

        return True

    def get_profile_args(self) -> Dict:
        """Passed down to GE profiler"""
        return {}
