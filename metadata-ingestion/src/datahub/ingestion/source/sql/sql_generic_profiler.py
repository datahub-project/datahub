import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Dict, Iterable, List, Optional, Tuple, Union, cast

from sqlalchemy import create_engine, inspect
from sqlalchemy.engine.reflection import Inspector

from datahub.ingestion.source.ge_data_profiler import (
    DatahubGEProfiler,
    GEProfilerRequest,
)
from datahub.ingestion.source.sql.sql_common import SQLAlchemyConfig, SQLSourceReport
from datahub.ingestion.source.sql.sql_generic import BaseTable, BaseView
from datahub.metadata.com.linkedin.pegasus2avro.dataset import DatasetProfile
from datahub.metadata.schema_classes import DatasetProfileClass
from datahub.utilities.stats_collections import TopKDict


@dataclass
class DetailedProfilerReportMixin:
    profiling_skipped_not_updated: TopKDict[str, int] = field(default_factory=TopKDict)
    profiling_skipped_size_limit: TopKDict[str, int] = field(default_factory=TopKDict)

    profiling_skipped_row_limit: TopKDict[str, int] = field(default_factory=TopKDict)
    num_tables_not_eligible_profiling: Dict[str, int] = field(default_factory=TopKDict)


class ProfilingSqlReport(DetailedProfilerReportMixin, SQLSourceReport):
    pass


@dataclass
class TableProfilerRequest(GEProfilerRequest):
    table: Union[BaseTable, BaseView]
    profile_table_level_only: bool = False


logger = logging.getLogger(__name__)


class GenericProfiler:
    def __init__(
        self, config: SQLAlchemyConfig, report: ProfilingSqlReport, platform: str
    ) -> None:
        self.config = config
        self.report = report
        self.platform = platform

    def generate_profiles(
        self,
        requests: List[TableProfilerRequest],
        max_workers: int,
        db_name: Optional[str] = None,
        platform: Optional[str] = None,
        profiler_args: Optional[Dict] = None,
    ) -> Iterable[Tuple[GEProfilerRequest, Optional[DatasetProfileClass]]]:
        ge_profile_requests: List[GEProfilerRequest] = [
            cast(GEProfilerRequest, request)
            for request in requests
            if not request.profile_table_level_only
        ]
        table_level_profile_requests: List[TableProfilerRequest] = [
            request for request in requests if request.profile_table_level_only
        ]
        for request in table_level_profile_requests:
            profile = DatasetProfile(
                timestampMillis=int(datetime.now().timestamp() * 1000),
                columnCount=len(request.table.columns),
                rowCount=request.table.rows_count,
                sizeInBytes=request.table.size_in_bytes,
            )
            yield (request, profile)

        if not ge_profile_requests:
            return

        # Otherwise, if column level profiling is enabled, use  GE profiler.
        ge_profiler = self.get_profiler_instance(db_name)
        yield from ge_profiler.generate_profiles(
            ge_profile_requests, max_workers, platform, profiler_args
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
        )

    def is_dataset_eligible_for_profiling(
        self,
        dataset_name: str,
        last_altered: Optional[datetime],
        size_in_bytes: Optional[int],
        rows_count: Optional[int],
    ) -> bool:
        threshold_time: Optional[datetime] = None
        if self.config.profiling.profile_if_updated_since_days is not None:
            threshold_time = datetime.now(timezone.utc) - timedelta(
                self.config.profiling.profile_if_updated_since_days
            )

        if not self.config.table_pattern.allowed(
            dataset_name
        ) or not self.config.profile_pattern.allowed(dataset_name):
            return False

        schema_name = dataset_name.rsplit(".", 1)[0]
        if (threshold_time is not None) and (
            last_altered is not None and last_altered < threshold_time
        ):
            self.report.profiling_skipped_not_updated[schema_name] = (
                self.report.profiling_skipped_not_updated.get(schema_name, 0) + 1
            )
            return False

        if self.config.profiling.profile_table_size_limit is not None and (
            size_in_bytes is None
            or size_in_bytes / (2**30)
            > self.config.profiling.profile_table_size_limit
        ):
            self.report.profiling_skipped_size_limit[schema_name] = (
                self.report.profiling_skipped_size_limit.get(schema_name, 0) + 1
            )
            return False

        if self.config.profiling.profile_table_row_limit is not None and (
            rows_count is None
            or rows_count > self.config.profiling.profile_table_row_limit
        ):
            self.report.profiling_skipped_row_limit[schema_name] = (
                self.report.profiling_skipped_row_limit.get(schema_name, 0) + 1
            )
            return False

        return True

    def get_profile_args(self) -> Dict:
        """Passed down to GE profiler"""
        return {}
