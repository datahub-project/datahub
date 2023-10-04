import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Callable, Iterable, List, Optional, cast

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.ge_data_profiler import GEProfilerRequest
from datahub.ingestion.source.sql.sql_config import SQLCommonConfig
from datahub.ingestion.source.sql.sql_generic import BaseTable
from datahub.ingestion.source.sql.sql_generic_profiler import (
    GenericProfiler,
    TableProfilerRequest,
)
from datahub.ingestion.source.unity.config import UnityCatalogGEProfilerConfig
from datahub.ingestion.source.unity.proxy_types import Table, TableReference
from datahub.ingestion.source.unity.report import UnityCatalogReport

logger = logging.getLogger(__name__)


@dataclass(init=False)
class UnityCatalogSQLGenericTable(BaseTable):
    ref: TableReference = field(init=False)

    def __init__(self, table: Table):
        self.name = table.name
        self.comment = table.comment
        self.created = table.created_at
        self.last_altered = table.updated_at
        self.column_count = len(table.columns)
        self.ref = table.ref


@dataclass
class UnityCatalogProfilerRequest(GEProfilerRequest):
    ref: TableReference
    profile_table_level_only: bool = False
    size_in_bytes: Optional[int] = None


class UnityCatalogGEProfiler(GenericProfiler):
    sql_common_config: SQLCommonConfig
    profiling_config: UnityCatalogGEProfilerConfig
    report: UnityCatalogReport
    dataset_urn_builder: Callable[[TableReference], str]

    def __init__(
        self,
        sql_common_config: SQLCommonConfig,
        profiling_config: UnityCatalogGEProfilerConfig,
        report: UnityCatalogReport,
        dataset_urn_builder: Callable[[TableReference], str],
    ) -> None:
        super().__init__(sql_common_config, report, "databricks")
        self.profiling_config = profiling_config
        self.dataset_urn_builder = dataset_urn_builder

    def get_workunits(self, tables: List[Table]) -> Iterable[MetadataWorkUnit]:
        # Extra default SQLAlchemy option for better connection pooling and threading.
        # https://docs.sqlalchemy.org/en/14/core/pooling.html#sqlalchemy.pool.QueuePool.params.max_overflow
        self.config.options.setdefault(
            "max_overflow", self.profiling_config.max_workers
        )

        url = self.config.get_sql_alchemy_url()
        engine = create_engine(url, **self.config.options)

        profile_requests = []
        for table in tables:
            profile_request = self.get_unity_profile_request(
                UnityCatalogSQLGenericTable(table), engine=engine
            )
            if profile_request is not None:
                profile_requests.append(profile_request)

        if len(profile_requests) == 0:
            return

        table_profile_requests = cast(List[TableProfilerRequest], profile_requests)
        for request, profile in self.generate_profiles(
            table_profile_requests,
            max_workers=self.profiling_config.max_workers,
            platform=self.platform,
            profiler_args=self.get_profile_args(),
        ):
            if profile is None:
                continue

            request = cast(UnityCatalogProfilerRequest, request)
            dataset_urn = self.dataset_urn_builder(request.ref)

            # We don't add to the profiler state if we only do table level profiling as it always happens
            if self.state_handler and not request.profile_table_level_only:
                self.state_handler.add_to_state(
                    dataset_urn, int(datetime.now().timestamp() * 1000)
                )

            yield MetadataChangeProposalWrapper(
                entityUrn=dataset_urn, aspect=profile
            ).as_workunit()

    def get_unity_profile_request(
        self, table: UnityCatalogSQLGenericTable, engine: Engine
    ) -> Optional[UnityCatalogProfilerRequest]:
        skip_profiling = False
        profile_table_level_only = self.profiling_config.profile_table_level_only

        dataset_name = table.ref.qualified_table_name
        size_in_bytes = _get_dataset_size_in_bytes(table, engine)
        if size_in_bytes is None:
            self.report.num_profile_missing_size_in_bytes += 1
        if not self.is_dataset_eligible_for_profiling(
            dataset_name,
            size_in_bytes=size_in_bytes,
            last_altered=table.last_altered,
            rows_count=None,
        ):
            # Profile only table level if dataset is filtered from profiling
            # due to size limits alone
            if self.is_dataset_eligible_for_profiling(
                dataset_name,
                last_altered=table.last_altered,
                size_in_bytes=0,
                rows_count=0,
            ):
                profile_table_level_only = True
            else:
                skip_profiling = True

        if table.column_count == 0:
            skip_profiling = True

        if skip_profiling:
            if self.profiling_config.report_dropped_profiles:
                self.report.report_dropped(dataset_name)
            return None

        self.report.report_entity_profiled(dataset_name)
        logger.debug(f"Preparing profiling request for {dataset_name}")
        return UnityCatalogProfilerRequest(
            ref=table.ref,
            pretty_name=dataset_name,
            batch_kwargs=dict(schema=table.ref.schema, table=table.name),
            profile_table_level_only=profile_table_level_only,
            size_in_bytes=size_in_bytes,
        )


def _get_dataset_size_in_bytes(
    table: UnityCatalogSQLGenericTable, engine: Engine
) -> Optional[int]:
    name = engine.dialect.identifier_preparer.quote(table.ref.qualified_table_name)
    row = engine.execute(f"DESCRIBE DETAIL {name}").fetchone()
    if row is None:
        return None
    else:
        try:
            return int(row._asdict()["sizeInBytes"])
        except Exception:
            return None
