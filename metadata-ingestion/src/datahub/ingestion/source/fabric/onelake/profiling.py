"""Table and column profiling for Fabric OneLake via the SQL Analytics Endpoint.

Profiling uses the same SQLAlchemy profiler and MSSQL adapter as the
`mssql-odbc` source. The SQL Analytics Endpoint is reached with
`mssql+pyodbc` and the Microsoft ODBC Driver for SQL Server.
"""

import logging
from dataclasses import dataclass
from typing import Callable, Iterable, List, Optional, cast

from sqlalchemy.engine import Engine

from datahub.configuration.common import AllowDenyPattern
from datahub.emitter.mce_builder import make_dataset_urn_with_platform_instance
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.fabric.onelake.report import FabricOneLakeSourceReport
from datahub.ingestion.source.profiling.common import ProfilerRequest
from datahub.ingestion.source.profiling.config import ProfilingConfig
from datahub.ingestion.source.sql.sql_report import SQLSourceReport
from datahub.ingestion.source.sql.sql_utils import check_table_with_profile_pattern

logger = logging.getLogger(__name__)

# Selects MSSQLAdapter inside SQLAlchemyProfiler. Fabric SQL is SQL Server.
PROFILING_SQL_PLATFORM = "mssql"


@dataclass
class FabricProfileTarget:
    """One ingested table to profile.

    `schema_name` and `table_name` are the SQL identifiers. `dataset_name` is
    the Fabric dataset name already used for the dataset URN.
    """

    schema_name: str
    table_name: str
    dataset_name: str


def emit_dataset_profiles(
    *,
    engine: Engine,
    report: FabricOneLakeSourceReport,
    profiling: ProfilingConfig,
    profile_pattern: AllowDenyPattern,
    targets: List[FabricProfileTarget],
    platform: str,
    env: str,
    platform_instance: Optional[str],
    field_path_transform: Callable[[str], str],
) -> Iterable[MetadataWorkUnit]:
    """Profile ingested tables and emit datasetProfile aspects on their URNs."""
    requests: List[ProfilerRequest] = []
    dataset_names_by_pretty_name: dict[str, str] = {}
    for target in targets:
        pretty_name = f"{target.schema_name}.{target.table_name}"
        if not check_table_with_profile_pattern(profile_pattern, pretty_name):
            report.profiling_skipped_table_profile_pattern[target.schema_name] += 1
            if profiling.report_dropped_profiles:
                report.report_dropped(f"profile of {pretty_name}")
            continue
        dataset_names_by_pretty_name[pretty_name] = target.dataset_name
        requests.append(
            ProfilerRequest(
                pretty_name=pretty_name,
                batch_kwargs={
                    "schema": target.schema_name,
                    "table": target.table_name,
                },
            )
        )

    if not requests:
        return

    from datahub.ingestion.source.sqlalchemy_profiler.sqlalchemy_profiler import (
        SQLAlchemyProfiler,
    )

    profiler = SQLAlchemyProfiler(
        conn=engine,
        # The profiler types its report as SQLSourceReport. Fabric's report
        # implements the methods the profiler calls.
        report=cast(SQLSourceReport, report),
        config=profiling,
        platform=PROFILING_SQL_PLATFORM,
        env=env,
        field_path_transform=field_path_transform,
    )
    logger.info(
        f"Profiling {len(requests)} table(s) through the SQL Analytics Endpoint"
    )
    for request, profile in profiler.generate_profiles(
        requests,
        profiling.max_workers,
        platform=PROFILING_SQL_PLATFORM,
    ):
        if profile is None:
            continue
        dataset_name = dataset_names_by_pretty_name.get(request.pretty_name)
        if dataset_name is None:
            logger.warning(
                f"No Fabric dataset name for profile of {request.pretty_name}"
            )
            continue
        dataset_urn = make_dataset_urn_with_platform_instance(
            platform,
            dataset_name,
            platform_instance,
            env,
        )
        report.report_entity_profiled(dataset_name)
        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=profile,
        ).as_workunit()
