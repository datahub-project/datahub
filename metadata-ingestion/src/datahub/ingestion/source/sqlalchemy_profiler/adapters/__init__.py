"""Platform adapter factory for SQL profiling."""

import logging
from typing import TYPE_CHECKING, Dict, Type

from sqlalchemy.engine import Engine

from datahub.ingestion.source.ge_profiling_config import ProfilingConfig
from datahub.ingestion.source.sql.sql_report import SQLSourceReport
from datahub.ingestion.source.sqlalchemy_profiler.base_adapter import PlatformAdapter

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


def get_adapter(
    platform: str,
    config: ProfilingConfig,
    report: SQLSourceReport,
    base_engine: Engine,
) -> PlatformAdapter:
    """
    Factory function to get platform-specific profiling adapter.

    Args:
        platform: Database platform name (e.g., "bigquery", "snowflake")
        config: Profiling configuration
        report: Report object for warnings/errors
        base_engine: SQLAlchemy engine

    Returns:
        Platform-specific adapter instance
    """
    from datahub.ingestion.source.sqlalchemy_profiler.adapters.athena import (
        AthenaAdapter,
    )
    from datahub.ingestion.source.sqlalchemy_profiler.adapters.bigquery import (
        BigQueryAdapter,
    )
    from datahub.ingestion.source.sqlalchemy_profiler.adapters.generic import (
        GenericAdapter,
    )
    from datahub.ingestion.source.sqlalchemy_profiler.adapters.mysql import MySQLAdapter
    from datahub.ingestion.source.sqlalchemy_profiler.adapters.postgres import (
        PostgresAdapter,
    )
    from datahub.ingestion.source.sqlalchemy_profiler.adapters.redshift import (
        RedshiftAdapter,
    )

    # Map platform names to adapter classes
    adapters: Dict[str, Type[PlatformAdapter]] = {
        "bigquery": BigQueryAdapter,
        "athena": AthenaAdapter,
        "postgresql": PostgresAdapter,
        "postgres": PostgresAdapter,
        "mysql": MySQLAdapter,
        "redshift": RedshiftAdapter,
        # Add more platforms as needed:
        # "snowflake": SnowflakeAdapter,
        # "databricks": DatabricksAdapter,
    }

    platform_lower = platform.lower()
    adapter_class: Type[PlatformAdapter] = adapters.get(platform_lower, GenericAdapter)

    logger.debug(f"Using {adapter_class.__name__} for platform '{platform}'")

    return adapter_class(config, report, base_engine)
