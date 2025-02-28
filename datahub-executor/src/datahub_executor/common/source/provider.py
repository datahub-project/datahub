from typing import cast

from datahub_executor.common.connection.bigquery.bigquery_connection import (
    BigQueryConnection,
)
from datahub_executor.common.connection.connection import Connection
from datahub_executor.common.connection.databricks.databricks_connection import (
    DatabricksConnection,
)
from datahub_executor.common.connection.redshift.redshift_connection import (
    RedshiftConnection,
)
from datahub_executor.common.connection.snowflake.snowflake_connection import (
    SnowflakeConnection,
)
from datahub_executor.common.constants import (
    BIGQUERY_PLATFORM_URN,
    DATABRICKS_PLATFORM_URN,
    REDSHIFT_PLATFORM_URN,
    SNOWFLAKE_PLATFORM_URN,
)
from datahub_executor.common.exceptions import UnsupportedPlatformException
from datahub_executor.common.source.bigquery.bigquery import BigQuerySource
from datahub_executor.common.source.databricks.databricks import DatabricksSource
from datahub_executor.common.source.redshift.redshift import RedshiftSource
from datahub_executor.common.source.snowflake.snowflake import SnowflakeSource
from datahub_executor.common.source.source import Source


class SourceProvider:
    """Base class for a provider of sources, or connectors to external systems."""

    def create_source_from_connection(self, connection: Connection) -> Source:
        # Simply instantiates a new source from a connection.
        # TODO: Cache the sources to avoid re-allocation.
        if connection.platform_urn == SNOWFLAKE_PLATFORM_URN:
            return SnowflakeSource(cast(SnowflakeConnection, connection))
        elif connection.platform_urn == REDSHIFT_PLATFORM_URN:
            return RedshiftSource(cast(RedshiftConnection, connection))
        elif connection.platform_urn == BIGQUERY_PLATFORM_URN:
            return BigQuerySource(cast(BigQueryConnection, connection))
        elif connection.platform_urn == DATABRICKS_PLATFORM_URN:
            return DatabricksSource(cast(DatabricksConnection, connection))
        else:
            raise UnsupportedPlatformException(
                message=f"No source class registered for connection with platform urn {connection.platform_urn}",
                platform_urn=connection.platform_urn,
            )
