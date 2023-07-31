from typing import cast

from datahub_monitors.connection.bigquery.bigquery_connection import BigQueryConnection
from datahub_monitors.connection.connection import Connection
from datahub_monitors.connection.redshift.redshift_connection import RedshiftConnection
from datahub_monitors.connection.snowflake.snowflake_connection import (
    SnowflakeConnection,
)
from datahub_monitors.constants import (
    BIGQUERY_PLATFORM_URN,
    REDSHIFT_PLATFORM_URN,
    SNOWFLAKE_PLATFORM_URN,
)
from datahub_monitors.exceptions import UnsupportedPlatformException
from datahub_monitors.source.bigquery.bigquery import BigQuerySource
from datahub_monitors.source.redshift.redshift import RedshiftSource
from datahub_monitors.source.snowflake.snowflake import SnowflakeSource
from datahub_monitors.source.source import Source


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
        else:
            raise UnsupportedPlatformException(
                message=f"No source class registered for connection with platform urn {connection.platform_urn}",
                platform_urn=connection.platform_urn,
            )
