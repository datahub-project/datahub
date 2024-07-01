import logging
from typing import Optional

from looker_sdk.error import SDKError
from looker_sdk.sdk.api40.models import DBConnection

from datahub.configuration.common import ConfigurationError
from datahub.ingestion.source.looker.looker_config import LookerConnectionDefinition
from datahub.ingestion.source.looker.looker_lib_wrapper import LookerAPI
from datahub.ingestion.source.looker.lookml_config import (
    LookMLSourceConfig,
    LookMLSourceReport,
)

logger = logging.getLogger(__name__)


def get_connection_def_based_on_connection_string(
    connection: str,
    source_config: LookMLSourceConfig,
    looker_client: Optional[LookerAPI],
    reporter: LookMLSourceReport,
) -> Optional[LookerConnectionDefinition]:
    if source_config.connection_to_platform_map is None:
        source_config.connection_to_platform_map = {}

    assert source_config.connection_to_platform_map is not None

    connection_def: Optional[LookerConnectionDefinition] = None

    if connection in source_config.connection_to_platform_map:
        connection_def = source_config.connection_to_platform_map[connection]
    elif looker_client:
        try:
            looker_connection: DBConnection = looker_client.connection(connection)
        except SDKError:
            logger.error(
                f"Failed to retrieve connection {connection} from Looker. This usually happens when the "
                f"credentials provided are not admin credentials."
            )
        else:
            try:
                connection_def = LookerConnectionDefinition.from_looker_connection(
                    looker_connection
                )

                # Populate the cache (using the config map) to avoid calling looker again for this connection
                source_config.connection_to_platform_map[connection] = connection_def
            except ConfigurationError:
                reporter.report_warning(
                    f"connection-{connection}",
                    "Failed to load connection from Looker",
                )

    return connection_def
