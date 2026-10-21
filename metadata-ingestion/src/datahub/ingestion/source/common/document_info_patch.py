import logging
from typing import Optional

from datahub.ingestion.graph.client import DataHubGraph
from datahub.utilities.server_config_util import ServiceFeature

logger = logging.getLogger(__name__)


def supports_document_info_patch(graph: Optional[DataHubGraph]) -> bool:
    """Whether Documents can be emitted with a documentInfo patch.

    A patch keeps related assets and related documents that users added in
    DataHub. Without a graph (e.g. a file sink) or on an older server, sources
    fall back to the full documentInfo upsert.
    """
    if graph is None:
        return False
    try:
        supported = (
            graph.server_config.supports_feature(ServiceFeature.DOCUMENT_INFO_PATCH)
            is True
        )
    except Exception:
        logger.warning(
            "Could not check whether the DataHub server supports documentInfo "
            "patches; writing the full documentInfo instead.",
            exc_info=True,
        )
        return False
    if not supported:
        logger.info(
            "DataHub server does not support documentInfo patches; re-ingesting a "
            "Document replaces related assets and related documents added in DataHub."
        )
    return supported
