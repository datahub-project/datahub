"""Helper utility functions for the custom SQLAlchemy profiler."""

import logging
from typing import List, Optional, Tuple

from datahub.emitter import mce_builder
from datahub.ingestion.graph.client import get_default_graph
from datahub.ingestion.graph.config import ClientMode
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    EditableSchemaMetadata,
)

logger: logging.Logger = logging.getLogger(__name__)


def _get_columns_to_ignore_sampling(
    dataset_name: str,
    tags_to_ignore: Optional[List[str]],
    platform: str,
    env: str,
) -> Tuple[bool, List[str]]:
    """
    Get columns to ignore sampling based on tags.

    Uses the DataHub Graph API (which uses GraphQL under the hood) to fetch
    dataset and column-level tags, matching the implementation in the
    Great Expectations profiler.

    Returns: (ignore_table_sampling, columns_list_to_ignore_sampling)
    """
    logger.debug("Collecting columns to ignore for sampling")

    ignore_table: bool = False
    columns_to_ignore: List[str] = []

    if not tags_to_ignore:
        return ignore_table, columns_to_ignore

    dataset_urn = mce_builder.make_dataset_urn(
        name=dataset_name, platform=platform, env=env
    )

    datahub_graph = get_default_graph(ClientMode.INGESTION)

    # Check dataset-level tags
    dataset_tags = datahub_graph.get_tags(dataset_urn)
    if dataset_tags:
        ignore_table = any(
            tag_association.tag.split("urn:li:tag:")[1] in tags_to_ignore
            for tag_association in dataset_tags.tags
        )

    # If table-level tag found, ignore entire table
    if not ignore_table:
        # Check column-level tags
        metadata = datahub_graph.get_aspect(
            entity_urn=dataset_urn, aspect_type=EditableSchemaMetadata
        )

        if metadata:
            for schemaField in metadata.editableSchemaFieldInfo:
                if schemaField.globalTags:
                    columns_to_ignore.extend(
                        schemaField.fieldPath
                        for tag_association in schemaField.globalTags.tags
                        if tag_association.tag.split("urn:li:tag:")[1] in tags_to_ignore
                    )

    return ignore_table, columns_to_ignore
