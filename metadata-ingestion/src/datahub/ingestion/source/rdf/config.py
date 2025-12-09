from typing import Optional

from datahub.configuration.source_common import (
    EnvConfigMixin,
    PlatformInstanceConfigMixin,
)
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
)


class RDFSourceConfig(
    StatefulIngestionConfigBase, EnvConfigMixin, PlatformInstanceConfigMixin
):
    """
    Configuration for RDF ingestion source.

    Add your RDF-specific configuration fields here.
    """

    # TODO: Add your RDF configuration fields
    # Example:
    # rdf_file_path: str = Field(description="Path to RDF file or directory")
    # rdf_format: str = Field(default="turtle", description="RDF format (turtle, n3, xml, etc.)")

    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = None
