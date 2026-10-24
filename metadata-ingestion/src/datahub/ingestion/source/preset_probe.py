from typing import ClassVar, Type

from datahub.ingestion.source.preset import PresetSource
from datahub.ingestion.source.superset import SupersetSource
from datahub.ingestion.source.superset_probe import SupersetMetadataProbe


class PresetMetadataProbe(SupersetMetadataProbe):
    """Superset's probe, logging in as Preset does: an API key and secret
    exchanged at manager_uri for a workspace token (PresetSource.login)."""

    source_class: ClassVar[Type[SupersetSource]] = PresetSource
