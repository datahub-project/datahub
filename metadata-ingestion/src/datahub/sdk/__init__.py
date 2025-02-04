import warnings

import datahub.metadata.schema_classes as models
from datahub.errors import ExperimentalWarning, SdkUsageError
from datahub.ingestion.graph.config import DatahubClientConfig
from datahub.metadata.urns import (
    ChartUrn,
    ContainerUrn,
    CorpGroupUrn,
    CorpUserUrn,
    DashboardUrn,
    DataPlatformInstanceUrn,
    DataPlatformUrn,
    DatasetUrn,
    DomainUrn,
    GlossaryTermUrn,
    SchemaFieldUrn,
    TagUrn,
)
from datahub.sdk.container import Container
from datahub.sdk.dataset import Dataset
from datahub.sdk.main_client import DataHubClient

warnings.warn(
    "The new datahub SDK (e.g. datahub.sdk.*) is experimental. "
    "Our typical backwards-compatibility and stability guarantees do not apply to this code. "
    "When it's promoted to stable, the import path will change "
    "from `from datahub.sdk import ...` to `from datahub import ...`.",
    ExperimentalWarning,
    stacklevel=2,
)
del warnings
del ExperimentalWarning
