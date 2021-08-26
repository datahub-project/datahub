import warnings

# This import retains backwards compatability, but is deprecated
# and should be avoided.
from datahub.ingestion.api.workunit import MetadataWorkUnit  # noqa: F401

warnings.warn(
    "importing from datahub.ingestion.source.metadata_common is deprecated; "
    "use datahub.ingestion.api.workunit instead"
)
