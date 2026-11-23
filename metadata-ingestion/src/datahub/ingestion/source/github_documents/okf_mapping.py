# Moved to datahub.ingestion.source.repository_documents.okf_mapping; this module keeps
# the old import path working for code built on github-documents.
import warnings

from datahub.ingestion.source.repository_documents.okf_mapping import (
    EMAIL_PATTERN as EMAIL_PATTERN,
    MAPPING_VERSION as MAPPING_VERSION,
    NATIVE_MAPPINGS as NATIVE_MAPPINGS,
    OkfLookupCache as OkfLookupCache,
    OkfMapping as OkfMapping,
    map_okf as map_okf,
)

warnings.warn(
    "datahub.ingestion.source.github_documents.okf_mapping has moved to "
    "datahub.ingestion.source.repository_documents.okf_mapping. The old path will be "
    "removed in a future release.",
    FutureWarning,
    stacklevel=2,
)
