# Moved to datahub.ingestion.source.repository_documents.okf_cleanup; this module keeps
# the old import path working for code built on github-documents.
import warnings

from datahub.ingestion.source.repository_documents.okf_cleanup import (
    clear_okf_metadata as clear_okf_metadata,
)

warnings.warn(
    "datahub.ingestion.source.github_documents.okf_cleanup has moved to "
    "datahub.ingestion.source.repository_documents.okf_cleanup. The old path will be "
    "removed in a future release.",
    FutureWarning,
    stacklevel=2,
)
