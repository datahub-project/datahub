# Moved to datahub.ingestion.source.repository_documents.okf_patches; this module keeps
# the old import path working for code built on github-documents.
import warnings

from datahub.ingestion.source.repository_documents.okf_patches import (
    Association as Association,
    reconcile_okf_associations as reconcile_okf_associations,
)

warnings.warn(
    "datahub.ingestion.source.github_documents.okf_patches has moved to "
    "datahub.ingestion.source.repository_documents.okf_patches. The old path will be "
    "removed in a future release.",
    FutureWarning,
    stacklevel=2,
)
