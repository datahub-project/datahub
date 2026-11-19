# Moved to datahub.ingestion.source.repository_documents.report; this module keeps
# the old import path working for code built on github-documents.
import warnings

from datahub.ingestion.source.repository_documents.report import (
    RepositoryDocumentsSourceReport as RepositoryDocumentsSourceReport,
)

warnings.warn(
    "datahub.ingestion.source.github_documents.github_documents_report has moved to "
    "datahub.ingestion.source.repository_documents.report. The old path will be "
    "removed in a future release.",
    FutureWarning,
    stacklevel=2,
)

GitHubDocumentsSourceReport = RepositoryDocumentsSourceReport
