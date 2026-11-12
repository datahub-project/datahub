# Moved to datahub.ingestion.source.repository_documents.report; this module keeps
# the old import path working for code built on github-documents.
from datahub.ingestion.source.repository_documents.report import (
    RepositoryDocumentsSourceReport as RepositoryDocumentsSourceReport,
)

GitHubDocumentsSourceReport = RepositoryDocumentsSourceReport
