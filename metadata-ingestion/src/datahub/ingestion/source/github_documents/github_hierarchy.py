# Moved to datahub.ingestion.source.repository_documents.hierarchy; this module keeps
# the old import path working for code built on github-documents.
import warnings

from datahub.ingestion.source.repository_documents.hierarchy import (
    RepositoryHierarchyExtractor as RepositoryHierarchyExtractor,
)

warnings.warn(
    "datahub.ingestion.source.github_documents.github_hierarchy has moved to "
    "datahub.ingestion.source.repository_documents.hierarchy. The old path will be "
    "removed in a future release.",
    FutureWarning,
    stacklevel=2,
)

GitHubHierarchyExtractor = RepositoryHierarchyExtractor
