# Moved to datahub.ingestion.source.repository_documents.hierarchy; this module keeps
# the old import path working for code built on github-documents.
from datahub.ingestion.source.repository_documents.hierarchy import (
    RepositoryHierarchyExtractor as RepositoryHierarchyExtractor,
)

GitHubHierarchyExtractor = RepositoryHierarchyExtractor
