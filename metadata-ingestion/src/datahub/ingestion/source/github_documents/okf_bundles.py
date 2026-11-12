# Moved to datahub.ingestion.source.repository_documents.okf_bundles; this module keeps
# the old import path working for code built on github-documents.
from typing import Callable, Dict, Iterable, List, Optional, Set

from datahub.ingestion.source.repository_documents.okf_bundles import (
    RESERVED_FILENAMES as RESERVED_FILENAMES,
    bundle_index_path as bundle_index_path,
    concept_id as concept_id,
    discover_bundles as _discover_bundles,
    enclosing_bundle as enclosing_bundle,
)
from datahub.ingestion.source.repository_documents.report import (
    RepositoryDocumentsSourceReport,
)
from datahub.metadata.schema_classes import OkfBundleClass


def discover_bundles(
    *,
    repo: str,
    file_paths: Iterable[str],
    index_paths: Set[str],
    explicit_roots: List[str],
    fetch: Callable[[str], Optional[str]],
    report: RepositoryDocumentsSourceReport,
    location: Optional[str] = None,
) -> Dict[str, OkfBundleClass]:
    return _discover_bundles(
        repo=repo,
        file_paths=file_paths,
        index_paths=index_paths,
        explicit_roots=explicit_roots,
        fetch=fetch,
        report=report,
        location=location or f"https://github.com/{repo}",
    )
