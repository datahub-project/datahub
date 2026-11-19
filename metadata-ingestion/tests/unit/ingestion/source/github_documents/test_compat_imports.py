import importlib
import warnings
from typing import List, Tuple

import pytest

_PACKAGE = "datahub.ingestion.source.github_documents"

# Public names importable from github_documents before the shared code moved to
# repository_documents. Code built on github-documents imports these, so they
# must keep resolving until the old paths are removed.
_OLD_IMPORTS: List[Tuple[str, List[str]]] = [
    (
        "github_api",
        [
            "GITHUB_API_BASE",
            "MAX_FILE_SIZE_BYTES",
            "REPO_URL_PATTERN",
            "GitHubApiClient",
            "GitHubFileInfo",
            "GitHubTokenProvider",
            "StaticTokenProvider",
            "collect_intermediate_directories",
            "make_dir_source_id",
            "make_file_source_id",
            "make_repo_source_id",
            "normalize_document_id",
            "parse_repo_identifier",
            "resolve_parent_dir_source_id",
        ],
    ),
    ("github_documents_config", ["GitHubDocumentsSourceConfig"]),
    ("github_documents_report", ["GitHubDocumentsSourceReport"]),
    (
        "github_documents_source",
        [
            "EXTRACTION_ALGO_VERSION",
            "GitHubDocumentsSource",
            "LAST_EXPORTED_CONTENT_HASH_KEY",
            "PROP_GITHUB_DIRECTORY_PATH",
            "PROP_GITHUB_FILE_PATH",
            "PROP_IMPORT_SOURCE_ID",
            "compute_file_content_hash",
            "compute_source_hash",
        ],
    ),
    ("github_hierarchy", ["GitHubHierarchyExtractor"]),
    (
        "okf_bundles",
        [
            "RESERVED_FILENAMES",
            "ROOT_PATH",
            "bundle_index_path",
            "concept_id",
            "discover_bundles",
            "enclosing_bundle",
        ],
    ),
    ("okf_cleanup", ["clear_okf_metadata"]),
    (
        "okf_frontmatter",
        [
            "FRONTMATTER_DELIMITER",
            "MAX_FRONTMATTER_BYTES",
            "OkfFrontmatter",
            "parse_frontmatter",
            "parse_okf_frontmatter",
        ],
    ),
    (
        "okf_mapping",
        [
            "EMAIL_PATTERN",
            "MAPPING_VERSION",
            "NATIVE_MAPPINGS",
            "OkfLookupCache",
            "OkfMapping",
            "map_okf",
        ],
    ),
    (
        "okf_metadata",
        ["parse_stamp", "parse_status", "parse_timestamp", "parse_verified"],
    ),
    ("okf_patches", ["Association", "reconcile_okf_associations"]),
]

# Modules that only forward to repository_documents, and so warn on import.
_MOVED_MODULES = [
    "github_hierarchy",
    "github_documents_report",
    "okf_bundles",
    "okf_cleanup",
    "okf_frontmatter",
    "okf_mapping",
    "okf_metadata",
    "okf_patches",
]


@pytest.mark.parametrize(
    "module, name",
    [(module, name) for module, names in _OLD_IMPORTS for name in names],
)
def test_old_github_documents_import_paths_resolve(module: str, name: str) -> None:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", FutureWarning)
        imported = importlib.import_module(f"{_PACKAGE}.{module}")
        assert hasattr(imported, name)


@pytest.mark.parametrize("module", _MOVED_MODULES)
def test_moved_modules_warn_on_import(module: str) -> None:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", FutureWarning)
        imported = importlib.import_module(f"{_PACKAGE}.{module}")
    # Reloading re-runs the module body in place, so the package attribute and
    # sys.modules keep pointing at the same module object.
    with pytest.warns(FutureWarning, match="repository_documents"):
        importlib.reload(imported)


# Names in modules that still hold their own code, kept only as aliases of a
# repository_documents name. Each warns when it is imported.
_DEPRECATED_ALIASES: List[Tuple[str, str]] = [
    ("github_api", "GitHubFileInfo"),
    ("github_api", "RepositoryDocumentsClient"),
    ("github_api", "MAX_FILE_SIZE_BYTES"),
    ("github_api", "collect_intermediate_directories"),
    ("github_api", "normalize_document_id"),
    ("github_api", "make_file_source_id"),
    ("github_api", "make_dir_source_id"),
    ("github_api", "make_repo_source_id"),
    ("github_api", "resolve_parent_dir_source_id"),
    ("github_documents_source", "EXTRACTION_ALGO_VERSION"),
    ("github_documents_source", "LAST_EXPORTED_CONTENT_HASH_KEY"),
    ("github_documents_source", "PROP_IMPORT_SOURCE_ID"),
    ("github_documents_source", "compute_file_content_hash"),
    ("github_documents_source", "compute_source_hash"),
]


@pytest.mark.parametrize("module, name", _DEPRECATED_ALIASES)
def test_deprecated_aliases_warn_on_access(module: str, name: str) -> None:
    imported = importlib.import_module(f"{_PACKAGE}.{module}")
    with pytest.warns(
        FutureWarning, match=rf"\.{name} is deprecated.*repository_documents"
    ):
        getattr(imported, name)


def test_github_source_id_aliases_keep_the_github_prefix() -> None:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", FutureWarning)
        from datahub.ingestion.source.github_documents.github_api import (
            make_file_source_id,
        )
    assert make_file_source_id("acme/docs", "a/b.md") == "github.acme.docs.a.b"
