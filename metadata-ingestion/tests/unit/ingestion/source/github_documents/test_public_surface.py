import importlib
from typing import List, Tuple

import pytest

_PACKAGE = "datahub.ingestion.source.github_documents"

# What github_documents keeps after the shared code moved to
# repository_documents. Code built on github-documents (including DataHub Cloud
# sync-back) imports these from these paths, so removing or moving one is a
# breaking change and must come with a docs/how/breaking-changes.md entry. The
# names are exercised by their own tests; this only guards the import paths.
_PUBLIC_NAMES: List[Tuple[str, str]] = [
    ("github_api", "GITHUB_API_BASE"),
    ("github_api", "REPO_URL_PATTERN"),
    ("github_api", "GitHubApiClient"),
    ("github_api", "GitHubTokenProvider"),
    ("github_api", "StaticTokenProvider"),
    ("github_api", "parse_repo_identifier"),
    ("github_documents_config", "GitHubDocumentsSourceConfig"),
    ("github_documents_source", "GitHubDocumentsSource"),
    ("github_documents_source", "PROP_GITHUB_DIRECTORY_PATH"),
    ("github_documents_source", "PROP_GITHUB_FILE_PATH"),
]


@pytest.mark.parametrize("module, name", _PUBLIC_NAMES)
def test_github_documents_public_names_resolve(module: str, name: str) -> None:
    assert hasattr(importlib.import_module(f"{_PACKAGE}.{module}"), name)
