"""Unit tests for GitHubDocumentsSourceConfig."""

import pytest
from pydantic import SecretStr, ValidationError

from datahub.ingestion.source.documents.document_import_mode import DocumentImportMode
from datahub.ingestion.source.github_documents.github_documents_config import (
    GitHubDocumentsSourceConfig,
)


def test_valid_config_defaults() -> None:
    config = GitHubDocumentsSourceConfig(
        github_token=SecretStr("ghp_test"),
        repository="acme/docs",
    )
    assert config.repository == "acme/docs"
    assert config.branch == "main"
    assert config.file_extensions == [".md", ".txt"]
    assert config.document_import_mode == DocumentImportMode.NATIVE
    assert config.document_mapping.source.type == "NATIVE"


def test_external_mode_sets_mapping() -> None:
    config = GitHubDocumentsSourceConfig(
        github_token=SecretStr("ghp_test"),
        repository="acme/docs",
        document_import_mode=DocumentImportMode.EXTERNAL,
    )
    assert config.document_mapping.source.type == "EXTERNAL"


def test_parent_document_sets_root_parent() -> None:
    config = GitHubDocumentsSourceConfig(
        github_token=SecretStr("ghp_test"),
        repository="acme/docs",
        parent_document_urn="urn:li:document:parent",
    )
    assert config.hierarchy.folder_mapping.root_parent == "urn:li:document:parent"


def test_missing_token_raises() -> None:
    with pytest.raises(ValidationError):
        GitHubDocumentsSourceConfig(repository="acme/docs")  # type: ignore[call-arg]
