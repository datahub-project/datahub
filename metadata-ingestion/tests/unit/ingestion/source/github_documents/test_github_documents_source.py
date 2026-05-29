"""Unit tests for GitHubDocumentsSource."""

from unittest.mock import MagicMock, patch

import pytest
from pydantic import SecretStr

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.documents.document_import_mode import DocumentImportMode
from datahub.ingestion.source.github_documents.github_api import GitHubFileInfo
from datahub.ingestion.source.github_documents.github_documents_config import (
    GitHubDocumentsSourceConfig,
)
from datahub.ingestion.source.github_documents.github_documents_source import (
    GitHubDocumentsSource,
)
from datahub.metadata.schema_classes import (
    DataPlatformInstanceClass,
    DocumentInfoClass,
    DocumentSourceTypeClass,
)


@pytest.fixture
def source() -> GitHubDocumentsSource:
    config = GitHubDocumentsSourceConfig(
        github_token=SecretStr("ghp_test"),
        repository="acme/docs",
        path_prefix="docs",
    )
    ctx = PipelineContext(run_id="test-run")
    return GitHubDocumentsSource(config, ctx)


def test_get_workunits_emits_data_platform_instance(
    source: GitHubDocumentsSource,
) -> None:
    source.client.list_matching_files = MagicMock(  # type: ignore[method-assign]
        return_value=([GitHubFileInfo(path="docs/readme.md", size=12)], False)
    )
    source.client.get_latest_commit_sha = MagicMock(  # type: ignore[method-assign]
        return_value="abc123"
    )
    source.client.fetch_file_content = MagicMock(  # type: ignore[method-assign]
        return_value="# Hello"
    )

    workunits = list(source.get_workunits())
    platform_instance_aspects = [
        wu.get_aspect_of_type(DataPlatformInstanceClass)
        for wu in workunits
        if isinstance(wu, MetadataWorkUnit)
    ]
    platform_instance_aspects = [a for a in platform_instance_aspects if a is not None]

    assert platform_instance_aspects, (
        "expected dataPlatformInstance on emitted documents"
    )
    assert all(
        aspect.platform == "urn:li:dataPlatform:github"
        for aspect in platform_instance_aspects
    )
    assert all(
        aspect.instance
        == "urn:li:dataPlatformInstance:(urn:li:dataPlatform:github,acme/docs)"
        for aspect in platform_instance_aspects
    )


def test_native_mode_emits_external_url(source: GitHubDocumentsSource) -> None:
    source.client.list_matching_files = MagicMock(  # type: ignore[method-assign]
        return_value=([GitHubFileInfo(path="docs/readme.md", size=12)], False)
    )
    source.client.get_latest_commit_sha = MagicMock(  # type: ignore[method-assign]
        return_value="abc123"
    )
    source.client.fetch_file_content = MagicMock(  # type: ignore[method-assign]
        return_value="# Hello"
    )

    workunits = list(source.get_workunits())
    file_info_aspects = [
        wu.get_aspect_of_type(DocumentInfoClass)
        for wu in workunits
        if isinstance(wu, MetadataWorkUnit)
    ]
    file_docs = [
        aspect
        for aspect in file_info_aspects
        if aspect
        and aspect.source
        and aspect.source.sourceType == DocumentSourceTypeClass.NATIVE
        and aspect.source.externalUrl
        and aspect.source.externalUrl.endswith("readme.md")
    ]
    assert file_docs, "expected NATIVE document with GitHub external URL"
    assert (
        file_docs[0].source.externalUrl
        == "https://github.com/acme/docs/blob/main/docs/readme.md"
    )


def test_get_workunits_emits_folder_then_file(source: GitHubDocumentsSource) -> None:
    source.client.list_matching_files = MagicMock(  # type: ignore[method-assign]
        return_value=([GitHubFileInfo(path="docs/readme.md", size=12)], False)
    )
    source.client.get_latest_commit_sha = MagicMock(  # type: ignore[method-assign]
        return_value="abc123"
    )
    source.client.fetch_file_content = MagicMock(  # type: ignore[method-assign]
        return_value="# Hello"
    )

    workunits = list(source.get_workunits())
    assert len(workunits) >= 1
    assert source.report.files_processed == 1


def test_get_workunits_reports_truncated_tree(source: GitHubDocumentsSource) -> None:
    source.client.list_matching_files = MagicMock(  # type: ignore[method-assign]
        return_value=([GitHubFileInfo(path="docs/readme.md", size=12)], True)
    )
    source.client.get_latest_commit_sha = MagicMock(  # type: ignore[method-assign]
        return_value="abc123"
    )
    source.client.fetch_file_content = MagicMock(  # type: ignore[method-assign]
        return_value="# Hello"
    )

    list(source.get_workunits())
    assert source.report.tree_truncated is True
    assert any(
        "truncated" in (entry.message or "").lower() for entry in source.report.warnings
    )


@patch(
    "datahub.ingestion.source.github_documents.github_documents_source.GitHubApiClient.list_matching_files",
    side_effect=RuntimeError("branch missing"),
)
def test_test_connection_failure(_mock_list: MagicMock) -> None:
    report = GitHubDocumentsSource.test_connection(
        {
            "github_token": "ghp_test",
            "repository": "acme/docs",
        }
    )
    assert report.basic_connectivity == "FAIL"
    assert report.internal_failure is True


def test_test_connection_success() -> None:
    with patch(
        "datahub.ingestion.source.github_documents.github_documents_source.GitHubApiClient.list_matching_files",
        return_value=([], False),
    ):
        report = GitHubDocumentsSource.test_connection(
            {
                "github_token": "ghp_test",
                "repository": "acme/docs",
            }
        )
    assert report.basic_connectivity == "PASS"


def test_test_connection_config_parse_failure() -> None:
    report = GitHubDocumentsSource.test_connection({"repository": "acme/docs"})
    assert report.internal_failure is True
    assert "Failed to parse config" in (report.internal_failure_reason or "")


def test_external_mode_emits_external_source_type(source: GitHubDocumentsSource) -> None:
    source.config.document_import_mode = DocumentImportMode.EXTERNAL
    source.client.list_matching_files = MagicMock(  # type: ignore[method-assign]
        return_value=([GitHubFileInfo(path="docs/readme.md", size=12)], False)
    )
    source.client.get_latest_commit_sha = MagicMock(  # type: ignore[method-assign]
        return_value="abc123"
    )
    source.client.fetch_file_content = MagicMock(  # type: ignore[method-assign]
        return_value="# Hello"
    )

    workunits = list(source.get_workunits())
    file_info_aspects = [
        wu.get_aspect_of_type(DocumentInfoClass)
        for wu in workunits
        if isinstance(wu, MetadataWorkUnit)
    ]
    external_docs = [
        aspect
        for aspect in file_info_aspects
        if aspect
        and aspect.source
        and aspect.source.sourceType == DocumentSourceTypeClass.EXTERNAL
    ]
    assert external_docs, "expected EXTERNAL document source type"


def test_repo_root_document_is_emitted(source: GitHubDocumentsSource) -> None:
    source.client.list_matching_files = MagicMock(  # type: ignore[method-assign]
        return_value=([GitHubFileInfo(path="docs/readme.md", size=12)], False)
    )
    source.client.get_latest_commit_sha = MagicMock(  # type: ignore[method-assign]
        return_value="abc123"
    )
    source.client.fetch_file_content = MagicMock(  # type: ignore[method-assign]
        return_value="# Hello"
    )

    list(source.get_workunits())
    assert source.report.folders_processed >= 1


def test_skipped_file_increments_files_skipped(source: GitHubDocumentsSource) -> None:
    source.client.list_matching_files = MagicMock(  # type: ignore[method-assign]
        return_value=([GitHubFileInfo(path="docs/readme.md", size=12)], False)
    )
    source.client.get_latest_commit_sha = MagicMock(  # type: ignore[method-assign]
        return_value="abc123"
    )
    source.client.fetch_file_content = MagicMock(  # type: ignore[method-assign]
        return_value=None
    )

    list(source.get_workunits())
    assert source.report.files_skipped == 1
    assert source.report.files_processed == 0


def test_no_matching_files_returns_early(source: GitHubDocumentsSource) -> None:
    source.client.list_matching_files = MagicMock(  # type: ignore[method-assign]
        return_value=([], False)
    )

    workunits = list(source.get_workunits())
    assert workunits == []


def test_parent_document_urn_skips_repo_root() -> None:
    config = GitHubDocumentsSourceConfig(
        github_token=SecretStr("ghp_test"),
        repository="acme/docs",
        path_prefix="docs",
        parent_document_urn="urn:li:document:parent",
    )
    ctx = PipelineContext(run_id="test-run")
    source = GitHubDocumentsSource(config, ctx)
    assert source._repo_root_source_id is None

    source.client.list_matching_files = MagicMock(  # type: ignore[method-assign]
        return_value=([GitHubFileInfo(path="docs/readme.md", size=12)], False)
    )
    source.client.get_latest_commit_sha = MagicMock(  # type: ignore[method-assign]
        return_value="abc123"
    )
    source.client.fetch_file_content = MagicMock(  # type: ignore[method-assign]
        return_value="# Hello"
    )

    list(source.get_workunits())
    assert source.report.folders_processed == 0
