"""Unit tests for GitHubDocumentsSource."""

from unittest.mock import MagicMock, patch

import pytest
from pydantic import SecretStr

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.documents.document_import_mode import DocumentImportMode
from datahub.ingestion.source.github_documents.github_api import (
    GitHubFileInfo,
    make_dir_source_id,
    make_file_source_id,
    make_repo_source_id,
)
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


def _entity_urns_by_source_id(workunits: list) -> dict[str, str]:
    urns: dict[str, str] = {}
    for wu in workunits:
        if not isinstance(wu, MetadataWorkUnit):
            continue
        info = wu.get_aspect_of_type(DocumentInfoClass)
        if not info or not info.customProperties:
            continue
        source_id = info.customProperties.get("import_source_id")
        if source_id:
            urns[source_id] = wu.get_urn()
    return urns


def _document_infos_by_source_id(workunits: list) -> dict[str, DocumentInfoClass]:
    infos: dict[str, DocumentInfoClass] = {}
    for wu in workunits:
        if not isinstance(wu, MetadataWorkUnit):
            continue
        info = wu.get_aspect_of_type(DocumentInfoClass)
        if info and info.customProperties:
            source_id = info.customProperties.get("import_source_id")
            if source_id:
                infos[source_id] = info
    return infos


def _mock_github_client(
    source: GitHubDocumentsSource,
    *,
    files: list[GitHubFileInfo],
    tree_truncated: bool = False,
    file_content: str | None = "# Hello",
) -> None:
    source.client.list_matching_files = MagicMock(  # type: ignore[method-assign]
        return_value=(files, tree_truncated)
    )
    source.client.get_latest_commit_sha = MagicMock(  # type: ignore[method-assign]
        return_value="abc123"
    )
    source.client.fetch_file_content = MagicMock(  # type: ignore[method-assign]
        return_value=file_content
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


def test_external_mode_emits_external_source_type(
    source: GitHubDocumentsSource,
) -> None:
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


def test_nested_file_hierarchy_links_parent_documents(
    source: GitHubDocumentsSource,
) -> None:
    owner_repo = "acme/docs"
    _mock_github_client(
        source,
        files=[GitHubFileInfo(path="docs/guides/setup.md", size=12)],
    )

    workunits = list(source.get_workunits())
    urns = _entity_urns_by_source_id(workunits)
    infos = _document_infos_by_source_id(workunits)

    repo_source_id = make_repo_source_id(owner_repo)
    guides_dir_source_id = make_dir_source_id(owner_repo, "docs/guides")
    file_source_id = make_file_source_id(owner_repo, "docs/guides/setup.md")

    repo_info = infos[repo_source_id]
    guides_info = infos[guides_dir_source_id]
    file_info = infos[file_source_id]

    assert repo_info.parentDocument is None
    assert guides_info.parentDocument is not None
    assert guides_info.parentDocument.document == urns[repo_source_id]
    assert file_info.parentDocument is not None
    assert file_info.parentDocument.document == urns[guides_dir_source_id]


def test_top_level_file_parent_is_repo_root(source: GitHubDocumentsSource) -> None:
    owner_repo = "acme/docs"
    _mock_github_client(
        source,
        files=[GitHubFileInfo(path="docs/readme.md", size=12)],
    )

    workunits = list(source.get_workunits())
    urns = _entity_urns_by_source_id(workunits)
    infos = _document_infos_by_source_id(workunits)

    repo_source_id = make_repo_source_id(owner_repo)
    file_source_id = make_file_source_id(owner_repo, "docs/readme.md")

    assert infos[file_source_id].parentDocument is not None
    assert infos[file_source_id].parentDocument.document == urns[repo_source_id]


def test_configured_parent_document_urn_used_for_top_level_files() -> None:
    parent_urn = "urn:li:document:parent"
    config = GitHubDocumentsSourceConfig(
        github_token=SecretStr("ghp_test"),
        repository="acme/docs",
        path_prefix="docs",
        parent_document_urn=parent_urn,
    )
    source = GitHubDocumentsSource(config, PipelineContext(run_id="test-run"))
    _mock_github_client(
        source,
        files=[GitHubFileInfo(path="docs/readme.md", size=12)],
    )

    workunits = list(source.get_workunits())
    infos = _document_infos_by_source_id(workunits)
    file_source_id = make_file_source_id("acme/docs", "docs/readme.md")

    assert infos[file_source_id].parentDocument is not None
    assert infos[file_source_id].parentDocument.document == parent_urn
