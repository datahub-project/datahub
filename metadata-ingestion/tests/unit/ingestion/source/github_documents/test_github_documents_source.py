"""Unit tests for GitHubDocumentsSource."""

from unittest.mock import MagicMock, patch

import pytest
from pydantic import SecretStr

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.graph.filters import RemovedStatusFilter
from datahub.ingestion.source.documents.document_import_mode import DocumentImportMode
from datahub.ingestion.source.github_documents.github_api import (
    GitHubFileInfo,
    make_dir_source_id,
    make_file_source_id,
    make_repo_source_id,
    normalize_document_id,
)
from datahub.ingestion.source.github_documents.github_documents_config import (
    GitHubDocumentsSourceConfig,
)
from datahub.ingestion.source.github_documents.github_documents_source import (
    GitHubDocumentsSource,
    compute_file_content_hash,
)
from datahub.metadata.schema_classes import (
    BrowsePathsV2Class,
    DataPlatformInstanceClass,
    DocumentInfoClass,
    DocumentSourceTypeClass,
    StatusClass,
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


def _browse_paths_by_urn(workunits: list) -> dict[str, BrowsePathsV2Class]:
    paths: dict[str, BrowsePathsV2Class] = {}
    for wu in workunits:
        if not isinstance(wu, MetadataWorkUnit):
            continue
        aspect = wu.get_aspect_of_type(BrowsePathsV2Class)
        if aspect is not None:
            paths[wu.get_urn()] = aspect
    return paths


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
    platform_instances: list[DataPlatformInstanceClass] = []
    for wu in workunits:
        if not isinstance(wu, MetadataWorkUnit):
            continue
        aspect = wu.get_aspect_of_type(DataPlatformInstanceClass)
        if aspect is not None:
            platform_instances.append(aspect)

    assert platform_instances, "expected dataPlatformInstance on emitted documents"
    for aspect in platform_instances:
        assert aspect.platform == "urn:li:dataPlatform:github"
        assert (
            aspect.instance
            == "urn:li:dataPlatformInstance:(urn:li:dataPlatform:github,acme/docs)"
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
    file_doc = file_docs[0]
    assert file_doc.source is not None
    assert (
        file_doc.source.externalUrl
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


def test_get_workunits_truncates_files_at_max_files_limit() -> None:
    config = GitHubDocumentsSourceConfig(
        github_token=SecretStr("ghp_test"),
        repository="acme/docs",
        max_files=2,
    )
    ctx = PipelineContext(run_id="test-run")
    source = GitHubDocumentsSource(config, ctx)
    matching_files = [
        GitHubFileInfo(path=f"docs/file-{index}.md", size=12) for index in range(5)
    ]
    source.client.list_matching_files = MagicMock(  # type: ignore[method-assign]
        return_value=(matching_files, False)
    )
    source.client.get_latest_commit_sha = MagicMock(  # type: ignore[method-assign]
        return_value="abc123"
    )
    source.client.fetch_file_content = MagicMock(  # type: ignore[method-assign]
        return_value="# Hello"
    )

    list(source.get_workunits())

    assert source.report.files_truncated_by_limit is True
    assert source.report.files_processed == 2
    assert source.client.fetch_file_content.call_count == 2
    assert any(
        "max_files" in (entry.message or "").lower() for entry in source.report.warnings
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
    assert report.basic_connectivity is not None
    assert report.basic_connectivity.capable is False
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
    assert report.basic_connectivity is not None
    assert report.basic_connectivity.capable is True


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


def test_create_repo_root_document_false_skips_repo_root() -> None:
    config = GitHubDocumentsSourceConfig(
        github_token=SecretStr("ghp_test"),
        repository="acme/docs",
        path_prefix="docs",
        create_repo_root_document=False,
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
    guides_parent = guides_info.parentDocument
    assert guides_parent is not None
    assert guides_parent.document == urns[repo_source_id]
    file_parent = file_info.parentDocument
    assert file_parent is not None
    assert file_parent.document == urns[guides_dir_source_id]


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

    file_parent = infos[file_source_id].parentDocument
    assert file_parent is not None
    assert file_parent.document == urns[repo_source_id]


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

    configured_parent = infos[file_source_id].parentDocument
    assert configured_parent is not None
    assert configured_parent.document == parent_urn


def test_browse_path_emitted_for_nested_file(source: GitHubDocumentsSource) -> None:
    owner_repo = "acme/docs"
    _mock_github_client(
        source,
        files=[GitHubFileInfo(path="docs/guides/setup.md", size=12)],
    )

    workunits = list(source.get_workunits())
    urns = _entity_urns_by_source_id(workunits)
    browse_paths = _browse_paths_by_urn(workunits)

    repo_source_id = make_repo_source_id(owner_repo)
    guides_dir_source_id = make_dir_source_id(owner_repo, "docs/guides")
    file_source_id = make_file_source_id(owner_repo, "docs/guides/setup.md")

    file_browse_path = browse_paths[urns[file_source_id]]
    # Default flow: the repo-root document provides the repo entry (clickable),
    # followed by folder ancestors; the file itself is excluded.
    assert [e.urn for e in file_browse_path.path] == [
        urns[repo_source_id],
        urns[guides_dir_source_id],
    ]
    assert [e.id for e in file_browse_path.path] == ["docs", "guides"]


def test_browse_path_not_emitted_for_repo_root(source: GitHubDocumentsSource) -> None:
    owner_repo = "acme/docs"
    _mock_github_client(
        source,
        files=[GitHubFileInfo(path="docs/readme.md", size=12)],
    )

    workunits = list(source.get_workunits())
    urns = _entity_urns_by_source_id(workunits)
    browse_paths = _browse_paths_by_urn(workunits)

    repo_source_id = make_repo_source_id(owner_repo)
    # Repo-root document has no ancestors (org disabled), so no browse path.
    assert urns[repo_source_id] not in browse_paths


def test_browse_path_includes_repo_name_without_root_document() -> None:
    # When the repo-root document is skipped, a synthetic repo entry keeps the
    # repository name in the browse path so users aren't confused.
    config = GitHubDocumentsSourceConfig(
        github_token=SecretStr("ghp_test"),
        repository="acme/docs",
        path_prefix="docs",
        create_repo_root_document=False,
    )
    source = GitHubDocumentsSource(config, PipelineContext(run_id="test-run"))
    _mock_github_client(
        source,
        files=[GitHubFileInfo(path="docs/guides/setup.md", size=12)],
    )

    workunits = list(source.get_workunits())
    urns = _entity_urns_by_source_id(workunits)
    browse_paths = _browse_paths_by_urn(workunits)

    guides_dir_source_id = make_dir_source_id("acme/docs", "docs/guides")
    file_source_id = make_file_source_id("acme/docs", "docs/guides/setup.md")

    file_browse_path = browse_paths[urns[file_source_id]]
    assert [e.id for e in file_browse_path.path] == ["docs", "guides"]
    # Repo entry is synthetic (no document exists for it) -> no URN.
    assert file_browse_path.path[0].urn is None
    assert file_browse_path.path[1].urn == urns[guides_dir_source_id]


def test_browse_path_includes_org_when_enabled() -> None:
    config = GitHubDocumentsSourceConfig(
        github_token=SecretStr("ghp_test"),
        repository="acme/docs",
        path_prefix="docs",
        include_organization_in_browse_path=True,
    )
    source = GitHubDocumentsSource(config, PipelineContext(run_id="test-run"))
    _mock_github_client(
        source,
        files=[GitHubFileInfo(path="docs/guides/setup.md", size=12)],
    )

    workunits = list(source.get_workunits())
    urns = _entity_urns_by_source_id(workunits)
    browse_paths = _browse_paths_by_urn(workunits)

    repo_source_id = make_repo_source_id("acme/docs")
    file_source_id = make_file_source_id("acme/docs", "docs/guides/setup.md")

    file_browse_path = browse_paths[urns[file_source_id]]
    # Org is the top-most entry; the repo-root document follows (clickable).
    assert [e.id for e in file_browse_path.path] == ["acme", "docs", "guides"]
    assert file_browse_path.path[0].urn is None  # org is synthetic
    assert file_browse_path.path[1].urn == urns[repo_source_id]


def test_browse_path_repo_can_be_disabled() -> None:
    config = GitHubDocumentsSourceConfig(
        github_token=SecretStr("ghp_test"),
        repository="acme/docs",
        path_prefix="docs",
        create_repo_root_document=False,
        include_repository_in_browse_path=False,
    )
    source = GitHubDocumentsSource(config, PipelineContext(run_id="test-run"))
    _mock_github_client(
        source,
        files=[GitHubFileInfo(path="docs/guides/setup.md", size=12)],
    )

    workunits = list(source.get_workunits())
    urns = _entity_urns_by_source_id(workunits)
    browse_paths = _browse_paths_by_urn(workunits)

    file_source_id = make_file_source_id("acme/docs", "docs/guides/setup.md")
    file_browse_path = browse_paths[urns[file_source_id]]
    # No repo-root doc and repo entry disabled -> only folder ancestors.
    assert [e.id for e in file_browse_path.path] == ["guides"]


def test_browse_path_roots_under_configured_parent() -> None:
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
        files=[GitHubFileInfo(path="docs/guides/setup.md", size=12)],
    )

    workunits = list(source.get_workunits())
    urns = _entity_urns_by_source_id(workunits)
    browse_paths = _browse_paths_by_urn(workunits)

    guides_dir_source_id = make_dir_source_id("acme/docs", "docs/guides")
    file_source_id = make_file_source_id("acme/docs", "docs/guides/setup.md")

    file_browse_path = browse_paths[urns[file_source_id]]
    # Configured external parent roots the path, then the synthetic repo name
    # (no repo-root doc in this mode), then in-repo folder ancestors.
    assert file_browse_path.path[0].urn == parent_urn
    assert [e.id for e in file_browse_path.path] == [
        "urn:li:document:parent",
        "docs",
        "guides",
    ]
    assert [e.urn for e in file_browse_path.path] == [
        parent_urn,
        None,
        urns[guides_dir_source_id],
    ]


def test_browse_path_failure_reports_warning_and_still_emits(
    source: GitHubDocumentsSource,
) -> None:
    _mock_github_client(
        source,
        files=[GitHubFileInfo(path="docs/guides/setup.md", size=12)],
    )

    with patch(
        "datahub.ingestion.source.github_documents.github_documents_source.GitHubHierarchyExtractor.build_browse_path_v2",
        side_effect=RuntimeError("boom"),
    ):
        workunits = list(source.get_workunits())

    # Document is still emitted despite the browse-path failure.
    infos = _document_infos_by_source_id(workunits)
    file_source_id = make_file_source_id("acme/docs", "docs/guides/setup.md")
    assert file_source_id in infos

    # Failure surfaces as a structured warning -> "succeeded with warnings".
    assert any(
        warning.title == "Browse path generation failed"
        for warning in source.report.warnings
    )


def test_file_document_includes_content_hash_and_blob_sha(
    source: GitHubDocumentsSource,
) -> None:
    content = "# Hello"
    _mock_github_client(
        source,
        files=[GitHubFileInfo(path="docs/readme.md", size=12, sha="blobabc")],
        file_content=content,
    )

    workunits = list(source.get_workunits())
    infos = _document_infos_by_source_id(workunits)
    file_source_id = make_file_source_id("acme/docs", "docs/readme.md")
    props = infos[file_source_id].customProperties

    assert props is not None
    assert props.get("content_hash") == compute_file_content_hash(content)
    assert props.get("extraction_algo_version") == "1"
    assert props.get("github_blob_sha") == "blobabc"


def test_get_workunit_processors_includes_stale_removal(
    source: GitHubDocumentsSource,
) -> None:
    from functools import partial

    from datahub.ingestion.source.state.stale_entity_removal_handler import (
        auto_stale_entity_removal,
    )

    processors = source.get_workunit_processors()
    stale_processors = [
        processor
        for processor in processors
        if isinstance(processor, partial)
        and processor.func is auto_stale_entity_removal
    ]
    assert stale_processors
    assert stale_processors[0].args[0] is source.stale_entity_removal_handler


def test_get_excluded_workunit_processors_excludes_auto_stale_processor(
    source: GitHubDocumentsSource,
) -> None:
    from datahub.ingestion.workunit_processors.auto_stale_entity_removal import (
        AutoStaleEntityRemovalProcessor,
    )

    assert source.get_excluded_workunit_processors() == [
        AutoStaleEntityRemovalProcessor
    ]


def test_skips_unchanged_file_when_graph_hash_matches(
    source: GitHubDocumentsSource,
) -> None:
    content = "# Hello"
    content_hash = compute_file_content_hash(content)
    _mock_github_client(
        source,
        files=[GitHubFileInfo(path="docs/readme.md", size=12)],
        file_content=content,
    )
    source.ctx.graph = MagicMock()
    mock_info = MagicMock()
    mock_info.customProperties = {"content_hash": content_hash}
    source.ctx.graph.get_aspect.return_value = mock_info

    list(source.get_workunits())
    assert source.report.files_processed == 0
    assert source.report.files_skipped_unchanged == 1


def test_skipping_unchanged_file_registers_entity_for_stale_removal(
    source: GitHubDocumentsSource,
) -> None:
    content = "# Hello"
    content_hash = compute_file_content_hash(content)
    _mock_github_client(
        source,
        files=[GitHubFileInfo(path="docs/readme.md", size=12)],
        file_content=content,
    )
    source.ctx.graph = MagicMock()
    source.ctx.graph.get_urns_by_filter.return_value = []
    mock_info = MagicMock()
    mock_info.customProperties = {"content_hash": content_hash}
    source.ctx.graph.get_aspect.return_value = mock_info
    source.stale_entity_removal_handler.add_entity_to_state = MagicMock()  # type: ignore[method-assign]

    list(source.get_workunits_internal())

    source_id = make_file_source_id("acme/docs", "docs/readme.md")
    expected_urn = f"urn:li:document:{normalize_document_id(source_id)}"
    source.stale_entity_removal_handler.add_entity_to_state.assert_called_once_with(
        "document",
        expected_urn,
    )


def test_prefer_existing_document_urn_prefers_non_canonical(
    source: GitHubDocumentsSource,
) -> None:
    source.stale_entity_removal_handler.add_entity_to_state = MagicMock()  # type: ignore[method-assign]
    canonical = "urn:li:document:canonical"
    native = "urn:li:document:native-uuid"
    assert (
        source._prefer_existing_document_urn(
            [canonical, native],
            canonical,
            path="docs/a.md",
            source_id="github.acme.docs.a",
        )
        == native
    )
    assert (
        source._prefer_existing_document_urn(
            [canonical],
            canonical,
            path="docs/a.md",
            source_id="github.acme.docs.a",
        )
        == canonical
    )
    assert (
        source._prefer_existing_document_urn(
            [],
            canonical,
            path="docs/a.md",
            source_id="github.acme.docs.a",
        )
        is None
    )


def test_prefer_existing_document_urn_is_deterministic_and_warns(
    source: GitHubDocumentsSource,
) -> None:
    source.stale_entity_removal_handler.add_entity_to_state = MagicMock()  # type: ignore[method-assign]
    canonical = "urn:li:document:canonical"
    first = source._prefer_existing_document_urn(
        ["urn:li:document:z", "urn:li:document:a"],
        canonical,
        path="docs/a.md",
        source_id="github.acme.docs.a",
    )
    second = source._prefer_existing_document_urn(
        ["urn:li:document:a", "urn:li:document:z"],
        canonical,
        path="docs/a.md",
        source_id="github.acme.docs.a",
    )
    assert first == second == "urn:li:document:a"
    assert any(
        "Ambiguous document identity" in (entry.title or "")
        for entry in source.report.warnings
    )


def test_reuses_existing_document_urn_from_import_source_id(
    source: GitHubDocumentsSource,
) -> None:
    existing_urn = "urn:li:document:native-uuid-1234"
    source_id = make_file_source_id("acme/docs", "docs/readme.md")
    _mock_github_client(
        source,
        files=[GitHubFileInfo(path="docs/readme.md", size=12)],
        file_content="# Hello from sync-back",
    )
    source.ctx.graph = MagicMock()
    source.ctx.graph.get_urns_by_filter.return_value = [existing_urn]
    source.ctx.graph.get_aspect.return_value = None

    workunits = list(source.get_workunits_internal())
    urns = {wu.get_urn() for wu in workunits if isinstance(wu, MetadataWorkUnit)}
    assert existing_urn in urns
    # Must not also emit the deterministic canonical URN for the same file.
    canonical_urn = f"urn:li:document:{normalize_document_id(source_id)}"
    assert canonical_urn not in urns


def test_skip_unchanged_uses_resolved_existing_urn(
    source: GitHubDocumentsSource,
) -> None:
    existing_urn = "urn:li:document:native-uuid-1234"
    content = "# Hello"
    content_hash = compute_file_content_hash(content)
    source_id = make_file_source_id("acme/docs", "docs/readme.md")
    _mock_github_client(
        source,
        files=[GitHubFileInfo(path="docs/readme.md", size=12)],
        file_content=content,
    )
    source.ctx.graph = MagicMock()
    source.ctx.graph.get_urns_by_filter.return_value = [existing_urn]
    mock_info = MagicMock()
    mock_info.customProperties = {
        "content_hash": content_hash,
        "import_source_id": source_id,
    }

    def get_aspect(urn: str, _aspect: object) -> object:
        # Canonical URN has no matching stamp; only the DataHub-native URN does.
        if urn == existing_urn:
            return mock_info
        return None

    source.ctx.graph.get_aspect.side_effect = get_aspect
    source.stale_entity_removal_handler.add_entity_to_state = MagicMock()  # type: ignore[method-assign]

    list(source.get_workunits_internal())

    assert source.report.files_skipped_unchanged == 1
    source.stale_entity_removal_handler.add_entity_to_state.assert_called_once_with(
        "document",
        existing_urn,
    )


def _file_document_urns(workunits: list, file_path: str) -> set[str]:
    urns: set[str] = set()
    for wu in workunits:
        if not isinstance(wu, MetadataWorkUnit):
            continue
        info = wu.get_aspect_of_type(DocumentInfoClass)
        if (
            info
            and info.customProperties
            and info.customProperties.get("github_file_path") == file_path
        ):
            urns.add(wu.get_urn())
    return urns


def test_import_without_graph_still_uses_canonical_urn(
    source: GitHubDocumentsSource,
) -> None:
    """Regression: plain GitHub import (no graph) must keep deterministic URNs."""
    source_id = make_file_source_id("acme/docs", "docs/readme.md")
    canonical_urn = f"urn:li:document:{normalize_document_id(source_id)}"
    _mock_github_client(
        source,
        files=[GitHubFileInfo(path="docs/readme.md", size=12)],
        file_content="# Hello",
    )
    assert source.ctx.graph is None

    workunits = list(source.get_workunits_internal())
    assert _file_document_urns(workunits, "docs/readme.md") == {canonical_urn}


def test_import_with_empty_identity_lookup_uses_canonical_urn(
    source: GitHubDocumentsSource,
) -> None:
    """Regression: when no sync-back stamp exists, import still mints canonical URN."""
    source_id = make_file_source_id("acme/docs", "docs/readme.md")
    canonical_urn = f"urn:li:document:{normalize_document_id(source_id)}"
    _mock_github_client(
        source,
        files=[GitHubFileInfo(path="docs/readme.md", size=12)],
        file_content="# Hello",
    )
    source.ctx.graph = MagicMock()
    source.ctx.graph.get_urns_by_filter.return_value = []
    source.ctx.graph.get_aspect.return_value = None

    workunits = list(source.get_workunits_internal())
    assert _file_document_urns(workunits, "docs/readme.md") == {canonical_urn}
    source.ctx.graph.get_urns_by_filter.assert_called()


def test_import_identity_lookup_failure_falls_back_to_canonical_urn(
    source: GitHubDocumentsSource,
) -> None:
    """Regression: search failures must not break import of new GitHub files.

    Reported via report.failure so StaleEntityRemovalHandler skips soft-deletion:
    falling back to the canonical URN could otherwise reap a stamped document.
    """
    source_id = make_file_source_id("acme/docs", "docs/readme.md")
    canonical_urn = f"urn:li:document:{normalize_document_id(source_id)}"
    _mock_github_client(
        source,
        files=[GitHubFileInfo(path="docs/readme.md", size=12)],
        file_content="# Hello",
    )
    source.ctx.graph = MagicMock()
    source.ctx.graph.get_urns_by_filter.side_effect = RuntimeError("search unavailable")
    source.ctx.graph.get_aspect.return_value = None

    workunits = list(source.get_workunits_internal())
    assert _file_document_urns(workunits, "docs/readme.md") == {canonical_urn}
    assert any(
        getattr(failure, "title", None) == "Document identity lookup failed"
        for failure in source.report.failures
    )


def test_import_identity_lookup_includes_hidden_lifecycle_stages(
    source: GitHubDocumentsSource,
) -> None:
    """Stamped docs in hideInSearch stages must be discoverable during identity reuse."""
    _mock_github_client(
        source,
        files=[GitHubFileInfo(path="docs/readme.md", size=12)],
        file_content="# Hello",
    )
    source.ctx.graph = MagicMock()
    source.ctx.graph.get_urns_by_filter.return_value = []
    source.ctx.graph.get_aspect.return_value = None

    list(source.get_workunits_internal())

    kwargs = source.ctx.graph.get_urns_by_filter.call_args.kwargs
    assert kwargs.get("include_hidden_lifecycle_stages") is True
    assert "include_draft" not in kwargs


def test_import_identity_fallback_lookup_failure_fails_run(
    source: GitHubDocumentsSource,
) -> None:
    """Fallback github_file_path search errors must not look like empty results."""
    source_id = make_file_source_id("acme/docs", "docs/readme.md")
    canonical_urn = f"urn:li:document:{normalize_document_id(source_id)}"
    _mock_github_client(
        source,
        files=[GitHubFileInfo(path="docs/readme.md", size=12)],
        file_content="# Hello",
    )
    source.ctx.graph = MagicMock()
    source.ctx.graph.get_aspect.return_value = None

    def urns_by_filter(**kwargs):
        values = kwargs.get("extraFilters", [{}])[0].get("values", [""])[0]
        if values.startswith("import_source_id="):
            return []
        raise RuntimeError("fallback search unavailable")

    source.ctx.graph.get_urns_by_filter.side_effect = urns_by_filter

    workunits = list(source.get_workunits_internal())
    assert _file_document_urns(workunits, "docs/readme.md") == {canonical_urn}
    assert any(
        getattr(failure, "title", None) == "Document identity lookup failed"
        for failure in source.report.failures
    )


def test_import_prefers_canonical_when_only_canonical_match_exists(
    source: GitHubDocumentsSource,
) -> None:
    source_id = make_file_source_id("acme/docs", "docs/readme.md")
    canonical_urn = f"urn:li:document:{normalize_document_id(source_id)}"
    _mock_github_client(
        source,
        files=[GitHubFileInfo(path="docs/readme.md", size=12)],
        file_content="# Hello",
    )
    source.ctx.graph = MagicMock()
    source.ctx.graph.get_urns_by_filter.return_value = [canonical_urn]
    source.ctx.graph.get_aspect.return_value = None

    workunits = list(source.get_workunits_internal())
    assert _file_document_urns(workunits, "docs/readme.md") == {canonical_urn}


def test_unsupported_search_flag_degrades_once_instead_of_failing(
    source: GitHubDocumentsSource,
) -> None:
    """Older servers reject includeHiddenLifecycleStages permanently.

    Failing the run would make every file fail forever, so retry without the flag
    and warn once. Draft-stage documents simply cannot be matched there.
    """
    _mock_github_client(
        source,
        files=[
            GitHubFileInfo(path="docs/a.md", size=12),
            GitHubFileInfo(path="docs/b.md", size=12),
        ],
        file_content="# Hello",
    )
    source.ctx.graph = MagicMock()
    source.ctx.graph.get_aspect.return_value = None

    def urns_by_filter(**kwargs):
        if kwargs.get("include_hidden_lifecycle_stages"):
            raise ValueError(
                "SearchFlags.includeHiddenLifecycleStages is not supported"
            )
        return []

    source.ctx.graph.get_urns_by_filter.side_effect = urns_by_filter

    list(source.get_workunits_internal())

    assert source.report.failures == []
    assert source.report.hidden_lifecycle_search_unsupported is True
    titles = [getattr(w, "title", None) for w in source.report.warnings]
    assert titles.count("Search flag unsupported by this DataHub server") == 1
    # Only the first attempt pays the rejected call; later lookups skip the flag.
    flagged = [
        call
        for call in source.ctx.graph.get_urns_by_filter.call_args_list
        if call.kwargs.get("include_hidden_lifecycle_stages")
    ]
    assert len(flagged) == 1


def test_unchanged_file_resurrects_soft_deleted_document(
    source: GitHubDocumentsSource,
) -> None:
    """Restoring a deleted file with identical content must un-delete its document.

    The unchanged-file short-circuit returns before any DocumentInfo is emitted, so
    without an explicit Status the document would stay soft-deleted forever.
    """
    source_id = make_file_source_id("acme/docs", "docs/readme.md")
    canonical_urn = f"urn:li:document:{normalize_document_id(source_id)}"
    content = "# Hello"
    _mock_github_client(
        source,
        files=[GitHubFileInfo(path="docs/readme.md", size=12)],
        file_content=content,
    )
    source.ctx.graph = MagicMock()
    existing = MagicMock()
    existing.customProperties = {"content_hash": compute_file_content_hash(content)}
    source.ctx.graph.get_aspect.return_value = existing

    def urns_by_filter(**kwargs):
        if kwargs.get("status") == RemovedStatusFilter.ONLY_SOFT_DELETED:
            return [canonical_urn]
        return []

    source.ctx.graph.get_urns_by_filter.side_effect = urns_by_filter

    workunits = list(source.get_workunits_internal())

    assert source.report.files_skipped_unchanged == 1
    assert source.report.documents_resurrected == 1
    statuses = [
        wu.get_aspect_of_type(StatusClass)
        for wu in workunits
        if isinstance(wu, MetadataWorkUnit) and wu.get_urn() == canonical_urn
    ]
    assert any(s is not None and s.removed is False for s in statuses)


def test_unchanged_file_not_soft_deleted_emits_nothing(
    source: GitHubDocumentsSource,
) -> None:
    """Unchanged files that are not soft-deleted must not pay a write per run."""
    content = "# Hello"
    _mock_github_client(
        source,
        files=[GitHubFileInfo(path="docs/readme.md", size=12)],
        file_content=content,
    )
    source.ctx.graph = MagicMock()
    existing = MagicMock()
    existing.customProperties = {"content_hash": compute_file_content_hash(content)}
    source.ctx.graph.get_aspect.return_value = existing
    source.ctx.graph.get_urns_by_filter.return_value = []

    workunits = list(source.get_workunits_internal())

    assert source.report.files_skipped_unchanged == 1
    assert source.report.documents_resurrected == 0
    assert not [
        wu
        for wu in workunits
        if isinstance(wu, MetadataWorkUnit)
        and wu.get_aspect_of_type(StatusClass) is not None
    ]


def test_folder_identity_fallback_queries_directory_path(
    source: GitHubDocumentsSource,
) -> None:
    """Folders carry github_directory_path; the file key could never match them."""
    _mock_github_client(
        source,
        files=[GitHubFileInfo(path="docs/guides/child.md", size=12)],
        file_content="# Child",
    )
    source.ctx.graph = MagicMock()
    source.ctx.graph.get_aspect.return_value = None
    source.ctx.graph.get_urns_by_filter.return_value = []

    list(source.get_workunits_internal())

    queried = {
        value
        for call in source.ctx.graph.get_urns_by_filter.call_args_list
        for rule in (call.kwargs.get("extraFilters") or [])
        for value in (rule.get("values") or [])
    }
    assert "github_directory_path=docs/guides" in queried
    assert "github_file_path=docs/guides" not in queried


def test_import_emits_status_not_removed_for_file_documents(
    source: GitHubDocumentsSource,
) -> None:
    _mock_github_client(
        source,
        files=[GitHubFileInfo(path="docs/readme.md", size=12)],
        file_content="# Hello",
    )
    source.ctx.graph = MagicMock()
    source.ctx.graph.get_urns_by_filter.return_value = []
    source.ctx.graph.get_aspect.return_value = None

    workunits = list(source.get_workunits_internal())
    statuses = [
        wu.get_aspect_of_type(StatusClass)
        for wu in workunits
        if isinstance(wu, MetadataWorkUnit)
    ]
    file_statuses = [status for status in statuses if status is not None]
    assert file_statuses
    assert all(status.removed is False for status in file_statuses)


def test_same_name_file_remains_sibling_of_folder(
    source: GitHubDocumentsSource,
) -> None:
    """``guides/guides.md`` stays a normal file; folder docs are metadata-only."""
    owner_repo = "acme/docs"
    _mock_github_client(
        source,
        files=[
            GitHubFileInfo(path="docs/guides/guides.md", size=20, sha="idx"),
            GitHubFileInfo(path="docs/guides/child.md", size=12, sha="child"),
        ],
        file_content="# Guides body",
    )

    workunits = list(source.get_workunits())
    infos = _document_infos_by_source_id(workunits)
    urns = _entity_urns_by_source_id(workunits)

    file_source_id = make_file_source_id(owner_repo, "docs/guides/guides.md")
    child_source_id = make_file_source_id(owner_repo, "docs/guides/child.md")
    dir_source_id = make_dir_source_id(owner_repo, "docs/guides")

    assert dir_source_id in infos
    assert infos[dir_source_id].customProperties["is_folder_document"] == "true"
    assert infos[dir_source_id].contents is not None
    assert infos[dir_source_id].contents.text == ""

    assert file_source_id in infos
    assert infos[file_source_id].customProperties.get("is_folder_document") is None
    assert infos[file_source_id].contents is not None
    assert infos[file_source_id].contents.text == "# Guides body"

    child_parent = infos[child_source_id].parentDocument
    assert child_parent is not None
    assert child_parent.document == urns[dir_source_id]


def test_identity_short_circuit_skips_search_for_canonical_match(
    source: GitHubDocumentsSource,
) -> None:
    """Classic imports at the canonical URN must not pay for a search round-trip."""
    source_id = make_file_source_id("acme/docs", "docs/readme.md")
    canonical_urn = f"urn:li:document:{normalize_document_id(source_id)}"
    _mock_github_client(
        source,
        files=[GitHubFileInfo(path="docs/readme.md", size=12)],
        file_content="# Hello",
    )
    source.ctx.graph = MagicMock()
    mock_info = MagicMock()
    mock_info.customProperties = {"import_source_id": source_id}
    source.ctx.graph.get_aspect.return_value = mock_info

    workunits = list(source.get_workunits_internal())
    assert _file_document_urns(workunits, "docs/readme.md") == {canonical_urn}
    source.ctx.graph.get_urns_by_filter.assert_not_called()


def test_existing_folder_document_does_not_overwrite_body(
    source: GitHubDocumentsSource,
) -> None:
    existing_urn = "urn:li:document:native-folder"
    dir_source_id = make_dir_source_id("acme/docs", "docs/guides")
    _mock_github_client(
        source,
        files=[GitHubFileInfo(path="docs/guides/child.md", size=12)],
        file_content="# Child",
    )
    source.ctx.graph = MagicMock()
    existing_info = MagicMock()
    existing_info.customProperties = {"import_source_id": dir_source_id}
    existing_info.title = "Guides"
    existing_info.contents = MagicMock()
    existing_info.contents.text = "rich body"

    def get_aspect(urn: str, _aspect: object) -> object:
        if urn == existing_urn:
            return existing_info
        return None

    source.ctx.graph.get_aspect.side_effect = get_aspect

    def urns_by_filter(**kwargs):  # type: ignore[no-untyped-def]
        for rule in kwargs.get("extraFilters") or []:
            values = rule.get("values") or []
            if any(f"import_source_id={dir_source_id}" == value for value in values):
                return [existing_urn]
        return []

    source.ctx.graph.get_urns_by_filter.side_effect = urns_by_filter

    workunits = list(source.get_workunits_internal())
    folder_infos = [
        wu.get_aspect_of_type(DocumentInfoClass)
        for wu in workunits
        if isinstance(wu, MetadataWorkUnit) and wu.get_urn() == existing_urn
    ]
    folder_infos = [info for info in folder_infos if info is not None]
    assert folder_infos == []
    statuses = [
        wu.get_aspect_of_type(StatusClass)
        for wu in workunits
        if isinstance(wu, MetadataWorkUnit) and wu.get_urn() == existing_urn
    ]
    assert any(status is not None and status.removed is False for status in statuses)
