"""Unit tests for GitHubDocumentsSource."""

import json
from typing import Any, Dict, List, Optional, Sequence, Tuple, Type, TypedDict, cast
from unittest.mock import MagicMock, patch

import pytest
from pydantic import SecretStr

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.graph.entity_aspect_specs import EntityAspectSpecs
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
from datahub.ingestion.source.github_documents.github_documents_report import (
    GitHubDocumentsSourceReport,
)
from datahub.ingestion.source.github_documents.github_documents_source import (
    GitHubDocumentsSource,
    compute_file_content_hash,
    compute_source_hash,
)
from datahub.ingestion.source.github_documents.okf_frontmatter import (
    parse_okf_frontmatter,
)
from datahub.ingestion.source.github_documents.okf_mapping import MAPPING_VERSION
from datahub.metadata.schema_classes import (
    AuditStampClass,
    BrowsePathsV2Class,
    ChangeTypeClass,
    DataPlatformInstanceClass,
    DocumentInfoClass,
    DocumentSourceTypeClass,
    ExecutionRequestInputClass,
    ExecutionRequestSourceClass,
    GlobalTagsClass,
    GlossaryTermAssociationClass,
    GlossaryTermInfoClass,
    GlossaryTermsClass,
    MetadataAttributionClass,
    MetadataChangeProposalClass,
    OkfBundleClass,
    OkfPropertiesClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
    StatusClass,
    SubTypesClass,
    TagAssociationClass,
    _Aspect,
)


def _raw_frontmatter_values(properties: OkfPropertiesClass) -> Dict[str, object]:
    assert properties.raw is not None
    parsed = parse_okf_frontmatter(
        properties.raw, path="mau.md", report=GitHubDocumentsSourceReport()
    )
    assert parsed is not None
    return parsed.values


class PatchOperation(TypedDict):
    op: str
    path: str
    value: Dict[str, object]


def _okf_patch_operations(
    workunits: List[MetadataWorkUnit], aspect_name: str
) -> List[PatchOperation]:
    operations: List[PatchOperation] = []
    for wu in workunits:
        metadata = wu.metadata
        if getattr(metadata, "aspectName", None) != aspect_name:
            continue
        assert isinstance(metadata, MetadataChangeProposalClass)
        assert metadata.changeType == ChangeTypeClass.PATCH
        assert metadata.aspect is not None
        payload = json.loads(metadata.aspect.value)
        operations.extend(cast(List[PatchOperation], payload["patch"]))
    return operations


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
    _mock_github_client(source, files=[])

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
    assert props.get("source_content_hash") == compute_source_hash(
        content, bundle=None, enable_okf=True
    )
    assert props.get("extraction_algo_version") == "2"
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
    mock_info.customProperties = {"source_content_hash": content_hash}
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
    mock_info.customProperties = {"source_content_hash": content_hash}
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
        "source_content_hash": content_hash,
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
    existing.customProperties = {
        "source_content_hash": compute_file_content_hash(content)
    }
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
    existing.customProperties = {
        "source_content_hash": compute_file_content_hash(content)
    }
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


@pytest.fixture
def okf_source() -> GitHubDocumentsSource:
    return GitHubDocumentsSource(
        GitHubDocumentsSourceConfig(
            github_token=SecretStr("test-token"),
            repository="example-org/analytics",
            path_prefix="docs",
            okf_bundle_roots=[""],
            enable_okf=True,
        ),
        PipelineContext(run_id="test-run"),
    )


@pytest.mark.parametrize("newline", ["\n", "\r\n"])
def test_okf_maps_metadata_and_preserves_frontmatter(newline: str) -> None:
    okf_source = GitHubDocumentsSource(
        GitHubDocumentsSourceConfig(
            github_token=SecretStr("test-token"),
            repository="example-org/analytics",
            okf_bundle_roots=[""],
            enable_okf=True,
            create_repo_root_document=False,
        ),
        PipelineContext(run_id="test-run"),
    )
    okf_source.ctx.graph = MagicMock()
    okf_source.ctx.graph.exists.return_value = True
    okf_source.ctx.graph.get_urns_by_filter.side_effect = lambda **kwargs: (
        ["urn:li:glossaryTerm:active-user-id"]
        if kwargs["entity_types"] == ["glossaryTerm"]
        else []
    )
    okf_source.ctx.graph.get_aspect.side_effect = lambda urn, cls: (
        GlossaryTermInfoClass(name="Active User", definition="", termSource="INTERNAL")
        if cls is GlossaryTermInfoClass
        else None
    )
    raw = newline.join(
        [
            "---",
            'okfVersion: "1"',
            "id: guide:mau",
            "type: Guide",
            "title: Monthly active users",
            "description: How MAU is defined.",
            'owners: [alice@example.com, "urn:li:corpGroup:data-platform"]',
            'tags: [metrics, "urn:li:tag:reviewed"]',
            'glossary_terms: [Active User, "urn:li:glossaryTerm:metric"]',
            "audience: analysts",
            "depends_on: [table:events]",
            "reviewed: 2026-01-01",
            "extra: {enabled: true, count: 3, empty: null}",
            "---",
            "",
        ]
    )
    body = "# Monthly active users" + newline + "Body." + newline
    _mock_github_client(
        okf_source,
        files=[GitHubFileInfo(path="mau.md", size=100, sha="blob123")],
        file_content=raw + body,
    )
    workunits = list(okf_source.get_workunits())
    aspects = {
        cls.ASPECT_NAME: next(
            wu.get_aspect_of_type(cls)
            for wu in workunits
            if wu.get_aspect_of_type(cls) is not None
        )
        for cls in (
            DocumentInfoClass,
            OkfPropertiesClass,
            SubTypesClass,
        )
    }
    info = aspects["documentInfo"]
    assert isinstance(info, DocumentInfoClass)
    assert info.title == "Monthly active users"
    assert info.contents.text == body
    assert info.customProperties is not None
    assert info.customProperties["content_hash"] == compute_file_content_hash(body)
    assert info.customProperties["source_content_hash"] == compute_source_hash(
        raw + body,
        bundle=OkfBundleClass(
            location="https://github.com/example-org/analytics", rootPath="."
        ),
        enable_okf=True,
    )
    okf = aspects["okfProperties"]
    assert isinstance(okf, OkfPropertiesClass)
    assert okf.raw == raw
    assert okf.bundle is not None
    assert okf.bundle.okfVersion is None
    assert okf.bundle.rootPath == "."
    assert okf.conceptId == "mau"
    assert okf.declaredId == "guide:mau"
    assert okf.type == "Guide"
    assert okf.description == "How MAU is defined."
    assert okf.revision is not None
    assert okf.bundle.location == "https://github.com/example-org/analytics"
    assert "mappingVersion" not in okf.to_obj()
    assert okf.revision.ref == "main"
    assert okf.revision.commit == "abc123"
    assert okf.revision.blobSha == "blob123"
    assert "frontmatter" not in okf.to_obj()
    values = _raw_frontmatter_values(okf)
    assert values["audience"] == "analysts"
    assert values["depends_on"] == ["table:events"]
    assert values["extra"] == {"enabled": True, "count": 3, "empty": None}
    assert values["reviewed"] == "2026-01-01"
    assert values["owners"] == ["alice@example.com", "urn:li:corpGroup:data-platform"]
    subtypes = aspects["subTypes"]
    assert isinstance(subtypes, SubTypesClass)
    assert subtypes.typeNames == ["Guide"]
    owners = [
        OwnerClass.from_obj(op["value"])
        for op in _okf_patch_operations(workunits, "ownership")
    ]
    tags = [
        TagAssociationClass.from_obj(op["value"])
        for op in _okf_patch_operations(workunits, "globalTags")
    ]
    terms = [
        GlossaryTermAssociationClass.from_obj(op["value"])
        for op in _okf_patch_operations(workunits, "glossaryTerms")
    ]
    assert [owner.owner for owner in owners] == [
        "urn:li:corpuser:alice@example.com",
        "urn:li:corpGroup:data-platform",
    ]
    assert all(owner.type == OwnershipTypeClass.TECHNICAL_OWNER for owner in owners)
    assert [tag.tag for tag in tags] == [
        "urn:li:tag:metrics",
        "urn:li:tag:reviewed",
    ]
    assert [term.urn for term in terms] == [
        "urn:li:glossaryTerm:active-user-id",
        "urn:li:glossaryTerm:metric",
    ]
    keyed: List[Tuple[str, Sequence[Any]]] = [
        ("owners", owners),
        ("tags", tags),
        ("glossary_terms", terms),
    ]
    for key, associations in keyed:
        for association in associations:
            attribution = association.attribution
            assert attribution is not None
            assert attribution.source is None
            assert attribution.actor == "urn:li:corpuser:__datahub_system"
            assert attribution.sourceDetail == {
                "origin": "okf",
                "bundle": "https://github.com/example-org/analytics",
                "rootPath": ".",
                "conceptId": "mau",
                "commit": "abc123",
                "key": key,
                "mappingVersion": MAPPING_VERSION,
            }
    assert okf_source.report.okf_files_processed == 1
    assert okf_source.report.okf_unresolved_values == 0


@pytest.mark.parametrize(
    "content,enabled,warning",
    [
        ("# Plain markdown\nBody.\n", True, False),
        ("---\ntitle: Some title\naudience: analysts\n---\n# Body\n", True, False),
        ("---\ntype: Guide\nowners: [\n---\n# Body\n", True, True),
        ("---\ntype: Guide\n# No closing delimiter\n", True, True),
        ("---\ntype: Guide\ntitle: Overridden\n---\n# Body\n", False, False),
        ("---\ntype: Guide\ntype: Table\n---\n# Body\n", True, True),
        ("---\ntype: Guide\n42: value\n---\n# Body\n", True, True),
        ("---\ntype: Guide\nextra: {42: value}\n---\n# Body\n", True, True),
        ("---\ntype: Guide\nextra: .nan\n---\n# Body\n", True, True),
    ],
)
def test_non_okf_and_invalid_frontmatter_preserve_original(
    okf_source: GitHubDocumentsSource, content: str, enabled: bool, warning: bool
) -> None:
    okf_source.config.enable_okf = enabled
    _mock_github_client(
        okf_source,
        files=[GitHubFileInfo(path="docs/readme.md", size=100)],
        file_content=content,
    )
    workunits = list(okf_source.get_workunits())
    infos = _document_infos_by_source_id(workunits)
    info = infos[make_file_source_id(okf_source.config.repository, "docs/readme.md")]
    assert info.title == "readme.md"
    assert info.contents.text == content
    assert not any(wu.get_aspect_of_type(OkfPropertiesClass) for wu in workunits)
    assert okf_source.report.okf_parse_errors == int(warning)
    assert bool(okf_source.report.warnings) == warning


def test_import_requires_a_resolved_commit(okf_source: GitHubDocumentsSource) -> None:
    _mock_github_client(
        okf_source,
        files=[GitHubFileInfo(path="docs/mau.md", size=100)],
    )
    commit_lookup = okf_source.client.get_latest_commit_sha
    assert isinstance(commit_lookup, MagicMock)
    commit_lookup.return_value = "unknown"
    with pytest.raises(RuntimeError, match="commit snapshot"):
        list(okf_source.get_workunits_internal())
    assert isinstance(okf_source.client.list_matching_files, MagicMock)
    assert isinstance(okf_source.client.fetch_file_content, MagicMock)
    okf_source.client.list_matching_files.assert_not_called()
    okf_source.client.fetch_file_content.assert_not_called()


def test_okf_unresolved_owner_preserves_value(
    okf_source: GitHubDocumentsSource,
) -> None:
    content = "---\ntype: Guide\nowners: [data-platform, alice@example.com]\n---\nBody"
    _mock_github_client(
        okf_source,
        files=[GitHubFileInfo(path="docs/mau.md", size=100)],
        file_content=content,
    )
    workunits = list(okf_source.get_workunits())
    owners = [
        OwnerClass.from_obj(op["value"])
        for op in _okf_patch_operations(workunits, "ownership")
    ]
    okf = next(
        wu.get_aspect_of_type(OkfPropertiesClass)
        for wu in workunits
        if wu.get_aspect_of_type(OkfPropertiesClass)
    )
    assert okf is not None
    assert [owner.owner for owner in owners] == ["urn:li:corpuser:alice@example.com"]
    assert _raw_frontmatter_values(okf)["owners"] == [
        "data-platform",
        "alice@example.com",
    ]
    assert okf_source.report.okf_unresolved_values == 1
    assert okf_source.report.warnings


def test_okf_resolves_username_and_executor_attribution(
    okf_source: GitHubDocumentsSource,
) -> None:
    okf_source.ctx.run_id = "urn:li:dataHubExecutionRequest:test-run"
    okf_source.ctx.graph = MagicMock()
    okf_source.ctx.graph.get_urns_by_filter.side_effect = lambda **kwargs: (
        ["urn:li:glossaryTerm:active-user-id"]
        if kwargs["entity_types"] == ["glossaryTerm"]
        else []
    )
    execution = _scheduled_execution("urn:li:dataHubIngestionSource:example")
    okf_source.ctx.graph.get_aspect.side_effect = lambda urn, cls: (
        execution
        if cls is ExecutionRequestInputClass
        else GlossaryTermInfoClass(
            name="Active User", definition="", termSource="INTERNAL"
        )
        if cls is GlossaryTermInfoClass
        else None
    )
    okf_source.ctx.graph.exists.side_effect = lambda urn: (
        urn
        in {
            "urn:li:corpuser:alice",
            "urn:li:tag:metrics",
        }
    )
    content = (
        "---\ntype: Guide\nowners: [alice]\ntags: [metrics]\n"
        "glossary_terms: [Active User]\n---\nBody"
    )
    _mock_github_client(
        okf_source,
        files=[GitHubFileInfo(path="docs/mau.md", size=100)],
        file_content=content,
    )
    workunits = list(okf_source.get_workunits())
    for aspect_name in ["ownership", "globalTags", "glossaryTerms"]:
        operations = _okf_patch_operations(workunits, aspect_name)
        assert len(operations) == 1
        attribution = operations[0]["value"]["attribution"]
        assert isinstance(attribution, dict)
        assert attribution["source"] == "urn:li:dataHubIngestionSource:example"
    assert okf_source.report.okf_unresolved_values == 0


def test_okf_hash_detects_frontmatter_and_mapping_changes() -> None:
    content = "---\ntype: Guide\ntitle: Original\n---\nBody"
    bundle = OkfBundleClass(
        location="https://github.com/example-org/analytics", rootPath="."
    )
    original = compute_source_hash(content, bundle=bundle, enable_okf=True)
    outside = compute_source_hash(content, bundle=None, enable_okf=True)
    assert original != compute_source_hash(
        content.replace("Original", "Updated"), bundle=bundle, enable_okf=True
    )
    assert original != compute_source_hash(content, bundle=bundle, enable_okf=False)
    with patch(
        "datahub.ingestion.source.github_documents.github_documents_source.MAPPING_VERSION",
        "next",
    ):
        assert original != compute_source_hash(content, bundle=bundle, enable_okf=True)
        assert outside == compute_source_hash(content, bundle=None, enable_okf=True)
        assert outside == compute_file_content_hash(content)


def _association_aspect(
    key: str, entries: List[Tuple[str, Optional[MetadataAttributionClass]]]
) -> _Aspect:
    if key == "owners":
        return OwnershipClass(
            owners=[
                OwnerClass(
                    owner=urn,
                    type=OwnershipTypeClass.TECHNICAL_OWNER,
                    attribution=attribution,
                )
                for urn, attribution in entries
            ]
        )
    if key == "tags":
        return GlobalTagsClass(
            tags=[
                TagAssociationClass(tag=urn, attribution=attribution)
                for urn, attribution in entries
            ]
        )
    return GlossaryTermsClass(
        terms=[
            GlossaryTermAssociationClass(urn=urn, attribution=attribution)
            for urn, attribution in entries
        ],
        auditStamp=AuditStampClass(time=0, actor="urn:li:corpuser:example"),
    )


def _existing_okf_attribution(
    concept_id: str = "docs/mau",
    origin: str = "okf",
    bundle: str = "https://github.com/example-org/analytics",
    root_path: str = ".",
) -> MetadataAttributionClass:
    return MetadataAttributionClass(
        time=0,
        actor="urn:li:corpuser:example",
        source="urn:li:dataHubIngestionSource:example",
        sourceDetail={
            "origin": origin,
            "bundle": bundle,
            "rootPath": root_path,
            "conceptId": concept_id,
        },
    )


def _mock_existing_associations(
    okf_source: GitHubDocumentsSource, current: _Aspect, *, fail_read: bool = False
) -> MagicMock:
    def get_aspect(urn: str, cls: Type[_Aspect]) -> Optional[_Aspect]:
        if cls is type(current):
            if fail_read:
                raise RuntimeError("Catalog read failed")
            return current
        return None

    graph = MagicMock()
    graph.get_aspect.side_effect = get_aspect
    graph.get_urns_by_filter.return_value = []
    graph.exists.return_value = True
    okf_source.ctx.graph = graph
    return graph


def _scheduled_execution(ingestion_source: str) -> ExecutionRequestInputClass:
    return ExecutionRequestInputClass(
        task="test",
        executorId="test",
        args={},
        requestedAt=0,
        source=ExecutionRequestSourceClass(
            type="SCHEDULED_INGESTION_SOURCE", ingestionSource=ingestion_source
        ),
    )


def _mock_managed_associations(
    okf_source: GitHubDocumentsSource, current: _Aspect
) -> MagicMock:
    """Existing associations, read by a run of a managed ingestion source."""
    graph = _mock_existing_associations(okf_source, current)
    okf_source.ctx.run_id = "urn:li:dataHubExecutionRequest:test-run"
    execution = _scheduled_execution("urn:li:dataHubIngestionSource:managed")
    graph.get_aspect.side_effect = lambda entity, cls: (
        execution
        if cls is ExecutionRequestInputClass
        else current
        if cls is type(current)
        else None
    )
    return graph


@pytest.mark.parametrize(
    "key,prefix,aspect_name",
    [
        ("owners", "corpuser", "ownership"),
        ("tags", "tag", "globalTags"),
        ("glossary_terms", "glossaryTerm", "glossaryTerms"),
    ],
)
@pytest.mark.parametrize("field_state", ["listed", "absent", "empty", "null", "blank"])
def test_okf_reimport_preserves_datahub_associations(
    okf_source: GitHubDocumentsSource,
    key: str,
    prefix: str,
    aspect_name: str,
    field_state: str,
) -> None:
    file_urn = f"urn:li:{prefix}:from-file"
    manual_urn = f"urn:li:{prefix}:from-datahub"
    current = _association_aspect(
        key,
        [
            (file_urn, _existing_okf_attribution()),
            (manual_urn, None),
            (file_urn, None),
            (f"urn:li:{prefix}:other-file", _existing_okf_attribution("docs/other")),
            (
                f"urn:li:{prefix}:other-bundle",
                _existing_okf_attribution(
                    bundle="https://github.com/example-org/other"
                ),
            ),
            (
                f"urn:li:{prefix}:other-root",
                _existing_okf_attribution(root_path="docs"),
            ),
            (
                f"urn:li:{prefix}:other-origin",
                _existing_okf_attribution(origin="other"),
            ),
        ],
    )
    _mock_existing_associations(okf_source, current)
    # Removing the key entirely must reconcile file-owned entries too.
    field = {
        "listed": f'{key}: ["{file_urn}"]\n',
        "absent": "",
        "empty": f"{key}: []\n",
        "null": f"{key}:\n",
        "blank": f'{key}: ""\n',
    }[field_state]
    _mock_github_client(
        okf_source,
        files=[GitHubFileInfo(path="docs/mau.md", size=100)],
        file_content=f"---\ntype: Guide\n{field}---\nBody",
    )
    workunits = list(okf_source.get_workunits())
    operations = _okf_patch_operations(workunits, aspect_name)
    expected = {
        f"urn:li:{prefix}:{suffix}"
        for suffix in ["other-file", "other-bundle", "other-root"]
    }
    expected.add(file_urn)
    if field_state == "listed":
        # The file now declares the value, so it takes over the DataHub edit.
        # Without an ingestion-source URN both share a key: OKF overwrites it.
        handover = [
            op for op in operations if "dataHubIngestionSource" not in op["path"]
        ]
        assert len(handover) == 1
        assert handover[0]["op"] == "add"
        assert file_urn in handover[0]["path"]
        value = handover[0]["value"]
        assert isinstance(value, dict)
        attribution = value["attribution"]
        assert isinstance(attribution, dict)
        assert attribution["sourceDetail"]["origin"] == "okf"
        operations = [op for op in operations if op not in handover]
    assert len(operations) == len(expected)
    for operation in operations:
        assert operation["op"] == "remove"
        assert any(urn in operation["path"] for urn in expected)
        assert "urn:li:dataHubIngestionSource:example" in operation["path"]
        assert "*" not in operation["path"]
        if key == "owners":
            assert "/TECHNICAL_OWNER/" in operation["path"]
    assert okf_source.report.okf_unresolved_values == 0


@pytest.mark.parametrize(
    "key,prefix,aspect_name,value",
    [
        ("owners", "corpuser", "ownership", "unresolved-owner"),
        ("owners", "corpuser", "ownership", "urn:li:tag:not-an-owner"),
        ("tags", "tag", "globalTags", "urn:li:corpuser:not-a-tag"),
        ("glossary_terms", "glossaryTerm", "glossaryTerms", "Missing Term"),
    ],
)
def test_okf_unresolved_values_never_remove_existing_associations(
    okf_source: GitHubDocumentsSource,
    key: str,
    prefix: str,
    aspect_name: str,
    value: str,
) -> None:
    current = _association_aspect(
        key,
        [(f"urn:li:{prefix}:previous", _existing_okf_attribution())],
    )
    graph = _mock_existing_associations(okf_source, current)
    graph.exists.return_value = False
    _mock_github_client(
        okf_source,
        files=[GitHubFileInfo(path="docs/mau.md", size=100)],
        file_content=f'---\ntype: Guide\n{key}: ["{value}"]\n---\nBody',
    )
    workunits = list(okf_source.get_workunits())
    assert _okf_patch_operations(workunits, aspect_name) == []
    okf = next(
        wu.get_aspect_of_type(OkfPropertiesClass)
        for wu in workunits
        if wu.get_aspect_of_type(OkfPropertiesClass)
    )
    assert okf is not None
    assert _raw_frontmatter_values(okf)[key] == [value]
    assert okf_source.report.okf_unresolved_values == 1


@pytest.mark.parametrize("fail_read", [False, True])
def test_okf_adds_owners_without_overwriting_datahub_owner(
    okf_source: GitHubDocumentsSource, fail_read: bool
) -> None:
    current = _association_aspect(
        "owners",
        [
            ("urn:li:corpuser:manual", None),
            ("urn:li:corpuser:previous", _existing_okf_attribution()),
        ],
    )
    _mock_existing_associations(okf_source, current, fail_read=fail_read)
    _mock_github_client(
        okf_source,
        files=[GitHubFileInfo(path="docs/mau.md", size=100)],
        file_content="---\ntype: Guide\nowners: [alice@example.com]\n---\nBody",
    )
    operations = _okf_patch_operations(list(okf_source.get_workunits()), "ownership")
    additions = [op for op in operations if op["op"] == "add"]
    assert len(additions) == 1
    assert additions[0]["value"]["owner"] == "urn:li:corpuser:alice@example.com"
    removals = [op for op in operations if op["op"] == "remove"]
    assert all("manual" not in op["path"] for op in operations)
    if fail_read:
        assert removals == []
        assert okf_source.report.warnings
    else:
        assert len(removals) == 1
        assert "urn:li:corpuser:previous" in removals[0]["path"]


def test_okf_partial_owner_resolution_is_add_only(
    okf_source: GitHubDocumentsSource,
) -> None:
    current = _association_aspect(
        "owners",
        [("urn:li:corpuser:previous", _existing_okf_attribution())],
    )
    graph = _mock_existing_associations(okf_source, current)
    graph.exists.return_value = False
    _mock_github_client(
        okf_source,
        files=[GitHubFileInfo(path="docs/mau.md", size=100)],
        file_content=(
            "---\ntype: Guide\nowners: [alice@example.com, unresolved-owner]\n---\nBody"
        ),
    )
    operations = _okf_patch_operations(list(okf_source.get_workunits()), "ownership")
    assert len(operations) == 1
    assert operations[0]["op"] == "add"
    assert operations[0]["value"]["owner"] == "urn:li:corpuser:alice@example.com"
    assert okf_source.report.okf_unresolved_values == 1


@pytest.mark.parametrize(
    "names,expected_urn",
    [
        ([], None),
        (["Active User", "Active User"], None),
        (["Another Term"], None),
        (["Active User"], "urn:li:glossaryTerm:term-0"),
    ],
)
def test_okf_term_names_require_one_exact_match(
    okf_source: GitHubDocumentsSource, names: List[str], expected_urn: Optional[str]
) -> None:
    terms = {
        f"urn:li:glossaryTerm:term-{index}": GlossaryTermInfoClass(
            name=name, definition="", termSource="INTERNAL"
        )
        for index, name in enumerate(names)
    }
    okf_source.ctx.graph = MagicMock()
    okf_source.ctx.graph.get_urns_by_filter.side_effect = lambda **kwargs: (
        list(terms) if kwargs["entity_types"] == ["glossaryTerm"] else []
    )
    okf_source.ctx.graph.get_aspect.side_effect = lambda urn, cls: (
        terms.get(urn) if cls is GlossaryTermInfoClass else None
    )
    _mock_github_client(
        okf_source,
        files=[GitHubFileInfo(path="docs/mau.md", size=100)],
        file_content="---\ntype: Guide\nglossary_terms: [Active User]\n---\nBody",
    )
    operations = _okf_patch_operations(
        list(okf_source.get_workunits()), "glossaryTerms"
    )
    if expected_urn is None:
        assert operations == []
        assert okf_source.report.okf_unresolved_values == 1
    else:
        assert len(operations) == 1
        assert operations[0]["value"]["urn"] == expected_urn
        assert okf_source.report.okf_unresolved_values == 0
    okf_source.ctx.graph.get_urns_by_filter.assert_any_call(
        entity_types=["glossaryTerm"],
        extraFilters=[
            {"field": "name", "condition": "EQUAL", "values": ["Active User"]}
        ],
    )


@pytest.mark.parametrize(
    "value,resolved",
    [("Active User", False), ("urn:li:glossaryTerm:term-id", True)],
)
def test_okf_term_resolution_without_graph(
    okf_source: GitHubDocumentsSource, value: str, resolved: bool
) -> None:
    _mock_github_client(
        okf_source,
        files=[GitHubFileInfo(path="docs/mau.md", size=100)],
        file_content=f'---\ntype: Guide\nglossary_terms: ["{value}"]\n---\nBody',
    )
    operations = _okf_patch_operations(
        list(okf_source.get_workunits()), "glossaryTerms"
    )
    assert len(operations) == int(resolved)
    if resolved:
        assert operations[0]["op"] == "add"
        assert operations[0]["value"]["urn"] == value
    assert okf_source.report.okf_unresolved_values == int(not resolved)


@pytest.mark.parametrize(
    "reason", ["malformed", "no-type", "disabled", "index-removed", "reserved"]
)
@pytest.mark.parametrize("edited_subtype", [False, True])
def test_stopping_concept_clears_only_okf_metadata(
    okf_source: GitHubDocumentsSource, reason: str, edited_subtype: bool
) -> None:
    original = "---\ntype: Guide\nowners: [alice@example.com]\n---\nBody"
    okf_source.config.okf_bundle_roots = []
    _mock_github_client(
        okf_source,
        files=[GitHubFileInfo(path="docs/mau.md", size=100)],
        file_content=original,
    )
    okf_source.client._index_file_paths = {"index.md"}
    fetch = okf_source.client.fetch_file_content
    assert isinstance(fetch, MagicMock)
    fetch.side_effect = lambda repo, path, ref: (
        '---\nokf_version: "0.2"\n---\nListing' if path == "index.md" else original
    )
    first = list(okf_source.get_workunits())
    stored = next(
        wu.get_aspect_of_type(OkfPropertiesClass)
        for wu in first
        if wu.get_aspect_of_type(OkfPropertiesClass)
    )
    assert stored is not None
    aspects: Dict[Type[_Aspect], _Aspect] = {
        OkfPropertiesClass: stored,
        SubTypesClass: SubTypesClass(
            typeNames=["Custom", "Guide"] if edited_subtype else ["Guide"]
        ),
        OwnershipClass: _association_aspect(
            "owners",
            [
                ("urn:li:corpuser:alice@example.com", _existing_okf_attribution()),
                ("urn:li:corpuser:carol", None),
            ],
        ),
        GlobalTagsClass: _association_aspect(
            "tags", [("urn:li:tag:metrics", _existing_okf_attribution())]
        ),
        GlossaryTermsClass: _association_aspect(
            "glossary_terms",
            [("urn:li:glossaryTerm:active-user", _existing_okf_attribution())],
        ),
    }
    graph = MagicMock()
    graph.get_urns_by_filter.return_value = []
    graph.get_aspect.side_effect = lambda urn, cls: aspects.get(cls)
    okf_source.ctx.graph = graph
    content = {
        "malformed": "---\ntype: Guide\nowners: [\n---\nBody",
        "no-type": "---\ntitle: Plain\n---\nBody",
    }.get(reason, original)
    if reason == "disabled":
        okf_source.config.enable_okf = False
    if reason == "index-removed":
        okf_source.client._index_file_paths = set()
    path = "docs/log.md" if reason == "reserved" else "docs/mau.md"
    _mock_github_client(
        okf_source, files=[GitHubFileInfo(path=path, size=100)], file_content=content
    )
    fetch = okf_source.client.fetch_file_content
    assert isinstance(fetch, MagicMock)
    fetch.side_effect = lambda repo, path, ref: (
        '---\nokf_version: "0.2"\n---\nListing' if path == "index.md" else content
    )
    workunits = list(okf_source.get_workunits())
    assert all(
        getattr(wu.metadata, "changeType", None) != ChangeTypeClass.DELETE
        for wu in workunits
    )
    neutral = [
        wu.get_aspect_of_type(OkfPropertiesClass)
        for wu in workunits
        if wu.get_aspect_of_type(OkfPropertiesClass) is not None
    ]
    assert len(neutral) == 1 and neutral[0] is not None
    assert neutral[0].to_obj() == {}
    # The marker goes last, so a failed removal is retried on the next run.
    okf_units = [
        wu
        for wu in workunits
        if wu.get_aspect_of_type(OkfPropertiesClass) is not None
        or "-okf-cleanup-" in wu.id
    ]
    assert okf_units[-1].get_aspect_of_type(OkfPropertiesClass) is not None
    subtypes = [
        wu.get_aspect_of_type(SubTypesClass)
        for wu in workunits
        if wu.get_aspect_of_type(SubTypesClass) is not None
    ]
    # Only the OKF type is removed; a subtype added in DataHub stays.
    assert len(subtypes) == 1 and subtypes[0] is not None
    assert subtypes[0].typeNames == (["Custom"] if edited_subtype else [])
    for aspect_name in ["ownership", "globalTags", "glossaryTerms"]:
        operations = _okf_patch_operations(workunits, aspect_name)
        assert len(operations) == 1 and operations[0]["op"] == "remove"
        assert "carol" not in operations[0]["path"]
    info = _document_infos_by_source_id(workunits)[
        make_file_source_id("example-org/analytics", path)
    ]
    assert info.contents.text == content


@pytest.mark.parametrize(
    "key,prefix,aspect_name",
    [
        ("owners", "corpuser", "ownership"),
        ("tags", "tag", "globalTags"),
        ("glossary_terms", "glossaryTerm", "glossaryTerms"),
    ],
)
@pytest.mark.parametrize("old_source", [None, "urn:li:dataHubIngestionSource:cli"])
def test_okf_source_change_moves_values_without_duplicates(
    okf_source: GitHubDocumentsSource,
    key: str,
    prefix: str,
    aspect_name: str,
    old_source: Optional[str],
) -> None:
    urn = f"urn:li:{prefix}:example"
    old = _existing_okf_attribution(root_path="previous")
    old.source = old_source
    current = _association_aspect(key, [(urn, old)])
    _mock_managed_associations(okf_source, current)
    _mock_github_client(
        okf_source,
        files=[GitHubFileInfo(path="docs/mau.md", size=100)],
        file_content=f'---\ntype: Guide\n{key}: ["{urn}"]\n---\nBody',
    )
    operations = _okf_patch_operations(list(okf_source.get_workunits()), aspect_name)
    assert [op["op"] for op in operations] == ["remove", "add"]
    assert (old_source or "") in operations[0]["path"]
    assert urn in operations[0]["path"]
    attribution = operations[1]["value"]["attribution"]
    assert isinstance(attribution, dict)
    assert attribution["source"] == "urn:li:dataHubIngestionSource:managed"


@pytest.mark.parametrize(
    "key,prefix,aspect_name",
    [
        ("owners", "corpuser", "ownership"),
        ("tags", "tag", "globalTags"),
        ("glossary_terms", "glossaryTerm", "glossaryTerms"),
    ],
)
def test_okf_declared_value_takes_over_datahub_edit(
    okf_source: GitHubDocumentsSource, key: str, prefix: str, aspect_name: str
) -> None:
    urn = f"urn:li:{prefix}:example"
    other_source = _existing_okf_attribution(origin="other")
    current = _association_aspect(key, [(urn, None), (urn, other_source)])
    _mock_managed_associations(okf_source, current)
    _mock_github_client(
        okf_source,
        files=[GitHubFileInfo(path="docs/mau.md", size=100)],
        file_content=f'---\ntype: Guide\n{key}: ["{urn}"]\n---\nBody',
    )
    operations = _okf_patch_operations(list(okf_source.get_workunits()), aspect_name)
    # The unattributed DataHub edit is removed; another source's value is kept.
    assert [op["op"] for op in operations] == ["remove", "add"]
    assert urn in operations[0]["path"]
    assert "dataHubIngestionSource" not in operations[0]["path"]
    attribution = operations[1]["value"]["attribution"]
    assert isinstance(attribution, dict)
    assert attribution["source"] == "urn:li:dataHubIngestionSource:managed"


@pytest.mark.parametrize("index_dir", ["", "docs/context/"])
@pytest.mark.parametrize("request_failed", [True, False])
def test_unreadable_index_skips_only_its_concepts(
    okf_source: GitHubDocumentsSource, index_dir: str, request_failed: bool
) -> None:
    okf_source.config.okf_bundle_roots = []
    concept = "docs/context/mau.md"
    plain = "docs/context/notes.md"
    index = f"{index_dir}index.md"
    _mock_github_client(
        okf_source,
        files=[
            GitHubFileInfo(path=concept, size=100),
            GitHubFileInfo(path=plain, size=100),
        ],
    )
    okf_source.client._index_file_paths = {index}
    # A failed request leaves an API error; an oversized file does not.
    okf_source.client._last_api_error = "HTTP 502" if request_failed else None
    fetch = okf_source.client.fetch_file_content
    assert isinstance(fetch, MagicMock)
    fetch.side_effect = lambda repo, path, ref: (
        None
        if path == index
        else "---\ntype: Guide\n---\nBody"
        if path == concept
        else "Plain notes"
    )
    graph = MagicMock()
    graph.get_urns_by_filter.return_value = []
    graph.get_aspect.return_value = None
    okf_source.ctx.graph = graph
    workunits = list(okf_source.get_workunits())
    infos = _document_infos_by_source_id(workunits)
    # Plain files import either way. A concept under an index that could not be
    # read keeps its previous import; an oversized index means no bundle.
    assert make_file_source_id("example-org/analytics", plain) in infos
    concept_id = make_file_source_id("example-org/analytics", concept)
    assert (concept_id in infos) is not request_failed
    assert not any(
        wu.get_aspect_of_type(OkfPropertiesClass) is not None for wu in workunits
    )


@pytest.mark.parametrize(
    "owner_type,type_urn,handed_over",
    [
        ("TECHNICAL_OWNER", None, True),
        ("TECHNICAL_OWNER", "urn:li:ownershipType:__system__technical_owner", True),
        ("BUSINESS_OWNER", None, False),
        ("NONE", "urn:li:ownershipType:__system__technical_owner", True),
        ("NONE", "urn:li:ownershipType:__system__business_owner", False),
        ("CUSTOM", "urn:li:ownershipType:steward", False),
    ],
)
def test_okf_owner_handover_keeps_other_roles(
    okf_source: GitHubDocumentsSource,
    owner_type: str,
    type_urn: Optional[str],
    handed_over: bool,
) -> None:
    owner = "urn:li:corpuser:bob"
    current = OwnershipClass(
        owners=[OwnerClass(owner=owner, type=owner_type, typeUrn=type_urn)]
    )
    _mock_managed_associations(okf_source, current)
    _mock_github_client(
        okf_source,
        files=[GitHubFileInfo(path="docs/mau.md", size=100)],
        file_content=f'---\ntype: Guide\nowners: ["{owner}"]\n---\nBody',
    )
    operations = _okf_patch_operations(list(okf_source.get_workunits()), "ownership")
    expected = ["remove", "add"] if handed_over else ["add"]
    assert [op["op"] for op in operations] == expected


@pytest.mark.parametrize("type_line", ["type:\n", 'type: ""\n', "type: 3\n", ""])
def test_frontmatter_without_concept_type_keeps_sync_back_skip(
    okf_source: GitHubDocumentsSource, type_line: str
) -> None:
    content = f"---\n{type_line}title: Plain\n---\nBody"
    info = MagicMock()
    info.customProperties = {
        "last_exported_content_hash": compute_file_content_hash(content)
    }
    graph = MagicMock()
    graph.get_urns_by_filter.return_value = []
    graph.get_aspect.side_effect = lambda urn, cls: (
        info if cls is DocumentInfoClass else None
    )
    okf_source.ctx.graph = graph
    _mock_github_client(
        okf_source,
        files=[GitHubFileInfo(path="docs/mau.md", size=100)],
        file_content=content,
    )
    list(okf_source.get_workunits())
    assert okf_source.report.files_skipped_unchanged == 1


@pytest.mark.parametrize(
    "hash_key", ["source_content_hash", "last_exported_content_hash", "content_hash"]
)
def test_hash_skip_contract(okf_source: GitHubDocumentsSource, hash_key: str) -> None:
    content = "---\ntype: Guide\n---\nBody"
    bundle = OkfBundleClass(
        location="https://github.com/example-org/analytics", rootPath="."
    )
    stored_hash = (
        compute_source_hash(content, bundle=bundle, enable_okf=True)
        if hash_key == "source_content_hash"
        else compute_file_content_hash(content)
    )
    graph = MagicMock()
    graph.get_urns_by_filter.return_value = []
    info = MagicMock()
    info.customProperties = {hash_key: stored_hash}
    graph.get_aspect.side_effect = lambda urn, cls: (
        info if cls is DocumentInfoClass else None
    )
    okf_source.ctx.graph = graph
    _mock_github_client(
        okf_source,
        files=[GitHubFileInfo(path="docs/mau.md", size=100)],
        file_content=content,
    )
    list(okf_source.get_workunits())
    # A concept matching only its last sync-back write is re-imported, so the
    # file takes over the values DataHub wrote into it. Plain documents still
    # skip on it (test_sync_back_skip_still_clears_stale_okf_state).
    skipped = hash_key == "source_content_hash"
    assert okf_source.report.files_skipped_unchanged == (1 if skipped else 0)
    assert okf_source.report.files_processed == (0 if skipped else 1)


def test_reads_one_pinned_snapshot(okf_source: GitHubDocumentsSource) -> None:
    calls: List[str] = []
    commit = "a" * 40

    def head(repo: str, branch: str) -> str:
        assert branch == "feature/docs"
        calls.append("head")
        return commit

    def tree(
        repo: str, ref: str, prefix: str, extensions: List[str]
    ) -> Tuple[List[GitHubFileInfo], bool]:
        assert calls == ["head"] and ref == commit
        calls.append("tree")
        return [GitHubFileInfo(path="docs/mau.md", size=100, sha="blob-at-head")], False

    def fetch(repo: str, path: str, ref: str) -> str:
        assert ref == commit
        calls.append(path)
        return (
            '---\nokf_version: "0.2"\n---\nListing'
            if path == "index.md"
            else "---\ntype: Guide\n---\nSnapshot body"
        )

    okf_source.config.branch = "feature/docs"
    okf_source.client.get_latest_commit_sha = MagicMock(  # type: ignore[method-assign]
        side_effect=head
    )
    okf_source.client.list_matching_files = MagicMock(  # type: ignore[method-assign]
        side_effect=tree
    )
    okf_source.client.fetch_file_content = MagicMock(  # type: ignore[method-assign]
        side_effect=fetch
    )
    okf_source.client._index_file_paths = {"index.md"}
    workunits = list(okf_source.get_workunits())
    properties = next(
        wu.get_aspect_of_type(OkfPropertiesClass)
        for wu in workunits
        if wu.get_aspect_of_type(OkfPropertiesClass)
    )
    assert properties is not None and properties.revision is not None
    assert properties.revision.commit == commit
    assert properties.revision.blobSha == "blob-at-head"
    assert properties.revision.ref == "feature/docs"
    info = _document_infos_by_source_id(workunits)[
        make_file_source_id("example-org/analytics", "docs/mau.md")
    ]
    assert info.contents.text == "Snapshot body"
    assert info.source is not None and info.source.externalUrl is not None
    assert "/feature/docs/" in info.source.externalUrl


def test_lookup_caches_are_shared_by_files_and_reset_each_run(
    okf_source: GitHubDocumentsSource,
) -> None:
    graph = MagicMock()
    graph.exists.side_effect = lambda urn: urn == "urn:li:corpuser:alice"

    def candidates(**kwargs: object) -> List[str]:
        if kwargs["entity_types"] != ["glossaryTerm"]:
            return []
        if kwargs["extraFilters"] == [
            {"field": "name", "condition": "EQUAL", "values": ["Active User"]}
        ]:
            return ["urn:li:glossaryTerm:missing-info", "urn:li:glossaryTerm:active"]
        return []

    graph.get_urns_by_filter.side_effect = candidates
    graph.get_aspect.side_effect = lambda urn, cls: (
        GlossaryTermInfoClass(name="Active User", definition="", termSource="INTERNAL")
        if cls is GlossaryTermInfoClass and urn == "urn:li:glossaryTerm:active"
        else None
    )
    okf_source.ctx.graph = graph
    _mock_github_client(
        okf_source,
        files=[GitHubFileInfo(path=f"docs/{name}.md", size=100) for name in ["a", "b"]],
        file_content=(
            "---\ntype: Guide\nowners: [alice, missing-user]\n"
            "glossary_terms: [Active User, Missing Term]\n---\nBody"
        ),
    )
    for run in [1, 2]:
        workunits = list(okf_source.get_workunits())
        assert len(_okf_patch_operations(workunits, "glossaryTerms")) == 2
        assert graph.exists.call_count == 2 * run
        term_searches = [
            c
            for c in graph.get_urns_by_filter.call_args_list
            if c.kwargs["entity_types"] == ["glossaryTerm"]
        ]
        assert len(term_searches) == 2 * run


def test_new_nested_bundle_removes_old_okf_values(
    okf_source: GitHubDocumentsSource,
) -> None:
    _mock_github_client(
        okf_source,
        files=[GitHubFileInfo(path="docs/mau.md", size=100)],
        file_content=(
            "---\ntype: Guide\nowners: [alice@example.com, bob@example.com]\n---\nBody"
        ),
    )
    first = list(okf_source.get_workunits())
    owners = [
        OwnerClass.from_obj(op["value"])
        for op in _okf_patch_operations(first, "ownership")
    ]
    _mock_existing_associations(okf_source, OwnershipClass(owners=owners))
    okf_source.client._index_file_paths = {"docs/index.md"}
    fetch = okf_source.client.fetch_file_content
    assert isinstance(fetch, MagicMock)
    fetch.side_effect = lambda repo, path, ref: (
        '---\nokf_version: "0.2"\n---\nListing'
        if path == "docs/index.md"
        else "---\ntype: Guide\nowners: [alice@example.com]\n---\nBody"
    )
    second = list(okf_source.get_workunits())
    operations = _okf_patch_operations(second, "ownership")
    assert [op["op"] for op in operations] == ["remove", "add"]
    assert "bob@example.com" in operations[0]["path"]
    owner = OwnerClass.from_obj(operations[1]["value"])
    assert owner.owner == "urn:li:corpuser:alice@example.com"
    assert owner.attribution is not None
    assert owner.attribution.sourceDetail["rootPath"] == "docs"
    assert owner.attribution.sourceDetail["conceptId"] == "mau"


def test_text_hash_stays_compatible_with_released_sync_back() -> None:
    assert compute_file_content_hash("# Hello") == (
        "bfb7d199fab898b4feab6d37fb8723adc1567544053ac085212ed6caa6b149f1"
    )


def test_mapping_bump_reimports_only_bundle_files(
    okf_source: GitHubDocumentsSource,
) -> None:
    content = "---\ntype: Guide\n---\nBody"
    okf_source.config.path_prefix = ""
    okf_source.config.okf_bundle_roots = ["docs"]
    bundle = OkfBundleClass(
        location="https://github.com/example-org/analytics", rootPath="docs"
    )
    infos: Dict[str, object] = {}
    for path, applicable_bundle in [("docs/mau.md", bundle), ("outside.md", None)]:
        info = MagicMock()
        info.customProperties = {
            "source_content_hash": compute_source_hash(
                content, bundle=applicable_bundle, enable_okf=True
            )
        }
        source_id = make_file_source_id("example-org/analytics", path)
        infos[f"urn:li:document:{normalize_document_id(source_id)}"] = info
    graph = MagicMock()
    graph.get_urns_by_filter.return_value = []
    graph.get_aspect.side_effect = lambda urn, cls: (
        infos.get(urn) if cls is DocumentInfoClass else None
    )
    okf_source.ctx.graph = graph
    _mock_github_client(
        okf_source,
        files=[
            GitHubFileInfo(path=path, size=100)
            for path in ["docs/mau.md", "outside.md"]
        ],
        file_content=content,
    )
    with patch(
        "datahub.ingestion.source.github_documents.github_documents_source.MAPPING_VERSION",
        "next",
    ):
        workunits = list(okf_source.get_workunits())
    assert okf_source.report.files_processed == 1
    assert okf_source.report.files_skipped_unchanged == 1
    infos_emitted = _document_infos_by_source_id(workunits)
    assert make_file_source_id("example-org/analytics", "docs/mau.md") in infos_emitted
    assert (
        make_file_source_id("example-org/analytics", "outside.md") not in infos_emitted
    )


def test_sync_back_skip_still_clears_stale_okf_state(
    okf_source: GitHubDocumentsSource,
) -> None:
    content = "Plain document exported from DataHub"
    okf_source.config.enable_okf = False
    stored = OkfPropertiesClass(
        bundle=OkfBundleClass(
            location="https://github.com/example-org/analytics", rootPath="."
        ),
        conceptId="docs/mau",
        type="Guide",
        raw="---\ntype: Guide\n---\n",
    )
    info = MagicMock()
    info.customProperties = {
        "last_exported_content_hash": compute_file_content_hash(content)
    }
    graph = MagicMock()
    graph.get_urns_by_filter.return_value = []
    graph.get_aspect.side_effect = lambda urn, cls: (
        stored
        if cls is OkfPropertiesClass
        else info
        if cls is DocumentInfoClass
        else None
    )
    okf_source.ctx.graph = graph
    _mock_github_client(
        okf_source,
        files=[GitHubFileInfo(path="docs/mau.md", size=100)],
        file_content=content,
    )
    workunits = list(okf_source.get_workunits())
    assert okf_source.report.files_skipped_unchanged == 1
    neutral = [
        wu.get_aspect_of_type(OkfPropertiesClass)
        for wu in workunits
        if wu.get_aspect_of_type(OkfPropertiesClass) is not None
    ]
    assert len(neutral) == 1 and neutral[0] is not None
    assert neutral[0].to_obj() == {}
    assert all(
        getattr(wu.metadata, "changeType", None) != ChangeTypeClass.DELETE
        for wu in workunits
    )


@pytest.mark.parametrize("with_graph", [False, True])
def test_default_import_preserves_frontmatter(
    source: GitHubDocumentsSource, with_graph: bool
) -> None:
    content = (
        "---\ntype: Guide\ntitle: Concept title\nowners: [alice@example.com]\n"
        "tags: [metrics]\nglossary_terms: [urn:li:glossaryTerm:active-user]\n---\nBody"
    )
    source.config.okf_bundle_roots = [""]
    if with_graph:
        graph = MagicMock()
        graph.get_aspect.return_value = None
        graph.get_urns_by_filter.return_value = []
        source.ctx.graph = graph
    _mock_github_client(
        source,
        files=[GitHubFileInfo(path="docs/guide.md", size=len(content))],
        file_content=content,
    )
    source.client._index_file_paths = {"index.md"}
    workunits = list(source.get_workunits())
    info = _document_infos_by_source_id(workunits)[
        make_file_source_id(source.config.repository, "docs/guide.md")
    ]
    assert info.contents.text == content
    assert info.title == "guide.md"
    assert info.customProperties is not None
    assert info.customProperties["source_content_hash"] == compute_file_content_hash(
        content
    )
    assert not any(wu.get_aspect_of_type(OkfPropertiesClass) for wu in workunits)
    assert not any(wu.get_aspect_of_type(SubTypesClass) for wu in workunits)
    for aspect_name in ["ownership", "globalTags", "glossaryTerms"]:
        assert not _okf_patch_operations(workunits, aspect_name)
    assert not source.report.warnings
    assert source.report.okf_files_processed == 0
    assert isinstance(source.client.fetch_file_content, MagicMock)
    source.client.fetch_file_content.assert_called_once_with(
        source.config.repository, "docs/guide.md", "abc123"
    )
    if with_graph:
        graph.get_entity_aspect_specs.assert_not_called()


@pytest.mark.parametrize("registry", ["unsupported", "forbidden"])
def test_okf_opt_in_does_not_check_server_support(
    okf_source: GitHubDocumentsSource, registry: str
) -> None:
    graph = MagicMock()
    if registry == "forbidden":
        graph.get_entity_aspect_specs.side_effect = PermissionError("403 Forbidden")
    else:
        graph.get_entity_aspect_specs.return_value = EntityAspectSpecs(
            entity_aspects={"document": {"documentInfo"}}
        )
    graph.get_aspect.return_value = None
    graph.get_urns_by_filter.return_value = []
    okf_source.ctx.graph = graph
    _mock_github_client(
        okf_source,
        files=[GitHubFileInfo(path="docs/guide.md", size=100)],
        file_content=(
            "---\ntype: Guide\nowners: [alice@example.com]\ntags: [metrics]\n"
            "glossary_terms: [urn:li:glossaryTerm:active-user]\n---\nBody"
        ),
    )
    workunits = list(okf_source.get_workunits())
    assert any(wu.get_aspect_of_type(OkfPropertiesClass) for wu in workunits)
    assert any(wu.get_aspect_of_type(SubTypesClass) for wu in workunits)
    for aspect_name in ["ownership", "globalTags", "glossaryTerms"]:
        assert _okf_patch_operations(workunits, aspect_name)
    info = _document_infos_by_source_id(workunits)[
        make_file_source_id(okf_source.config.repository, "docs/guide.md")
    ]
    assert info.contents.text == "Body"
    assert okf_source.report.okf_files_processed == 1
    graph.get_entity_aspect_specs.assert_not_called()
    assert not okf_source.report.warnings


@pytest.mark.parametrize(
    "scenario",
    ["concept", "plain", "malformed", "no-type", "disabled", "outside", "reserved"],
)
def test_source_never_emits_delete_workunits(
    okf_source: GitHubDocumentsSource, scenario: str
) -> None:
    content = {
        "plain": "# Plain document",
        "malformed": "---\ntype: Guide\nowners: [\n---\nBody",
        "no-type": "---\ntitle: Plain document\n---\nBody",
    }.get(scenario, "---\ntype: Guide\nowners: [alice@example.com]\n---\nBody")
    if scenario == "disabled":
        okf_source.config.enable_okf = False
    if scenario == "outside":
        okf_source.config.okf_bundle_roots = []
    path = "docs/log.md" if scenario == "reserved" else "docs/mau.md"
    _mock_github_client(
        okf_source,
        files=[GitHubFileInfo(path=path, size=100)],
        file_content=content,
    )
    stored = OkfPropertiesClass(
        bundle=OkfBundleClass(
            location="https://github.com/example-org/analytics", rootPath="."
        ),
        conceptId="docs/mau",
        type="Guide",
        raw="---\ntype: Guide\n---\n",
    )
    aspects: Dict[Type[_Aspect], _Aspect] = {
        OkfPropertiesClass: stored,
        SubTypesClass: SubTypesClass(typeNames=["Guide"]),
        OwnershipClass: _association_aspect(
            "owners",
            [("urn:li:corpuser:alice@example.com", _existing_okf_attribution())],
        ),
    }
    graph = MagicMock()
    graph.get_urns_by_filter.return_value = []
    graph.get_aspect.side_effect = lambda urn, cls: aspects.get(cls)
    okf_source.ctx.graph = graph
    workunits = list(okf_source.get_workunits())
    assert workunits
    assert all(
        getattr(wu.metadata, "changeType", None) != ChangeTypeClass.DELETE
        for wu in workunits
    )
    for wu in workunits:
        metadata = wu.metadata
        mcp = (
            metadata.make_mcp()
            if isinstance(metadata, MetadataChangeProposalWrapper)
            else metadata
        )
        assert isinstance(mcp, MetadataChangeProposalClass)
        assert mcp.aspect is not None
        properties = wu.get_aspect_of_type(OkfPropertiesClass)
        if properties is not None:
            assert getattr(wu.metadata, "changeType", None) == ChangeTypeClass.UPSERT
            assert (properties.raw is not None) == (scenario == "concept")
            if properties.raw is None:
                assert json.loads(mcp.aspect.value) == {}


@pytest.mark.parametrize("unchanged", [False, True])
def test_already_neutral_okf_state_is_not_cleaned_again(
    okf_source: GitHubDocumentsSource, unchanged: bool
) -> None:
    content = "# Plain document"
    info = MagicMock()
    info.customProperties = (
        {"source_content_hash": compute_file_content_hash(content)} if unchanged else {}
    )
    neutral = OkfPropertiesClass()
    graph = MagicMock()
    graph.get_urns_by_filter.return_value = []
    graph.get_aspect.side_effect = lambda urn, cls: (
        neutral
        if cls is OkfPropertiesClass
        else info
        if cls is DocumentInfoClass
        else None
    )
    okf_source.ctx.graph = graph
    _mock_github_client(
        okf_source,
        files=[GitHubFileInfo(path="docs/mau.md", size=100)],
        file_content=content,
    )
    workunits = list(okf_source.get_workunits())
    assert not any(wu.get_aspect_of_type(OkfPropertiesClass) for wu in workunits)
    assert not any(wu.get_aspect_of_type(SubTypesClass) for wu in workunits)
    for aspect_name in ["ownership", "globalTags", "glossaryTerms"]:
        assert _okf_patch_operations(workunits, aspect_name) == []
    assert all(
        getattr(wu.metadata, "changeType", None) != ChangeTypeClass.DELETE
        for wu in workunits
    )
