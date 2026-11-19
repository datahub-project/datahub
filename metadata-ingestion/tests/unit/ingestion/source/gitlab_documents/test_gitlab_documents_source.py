from typing import Dict, List
from unittest.mock import MagicMock, patch

import pytest
import requests_mock

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.gitlab_documents.gitlab_documents_config import (
    GitLabDocumentsSourceConfig,
)
from datahub.ingestion.source.gitlab_documents.gitlab_documents_source import (
    GitLabDocumentsSource,
)
from datahub.ingestion.source.repository_documents.paths import (
    make_file_source_id,
    normalize_document_id,
)
from datahub.metadata.schema_classes import (
    BrowsePathsV2Class,
    DataPlatformInstanceClass,
    DocumentInfoClass,
    DocumentSourceTypeClass,
)

BASE = "https://gitlab.example.com"
API = f"{BASE}/api/v4/projects/acme%2Fdata%2Fdocs"


def test_config_resolves_base_url_from_repository_url() -> None:
    config = GitLabDocumentsSourceConfig.model_validate(
        {
            "gitlab_token": "glpat-test",
            "repository": f"{BASE}/acme/data/docs/-/tree/main",
        }
    )
    assert config.base_url == BASE
    assert config.repository == "acme/data/docs"


def test_config_project_url_overrides_default_base_url() -> None:
    # Recipes from the UI always carry base_url: https://gitlab.com.
    config = GitLabDocumentsSourceConfig.model_validate(
        {
            "gitlab_token": "glpat-test",
            "base_url": "https://gitlab.com",
            "repository": f"{BASE}/acme/data/docs",
        }
    )
    assert config.base_url == BASE
    assert config.repository == "acme/data/docs"


def test_config_rejects_empty_token() -> None:
    with pytest.raises(ValueError):
        GitLabDocumentsSourceConfig.model_validate(
            {"gitlab_token": " ", "repository": "acme/docs"}
        )


def test_emits_gitlab_documents_with_project_urls() -> None:
    source = GitLabDocumentsSource(
        GitLabDocumentsSourceConfig.model_validate(
            {
                "gitlab_token": "glpat-test",
                "base_url": BASE,
                "repository": "acme/data/docs",
                "path_prefix": "docs",
                "include_organization_in_browse_path": True,
            }
        ),
        PipelineContext(run_id="test-run"),
    )
    with requests_mock.Mocker() as m:
        m.get(f"{API}/repository/branches/main", json={"commit": {"id": "abc123"}})
        m.get(
            f"{API}/repository/tree",
            json=[{"id": "blob1", "type": "blob", "path": "docs/guides/a.md"}],
        )
        m.get(
            f"{API}/repository/files/docs%2Fguides%2Fa.md/raw",
            text="# Hello",
        )
        workunits = [
            wu for wu in source.get_workunits() if isinstance(wu, MetadataWorkUnit)
        ]

    infos = [
        info
        for wu in workunits
        if (info := wu.get_aspect_of_type(DocumentInfoClass)) is not None
    ]
    by_path: Dict[str, DocumentInfoClass] = {
        (info.customProperties.get("gitlab_file_path"))
        or info.customProperties.get("gitlab_directory_path")
        or "": info
        for info in infos
    }

    file_info = by_path["docs/guides/a.md"]
    assert file_info.customProperties["gitlab_repo"] == "acme/data/docs"
    assert file_info.customProperties["gitlab_commit_sha"] == "abc123"
    assert file_info.customProperties["gitlab_blob_sha"] == "blob1"
    assert not any(key.startswith("github_") for key in file_info.customProperties)
    assert file_info.source is not None
    assert file_info.source.externalUrl == (
        f"{BASE}/acme/data/docs/-/blob/main/docs/guides/a.md"
    )

    folder_info = by_path["docs/guides"]
    assert folder_info.source is not None
    assert folder_info.source.externalUrl == (
        f"{BASE}/acme/data/docs/-/tree/main/docs/guides"
    )

    platforms = {
        aspect.platform
        for wu in workunits
        if (aspect := wu.get_aspect_of_type(DataPlatformInstanceClass)) is not None
    }
    assert platforms == {"urn:li:dataPlatform:gitlab"}

    browse_ids: List[List[str]] = [
        [entry.id for entry in browse.path]
        for wu in workunits
        if (browse := wu.get_aspect_of_type(BrowsePathsV2Class)) is not None
    ]
    assert ["acme/data", "docs", "guides"] in browse_ids


def test_max_files_warning_names_gitlab() -> None:
    source = GitLabDocumentsSource(
        GitLabDocumentsSourceConfig.model_validate(
            {
                "gitlab_token": "glpat-test",
                "base_url": BASE,
                "repository": "acme/data/docs",
                "max_files": 1,
                "create_repo_root_document": False,
            }
        ),
        PipelineContext(run_id="test-run"),
    )
    with requests_mock.Mocker() as m:
        m.get(f"{API}/repository/branches/main", json={"commit": {"id": "abc123"}})
        m.get(
            f"{API}/repository/tree",
            json=[
                {"id": "blob1", "type": "blob", "path": "a.md"},
                {"id": "blob2", "type": "blob", "path": "b.md"},
            ],
        )
        m.get(
            f"{API}/repository/files/a.md/raw",
            text="# A",
        )
        list(source.get_workunits())

    assert source.report.files_truncated_by_limit
    assert {w.title for w in source.report.warnings} == {
        "Matching files truncated by max_files"
    }


def _source(**config: object) -> GitLabDocumentsSource:
    return GitLabDocumentsSource(
        GitLabDocumentsSourceConfig.model_validate(
            {"gitlab_token": "glpat-test", "repository": "acme/data/docs", **config}
        ),
        PipelineContext(run_id="test-run"),
    )


def test_failed_file_fetch_keeps_previous_import_and_warns() -> None:
    source = _source(base_url=BASE, create_repo_root_document=False)
    previous_urn = "urn:li:document:" + normalize_document_id(
        make_file_source_id("acme/data/docs", "a.md", source_prefix="gitlab")
    )
    handler = source.stale_entity_removal_handler
    handler.checkpointing_enabled = True
    handler.add_entity_to_state = MagicMock()  # type: ignore[method-assign]
    source.state_provider.get_last_checkpoint = MagicMock(  # type: ignore[method-assign]
        return_value=MagicMock(state=MagicMock(urns=[previous_urn]))
    )
    with requests_mock.Mocker() as m:
        m.get(f"{API}/repository/branches/main", json={"commit": {"id": "abc123"}})
        m.get(
            f"{API}/repository/tree",
            json=[{"id": "blob1", "type": "blob", "path": "a.md"}],
        )
        m.get(f"{API}/repository/files/a.md/raw", status_code=500)
        with patch("time.sleep"):
            list(source.get_workunits_internal())

    # Registered as seen, so stale removal does not delete the earlier import.
    handler.add_entity_to_state.assert_called_once_with("document", previous_urn)
    assert source.report.files_skipped == 1
    assert "Repository file not read" in {w.title for w in source.report.warnings}


def test_unresolved_branch_error_includes_the_api_cause() -> None:
    source = _source(base_url=BASE)
    with requests_mock.Mocker() as m:
        m.get(f"{API}/repository/branches/main", status_code=401)
        with pytest.raises(RuntimeError, match="401"):
            list(source.get_workunits())


def test_test_connection_resolves_branch_then_lists_tree() -> None:
    config = {
        "gitlab_token": "glpat-test",
        "base_url": BASE,
        "repository": "acme/data/docs",
    }
    with requests_mock.Mocker() as m:
        m.get(f"{API}/repository/branches/main", json={"commit": {"id": "abc123"}})
        m.get(f"{API}/repository/tree", json=[])
        report = GitLabDocumentsSource.test_connection(config)
        assert m.request_history[-1].qs["ref"] == ["abc123"]
    assert report.basic_connectivity is not None
    assert report.basic_connectivity.capable is True

    with requests_mock.Mocker() as m:
        m.get(f"{API}/repository/branches/main", status_code=401)
        report = GitLabDocumentsSource.test_connection(config)
    assert report.basic_connectivity is not None
    assert report.basic_connectivity.capable is False
    assert "401" in (report.basic_connectivity.failure_reason or "")


def test_self_managed_instance_under_a_path_with_special_characters() -> None:
    base = "https://host.example.com/gitlab"
    api = f"{base}/api/v4/projects/acme%2Fdocs"
    source = _source(
        base_url=base,
        repository="acme/docs",
        branch="release/1.0",
        create_repo_root_document=False,
        document_import_mode="EXTERNAL",
    )
    with requests_mock.Mocker() as m:
        m.get(
            f"{api}/repository/branches/release%2F1.0",
            json={"commit": {"id": "abc123"}},
        )
        m.get(
            f"{api}/repository/tree",
            json=[{"id": "blob1", "type": "blob", "path": "my notes.md"}],
        )
        m.get(f"{api}/repository/files/my%20notes.md/raw", text="# Notes")
        infos = [
            info
            for wu in source.get_workunits()
            if isinstance(wu, MetadataWorkUnit)
            and (info := wu.get_aspect_of_type(DocumentInfoClass)) is not None
        ]

    assert len(infos) == 1
    assert infos[0].source is not None
    assert infos[0].source.sourceType == DocumentSourceTypeClass.EXTERNAL
    assert infos[0].source.externalUrl == (
        f"{base}/acme/docs/-/blob/release/1.0/my%20notes.md"
    )


def test_tree_without_pagination_headers_skips_stale_removal() -> None:
    # A proxy that strips pagination headers leaves the listing possibly partial,
    # so the run must not soft-delete documents it did not see.
    source = _source(base_url=BASE, create_repo_root_document=False)
    # Images are listed but not imported, so no file is fetched.
    page = [{"id": f"b{i}", "type": "blob", "path": f"{i}.png"} for i in range(100)]
    with requests_mock.Mocker() as m:
        m.get(f"{API}/repository/branches/main", json={"commit": {"id": "abc123"}})
        m.get(f"{API}/repository/tree", json=page)
        # The follow-up offset request shows more entries exist.
        m.get(
            f"{API}/repository/tree?page=2",
            json=[{"id": "b100", "type": "blob", "path": "100.png"}],
        )
        list(source.get_workunits())

    assert source.report.tree_truncated is True
    assert "Repository tree truncated" in {w.title for w in source.report.warnings}
    assert not source.report.failures


def _urns_and_instances(source: GitLabDocumentsSource) -> Dict[str, str]:
    with requests_mock.Mocker() as m:
        m.get(f"{API}/repository/branches/main", json={"commit": {"id": "abc123"}})
        m.get(
            f"{API}/repository/tree",
            json=[{"id": "blob1", "type": "blob", "path": "guide.md"}],
        )
        m.get(f"{API}/repository/files/guide.md/raw", text="# Guide")
        workunits = [
            wu for wu in source.get_workunits() if isinstance(wu, MetadataWorkUnit)
        ]
    return {
        wu.get_urn(): aspect.instance or ""
        for wu in workunits
        if (aspect := wu.get_aspect_of_type(DataPlatformInstanceClass)) is not None
    }


def test_platform_instance_separates_ids_and_sets_the_instance() -> None:
    without = _urns_and_instances(
        _source(base_url=BASE, create_repo_root_document=False)
    )
    with_instance = _urns_and_instances(
        _source(base_url=BASE, create_repo_root_document=False, platform_instance="eu")
    )

    assert set(without) == {
        "urn:li:document:"
        + normalize_document_id(
            make_file_source_id("acme/data/docs", "guide.md", source_prefix="gitlab")
        )
    }
    assert set(without.values()) == {
        "urn:li:dataPlatformInstance:(urn:li:dataPlatform:gitlab,acme/data/docs)"
    }
    assert set(with_instance) == {
        "urn:li:document:gitlab-eu.acme.data.docs.guide-288074fadb9f1197"
    }
    assert set(with_instance.values()) == {
        "urn:li:dataPlatformInstance:(urn:li:dataPlatform:gitlab,eu)"
    }


@pytest.mark.parametrize("platform_instance", ["eu.prod", "eu,prod", "eu(1)", "  "])
def test_platform_instance_rejects_separators_and_blanks(
    platform_instance: str,
) -> None:
    with pytest.raises(ValueError, match="platform_instance"):
        _source(platform_instance=platform_instance)


def test_platform_instance_is_stripped() -> None:
    assert _source(platform_instance="  eu ").config.platform_instance == "eu"


def test_tree_truncation_warning_names_the_api_error() -> None:
    # When the follow-up request fails, the warning must say why the listing
    # could not be confirmed complete.
    source = _source(base_url=BASE, create_repo_root_document=False)
    page = [{"id": f"b{i}", "type": "blob", "path": f"{i}.png"} for i in range(100)]
    with requests_mock.Mocker() as m:
        m.get(f"{API}/repository/branches/main", json={"commit": {"id": "abc123"}})
        m.get(f"{API}/repository/tree", json=page)
        m.get(f"{API}/repository/tree?page=2", status_code=403)
        list(source.get_workunits())

    [warning] = [
        w for w in source.report.warnings if w.title == "Repository tree truncated"
    ]
    assert any("HTTP 403" in context for context in warning.context)
