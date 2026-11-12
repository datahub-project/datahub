import base64
from typing import Dict, List
from unittest.mock import MagicMock

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
            f"{API}/repository/files/docs%2Fguides%2Fa.md",
            json={
                "size": 7,
                "encoding": "base64",
                "content": base64.b64encode(b"# Hello").decode(),
            },
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
            f"{API}/repository/files/a.md",
            json={
                "size": 3,
                "encoding": "base64",
                "content": base64.b64encode(b"# A").decode(),
            },
        )
        list(source.get_workunits())

    assert source.report.files_truncated_by_limit
    assert {w.title for w in source.report.warnings} == {"gitlab-files-truncated"}


def _source(**config: object) -> GitLabDocumentsSource:
    return GitLabDocumentsSource(
        GitLabDocumentsSourceConfig.model_validate(
            {"gitlab_token": "glpat-test", "repository": "acme/data/docs", **config}
        ),
        PipelineContext(run_id="test-run"),
    )


def _encoded(text: str) -> Dict[str, object]:
    raw = text.encode()
    return {
        "size": len(raw),
        "encoding": "base64",
        "content": base64.b64encode(raw).decode(),
    }


def test_failed_file_fetch_keeps_previous_import_and_warns() -> None:
    source = _source(base_url=BASE, create_repo_root_document=False)
    source.stale_entity_removal_handler.add_entity_to_state = MagicMock()  # type: ignore[method-assign]
    with requests_mock.Mocker() as m:
        m.get(f"{API}/repository/branches/main", json={"commit": {"id": "abc123"}})
        m.get(
            f"{API}/repository/tree",
            json=[{"id": "blob1", "type": "blob", "path": "a.md"}],
        )
        m.get(f"{API}/repository/files/a.md", status_code=500)
        list(source.get_workunits())

    # Registered as seen, so stale removal does not delete the earlier import.
    source.stale_entity_removal_handler.add_entity_to_state.assert_called_once()
    assert source.report.files_skipped == 1
    assert "gitlab-file-not-read" in {w.title for w in source.report.warnings}


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
        m.get(f"{api}/repository/files/my%20notes.md", json=_encoded("# Notes"))
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
