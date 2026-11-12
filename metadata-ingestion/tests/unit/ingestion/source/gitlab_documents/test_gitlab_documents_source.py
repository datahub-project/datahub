import base64
from typing import Dict, List

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
