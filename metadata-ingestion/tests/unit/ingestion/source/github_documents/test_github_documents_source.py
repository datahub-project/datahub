"""Unit tests for GitHubDocumentsSource."""

from unittest.mock import MagicMock, patch

import pytest
from pydantic import SecretStr

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.github_documents.github_api import GitHubFileInfo
from datahub.ingestion.source.github_documents.github_documents_config import (
    GitHubDocumentsSourceConfig,
)
from datahub.ingestion.source.github_documents.github_documents_source import (
    GitHubDocumentsSource,
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


def test_get_workunits_emits_folder_then_file(source: GitHubDocumentsSource) -> None:
    source.client.list_matching_files = MagicMock(  # type: ignore[method-assign]
        return_value=[GitHubFileInfo(path="docs/readme.md", size=12)]
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
