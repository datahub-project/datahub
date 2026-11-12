import base64
from unittest.mock import patch

import pytest
import requests_mock

from datahub.ingestion.source.gitlab_documents.gitlab_api import (
    GitLabApiClient,
    parse_project_path,
)

API = "https://gitlab.com/api/v4/projects/acme%2Fdata%2Fdocs"


@pytest.mark.parametrize(
    "raw, base_url, expected",
    [
        ("acme/docs", None, ("https://gitlab.com", "acme/docs")),
        ("acme/data/docs", None, ("https://gitlab.com", "acme/data/docs")),
        (
            "https://gitlab.example.com/acme/docs/-/tree/main/docs",
            None,
            ("https://gitlab.example.com", "acme/docs"),
        ),
        (
            "https://host.example.com/gitlab/acme/docs.git",
            "https://host.example.com/gitlab/",
            ("https://host.example.com/gitlab", "acme/docs"),
        ),
    ],
)
def test_parse_project_path(raw: str, base_url: str, expected: tuple) -> None:
    assert parse_project_path(raw, base_url) == expected


@pytest.mark.parametrize(
    "raw, base_url",
    [
        ("docs", None),
        ("https://gitlab.com/acme", None),
        ("https://other.example.com/acme/docs", "https://gitlab.example.com"),
    ],
)
def test_parse_project_path_rejects_invalid(raw: str, base_url: str) -> None:
    with pytest.raises(ValueError):
        parse_project_path(raw, base_url)


def test_list_matching_files_follows_pagination_and_filters() -> None:
    client = GitLabApiClient("glpat-test")
    with requests_mock.Mocker() as m:
        m.get(
            f"{API}/repository/tree?ref=main&recursive=true&per_page=100",
            json=[
                {"id": "b1", "type": "blob", "path": "docs/a.md"},
                {"id": "t1", "type": "tree", "path": "docs/guides"},
                {"id": "b2", "type": "blob", "path": "src/app.py"},
                {"id": "b3", "type": "blob", "path": "index.md"},
            ],
            headers={"Link": f'<{API}/repository/tree?page=2>; rel="next"'},
        )
        m.get(
            f"{API}/repository/tree?page=2",
            json=[{"id": "b4", "type": "blob", "path": "docs/guides/b.md"}],
        )

        files, truncated = client.list_matching_files(
            "acme/data/docs", "main", "docs", [".md"]
        )

        assert m.request_history[0].headers["PRIVATE-TOKEN"] == "glpat-test"
    assert [(f.path, f.sha) for f in files] == [
        ("docs/a.md", "b1"),
        ("docs/guides/b.md", "b4"),
    ]
    assert not truncated
    assert client.index_file_paths == {"index.md"}


def test_list_matching_files_raises_with_error_detail() -> None:
    client = GitLabApiClient("glpat-test")
    with requests_mock.Mocker() as m:
        m.get(f"{API}/repository/tree", status_code=404)
        with pytest.raises(RuntimeError, match="HTTP 404"):
            client.list_matching_files("acme/data/docs", "main", "", [".md"])


def test_fetch_file_content_decodes_base64_and_skips_large_files() -> None:
    client = GitLabApiClient("glpat-test")
    with requests_mock.Mocker() as m:
        m.get(
            f"{API}/repository/files/docs%2Fa.md?ref=main",
            json={
                "size": 7,
                "encoding": "base64",
                "content": base64.b64encode(b"# Hello").decode(),
            },
        )
        m.get(
            f"{API}/repository/files/docs%2Fbig.md?ref=main",
            json={"size": 5_000_000, "encoding": "base64", "content": ""},
        )
        assert client.fetch_file_content("acme/data/docs", "docs/a.md", "main") == (
            "# Hello"
        )
        assert (
            client.fetch_file_content("acme/data/docs", "docs/big.md", "main") is None
        )


def test_get_latest_commit_sha_retries_once_on_rate_limit() -> None:
    client = GitLabApiClient("glpat-test")
    with requests_mock.Mocker() as m, patch("time.sleep") as sleep:
        m.get(
            f"{API}/repository/branches/main",
            [
                {"status_code": 429, "headers": {"Retry-After": "3"}},
                {"json": {"commit": {"id": "abc123"}}},
            ],
        )
        assert client.get_latest_commit_sha("acme/data/docs", "main") == "abc123"
    sleep.assert_called_once_with(3)


def test_html_response_is_treated_as_failure() -> None:
    client = GitLabApiClient("glpat-test")
    with requests_mock.Mocker() as m:
        m.get(f"{API}/repository/branches/main", text="<html>login</html>")
        assert client.get_latest_commit_sha("acme/data/docs", "main") == "unknown"
