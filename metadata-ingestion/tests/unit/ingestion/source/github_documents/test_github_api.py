"""Unit tests for GitHub API helpers."""

import base64
from unittest.mock import MagicMock, patch

import pytest

from datahub.ingestion.source.github_documents.github_api import (
    GitHubApiClient,
    GitHubFileInfo,
    _matches_filters,
    collect_intermediate_directories,
    make_dir_source_id,
    make_file_source_id,
    make_repo_source_id,
    normalize_document_id,
    parse_repo_identifier,
    resolve_parent_dir_source_id,
)


def test_parse_repo_identifier_shorthand() -> None:
    assert parse_repo_identifier("acme/docs") == "acme/docs"


def test_parse_repo_identifier_url() -> None:
    assert parse_repo_identifier("https://github.com/acme/docs.git") == "acme/docs"


def test_parse_repo_identifier_invalid() -> None:
    with pytest.raises(ValueError):
        parse_repo_identifier("not-a-repo")


def test_make_file_and_dir_source_ids() -> None:
    assert (
        make_file_source_id("acme/docs", "guides/setup.md")
        == "github.acme.docs.guides.setup"
    )
    assert make_dir_source_id("acme/docs", "guides") == "github.acme.docs.guides._dir"


def test_collect_intermediate_directories() -> None:
    files = [GitHubFileInfo(path="docs/guides/setup.md", size=10)]
    dirs = collect_intermediate_directories(files, "docs")
    assert dirs == {"docs/guides"}


def test_resolve_parent_dir_source_id() -> None:
    parent = resolve_parent_dir_source_id("acme/docs", "docs/guides/setup.md", "docs")
    assert parent == make_dir_source_id("acme/docs", "docs/guides")


def test_resolve_parent_dir_source_id_uses_repo_root() -> None:
    repo_root = make_repo_source_id("acme/docs")
    parent = resolve_parent_dir_source_id(
        "acme/docs",
        "readme.md",
        "",
        repo_root_source_id=repo_root,
    )
    assert parent == repo_root


def test_matches_filters_respects_path_prefix_boundary() -> None:
    assert _matches_filters("docs/readme.md", "docs", {".md"})
    assert not _matches_filters("documentation/readme.md", "docs", {".md"})


def test_normalize_document_id_includes_hash_suffix() -> None:
    id_a = normalize_document_id("upload.docs/a b")
    id_b = normalize_document_id("upload.docs/a-b")
    assert id_a != id_b
    assert len(id_a.split("-")[-1]) == 16


def test_fetch_file_content_decodes_non_utf8_with_replacement() -> None:
    client = GitHubApiClient("ghp_test")
    invalid_utf8 = base64.b64encode(b"\xff\xfe").decode("ascii")
    with patch.object(client, "_get_json") as mock_get_json:
        mock_get_json.return_value = {
            "content": invalid_utf8,
            "encoding": "base64",
            "size": 2,
        }
        content = client.fetch_file_content("acme/docs", "bad.txt", "main")
    assert content is not None
    assert "\ufffd" in content


def test_list_matching_files_returns_truncated_flag() -> None:
    client = GitHubApiClient("ghp_test")
    with patch.object(client, "_get_json") as mock_get_json:
        mock_get_json.return_value = {
            "tree": [{"type": "blob", "path": "readme.md", "size": 1}],
            "truncated": True,
        }
        files, tree_truncated = client.list_matching_files(
            "acme/docs", "main", "", [".md"]
        )
    assert tree_truncated is True
    assert len(files) == 1


def test_make_repo_source_id() -> None:
    assert make_repo_source_id("acme/docs") == "github.acme.docs._repo"


def test_parse_repo_identifier_http_url() -> None:
    assert parse_repo_identifier("http://github.com/acme/docs") == "acme/docs"


def test_list_matching_files_raises_when_tree_missing() -> None:
    client = GitHubApiClient("ghp_test")
    with (
        patch.object(client, "_get_json", return_value=None),
        pytest.raises(RuntimeError, match="Could not access branch"),
    ):
        client.list_matching_files("acme/docs", "missing", "", [".md"])


def test_list_matching_files_filters_extensions_and_prefix() -> None:
    client = GitHubApiClient("ghp_test")
    with patch.object(client, "_get_json") as mock_get_json:
        mock_get_json.return_value = {
            "tree": [
                {"type": "blob", "path": "docs/readme.md", "size": 1},
                {"type": "blob", "path": "docs/image.png", "size": 1},
                {"type": "tree", "path": "docs", "size": 0},
            ],
            "truncated": False,
        }
        files, tree_truncated = client.list_matching_files(
            "acme/docs", "main", "docs", [".md"]
        )
    assert tree_truncated is False
    assert [file.path for file in files] == ["docs/readme.md"]


def test_fetch_file_content_returns_none_when_api_fails() -> None:
    client = GitHubApiClient("ghp_test")
    with patch.object(client, "_get_json", return_value=None):
        assert client.fetch_file_content("acme/docs", "readme.md", "main") is None


def test_fetch_file_content_skips_oversized_file() -> None:
    client = GitHubApiClient("ghp_test")
    with patch.object(client, "_get_json") as mock_get_json:
        mock_get_json.return_value = {
            "size": 2_000_000,
            "content": "",
            "encoding": "base64",
        }
        assert client.fetch_file_content("acme/docs", "big.md", "main") is None


def test_fetch_file_content_decodes_base64_utf8() -> None:
    client = GitHubApiClient("ghp_test")
    encoded = base64.b64encode(b"# Hello").decode("ascii")
    with patch.object(client, "_get_json") as mock_get_json:
        mock_get_json.return_value = {
            "size": 7,
            "content": encoded,
            "encoding": "base64",
        }
        assert client.fetch_file_content("acme/docs", "readme.md", "main") == "# Hello"


def test_fetch_file_content_uses_download_url() -> None:
    client = GitHubApiClient("ghp_test")
    with patch.object(client, "_get_json") as mock_get_json:
        mock_get_json.return_value = {
            "download_url": "https://raw.githubusercontent.com/a/b"
        }
        with patch.object(
            client, "_get_text", return_value="raw text"
        ) as mock_get_text:
            content = client.fetch_file_content("acme/docs", "readme.md", "main")
    assert content == "raw text"
    mock_get_text.assert_called_once_with("https://raw.githubusercontent.com/a/b")


def test_get_latest_commit_sha_returns_unknown_when_missing() -> None:
    client = GitHubApiClient("ghp_test")
    with patch.object(client, "_get_json", return_value={}):
        assert client.get_latest_commit_sha("acme/docs", "main") == "unknown"


def test_get_latest_commit_sha_returns_sha() -> None:
    client = GitHubApiClient("ghp_test")
    with patch.object(client, "_get_json", return_value={"commit": {"sha": "abc123"}}):
        assert client.get_latest_commit_sha("acme/docs", "main") == "abc123"


def test_get_json_returns_none_on_http_error() -> None:
    client = GitHubApiClient("ghp_test")
    response = MagicMock()
    response.status_code = 404
    with patch.object(client._session, "get", return_value=response):
        assert client._get_json("https://api.github.com/test") is None


def test_get_text_returns_none_on_http_error() -> None:
    client = GitHubApiClient("ghp_test")
    response = MagicMock()
    response.status_code = 500
    with patch.object(client._session, "get", return_value=response):
        assert client._get_text("https://raw.githubusercontent.com/a/b") is None


def test_collect_intermediate_directories_without_prefix() -> None:
    files = [GitHubFileInfo(path="guides/setup.md", size=10)]
    assert collect_intermediate_directories(files, "") == {"guides"}
