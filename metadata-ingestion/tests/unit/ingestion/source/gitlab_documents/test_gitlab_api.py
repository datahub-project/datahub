from unittest.mock import patch

import pytest
import requests
import requests_mock

from datahub.ingestion.source.gitlab_documents.gitlab_api import (
    MAX_REQUEST_ATTEMPTS,
    GitLabApiClient,
    parse_project_path,
)
from datahub.ingestion.source.repository_documents.client import MAX_FILE_SIZE_BYTES

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


def test_fetch_file_content_reads_the_raw_file_and_skips_large_files() -> None:
    client = GitLabApiClient("glpat-test")
    with requests_mock.Mocker() as m:
        m.get(f"{API}/repository/files/docs%2Fa.md/raw?ref=main", text="# Hello")
        m.get(
            f"{API}/repository/files/docs%2Fbig.md/raw?ref=main",
            content=b"",
            headers={"Content-Length": str(MAX_FILE_SIZE_BYTES + 1)},
        )
        assert client.fetch_file_content("acme/data/docs", "docs/a.md", "main") == (
            "# Hello"
        )
        assert (
            client.fetch_file_content("acme/data/docs", "docs/big.md", "main") is None
        )
        assert not any(r.method == "HEAD" for r in m.request_history)


def test_fetch_file_content_stops_reading_a_file_without_a_declared_size() -> None:
    client = GitLabApiClient("glpat-test")
    with requests_mock.Mocker() as m:
        m.get(
            f"{API}/repository/files/docs%2Fbig.md/raw?ref=main",
            content=b"x" * (MAX_FILE_SIZE_BYTES + 1),
        )
        assert (
            client.fetch_file_content("acme/data/docs", "docs/big.md", "main") is None
        )


def test_file_skipped_for_size_does_not_report_an_earlier_error() -> None:
    client = GitLabApiClient("glpat-test")
    with requests_mock.Mocker() as m, patch("time.sleep"):
        failing = m.get(
            f"{API}/repository/files/docs%2Fa.md/raw?ref=main", status_code=503
        )
        m.get(
            f"{API}/repository/files/docs%2Fbig.md/raw?ref=main",
            content=b"",
            headers={"Content-Length": str(MAX_FILE_SIZE_BYTES + 1)},
        )
        assert client.fetch_file_content("acme/data/docs", "docs/a.md", "main") is None
        assert failing.call_count == MAX_REQUEST_ATTEMPTS
        assert client.last_api_error is not None
        assert "503" in client.last_api_error
        assert (
            client.fetch_file_content("acme/data/docs", "docs/big.md", "main") is None
        )
        assert client.last_api_error is None


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
        assert client.get_latest_commit_sha("acme/data/docs", "main") is None


def test_list_matching_files_fails_when_a_later_page_fails() -> None:
    # A partial listing would let stale removal delete the unread pages' files.
    client = GitLabApiClient("glpat-test")
    with requests_mock.Mocker() as m:
        m.get(
            f"{API}/repository/tree?ref=main&recursive=true&per_page=100",
            json=[{"id": "b1", "type": "blob", "path": "a.md"}],
            headers={"Link": f'<{API}/repository/tree?page=2>; rel="next"'},
        )
        m.get(f"{API}/repository/tree?page=2", status_code=502)
        with patch("time.sleep"), pytest.raises(RuntimeError, match="502"):
            client.list_matching_files("acme/data/docs", "main", "", [".md"])
        # The failing page is retried before the run gives up.
        assert m.call_count == 1 + 4


def test_list_matching_files_falls_back_to_next_page_header() -> None:
    client = GitLabApiClient("glpat-test")
    with requests_mock.Mocker() as m:
        m.get(
            f"{API}/repository/tree",
            [
                {
                    "json": [{"id": "b1", "type": "blob", "path": "a.md"}],
                    "headers": {"X-Next-Page": "2"},
                },
                {
                    "json": [{"id": "b2", "type": "blob", "path": "b.md"}],
                    "headers": {"X-Next-Page": ""},
                },
            ],
        )
        files, truncated = client.list_matching_files(
            "acme/data/docs", "main", "", [".md"]
        )
    assert [f.path for f in files] == ["a.md", "b.md"]
    assert m.request_history[1].qs["page"] == ["2"]
    assert truncated is False


def _full_tree_page(start: int = 0) -> list:
    return [
        {"id": f"b{i}", "type": "blob", "path": f"{i}.md"}
        for i in range(start, start + 100)
    ]


@pytest.mark.parametrize(
    "next_offset_page, expected_truncated",
    [
        # A proxy stripped the headers and more entries exist.
        ([{"id": "b100", "type": "blob", "path": "100.md"}], True),
        # Keyset listing of exactly one full page: nothing follows it.
        ([], False),
    ],
)
def test_full_page_without_pagination_headers_checks_for_more(
    next_offset_page: list, expected_truncated: bool
) -> None:
    client = GitLabApiClient("glpat-test")
    with requests_mock.Mocker() as m:
        m.get(f"{API}/repository/tree", json=_full_tree_page())
        m.get(f"{API}/repository/tree?page=2", json=next_offset_page)
        files, truncated = client.list_matching_files(
            "acme/data/docs", "main", "", [".md"]
        )
    assert len(files) == 100
    assert truncated is expected_truncated
    probe = m.request_history[-1]
    assert probe.qs["page"] == ["2"]
    assert "pagination" not in probe.qs


def test_keyset_last_full_page_is_not_truncated() -> None:
    client = GitLabApiClient("glpat-test")
    next_link = f"{API}/repository/tree?pagination=keyset&page_token=abc"
    with requests_mock.Mocker() as m:
        m.get(
            f"{API}/repository/tree?ref=main",
            json=_full_tree_page(),
            headers={"Link": f'<{next_link}>; rel="next"'},
        )
        # Keyset's last page carries neither Link nor X-Next-Page.
        m.get(next_link, json=_full_tree_page(start=100))
        files, truncated = client.list_matching_files(
            "acme/data/docs", "main", "", [".md"]
        )
    assert len(files) == 200
    assert not truncated
    assert m.call_count == 2


def test_next_link_from_another_host_is_replayed_on_the_configured_instance() -> None:
    # Behind a TLS proxy, GitLab builds links from its external_url, which can
    # differ from the configured instance in scheme, host and port.
    client = GitLabApiClient("glpat-test")
    proxied = "http://gitlab.internal:8080/api/v4/projects/acme%2Fdata%2Fdocs"
    with requests_mock.Mocker() as m:
        m.get(
            f"{API}/repository/tree",
            [
                {
                    "json": [{"id": "b1", "type": "blob", "path": "a.md"}],
                    "headers": {
                        "Link": f'<{proxied}/repository/tree?id_after=b1>; rel="next"'
                    },
                },
                {"json": [{"id": "b2", "type": "blob", "path": "b.md"}]},
            ],
        )
        files, _ = client.list_matching_files("acme/data/docs", "main", "", [".md"])

    assert [f.path for f in files] == ["a.md", "b.md"]
    assert [r.netloc for r in m.request_history] == ["gitlab.com", "gitlab.com"]
    assert m.request_history[1].qs["id_after"] == ["b1"]


def test_list_matching_files_refuses_a_next_link_that_is_not_an_api_link() -> None:
    client = GitLabApiClient("glpat-test")
    with requests_mock.Mocker() as m:
        m.get(
            f"{API}/repository/tree",
            json=[],
            headers={"Link": '<http://internal.example/tree?page=2>; rel="next"'},
        )
        with pytest.raises(RuntimeError, match="not an API link"):
            client.list_matching_files("acme/data/docs", "main", "", [".md"])


def test_html_page_instead_of_a_file_is_reported_as_an_api_error() -> None:
    client = GitLabApiClient("glpat-test")
    with requests_mock.Mocker() as m:
        m.get(
            f"{API}/repository/files/a.md/raw",
            text="<html>Sign in</html>",
            headers={"Content-Type": "text/html"},
        )
        assert client.fetch_file_content("acme/data/docs", "a.md", "main") is None
    assert client.last_api_error is not None
    assert "HTML" in client.last_api_error


def test_transient_timeout_is_retried() -> None:
    client = GitLabApiClient("glpat-test")
    with requests_mock.Mocker() as m, patch("time.sleep"):
        m.get(
            f"{API}/repository/branches/main",
            [
                {"exc": requests.exceptions.ConnectTimeout},
                {"json": {"commit": {"id": "abc123"}}},
            ],
        )
        assert client.get_latest_commit_sha("acme/data/docs", "main") == "abc123"


def test_tree_listing_requests_keyset_pagination() -> None:
    client = GitLabApiClient("glpat-test")
    with requests_mock.Mocker() as m:
        m.get(f"{API}/repository/tree", json=[])
        client.list_matching_files("acme/data/docs", "main", "", [".md"])
    assert m.request_history[0].qs["pagination"] == ["keyset"]


def test_raw_file_is_retried_after_a_transient_error() -> None:
    client = GitLabApiClient("glpat-test")
    with requests_mock.Mocker() as m, patch("time.sleep"):
        m.get(
            f"{API}/repository/files/a.md/raw",
            [{"status_code": 503}, {"text": "# A"}],
        )
        assert client.fetch_file_content("acme/data/docs", "a.md", "main") == "# A"


def test_failure_while_reading_a_file_body_is_reported_not_raised() -> None:
    client = GitLabApiClient("glpat-test")
    with (
        requests_mock.Mocker() as m,
        patch("time.sleep"),
        patch.object(
            requests.Response,
            "iter_content",
            side_effect=requests.exceptions.ChunkedEncodingError("reset"),
        ),
    ):
        m.get(f"{API}/repository/files/a.md/raw", text="# A")
        assert client.fetch_file_content("acme/data/docs", "a.md", "main") is None
        # Retried before giving up.
        assert m.call_count == MAX_REQUEST_ATTEMPTS
    assert client.last_api_error is not None


def test_redirects_are_reported_not_followed() -> None:
    # requests would keep the PRIVATE-TOKEN header on a redirect to another host.
    client = GitLabApiClient("glpat-test")
    with requests_mock.Mocker() as m:
        m.get(
            f"{API}/repository/files/a.md/raw",
            status_code=302,
            headers={"Location": "https://sso.example.com/login"},
        )
        sso = m.get("https://sso.example.com/login", text="<html>Sign in</html>")
        assert client.fetch_file_content("acme/data/docs", "a.md", "main") is None
        assert not sso.called
    assert client.last_api_error is not None
    assert "sso.example.com" in client.last_api_error


def test_next_link_is_rebased_onto_an_instance_under_a_relative_path() -> None:
    client = GitLabApiClient("glpat-test", base_url="https://host.example/gitlab")
    api = "https://host.example/gitlab/api/v4/projects/acme%2Fdata%2Fdocs"
    proxied = "http://internal:8080/gitlab/api/v4/projects/acme%2Fdata%2Fdocs"
    with requests_mock.Mocker() as m:
        m.get(
            f"{api}/repository/tree",
            [
                {
                    "json": [{"id": "b1", "type": "blob", "path": "a.md"}],
                    "headers": {
                        "Link": f'<{proxied}/repository/tree?id_after=b1>; rel="next"'
                    },
                },
                {"json": [{"id": "b2", "type": "blob", "path": "b.md"}]},
            ],
        )
        files, _ = client.list_matching_files("acme/data/docs", "main", "", [".md"])
    assert [f.path for f in files] == ["a.md", "b.md"]
    assert {r.netloc for r in m.request_history} == {"host.example"}


def test_tree_page_failing_mid_body_is_retried() -> None:
    client = GitLabApiClient("glpat-test")
    with requests_mock.Mocker() as m, patch("time.sleep"):
        tree = m.get(
            f"{API}/repository/tree",
            [
                {"exc": requests.exceptions.ChunkedEncodingError("reset mid-body")},
                {"json": [{"type": "blob", "path": "a.md", "id": "blob-a"}]},
            ],
        )
        files, truncated = client.list_matching_files(
            "acme/data/docs", "main", "", [".md"]
        )
    assert tree.call_count == 2
    assert [file.path for file in files] == ["a.md"]
    assert not truncated


def test_json_redirect_is_reported_not_followed() -> None:
    client = GitLabApiClient("glpat-test")
    with requests_mock.Mocker() as m:
        m.get(
            f"{API}/repository/tree",
            status_code=302,
            headers={"Location": "https://sso.example.com/login"},
        )
        sso = m.get("https://sso.example.com/login", text="<html>Sign in</html>")
        with pytest.raises(RuntimeError, match="sso.example.com"):
            client.list_matching_files("acme/data/docs", "main", "", [".md"])
        assert not sso.called


@pytest.mark.parametrize(
    "endpoint, body",
    [
        ("repository/tree", {"message": "not a list"}),
        ("repository/branches/main", {"name": "main"}),
    ],
    ids=["tree is not a list", "branch without commit"],
)
def test_unexpected_response_shape_is_reported(endpoint: str, body: dict) -> None:
    client = GitLabApiClient("glpat-test")
    with requests_mock.Mocker() as m:
        m.get(f"{API}/{endpoint}", json=body)
        if endpoint == "repository/tree":
            with pytest.raises(RuntimeError, match="unexpected response shape"):
                client.list_matching_files("acme/data/docs", "main", "", [".md"])
        else:
            assert client.get_latest_commit_sha("acme/data/docs", "main") is None
    assert client.last_api_error is not None
    assert "unexpected response shape" in client.last_api_error
