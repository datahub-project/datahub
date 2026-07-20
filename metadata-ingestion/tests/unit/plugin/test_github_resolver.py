"""Tests for the GitHub resolver."""

import os
from unittest.mock import MagicMock, patch

import pytest

from datahub.plugin.github_resolver import (
    GitHubSpec,
    ResolvedGitSource,
    ResolvedWheel,
    _resolve_github_token,
    download_wheel,
    resolve_github_spec,
)


class TestGitHubSpec:
    def test_parse_basic(self) -> None:
        spec = GitHubSpec.parse("github:acme/datahub-salesforce")
        assert spec is not None
        assert spec.owner == "acme"
        assert spec.repo == "datahub-salesforce"
        assert spec.version is None

    def test_parse_with_version(self) -> None:
        spec = GitHubSpec.parse("github:acme/my-plugin@v1.2.0")
        assert spec is not None
        assert spec.owner == "acme"
        assert spec.repo == "my-plugin"
        assert spec.version == "v1.2.0"

    def test_parse_invalid(self) -> None:
        assert GitHubSpec.parse("not-a-github-spec") is None
        assert GitHubSpec.parse("pypi:some-package") is None
        assert GitHubSpec.parse("") is None

    def test_parse_no_owner(self) -> None:
        assert GitHubSpec.parse("github:just-repo") is None

    def test_rejects_empty_owner(self) -> None:
        with pytest.raises(ValueError, match="owner must not be empty"):
            GitHubSpec(owner="", repo="my-repo", version=None)

    def test_rejects_empty_repo(self) -> None:
        with pytest.raises(ValueError, match="repo must not be empty"):
            GitHubSpec(owner="acme", repo="  ", version=None)


class TestResolvedDataclassValidation:
    def test_resolved_wheel_rejects_empty_url(self) -> None:
        with pytest.raises(ValueError, match="download_url must not be empty"):
            ResolvedWheel(download_url="", version="1.0")

    def test_resolved_wheel_rejects_empty_version(self) -> None:
        with pytest.raises(ValueError, match="version must not be empty"):
            ResolvedWheel(download_url="https://example.com/a.whl", version="")

    def test_resolved_git_source_rejects_empty_url(self) -> None:
        with pytest.raises(ValueError, match="download_url must not be empty"):
            ResolvedGitSource(download_url="", version="1.0")

    def test_resolved_git_source_rejects_empty_version(self) -> None:
        with pytest.raises(ValueError, match="version must not be empty"):
            ResolvedGitSource(
                download_url="git+https://github.com/a/b.git@v1", version=""
            )


class TestResolveGithubSpec:
    @patch("datahub.plugin.github_resolver.requests.get")
    def test_resolve_latest_with_wheel(self, mock_get: MagicMock) -> None:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "tag_name": "v1.0.0",
            "assets": [
                {
                    "name": "my_plugin-1.0.0-py3-none-any.whl",
                    "browser_download_url": "https://github.com/acme/my-plugin/releases/download/v1.0.0/my_plugin-1.0.0-py3-none-any.whl",
                }
            ],
        }
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        result = resolve_github_spec("github:acme/my-plugin")
        assert isinstance(result, ResolvedWheel)
        assert result.version == "1.0.0"
        assert "my_plugin-1.0.0-py3-none-any.whl" in result.download_url

    @patch("datahub.plugin.github_resolver.requests.get")
    def test_resolve_fallback_to_git(self, mock_get: MagicMock) -> None:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "tag_name": "v2.0.0",
            "assets": [],  # No wheel
        }
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        result = resolve_github_spec("github:acme/my-plugin")
        assert isinstance(result, ResolvedGitSource)
        assert "git+https://" in result.download_url
        assert result.version == "2.0.0"

    @patch("datahub.plugin.github_resolver.requests.get")
    def test_resolve_specific_version(self, mock_get: MagicMock) -> None:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "tag_name": "v1.5.0",
            "assets": [
                {
                    "name": "plugin-1.5.0-py3-none-any.whl",
                    "browser_download_url": "https://example.com/plugin.whl",
                }
            ],
        }
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        result = resolve_github_spec("github:acme/plugin@v1.5.0")
        assert result.version == "1.5.0"

    @patch("datahub.plugin.github_resolver.requests.get")
    def test_resolve_404(self, mock_get: MagicMock) -> None:
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_get.return_value = mock_response

        with pytest.raises(ValueError, match="No releases found"):
            resolve_github_spec("github:acme/nonexistent")

    def test_resolve_invalid_spec(self) -> None:
        with pytest.raises(ValueError, match="Invalid GitHub plugin spec"):
            resolve_github_spec("not-a-spec")


class TestResolveGithubToken:
    def test_returns_env_var_when_set(self) -> None:
        with patch.dict(os.environ, {"GITHUB_TOKEN": "env-token-123"}):
            assert _resolve_github_token() == "env-token-123"

    def test_falls_back_to_gh_cli(self) -> None:
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("GITHUB_TOKEN", None)
            mock_result = MagicMock()
            mock_result.returncode = 0
            mock_result.stdout = "gh-cli-token-456\n"
            with patch(
                "datahub.plugin.github_resolver.subprocess.run",
                return_value=mock_result,
            ):
                assert _resolve_github_token() == "gh-cli-token-456"

    def test_returns_none_when_no_auth(self) -> None:
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("GITHUB_TOKEN", None)
            with patch(
                "datahub.plugin.github_resolver.subprocess.run",
                side_effect=FileNotFoundError("gh not found"),
            ):
                assert _resolve_github_token() is None

    def test_returns_none_on_gh_cli_failure(self) -> None:
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("GITHUB_TOKEN", None)
            mock_result = MagicMock()
            mock_result.returncode = 1
            mock_result.stdout = ""
            with patch(
                "datahub.plugin.github_resolver.subprocess.run",
                return_value=mock_result,
            ):
                assert _resolve_github_token() is None


class TestDownloadWheel:
    def test_downloads_via_api_url_with_auth(self) -> None:
        resolved = ResolvedWheel(
            download_url="https://github.com/acme/plugin/releases/download/v1/a.whl",
            version="1.0",
            asset_api_url="https://api.github.com/repos/acme/plugin/releases/assets/123",
            asset_filename="a-1.0-py3-none-any.whl",
        )
        mock_resp = MagicMock()
        mock_resp.iter_content.return_value = [b"PK\x03\x04fake-whl"]
        mock_resp.raise_for_status = MagicMock()

        with (
            patch(
                "datahub.plugin.github_resolver.requests.get", return_value=mock_resp
            ) as mock_get,
            patch(
                "datahub.plugin.github_resolver._resolve_github_token",
                return_value="my-token",
            ),
        ):
            path = download_wheel(resolved)

        # Verify API URL was used (not browser URL)
        call_url = mock_get.call_args[0][0]
        assert call_url == resolved.asset_api_url
        # Verify auth header
        call_headers = mock_get.call_args[1]["headers"]
        assert call_headers["Authorization"] == "Bearer my-token"
        assert call_headers["Accept"] == "application/octet-stream"
        # Verify file was written with correct name
        assert path.endswith("a-1.0-py3-none-any.whl")
        assert os.path.isfile(path)
        # Cleanup
        os.unlink(path)
        os.rmdir(os.path.dirname(path))

    def test_falls_back_to_browser_url_without_api_url(self) -> None:
        resolved = ResolvedWheel(
            download_url="https://github.com/acme/plugin/releases/download/v1/a.whl",
            version="1.0",
        )
        mock_resp = MagicMock()
        mock_resp.iter_content.return_value = [b"PK\x03\x04fake"]
        mock_resp.raise_for_status = MagicMock()

        with (
            patch(
                "datahub.plugin.github_resolver.requests.get", return_value=mock_resp
            ) as mock_get,
            patch(
                "datahub.plugin.github_resolver._resolve_github_token",
                return_value=None,
            ),
        ):
            path = download_wheel(resolved)

        call_url = mock_get.call_args[0][0]
        assert call_url == resolved.download_url
        assert path.endswith("plugin.whl")  # default filename
        os.unlink(path)
        os.rmdir(os.path.dirname(path))

    def test_cleans_up_on_write_failure(self) -> None:
        resolved = ResolvedWheel(
            download_url="https://example.com/a.whl",
            version="1.0",
            asset_filename="a.whl",
        )
        mock_resp = MagicMock()
        mock_resp.iter_content.side_effect = IOError("disk full")
        mock_resp.raise_for_status = MagicMock()

        with (
            patch(
                "datahub.plugin.github_resolver.requests.get", return_value=mock_resp
            ),
            patch(
                "datahub.plugin.github_resolver._resolve_github_token",
                return_value=None,
            ),
            pytest.raises(IOError, match="disk full"),
        ):
            download_wheel(resolved)

    def test_raises_on_403_access_denied(self) -> None:
        resolved = ResolvedWheel(
            download_url="https://github.com/private/repo/releases/download/v1/a.whl",
            version="1.0",
            asset_api_url="https://api.github.com/repos/private/repo/releases/assets/1",
            asset_filename="a.whl",
        )
        mock_resp = MagicMock()
        mock_resp.status_code = 403

        with (
            patch(
                "datahub.plugin.github_resolver.requests.get", return_value=mock_resp
            ),
            patch(
                "datahub.plugin.github_resolver._resolve_github_token",
                return_value=None,
            ),
            pytest.raises(ValueError, match="Access denied"),
        ):
            download_wheel(resolved)

    def test_raises_on_404_not_found(self) -> None:
        resolved = ResolvedWheel(
            download_url="https://github.com/acme/plugin/releases/download/v1/a.whl",
            version="1.0",
            asset_filename="a.whl",
        )
        mock_resp = MagicMock()
        mock_resp.status_code = 404

        with (
            patch(
                "datahub.plugin.github_resolver.requests.get", return_value=mock_resp
            ),
            patch(
                "datahub.plugin.github_resolver._resolve_github_token",
                return_value=None,
            ),
            pytest.raises(ValueError, match="not found"),
        ):
            download_wheel(resolved)
