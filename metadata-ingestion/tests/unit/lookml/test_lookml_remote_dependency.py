import pathlib
from typing import Optional
from unittest.mock import MagicMock

import pytest

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.looker.lookml_config import (
    BASE_PROJECT_NAME,
    LookMLSourceConfig,
)
from datahub.ingestion.source.looker.lookml_source import (
    LookMLSource,
    _hostname_from_git_url,
    check_remote_dependency_url,
)


def test_hostname_from_https_url() -> None:
    assert _hostname_from_git_url("https://github.com/org/repo.git") == "github.com"


def test_hostname_from_ssh_url() -> None:
    assert (
        _hostname_from_git_url("ssh://git@gitlab.example.com/org/repo.git")
        == "gitlab.example.com"
    )


def test_hostname_from_scp_style_url() -> None:
    assert _hostname_from_git_url("git@github.com:org/repo.git") == "github.com"


def test_hostname_from_empty_url() -> None:
    assert _hostname_from_git_url("   ") is None


def test_https_github_url_allowed_without_domain_list() -> None:
    result = check_remote_dependency_url("https://github.com/org/repo.git")
    assert result.allowed
    assert result.hostname == "github.com"


def test_git_at_url_allowed_without_domain_list() -> None:
    result = check_remote_dependency_url("git@github.com:org/repo.git")
    assert result.allowed
    assert result.hostname == "github.com"


def test_http_scheme_rejected() -> None:
    result = check_remote_dependency_url("http://127.0.0.1:9876/attacker-repo.git")
    assert not result.allowed
    assert result.reason is not None
    assert "scheme" in result.reason


def test_file_scheme_rejected() -> None:
    result = check_remote_dependency_url("file:///etc/passwd")
    assert not result.allowed


def test_localhost_rejected() -> None:
    result = check_remote_dependency_url("https://localhost/repo.git")
    assert not result.allowed
    assert result.hostname == "localhost"


def test_loopback_ip_rejected() -> None:
    result = check_remote_dependency_url("https://127.0.0.1/repo.git")
    assert not result.allowed


def test_link_local_metadata_ip_rejected() -> None:
    result = check_remote_dependency_url("https://169.254.169.254/latest/meta-data")
    assert not result.allowed


def test_allowlist_accepts_matching_host() -> None:
    result = check_remote_dependency_url(
        "https://github.com/org/repo.git",
        allowed_domains=["github.com"],
    )
    assert result.allowed


def test_allowlist_accepts_subdomain() -> None:
    result = check_remote_dependency_url(
        "https://gist.github.com/org/repo.git",
        allowed_domains=["github.com"],
    )
    assert result.allowed


def test_allowlist_rejects_other_host() -> None:
    result = check_remote_dependency_url(
        "https://evil.example/repo.git",
        allowed_domains=["github.com"],
    )
    assert not result.allowed
    assert result.reason is not None
    assert "allowed_remote_dependency_domains" in result.reason


def test_empty_allowlist_rejects_all_hosts() -> None:
    result = check_remote_dependency_url(
        "https://github.com/org/repo.git",
        allowed_domains=[],
    )
    assert not result.allowed


def _lookml_source_for_folder(
    folder: pathlib.Path, extra_config: Optional[dict] = None
) -> LookMLSource:
    payload: dict = {
        "base_folder": str(folder),
        "connection_to_platform_map": {"my_connection": "postgres"},
        "project_name": "test_project",
    }
    if extra_config:
        payload.update(extra_config)
    config = LookMLSourceConfig.model_validate(payload)
    ctx = MagicMock(spec=PipelineContext)
    ctx.graph = None
    return LookMLSource(config, ctx)


def test_get_manifest_reads_lkml_plural_keys(tmp_path: pathlib.Path) -> None:
    (tmp_path / "manifest.lkml").write_text(
        """
project_name: "ssrf_repro_project"

local_dependency: {
  project: "looker-hub"
}

remote_dependency: ssrf_test {
  url: "https://github.com/org/repo.git"
  ref: "main"
}
"""
    )
    source = _lookml_source_for_folder(tmp_path)
    manifest = source.get_manifest_if_present(tmp_path)
    assert manifest is not None
    assert manifest.local_dependencies == ["looker-hub"]
    assert len(manifest.remote_dependencies) == 1
    assert manifest.remote_dependencies[0].name == "ssrf_test"
    assert manifest.remote_dependencies[0].url == "https://github.com/org/repo.git"


def test_recursively_check_manifests_does_not_clone_http_url(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / "manifest.lkml").write_text(
        """
project_name: "ssrf_repro_project"

remote_dependency: ssrf_test {
  url: "http://127.0.0.1:9876/attacker-repo.git"
  ref: "main"
}
"""
    )
    mock_clone = MagicMock()
    monkeypatch.setattr(
        "datahub.ingestion.source.looker.lookml_source.GitClone",
        MagicMock(return_value=MagicMock(clone=mock_clone)),
    )
    source = _lookml_source_for_folder(tmp_path)
    source.base_projects_folder[BASE_PROJECT_NAME] = tmp_path
    source._recursively_check_manifests(
        tmp_dir=str(tmp_path / "tmp"),
        project_name=BASE_PROJECT_NAME,
        project_visited=set(),
        manifest_constants={},
    )
    mock_clone.assert_not_called()
    assert source.reporter.warnings


def test_recursively_check_manifests_does_not_clone_host_outside_allowlist(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / "manifest.lkml").write_text(
        """
project_name: "ssrf_repro_project"

remote_dependency: other_host {
  url: "https://evil.example/repo.git"
  ref: "main"
}
"""
    )
    mock_clone = MagicMock()
    monkeypatch.setattr(
        "datahub.ingestion.source.looker.lookml_source.GitClone",
        MagicMock(return_value=MagicMock(clone=mock_clone)),
    )
    source = _lookml_source_for_folder(
        tmp_path, extra_config={"allowed_remote_dependency_domains": ["github.com"]}
    )
    source.base_projects_folder[BASE_PROJECT_NAME] = tmp_path
    source._recursively_check_manifests(
        tmp_dir=str(tmp_path / "tmp"),
        project_name=BASE_PROJECT_NAME,
        project_visited=set(),
        manifest_constants={},
    )
    mock_clone.assert_not_called()
