import pathlib
from typing import Optional
from unittest.mock import MagicMock

import pytest

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.git.git_import import GitClone as _RealGitClone
from datahub.ingestion.source.looker.lookml_config import (
    BASE_PROJECT_NAME,
    LookMLSourceConfig,
)
from datahub.ingestion.source.looker.lookml_source import (
    LookMLSource,
    _hostname_from_git_url,
    check_remote_dependency_url,
)

# A pattern that allows github.com and its subdomains (the documented form).
_GITHUB_PATTERN = AllowDenyPattern(allow=["^github\\.com$", ".*\\.github\\.com$"])


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
        allowed_pattern=AllowDenyPattern(allow=["^github\\.com$"]),
    )
    assert result.allowed


def test_allowlist_accepts_subdomain() -> None:
    result = check_remote_dependency_url(
        "https://gist.github.com/org/repo.git",
        allowed_pattern=_GITHUB_PATTERN,
    )
    assert result.allowed


def test_allowlist_rejects_other_host() -> None:
    result = check_remote_dependency_url(
        "https://evil.example/repo.git",
        allowed_pattern=_GITHUB_PATTERN,
    )
    assert not result.allowed
    assert result.reason is not None
    assert "remote_dependency_domain_pattern" in result.reason


def test_empty_allowlist_rejects_all_hosts() -> None:
    result = check_remote_dependency_url(
        "https://github.com/org/repo.git",
        allowed_pattern=AllowDenyPattern(allow=[]),
    )
    assert not result.allowed


def test_deny_pattern_overrides_allow_all() -> None:
    # allow_all() would normally permit evil.example; a deny pattern blocks it.
    result = check_remote_dependency_url(
        "https://evil.example/repo.git",
        allowed_pattern=AllowDenyPattern(allow=[".*"], deny=["^evil\\.example$"]),
    )
    assert not result.allowed
    assert result.reason is not None
    assert "remote_dependency_domain_pattern" in result.reason


def test_unanchored_pattern_matches_prefix_only() -> None:
    # AllowDenyPattern matches from the start of the string, not the full string.
    # An unanchored pattern "github.com" matches "github.com.evil.com" too —
    # operators must anchor with ^...$ for exact matches. This test documents
    # that behavior so it is not silently changed.
    result = check_remote_dependency_url(
        "https://github.com.evil.com/repo.git",
        allowed_pattern=AllowDenyPattern(allow=["github\\.com"]),
    )
    # Host is accepted by the (permissive, unanchored) pattern; the hard
    # denylist does not block github.com.evil.com. This is expected — the
    # operator is responsible for anchoring security-relevant patterns.
    assert result.allowed
    assert result.hostname == "github.com.evil.com"


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
        tmp_path,
        extra_config={
            "remote_dependency_domain_pattern": {"allow": ["^github\\.com$"]}
        },
    )
    source.base_projects_folder[BASE_PROJECT_NAME] = tmp_path
    source._recursively_check_manifests(
        tmp_dir=str(tmp_path / "tmp"),
        project_name=BASE_PROJECT_NAME,
        project_visited=set(),
        manifest_constants={},
    )
    mock_clone.assert_not_called()


def test_recursively_check_manifests_clones_valid_allowed_url(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A valid allowlisted URL is cloned and registered for downstream parsing."""
    (tmp_path / "manifest.lkml").write_text(
        """
project_name: "ssrf_repro_project"

remote_dependency: shared {
  url: "https://github.com/org/shared-lookml.git"
  ref: "main"
}
"""
    )
    fake_repo = MagicMock()
    fake_repo.active_branch.name = "main"
    mock_clone = MagicMock(return_value=pathlib.Path("/tmp/checkout"))
    mock_cloner = MagicMock()
    mock_cloner.clone = mock_clone
    mock_cloner.get_last_repo_cloned.return_value = fake_repo
    monkeypatch.setattr(
        "datahub.ingestion.source.looker.lookml_source.GitClone",
        MagicMock(return_value=mock_cloner),
    )
    source = _lookml_source_for_folder(
        tmp_path,
        extra_config={
            "remote_dependency_domain_pattern": {"allow": ["^github\\.com$"]}
        },
    )
    source.base_projects_folder[BASE_PROJECT_NAME] = tmp_path
    source._recursively_check_manifests(
        tmp_dir=str(tmp_path / "tmp"),
        project_name=BASE_PROJECT_NAME,
        project_visited=set(),
        manifest_constants={},
    )
    mock_clone.assert_called_once()
    assert "shared" in source.base_projects_folder


# ---- Bypass cases raised by review ----


def test_trailing_dot_localhost_rejected() -> None:
    result = check_remote_dependency_url("https://localhost./repo.git")
    assert not result.allowed


def test_decimal_ip_rejected() -> None:
    # 2130706433 == 127.0.0.1; curl/git accept this decimal form.
    result = check_remote_dependency_url("https://2130706433/repo.git")
    assert not result.allowed


def test_hex_ip_rejected() -> None:
    result = check_remote_dependency_url("https://0x7f000001/repo.git")
    assert not result.allowed


def test_ipv4_mapped_ipv6_rejected() -> None:
    result = check_remote_dependency_url("https://[::ffff:127.0.0.1]/repo.git")
    assert not result.allowed


def test_dns_rebinding_to_loopback_rejected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import socket as _socket

    def fake_getaddrinfo(host, *args, **kwargs):
        assert host == "127.0.0.1.nip.io"
        return [(None, None, None, None, ("127.0.0.1", 0))]

    monkeypatch.setattr(_socket, "getaddrinfo", fake_getaddrinfo)
    result = check_remote_dependency_url("https://127.0.0.1.nip.io/repo.git")
    assert not result.allowed
    assert result.reason is not None
    assert "resolves to a blocked IP" in result.reason


def test_malformed_url_does_not_raise() -> None:
    result = check_remote_dependency_url("https://[::1/repo.git")
    assert not result.allowed


def test_url_with_credentials_is_sanitized_in_warning(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / "manifest.lkml").write_text(
        """
project_name: "ssrf_repro_project"

remote_dependency: leak {
  url: "https://user:supersecret@evil.example/repo.git"
  ref: "main"
}
"""
    )
    mock_clone = MagicMock()
    mock_git_clone = MagicMock(return_value=MagicMock(clone=mock_clone))
    # Keep the real static sanitizer so credentials are actually redacted.
    mock_git_clone.sanitize_repo_url = _RealGitClone.sanitize_repo_url
    monkeypatch.setattr(
        "datahub.ingestion.source.looker.lookml_source.GitClone",
        mock_git_clone,
    )
    source = _lookml_source_for_folder(
        tmp_path,
        extra_config={
            "remote_dependency_domain_pattern": {"allow": ["^github\\.com$"]}
        },
    )
    source.base_projects_folder[BASE_PROJECT_NAME] = tmp_path
    source._recursively_check_manifests(
        tmp_dir=str(tmp_path / "tmp"),
        project_name=BASE_PROJECT_NAME,
        project_visited=set(),
        manifest_constants={},
    )
    mock_clone.assert_not_called()
    contexts = [str(w.context) for w in source.reporter.warnings]
    assert contexts, "expected a skip warning"
    joined = " ".join(contexts)
    assert "supersecret" not in joined
    assert "*****" in joined  # sanitized userinfo marker
