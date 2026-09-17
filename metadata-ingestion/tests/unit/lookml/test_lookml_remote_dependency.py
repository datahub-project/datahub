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


def test_ipv4_mapped_alicloud_metadata_rejected() -> None:
    # Mapped form has none of is_loopback/is_link_local/is_unspecified.
    result = check_remote_dependency_url("https://[::ffff:100.100.100.200]/repo.git")
    assert not result.allowed


def test_ipv4_mapped_aws_metadata_rejected() -> None:
    result = check_remote_dependency_url("https://[::ffff:169.254.169.254]/repo.git")
    assert not result.allowed


def test_ipv6_zone_id_link_local_rejected() -> None:
    # A link-local IPv6 host given with a zone id in scp-bracket form
    # (git@[fe80::1%eth0]:...) is recognized as link-local and rejected.
    result = check_remote_dependency_url("git@[fe80::1%eth0]:org/repo.git")
    assert not result.allowed


def test_octal_ip_rejected() -> None:
    # 017700000001 (octal) == 2130706433 == 127.0.0.1.
    result = check_remote_dependency_url("https://017700000001/repo.git")
    assert not result.allowed


def test_octal_dotted_ip_rejected() -> None:
    # 0177.0.0.1 (per-octet octal) == 127.0.0.1.
    result = check_remote_dependency_url("https://0177.0.0.1/repo.git")
    assert not result.allowed


def test_hex_dotted_ip_rejected() -> None:
    # Per-octet hex: 0x7f.0.0.1 == 127.0.0.1.
    result = check_remote_dependency_url("https://0x7f.0.0.1/repo.git")
    assert not result.allowed


def test_percent_encoded_metadata_ip_rejected() -> None:
    # git/libcurl percent-decode the host before connecting, so %2e must not
    # hide the target: 169%2e254%2e169%2e254 decodes to the metadata IP.
    result = check_remote_dependency_url("https://169%2e254%2e169%2e254/repo.git")
    assert not result.allowed


def test_percent_encoded_loopback_rejected() -> None:
    result = check_remote_dependency_url("https://127%2e0%2e0%2e1/repo.git")
    assert not result.allowed


def test_percent_encoded_metadata_host_rejected() -> None:
    result = check_remote_dependency_url("https://metadata%2egoogle%2einternal/x.git")
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


# ---- Bypasses reproduced during the ING-2573 takeover (red before the fix) ----


@pytest.mark.parametrize(
    "url",
    [
        "git@github.com@127.0.0.1:org/repo.git",
        "git@github.com@169.254.169.254:org/repo.git",
    ],
    ids=["scp_double_at_loopback", "scp_double_at_metadata"],
)
def test_scp_double_at_host_is_blocked(url: str) -> None:
    # ssh connects to the host after the LAST '@', so a leading "github.com" does
    # not make these safe -- they reach 127.0.0.1 / the metadata IP.
    result = check_remote_dependency_url(url)
    assert not result.allowed


@pytest.mark.parametrize(
    "url",
    ["https://127.1/repo.git", "https://127.0.1/repo.git"],
    ids=["short_2part", "short_3part"],
)
def test_short_form_ipv4_blocked_without_dns(
    url: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    # 127.1 / 127.0.1 decode to loopback; they must be caught as IP literals,
    # not left to a DNS lookup that fails open.
    import socket as _socket

    def _no_dns(*args: object, **kwargs: object) -> None:
        raise _socket.gaierror("dns disabled")

    monkeypatch.setattr(_socket, "getaddrinfo", _no_dns)
    result = check_remote_dependency_url(url)
    assert not result.allowed


def test_oracle_cloud_metadata_ip_rejected() -> None:
    # 192.0.0.0/24 is IETF Protocol Assignments, not covered by the generic
    # loopback / link-local / unspecified checks.
    result = check_remote_dependency_url("https://192.0.0.192/repo.git")
    assert not result.allowed


@pytest.mark.parametrize(
    "url",
    ["https://[fd00:ec2::254]/repo.git", "https://100.100.100.200/repo.git"],
    ids=["aws_ipv6_imds", "alicloud_metadata"],
)
def test_load_bearing_metadata_ips_stay_blocked(url: str) -> None:
    # Not caught by the generic checks; must remain in the explicit blocklist
    # after it is trimmed.
    result = check_remote_dependency_url(url)
    assert not result.allowed


def test_get_manifest_drops_blocks_missing_required_keys(
    tmp_path: pathlib.Path,
) -> None:
    # A url-less remote_dependency and a project-less local_dependency are valid
    # (project_dependencies supplies the repo); they must be dropped, not crash
    # get_manifest_if_present with a KeyError once the plural keys are read.
    (tmp_path / "manifest.lkml").write_text(
        """
project_name: "p"

remote_dependency: no_url {
  ref: "main"
}

local_dependency: {
  ref: "x"
}
"""
    )
    source = _lookml_source_for_folder(tmp_path)
    manifest = source.get_manifest_if_present(tmp_path)
    assert manifest is not None
    assert manifest.remote_dependencies == []
    assert manifest.local_dependencies == []


def test_unanchored_pattern_matches_prefix_only() -> None:
    # AllowDenyPattern matches from the START of the string, so an unanchored
    # "github\\.com" also matches "github.com.evil.example". Config validation
    # rejects such patterns; this pins the underlying function behavior so it
    # cannot change silently.
    result = check_remote_dependency_url(
        "https://github.com.evil.example/repo.git",
        allowed_pattern=AllowDenyPattern(allow=["github\\.com"]),
    )
    assert result.allowed
    assert result.hostname == "github.com.evil.example"


@pytest.mark.parametrize(
    "url",
    [
        "git@[::1]:org/repo.git",
        "git@[fd00:ec2::254]:org/repo.git",
        "git@[::1]:evil@path",
    ],
    ids=["ipv6_loopback", "ipv6_aws_imds", "ipv6_loopback_path_at"],
)
def test_scp_bracketed_ipv6_host_is_blocked(url: str) -> None:
    # ssh strips the brackets and connects to the IPv6 host (git@[::1]:x -> ::1),
    # so the validator must unwrap [ ] instead of reading '[' and allowing it.
    result = check_remote_dependency_url(url)
    assert not result.allowed


@pytest.mark.parametrize(
    "url",
    [
        "[127.0.0.1]@github.com:x/repo.git",
        "[::1]@github.com:x/repo.git",
        "[169.254.169.254]@github.com:x/repo.git",
        "git@[x@127.0.0.1:repo.git",
        "git@[@127.0.0.1:repo.git",
    ],
    ids=[
        "leading_bracket_loopback",
        "leading_bracket_ipv6",
        "leading_bracket_metadata",
        "unmatched_bracket_user",
        "unmatched_bracket_empty",
    ],
)
def test_scp_bracket_host_confusion_is_blocked(url: str) -> None:
    # git unwraps a bracket group at the start (or right after '@') as the host
    # and discards a trailing @benign-host, and an unmatched '[' desyncs a naive
    # parser, so ssh clones from the bracketed IP while a benign host is read.
    result = check_remote_dependency_url(url)
    assert not result.allowed


def test_leading_bracket_defeats_allowlist() -> None:
    # The operator allowlisted github.com, but git clones from 127.0.0.1 (the
    # leading bracket group), so this must be blocked, not read as github.com.
    result = check_remote_dependency_url(
        "[127.0.0.1]@github.com:x/repo.git",
        allowed_pattern=AllowDenyPattern(allow=["^github\\.com$"]),
    )
    assert not result.allowed


@pytest.mark.parametrize(
    "url",
    [
        "[github.com]@[169.254.169.254]:x/repo.git",
        "[github.com]@[::1]:x/repo.git",
        "[github.com]@[fd00:ec2::254]:x/repo.git",
        "[foo]@[127.0.0.1]:x/repo.git",
    ],
    ids=["dual_metadata", "dual_loopback", "dual_ipv6_imds", "dual_loopback2"],
)
def test_scp_dual_bracket_host_confusion_is_blocked(url: str) -> None:
    # git treats a leading [benign] as the user and connects to the SECOND
    # bracket, so [github.com]@[169.254.169.254]:x clones from the metadata IP
    # while a naive parser reads github.com. A multi-bracket authority is
    # ambiguous and must be rejected.
    result = check_remote_dependency_url(url)
    assert not result.allowed


def test_dual_bracket_defeats_anchored_allowlist() -> None:
    # The validator would read the host as github.com and pass an anchored
    # ^github\.com$ allowlist, but git clones from 169.254.169.254, so it must
    # be blocked regardless of the allowlist.
    result = check_remote_dependency_url(
        "[github.com]@[169.254.169.254]:x/repo.git",
        allowed_pattern=AllowDenyPattern(allow=["^github\\.com$"]),
    )
    assert not result.allowed
