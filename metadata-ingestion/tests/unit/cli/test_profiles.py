import base64
import json
import os
import time
from pathlib import Path
from typing import Iterator
from unittest.mock import Mock, patch

import click
import pytest
import yaml
from click.testing import CliRunner

from datahub.cli import config_utils
from datahub.cli.config_utils import (
    MissingConfigError,
    load_client_config,
    persist_raw_datahub_config,
    profile_path,
    resolve_profile,
    select_cli_profile,
    write_oauth_config,
)
from datahub.cli.profile_cli import profile, with_profile_target
from datahub.entrypoints import datahub, init, lift_profile_args


@pytest.fixture(autouse=True)
def home(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Path]:
    monkeypatch.setenv("HOME", str(tmp_path))
    for key in list(os.environ):
        if key.startswith(("DATAHUB_GMS_", "DATAHUB_AUTH_")) or key in {
            "DATAHUB_PROFILE",
            "DATAHUB_SKIP_CONFIG",
            "DATAHUB_USERNAME",
            "DATAHUB_PASSWORD",
        }:
            monkeypatch.delenv(key, raising=False)
    config_utils.reset_profile_cache()
    yield tmp_path
    config_utils.reset_profile_cache()


def store(
    name: str, server: str = "https://acme.example.com/gms", token: str = "secret"
) -> Path:
    path = profile_path(name)
    persist_raw_datahub_config({"gms": {"server": server, "token": token}}, path)
    return path


def invoke(*args: str) -> click.testing.Result:
    return CliRunner().invoke(datahub, lift_profile_args(list(args)))


def test_resolution_order_and_profile_credentials(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store("default", "http://localhost:8080", "legacy")
    store("acme")
    store("acme-staging", "https://acme-staging.example.com/gms", "staging")
    assert load_client_config().token == "legacy"
    config_utils.current_profile_path().write_text("acme\n")
    assert resolve_profile().source == "current-profile"
    assert load_client_config().token == "secret"
    monkeypatch.setenv("DATAHUB_GMS_URL", "http://localhost:9080")
    monkeypatch.setenv("DATAHUB_GMS_TOKEN", "environment")
    assert load_client_config().server == "http://localhost:9080"
    monkeypatch.setenv("DATAHUB_PROFILE", "acme-staging")
    assert resolve_profile().source == "DATAHUB_PROFILE"
    assert load_client_config().token == "staging"
    assert load_client_config(profile="acme").token == "secret"
    with select_cli_profile("acme"):
        assert load_client_config().token == "secret"
    assert load_client_config().token == "staging"


def test_conflicting_environment_warns_once(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    store("acme")
    monkeypatch.setenv("DATAHUB_GMS_URL", "http://localhost:8080")
    monkeypatch.setenv("DATAHUB_PROFILE", "acme")
    load_client_config()
    load_client_config()
    stderr = capsys.readouterr().err
    assert stderr.count("Warning:") == 1
    assert (
        "https://acme.example.com/gms" in stderr and "http://localhost:8080" in stderr
    )
    assert "secret" not in stderr


def test_same_environment_server_does_not_warn(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    store("acme")
    monkeypatch.setenv("DATAHUB_GMS_URL", "https://acme.example.com/gms/")
    assert load_client_config(profile="acme").token == "secret"
    assert not capsys.readouterr().err


@pytest.mark.parametrize("source", ["flag", "environment", "pointer"])
def test_unknown_profile_never_falls_back(
    source: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    store("default")
    store("acme")
    if source == "environment":
        monkeypatch.setenv("DATAHUB_PROFILE", "nope")
    elif source == "pointer":
        config_utils.current_profile_path().write_text("nope\n")
    with pytest.raises(MissingConfigError) as exc:
        load_client_config(profile="nope" if source == "flag" else None)
    assert "acme" in str(exc.value) and "https://acme.example.com/gms" in str(exc.value)
    assert "secret" not in str(exc.value)


def test_skip_config_conflict_and_legacy_behavior(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store("acme")
    config_utils.current_profile_path().write_text("nope\n")
    monkeypatch.setenv("DATAHUB_SKIP_CONFIG", "true")
    with pytest.raises(MissingConfigError):
        load_client_config()
    monkeypatch.setenv("DATAHUB_GMS_URL", "http://localhost:8080")
    assert load_client_config().server == "http://localhost:8080"
    with pytest.raises(click.UsageError):
        load_client_config(profile="acme")
    monkeypatch.setenv("DATAHUB_PROFILE", "acme")
    with pytest.raises(click.UsageError):
        load_client_config()


def test_legacy_host_port_and_token_only(monkeypatch: pytest.MonkeyPatch) -> None:
    store("default", token="stored")
    monkeypatch.setenv("DATAHUB_GMS_TOKEN", "env-token")
    assert load_client_config().token == "stored"
    monkeypatch.setenv("DATAHUB_GMS_HOST", "localhost")
    monkeypatch.setenv("DATAHUB_GMS_PORT", "9080")
    monkeypatch.setenv("DATAHUB_GMS_PROTOCOL", "https")
    assert load_client_config().server == "https://localhost:9080"
    assert load_client_config().token == "env-token"


def test_home_hack_layout_and_private_permissions(home: Path) -> None:
    path = store("acme")
    (path.parent / "telemetry-config.json").write_text("{}")
    assert load_client_config(profile="acme").token == "secret"
    assert path.stat().st_mode & 0o777 == 0o600
    assert path.parent.stat().st_mode & 0o777 == 0o700
    legacy = store("default")
    legacy.chmod(0o644)
    store("default")
    assert legacy.stat().st_mode & 0o777 == 0o600
    assert not (home / ".datahub" / "profiles" / "acme" / ".datahub").exists()


@pytest.mark.parametrize("name", ["../escape", "Acme", "", "a" * 64])
def test_invalid_names_cannot_escape_profiles(name: str) -> None:
    result = invoke(
        "init", "--profile", name, "--host", "http://localhost:8080", "--token", "x"
    )
    assert result.exit_code != 0


def test_init_repoint_refusal_and_refresh() -> None:
    path = store("default")
    result = invoke(
        "init",
        "--host",
        "https://acme-staging.example.com/gms",
        "--token",
        "tok-should-not-leak",
    )
    assert result.exit_code != 0
    assert "--profile acme-staging" in result.output
    assert "tok-should-not-leak" not in result.output
    assert yaml.safe_load(path.read_text())["gms"]["token"] == "secret"
    result = invoke("init", "--host", "https://acme.example.com/gms/", "--token", "new")
    assert result.exit_code == 0, result.output
    assert yaml.safe_load(path.read_text())["gms"]["token"] == "new"
    result = invoke(
        "init", "--force", "--host", "http://localhost:8080", "--token", "forced"
    )
    assert result.exit_code == 0, result.output
    assert yaml.safe_load(path.read_text())["gms"]["server"] == "http://localhost:8080"


def test_init_profile_destination_ignores_host_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store("acme")
    config_utils.current_profile_path().write_text("acme\n")
    monkeypatch.setenv("DATAHUB_GMS_URL", "http://localhost:8080")
    result = invoke("init", "--token", "new")
    assert result.exit_code != 0
    result = invoke(
        "init",
        "--profile",
        "local",
        "--token",
        "new",
        "--description",
        "Local development",
    )
    assert result.exit_code == 0, result.output
    assert config_utils.current_profile_path().read_text() == "acme\n"
    raw = yaml.safe_load(profile_path("local").read_text())
    assert raw["profile"]["description"] == "Local development"
    assert raw["gms"]["server"] == "http://localhost:8080"
    assert not profile_path("default").exists()


def test_profile_save_copies_file_ignoring_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store("default")
    monkeypatch.setenv("DATAHUB_GMS_URL", "http://localhost:8080")
    monkeypatch.setenv("DATAHUB_GMS_TOKEN", "different")
    result = invoke("profile", "save", "acme")
    assert result.exit_code == 0, result.output
    assert load_client_config(profile="acme").token == "secret"
    assert invoke("profile", "save", "acme").exit_code != 0
    assert invoke("profile", "save", "local", "--from", "acme").exit_code == 0
    assert load_client_config(profile="local").token == "secret"


def test_use_rename_remove_and_noninteractive_confirmation() -> None:
    store("acme")
    assert invoke("profile", "use", "acme").exit_code != 0
    assert not config_utils.current_profile_path().exists()
    assert invoke("profile", "use", "acme", "--yes").exit_code == 0
    assert invoke("profile", "rename", "acme", "acme-staging").exit_code == 0
    assert resolve_profile().name == "acme-staging"
    assert invoke("profile", "remove", "acme-staging").exit_code != 0
    assert invoke("profile", "remove", "default", "--yes").exit_code != 0
    assert invoke("profile", "remove", "acme-staging", "--yes").exit_code == 0
    assert not profile_path("acme-staging").exists()
    # A stale selection is reported until the user explicitly clears it.
    assert invoke("profile", "current").exit_code != 0
    assert invoke("profile", "use", "default", "--yes").exit_code == 0
    assert not config_utils.current_profile_path().exists()


def test_discovery_redacts_and_reports_expiry() -> None:
    payload = (
        base64.urlsafe_b64encode(json.dumps({"exp": int(time.time()) - 60}).encode())
        .decode()
        .rstrip("=")
    )
    token = f"header.{payload}.signature"
    store("acme", token=token)
    result = invoke("--profile", "acme", "profile", "list", "--format", "json")
    assert result.exit_code == 0, result.output
    info = json.loads(result.stdout)[0]
    assert info["active"] and info["token_status"] == "expired"
    result = invoke("--profile", "acme", "profile", "current", "--format", "json")
    assert result.exit_code == 0, result.output
    info = json.loads(result.stdout)
    assert set(info) == {"name", "server", "source", "token_expires"}
    assert info["source"] == "--profile" and info["token_expires"] < time.time()
    for args in [
        ("profile", "show", "acme"),
        ("profile", "list"),
        ("--profile", "acme", "profile", "current"),
    ]:
        assert token not in invoke(*args).output
    assert token in invoke("profile", "show", "acme", "--show-token").output


def test_doctor_repairs_only_with_fix() -> None:
    path = store("acme")
    path.write_text("gms_url: https://acme.example.com/gms\ngms_token: old\n")
    path.chmod(0o644)
    extra = path.parent / "notes.txt"
    extra.write_text("keep me")
    result = invoke("profile", "doctor")
    assert result.exit_code == 0, result.output
    assert "gms_url" in yaml.safe_load(path.read_text())
    assert path.stat().st_mode & 0o777 == 0o644
    assert invoke("profile", "doctor", "--fix").exit_code == 0
    raw = yaml.safe_load(path.read_text())
    assert raw["gms"]["token"] == "old" and "gms_token" not in raw
    assert path.stat().st_mode & 0o777 == 0o600
    assert extra.read_text() == "keep me"


def test_oauth_refresh_writes_to_selected_profile_only() -> None:
    legacy = store("default", token="untouched")
    payload = (
        base64.urlsafe_b64encode(json.dumps({"exp": int(time.time()) - 1}).encode())
        .decode()
        .rstrip("=")
    )
    path = profile_path("acme")
    write_oauth_config(
        "https://acme.example.com/gms",
        f"header.{payload}.signature",
        "client",
        "refresh",
        path=path,
    )
    with patch(
        "requests.post",
        return_value=Mock(
            status_code=200,
            json=lambda: {"access_token": "renewed", "refresh_token": "rotated"},
        ),
    ):
        assert load_client_config(profile="acme").token == "renewed"
    assert yaml.safe_load(path.read_text())["oauth"]["refresh_token"] == "rotated"
    assert yaml.safe_load(legacy.read_text())["gms"]["token"] == "untouched"
    cfg = load_client_config(profile="acme", refresh_per_request=True)
    assert cfg.auth is not None and cfg.auth.config is not None
    assert cfg.auth.config["config_file"] == str(path)
    assert path.stat().st_mode & 0o777 == 0o600


@pytest.mark.parametrize(
    "args, expected",
    [
        (["search", "x", "--profile", "acme"], ["--profile", "acme", "search", "x"]),
        (
            ["profile", "current", "--profile=acme"],
            ["--profile=acme", "profile", "current"],
        ),
        (
            ["search", "--", "--profile", "literal"],
            ["search", "--", "--profile", "literal"],
        ),
    ],
)
def test_argv_prepass(args: list[str], expected: list[str]) -> None:
    assert lift_profile_args(args) == expected


def test_unknown_profile_rejected_before_unrelated_command() -> None:
    store("acme")
    result = invoke("--profile", "nope", "version")
    assert result.exit_code != 0


def test_write_target_announcement() -> None:
    store("acme")

    @click.command("put")
    @with_profile_target
    def command() -> None:
        click.echo("done")

    with select_cli_profile("acme"):
        result = CliRunner().invoke(command)
    assert result.exit_code == 0, result.output
    assert "secret" not in result.output
    assert "acme" in result.stderr and "https://acme.example.com/gms" in result.stderr
    assert "done" in result.stdout


def test_agent_help_and_context() -> None:
    for args in [("--help",), ("--agent-context",)]:
        result = CliRunner().invoke(profile, list(args))
        assert result.exit_code == 0
        assert "DATAHUB_PROFILE" in result.output
        assert "profile current --format json" in result.output
    result = CliRunner().invoke(init, ["--agent-context"])
    assert result.exit_code == 0
    assert "Never run `datahub init` without" in result.output


def test_named_profile_keeps_environment_oauth_precedence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store("acme")
    monkeypatch.setenv("DATAHUB_AUTH_TYPE", "k8s_oidc")
    config = load_client_config(profile="acme")
    assert config.server == "https://acme.example.com/gms"
    assert config.token is None
    assert config.auth is not None and config.auth.type == "k8s_oidc"


def test_environment_and_skip_do_not_read_saved_config(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store("default")
    monkeypatch.setenv("DATAHUB_SKIP_CONFIG", "true")
    monkeypatch.setenv("DATAHUB_GMS_URL", "http://localhost:8080")
    with patch.object(
        config_utils,
        "stored_server",
        side_effect=AssertionError("unexpected file read"),
    ):
        assert load_client_config().server == "http://localhost:8080"


@pytest.mark.parametrize(
    "group_name, command_name, announces",
    [
        ("put", "aspect", True),
        ("ingest", "run", True),
        ("ingest", "list-source-runs", False),
        ("user", "upsert", True),
        ("dataproduct", "add_owner", True),
        ("container", "tag", True),
        ("dataset", "get", False),
    ],
)
def test_nested_write_announcements(
    group_name: str, command_name: str, announces: bool
) -> None:
    store("acme")

    @click.group("datahub")
    def root() -> None:
        pass

    @root.group(group_name)
    def group() -> None:
        pass

    @group.command(command_name)
    @with_profile_target
    def command() -> None:
        click.echo("done")

    with select_cli_profile("acme"):
        result = CliRunner().invoke(root, [group_name, command_name])
    assert result.exit_code == 0, result.output
    assert ("https://acme.example.com/gms" in result.stderr) == announces
    assert result.stdout.strip() == "done"
