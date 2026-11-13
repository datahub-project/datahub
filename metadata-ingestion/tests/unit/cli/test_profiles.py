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
from datahub.entrypoints import datahub, init, lift_profile_args, main


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
    # An inherited DATAHUB_PROFILE must not break env-only subprocesses.
    monkeypatch.setenv("DATAHUB_PROFILE", "acme")
    assert load_client_config().server == "http://localhost:8080"


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
        ("ingest", "deploy", True),
        ("ingest", "run", False),
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


def test_removing_active_profile_resets_pointer() -> None:
    store("default", "http://localhost:8080")
    store("staging")
    assert invoke("profile", "use", "staging", "--yes").exit_code == 0
    assert invoke("profile", "remove", "staging", "--yes").exit_code == 0
    assert not config_utils.current_profile_path().exists()
    assert load_client_config().server == "http://localhost:8080"


def test_stale_pointer_still_lists_profiles() -> None:
    store("default", "http://localhost:8080")
    config_utils.current_profile_path().parent.mkdir(parents=True, exist_ok=True)
    config_utils.current_profile_path().write_text("gone\n")
    result = invoke("profile", "list")
    assert result.exit_code == 0
    assert "default" in result.output
    assert invoke("profile", "use", "default", "--yes").exit_code == 0
    assert load_client_config().server == "http://localhost:8080"


def test_environment_oauth_not_sent_to_explicit_profile_server(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store("acme", token="acme-token")
    monkeypatch.setenv("DATAHUB_GMS_URL", "https://other.example.com/gms")
    monkeypatch.setenv("DATAHUB_AUTH_TYPE", "k8s_oidc")
    config = load_client_config(profile="acme")
    assert config.server == "https://acme.example.com/gms"
    assert config.auth is None
    assert config.token == "acme-token"


def test_environment_oauth_kept_when_env_url_names_the_profile_server(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store("acme", token="acme-token")
    monkeypatch.setenv("DATAHUB_GMS_URL", "https://acme.example.com/gms/")
    monkeypatch.setenv("DATAHUB_AUTH_TYPE", "k8s_oidc")
    config = load_client_config(profile="acme")
    assert config.token is None
    assert config.auth is not None and config.auth.type == "k8s_oidc"


def test_use_shell_prints_export_without_moving_pointer() -> None:
    store("staging")
    result = invoke("profile", "use", "--shell", "staging")
    assert result.exit_code == 0, result.output
    assert result.stdout.strip() == "export DATAHUB_PROFILE=staging"
    assert not config_utils.current_profile_path().exists()
    assert invoke("profile", "use", "--shell", "missing").exit_code != 0


def test_pointer_source_is_named_in_target_line() -> None:
    store("staging")
    invoke("profile", "use", "staging", "--yes")
    result = invoke("profile", "current")
    assert "datahub profile use" in result.output


def test_stale_pointer_does_not_block_ingest_with_its_own_sink(home: Path) -> None:
    # The write announcement must not fail a recipe that never reads the
    # CLI connection (here a file sink).
    config_utils.current_profile_path().parent.mkdir(parents=True, exist_ok=True)
    config_utils.current_profile_path().write_text("deleted\n")
    recipe = home / "recipe.yml"
    out = home / "out.json"
    events = home / "events.json"
    events.write_text("[]")
    recipe.write_text(
        yaml.safe_dump(
            {
                "source": {"type": "file", "config": {"path": str(events)}},
                "sink": {"type": "file", "config": {"filename": str(out)}},
            }
        )
    )
    result = invoke("ingest", "run", "-c", str(recipe), "--no-default-report")
    assert result.exit_code == 0, result.output
    assert out.exists()


def _rest_recipe(home: Path, server: str) -> Path:
    events = home / "events.json"
    events.write_text("[]")
    recipe = home / "rest-recipe.yml"
    recipe.write_text(
        yaml.safe_dump(
            {
                "source": {"type": "file", "config": {"path": str(events)}},
                "sink": {"type": "datahub-rest", "config": {"server": server}},
            }
        )
    )
    return recipe


def test_explicit_profile_contradicting_recipe_sink_is_refused(
    home: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The recipe takes ${DATAHUB_GMS_URL} from an environment exported for
    # another instance; --profile must not appear to confirm that target.
    store("staging", "https://staging.example.com/gms")
    monkeypatch.setenv("DATAHUB_GMS_URL", "https://prod.example.com/gms")
    recipe = _rest_recipe(home, "${DATAHUB_GMS_URL}")
    with patch("datahub.ingestion.run.pipeline.Pipeline.create") as create:
        result = invoke("--profile", "staging", "ingest", "run", "-c", str(recipe))
    assert result.exit_code != 0
    assert "https://prod.example.com/gms" in result.output
    create.assert_not_called()


def test_recipe_sink_check_matches_or_warns_for_ambient_selection(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    from datahub.cli.ingest_cli import _check_sink_against_profile

    def sink(server: str) -> dict:
        return {"sink": {"type": "datahub-rest", "config": {"server": server}}}

    store("staging", "https://staging.example.com/gms")
    _check_sink_against_profile(sink("https://elsewhere.example.com/gms"))
    assert capsys.readouterr().err == ""  # no named profile: unchanged behavior

    monkeypatch.setenv("DATAHUB_PROFILE", "staging")
    _check_sink_against_profile(sink("https://staging.example.com/gms/"))
    assert "[recipe sink]" in capsys.readouterr().err
    _check_sink_against_profile(sink("https://elsewhere.example.com/gms"))
    assert "Warning" in capsys.readouterr().err
    _check_sink_against_profile({})
    assert "[default sink]" in capsys.readouterr().err
    with select_cli_profile("staging"):
        with pytest.raises(click.UsageError):
            _check_sink_against_profile(sink("https://elsewhere.example.com/gms"))


def test_connection_resolves_without_a_home_directory(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arbitrary-UID containers: no HOME and no passwd entry; Path.home() raises.
    monkeypatch.setattr("os.path.expanduser", lambda path: path)
    monkeypatch.setenv("DATAHUB_GMS_URL", "http://gms:8080")
    assert load_client_config().server == "http://gms:8080"


def test_auto_decorators_announce_without_replacing_callbacks() -> None:
    from datahub.cli.cli_utils import enable_auto_decorators

    store("acme")

    @click.group("datahub")
    def root() -> None:
        pass

    @root.group("user")
    def user() -> None:
        pass

    @user.command("upsert")
    def upsert() -> None:
        click.echo("done")

    enable_auto_decorators(root)
    enable_auto_decorators(root)  # idempotent: one announcement, not two
    with select_cli_profile("acme"):
        result = CliRunner().invoke(root, ["user", "upsert"])
    assert result.exit_code == 0, result.output
    assert result.stderr.count("https://acme.example.com/gms") == 1


def test_lite_config_stays_in_default_datahubenv(home: Path) -> None:
    from datahub.cli.lite_cli import get_lite_config, write_lite_config

    store("default", "http://localhost:8080")
    store("acme")
    with select_cli_profile("acme"):
        write_lite_config(get_lite_config())
    assert "lite" in yaml.safe_load((home / ".datahubenv").read_text())
    assert "lite" not in yaml.safe_load(profile_path("acme").read_text())


def test_oauth_refresh_follows_symlinked_config(home: Path) -> None:
    from datahub.ingestion.auth.oauth_session import _write_atomically

    real = home / "dotfiles" / "datahubenv"
    real.parent.mkdir()
    real.write_text("gms:\n  server: http://old\n")
    link = home / ".datahubenv"
    link.symlink_to(real)
    _write_atomically(str(link), {"gms": {"server": "http://new"}})
    assert link.is_symlink()
    assert "http://new" in real.read_text()


def test_list_marks_no_profile_active_under_env_url(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store("default", "http://localhost:8080")
    store("staging")
    invoke("profile", "use", "staging", "--yes")
    monkeypatch.setenv("DATAHUB_GMS_URL", "https://other.example.com/gms")
    result = invoke("profile", "list", "--format", "json")
    assert result.exit_code == 0
    assert not any(p["active"] for p in json.loads(result.output))


def test_reinit_keeps_description() -> None:
    args = ["init", "--profile", "acme", "--host", "https://acme.example.com/gms"]
    assert invoke(*args, "--token", "a", "--description", "Prod").exit_code == 0
    assert invoke(*args, "--token", "b", "--force").exit_code == 0
    raw = yaml.safe_load(profile_path("acme").read_text())
    assert raw["gms"]["token"] == "b"
    assert raw["profile"]["description"] == "Prod"


def test_writes_follow_symlinked_config(home: Path) -> None:
    real = home / "dotfiles" / "datahubenv"
    real.parent.mkdir()
    real.write_text("gms:\n  server: http://old\n")
    (home / ".datahubenv").symlink_to(real)
    store("default", "http://localhost:8080")
    assert (home / ".datahubenv").is_symlink()
    assert "http://localhost:8080" in real.read_text()


def test_secret_refs_and_chunking_follow_selected_profile(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from datahub.ingestion.agent.secrets import DatahubEnvResolver
    from datahub.ingestion.source.unstructured.chunking_config import (
        _get_default_gms_server,
        _get_default_gms_token,
    )

    store("default", "http://localhost:8080", "local-token")
    store("acme", token="acme-token")
    monkeypatch.setenv("DATAHUB_GMS_TOKEN", "env-token")
    with select_cli_profile("acme"):
        assert DatahubEnvResolver().resolve("DATAHUB_GMS_TOKEN") == "acme-token"
        assert _get_default_gms_server() == "https://acme.example.com/gms"
        token = _get_default_gms_token()
        assert token is not None and token.get_secret_value() == "acme-token"
    assert DatahubEnvResolver().resolve("DATAHUB_GMS_URL") == "http://localhost:8080"


def test_init_profile_through_main_entrypoint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # main() forces the "spawn" start method process-wide; keep that out of
    # the rest of the test session.
    monkeypatch.setattr("multiprocessing.set_start_method", Mock())
    with pytest.raises(SystemExit) as exc:
        main(
            [
                "init",
                "--profile",
                "acme",
                "--host",
                "https://acme.example.com/gms",
                "--token",
                "tok",
            ]
        )
    assert exc.value.code in (0, None)
    assert stored_server_of("acme") == "https://acme.example.com/gms"
    assert not profile_path("default").exists()
    assert not config_utils.current_profile_path().exists()


def stored_server_of(name: str) -> str:
    return yaml.safe_load(profile_path(name).read_text())["gms"]["server"]


def test_repoint_hint_suggests_an_unused_profile_name() -> None:
    store("default")
    store("acme-staging", "https://elsewhere.example.com/gms")
    result = invoke(
        "init", "--host", "https://acme-staging.example.com/gms", "--token", "t"
    )
    assert result.exit_code != 0
    assert "--profile acme-staging-2" in result.output


def test_empty_profile_selection_counts_as_unset(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store("default", "http://localhost:8080")
    monkeypatch.setenv("DATAHUB_PROFILE", "")
    config_utils.current_profile_path().parent.mkdir(parents=True, exist_ok=True)
    config_utils.current_profile_path().write_text("\n")
    assert load_client_config().server == "http://localhost:8080"


def test_stale_or_unknown_selection_keeps_repair_commands_working(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store("default", "http://localhost:8080")
    config_utils.current_profile_path().parent.mkdir(parents=True, exist_ok=True)
    config_utils.current_profile_path().write_text("gone\n")
    assert invoke("version").exit_code == 0
    monkeypatch.setenv("DATAHUB_PROFILE", "typo")
    result = invoke("profile", "list")
    assert result.exit_code == 0
    assert "default" in result.output


def test_profile_flag_reaches_child_processes() -> None:
    store("acme")
    seen: list = []

    @datahub.command("probe-env")
    def probe_env() -> None:
        seen.append(os.environ.get("DATAHUB_PROFILE"))

    try:
        assert invoke("--profile", "acme", "probe-env").exit_code == 0
    finally:
        datahub.commands.pop("probe-env")
    assert seen == ["acme"]
    assert "DATAHUB_PROFILE" not in os.environ


def test_save_does_not_share_an_oauth_session() -> None:
    write_oauth_config(
        host="https://acme.example.com/gms",
        access_token="access",
        client_id="cli",
        refresh_token="refresh",
        path=profile_path("default"),
    )
    result = invoke("profile", "save", "copy")
    assert result.exit_code == 0, result.output
    raw = yaml.safe_load(profile_path("copy").read_text())
    assert "oauth" not in raw
    assert raw["gms"]["token"] == "access"


def test_chunking_defaults_fail_on_unknown_profile(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from datahub.ingestion.source.unstructured.chunking_config import (
        _get_default_gms_server,
    )

    monkeypatch.setenv("DATAHUB_PROFILE", "missing")
    with pytest.raises(MissingConfigError):
        _get_default_gms_server()
