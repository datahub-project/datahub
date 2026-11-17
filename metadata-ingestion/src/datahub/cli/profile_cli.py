import importlib.resources
import json
import os
import shutil
import sys
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from functools import wraps
from typing import Callable, Dict, Optional, ParamSpec, TypeVar

import click
import yaml

from datahub.cli.agent_context import make_agent_aware_group
from datahub.cli.config_utils import (
    MissingConfigError,
    ResolvedProfile,
    available_profile_message,
    available_profiles,
    current_profile_path,
    ensure_private_directory,
    persist_raw_datahub_config,
    profile_path,
    profile_target_line,
    profile_token_expiry,
    read_profile_config,
    reset_profile_cache,
    resolve_profile,
)


@dataclass(frozen=True)
class ProfileInfo:
    name: str
    server: Optional[str]
    token_expires: Optional[float]
    token_status: str
    description: Optional[str]
    active: bool


def _read_profile(name: str) -> Dict[str, object]:
    path = profile_path(name)
    if not path.is_file():
        raise MissingConfigError(
            f"Unknown profile {name!r}. {available_profile_message()}"
        )
    try:
        return read_profile_config(path)
    except (ValueError, yaml.YAMLError) as exc:
        raise click.ClickException(f"Cannot read {path}: {exc}") from exc


def _token_status(raw: Dict[str, object]) -> str:
    expiry = profile_token_expiry(raw)
    gms = raw.get("gms")
    if not isinstance(gms, dict) or not gms.get("token"):
        return "no token"
    if expiry is None:
        return "no expiry"
    if expiry <= time.time():
        return "expired"
    return "valid until " + datetime.fromtimestamp(expiry, timezone.utc).isoformat()


def _require_yes(yes: bool, prompt: str) -> None:
    if yes:
        return
    if not sys.stdin.isatty():
        raise click.UsageError("Non-interactive use requires --yes.")
    click.confirm(prompt, abort=True)


def set_current_profile(name: str) -> None:
    pointer = current_profile_path()
    if name == "default":
        pointer.unlink(missing_ok=True)
    else:
        ensure_private_directory(pointer.parent)
        pointer.write_text(name + "\n", encoding="utf-8")
        pointer.chmod(0o600)
    reset_profile_cache()


def _agent_context() -> str:
    return (
        importlib.resources.files("datahub.cli.resources")
        .joinpath("PROFILE_AGENT_CONTEXT.md")
        .read_text(encoding="utf-8")
    )


@click.group(
    cls=make_agent_aware_group("PROFILE_AGENT_CONTEXT.md"), invoke_without_command=True
)
@click.option(
    "--agent-context", is_flag=True, help="Print agent connection rules and exit."
)
@click.pass_context
def profile(ctx: click.Context, agent_context: bool) -> None:
    """Manage named DataHub connections."""
    if agent_context:
        click.echo(_agent_context())
        ctx.exit()
    if ctx.invoked_subcommand is None:
        click.echo(ctx.get_help())


@profile.command("list")
@click.option(
    "--format", "output_format", type=click.Choice(["text", "json"]), default="text"
)
def list_profiles(output_format: str) -> None:
    """List profiles without revealing credentials."""
    try:
        resolved: Optional[ResolvedProfile] = resolve_profile()
    except (MissingConfigError, click.UsageError) as exc:
        # An unknown or stale selection must not hide the list needed to fix it.
        click.echo(f"Warning: {exc}", err=True)
        resolved = None
    # With DATAHUB_GMS_URL in effect no stored profile is in use.
    active_name = (
        resolved.name
        if resolved is not None and resolved.source != "DATAHUB_GMS_URL"
        else None
    )
    profiles: list[ProfileInfo] = []
    for item in available_profiles():
        try:
            raw = _read_profile(item.name)
            metadata = raw.get("profile")
            description = (
                metadata.get("description") if isinstance(metadata, dict) else None
            )
            profiles.append(
                ProfileInfo(
                    name=item.name,
                    server=item.server,
                    token_expires=profile_token_expiry(raw),
                    token_status=_token_status(raw),
                    description=description if isinstance(description, str) else None,
                    active=item.name == active_name,
                )
            )
        except click.ClickException:
            profiles.append(
                ProfileInfo(
                    name=item.name,
                    server=item.server,
                    token_expires=None,
                    token_status="invalid config",
                    description=None,
                    active=item.name == active_name,
                )
            )
    if output_format == "json":
        click.echo(json.dumps([asdict(p) for p in profiles]))
    else:
        for info in profiles:
            click.echo(
                f"{'*' if info.active else ' '} {info.name}  {info.server or '(invalid config)'}  {info.token_status}  {info.description or ''}".rstrip()
            )
        if not profiles:
            click.echo(available_profile_message())
        if resolved is not None and resolved.source == "DATAHUB_GMS_URL":
            click.echo(f"Active: DATAHUB_GMS_URL ({resolved.server}), not a profile.")


@profile.command("current")
@click.option(
    "--format", "output_format", type=click.Choice(["text", "json"]), default="text"
)
def current(output_format: str) -> None:
    """Report the resolved connection and its selection source."""
    resolved = resolve_profile()
    expiry = None
    if resolved.path.is_file() and resolved.source != "DATAHUB_GMS_URL":
        expiry = profile_token_expiry(_read_profile(resolved.name))
    elif resolved.source == "DATAHUB_GMS_URL":
        from datahub.configuration.env_vars import get_gms_token
        from datahub.ingestion.auth.oauth_session import decode_jwt_exp

        token = get_gms_token()
        expiry = decode_jwt_exp(token) if token else None
    if output_format == "json":
        click.echo(
            json.dumps(
                {
                    "name": resolved.name,
                    "server": resolved.server,
                    "source": resolved.source,
                    "token_expires": expiry,
                }
            )
        )
    else:
        click.echo(profile_target_line(resolved))


@profile.command("use")
@click.argument("name")
@click.option("--yes", is_flag=True)
@click.option(
    "--shell",
    "shell_only",
    is_flag=True,
    help='Print an export for this shell only, e.g. eval "$(datahub profile use --shell prod)".',
)
def use(name: str, yes: bool, shell_only: bool) -> None:
    """Change the default for all shells on this machine, or with --shell, for one shell."""
    if name != "default":
        # Also validates the name, which keeps the printed export shell-safe.
        _read_profile(name)
    if shell_only:
        click.echo(f"export DATAHUB_PROFILE={name}")
        return
    _require_yes(yes, f"Use profile {name!r} as the machine default?")
    set_current_profile(name)
    click.echo(f"Active profile: {name}")
    click.echo(
        "This applies to every shell and agent on this machine. To switch only "
        f'this shell, run: eval "$(datahub profile use --shell {name})"',
        err=True,
    )


@profile.command("save")
@click.argument("name")
@click.option("--from", "source", default="default", show_default=True)
@click.option("--force", is_flag=True)
def save(name: str, source: str, force: bool) -> None:
    """Copy a stored connection into a named profile, ignoring environment credentials."""
    target = profile_path(name)
    if name == "default":
        raise click.UsageError("Save requires a named profile other than default.")
    if target.exists() and not force:
        raise click.UsageError(
            f"Profile {name!r} already exists. Use --force to overwrite."
        )
    raw = _read_profile(source)
    # Refresh tokens rotate on use; two files sharing one would revoke each other.
    had_oauth = raw.pop("oauth", None) is not None
    persist_raw_datahub_config(raw, target)
    click.echo(f"Saved profile {name!r} from {source!r}.")
    if had_oauth:
        click.echo(
            f"The OAuth session was not copied; {name!r} uses the current access token "
            f"until it expires. Run `datahub --profile {name} init --oauth` for its own session.",
            err=True,
        )


@profile.command("show")
@click.argument("name")
@click.option("--show-token", is_flag=True)
def show(name: str, show_token: bool) -> None:
    """Inspect a stored connection. Tokens are redacted by default."""
    raw = _read_profile(name)
    gms = raw.get("gms")
    oauth = raw.get("oauth")
    server = gms.get("server") if isinstance(gms, dict) else None
    token = gms.get("token") if isinstance(gms, dict) else None
    mode = (
        "OAuth with refresh"
        if isinstance(oauth, dict) and oauth.get("refresh_token")
        else "static"
    )
    click.echo(
        f"Profile: {name}\nServer: {server}\nToken: {token if show_token else '(redacted)'}\nToken status: {_token_status(raw)}\nAuth mode: {mode}\nFile: {profile_path(name)}"
    )


@profile.command("remove")
@click.argument("name")
@click.option("--yes", is_flag=True)
def remove(name: str, yes: bool) -> None:
    """Remove a named profile directory after confirmation."""
    if name == "default":
        raise click.UsageError("The default profile cannot be removed.")
    _read_profile(name)
    _require_yes(yes, f"Delete profile {name!r} and its directory?")
    directory = profile_path(name).parent
    if directory.is_symlink():
        directory.unlink()
    else:
        shutil.rmtree(directory)
    pointer = current_profile_path()
    if pointer.is_file() and pointer.read_text(encoding="utf-8").strip() == name:
        pointer.unlink()
        click.echo("It was the active profile; the default profile is active now.")
    reset_profile_cache()
    click.echo(f"Removed profile {name!r}.")


@profile.command("rename")
@click.argument("old")
@click.argument("new")
def rename(old: str, new: str) -> None:
    """Rename a profile and preserve its active selection."""
    source, target = profile_path(old), profile_path(new)
    if "default" in (old, new):
        raise click.UsageError("The default profile cannot be renamed or replaced.")
    _read_profile(old)
    if target.parent.exists():
        raise click.UsageError(f"Profile directory {new!r} already exists.")
    source.parent.rename(target.parent)
    pointer = current_profile_path()
    if pointer.is_file() and pointer.read_text(encoding="utf-8").strip() == old:
        set_current_profile(new)
    reset_profile_cache()
    click.echo(f"Renamed profile {old!r} to {new!r}.")


@profile.command("doctor")
@click.option(
    "--fix",
    is_flag=True,
    help="Repair legacy keys and permissions. Never delete extra files.",
)
def doctor(fix: bool) -> None:
    """Diagnose stored profiles without making network requests."""
    issues = 0
    for item in available_profiles():
        try:
            raw = _read_profile(item.name)
        except click.ClickException as exc:
            click.echo(f"{item.name}: {exc}. Fix the YAML in {item.path}.")
            issues += 1
            continue
        if _token_status(raw) == "expired":
            click.echo(
                f"{item.name}: token expired. Run datahub init --profile {item.name} --host {item.server or '<host>'} with the same authentication flags."
            )
            issues += 1
        legacy = {"gms_url": "server", "gms_token": "token"}
        gms_value = raw.get("gms")
        gms = dict(gms_value) if isinstance(gms_value, dict) else {}
        changed = False
        for old, new in legacy.items():
            if old in raw or old in gms:
                click.echo(
                    f"{item.name}: legacy {old}. Move it to gms.{new} (or run doctor --fix)."
                )
                issues += 1
                if fix:
                    value = raw.pop(old, None)
                    value = gms.pop(old, value)
                    if new not in gms:
                        gms[new] = value
                    raw["gms"] = gms
                    changed = True
        paths = [item.path]
        if item.name != "default":
            paths.append(item.path.parent)
        for path in paths:
            expected = 0o700 if path.is_dir() else 0o600
            if os.name != "nt" and path.stat().st_mode & 0o777 != expected:
                click.echo(
                    f"{item.name}: loose permissions on {path}. Run chmod {expected:o} {path} (or doctor --fix)."
                )
                issues += 1
                if fix:
                    path.chmod(expected)
        if item.name != "default":
            for extra in sorted(item.path.parent.iterdir()):
                if extra.name not in {".datahubenv", ".datahubenv.lock"}:
                    click.echo(
                        f"{item.name}: stray non-config file {extra}. Inspect it and move it out of the profile directory if unneeded."
                    )
                    issues += 1
        if changed:
            persist_raw_datahub_config(raw, item.path)
    click.echo(
        f"Checked profiles: {issues} issue(s){' (repairs applied where possible)' if fix else ''}."
    )


_P = ParamSpec("_P")
_R = TypeVar("_R")


def _writes_metadata(ctx: click.Context) -> bool:
    commands: list[str] = []
    current: Optional[click.Context] = ctx
    while current is not None:
        if current.command.name and current.command.name != "datahub":
            commands.insert(0, current.command.name)
        current = current.parent
    if not commands or commands[0] == "profile" or isinstance(ctx.command, click.Group):
        return False
    if commands[0] in {"put", "delete", "container", "migrate"}:
        return True
    if commands[0] == "ingest":
        # `run` and `replay` write through the recipe's sink, so they announce
        # the real destination themselves once the recipe is loaded.
        return len(commands) > 1 and commands[1] in {"deploy", "mcps", "rollback"}
    verbs = {
        "upsert",
        "register",
        "create",
        "update",
        "remove",
        "add",
        "set",
        "unset",
        "assign",
        "unassign",
        "attach",
        "detach",
        "apply",
        "submit",
        "publish",
        "import",
        "rollback",
    }
    if any(command.replace("_", "-").split("-")[0] in verbs for command in commands):
        return True
    return bool(ctx.params.get("to_datahub") or ctx.params.get("update"))


def _announce_target(ctx: click.Context) -> None:
    if not _writes_metadata(ctx):
        return
    try:
        resolved: Optional[ResolvedProfile] = resolve_profile()
    except (MissingConfigError, click.UsageError):
        # Only an announcement: a command that needs the connection
        # reports the error itself, and one that doesn't (an ingest
        # recipe with its own sink) must not fail on a stale pointer.
        resolved = None
    if resolved is not None and resolved.name != "default":
        click.echo(profile_target_line(resolved), err=True)


def with_profile_target(callback: Callable[_P, _R]) -> Callable[_P, _R]:
    @wraps(callback)
    def wrapped(*args: _P.args, **kwargs: _P.kwargs) -> _R:
        ctx = click.get_current_context(silent=True)
        if ctx is not None:
            _announce_target(ctx)
        return callback(*args, **kwargs)

    return wrapped


def announce_profile_target(command: click.Command) -> None:
    """Announce the write target before `command` runs.

    Hooks the command's invoke rather than replacing its callback: telemetry
    detects its own wrappers by walking the callback, and an instrumented
    callback must stay the same object (see enable_auto_decorators).
    """
    original = command.invoke
    if getattr(original, "_announces_profile_target", False):
        return

    def invoke(ctx: click.Context) -> object:
        _announce_target(ctx)
        return original(ctx)

    invoke._announces_profile_target = True  # type: ignore[attr-defined]
    command.invoke = invoke  # type: ignore[method-assign]
