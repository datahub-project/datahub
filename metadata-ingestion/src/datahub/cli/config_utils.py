"""
For helper methods to contain manipulation of the config file in local system.
"""

import logging
import os
import re
import sys
import tempfile
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Dict, Iterator, List, Mapping, Optional, Tuple

import click
import yaml
from pydantic import BaseModel, ValidationError

from datahub.configuration.env_vars import (
    get_gms_host,
    get_gms_port,
    get_gms_protocol,
    get_gms_token,
    get_gms_url,
    get_skip_config,
    get_system_client_id,
    get_system_client_secret,
)
from datahub.ingestion.auth.env import ENV_AUTH_TYPE, build_auth_config_from_env
from datahub.ingestion.auth.oauth_session import (
    OAUTH_SESSION_AUTH_TYPE,
    decode_jwt_exp,
    has_oauth_session,
    read_session_token,
)
from datahub.ingestion.auth.registry import AuthConfig
from datahub.ingestion.graph.config import DatahubClientConfig

logger = logging.getLogger(__name__)

CONDENSED_DATAHUB_CONFIG_PATH = "~/.datahubenv"
DATAHUB_CONFIG_PATH: str = os.path.expanduser(CONDENSED_DATAHUB_CONFIG_PATH)
DATAHUB_ROOT_FOLDER: str = os.path.expanduser("~/.datahub")
ENV_SKIP_CONFIG = "DATAHUB_SKIP_CONFIG"

ENV_DATAHUB_SYSTEM_CLIENT_ID = "DATAHUB_SYSTEM_CLIENT_ID"
ENV_DATAHUB_SYSTEM_CLIENT_SECRET = "DATAHUB_SYSTEM_CLIENT_SECRET"

ENV_METADATA_HOST_URL = "DATAHUB_GMS_URL"
ENV_METADATA_TOKEN = "DATAHUB_GMS_TOKEN"
ENV_METADATA_HOST = "DATAHUB_GMS_HOST"
ENV_METADATA_PORT = "DATAHUB_GMS_PORT"
ENV_METADATA_PROTOCOL = "DATAHUB_GMS_PROTOCOL"


class MissingConfigError(Exception):
    SHOW_STACK_TRACE = False


def get_system_auth() -> Optional[str]:
    system_client_id = get_system_client_id()
    system_client_secret = get_system_client_secret()
    if system_client_id is not None and system_client_secret is not None:
        return f"Basic {system_client_id}:{system_client_secret}"
    return None


def _should_skip_config() -> bool:
    return get_skip_config()


@dataclass(frozen=True)
class ResolvedProfile:
    name: str
    path: Path
    source: str
    server: Optional[str]


_cli_profile: ContextVar[Optional[str]] = ContextVar(
    "datahub_cli_profile", default=None
)
_PROFILE_NAME = re.compile(r"[a-z0-9][a-z0-9._-]{0,62}\Z")


def validate_profile_name(name: str) -> str:
    if not _PROFILE_NAME.fullmatch(name):
        raise click.UsageError("Profile names must match [a-z0-9][a-z0-9._-]{0,62}.")
    return name


def _home() -> Path:
    # Not Path.home(): it raises when HOME is unset and the UID has no passwd
    # entry (containers run as an arbitrary UID). expanduser leaves "~" in
    # place instead, as the legacy import-time DATAHUB_CONFIG_PATH did.
    return Path(os.path.expanduser("~"))


def _profile_path_in(home: Path, name: str) -> Path:
    if name == "default":
        return home / ".datahubenv"
    return home / ".datahub" / "profiles" / name / ".datahubenv"


def profile_path(name: str) -> Path:
    validate_profile_name(name)
    return _profile_path_in(_home(), name)


def current_profile_path() -> Path:
    return _home() / ".datahub" / "current-profile"


def read_profile_config(path: Path) -> Dict[str, object]:
    with path.open() as stream:
        raw = yaml.safe_load(stream)
    if not isinstance(raw, dict) or not all(isinstance(k, str) for k in raw):
        raise ValueError(f"{path} must contain a YAML mapping.")
    return raw


def stored_server(path: Path) -> Optional[str]:
    raw = read_profile_config(path)
    gms = raw.get("gms")
    server = gms.get("server") if isinstance(gms, dict) else None
    return server if isinstance(server, str) else None


def available_profiles() -> List[ResolvedProfile]:
    names = ["default"]
    directory = _home() / ".datahub" / "profiles"
    if directory.is_dir():
        names.extend(
            sorted(
                p.name
                for p in directory.iterdir()
                if _PROFILE_NAME.fullmatch(p.name) and p.name != "default"
            )
        )
    profiles: List[ResolvedProfile] = []
    for name in names:
        path = profile_path(name)
        if path.is_file():
            try:
                server = stored_server(path)
            except (ValueError, OSError, yaml.YAMLError):
                server = None
            profiles.append(
                ResolvedProfile(name=name, path=path, source="file", server=server)
            )
    return profiles


def available_profile_message() -> str:
    profiles = available_profiles()
    return "Available profiles: " + (
        ", ".join(f"{p.name} ({p.server or 'invalid config'})" for p in profiles)
        or "none. Create one with datahub init --profile local."
    )


def env_profile() -> Optional[str]:
    """DATAHUB_PROFILE, treating an empty value (e.g. `export DATAHUB_PROFILE=`) as unset."""
    return os.environ.get("DATAHUB_PROFILE", "").strip() or None


def explicit_profile_selected() -> bool:
    """True when --profile or DATAHUB_PROFILE picks a profile over raw GMS env vars."""
    if _cli_profile.get() is not None:
        return True
    return env_profile() is not None and not _should_skip_config()


def reset_profile_cache() -> None:
    _resolve_profile.cache_clear()


@contextmanager
def select_cli_profile(name: Optional[str]) -> Iterator[None]:
    token = _cli_profile.set(name)
    reset_profile_cache()
    try:
        yield
    finally:
        _cli_profile.reset(token)
        reset_profile_cache()


def resolve_profile(
    profile: Optional[str] = None,
    *,
    allow_create: bool = False,
    for_init: bool = False,
) -> ResolvedProfile:
    explicit = profile if profile is not None else _cli_profile.get()
    source = "--profile" if explicit is not None else "DATAHUB_PROFILE"
    if explicit is not None and _should_skip_config():
        raise click.UsageError(
            "An explicit profile cannot be combined with DATAHUB_SKIP_CONFIG."
        )
    if explicit is None:
        explicit = env_profile()
        if explicit is not None and _should_skip_config():
            # Executors set DATAHUB_SKIP_CONFIG for subprocesses that may inherit
            # DATAHUB_PROFILE from a user shell; the env-only contract wins.
            logger.debug("DATAHUB_SKIP_CONFIG is set; ignoring DATAHUB_PROFILE.")
            explicit = None
    env_server, _ = _get_config_from_env(warn_deprecated=False)
    pointer = current_profile_path()
    active = None
    if explicit is None and not _should_skip_config() and (for_init or not env_server):
        if pointer.is_file():
            active = pointer.read_text(encoding="utf-8").strip() or None
    return _resolve_profile(
        home=str(_home()),
        explicit=explicit,
        source=source,
        env_server=env_server,
        active=active,
        allow_create=allow_create,
        for_init=for_init,
        skip_config=_should_skip_config(),
    )


@lru_cache(maxsize=32)
def _resolve_profile(
    home: str,
    explicit: Optional[str],
    source: str,
    env_server: Optional[str],
    active: Optional[str],
    allow_create: bool,
    for_init: bool,
    skip_config: bool,
) -> ResolvedProfile:
    name = (
        explicit
        if explicit is not None
        else active
        if active is not None
        else "default"
    )
    validate_profile_name(name)
    path = _profile_path_in(Path(home), name)
    if explicit is None:
        source = "current-profile" if active is not None else "default"
        if env_server and not for_init:
            source = "DATAHUB_GMS_URL"
    if explicit is None and (source == "DATAHUB_GMS_URL" or skip_config):
        return ResolvedProfile(name=name, path=path, source=source, server=env_server)
    if (
        not path.is_file()
        and not allow_create
        and (explicit is not None or name != "default")
    ):
        hint = (
            f" {current_profile_path()} names it; run `datahub profile use default` to reset."
            if source == "current-profile"
            else ""
        )
        raise MissingConfigError(
            f"Unknown profile {name!r}.{hint} {available_profile_message()}"
        )
    server = None
    if path.is_file():
        try:
            server = stored_server(path)
        except (ValueError, OSError, yaml.YAMLError):
            # Loading reports malformed configs; discovery and init must still work.
            pass
    if source == "DATAHUB_GMS_URL":
        server = env_server
    elif explicit is not None and env_server and server and not for_init:
        if not same_server(server, env_server):
            click.echo(
                f"Warning: profile {name!r} selects {server}; ignoring DATAHUB_GMS_URL ({env_server}).",
                err=True,
            )
    return ResolvedProfile(name=name, path=path, source=source, server=server)


def same_server(left: Optional[str], right: Optional[str]) -> bool:
    if not left or not right:
        return False
    return left.rstrip("/") == right.rstrip("/")


def ensure_private_directory(path: Path) -> None:
    if not path.exists():
        ensure_private_directory(path.parent)
        path.mkdir(mode=0o700, exist_ok=True)


def persist_raw_datahub_config(
    config: Mapping[str, object], path: Optional[Path] = None
) -> None:
    target = path or resolve_profile(for_init=True, allow_create=True).path
    # Users keep ~/.datahubenv or profile directories as symlinks (dotfiles repos,
    # older HOME-override layouts); replacing the link would orphan its target.
    target = Path(os.path.realpath(target))
    ensure_private_directory(target.parent)
    fd, temporary = tempfile.mkstemp(dir=target.parent, prefix=".datahubenv.")
    try:
        with os.fdopen(fd, "w") as outfile:
            yaml.safe_dump(dict(config), outfile, default_flow_style=False)
        os.chmod(temporary, 0o600)
        os.replace(temporary, target)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)
    reset_profile_cache()


def get_raw_client_config(path: Optional[Path] = None) -> Optional[Dict[str, object]]:
    target = path or resolve_profile(for_init=True).path
    try:
        return read_profile_config(target)
    except (yaml.YAMLError, ValueError) as exc:
        click.secho(f"{target} malformed, error: {exc}", bold=True)
        return None


def profile_token_expiry(raw: Dict[str, object]) -> Optional[float]:
    gms = raw.get("gms")
    token = gms.get("token") if isinstance(gms, dict) else None
    return decode_jwt_exp(token) if isinstance(token, str) else None


_SOURCE_LABELS = {
    "current-profile": "current-profile, the machine-wide default set by `datahub profile use`",
}


def profile_target_line(resolved: ResolvedProfile) -> str:
    source = _SOURCE_LABELS.get(resolved.source, resolved.source)
    return f"{resolved.name}  {resolved.server or '(no server)'}  (from {source})"


_NAMED_PROFILE_SOURCES = frozenset({"--profile", "DATAHUB_PROFILE", "current-profile"})


def selected_named_profile() -> Optional[ResolvedProfile]:
    """The selected profile when one is chosen by name rather than by legacy
    defaults or DATAHUB_GMS_URL. Such a profile owns both the server and its
    credential, so neither may be taken from the environment on its own."""
    resolved = resolve_profile()
    return resolved if resolved.source in _NAMED_PROFILE_SOURCES else None


class OAuthSessionConfig(BaseModel):
    """OAuth2 session tokens stored alongside GMS credentials in ~/.datahubenv."""

    client_id: str
    refresh_token: Optional[str] = None
    # Stored from the discovery document so refresh doesn't rely on a hardcoded path.
    token_endpoint: Optional[str] = None


class DatahubConfig(BaseModel):
    gms: DatahubClientConfig
    oauth: Optional[OAuthSessionConfig] = None


def _get_config_from_env(
    *, warn_deprecated: bool = True
) -> Tuple[Optional[str], Optional[str]]:
    host = get_gms_host()
    port = get_gms_port()
    token = get_gms_token()
    protocol = get_gms_protocol()
    url = get_gms_url()
    if port is not None:
        url = f"{protocol}://{host}:{port}"
        return url, token
    # The reason for using host as URL is backward compatibility
    # If port is not being used we assume someone is using host env var as URL
    if url is None and host is not None and warn_deprecated:
        logger.warning(
            f"Do not use {ENV_METADATA_HOST} as URL. Use {ENV_METADATA_HOST_URL} instead"
        )
    return url or host, token


def require_config_from_env() -> Tuple[str, Optional[str]]:
    host, token = _get_config_from_env()
    if host is None:
        raise MissingConfigError("No GMS host was provided in env variables.")
    return host, token


def get_url_from_env() -> Optional[str]:
    """The env-configured GMS URL, or None when the environment does not set one."""
    url, _ = _get_config_from_env()
    return url


def load_client_config(
    profile: Optional[str] = None, *, refresh_per_request: bool = False
) -> DatahubClientConfig:
    """Resolve environment credentials or the selected connection profile.

    For a `datahub init --oauth` session the returned `token` is the session's
    current access token. With `refresh_per_request`, the config instead carries
    a token provider that refreshes it as it expires, for long-running clients.
    """
    # DATAHUB_AUTH_TYPE engages an env-configured OAuth token provider and takes
    # precedence over a static DATAHUB_GMS_TOKEN (the two are mutually exclusive
    # on DatahubClientConfig). Resolving it here is what lets processes that only
    # inherit environment variables — e.g. ingestion recipe subprocesses spawned
    # by the Remote Executor, whose default sink resolves through this function —
    # authenticate with short-lived OAuth tokens.
    resolved = resolve_profile(profile)
    auth_env = build_auth_config_from_env()
    gms_host_env, gms_token_env = _get_config_from_env()
    if (
        auth_env is not None
        and gms_host_env
        and resolved.source != "DATAHUB_GMS_URL"
        and not same_server(resolved.server, gms_host_env)
    ):
        # Env OAuth is minted for the env server; a profile that points
        # elsewhere must not receive its tokens. On the same server it still wins.
        logger.warning(
            f"{ENV_AUTH_TYPE} is configured for {ENV_METADATA_HOST_URL}; ignoring it "
            f"because profile {resolved.name!r} was selected."
        )
        auth_env = None
    if auth_env is not None and get_system_client_id() is not None:
        logger.warning(
            f"Both {ENV_AUTH_TYPE} and {ENV_DATAHUB_SYSTEM_CLIENT_ID} are set; "
            f"using {ENV_AUTH_TYPE} and ignoring the system client credentials."
        )

    if gms_host_env and resolved.source == "DATAHUB_GMS_URL":
        if auth_env is not None:
            return DatahubClientConfig(server=gms_host_env, auth=auth_env)
        # TODO We should also load system auth credentials here.
        return DatahubClientConfig(server=gms_host_env, token=gms_token_env)

    if _should_skip_config():
        raise MissingConfigError(
            "You have set the skip config flag, but no GMS host or token was provided in env variables."
        )

    try:
        _ensure_datahub_config(resolved.path)
        client_config_dict = get_raw_client_config(resolved.path)
        datahub_config: DatahubClientConfig = DatahubConfig.model_validate(
            client_config_dict
        ).gms
    except MissingConfigError:
        if auth_env is not None:
            # A fully env-configured OAuth container is missing only the server
            # URL — telling it to run `datahub init` would be misleading.
            raise MissingConfigError(
                f"{ENV_AUTH_TYPE} is set but no GMS server was provided. "
                f"Set {ENV_METADATA_HOST_URL} (or run `datahub init --profile local` to create "
                f"a {resolved.path} file)."
            ) from None
        raise
    except ValidationError as e:
        click.echo(f"Error loading your {resolved.path}")
        click.echo(e, err=True)
        sys.exit(1)

    if auth_env is not None:
        # Env-configured OAuth overrides a static token stored in the config
        # file, and supersedes the browser-flow (`datahub init --oauth`) token
        # refresh below.
        if datahub_config.token:
            logger.warning(
                f"{ENV_AUTH_TYPE} is set; ignoring the static token stored in "
                f"{resolved.path}."
            )
        return datahub_config.model_copy(update={"token": None, "auth": auth_env})

    if refresh_per_request and has_oauth_session(client_config_dict):
        return datahub_config.model_copy(
            update={
                "token": None,
                "auth": AuthConfig(
                    type=OAUTH_SESSION_AUTH_TYPE,
                    config={"config_file": str(resolved.path)},
                ),
            }
        )

    refreshed_token = refresh_oauth_token_if_needed(resolved.path)
    if refreshed_token is not None:
        datahub_config = datahub_config.model_copy(update={"token": refreshed_token})

    return datahub_config


def _ensure_datahub_config(path: Optional[Path] = None) -> None:
    target = path or resolve_profile().path
    if not target.is_file():
        raise MissingConfigError(
            f"No {target} file found, and no configuration was found in environment variables. "
            f"Run `datahub init --profile local` to create a profile. {available_profile_message()}"
        )


def _carry_profile_metadata(
    config_dict: Dict[str, object], path: Optional[Path], description: Optional[str]
) -> None:
    if description is not None:
        config_dict["profile"] = {"description": description}
        return
    target = path or resolve_profile(for_init=True, allow_create=True).path
    if not target.is_file():
        return
    try:
        previous = read_profile_config(target).get("profile")
    except (OSError, ValueError, yaml.YAMLError):
        return
    if isinstance(previous, dict):
        config_dict["profile"] = previous


def write_gms_config(
    host: str,
    token: Optional[str],
    merge_with_previous: bool = True,
    *,
    path: Optional[Path] = None,
    description: Optional[str] = None,
) -> None:
    config = DatahubConfig(gms=DatahubClientConfig(server=host, token=token))
    if merge_with_previous:
        try:
            previous_config = get_raw_client_config(path)
            assert isinstance(previous_config, dict)
        except Exception as e:
            # ok to fail on this
            previous_config = {}
            logger.debug(f"Failed to retrieve config: {e}. This isn't fatal.")
        config_dict = {**previous_config, **config.model_dump(exclude={"oauth"})}
    else:
        config_dict = config.model_dump(exclude={"oauth"})
    _carry_profile_metadata(config_dict, path, description)
    persist_raw_datahub_config(config_dict, path)


def write_oauth_config(
    host: str,
    access_token: str,
    client_id: str,
    refresh_token: Optional[str],
    token_endpoint: Optional[str] = None,
    *,
    path: Optional[Path] = None,
    description: Optional[str] = None,
) -> None:
    """Write GMS config together with OAuth2 session tokens to ~/.datahubenv."""
    config = DatahubConfig(gms=DatahubClientConfig(server=host, token=access_token))
    oauth = OAuthSessionConfig(
        client_id=client_id,
        refresh_token=refresh_token,
        token_endpoint=token_endpoint,
    )
    config_dict = config.model_dump()
    config_dict["oauth"] = oauth.model_dump(exclude_none=True)
    _carry_profile_metadata(config_dict, path, description)
    persist_raw_datahub_config(config_dict, path)


def refresh_oauth_token_if_needed(path: Optional[Path] = None) -> Optional[str]:
    """
    If OAuth2 session tokens are stored and the access token is within 5 minutes of
    expiry, refresh it using the stored refresh token.

    Returns the new access token if refreshed, None if no refresh was needed or if
    the refresh fails (failures are non-fatal — the existing token is left in place).
    """
    target = path or resolve_profile(for_init=True).path
    if not target.is_file():
        return None

    try:
        raw = get_raw_client_config(target)
        if raw is None or not has_oauth_session(raw):
            return None
        token, _ = read_session_token(str(target))
        gms = raw.get("gms")
        return token if isinstance(gms, dict) and token != gms.get("token") else None
    except Exception as e:
        logger.debug("OAuth2 token refresh failed (non-fatal): %s", e, exc_info=True)
        return None
