from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from typing import Optional

import click
from datahub.cli.cli_utils import (
    fixup_gms_url,
    generate_access_token,
    guess_frontend_url_from_gms_url,
)
from datahub.cli.config_utils import (
    DATAHUB_CONFIG_PATH,
    DatahubConfig,
    MissingConfigError,
    get_raw_client_config,
    load_client_config,
)

try:
    from datahub.cli.config_utils import refresh_oauth_token_if_needed
except ImportError:

    def refresh_oauth_token_if_needed() -> Optional[str]:  # type: ignore[misc]
        return None


@dataclass(frozen=True)
class AuthzPerfCredentials:
    gms_url: str
    frontend_url: str
    token: str


def _resolve_config_value(
    *,
    arg_value: Optional[str],
    env_var: str,
    prompt_text: str,
    hide_input: bool = False,
    allow_prompt: bool,
) -> str:
    if arg_value is not None:
        return arg_value
    env_value = os.environ.get(env_var)
    if env_value:
        return env_value
    if not allow_prompt:
        raise SystemExit(
            f"Missing {env_var} and no interactive terminal for {prompt_text.lower()}."
        )
    return click.prompt(
        text=prompt_text,
        type=str,
        hide_input=hide_input,
    )


def _load_token_from_datahubenv_file() -> tuple[Optional[str], Optional[str]]:
    if not os.path.isfile(DATAHUB_CONFIG_PATH):
        return None, None
    try:
        raw = get_raw_client_config()
        if not raw:
            return None, None
        gms = DatahubConfig.model_validate(raw).gms
        token = gms.token
        refreshed = refresh_oauth_token_if_needed()
        if refreshed is not None:
            token = refreshed
        if token and not token.strip():
            token = None
        return gms.server, token
    except Exception:
        return None, None


def _load_datahub_config() -> tuple[Optional[str], Optional[str]]:
    file_url, file_token = _load_token_from_datahubenv_file()
    try:
        cfg = load_client_config()
    except MissingConfigError:
        return file_url, file_token

    # load_client_config returns env URL without reading ~/.datahubenv when
    # DATAHUB_GMS_URL is set but DATAHUB_GMS_TOKEN is not.
    url = cfg.server or file_url
    token = cfg.token or file_token
    return url, token


def _resolve_gms_url(gms_url: Optional[str]) -> str:
    config_url, _ = _load_datahub_config()
    return fixup_gms_url(
        gms_url
        or os.environ.get("DATAHUB_GMS_URL")
        or config_url
        or "http://localhost:8080"
    )


def _resolve_token_from_login(
    *,
    gms_url: str,
    username: Optional[str],
    password: Optional[str],
    allow_prompt: bool,
) -> str:
    if not allow_prompt and username is None and password is None:
        env_user = os.environ.get("DATAHUB_USERNAME")
        env_pass = os.environ.get("DATAHUB_PASSWORD")
        if not env_user or not env_pass:
            raise SystemExit(
                "No GMS token found in ~/.datahubenv or DATAHUB_GMS_TOKEN. "
                "Run `datahub init`, pass --username/--password, or set "
                "DATAHUB_USERNAME/DATAHUB_PASSWORD."
            )

    resolved_username = _resolve_config_value(
        arg_value=username,
        env_var="DATAHUB_USERNAME",
        prompt_text="Enter your DataHub username",
        allow_prompt=allow_prompt,
    )
    resolved_password = _resolve_config_value(
        arg_value=password,
        env_var="DATAHUB_PASSWORD",
        prompt_text="Enter your DataHub password",
        hide_input=True,
        allow_prompt=allow_prompt,
    )
    _, token = generate_access_token(
        username=resolved_username,
        password=resolved_password,
        gms_url=gms_url,
    )
    return token


def resolve_credentials(
    *,
    gms_url: Optional[str] = None,
    frontend_url: Optional[str] = None,
    username: Optional[str] = None,
    password: Optional[str] = None,
    token: Optional[str] = None,
    allow_prompt: bool = True,
) -> AuthzPerfCredentials:
    """Resolve coordinator credentials for setup (datapack probe/load).

    Precedence for GMS URL: CLI ``--gms-url`` > ``DATAHUB_GMS_URL`` > ``~/.datahubenv`` >
    ``http://localhost:8080``.

    Precedence for auth token: ``~/.datahubenv`` / ``DATAHUB_GMS_TOKEN`` >
    mint via username/password (CLI flags, env vars, or interactive prompt).
    """
    resolved_gms = _resolve_gms_url(gms_url)
    resolved_frontend = frontend_url or guess_frontend_url_from_gms_url(resolved_gms)

    if token and token.strip():
        return AuthzPerfCredentials(
            gms_url=resolved_gms,
            frontend_url=resolved_frontend,
            token=token,
        )

    _, config_token = _load_datahub_config()
    token = config_token or os.environ.get("DATAHUB_GMS_TOKEN")
    if token and token.strip():
        return AuthzPerfCredentials(
            gms_url=resolved_gms,
            frontend_url=resolved_frontend,
            token=token,
        )

    minted = _resolve_token_from_login(
        gms_url=resolved_gms,
        username=username,
        password=password,
        allow_prompt=allow_prompt,
    )
    return AuthzPerfCredentials(
        gms_url=resolved_gms,
        frontend_url=resolved_frontend,
        token=minted,
    )


def credentials_allow_prompt() -> bool:
    return sys.stdin.isatty()
