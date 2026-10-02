"""Access tokens for a ``datahub init --oauth`` session, refreshed as they expire."""

from __future__ import annotations

import base64
import contextlib
import json
import logging
import os
import tempfile
import time
from typing import Any, Iterator, Optional, Tuple

import requests
import yaml
from pydantic import Field

from datahub.configuration.common import ConfigModel, ConfigurationError
from datahub.emitter.token_provider import (
    DEFAULT_REFRESH_BUFFER_SECONDS,
    TokenProvider,
    TokenResult,
)

try:
    import fcntl
except ImportError:  # pragma: no cover  # Windows: refreshes are not serialized.
    fcntl = None  # type: ignore[assignment]

logger = logging.getLogger(__name__)

OAUTH_SESSION_AUTH_TYPE = "oauth_session"


def decode_jwt_exp(token: str) -> Optional[float]:
    """The exp claim of a JWT in epoch seconds, without verifying the signature."""
    try:
        payload = token.split(".")[1]
        payload += "=" * (-len(payload) % 4)
        exp = json.loads(base64.urlsafe_b64decode(payload)).get("exp")
        return float(exp) if exp else None
    except (IndexError, ValueError, TypeError):
        return None


def has_oauth_session(raw: Any) -> bool:
    """Whether a parsed credentials file holds a refreshable OAuth session."""
    oauth = raw.get("oauth") if isinstance(raw, dict) else None
    return (
        isinstance(oauth, dict)
        and bool(oauth.get("refresh_token"))
        and bool(oauth.get("client_id"))
        and bool((raw.get("gms") or {}).get("server"))
    )


@contextlib.contextmanager
def _locked(config_file: str) -> Iterator[None]:
    # The server can rotate the refresh token on each refresh (RFC 6749 §10.4).
    # Serializing refreshes, and re-reading the file inside the lock, keeps two
    # processes from presenting a refresh token the server has already replaced.
    if fcntl is None:
        yield
        return
    with open(f"{config_file}.lock", "a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(lock, fcntl.LOCK_UN)


def _refresh(raw: dict) -> bool:
    """Exchange the stored refresh token, updating ``raw`` in place on success."""
    oauth, gms = raw["oauth"], raw["gms"]
    # Prefer the token_endpoint stored from the discovery document; fall back to
    # the conventional path so configs written before this field was added work.
    endpoint = (
        oauth.get("token_endpoint") or f"{gms['server'].rstrip('/')}/auth/oauth2/token"
    )
    try:
        resp = requests.post(
            endpoint,
            data={
                "grant_type": "refresh_token",
                "refresh_token": oauth["refresh_token"],
                "client_id": oauth["client_id"],
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=5,
        )
        access_token = (
            resp.json().get("access_token") if resp.status_code == 200 else None
        )
    except (requests.RequestException, ValueError) as e:
        logger.debug("OAuth2 token refresh failed: %s", e)
        return False
    if not access_token:
        logger.debug("OAuth2 token refresh failed: HTTP %s", resp.status_code)
        return False
    gms["token"] = access_token
    # Keep the old refresh token if the server does not rotate it.
    oauth["refresh_token"] = resp.json().get("refresh_token") or oauth["refresh_token"]
    oauth.pop("token_expiry", None)  # legacy field from dev-iteration configs
    return True


def _write_atomically(config_file: str, raw: dict) -> None:
    # The server may already have rotated the refresh token, so a truncated file
    # would lose the session. Write a sibling file and rename it into place;
    # readers that skip the lock see either the old file or the new one.
    directory, name = os.path.split(os.path.abspath(config_file))
    fd, temporary = tempfile.mkstemp(dir=directory, prefix=f".{name}.tmp.")
    try:
        with os.fdopen(fd, "w") as stream:
            yaml.dump(raw, stream, default_flow_style=False)
        os.chmod(temporary, os.stat(config_file).st_mode & 0o777)
        os.replace(temporary, config_file)
    except BaseException:
        with contextlib.suppress(OSError):
            os.unlink(temporary)
        raise


def read_session_token(config_file: str) -> Tuple[str, Optional[float]]:
    """The session's access token and expiry, refreshed first if it expires soon.

    A failed refresh is not fatal: the current token is returned.
    """
    with _locked(config_file):
        with open(config_file) as stream:
            raw = yaml.safe_load(stream)
        token = (raw.get("gms") or {}).get("token")
        if not token:
            raise ConfigurationError(f"'{config_file}' has no gms.token.")
        expires_at = decode_jwt_exp(token)
        if (
            expires_at is None
            or expires_at - time.time() > DEFAULT_REFRESH_BUFFER_SECONDS
            or not has_oauth_session(raw)
            or not _refresh(raw)
        ):
            return token, expires_at
        _write_atomically(config_file, raw)
        new_token: str = raw["gms"]["token"]
        return new_token, decode_jwt_exp(new_token)


class OAuthSessionTokenProviderConfig(ConfigModel):
    config_file: str = Field(
        description="Credentials file written by `datahub init --oauth`.",
    )


class OAuthSessionTokenProvider(TokenProvider):
    """Presents a ``datahub init --oauth`` session's access token, refreshed as
    it expires. Processes that share the session file share its refreshes."""

    def __init__(self, config: OAuthSessionTokenProviderConfig) -> None:
        self._config_file = config.config_file

    def get_token(self) -> TokenResult:
        token, expires_at = read_session_token(self._config_file)
        if expires_at is not None and expires_at <= time.time():
            raise ConfigurationError(
                f"The DataHub OAuth session in '{self._config_file}' has expired. "
                "Run `datahub init --oauth` again."
            )
        return TokenResult(token, expires_at)

    @classmethod
    def create(cls, config: Optional[dict]) -> "OAuthSessionTokenProvider":
        return cls(OAuthSessionTokenProviderConfig.model_validate(config or {}))
