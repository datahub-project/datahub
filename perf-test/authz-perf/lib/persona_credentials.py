from __future__ import annotations

from typing import Iterable
from urllib.parse import urlparse

import requests

from lib.graphql import log
from lib.persona_password_seed import resolve_persona_password

CREATE_RESET_TOKEN_MUTATION = """
mutation createNativeUserResetToken($input: CreateNativeUserResetTokenInput!) {
  createNativeUserResetToken(input: $input) {
    resetToken
  }
}
"""


def is_remote_gms_url(gms_url: str) -> bool:
    host = (urlparse(gms_url).hostname or "").lower()
    return host not in {"localhost", "127.0.0.1", "::1"}


def should_set_persona_passwords(
    gms_url: str,
    *,
    env_file: str | None,
    explicit: bool | None,
) -> bool:
    if explicit is False:
        return False
    if explicit is True:
        return True
    return env_file is not None or is_remote_gms_url(gms_url)


def _admin_session(admin_token: str) -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "Authorization": f"Bearer {admin_token}",
            "Content-Type": "application/json",
        }
    )
    return session


def set_persona_password(
    *,
    frontend_url: str,
    admin_token: str,
    persona: str,
    gms_url: str,
    password: str | None = None,
    timeout_sec: float = 30.0,
) -> None:
    resolved_password = (
        password if password is not None else resolve_persona_password(persona, gms_url)
    )
    user_urn = f"urn:li:corpuser:{persona}"
    base = frontend_url.rstrip("/")
    session = _admin_session(admin_token)

    graphql_resp = session.post(
        f"{base}/api/v2/graphql",
        json={
            "query": CREATE_RESET_TOKEN_MUTATION,
            "variables": {"input": {"userUrn": user_urn}},
        },
        timeout=timeout_sec,
    )
    graphql_resp.raise_for_status()
    graphql_body = graphql_resp.json()
    if graphql_body.get("errors"):
        raise RuntimeError(
            f"createNativeUserResetToken failed for {persona}: {graphql_body['errors']}"
        )
    reset_token = graphql_body["data"]["createNativeUserResetToken"]["resetToken"]
    if not reset_token:
        raise RuntimeError(f"empty reset token for {persona}")

    reset_resp = session.post(
        f"{base}/resetNativeUserCredentials",
        json={
            "email": persona,
            "password": resolved_password,
            "resetToken": reset_token,
        },
        timeout=timeout_sec,
    )
    if not reset_resp.ok:
        detail = reset_resp.text[:300] if reset_resp.text else f"HTTP {reset_resp.status_code}"
        raise RuntimeError(
            f"resetNativeUserCredentials failed for {persona}: {detail}"
        )


def ensure_persona_passwords(
    *,
    frontend_url: str,
    admin_token: str,
    gms_url: str,
    personas: Iterable[str],
) -> None:
    for persona in personas:
        log(f"setting persona password: {persona}")
        set_persona_password(
            frontend_url=frontend_url,
            admin_token=admin_token,
            persona=persona,
            gms_url=gms_url,
        )
