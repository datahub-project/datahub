from __future__ import annotations

import base64
import hashlib
import hmac
import os
import secrets
import socket
from pathlib import Path

from lib.gms_config import gms_host_from_url

SEED_PATH = Path.home() / ".datahub" / "authz-perf-results" / ".persona-password-seed"
ENV_SEED = "AUTHZ_PERF_PERSONA_PASSWORD_SEED"
_LOCAL_HOSTS = frozenset({"localhost", "127.0.0.1", "::1"})


def is_local_gms_url(gms_url: str) -> bool:
    host = gms_host_from_url(gms_url).split(":")[0].lower()
    return host in _LOCAL_HOSTS


def load_or_create_seed() -> str:
    env_seed = os.environ.get(ENV_SEED, "").strip()
    if env_seed:
        return env_seed

    SEED_PATH.parent.mkdir(parents=True, exist_ok=True)
    if SEED_PATH.exists():
        seed = SEED_PATH.read_text(encoding="utf-8").strip()
        if seed:
            return seed

    seed = secrets.token_hex(32)
    SEED_PATH.write_text(seed + "\n", encoding="utf-8")
    SEED_PATH.chmod(0o600)
    return seed


def derive_persona_password(persona: str, gms_url: str) -> str:
    """Deterministic per (local machine seed, remote host, persona); not predictable."""
    gms_host = gms_host_from_url(gms_url)
    local_host = socket.gethostname().lower()
    seed = load_or_create_seed()
    digest = hmac.new(
        seed.encode("utf-8"),
        f"{local_host}:{gms_host}:{persona}".encode("utf-8"),
        hashlib.sha256,
    ).digest()
    # DataHub requires >= 8 chars; 22 url-safe chars from 256-bit HMAC.
    return base64.urlsafe_b64encode(digest).decode("ascii").rstrip("=")[:22]


def resolve_persona_password(persona: str, gms_url: str) -> str:
    if is_local_gms_url(gms_url):
        return persona
    return derive_persona_password(persona, gms_url)
