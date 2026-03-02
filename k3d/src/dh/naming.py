"""Worktree name derivation and naming helpers — replaces common.sh naming functions."""

from __future__ import annotations

import hashlib
import os
import re

from dh.utils import die, run


def git_root() -> str:
    """Return the absolute path of the current git worktree root."""
    result = run(
        ["git", "rev-parse", "--show-toplevel"], capture_output=True, check=False
    )
    if result.returncode != 0:
        die("Not inside a git repository.")
    return result.stdout.strip()


def derive_worktree_name() -> str:
    """Derive a short, unique name from the current git worktree directory.

    Priority:
      1. $DH_WORKTREE_NAME env var (explicit override)
      2. basename of git repo root, with "datahub." / "datahub-" prefix stripped
    """
    env_name = os.environ.get("DH_WORKTREE_NAME", "")
    if env_name:
        return _sanitize_name(env_name)

    root = git_root()
    base = os.path.basename(root)

    # Strip common prefixes
    for prefix in ("datahub.", "datahub-"):
        if base.startswith(prefix):
            base = base[len(prefix) :]
            break

    if base == "datahub":
        base = "main"

    return _sanitize_name(base)


def _sanitize_name(name: str) -> str:
    """Lowercase, replace non-alphanum with hyphens, trim, truncate to 20 chars."""
    name = name.lower()
    name = re.sub(r"[^a-z0-9-]", "-", name)
    name = re.sub(r"-+", "-", name).strip("-")
    if len(name) > 20:
        h = hashlib.md5(name.encode()).hexdigest()[:4]
        name = f"{name[:15]}-{h}"
    return name


def namespace_for(name: str) -> str:
    return f"dh-{name}"


def db_name_for(name: str) -> str:
    return f"datahub_{name.replace('-', '_')}"


def es_prefix_for(name: str) -> str:
    return name.replace("-", "_")


def kafka_prefix_for(name: str) -> str:
    return f"{name.replace('-', '_')}_"


def hostname_for(name: str) -> str:
    return f"{name}.datahub.localhost"


def gms_hostname_for(name: str) -> str:
    return f"gms.{name}.datahub.localhost"
