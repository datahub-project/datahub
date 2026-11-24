"""Dependency-free profile storage primitives, also used by the dev launcher."""

import os
import re
import tempfile
from pathlib import Path


def profile_path_in(home: Path, name: str) -> Path:
    if not re.fullmatch(r"[a-z0-9][a-z0-9._-]{0,62}", name):
        raise ValueError("Invalid profile name")
    if name == "default":
        return home / ".datahubenv"
    return home / ".datahub" / "profiles" / name / ".datahubenv"


def ensure_private_directory(path: Path) -> None:
    if not path.exists():
        ensure_private_directory(path.parent)
        path.mkdir(mode=0o700, exist_ok=True)


def write_profile_text(target: Path, content: str) -> None:
    # Preserve dotfile symlinks, and set permissions before publishing credentials.
    target = Path(os.path.realpath(target))
    ensure_private_directory(target.parent)
    fd, temporary = tempfile.mkstemp(dir=target.parent, prefix=".datahubenv.")
    try:
        with os.fdopen(fd, "w") as outfile:
            outfile.write(content)
        os.chmod(temporary, 0o600)
        os.replace(temporary, target)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)
