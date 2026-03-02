"""Template expansion using string.Template — replaces envsubst."""

from __future__ import annotations

from pathlib import Path
from string import Template


def expand_template(template_path: Path, variables: dict[str, str]) -> str:
    """Read a template file and substitute ${VAR} placeholders."""
    return Template(template_path.read_text()).substitute(variables)
