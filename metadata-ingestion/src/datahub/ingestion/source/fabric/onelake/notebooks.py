"""Fabric notebook listing and fabricGitSource definition parsing.

Notebooks are ingested as datasets with subtype Notebook, matching the
Databricks Unity Catalog notebook emission. The path used by
``notebook_pattern`` is the workspace folder path plus the display name
(``/<folder>/.../<name>``), because the list API returns a folder id
rather than a path string.
"""

import base64
from dataclasses import dataclass
from typing import Dict, Optional, Tuple

# fabricGitSource content files, by language. The .platform part is metadata
# and is not the notebook source.
_LANGUAGE_BY_SUFFIX = {
    ".py": "python",
    ".sql": "sql",
    ".scala": "scala",
    ".r": "r",
}


@dataclass
class FabricNotebook:
    """A notebook item from GET /workspaces/{workspaceId}/notebooks."""

    id: str
    display_name: str
    workspace_id: str
    description: Optional[str] = None
    folder_id: Optional[str] = None


def notebook_path(
    display_name: str,
    folder_id: Optional[str],
    folders: Dict[str, Tuple[str, Optional[str]]],
) -> str:
    """Build ``/<folder>/.../<name>`` from a folder id and display name.

    ``folders`` maps folder id to ``(display_name, parent_folder_id)``.
    A missing folder id, or a folder the listing did not return, stops the
    walk so the path is still usable for ``notebook_pattern``.
    """
    parts = [display_name]
    seen: set[str] = set()
    current = folder_id
    while current and current not in seen:
        seen.add(current)
        folder = folders.get(current)
        if folder is None:
            break
        name, parent = folder
        if name:
            parts.append(name)
        current = parent
    parts.reverse()
    return "/" + "/".join(parts)


def language_from_part_path(path: str) -> Optional[str]:
    """Language for a fabricGitSource content part, from its file suffix."""
    lowered = path.lower()
    for suffix, language in _LANGUAGE_BY_SUFFIX.items():
        if lowered.endswith(suffix):
            return language
    return None


def decode_notebook_definition(
    payload: dict,
) -> Tuple[Optional[str], Optional[str]]:
    """Decode the fabricGitSource content part.

    Returns ``(source_text, language)``. The ``.platform`` part is skipped.
    ``language`` is None when the content file suffix is not a known notebook
    language.
    """
    definition = payload.get("definition") if isinstance(payload, dict) else None
    if not isinstance(definition, dict):
        definition = payload if isinstance(payload, dict) else {}
    parts = definition.get("parts") or []
    for part in parts:
        if not isinstance(part, dict):
            continue
        path = part.get("path") or ""
        if path == ".platform" or path.endswith("/.platform"):
            continue
        encoded = part.get("payload")
        if not encoded:
            continue
        source = base64.b64decode(encoded).decode("utf-8", errors="replace")
        return source, language_from_part_path(path)
    return None, None
