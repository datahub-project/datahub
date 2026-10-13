"""Fabric notebook listing and fabricGitSource definition parsing.

Notebooks are ingested as datasets with subtype Notebook, matching the
Databricks Unity Catalog notebook emission. The path used by
``notebook_pattern`` is the workspace folder path plus the display name
(``/<folder>/.../<name>``), because the list API returns a folder id
rather than a path string.
"""

import base64
import re
from dataclasses import dataclass
from enum import Enum
from typing import Dict, List, Optional, Tuple

# fabricGitSource content files, by language. The .platform part is metadata
# and is not the notebook source.
_LANGUAGE_BY_SUFFIX = {
    ".py": "python",
    ".sql": "sql",
    ".scala": "scala",
    ".r": "r",
}

# A section delimiter in an exported fabricGitSource file, e.g.
# "# CELL ********************". The keyword decides the cell kind; the
# "# META {...}" lines inside a METADATA block are not delimiters (no word
# boundary after "META" in "METADATA"), so they stay inside the skipped block.
# "# PARAMETERS CELL" is a parameterized code cell (pipeline inputs) and is
# matched first so its content is not swallowed by the preceding block.
_SECTION_RE = re.compile(
    r"^#\s*(PARAMETERS\s+CELL|CELL|MARKDOWN|METADATA)\b.*$", re.MULTILINE
)
# The "# MAGIC " line prefix some exports put on markdown and magic cells.
_MAGIC_PREFIX_RE = re.compile(r"^#\s?MAGIC\s?", re.MULTILINE)
# A leading cell magic that only *selects the cell language* (%%sql, %md, ...).
# Restricted to known language/display selectors so executable magics the user
# wrote - %pip, %run, %%configure, %sh, ... - are kept in the view definition.
# Only the selector token is dropped: inline content on the same line (e.g.
# "%md # Heading") is kept, and a selector that is alone on its line takes the
# newline with it so the cell body is not left with a blank first line.
_LEADING_MAGIC_RE = re.compile(
    r"\A\s*%{1,2}(?:markdown|md|sparksql|sparkr|pyspark|spark|scala|sql|html)\b"
    r"[ \t]*(?:\r?\n|(?=\S)|\Z)",
    re.IGNORECASE,
)


class _CellKind(str, Enum):
    CODE = "code"
    MARKDOWN = "markdown"


@dataclass
class ParsedCell:
    kind: _CellKind
    source: str


@dataclass
class FabricNotebook:
    """A notebook item from GET /workspaces/{workspaceId}/notebooks."""

    id: str
    display_name: str
    workspace_id: str
    description: Optional[str] = None
    folder_id: Optional[str] = None
    owner_user_principal_name: Optional[str] = None
    sensitivity_label_id: Optional[str] = None


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


def _clean_cell_block(block: str) -> str:
    text = _MAGIC_PREFIX_RE.sub("", block)
    # Drop the language-selector magic line so a %%sql cell is the SQL itself
    # and a %md cell is the markdown itself.
    text = _LEADING_MAGIC_RE.sub("", text, count=1)
    return text.strip()


def parse_notebook_cells(content: Optional[str]) -> List[ParsedCell]:
    """Split a decoded notebook source file into ordered cells.

    The ``# METADATA`` blocks (the ``# META {...}`` kernel/cell JSON Fabric
    writes) are dropped: they are scaffolding, not user content. A source file
    with no cell delimiters (a bare ``.sql``/``.py`` export) is returned as a
    single code cell.
    """
    if not content or not content.strip():
        return []

    matches = list(_SECTION_RE.finditer(content))
    if not matches:
        return [ParsedCell(_CellKind.CODE, content.strip())]

    cells: List[ParsedCell] = []
    for i, match in enumerate(matches):
        keyword = match.group(1).upper()
        if keyword == "METADATA":
            continue
        start = match.end()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(content)
        kind = _CellKind.MARKDOWN if keyword == "MARKDOWN" else _CellKind.CODE
        source = _clean_cell_block(content[start:end])
        if source:
            cells.append(ParsedCell(kind, source))
    return cells


def _comment_prefix(language: Optional[str]) -> str:
    return "--" if (language or "").lower() == "sql" else "#"


def notebook_view_logic(
    content: Optional[str], language: Optional[str]
) -> Optional[str]:
    """Build the dataset view definition from a notebook's cells.

    Returns the code cells joined with a per-cell delimiter, keeping the user's
    own in-code comments but dropping all Fabric scaffolding (the ``# META``
    metadata and ``# CELL``/``# MARKDOWN`` markers). Markdown cells are kept as
    a labelled comment block so no user-authored content is lost. Returns None
    when the notebook has no usable source, so the caller can skip the View
    subtype.
    """
    cells = parse_notebook_cells(content)
    if not cells:
        return None

    # A bare single-cell export carries no cell structure worth annotating.
    if len(cells) == 1 and cells[0].kind is _CellKind.CODE:
        return cells[0].source

    prefix = _comment_prefix(language)
    blocks: List[str] = []
    code_index = 0
    for cell in cells:
        if cell.kind is _CellKind.MARKDOWN:
            body = "\n".join(
                f"{prefix} {line}".rstrip() for line in cell.source.splitlines()
            )
            blocks.append(f"{prefix} \u2500\u2500 Markdown \u2500\u2500\n{body}")
        else:
            code_index += 1
            blocks.append(
                f"{prefix} \u2500\u2500 Cell {code_index} \u2500\u2500\n{cell.source}"
            )
    return "\n\n".join(blocks) or None
