"""Regex search over string metadata, rendered as bounded stream excerpts.

Original string values are searched, so paths cannot create hits or change
anchors. Only a pattern that matches no value at all falls back to whole
"path: value" lines, since agents copy the rendered format into their patterns.
Excerpts come from one deterministic "path: value" stream, whose offsets also let
a continuation resume inside a match without discarding its regex prefix.

The hit count includes regex matches ending beyond the cursor and zero-width
matches at or after it, even when an earlier excerpt already covers their text.
"""

import json
from dataclasses import dataclass
from typing import Annotated, Any, Dict, Optional, TypedDict

import re2  # type: ignore[import-untyped]
from fastmcp.exceptions import ToolError
from pydantic import Field

from .. import arg_validation
from ..graphql_helpers import GREP_OUTPUT_CHAR_BUDGET
from ..read_backend import get_read_backend
from ..result_policy import (
    POLICY_DOCUMENT_SUBTYPE,
    configuration_response,
    is_configuration,
    label_reader_response,
)
from ..version_requirements import read_only
from .documents import RE2_MAX_REPEAT, clamp_repetitions, re2_error_text

_ID_KEYS = ("fieldPath", "urn", "tag", "name", "key")


def _list_item_label(item: Any) -> str | None:
    """Find a human-readable identifier for a dict inside a list."""
    if not isinstance(item, dict):
        return None
    for key in _ID_KEYS:
        val = item.get(key)
        if isinstance(val, str) and val:
            return f"{key}={val}"
    return None


def _flatten_to_stream(data: Any, path: str, entries: list[tuple[str, str]]) -> None:
    """Recursively flatten a JSON-like tree into (path, value) entries."""
    if isinstance(data, str):
        entries.append((path, data))
    elif isinstance(data, dict):
        for key in sorted(data):
            _flatten_to_stream(data[key], f"{path}.{key}", entries)
    elif isinstance(data, list):
        for i, item in enumerate(data):
            label = _list_item_label(item)
            segment = f"[{label}]" if label else f"[{i}]"
            _flatten_to_stream(item, f"{path}{segment}", entries)


@dataclass(frozen=True)
class _ValueEntry:
    path: str
    value: str
    start: int
    value_start: int


@dataclass(frozen=True)
class _Stream:
    text: str
    entries: list[_ValueEntry]


class _Snippet(TypedDict):
    path: str
    snippet: str
    position: int
    snippet_start: int


@dataclass(frozen=True)
class _Excerpt:
    path: str
    position: int
    start: int
    end: int
    size: int

    def as_snippet(self, stream: str) -> _Snippet:
        return {
            "path": self.path,
            "snippet": ("..." if self.start else "")
            + stream[self.start : self.end]
            + ("..." if self.end < len(stream) else ""),
            "position": self.position,
            "snippet_start": self.start,
        }


def _fit_excerpt(
    stream: str,
    path: str,
    position: int,
    start: int,
    end: int,
    budget: int,
    minimum_end: int,
    previous: _Excerpt | None = None,
) -> _Excerpt | None:
    """Fit an excerpt reaching minimum_end, including JSON escaping and metadata."""
    if previous is not None:
        start, position = previous.start, previous.position
        content_start = previous.end
        # Reuse the encoded prefix's size, removing its old trailing ellipsis.
        # Only new text needs encoding; render the complete snippet once at the end.
        base_size = previous.size - (3 if content_start < len(stream) else 0)
    else:
        content_start = start
        base_size = len(
            json.dumps(
                {
                    "path": path,
                    "snippet": "..." if start else "",
                    "position": position,
                    "snippet_start": start,
                }
            )
        )

    def candidate(stop: int) -> _Excerpt:
        size = (
            base_size
            + len(json.dumps(stream[content_start:stop]))
            - 2  # surrounding JSON quotes, already counted in base_size
            + (3 if stop < len(stream) else 0)
        )
        return _Excerpt(path=path, position=position, start=start, end=stop, size=size)

    # A serialized character costs at least one character. Avoid copying an
    # unbounded regex match merely to discover that it exceeds the budget.
    high = min(end, content_start + max(budget - base_size, 0))
    if high < minimum_end:
        return None
    full = candidate(high)
    if full.size <= budget:
        return full
    low = minimum_end
    best: _Excerpt | None = None
    while low <= high:
        middle = (low + high) // 2
        excerpt = candidate(middle)
        if excerpt.size <= budget:
            best = excerpt
            low = middle + 1
        else:
            high = middle - 1
    return best


def _index_stream(aspects: Dict[str, Any]) -> _Stream:
    entries: list[tuple[str, str]] = []
    for aspect_name in sorted(aspects):
        _flatten_to_stream(aspects[aspect_name], aspect_name, entries)
    lines: list[str] = []
    indexed: list[_ValueEntry] = []
    offset = 0
    for path, value in entries:
        line = f"{path}: {value}"
        indexed.append(
            _ValueEntry(
                path=path, value=value, start=offset, value_start=offset + len(path) + 2
            )
        )
        lines.append(line)
        offset += len(line) + 1  # the "\n" separator
    return _Stream(text="\n".join(lines), entries=indexed)


def _build_stream(aspects: Dict[str, Any]) -> str:
    """Flatten all aspects into one searchable text stream."""
    return _index_stream(aspects).text


@read_only
def grep_entities(
    urn: Annotated[str, Field(validation_alias=arg_validation.URN_ALIASES)],
    pattern: Annotated[
        str,
        Field(
            description="RE2 regex, case-sensitive by default. Use (?i) to ignore case "
            "and | for alternatives, e.g. '(?i)email|phone'."
        ),
    ],
    context_chars: Annotated[
        int,
        Field(
            ge=0,
            le=2000,
            description="Characters of context on each side of a match in snippets.",
        ),
    ] = 120,
    start_offset: Annotated[
        int,
        Field(
            ge=0,
            validation_alias=arg_validation.START_OFFSET_ALIASES,
            description="Character offset to start searching from (default: 0). "
            "Pass the next_offset value from a previous truncated response "
            "to continue where it left off.",
        ),
    ] = 0,
    max_matches: Annotated[
        int, Field(ge=1, description="Maximum snippets per response (default: 20).")
    ] = 20,
) -> Dict[str, Any]:
    """Search string metadata across all aspects of one DataHub entity using RE2 regex.

    Each string value is searched separately; anchors apply within each value,
    and patterns cannot span values. If the pattern matches no value at all, each
    entry is searched as its whole "path: value" line instead and the response
    has searched: "lines". There ^ anchors at the start of the path, $ at the end
    of the value, and patterns cannot span entries. If any value matches, paths
    are not searched; search a path term in a separate call.

    Returns matches with path, snippet, position, and snippet_start, plus
    matchCount and streamLength. Snippets come from a deterministic "path: value"
    stream and can include neighboring entries. path identifies the matching
    value. position is the first hit's original stream offset, which can precede
    the cursor on continuation; snippet_start is the displayed text's offset,
    excluding an added leading ellipsis. matchCount counts remaining regex hits,
    including zero-width hits, and can exceed the snippet count.

    Responses contain at most max_matches snippets (default: 20) and 60K JSON
    characters. Consecutive hits within one value merge when separated only by
    line breaks or nothing. A whole-line pattern matching consecutive lines can
    therefore fill the output budget in one snippet.

    When next_offset is present, pass it as start_offset with the same urn,
    pattern, and context_chars to continue; stop when next_offset is absent.
    Use .* and follow these cursors to read the complete string stream. For many
    short fields, raise max_matches (e.g. 1000) to read more fields per page.
    """
    urn = arg_validation.require_urn(urn, tool="grep_entities")

    if is_configuration(urn):
        return configuration_response(urn)

    if not pattern or not pattern.strip():
        return label_reader_response(
            {"urn": urn, "pattern": pattern, "matches": [], "matchCount": 0}, urn
        )

    pattern_note: Optional[str] = None
    try:
        regex = re2.compile(pattern)
    except re2.error as e:
        clamped = clamp_repetitions(pattern)
        if clamped == pattern:
            raise ToolError(
                f"Invalid regex pattern: {re2_error_text(e)}. Use RE2 syntax."
            ) from e
        try:
            regex = re2.compile(clamped)
        except re2.error as e2:
            raise ToolError(
                f"Invalid regex pattern: {re2_error_text(e2)}. Use RE2 syntax."
            ) from e2
        pattern_note = (
            f"A repetition exceeded RE2's max ({RE2_MAX_REPEAT}); "
            f"the pattern was clamped. Use context_chars for large windows."
        )

    backend = get_read_backend()

    # aspects() answers existence itself (None), inside the same error handling as
    # the read.
    try:
        aspects = backend.aspects(urn)
    except (OSError, ConnectionError, TimeoutError) as e:
        raise ToolError("Failed to fetch entity metadata. Retry the request.") from e

    if aspects is None:
        raise ToolError(f"Entity {urn} not found")
    if not aspects:
        return label_reader_response(
            {
                "urn": urn,
                "pattern": pattern,
                "matches": [],
                "matchCount": 0,
                "streamLength": 0,
                "note": "Entity exists but has no aspects.",
            },
            urn,
        )

    subtypes = (aspects.get("subTypes") or {}).get("typeNames")
    if isinstance(subtypes, list) and POLICY_DOCUMENT_SUBTYPE in subtypes:
        return configuration_response(urn)

    reader = label_reader_response({"urn": urn}, urn, aspects=aspects)
    if reader.get("withheld"):
        return reader

    indexed = _index_stream(aspects)
    stream = indexed.text
    stream_length = len(stream)

    excerpts: list[_Excerpt] = []
    out: Dict[str, Any] = {
        "urn": urn,
        "pattern": pattern,
        "matches": [],
        "matchCount": 0,
        "streamLength": stream_length,
        **reader,
    }
    if pattern_note:
        out["pattern_note"] = pattern_note
    if start_offset >= stream_length:
        return out

    # Decided over the whole stream, ignoring the cursor, so every page of one
    # search reads the same text.
    search_lines = not any(
        regex.search(entry.value) is not None for entry in indexed.entries
    )
    if search_lines:
        out["searched"] = "lines"

    # Reserve the envelope and worst-case counter/cursor widths before charging
    # each serialized match. RE2 can also yield zero-width hits at boundaries.
    reserved = dict(
        out,
        matchCount=2 * (stream_length + len(indexed.entries) + 1),
        next_offset=stream_length,
    )
    budget = GREP_OUTPUT_CHAR_BUDGET - len(json.dumps(reserved))
    if budget <= 0:
        raise ToolError(
            "The pattern and entity metadata exceed the output budget. Shorten the pattern."
        )
    total_hits = 0
    shown_until = start_offset
    unshown = False

    for entry in indexed.entries:
        previous_excerpt: _Excerpt | None = None
        previous_match_end: int | None = None
        if search_lines:
            text = stream[entry.start : entry.value_start + len(entry.value)]
            base = entry.start
        else:
            text, base = entry.value, entry.value_start
        # Search the entire original text, including on continuation. Slicing
        # at the cursor would lose clipped literals and change ^ / word boundaries.
        for match in regex.finditer(text):
            gap_start = previous_match_end
            previous_match_end = match.end()
            position = base + match.start()
            match_end = base + match.end()
            if match_end < start_offset or (
                match_end == start_offset and match.start() != match.end()
            ):
                continue
            total_hits += 1
            if match_end <= shown_until and (excerpts or position < start_offset):
                continue
            if unshown:
                continue

            start = max(0, position - context_chars)
            if match.start() == 0:
                # Include the prefix and preceding separator so .* can read the
                # complete stream even with long paths or zero context.
                start = min(start, max(0, entry.start - 1))
            continues_shown_text = not stream[shown_until:position].strip("\n")
            if continues_shown_text:
                start = shown_until
            start = max(start, shown_until)
            end = min(stream_length, match_end + context_chars)
            if end <= start:
                continue

            # Compare regex boundaries, not context boundaries: regularly spaced
            # hits can otherwise align with trailing context and chain indefinitely.
            merge = (
                previous_excerpt is not None
                and gap_start is not None
                and not text[gap_start : match.start()].strip("\n")
            )
            if not merge and len(excerpts) >= max_matches:
                unshown = True
                continue
            separator_size = 2 if excerpts and not merge else 0
            available = budget - separator_size
            if merge and previous_excerpt is not None:
                available += previous_excerpt.size

            # Preserve leading context. If the remainder cannot reach matched
            # text, leave this hit for a fresh page instead of emitting context.
            minimum_end = max(shown_until + 1, position + (match_end > position))
            excerpt = _fit_excerpt(
                stream,
                entry.path,
                position,
                start,
                end,
                available,
                minimum_end,
                previous=previous_excerpt if merge else None,
            )
            if excerpt is None:
                unshown = True
                continue
            if merge and previous_excerpt is not None:
                excerpts[-1] = excerpt
                budget += previous_excerpt.size
            else:
                excerpts.append(excerpt)
            budget -= excerpt.size + separator_size
            previous_excerpt = excerpt
            shown_until = excerpt.end
            if shown_until < match_end:
                unshown = True

    out["matchCount"] = total_hits
    out["matches"] = [excerpt.as_snippet(stream) for excerpt in excerpts]
    if unshown:
        if not excerpts:
            raise ToolError(
                "A match's path, leading context, and response metadata exceed the output budget. "
                "Reduce context_chars or narrow the pattern."
            )
        out["next_offset"] = shown_until
    return out
