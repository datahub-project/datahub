import json
import logging
import re
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from datahub.ingestion.source.powerbi.rest_api_wrapper.fabric_resolver import (
    DefinitionPart,
)

logger = logging.getLogger(__name__)

# Parts of a Dataflow Gen2 definition (format-agnostic getDefinition).
MASHUP_PART_SUFFIX = "mashup.pq"
QUERY_METADATA_PART_SUFFIX = "querymetadata.json"

KEY_QUERIES_METADATA = "queriesMetadata"
KEY_LOAD_ENABLED = "loadEnabled"

SECTION_KEYWORD = "section"
SHARED_KEYWORD = "shared"

STRING_QUOTE = '"'
QUOTED_IDENTIFIER_PREFIX = '#"'
STATEMENT_TERMINATOR = ";"
LINE_COMMENT = "//"
BLOCK_COMMENT_START = "/*"
BLOCK_COMMENT_END = "*/"
ATTRIBUTE_OPEN = "["
ATTRIBUTE_CLOSE = "]"
ASSIGNMENT = "="

_PLAIN_IDENTIFIER = re.compile(r"[A-Za-z_][\w.]*")
_QUOTED_IDENTIFIER = re.compile(r'#"((?:[^"]|"")*)"')


@dataclass
class ParsedDataflowQuery:
    name: str
    expression: str
    is_shared: bool
    # From queryMetadata.json; None when the query has no metadata entry.
    load_enabled: Optional[bool] = None

    @property
    def is_entity(self) -> bool:
        """A query that lands in the dataflow's storage, i.e. a table a
        consumer can read through ``PowerPlatform.Dataflows``. The metadata
        flag is authoritative; without it ``shared`` is the best signal."""
        if self.load_enabled is not None:
            return self.load_enabled
        return self.is_shared


@dataclass
class ParsedDataflow:
    queries: List[ParsedDataflowQuery] = field(default_factory=list)
    has_mashup: bool = False
    # Paths of parts that were present but could not be parsed.
    failed_parts: List[str] = field(default_factory=list)


def parse_dataflow_definition(parts: List[DefinitionPart]) -> ParsedDataflow:
    parsed = ParsedDataflow()
    load_flags: Dict[str, bool] = {}
    for part in parts:
        lowered = part.path.lower()
        if lowered.endswith(MASHUP_PART_SUFFIX):
            try:
                parsed.queries = parse_section_document(part.text)
                parsed.has_mashup = True
            except ValueError as e:
                logger.debug("Unparseable mashup part %s: %s", part.path, e)
                parsed.failed_parts.append(part.path)
        elif lowered.endswith(QUERY_METADATA_PART_SUFFIX):
            try:
                load_flags = _load_flags_from_query_metadata(part.text)
            except (json.JSONDecodeError, TypeError, AttributeError) as e:
                logger.debug("Unparseable query metadata %s: %s", part.path, e)
                parsed.failed_parts.append(part.path)
    for query in parsed.queries:
        query.load_enabled = load_flags.get(query.name)
    return parsed


def _load_flags_from_query_metadata(text: str) -> Dict[str, bool]:
    body = json.loads(text)
    queries = body.get(KEY_QUERIES_METADATA) or {}
    flags: Dict[str, bool] = {}
    for name, metadata in queries.items():
        if isinstance(metadata, dict) and isinstance(
            metadata.get(KEY_LOAD_ENABLED), bool
        ):
            flags[name] = metadata[KEY_LOAD_ENABLED]
    return flags


def parse_section_document(text: str) -> List[ParsedDataflowQuery]:
    """Split a Power Query section document (``section Section1; shared A =
    ...; B = ...;``) into its member queries.

    Statements are separated by top-level ``;``; semicolons inside string
    literals, quoted identifiers and comments are not separators. A member
    may carry an attribute record (``[StagingDefinition = ...]``) before the
    optional ``shared`` keyword.
    """
    queries: List[ParsedDataflowQuery] = []
    for statement in _split_statements(text):
        stripped = statement.strip()
        if not stripped or _starts_with_keyword(stripped, SECTION_KEYWORD):
            continue
        member = _parse_member(stripped)
        if member is None:
            raise ValueError(f"Unrecognized section member: {stripped[:80]!r}")
        queries.append(member)
    return queries


def _starts_with_keyword(text: str, keyword: str) -> bool:
    return text.startswith(keyword) and (
        len(text) == len(keyword) or text[len(keyword)].isspace()
    )


def _skip_string(text: str, index: int) -> int:
    """``index`` points at the opening quote; returns the index just past the
    closing quote. A doubled quote is an escaped quote in M."""
    index += 1
    while index < len(text):
        if text[index] == STRING_QUOTE:
            if index + 1 < len(text) and text[index + 1] == STRING_QUOTE:
                index += 2
                continue
            return index + 1
        index += 1
    raise ValueError("Unterminated string literal")


def _split_statements(text: str) -> List[str]:
    """Top-level statements of the document, with comments removed."""
    statements: List[str] = []
    current: List[str] = []
    index = 0
    length = len(text)
    while index < length:
        char = text[index]
        if char == STRING_QUOTE:
            end = _skip_string(text, index)
            current.append(text[index:end])
            index = end
            continue
        if text.startswith(LINE_COMMENT, index):
            newline = text.find("\n", index)
            index = length if newline < 0 else newline
            continue
        if text.startswith(BLOCK_COMMENT_START, index):
            end = text.find(BLOCK_COMMENT_END, index + len(BLOCK_COMMENT_START))
            if end < 0:
                raise ValueError("Unterminated block comment")
            index = end + len(BLOCK_COMMENT_END)
            continue
        if char == STATEMENT_TERMINATOR:
            statements.append("".join(current))
            current = []
        else:
            current.append(char)
        index += 1
    tail = "".join(current)
    if tail.strip():
        statements.append(tail)
    return statements


def _skip_attribute_record(text: str, index: int) -> int:
    """``index`` points at ``[``; returns the index just past the matching
    ``]``, honouring nested records and strings."""
    depth = 0
    while index < len(text):
        char = text[index]
        if char == STRING_QUOTE:
            index = _skip_string(text, index)
            continue
        if char == ATTRIBUTE_OPEN:
            depth += 1
        elif char == ATTRIBUTE_CLOSE:
            depth -= 1
            if depth == 0:
                return index + 1
        index += 1
    raise ValueError("Unterminated attribute record")


def _parse_member(statement: str) -> Optional[ParsedDataflowQuery]:
    index = 0
    if statement.startswith(ATTRIBUTE_OPEN):
        index = _skip_attribute_record(statement, 0)
    rest = statement[index:].lstrip()
    is_shared = _starts_with_keyword(rest, SHARED_KEYWORD)
    if is_shared:
        rest = rest[len(SHARED_KEYWORD) :].lstrip()

    if rest.startswith(QUOTED_IDENTIFIER_PREFIX):
        match = _QUOTED_IDENTIFIER.match(rest)
        if match is None:
            return None
        name = match.group(1).replace(STRING_QUOTE * 2, STRING_QUOTE)
    else:
        match = _PLAIN_IDENTIFIER.match(rest)
        if match is None:
            return None
        name = match.group(0)
    rest = rest[match.end() :].lstrip()
    if not rest.startswith(ASSIGNMENT):
        return None
    expression = rest[len(ASSIGNMENT) :].strip()
    if not expression:
        return None
    return ParsedDataflowQuery(name=name, expression=expression, is_shared=is_shared)


def quote_identifier(name: str) -> str:
    return f'{QUOTED_IDENTIFIER_PREFIX}{name.replace(STRING_QUOTE, STRING_QUOTE * 2)}"'


def compose_let_expression(bindings: Dict[str, str], target: str) -> str:
    """Wrap the section's queries (name -> expression, ``target`` among them)
    in one ``let`` whose output is ``target``, so a reference from one query
    to another resolves through the normal let-scope lookup of the M
    resolver."""
    lines = [
        f"    {quote_identifier(name)} = {expression}"
        for name, expression in bindings.items()
    ]
    return "let\n" + ",\n".join(lines) + "\nin\n    " + quote_identifier(target)
