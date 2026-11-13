"""Parameter types for list arguments that clients may send as a string.

MCP clients and LLMs frequently serialize a list-typed argument as a *string*: either a
JSON-encoded array (``'["a", "b"]'``) or a single bare value (``'a'``). Pydantic rejects
that against a plain ``List[str]`` before the tool body runs.

Widening the annotation to ``List[str] | str`` would accept it, but publishes
``anyOf: [array, string]``. Strict function-calling APIs reject a parameter with no
single declared type: Google Gemini (acryldata/mcp-server-datahub#98), and several
OpenAI-compatible endpoints, which reject the *whole* tool list with a 422
(acryldata/mcp-server-datahub#173). So:

- ``StrList`` (the default): coerce the string to a list in a ``BeforeValidator``. The
  schema stays a plain array and the tool body always receives a list.
- ``StrListOrStr``: only for tools whose body must see the original string (e.g.
  ``get_entities`` returns a single dict for a bare URN). The runtime type stays the
  union and ``WithJsonSchema`` publishes a plain array.
"""

from typing import Annotated, Dict, List, Optional, Tuple, TypedDict

from datahub.metadata.urns import DatasetUrn, SchemaFieldUrn, Urn
from datahub.utilities.urns.error import InvalidUrnError
from fastmcp.exceptions import ToolError
from pydantic import AliasChoices, BeforeValidator, JsonValue, WithJsonSchema
from pydantic_core import from_json


def coerce_str_list(
    value: Optional[List[str] | str],
) -> Optional[List[str]]:
    """Normalize a list argument that an MCP client may have sent as a string.

    Parsing is delegated to pydantic's own strict JSON parser: a string that parses to a
    JSON array is used as-is, anything else is treated as a single value. Item
    validation stays with the outer ``List[str]``, so this only decides list-vs-scalar.

    Malformed JSON is deliberately not repaired (unlike ``get_entities``, which uses
    ``repair_json``): most of these parameters feed write tools, where guessing at a
    broken array is riskier than treating the string as one literal value.

    ``""`` and ``"null"`` mean "no value", not a one-item list: they become ``None``, so
    an optional parameter is treated as omitted and a required one is still rejected.
    Otherwise ``column_paths="null"`` would tag a column literally named ``null``.
    """
    if not isinstance(value, str):
        return value
    stripped = value.strip()
    if not stripped:
        return None
    try:
        parsed = from_json(stripped)
    except ValueError:
        return [stripped]
    if parsed is None:
        return None
    return parsed if isinstance(parsed, list) else [stripped]


StrList = Annotated[List[str], BeforeValidator(coerce_str_list)]
"""A list of strings; a bare or JSON-stringified string is coerced to a list."""

# The optional variants put the validator *outside* the Optional, so a coerced ``None``
# (from ``""`` or ``"null"``) is accepted. ``Optional[StrList]`` would check for None
# first and then reject the coerced None as "not a valid list".
OptionalStrList = Annotated[Optional[List[str]], BeforeValidator(coerce_str_list)]
"""``StrList`` that may be omitted; ``""`` and ``"null"`` count as omitted."""

OptionalNullableItemStrList = Annotated[
    Optional[List[Optional[str]]], BeforeValidator(coerce_str_list)
]
"""Like ``OptionalStrList``, but items may be null (e.g. ``column_paths``)."""


def coerce_json_object(value: object) -> object:
    """Parse an object argument a client sent as a JSON string.

    Anything that does not parse to an object is passed on unchanged for the
    outer type to reject. ``""`` and ``"null"`` mean omitted, as for lists.
    """
    if not isinstance(value, str):
        return value
    stripped = value.strip()
    if not stripped:
        return None
    try:
        parsed = from_json(stripped)
    except ValueError:
        return value
    return parsed if parsed is None or isinstance(parsed, dict) else value


OptionalJsonObject = Annotated[
    Optional[Dict[str, JsonValue]], BeforeValidator(coerce_json_object)
]
"""A JSON object that may be omitted; a JSON-stringified object is parsed."""

StrListOrStr = Annotated[
    List[str] | str,
    WithJsonSchema({"type": "array", "items": {"type": "string"}}),
]
"""A list of strings in the schema; a single string reaches the tool body unchanged."""


def coerce_keyword_list(
    value: Optional[List[str] | str],
) -> Optional[List[str]]:
    """Like ``coerce_str_list``, and also split comma-joined keywords.

    Only for keyword arguments: a URN can contain commas.
    """
    values = coerce_str_list(value)
    if not isinstance(values, list):
        return values
    keywords: List[str] = []
    for item in values:
        if isinstance(item, str):
            keywords.extend(piece.strip() for piece in item.split(",") if piece.strip())
        else:
            # BeforeValidator sees unvalidated items; leave them for pydantic to reject.
            keywords.append(item)
    return keywords


# Other names agents use for the same parameter, for ``Field(validation_alias=...)``.
# An alias doesn't change the published schema: each parameter still advertises one
# name, so the alternatives cost no tokens and strict clients see no union. A
# canonical name missing from its own set becomes unreachable, so every name these
# are applied to appears in them.
FILTER_ALIASES = AliasChoices("filter", "filters")
URN_ALIASES = AliasChoices(
    "urn", "dataset_urn", "dataset", "datasetUrn", "entityUrn", "source_urn"
)
# Not `filter` or `query`: those are canonical on other tools for a different shape,
# and a filter expression taken as a keyword would match no column.
KEYWORDS_ALIASES = AliasChoices(
    "keywords",
    "keyword",
    "filterKeyword",
    "filter_keyword",
    "keyword_filter",
    "field_keyword",
)
URNS_ALIASES = AliasChoices("urns", "urn", "entityUrns", "dataset_urns", "dataset_urn")
FIELDS_ALIASES = AliasChoices("fields", "aspects")
UPSTREAM_ALIASES = AliasChoices("upstream", "direction")
MAX_HOPS_ALIASES = AliasChoices("max_hops", "depth")
SIZE_ALIASES = AliasChoices(
    "num_results",
    "limit",
    "count",
    "max_results",
    "numResults",
    "page_size",
    "pageSize",
)
OFFSET_ALIASES = AliasChoices("offset", "start")
# For a character cursor within one result, unlike OFFSET_ALIASES (a result index).
START_OFFSET_ALIASES = AliasChoices("start_offset", "offset", "start")
DOCUMENT_URNS_ALIASES = AliasChoices(
    "urns", "urn", "entityUrns", "documentUrns", "document_urns"
)


DATASET_URN_EXAMPLE = (
    "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)"
)
DOCUMENT_URN_EXAMPLE = "urn:li:document:01234567-89ab-cdef-0123-456789abcdef"


def coerce_lineage_direction(value: object) -> object:
    """Map a direction string onto the boolean `get_lineage.upstream` takes.

    `direction="upstream"` is the most-guessed lineage argument: the tool description
    opens with "upstream or downstream lineage" while the parameter is a bool.
    Unrecognized values pass through, so pydantic still handles real bools and its
    own "true"/"false" coercion.
    """
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in ("upstream", "upstreams"):
            return True
        if normalized in ("downstream", "downstreams"):
            return False
        if normalized in ("both", "all", "bidirectional", "either"):
            # Falling through to pydantic gives "Input should be a valid boolean"
            # against a parameter the schema does not publish, and the natural repair
            # from that message is direction=true: upstream only, half the graph.
            raise ToolError(
                f"get_lineage: direction={value!r} is not supported. `upstream` is a "
                "bool, so one call returns one direction. Call get_lineage twice: once "
                "with upstream=true, once with upstream=false."
            )
    return value


def schema_field_urn(urn: str, column: Optional[str], *, tool: str) -> str:
    """The URN of ``column`` in dataset ``urn``; ``urn`` itself when there is no column."""
    if not column:
        return urn
    parsed = Urn.from_string(urn)
    if not isinstance(parsed, DatasetUrn):
        raise ToolError(
            f"{tool}: `column` needs a dataset URN, got {urn}. Only datasets have "
            "columns; leave `column` out for any other entity."
        )
    return str(SchemaFieldUrn(parsed, column))


class SkippedUrn(TypedDict):
    """One value a batch URN parameter rejected. Reaches the caller in a tool result."""

    urn: str
    error: str


def urn_error_message(
    value: str,
    *,
    tool: str,
    param: str = "urn",
    example: str = DATASET_URN_EXAMPLE,
    resolve_with: str = "search",
) -> Optional[str]:
    """Why ``value`` is not a usable DataHub URN, or None when it is.

    An empty value, a name, and a malformed URN get different messages because the
    caller's next step differs: a name needs a lookup, so the message names the
    tool that does it (``resolve_with``); a malformed URN gets the parser's reason.
    """
    candidate = value.strip()
    if not candidate:
        return (
            f"{tool}: `{param}` is required and was empty. "
            f"Expected a DataHub URN, e.g. {example}."
        )
    try:
        Urn.from_string(candidate)
    except InvalidUrnError as e:
        if candidate.startswith("urn:li:"):
            return (
                f"{tool}: `{param}` is not a valid DataHub URN: {e}. "
                f"Expected the form {example}."
            )
        return (
            f"{tool}: `{param}` must be a DataHub URN, got {candidate!r}. "
            f"Expected the form {example}. To turn a name into a URN, call "
            f'{resolve_with}(query="{candidate}") and pass the `urn` from the result.'
        )
    return None


def require_urn(value: str, *, tool: str, param: str = "urn") -> str:
    """``value`` stripped, or a ToolError saying how to get a URN."""
    problem = urn_error_message(value, tool=tool, param=param)
    if problem is not None:
        raise ToolError(problem)
    return value.strip()


def partition_urns(
    values: List[str],
    *,
    tool: str,
    param: str = "urns",
    example: str = DATASET_URN_EXAMPLE,
    resolve_with: str = "search",
) -> Tuple[List[str], List[SkippedUrn]]:
    """Split ``values`` into usable URNs (stripped) and rejected ones.

    For a batch parameter, where one bad entry should not discard the good ones.
    """
    valid: List[str] = []
    skipped: List[SkippedUrn] = []
    for index, candidate in enumerate(values):
        problem = urn_error_message(
            candidate,
            tool=tool,
            param=f"{param}[{index}]",
            example=example,
            resolve_with=resolve_with,
        )
        if problem is None:
            valid.append(candidate.strip())
        else:
            skipped.append({"urn": candidate, "error": problem})
    return valid, skipped


def no_usable_urns_error(tool: str, skipped: List[SkippedUrn]) -> str:
    """The message for a batch where every value was rejected, naming each one."""
    return f"{tool}: none of the {len(skipped)} values are DataHub URNs.\n" + "\n".join(
        entry["error"] for entry in skipped
    )
