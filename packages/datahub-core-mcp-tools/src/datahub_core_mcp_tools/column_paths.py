"""The fieldPath a column-level write sends, from the column a caller names.

GMS accepts a column-level write (tags, glossary terms, a description) only when
its subResource equals a fieldPath of the dataset's own schema exactly. Many
ingested schemas use v2 field paths such as ``[version=2.0].[type=string].email``,
so the column name a caller knows is not the path GMS wants. Every column-level
write resolves the name it is given here before writing.
"""

from typing import List, Mapping

from datahub.utilities.urns.field_paths import get_simple_field_path_from_v2_field_path
from fastmcp.exceptions import ToolError

from .read_backend import get_read_backend

# Suggested near matches named in a not-found error.
_MAX_SUGGESTIONS = 5


def schema_field_paths(aspects: Mapping[str, object]) -> List[str]:
    """The fieldPaths of the ``schemaMetadata`` in ``aspects``, which may be raw
    aspects or a GraphQL entity selecting ``schemaMetadata { fields { fieldPath } }``.
    """
    schema = aspects.get("schemaMetadata")
    fields = schema.get("fields") if isinstance(schema, dict) else None
    paths = [
        path
        for field in (fields if isinstance(fields, list) else [])
        if isinstance(field, dict) and isinstance(path := field.get("fieldPath"), str)
    ]
    return list(dict.fromkeys(paths))


def read_schema_field_paths(urn: str, *, tool: str) -> List[str]:
    """The fieldPaths of the schema GMS checks a column-level write against: the
    dataset's own current ``schemaMetadata``, not a sibling's."""
    try:
        aspects = get_read_backend().aspects(urn)
    except Exception as e:
        raise ToolError(
            f"{tool}: could not read the columns of {urn}, so nothing was written: {e}"
        ) from e
    if aspects is None:
        raise ToolError(f"{tool}: {urn} does not exist in DataHub.")
    return schema_field_paths(aspects)


def _simple(path: str) -> str:
    return get_simple_field_path_from_v2_field_path(path)


def resolve_column_path(
    field_paths: List[str], column: str, *, urn: str, tool: str
) -> str:
    """The fieldPath among ``field_paths`` that ``column`` names.

    ``column`` is an exact fieldPath, or a column name: the path with its v2
    ``[...]`` tokens dropped (``email``, or ``address.city`` for a nested field),
    compared exactly and then ignoring case, as get_entities matches column names.
    A name that fits several columns (e.g. the branches of a v2 union) is refused
    rather than guessed.
    """
    if column in field_paths:
        return column
    if not field_paths:
        raise ToolError(
            f"{tool}: column {column!r} is not in the schema of {urn}: the dataset "
            "has no schema fields."
        )
    name = _simple(column)
    simple_paths = {path: _simple(path) for path in field_paths}
    matches = [path for path, simple in simple_paths.items() if simple == name] or [
        path for path, simple in simple_paths.items() if simple.lower() == name.lower()
    ]
    if len(matches) == 1:
        return matches[0]
    if matches:
        raise ToolError(
            f"{tool}: column {column!r} matches {len(matches)} columns of {urn}: "
            f"{', '.join(repr(m) for m in matches)}. Pass the exact fieldPath of the "
            "one you mean, as list_schema_fields returns it."
        )
    leaf = name.rsplit(".", 1)[-1].lower()
    suggestions = [
        path
        for path, simple in simple_paths.items()
        if simple.rsplit(".", 1)[-1].lower() == leaf
    ][:_MAX_SUGGESTIONS]
    hint = (
        f" Columns with that name: {', '.join(repr(s) for s in suggestions)}."
        if suggestions
        else ""
    )
    raise ToolError(
        f"{tool}: column {column!r} is not in the schema of {urn}.{hint} Find "
        "its exact path with list_schema_fields."
    )
