"""Fold a dataset's sibling metadata into its own, the way the DataHub UI does.

A warehouse table and the dbt model that builds it are siblings: two entities for one
physical table, each holding what its own source ingested. Column descriptions written
in dbt therefore sit on the dbt entity only. The UI merges siblings before rendering a
page (`combineEntityDataWithSiblings` in datahub-web-react's `siblingUtils.ts`), so a
user on the warehouse table sees the dbt docs, while a tool that read the warehouse
entity alone reported the same columns as undocumented.

Only the documentation-bearing parts are merged, at table and column level:
descriptions, `documentation` entries, tags and glossary terms. Precedence follows the
UI: the sibling marked primary wins a conflict (dbt, for a dbt model), and when none is
marked the requested entity does. Lists are unioned. Columns are matched by field path
ignoring case and v2 field-path tokens, as the UI matches them. Columns that only a
sibling has are not added, since the schema describes the requested table; `siblings`
names the sibling URNs so a caller can read them directly.

`SIBLING_MERGE_ENABLED=false` turns it off: the sibling query is not sent, and the
tools return each entity's own metadata only, with no `siblings` key.
"""

from typing import Callable, Optional

from datahub.cli.env_utils import get_boolean_env_variable
from datahub.utilities.urns.field_paths import get_simple_field_path_from_v2_field_path

SIBLINGS_KEY = "siblings"
SIBLINGS_SEARCH_KEY = "siblingsSearch"
ENABLED_ENV_VAR = "SIBLING_MERGE_ENABLED"


def sibling_merge_enabled() -> bool:
    """Read per call so a changed setting applies without a restart in tests."""
    return get_boolean_env_variable(ENABLED_ENV_VAR, default=True)


# (aspect key, list key inside it, identity of one entry) for each list that is unioned.
_LIST_MERGES: tuple[tuple[str, str, Callable[[dict], object]], ...] = (
    ("tags", "tags", lambda entry: _as_dict(entry.get("tag")).get("urn")),
    ("glossaryTerms", "terms", lambda entry: _as_dict(entry.get("term")).get("urn")),
    ("documentation", "documentations", lambda entry: entry.get("documentation")),
)


def _as_dict(value: object) -> dict:
    return value if isinstance(value, dict) else {}


def _as_list(value: object) -> list:
    return value if isinstance(value, list) else []


def _is_primary(entity: dict) -> bool:
    return bool(_as_dict(entity.get(SIBLINGS_KEY)).get("isPrimary"))


def _child(holder: dict, key: str) -> dict:
    """`holder[key]` as a dict, created when absent or null."""
    if not isinstance(holder.get(key), dict):
        holder[key] = {}
    return holder[key]


def _ranked(entity: dict, siblings: list[dict]) -> list[dict]:
    """The entity and its siblings, the one whose value wins a conflict first."""
    if _is_primary(entity) or not any(_is_primary(s) for s in siblings):
        return [entity, *siblings]
    primary = [s for s in siblings if _is_primary(s)]
    rest = [s for s in siblings if not _is_primary(s)]
    return [*primary, entity, *rest]


def _merge_description(target: dict, sources: list[dict]) -> None:
    text = next(
        (
            s["description"]
            for s in sources
            if isinstance(s.get("description"), str) and s["description"].strip()
        ),
        None,
    )
    if text is not None:
        target["description"] = text


def _merge_lists(target: dict, sources: list[dict]) -> None:
    for aspect, items_key, identity in _LIST_MERGES:
        seen: set[object] = set()
        merged: list[dict] = []
        for source in sources:
            for item in _as_list(_as_dict(source.get(aspect)).get(items_key)):
                if not isinstance(item, dict):
                    continue
                key = identity(item)
                if key in seen:
                    continue
                seen.add(key)
                merged.append(item)
        if merged:
            _child(target, aspect)[items_key] = merged


def _field_key(field_path: object) -> Optional[str]:
    if not isinstance(field_path, str) or not field_path:
        return None
    return get_simple_field_path_from_v2_field_path(field_path.lower())


def _by_field_key(fields: object) -> dict[str, dict]:
    keyed: dict[str, dict] = {}
    for field in _as_list(fields):
        if isinstance(field, dict) and (key := _field_key(field.get("fieldPath"))):
            keyed.setdefault(key, field)
    return keyed


def _editable_fields(entity: dict) -> list:
    return _as_list(
        _as_dict(entity.get("editableSchemaMetadata")).get("editableSchemaFieldInfo")
    )


def _merge_columns(entity: dict, ranked: list[dict]) -> None:
    own_fields = _as_list(_as_dict(entity.get("schemaMetadata")).get("fields"))
    if not own_fields:
        return
    # The entity's own columns and edits are matched by exact fieldPath, as they are
    # without siblings: two of its columns can share a normalized key (`ID` and `id`,
    # or the branches of a v2 union), and each keeps its own metadata. The normalized
    # key only finds the matching column on a sibling. None marks the entity's slot.
    own_edits = {
        e["fieldPath"]: e
        for e in _editable_fields(entity)
        if isinstance(e, dict) and isinstance(e.get("fieldPath"), str)
    }
    schemas = [
        None
        if e is entity
        else _by_field_key(_as_dict(e.get("schemaMetadata")).get("fields"))
        for e in ranked
    ]
    editables = [
        None if e is entity else _by_field_key(_editable_fields(e)) for e in ranked
    ]
    merged_editable: list[dict] = []
    for field in own_fields:
        key = _field_key(field.get("fieldPath")) if isinstance(field, dict) else None
        if key is None:
            continue
        sources = [
            s
            for schema in schemas
            if (s := field if schema is None else schema.get(key)) is not None
        ]
        _merge_description(field, sources)
        _merge_lists(field, sources)
        # A column's propagated and AI-generated descriptions live on its
        # schemaFieldEntity, not on the schema field itself.
        _merge_lists(
            _child(field, "schemaFieldEntity"),
            [_as_dict(s.get("schemaFieldEntity")) for s in sources],
        )
        if edited := [
            e
            for editable in editables
            if (
                e := own_edits.get(field["fieldPath"])
                if editable is None
                else editable.get(key)
            )
            is not None
        ]:
            # Re-keyed to this entity's own fieldPath, which is what the schema
            # cleaning looks edits up by; a sibling may spell the path differently.
            merged = {**edited[0], "fieldPath": field["fieldPath"]}
            _merge_description(merged, edited)
            _merge_lists(merged, edited)
            merged_editable.append(merged)
    _child(entity, "editableSchemaMetadata")["editableSchemaFieldInfo"] = (
        merged_editable
    )


def _siblings(entity: dict) -> list[dict]:
    results = _as_list(_as_dict(entity.get(SIBLINGS_SEARCH_KEY)).get("searchResults"))
    return [
        sibling
        for result in results
        if (sibling := _as_dict(_as_dict(result).get("entity"))).get("urn")
        and sibling["urn"] != entity.get("urn")
    ]


def merge_siblings(entity: dict) -> None:
    """Merge sibling metadata into a raw GetEntity result, in place.

    Expects the `siblingsSearch` key `GraphQLReadBackend.entity` attaches. Replaces it
    and the raw `siblings` aspect with `siblings`: the sibling URNs, or nothing when the
    entity has none. Runs before any cleaning or truncation, so the merged text goes
    through the same caps and field selection as the entity's own. May leave empty
    containers behind, which `clean_gql_response` drops.
    """
    siblings = _siblings(entity)
    entity.pop(SIBLINGS_SEARCH_KEY, None)
    if not siblings:
        entity.pop(SIBLINGS_KEY, None)
        return

    ranked = _ranked(entity, siblings)
    for key in ("properties", "editableProperties"):
        _merge_description(_child(entity, key), [_as_dict(e.get(key)) for e in ranked])
    _merge_lists(entity, ranked)
    _merge_columns(entity, ranked)
    entity[SIBLINGS_KEY] = [s["urn"] for s in siblings]
