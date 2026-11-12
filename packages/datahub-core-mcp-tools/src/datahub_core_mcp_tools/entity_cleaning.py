"""Shape a raw entity read for an LLM: sanitize, cap and select what it returns.

Shared by the tools that return whole entities (``get_entities``,
``list_schema_fields``). Every cap here keeps one response inside an agent's
context window; the limits themselves live in graphql_helpers.
"""

import html
import re
from typing import Callable, Iterator, List, Optional, Sequence

from datahub.utilities.urns.field_paths import get_simple_field_path_from_v2_field_path
from loguru import logger

from .freshness import with_last_modified_dates
from .graphql_helpers import (
    CUSTOM_PROPERTY_VALUE_HARD_LIMIT,
    DESCRIPTION_LENGTH_HARD_LIMIT,
    DOCUMENT_CONTENT_CHAR_LIMIT,
    ENTITY_SCHEMA_TOKEN_BUDGET,
    MAX_ASSERTION_KEY_VALUE_CHARS,
    MAX_ASSERTION_KEY_VALUE_ENTRIES,
    MAX_CONTRACT_DATA_QUALITY_ASSERTIONS,
    QUERY_LENGTH_HARD_LIMIT,
    SCHEMA_FIELD_TEXT_LENGTH_LIMIT,
    clean_gql_response,
    truncate_with_ellipsis,
)
from .schema_matching import FieldText, SchemaFieldMatcher, SubstringMatcher
from .token_estimator import estimate_dict_tokens

# A stable prefix, so a caller can recognize a truncated value.
CUSTOM_PROPERTY_TRUNCATION_MARKER = (
    "... [value truncated: showing {shown} of {total} chars]"
)

_IMG_TAG_START_RE = re.compile(r"<img(?=[\s/>])", re.IGNORECASE)
_IMG_TAG_DELIMITER_RE = re.compile(r"""[<>="']|\s+""")
_IMG_ATTRIBUTE_RE = re.compile(
    r"""[\s/]+([^\s/=<>"']+)(?:\s*=\s*(?:"([^"]*)"|'([^']*)'|([^\s<>"'][^\s<>]*)))?"""
)
_MARKDOWN_DATA_EMBED_RE = re.compile(r"!\[([^\]]*)\]\(data:[^)]+\)")


def _replace_data_uri_img(tag: str) -> str:
    """A data-URI image's alt text; any other or unparseable tag unchanged."""
    attributes: dict[str, str] = {}
    pos = len("<img")
    while attribute := _IMG_ATTRIBUTE_RE.match(tag, pos):
        name = attribute.group(1).lower()
        if name in {"src", "alt"} and name not in attributes:
            # Raw values, so the final html.unescape decodes alt text only once.
            attributes[name] = (
                attribute.group(2) or attribute.group(3) or attribute.group(4) or ""
            )
        pos = attribute.end()
    if tag[pos:].strip().removeprefix("/").strip() != ">":
        return tag
    src = html.unescape(attributes.get("src", "")).lstrip()
    if src[:5].lower() != "data:":
        return tag
    return attributes.get("alt", "")


def _replace_data_uri_images(text: str) -> str:
    """Replace complete data-URI ``<img>`` tags, leaving unparseable spans as they are."""
    parts: list[str] = []
    copied_through = pos = 0
    while start := _IMG_TAG_START_RE.search(text, pos):
        quote = ""
        pending_value = unquoted_value = False
        previous_end = start.end()
        end = len(text)
        for delimiter in _IMG_TAG_DELIMITER_RE.finditer(text, start.end()):
            char = delimiter.group()
            if not quote and pending_value and delimiter.start() > previous_end:
                pending_value = False
                unquoted_value = True
            previous_end = delimiter.end()
            if quote:
                if char == quote:
                    quote = ""
            elif char.isspace():
                unquoted_value = False
            elif char == "=" and not unquoted_value:
                pending_value = True
            elif char in {"'", '"'}:
                # Apostrophes in prose or unquoted values must not hide later tags.
                if pending_value:
                    quote = char
                pending_value = False
            elif char in {"<", ">"}:
                end = delimiter.start()
                break
        if end == len(text):
            break
        # Resume at an unquoted '<' rather than rescanning a malformed tag, so
        # repeated unclosed tags stay linear-time.
        if text[end] == "<":
            pos = end
            continue
        end += 1
        parts.append(text[copied_through : start.start()])
        parts.append(_replace_data_uri_img(text[start.start() : end]))
        copied_through = pos = end
    parts.append(text[copied_through:])
    return "".join(parts)


def sanitize_and_truncate_description(
    text: str, max_length: int, *, dropped_count: bool = False
) -> str:
    """A description without embedded images, cut to ``max_length`` on a word.

    Data-URI images (HTML or markdown) can run to megabytes, so they become their
    alt text. Other markup is kept: descriptions often embed SQL, and stripping
    anything between ``<`` and ``>`` would delete comparisons. ``dropped_count``
    marks a cut with ``[+N chars]`` (see ``truncate_with_ellipsis``).
    """
    if not text:
        return text
    try:
        sanitized = html.unescape(_replace_data_uri_images(text)).strip()
        sanitized = _MARKDOWN_DATA_EMBED_RE.sub(r"\1", sanitized).strip()
        return truncate_with_ellipsis(
            sanitized, max_length, word_boundary=True, dropped_count=dropped_count
        )
    except Exception as e:
        logger.warning(f"Could not sanitize a description: {e}")
        return text[:max_length]


def truncate_custom_property_values(
    custom_properties: object, max_length: int = CUSTOM_PROPERTY_VALUE_HARD_LIMIT
) -> None:
    """Cap long customProperties string values in place, marking each cut.

    Takes the GraphQL ``[{key, value}]`` shape or a plain mapping.
    """

    def cap(value: str) -> str:
        if len(value) <= max_length:
            return value
        marker = CUSTOM_PROPERTY_TRUNCATION_MARKER.format(
            shown=max_length, total=len(value)
        )
        return value[:max_length] + marker

    if isinstance(custom_properties, list):
        for entry in custom_properties:
            if isinstance(entry, dict) and isinstance(entry.get("value"), str):
                entry["value"] = cap(entry["value"])
    elif isinstance(custom_properties, dict):
        for key, value in custom_properties.items():
            if isinstance(value, str):
                custom_properties[key] = cap(value)


def truncate_descriptions(
    data: dict | list, max_length: int = DESCRIPTION_LENGTH_HARD_LIMIT
) -> None:
    """Cap every ``description`` and customProperties value in a result, in place."""
    if isinstance(data, dict):
        for key, value in data.items():
            if key == "description" and isinstance(value, str):
                data[key] = sanitize_and_truncate_description(value, max_length)
            elif key == "customProperties":
                truncate_custom_property_values(value)
            elif isinstance(value, (dict, list)):
                truncate_descriptions(value, max_length)
    elif isinstance(data, list):
        for item in data:
            truncate_descriptions(item, max_length)


def truncate_query(query: str) -> str:
    return truncate_with_ellipsis(
        query, QUERY_LENGTH_HARD_LIMIT, suffix="... [truncated]"
    )


def _cap_info_executable_text(info: dict) -> None:
    """Cap a semantic model's definition and each metric expression like view SQL."""
    native_definition = info.get("nativeDefinition")
    if isinstance(native_definition, str):
        info["nativeDefinition"] = truncate_query(native_definition)
    expression = info.get("expression")
    dialects = expression.get("dialects") if isinstance(expression, dict) else None
    for dialect in dialects if isinstance(dialects, list) else []:
        if isinstance(dialect, dict) and isinstance(dialect.get("expression"), str):
            dialect["expression"] = truncate_query(dialect["expression"])


def _cap_key_values(entries: object) -> tuple[list[dict[str, str]], int]:
    """The first distinct keys of a ``[{key, value}]`` list, values cut, and the key count."""
    if not isinstance(entries, list):
        return [], 0
    seen: dict[str, str] = {}
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        key, value = entry.get("key"), entry.get("value")
        if key and value is not None and key not in seen:
            seen[key] = str(value)
    capped = [
        {
            "key": key,
            "value": truncate_with_ellipsis(
                value, MAX_ASSERTION_KEY_VALUE_CHARS, dropped_count=True
            ),
        }
        for key, value in list(seen.items())[:MAX_ASSERTION_KEY_VALUE_ENTRIES]
    ]
    return capped, len(seen)


def _cap_key_values_in_place(container: object, field: str) -> None:
    """Cap ``container[field]``; a cut list gets ``<field>Total`` beside it."""
    if not isinstance(container, dict) or field not in container:
        return
    capped, total = _cap_key_values(container[field])
    if capped:
        container[field] = capped
    else:
        container.pop(field)
    if total > len(capped):
        container[f"{field}Total"] = total


def cap_assertion_key_values(assertion: dict) -> None:
    """Cap an assertion's customProperties, nativeParameters, and each run's
    nativeResults and error properties, in place."""
    if not str(assertion.get("urn")).startswith("urn:li:assertion:"):
        return
    info = assertion.get("info")
    if isinstance(info, dict):
        _cap_key_values_in_place(info, "customProperties")
        for definition in info.values():
            _cap_key_values_in_place(definition, "nativeParameters")
    run_events = assertion.get("runEvents")
    if not isinstance(run_events, dict):
        return
    for event in run_events.get("runEvents") or []:
        result = event.get("result") if isinstance(event, dict) else None
        if isinstance(result, dict):
            _cap_key_values_in_place(result, "nativeResults")
            _cap_key_values_in_place(result.get("error"), "properties")


def _contract_member_rank(member: object) -> int:
    """Members that decide a contract's verdict first: failing, erroring, passing."""
    assertion = member.get("assertion") if isinstance(member, dict) else None
    runs = ((assertion or {}).get("runEvents") or {}).get("runEvents") or []
    result_type = ((runs[0] or {}).get("result") or {}).get("type") if runs else None
    return {"FAILURE": 0, "ERROR": 1, "SUCCESS": 2}.get(result_type or "", 3)


def cap_data_contract_members(contract: dict) -> None:
    """Cap a data contract's data quality members, keeping the ones that decide its
    verdict; a cut list gets ``dataQualityTotal`` beside it."""
    if not str(contract.get("urn")).startswith("urn:li:dataContract:"):
        return
    properties = contract.get("properties")
    members = properties.get("dataQuality") if isinstance(properties, dict) else None
    if (
        not isinstance(properties, dict)
        or not isinstance(members, list)
        or len(members) <= MAX_CONTRACT_DATA_QUALITY_ASSERTIONS
    ):
        return
    properties["dataQuality"] = sorted(members, key=_contract_member_rank)[
        :MAX_CONTRACT_DATA_QUALITY_ASSERTIONS
    ]
    properties["dataQualityTotal"] = len(members)


# Where a `documentation` entry came from: the `attribution.sourceDetail` flag DataHub
# writes, and the label the agent sees. Ordered the way the UI picks a winner, so an
# entry with two flags reads the same in both. "extracted" marks an excerpt of a
# human-written document linked to the asset; its value is stored on entries
# already written, so it must not change.
_DOCUMENTATION_ORIGINS = (
    ("ui", "ui_authored"),
    ("propagated", "propagated"),
    ("extracted", "document_extracted"),
    ("inferred", "ai_generated"),
)
# Ingested docs and anything written before attribution existed carry no flag.
_UNKNOWN_DOCUMENTATION_ORIGIN = "unknown"


def _documentation_origin(attribution: object) -> str:
    source_detail = (
        (attribution.get("sourceDetail") or []) if isinstance(attribution, dict) else []
    )
    flagged = {
        entry.get("key")
        for entry in source_detail
        if isinstance(entry, dict) and entry.get("value") == "true"
    }
    return next(
        (origin for flag, origin in _DOCUMENTATION_ORIGINS if flag in flagged),
        _UNKNOWN_DOCUMENTATION_ORIGIN,
    )


def documentation_entries(
    documentation: object, *, max_length: int, dropped_count: bool = False
) -> list[dict]:
    """One ``{"text", "origin"}`` per entry of a ``documentation`` aspect.

    That aspect is the only home of a propagated or AI-generated description, and
    the origin travels with the text because the two are not equally reliable.
    """
    entries = (
        (documentation.get("documentations") or [])
        if isinstance(documentation, dict)
        else []
    )
    return [
        {
            "text": sanitize_and_truncate_description(
                entry["documentation"], max_length, dropped_count=dropped_count
            ),
            "origin": _documentation_origin(entry.get("attribution")),
        }
        for entry in entries
        if isinstance(entry, dict) and entry.get("documentation")
    ]


def documentation_text_of(entries: object) -> list[str]:
    """The text of ``documentation_entries`` output, for matching and sizing."""
    return [
        entry["text"]
        for entry in (entries if isinstance(entries, list) else [])
        if isinstance(entry, dict) and isinstance(entry.get("text"), str)
    ]


def _field_path_keys(path: str) -> set[str]:
    """A fieldPath and its v2-simplified form, lowercased, for exact name comparison."""
    return {path.lower(), get_simple_field_path_from_v2_field_path(path).lower()}


def _compact_semantic_field_annotation(annotation: object) -> Optional[dict]:
    """A column's role and expression without the GraphQL wrappers; None when empty.

    A Semantic Model Dataset pays this once per column inside the schema token
    budget, so only the type, aggregation, dialect expressions (capped like view
    SQL) and ``dimension.isTime`` are kept.
    """
    if not isinstance(annotation, dict):
        return None
    compact: dict = {}
    for key in ("type", "aggregationFunction"):
        if isinstance(annotation.get(key), str) and annotation[key]:
            compact[key] = annotation[key]
    expression = annotation.get("expression")
    dialects_in = expression.get("dialects") if isinstance(expression, dict) else None
    dialects: list[dict] = []
    for dialect in dialects_in if isinstance(dialects_in, list) else []:
        if not isinstance(dialect, dict):
            continue
        text = dialect.get("expression")
        if not isinstance(text, str) or not text:
            continue
        entry: dict = {"expression": truncate_query(text)}
        if isinstance(dialect.get("dialect"), str) and dialect["dialect"]:
            entry = {"dialect": dialect["dialect"], **entry}
        dialects.append(entry)
    if dialects:
        compact["expression"] = {"dialects": dialects}
    dimension = annotation.get("dimension")
    if isinstance(dimension, dict) and isinstance(dimension.get("isTime"), bool):
        compact["dimension"] = {"isTime": dimension["isTime"]}
    return compact or None


def _compact_schema_field_entity_attrs(
    schema_field_entity: dict,
    *,
    include_structured_properties: bool,
    text_max_length: int = SCHEMA_FIELD_TEXT_LENGTH_LIMIT,
    dropped_count: bool = False,
) -> dict:
    """A SchemaFieldEntity's documentation, annotation and (when asked) structured
    properties, compacted. Absent inputs produce no key, so the result merges
    straight into a field dict."""
    out: dict = {}
    if entries := documentation_entries(
        schema_field_entity.get("documentation"),
        max_length=text_max_length,
        dropped_count=dropped_count,
    ):
        out["documentation"] = entries
    if include_structured_properties and (
        wrapper := schema_field_entity.get("structuredProperties")
    ):
        compact_props = []
        for entry in wrapper.get("properties") or []:
            definition = (entry.get("structuredProperty") or {}).get("definition") or {}
            name = definition.get("displayName") or definition.get("qualifiedName")
            if not name:
                continue
            values = []
            for v in entry.get("values") or []:
                if sv := v.get("stringValue"):
                    values.append(sv)
                elif nv := v.get("numberValue"):
                    values.append(nv)
            compact_props.append({"name": name, "values": values})
        if compact_props:
            out["structuredProperties"] = compact_props
    if annotation := _compact_semantic_field_annotation(
        schema_field_entity.get("semanticFieldAnnotation")
    ):
        out["semanticFieldAnnotation"] = annotation
    return out


def _sort_fields_by_priority(fields: List[dict]) -> Iterator[dict]:
    """Keys first, then described, then tagged or termed columns, then by fieldPath.

    A deterministic order, so a token budget always cuts the least useful columns.
    """

    def priority(field: dict) -> tuple[int, int, int, str]:
        key_score = (
            2 if field.get("isPartOfKey") else int(bool(field.get("isPartitioningKey")))
        )
        return (
            -key_score,
            -int(bool(field.get("description"))),
            -int(bool(field.get("tags") or field.get("glossaryTerms"))),
            field.get("fieldPath", ""),
        )

    return iter(sorted(fields, key=priority))


def _names_of(wrapper: object, list_key: str, item_key: str) -> list[str]:
    """Tag or term names out of a GlobalTags / GlossaryTerms wrapper."""
    items = wrapper.get(list_key) if isinstance(wrapper, dict) else None
    names = []
    for item in items if isinstance(items, list) else []:
        properties = (item.get(item_key) or {}).get("properties") or {}
        if name := properties.get("name"):
            names.append(name)
    return names


def _clean_schema_fields(
    sorted_fields: Iterator[dict],
    editable_map: dict[str, dict],
    *,
    include_schema_field_entity_attrs: bool = False,
    full_text_columns: Optional[Sequence[str]] = None,
) -> Iterator[dict]:
    """Each column with only what an agent needs, UI edits merged as ``edited*``.

    Per-column text is a short preview (``SCHEMA_FIELD_TEXT_LENGTH_LIMIT``, with a
    ``[+N chars]`` marker when cut) so a whole-schema dump stays cheap. A column in
    ``full_text_columns``, which the caller targeted by name or keyword, gets its
    full text instead.
    """
    full_text_paths = set(full_text_columns or ())
    for f in sorted_fields:
        field_path = f["fieldPath"]
        text_cap = (
            DESCRIPTION_LENGTH_HARD_LIMIT
            if field_path in full_text_paths
            else SCHEMA_FIELD_TEXT_LENGTH_LIMIT
        )

        def preview(text: str, cap: int = text_cap) -> str:
            return truncate_with_ellipsis(
                text, cap, word_boundary=True, dropped_count=True
            )

        field_dict: dict = {"fieldPath": field_path}
        for key in ("type", "nativeDataType"):
            if f.get(key):
                field_dict[key] = f[key]
        if description := f.get("description"):
            field_dict["description"] = preview(description)
        if f.get("nullable") is not None:
            field_dict["nullable"] = f["nullable"]
        if label := f.get("label"):
            field_dict["label"] = label
        for flag in ("isPartOfKey", "isPartitioningKey", "recursive"):
            if f.get(flag):
                field_dict[flag] = True

        if schema_field_entity := f.get("schemaFieldEntity"):
            deprecation = schema_field_entity.get("deprecation") or {}
            if deprecation.get("deprecated"):
                field_dict["deprecated"] = {
                    "deprecated": True,
                    "note": preview(deprecation.get("note", "")),
                }
            field_dict.update(
                _compact_schema_field_entity_attrs(
                    schema_field_entity,
                    include_structured_properties=include_schema_field_entity_attrs,
                    text_max_length=text_cap,
                    dropped_count=True,
                )
            )

        if tags := _names_of(f.get("tags"), "tags", "tag"):
            field_dict["tags"] = tags
        if terms := _names_of(f.get("glossaryTerms"), "terms", "term"):
            field_dict["glossaryTerms"] = terms

        # UI edits are added only where they differ from what ingestion wrote.
        if editable := editable_map.get(field_path):
            if editable_desc := editable.get("description"):
                capped_edit = preview(editable_desc)
                if capped_edit != field_dict.get("description", ""):
                    field_dict["editedDescription"] = capped_edit
            edited_tags = _names_of(editable.get("tags"), "tags", "tag")
            if edited_tags and edited_tags != field_dict.get("tags", []):
                field_dict["editedTags"] = edited_tags
            edited_terms = _names_of(editable.get("glossaryTerms"), "terms", "term")
            if edited_terms and edited_terms != field_dict.get("glossaryTerms", []):
                field_dict["editedGlossaryTerms"] = edited_terms

        yield field_dict


def _select_named_columns(
    fields: list[dict], names: list[str], matcher: SchemaFieldMatcher
) -> tuple[list[dict], Callable[[List[dict]], Iterator[dict]]]:
    """The columns ``names`` match, and an order that puts exact names first.

    A column named exactly is kept and ranked first whatever the matcher scores it,
    so a broad name beside it cannot push it out of the budget.
    """
    name_keys = set().union(*map(_field_path_keys, names))
    scores = matcher.scores(
        names, [FieldText(path=str(f.get("fieldPath") or ""), text=()) for f in fields]
    )
    ranked = [
        (bool(_field_path_keys(str(f.get("fieldPath") or "")) & name_keys), score, f)
        for score, f in zip(scores, fields, strict=True)
    ]
    ranked = [(exact, score, f) for exact, score, f in ranked if exact or score > 0]
    rank_of = {id(f): (not exact, -score) for exact, score, f in ranked}

    def best_match_first(fs: List[dict]) -> Iterator[dict]:
        return iter(sorted(_sort_fields_by_priority(fs), key=lambda f: rank_of[id(f)]))

    return [f for _, _, f in ranked], best_match_first


def _clean_schema_metadata(
    response: dict,
    schema_metadata: dict,
    *,
    sort_fn: Optional[Callable[[List[dict]], Iterator[dict]]],
    offset: int,
    limit: Optional[int],
    include_schema_field_entity_attrs: bool,
    full_text_columns: Optional[Sequence[str]],
    selected_columns: Optional[Sequence[str]],
    column_matcher: Optional[SchemaFieldMatcher],
    schema_token_budget: Optional[int],
) -> None:
    platform_schema = schema_metadata.get("platformSchema")
    if isinstance(platform_schema, dict) and not platform_schema.get("schema"):
        del schema_metadata["platformSchema"]

    fields = schema_metadata.get("fields")
    if not fields:
        return
    # Named columns are chosen from the whole schema, before the budget, so a column
    # the budget would cut is not reported as missing.
    names = [n.strip() for n in (selected_columns or []) if n and n.strip()]
    if names:
        schema_metadata["totalColumns"] = len(fields)
        fields, best_match_first = _select_named_columns(
            fields, names, column_matcher or SubstringMatcher()
        )
        schema_metadata["matchedColumns"] = len(fields)
        full_text_columns = [f["fieldPath"] for f in fields]
        sort_fn = sort_fn or best_match_first

    editable_map: dict[str, dict] = {}
    editable_schema = response.get("editableSchemaMetadata") or {}
    for editable_field in editable_schema.get("editableSchemaFieldInfo") or []:
        if field_path := editable_field.get("fieldPath"):
            editable_map[field_path] = editable_field

    cleaned_fields = _clean_schema_fields(
        (sort_fn or _sort_fields_by_priority)(fields),
        editable_map,
        include_schema_field_entity_attrs=include_schema_field_entity_attrs,
        full_text_columns=full_text_columns,
    )
    budget = (
        ENTITY_SCHEMA_TOKEN_BUDGET
        if schema_token_budget is None
        else schema_token_budget
    )
    selected: list[dict] = []
    used = 0
    for index, field in enumerate(cleaned_fields):
        if index < offset:
            continue
        if limit is not None and len(selected) >= limit:
            break
        tokens = estimate_dict_tokens(field)
        # At least one field after the offset, however large.
        if used + tokens > budget and selected:
            break
        selected.append(field)
        used += tokens

    total_fields = len(fields)
    if len(selected) < total_fields - offset:
        schema_metadata["schemaFieldsTruncated"] = {
            "totalFields": total_fields,
            "includedFields": len(selected),
            "offset": offset,
        }
    schema_metadata["fields"] = selected


def _cap_document_contents(info: dict) -> None:
    contents = info.get("contents")
    text = contents.get("text") if isinstance(contents, dict) else None
    if not isinstance(contents, dict) or not isinstance(text, str):
        return
    if len(text) <= DOCUMENT_CONTENT_CHAR_LIMIT:
        return
    contents["text"] = (
        text[:DOCUMENT_CONTENT_CHAR_LIMIT]
        + f"\n\n[Content truncated. Use grep_documents(start_offset={DOCUMENT_CONTENT_CHAR_LIMIT}) to continue.]"
    )
    contents["_truncated"] = True
    contents["_originalLengthChars"] = len(text)
    contents["_truncatedAtChar"] = DOCUMENT_CONTENT_CHAR_LIMIT


def clean_get_entities_response(
    raw_response: dict,
    *,
    sort_fn: Optional[Callable[[List[dict]], Iterator[dict]]] = None,
    offset: int = 0,
    limit: Optional[int] = None,
    include_schema_field_entity_attrs: bool = False,
    full_text_columns: Optional[Sequence[str]] = None,
    selected_columns: Optional[Sequence[str]] = None,
    column_matcher: Optional[SchemaFieldMatcher] = None,
    schema_token_budget: Optional[int] = None,
) -> dict:
    """A raw entity read, cleaned and bounded for an LLM.

    Drops GraphQL noise (``clean_gql_response``); compacts ``documentation``;
    cleans schema fields and merges ``editableSchemaMetadata`` into them as
    ``edited*`` keys; selects fields by ``sort_fn`` order, ``offset``, ``limit``
    and the schema token budget (``schemaFieldsTruncated`` says when that cut);
    caps view SQL, semantic-model and metric expressions, document contents,
    assertion key/value lists and data contract members; and turns lastModified
    stamps into dates.

    ``selected_columns`` (``get_entities(schema_columns=...)``) keeps only the
    columns those names match, chosen from the whole schema before the budget,
    each with full text; ``schemaMetadata.totalColumns`` and ``matchedColumns``
    record the table's width and the matches. ``column_matcher`` scores them
    (substring matching by default).
    """
    response = clean_gql_response(raw_response)
    if not response:
        return response

    if str(response.get("urn", "")).startswith("urn:li:schemaField:"):
        # A schemaField read by its own URN is a targeted lookup: full-length text.
        compacted = _compact_schema_field_entity_attrs(
            response,
            include_structured_properties=include_schema_field_entity_attrs,
            text_max_length=DESCRIPTION_LENGTH_HARD_LIMIT,
            dropped_count=True,
        )
        drop = ["documentation", "structuredProperties", "semanticFieldAnnotation"]
        if not include_schema_field_entity_attrs:
            drop += ["fieldPath", "parent", "businessAttributes", "deprecation"]
        for key in drop:
            response.pop(key, None)
        response.update(compacted)
    elif entries := documentation_entries(
        response.get("documentation"), max_length=DESCRIPTION_LENGTH_HARD_LIMIT
    ):
        response["documentation"] = entries
    else:
        response.pop("documentation", None)

    if isinstance(schema_metadata := response.get("schemaMetadata"), dict):
        _clean_schema_metadata(
            response,
            schema_metadata,
            sort_fn=sort_fn,
            offset=offset,
            limit=limit,
            include_schema_field_entity_attrs=include_schema_field_entity_attrs,
            full_text_columns=full_text_columns,
            selected_columns=selected_columns,
            column_matcher=column_matcher,
            schema_token_budget=schema_token_budget,
        )
    response.pop("editableSchemaMetadata", None)

    view_properties = response.get("viewProperties")
    if isinstance(view_properties, dict) and view_properties.get("logic"):
        view_properties["logic"] = truncate_query(view_properties["logic"])
    if isinstance(info := response.get("info"), dict):
        _cap_info_executable_text(info)
        _cap_document_contents(info)

    cap_assertion_key_values(response)
    cap_data_contract_members(response)
    return with_last_modified_dates(response)
