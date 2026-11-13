"""Builds the text to embed for any entity type from the server's entity registry.

The registry specifications API exposes, for every entity, the ``@Searchable``
annotations of its aspects. Keyword search indexes exactly those fields, so the
text built here covers the same metadata a user can already search for, without
per-entity-type code: new entity types and new aspects are picked up from the
server's registry (including model plugins) as soon as they are registered.
"""

import html
import re
from dataclasses import dataclass, field
from typing import (
    Any,
    Callable,
    Dict,
    FrozenSet,
    Iterable,
    List,
    Optional,
    Sequence,
    Set,
    Tuple,
)

from datahub.ingestion.source.datahub_entity_embeddings.config import EntityTextConfig
from datahub.metadata.urns import Urn
from datahub.utilities.urns.error import InvalidUrnError

TEXT_FIELD_TYPES = frozenset({"TEXT", "TEXT_PARTIAL", "WORD_GRAM"})
URN_FIELD_TYPES = frozenset({"URN", "URN_PARTIAL"})
ENTITY_NAME_ALIAS = "_entityName"
SEMANTIC_CONTENT_ASPECT = "semanticContent"

# DataHub field paths v2 encode types inline, e.g. "[version=2.0].[type=struct].a.[type=string].b".
_FIELD_PATH_V2_TOKEN = re.compile(r"\[[^\]]*\]\.?")
_WHITESPACE = re.compile(r"[ \t]+")
_BLANK_LINES = re.compile(r"\n\s*\n\s*(\n\s*)+")
_HTML_TAG = re.compile(r"<[^>]+>")

ReferenceResolver = Callable[[str], Optional[str]]


@dataclass(frozen=True)
class FieldSpec:
    aspect: str
    path: Tuple[str, ...]
    field_name: str
    is_text: bool
    is_urn: bool
    is_map: bool
    search_tier: Optional[int]
    is_entity_name: bool
    sanitize_rich_text: bool = False

    @property
    def array_split(self) -> Optional[int]:
        """Index of the first ``*`` when it is followed by a sub-path (an array of records)."""
        for i, component in enumerate(self.path):
            if component == "*" and i < len(self.path) - 1:
                return i
        return None


@dataclass
class EntityTextSpec:
    entity_type: str
    search_group: Optional[str]
    key_aspect: str
    aspects: List[str]
    fields: List[FieldSpec]
    has_semantic_content: bool
    all_aspects: FrozenSet[str] = frozenset()

    @property
    def name_fields(self) -> List[FieldSpec]:
        return [f for f in self.fields if f.is_entity_name]


def _spec_fields(
    aspect_name: str,
    aspect_spec: Dict[str, Any],
    field_types: Dict[str, Sequence[str]],
) -> Iterable[FieldSpec]:
    for spec in (aspect_spec.get("searchableFieldSpec") or {}).values():
        annotation = (spec.get("annotations") or {}).get("searchableAnnotation") or {}
        field_name = annotation.get("fieldName")
        if not field_name:
            continue
        types = set(field_types.get(field_name) or [])
        is_entity_name = ENTITY_NAME_ALIAS in (annotation.get("fieldNameAliases") or [])
        is_text = bool(types & TEXT_FIELD_TYPES)
        is_urn = bool(types & URN_FIELD_TYPES)
        # queryByDefault is the server's own signal for "part of the entity's searchable
        # content"; it excludes URLs, timestamps and other filter-only fields.
        if not (is_text or is_urn) or not (
            annotation.get("queryByDefault") or is_entity_name
        ):
            continue
        schema = spec.get("pegasusSchema")
        yield FieldSpec(
            aspect=aspect_name,
            path=tuple(spec.get("pathComponents") or ()),
            field_name=field_name,
            is_text=is_text,
            is_urn=is_urn and not is_text,
            is_map=isinstance(schema, dict) and schema.get("type") == "map",
            search_tier=annotation.get("searchTier"),
            is_entity_name=is_entity_name,
            sanitize_rich_text=bool(annotation.get("sanitizeRichText")),
        )


def parse_registry(elements: Iterable[Dict[str, Any]]) -> Dict[str, EntityTextSpec]:
    """Parse ``/openapi/v1/registry/models/entity/specifications`` elements."""
    specs: Dict[str, EntityTextSpec] = {}
    for element in elements:
        entity_type = element.get("name")
        if not entity_type:
            continue
        field_types = element.get("searchableFieldTypes") or {}
        key_aspect = element.get("keyAspectName") or ""
        aspect_specs = [element.get("keyAspectSpec") or {}] + list(
            element.get("aspectSpecs") or []
        )
        aspects: List[str] = []
        all_aspects: Set[str] = set()
        fields: List[FieldSpec] = []
        seen = set()
        has_semantic_content = False
        for aspect_spec in aspect_specs:
            annotation = aspect_spec.get("aspectAnnotation") or {}
            aspect_name = annotation.get("name")
            if not aspect_name:
                continue
            all_aspects.add(aspect_name)
            if aspect_name == SEMANTIC_CONTENT_ASPECT:
                has_semantic_content = True
                continue
            if annotation.get("timeseries"):
                continue
            for spec in _spec_fields(aspect_name, aspect_spec, field_types):
                if (spec.aspect, spec.path) in seen:
                    continue
                seen.add((spec.aspect, spec.path))
                fields.append(spec)
                if aspect_name not in aspects:
                    aspects.append(aspect_name)
        specs[entity_type] = EntityTextSpec(
            entity_type=entity_type,
            search_group=(element.get("entityAnnotation") or {}).get("searchGroup"),
            key_aspect=key_aspect,
            aspects=aspects,
            fields=fields,
            has_semantic_content=has_semantic_content,
            all_aspects=frozenset(all_aspects),
        )
    return specs


def _unwrap_union(value: Any, component: str) -> Any:
    # Pegasus JSON wraps union members as {"<fully.qualified.Type>": {...}}.
    if (
        isinstance(value, dict)
        and component not in value
        and len(value) == 1
        and "." in next(iter(value))
    ):
        return next(iter(value.values()))
    return value


def extract_values(value: Any, path: Sequence[str]) -> List[Any]:
    """Follow a searchable path through an aspect value; ``*`` fans out arrays and maps."""
    if not path:
        return [] if value is None else [value]
    component, rest = path[0], path[1:]
    if component == "*":
        if isinstance(value, list):
            items: Iterable[Any] = value
        elif isinstance(value, dict):
            items = value.values()
        else:
            return []
        return [v for item in items for v in extract_values(item, rest)]
    value = _unwrap_union(value, component)
    if not isinstance(value, dict) or component not in value:
        return []
    return extract_values(value[component], rest)


def humanize(name: str) -> str:
    words = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", " ", name).replace("_", " ").split()
    if not words:
        return name
    text = " ".join(words).lower()
    return text[0].upper() + text[1:]


def _parse_urn(urn: str) -> Optional[Urn]:
    # URN values come from aspects written by any source; a malformed one is skipped.
    try:
        return Urn.from_string(urn)
    except InvalidUrnError:
        return None


def urn_entity_type(urn: str) -> Optional[str]:
    parsed = _parse_urn(urn)
    return parsed.entity_type if parsed else None


def urn_id(urn: str) -> str:
    """The entity id of a single-id URN (e.g. a tag's name), else the URN itself."""
    parsed = _parse_urn(urn)
    if parsed is None or len(parsed.entity_ids) != 1:
        return urn
    return parsed.entity_ids[0]


def clean_field_path(path: str) -> str:
    return _FIELD_PATH_V2_TOKEN.sub("", path).strip(".") or path


@dataclass
class _Bucket:
    label: str
    tier: int
    values: List[str] = field(default_factory=list)


@dataclass
class _Group:
    label: str
    items: Dict[str, Dict[str, List[str]]] = field(default_factory=dict)

    def touch(self, key: str) -> Dict[str, List[str]]:
        return self.items.setdefault(key, {})

    def add(self, key: str, label: str, value: str) -> None:
        values = self.touch(key).setdefault(label, [])
        if value not in values:
            values.append(value)


@dataclass
class _Collected:
    names: List[str] = field(default_factory=list)
    buckets: Dict[str, _Bucket] = field(default_factory=dict)
    groups: Dict[str, _Group] = field(default_factory=dict)
    seen: Set[str] = field(default_factory=set)

    def add_name(self, value: str) -> None:
        if value.casefold() not in self.seen:
            self.seen.add(value.casefold())
            self.names.append(value)

    def add_value(self, label: str, tier: int, value: str) -> None:
        # The same value often appears in several fields (e.g. a dataset's key name
        # and its qualifiedName, or a description copied by two sources).
        if value.casefold() in self.seen:
            return
        self.seen.add(value.casefold())
        bucket = self.buckets.setdefault(label, _Bucket(label, tier))
        bucket.tier = min(bucket.tier, tier)
        bucket.values.append(value)


class EntityTextBuilder:
    """Renders an entity's searchable metadata as markdown."""

    def __init__(self, config: EntityTextConfig) -> None:
        self.config = config
        self._excluded_values = [re.compile(p) for p in config.exclude_value_patterns]

    def _normalize(self, value: Any, rich_text: bool = False) -> Optional[str]:
        if isinstance(value, bool) or value is None:
            return None
        if isinstance(value, (int, float)):
            value = str(value)
        if not isinstance(value, str):
            return None
        if rich_text:
            # Mirrors @Searchable(sanitizeRichText=true): index the text, not the markup.
            value = html.unescape(_HTML_TAG.sub(" ", value))
        text = "\n".join(
            line.strip() for line in _WHITESPACE.sub(" ", value).splitlines()
        )
        text = _BLANK_LINES.sub("\n\n", text).strip()
        if not text:
            return None
        if len(text) > self.config.max_value_chars:
            text = text[: self.config.max_value_chars].rstrip() + "…"
        return text

    def _label(self, spec: FieldSpec) -> str:
        return self.config.field_labels.get(spec.field_name) or humanize(
            spec.field_name
        )

    def _render_value(
        self, spec: FieldSpec, raw: Any, resolve: ReferenceResolver
    ) -> List[str]:
        if spec.is_urn:
            if not isinstance(raw, str):
                return []
            ref_type = urn_entity_type(raw)
            if ref_type not in self.config.reference_entity_types:
                return []
            name = resolve(raw)
            if name is None and ref_type in self.config.reference_id_fallback_types:
                name = urn_id(raw)
            normalized = self._normalize(name)
            if not normalized or any(
                p.search(normalized) for p in self._excluded_values
            ):
                return []
            return [normalized]
        if spec.is_map:
            if not self.config.include_custom_properties or not isinstance(raw, dict):
                return []
            rendered = []
            # Map order is not stable across responses; it must not change the hash.
            for key, val in sorted(raw.items()):
                normalized = self._normalize(val)
                if normalized:
                    rendered.append(f"{key}: {normalized}")
            return rendered
        normalized = self._normalize(raw, spec.sanitize_rich_text)
        if normalized and spec.path and spec.path[-1] == "fieldPath":
            normalized = clean_field_path(normalized)
        return [normalized] if normalized else []

    def _item_key(self, item: Any) -> Optional[str]:
        if not isinstance(item, dict):
            return None
        for key_field in self.config.item_key_fields:
            value = item.get(key_field)
            if isinstance(value, str) and value.strip():
                return (
                    clean_field_path(value.strip())
                    if key_field == "fieldPath"
                    else value.strip()
                )
        return None

    def _collect(
        self,
        spec: EntityTextSpec,
        aspects: Dict[str, Any],
        resolve: ReferenceResolver,
        collected: _Collected,
        names_as_values: bool = False,
    ) -> None:
        # Names first, or a field with the same value (e.g. qualifiedName) claims it
        # and the title falls back to the URN. Then highest search tier first, so a
        # value repeated by a lower-tier field (e.g. the key aspect) is attributed to
        # the more meaningful one. Aspect and path break ties: GMS serves fields in no
        # particular order.
        for field_spec in sorted(
            spec.fields,
            key=lambda f: (not f.is_entity_name, f.search_tier or 3, f.aspect, f.path),
        ):
            aspect_value = aspects.get(field_spec.aspect)
            if aspect_value is None:
                continue
            split = field_spec.array_split
            if split is None:
                label = self._label(field_spec)
                tier = field_spec.search_tier or 3
                for raw in extract_values(aspect_value, field_spec.path):
                    for value in self._render_value(field_spec, raw, resolve):
                        if field_spec.is_entity_name and not names_as_values:
                            collected.add_name(value)
                        elif field_spec.is_entity_name:
                            collected.add_value("Also known as", 1, value)
                        else:
                            collected.add_value(label, tier, value)
            else:
                self._collect_items(field_spec, split, aspect_value, resolve, collected)

    def _collect_items(
        self,
        field_spec: FieldSpec,
        split: int,
        aspect_value: Any,
        resolve: ReferenceResolver,
        collected: _Collected,
    ) -> None:
        """Arrays of records: keyed items (e.g. columns) form a section, others are inline."""
        group_path = f"{field_spec.aspect}.{'.'.join(field_spec.path[:split])}"
        group_path = self.config.merge_array_groups.get(group_path, group_path)
        sub_path = field_spec.path[split + 1 :]
        is_key_field = sub_path[-1] in self.config.item_key_fields
        label = self._label(field_spec)
        tier = field_spec.search_tier or 3
        for array in extract_values(aspect_value, field_spec.path[:split]):
            if not isinstance(array, list):
                continue
            for element in array:
                key = self._item_key(element)
                if key is None:
                    # e.g. tag or glossary term associations: a plain list of values.
                    for raw in extract_values(element, sub_path):
                        for value in self._render_value(field_spec, raw, resolve):
                            collected.add_value(label, tier, value)
                    continue
                group = collected.groups.get(group_path)
                if group is None:
                    group = collected.groups[group_path] = _Group(
                        self.config.group_labels.get(group_path)
                        or humanize(field_spec.path[split - 1] if split else group_path)
                    )
                if is_key_field:
                    group.touch(key)
                    continue
                for raw in extract_values(element, sub_path):
                    for value in self._render_value(field_spec, raw, resolve):
                        group.add(key, label, value)

    def _render_group(self, group: _Group) -> List[str]:
        described: List[str] = []
        bare: List[str] = []
        for key, labelled in group.items.items():
            details = [
                "; ".join(values)
                if label in self.config.unlabelled_item_fields
                else f"{label}: {', '.join(values)}"
                for label, values in labelled.items()
            ]
            if details:
                described.append(f"- {key}: {'; '.join(details)}")
            else:
                bare.append(key)
        if not described and not bare:
            return []
        limit = self.config.max_items_per_group
        lines = [f"## {group.label}", ""]
        if described:
            lines += described[:limit] + [""]
        remaining = bare[: max(limit - len(described), 0)]
        if remaining:
            listed = ", ".join(remaining)
            lines += [
                f"Other {group.label.lower()}: {listed}" if described else listed,
                "",
            ]
        return lines

    def build(
        self,
        spec: EntityTextSpec,
        urn: str,
        aspects: Dict[str, Any],
        resolve: ReferenceResolver,
        siblings: Sequence[Tuple[EntityTextSpec, Dict[str, Any]]] = (),
    ) -> str:
        """Return markdown text for the entity, or "" when it has no searchable content."""
        collected = _Collected()
        self._collect(spec, aspects, resolve, collected)
        title = collected.names[0] if collected.names else urn_id(urn)
        collected.seen.add(title.casefold())
        for sibling_spec, sibling_aspects in siblings:
            self._collect(
                sibling_spec, sibling_aspects, resolve, collected, names_as_values=True
            )
        other_names = collected.names[1:]
        if other_names:
            for name in other_names:
                collected.seen.discard(name.casefold())
                collected.add_value("Also known as", 1, name)

        if not collected.names and not collected.buckets and not collected.groups:
            return ""
        # Drop key-aspect values that only repeat the title (e.g. a tag's id).
        for bucket in collected.buckets.values():
            bucket.values = [
                v for v in bucket.values if v.casefold() != title.casefold()
            ]
        buckets = [b for b in collected.buckets.values() if b.values]

        type_label = self.config.entity_type_labels.get(spec.entity_type) or humanize(
            spec.entity_type
        )
        lines = [f"# {type_label}: {title}", ""]
        inline: List[str] = []
        sections: List[str] = []
        for bucket in sorted(buckets, key=lambda b: b.tier):
            joined = ", ".join(bucket.values)
            if len(joined) <= self.config.inline_value_chars and "\n" not in joined:
                inline.append(f"{bucket.label}: {joined}")
            elif all(len(v) <= self.config.inline_value_chars for v in bucket.values):
                sections += [f"## {bucket.label}", "", joined, ""]
            else:
                sections += [f"## {bucket.label}", ""]
                sections += [value + "\n" for value in bucket.values]
        if inline:
            lines += inline + [""]
        lines += sections
        for group in collected.groups.values():
            lines += self._render_group(group)

        text = "\n".join(lines).strip() + "\n"
        limit = self.config.max_text_chars
        if len(text) > limit:
            cut = text.rfind("\n", 0, limit)
            text = text[: cut + 1] if cut > 0 else text[:limit]
        return text
