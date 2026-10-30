import json
import logging
import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set

from datahub.ingestion.source.powerbi.rest_api_wrapper.data_classes import (
    PBI_DATASET_CATALOG_PREFIX,
    FieldReferenceKind,
    PageFieldReference,
)
from datahub.ingestion.source.powerbi.rest_api_wrapper.fabric_resolver import (
    DefinitionPart,
)

logger = logging.getLogger(__name__)

LEGACY_REPORT_PATH = "report.json"
PBIR_PAGE_RE = re.compile(r"^definition/pages/([^/]+)/page\.json$")
PBIR_VISUAL_RE = re.compile(r"^definition/pages/([^/]+)/visuals/([^/]+)/visual\.json$")
RDL_SUFFIX = ".rdl"
RDL_DATASET_BINDING_RE = re.compile(
    re.escape(PBI_DATASET_CATALOG_PREFIX) + r"([0-9a-fA-F-]{36})"
)

KEY_SECTIONS = "sections"
KEY_VISUAL_CONTAINERS = "visualContainers"
KEY_CONFIG = "config"
KEY_FILTERS = "filters"
KEY_SINGLE_VISUAL = "singleVisual"
KEY_PROTOTYPE_QUERY = "prototypeQuery"
KEY_VISUAL_TYPE = "visualType"
KEY_NAME = "name"
KEY_DISPLAY_NAME = "displayName"
KEY_VISUAL = "visual"
KEY_QUERY = "query"
KEY_FILTER_CONFIG = "filterConfig"
KEY_FROM = "From"
KEY_FROM_NAME = "Name"
KEY_ENTITY = "Entity"
KEY_SOURCE = "Source"
KEY_COLUMN = "Column"
KEY_MEASURE = "Measure"
KEY_PROPERTY_VARIATION_SOURCE = "PropertyVariationSource"
KEY_EXPRESSION = "Expression"
KEY_SOURCE_REF = "SourceRef"
KEY_PROPERTY = "Property"

FIELD_KIND_COLUMN: FieldReferenceKind = "column"
FIELD_KIND_MEASURE: FieldReferenceKind = "measure"


@dataclass
class ParsedVisual:
    name: str
    visual_type: Optional[str]
    fields: List[PageFieldReference]


@dataclass
class ParsedPage:
    name: str
    display_name: Optional[str]
    visuals: List[ParsedVisual] = field(default_factory=list)

    @property
    def field_refs(self) -> List[PageFieldReference]:
        seen: Set[PageFieldReference] = set()
        ordered: List[PageFieldReference] = []
        for visual in self.visuals:
            for ref in visual.fields:
                if ref not in seen:
                    seen.add(ref)
                    ordered.append(ref)
        return ordered

    @property
    def visual_types(self) -> List[str]:
        return sorted({v.visual_type for v in self.visuals if v.visual_type})


@dataclass
class ParsedReportDefinition:
    pages: Dict[str, ParsedPage] = field(default_factory=dict)
    # Definition parts (or embedded JSON strings) that were not valid JSON.
    failed_parts: List[str] = field(default_factory=list)

    def page_by_display_name(self, display_name: str) -> Optional[ParsedPage]:
        for page in self.pages.values():
            if page.display_name == display_name:
                return page
        return None


def _entity_of(
    expression_holder: Dict[str, Any], aliases: Dict[str, str]
) -> Optional[str]:
    """``SourceRef`` is ``{Entity: "Table"}`` in PBIR and ``{Source: "e"}`` (an
    alias declared in the enclosing query's ``From`` list) in the legacy format."""
    source_ref = (expression_holder.get(KEY_EXPRESSION) or {}).get(KEY_SOURCE_REF) or {}
    entity = source_ref.get(KEY_ENTITY)
    if entity:
        return entity
    alias = source_ref.get(KEY_SOURCE)
    return aliases.get(alias) if alias else None


def _collect_field_refs(
    node: Any, out: List[PageFieldReference], aliases: Optional[Dict[str, str]] = None
) -> None:
    aliases = aliases or {}
    if isinstance(node, dict):
        from_list = node.get(KEY_FROM)
        if isinstance(from_list, list):
            scoped = dict(aliases)
            for entry in from_list:
                if (
                    isinstance(entry, dict)
                    and entry.get(KEY_FROM_NAME)
                    and entry.get(KEY_ENTITY)
                ):
                    scoped[entry[KEY_FROM_NAME]] = entry[KEY_ENTITY]
            aliases = scoped

        for key, kind in (
            (KEY_COLUMN, FIELD_KIND_COLUMN),
            (KEY_MEASURE, FIELD_KIND_MEASURE),
            # A date-hierarchy variation ultimately reads one column.
            (KEY_PROPERTY_VARIATION_SOURCE, FIELD_KIND_COLUMN),
        ):
            holder = node.get(key)
            if isinstance(holder, dict):
                entity = _entity_of(holder, aliases)
                prop = holder.get(KEY_PROPERTY)
                if entity and prop:
                    ref = PageFieldReference(table=entity, field=prop, kind=kind)
                    if ref not in out:
                        out.append(ref)

        for value in node.values():
            _collect_field_refs(value, out, aliases)
    elif isinstance(node, list):
        for value in node:
            _collect_field_refs(value, out, aliases)


def _loads_json(
    text: Optional[str], default: Any, label: str, failed: List[str]
) -> Any:
    """Parse report JSON (a part, or a JSON string embedded in the legacy
    format); a syntax error, or a document whose top-level shape differs from
    `default` (an object where a list is expected or vice versa), is recorded
    under `label` for the caller to report and the default is returned so the
    remaining visuals still parse."""
    if not text:
        return default
    try:
        loaded = json.loads(text)
    except ValueError:
        failed.append(label)
        return default
    if not isinstance(loaded, type(default)):
        failed.append(label)
        return default
    return loaded


def _parse_legacy(report_json: str) -> ParsedReportDefinition:
    parsed = ParsedReportDefinition()
    report = _loads_json(report_json, {}, LEGACY_REPORT_PATH, parsed.failed_parts)
    for section in report.get(KEY_SECTIONS) or []:
        page_name = section.get(KEY_NAME)
        if not page_name:
            continue
        page = ParsedPage(name=page_name, display_name=section.get(KEY_DISPLAY_NAME))
        for index, container in enumerate(section.get(KEY_VISUAL_CONTAINERS) or []):
            label = f"{LEGACY_REPORT_PATH}#{page_name}/{index}"
            config = _loads_json(
                container.get(KEY_CONFIG), {}, label, parsed.failed_parts
            )
            single_visual = config.get(KEY_SINGLE_VISUAL) or {}
            fields: List[PageFieldReference] = []
            _collect_field_refs(single_visual.get(KEY_PROTOTYPE_QUERY), fields)
            _collect_field_refs(
                _loads_json(container.get(KEY_FILTERS), [], label, parsed.failed_parts),
                fields,
            )
            page.visuals.append(
                ParsedVisual(
                    name=config.get(KEY_NAME) or "",
                    visual_type=single_visual.get(KEY_VISUAL_TYPE),
                    fields=fields,
                )
            )
        parsed.pages[page_name] = page
    return parsed


def _parse_pbir(parts: List[DefinitionPart]) -> ParsedReportDefinition:
    parsed = ParsedReportDefinition()
    for part in parts:
        match = PBIR_PAGE_RE.match(part.path)
        if match:
            page_json = _loads_json(part.text, {}, part.path, parsed.failed_parts)
            parsed.pages[match.group(1)] = ParsedPage(
                name=page_json.get(KEY_NAME) or match.group(1),
                display_name=page_json.get(KEY_DISPLAY_NAME),
            )
    for part in parts:
        match = PBIR_VISUAL_RE.match(part.path)
        if not match:
            continue
        page_folder = match.group(1)
        page = parsed.pages.get(page_folder)
        if page is None:
            page = ParsedPage(name=page_folder, display_name=None)
            parsed.pages[page_folder] = page
        visual_json = _loads_json(part.text, {}, part.path, parsed.failed_parts)
        visual = visual_json.get(KEY_VISUAL) or {}
        fields: List[PageFieldReference] = []
        _collect_field_refs(visual.get(KEY_QUERY), fields)
        _collect_field_refs(visual_json.get(KEY_FILTER_CONFIG), fields)
        page.visuals.append(
            ParsedVisual(
                name=visual_json.get(KEY_NAME) or match.group(2),
                visual_type=visual.get(KEY_VISUAL_TYPE),
                fields=fields,
            )
        )
    return parsed


def parse_report_definition(parts: List[DefinitionPart]) -> ParsedReportDefinition:
    """Visual field references per page from a report ``getDefinition``.

    Handles both the folder-per-visual PBIR layout and the single
    ``report.json`` PBIR-Legacy layout.
    """
    for part in parts:
        if part.path == LEGACY_REPORT_PATH:
            return _parse_legacy(part.text)
    return _parse_pbir(parts)


def parse_rdl_dataset_binding(parts: List[DefinitionPart]) -> Optional[str]:
    """Semantic-model id a paginated report is bound to, from its ``.rdl``.

    Power BI encodes the binding as an AnalysisServices data source whose
    database is ``sobe_wowvirtualserver-<datasetId>``; a plain text search is
    enough and avoids parsing the RDL XML.
    """
    for part in parts:
        if not part.path.endswith(RDL_SUFFIX):
            continue
        match = RDL_DATASET_BINDING_RE.search(part.text)
        if match:
            return match.group(1)
    return None
