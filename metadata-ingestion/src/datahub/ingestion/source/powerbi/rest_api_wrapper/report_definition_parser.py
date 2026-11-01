import functools
import json
import logging
import re
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

from datahub.ingestion.source.powerbi.rest_api_wrapper.data_classes import (
    PBI_DATASET_CATALOG_PREFIX,
    FieldReferenceKind,
    FilterScope,
    PageFieldReference,
    ReportFilter,
)
from datahub.ingestion.source.powerbi.rest_api_wrapper.fabric_resolver import (
    DefinitionPart,
)

logger = logging.getLogger(__name__)

JsonNode = Any  # report-definition JSON; callers narrow with isinstance

LEGACY_REPORT_PATH = "report.json"
PBIR_REPORT_PATH = "definition/report.json"
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
KEY_VISUAL_CONTAINER_OBJECTS = "visualContainerObjects"
KEY_LEGACY_VC_OBJECTS = "vcObjects"
KEY_TITLE = "title"
KEY_PROPERTIES = "properties"
KEY_TEXT = "text"
KEY_EXPR = "expr"
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
KEY_FIELD = "field"
KEY_LEGACY_FILTER_EXPRESSION = "expression"
KEY_TYPE = "type"
KEY_FILTER = "filter"
KEY_WHERE = "Where"
KEY_CONDITION = "Condition"
KEY_IN = "In"
KEY_VALUES = "Values"
KEY_LITERAL = "Literal"
KEY_VALUE = "Value"
KEY_COMPARISON = "Comparison"
KEY_COMPARISON_KIND = "ComparisonKind"
KEY_LEFT = "Left"
KEY_RIGHT = "Right"
KEY_NOT = "Not"
KEY_AND = "And"
KEY_OR = "Or"
KEY_CONTAINS = "Contains"
KEY_STARTS_WITH = "StartsWith"
KEY_TOP_N = "TopN"
KEY_COUNT = "Count"
KEY_ORDER_BY = "OrderBy"
KEY_DIRECTION = "Direction"
KEY_DATE_ADD = "DateAdd"
KEY_AMOUNT = "Amount"
KEY_TIME_UNIT = "TimeUnit"

FIELD_KIND_COLUMN: FieldReferenceKind = "column"
FIELD_KIND_MEASURE: FieldReferenceKind = "measure"

FILTER_SCOPE_REPORT: FilterScope = "report"
FILTER_SCOPE_PAGE: FilterScope = "page"
FILTER_SCOPE_VISUAL: FilterScope = "visual"

FILTER_TYPE_RELATIVE_DATE = "RelativeDate"
FILTER_TYPE_RELATIVE_TIME = "RelativeTime"
# Longest raw condition JSON kept when no readable rendering is possible.
RAW_CONDITION_MAX_CHARS = 200
# Longest rendered condition kept; it ends up in a custom property.
RENDERED_CONDITION_MAX_CHARS = 500
# `In [...]` values shown before the rest is summarised as a count.
IN_VALUES_MAX = 20
TRUNCATION_MARKER = "…"
CLAUSE_JOINER = " And "
COMPACT_JSON_SEPARATORS = (",", ":")
# Query-language ComparisonKind codes.
COMPARISON_OPERATORS: Dict[int, str] = {0: "=", 1: ">", 2: ">=", 3: "<", 4: "<="}
# Query-language TimeUnit codes.
TIME_UNITS: Dict[int, str] = {
    0: "day",
    1: "week",
    2: "month",
    3: "year",
    4: "decade",
    5: "second",
    6: "minute",
    7: "hour",
}
RELATIVE_THIS = "This"
RELATIVE_LAST = "Last"
RELATIVE_NEXT = "Next"
TOP_N_DESCENDING = "Top"
TOP_N_ASCENDING = "Bottom"
ORDER_DIRECTION_DESCENDING = 2
# Literal values carry a type suffix (10L, 1.5D) or a type prefix (datetime'...').
LITERAL_NUMBER_SUFFIXES = "LDldM"
LITERAL_NUMBER_RE = re.compile(r"^-?\d+(\.\d+)?[" + LITERAL_NUMBER_SUFFIXES + r"]?$")
LITERAL_TYPED_STRING_RE = re.compile(r"^[A-Za-z]+'(.*)'$")
LITERAL_QUOTE = "'"


@dataclass
class ParsedVisual:
    name: str
    visual_type: Optional[str]
    # Every field the visual reads: its query and its visual-level filters.
    fields: List[PageFieldReference]
    title: Optional[str] = None
    filters: List[ReportFilter] = field(default_factory=list)
    # True when the visual's definition (its part, or its legacy `config`
    # string) was not valid JSON; nothing but its position is known then.
    definition_failed: bool = False


def _ordered_unique(refs: List[PageFieldReference]) -> List[PageFieldReference]:
    return list(dict.fromkeys(refs))


def filter_fields(filters: List[ReportFilter]) -> List[PageFieldReference]:
    """Every model field the given filters read, in order, duplicates kept."""
    return [ref for f in filters for ref in f.fields]


@dataclass
class ParsedPage:
    name: str
    display_name: Optional[str]
    visuals: List[ParsedVisual] = field(default_factory=list)
    filters: List[ReportFilter] = field(default_factory=list)

    @property
    def field_refs(self) -> List[PageFieldReference]:
        """Fields the page's visuals read, then fields its page-level filters
        read, each once."""
        visual_refs = [ref for visual in self.visuals for ref in visual.fields]
        return _ordered_unique([*visual_refs, *filter_fields(self.filters)])

    @property
    def visual_types(self) -> List[str]:
        return sorted({v.visual_type for v in self.visuals if v.visual_type})


@dataclass
class ParsedReportDefinition:
    pages: Dict[str, ParsedPage] = field(default_factory=dict)
    # Report-level filters, which apply to every page.
    filters: List[ReportFilter] = field(default_factory=list)
    # Definition parts (or embedded JSON strings) that were not valid JSON.
    failed_parts: List[str] = field(default_factory=list)
    # Filter blocks (a report's, a page's, or one filter in it) that could not
    # be read; the remaining filters and the page itself still parse.
    failed_filters: List[str] = field(default_factory=list)

    def page_by_display_name(self, display_name: str) -> Optional[ParsedPage]:
        for page in self.pages.values():
            if page.display_name == display_name:
                return page
        return None

    def page_field_refs(self, page: ParsedPage) -> List[PageFieldReference]:
        """The page's own fields followed by those the report-level filters
        read (they apply to every page), each once."""
        return _ordered_unique([*page.field_refs, *filter_fields(self.filters)])


def _dict_at(node: JsonNode, *keys: str) -> Dict[str, JsonNode]:
    """The object reached by following `keys`, or ``{}`` when any step is
    missing or not an object."""
    for key in keys:
        if not isinstance(node, dict):
            return {}
        node = node.get(key)
    return node if isinstance(node, dict) else {}


def _from_aliases(from_list: JsonNode, base: Dict[str, str]) -> Dict[str, str]:
    """`base` extended with the ``Name -> Entity`` aliases a query's ``From``
    list declares."""
    if not isinstance(from_list, list):
        return base
    scoped = dict(base)
    for entry in from_list:
        if (
            isinstance(entry, dict)
            and entry.get(KEY_FROM_NAME)
            and entry.get(KEY_ENTITY)
        ):
            scoped[entry[KEY_FROM_NAME]] = entry[KEY_ENTITY]
    return scoped


def _entity_of(
    expression_holder: Dict[str, JsonNode], aliases: Dict[str, str]
) -> Optional[str]:
    """``SourceRef`` is ``{Entity: "Table"}`` in PBIR and ``{Source: "e"}`` (an
    alias declared in the enclosing query's ``From`` list) in the legacy format."""
    source_ref = _dict_at(expression_holder, KEY_EXPRESSION, KEY_SOURCE_REF)
    entity = source_ref.get(KEY_ENTITY)
    if entity:
        return entity
    alias = source_ref.get(KEY_SOURCE)
    return aliases.get(alias) if alias else None


def _collect_field_refs(
    node: JsonNode,
    out: List[PageFieldReference],
    aliases: Optional[Dict[str, str]] = None,
) -> None:
    aliases = aliases or {}
    if isinstance(node, dict):
        aliases = _from_aliases(node.get(KEY_FROM), aliases)
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


def _visual_title(objects: JsonNode) -> Optional[str]:
    """The visual's title from its formatting objects (PBIR
    ``visual.visualContainerObjects`` or legacy ``singleVisual.vcObjects``),
    where ``title[0].properties.text.expr.Literal.Value`` holds ``'Title'``."""
    title_entries = _dict_at(objects).get(KEY_TITLE)
    if not isinstance(title_entries, list) or not title_entries:
        return None
    value = _dict_at(
        title_entries[0], KEY_PROPERTIES, KEY_TEXT, KEY_EXPR, KEY_LITERAL
    ).get(KEY_VALUE)
    if not isinstance(value, str):
        return None
    if (
        len(value) >= 2
        and value.startswith(LITERAL_QUOTE)
        and value.endswith(LITERAL_QUOTE)
    ):
        value = value[1:-1].replace(LITERAL_QUOTE * 2, LITERAL_QUOTE)
    return value or None


def _truncate(text: str, limit: int) -> str:
    if len(text) <= limit:
        return text
    return text[: limit - 1] + TRUNCATION_MARKER


def _render_literal(node: JsonNode) -> Optional[str]:
    """``{"Literal": {"Value": "'Open'"}}`` -> ``"Open"``, ``10L`` -> ``10``,
    ``datetime'2024-01-01'`` -> ``"2024-01-01"``."""
    value = _dict_at(node, KEY_LITERAL).get(KEY_VALUE)
    if not isinstance(value, str):
        return None
    if (
        len(value) >= 2
        and value.startswith(LITERAL_QUOTE)
        and value.endswith(LITERAL_QUOTE)
    ):
        return json.dumps(value[1:-1].replace(LITERAL_QUOTE * 2, LITERAL_QUOTE))
    typed = LITERAL_TYPED_STRING_RE.match(value)
    if typed:
        return json.dumps(typed.group(1))
    if LITERAL_NUMBER_RE.match(value):
        return value.rstrip(LITERAL_NUMBER_SUFFIXES)
    return value


def _render_field(node: JsonNode, aliases: Dict[str, str]) -> Optional[str]:
    refs: List[PageFieldReference] = []
    _collect_field_refs(node, refs, aliases)
    return refs[0].label if refs else None


ConditionRenderer = Callable[[Dict[str, JsonNode], Dict[str, str]], Optional[str]]


def _render_in(body: Dict[str, JsonNode], aliases: Dict[str, str]) -> Optional[str]:
    values = body.get(KEY_VALUES)
    if not isinstance(values, list):
        return None
    rows: List[str] = []
    for row in values[:IN_VALUES_MAX]:
        if not isinstance(row, list):
            return None
        cells: List[str] = []
        for cell in row:
            rendered = _render_literal(cell)
            if rendered is None:
                return None
            cells.append(rendered)
        rows.append(cells[0] if len(cells) == 1 else f"({', '.join(cells)})")
    hidden = len(values) - len(rows)
    if hidden > 0:
        rows.append(f"{TRUNCATION_MARKER} (+{hidden} more)")
    return f"{KEY_IN} [{', '.join(rows)}]"


def _render_comparison(
    body: Dict[str, JsonNode], aliases: Dict[str, str]
) -> Optional[str]:
    kind = body.get(KEY_COMPARISON_KIND)
    symbol = COMPARISON_OPERATORS.get(kind) if isinstance(kind, int) else None
    right = _render_literal(body.get(KEY_RIGHT))
    if symbol is None or right is None:
        return None
    return f"{symbol} {right}"


def _render_not(body: Dict[str, JsonNode], aliases: Dict[str, str]) -> Optional[str]:
    inner = _render_condition(body.get(KEY_EXPRESSION), aliases)
    return f"{KEY_NOT} {inner}" if inner else None


def _render_binary(
    operator: str, body: Dict[str, JsonNode], aliases: Dict[str, str]
) -> Optional[str]:
    left = _render_condition(body.get(KEY_LEFT), aliases)
    right = _render_condition(body.get(KEY_RIGHT), aliases)
    if left is None or right is None:
        return None
    return f"({left}) {operator} ({right})"


def _render_text_match(
    operator: str, body: Dict[str, JsonNode], aliases: Dict[str, str]
) -> Optional[str]:
    right = _render_literal(body.get(KEY_RIGHT))
    return f"{operator} {right}" if right else None


def _render_top_n(body: Dict[str, JsonNode], aliases: Dict[str, str]) -> Optional[str]:
    count = body.get(KEY_COUNT)
    if not isinstance(count, int):
        return None
    order_by = body.get(KEY_ORDER_BY)
    first = order_by[0] if isinstance(order_by, list) and order_by else {}
    if not isinstance(first, dict):
        return None
    descending = (
        first.get(KEY_DIRECTION, ORDER_DIRECTION_DESCENDING)
        == ORDER_DIRECTION_DESCENDING
    )
    prefix = f"{TOP_N_DESCENDING if descending else TOP_N_ASCENDING} {count}"
    by = _render_field(first.get(KEY_EXPRESSION), aliases)
    return f"{prefix} by {by}" if by else prefix


CONDITION_RENDERERS: Dict[str, ConditionRenderer] = {
    KEY_IN: _render_in,
    KEY_COMPARISON: _render_comparison,
    KEY_NOT: _render_not,
    KEY_AND: functools.partial(_render_binary, KEY_AND),
    KEY_OR: functools.partial(_render_binary, KEY_OR),
    KEY_CONTAINS: functools.partial(_render_text_match, KEY_CONTAINS),
    KEY_STARTS_WITH: functools.partial(_render_text_match, KEY_STARTS_WITH),
    KEY_TOP_N: _render_top_n,
}


def _render_condition(condition: JsonNode, aliases: Dict[str, str]) -> Optional[str]:
    """One ``Where[].Condition`` as text, or None when its shape is not one
    this renderer knows (the caller then falls back to the raw JSON)."""
    if not isinstance(condition, dict) or len(condition) != 1:
        return None
    operator, body = next(iter(condition.items()))
    renderer = CONDITION_RENDERERS.get(operator)
    if renderer is None or not isinstance(body, dict):
        return None
    return renderer(body, aliases)


def _render_relative_date(where: List[JsonNode]) -> Optional[str]:
    """``>= DateAdd(Now, -30, day)`` (usually paired with a ``< Now`` bound)
    as ``Last 30 days``."""
    for clause in where:
        comparison = _dict_at(clause, KEY_CONDITION, KEY_COMPARISON)
        for side in (KEY_RIGHT, KEY_LEFT):
            date_add = _dict_at(comparison, side, KEY_DATE_ADD)
            if not date_add:
                continue
            amount = date_add.get(KEY_AMOUNT)
            time_unit = date_add.get(KEY_TIME_UNIT)
            unit = TIME_UNITS.get(time_unit) if isinstance(time_unit, int) else None
            if not isinstance(amount, int) or unit is None:
                return None
            if amount == 0:
                return f"{RELATIVE_THIS} {unit}"
            count = abs(amount)
            plural = "" if count == 1 else "s"
            direction = RELATIVE_LAST if amount < 0 else RELATIVE_NEXT
            return f"{direction} {count} {unit}{plural}"
    return None


def _render_filter_condition(
    filter_body: Dict[str, JsonNode],
    filter_type: Optional[str],
    aliases: Dict[str, str],
) -> Optional[str]:
    where = filter_body.get(KEY_WHERE)
    if not isinstance(where, list) or not where:
        return None
    if filter_type in (FILTER_TYPE_RELATIVE_DATE, FILTER_TYPE_RELATIVE_TIME):
        relative = _render_relative_date(where)
        if relative:
            return relative
    rendered: List[str] = []
    for clause in where:
        text = _render_condition(_dict_at(clause).get(KEY_CONDITION), aliases)
        if text is None:
            raw = json.dumps(where, separators=COMPACT_JSON_SEPARATORS)
            return _truncate(raw, RAW_CONDITION_MAX_CHARS)
        rendered.append(text)
    return _truncate(CLAUSE_JOINER.join(rendered), RENDERED_CONDITION_MAX_CHARS)


def _parse_filter(entry: Dict[str, JsonNode], scope: FilterScope) -> ReportFilter:
    # PBIR names the declared target `field`; the legacy format `expression`.
    target_node = entry.get(KEY_FIELD)
    if target_node is None:
        target_node = entry.get(KEY_LEGACY_FILTER_EXPRESSION)
    filter_body = _dict_at(entry, KEY_FILTER)
    aliases = _from_aliases(filter_body.get(KEY_FROM), {})
    fields: List[PageFieldReference] = []
    _collect_field_refs(target_node, fields, aliases)
    target = fields[0] if fields else None
    # Everything the condition reads too (a TopN's ordering measure, say).
    _collect_field_refs(filter_body.get(KEY_WHERE), fields, aliases)
    filter_type = entry.get(KEY_TYPE)
    condition = _render_filter_condition(filter_body, filter_type, aliases)
    if target is None and condition is None:
        raise ValueError("neither a target field nor a condition was found")
    return ReportFilter(
        scope=scope,
        name=str(entry.get(KEY_DISPLAY_NAME) or entry.get(KEY_NAME) or ""),
        filter_type=filter_type if isinstance(filter_type, str) else None,
        target=target,
        fields=fields,
        condition=condition,
    )


def parse_filters(
    filters: JsonNode, scope: FilterScope, label: str, failed: List[str]
) -> List[ReportFilter]:
    """Filters from a decoded filter list (PBIR ``filterConfig.filters`` of a
    report, page or visual, or a legacy ``filters`` list).

    A block of the wrong shape, or a single filter that cannot be read, is
    recorded under `label` in `failed` and skipped; the remaining filters
    still parse.
    """
    if filters is None:
        return []
    if not isinstance(filters, list):
        failed.append(label)
        return []
    parsed: List[ReportFilter] = []
    for index, entry in enumerate(filters):
        if not isinstance(entry, dict):
            failed.append(f"{label}#{index}")
            continue
        try:
            parsed.append(_parse_filter(entry, scope))
        except Exception as e:
            logger.debug("Filter %s#%s not parsed", label, index, exc_info=True)
            failed.append(f"{label}#{index} ({type(e).__name__}: {e})")
    return parsed


def _loads_json(
    text: JsonNode, default: JsonNode, label: str, failed: List[str]
) -> JsonNode:
    """Parse report JSON (a part, or a JSON string embedded in the legacy
    format); a syntax error, a value that is not a string, or a document
    whose top-level shape differs from `default` (an object where a list is
    expected or vice versa), is recorded under `label` for the caller to
    report and the default is returned so the remaining content still parses.
    An absent value (`None`) returns the default silently; an empty string is
    a syntax error like any other."""
    if text is None:
        return default
    if not isinstance(text, str):
        failed.append(label)
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


def _legacy_filters(
    holder: Dict[str, JsonNode],
    scope: FilterScope,
    label: str,
    parsed: ParsedReportDefinition,
) -> List[ReportFilter]:
    """Legacy filters are a JSON string under ``filters`` in the report or
    a section."""
    return parse_filters(
        _loads_json(holder.get(KEY_FILTERS), [], label, parsed.failed_filters),
        scope,
        label,
        parsed.failed_filters,
    )


def _pbir_filters(
    doc: Dict[str, JsonNode],
    scope: FilterScope,
    label: str,
    parsed: ParsedReportDefinition,
) -> List[ReportFilter]:
    """PBIR filters sit under ``filterConfig.filters`` of a report, page or
    visual document."""
    filter_config = doc.get(KEY_FILTER_CONFIG)
    if filter_config is None:
        return []
    if not isinstance(filter_config, dict):
        parsed.failed_filters.append(label)
        return []
    return parse_filters(
        filter_config.get(KEY_FILTERS), scope, label, parsed.failed_filters
    )


def _parse_legacy(report_json: str) -> ParsedReportDefinition:
    parsed = ParsedReportDefinition()
    report = _loads_json(report_json, {}, LEGACY_REPORT_PATH, parsed.failed_parts)
    parsed.filters = _legacy_filters(
        report, FILTER_SCOPE_REPORT, LEGACY_REPORT_PATH, parsed
    )
    for section in report.get(KEY_SECTIONS) or []:
        page_name = section.get(KEY_NAME)
        if not page_name:
            continue
        page = ParsedPage(name=page_name, display_name=section.get(KEY_DISPLAY_NAME))
        page.filters = _legacy_filters(
            section, FILTER_SCOPE_PAGE, f"{LEGACY_REPORT_PATH}#{page_name}", parsed
        )
        for index, container in enumerate(section.get(KEY_VISUAL_CONTAINERS) or []):
            label = f"{LEGACY_REPORT_PATH}#{page_name}/{index}"
            failed_before = len(parsed.failed_parts)
            config = _loads_json(
                container.get(KEY_CONFIG), {}, label, parsed.failed_parts
            )
            single_visual = config.get(KEY_SINGLE_VISUAL) or {}
            fields: List[PageFieldReference] = []
            _collect_field_refs(single_visual.get(KEY_PROTOTYPE_QUERY), fields)
            visual_filters = _loads_json(
                container.get(KEY_FILTERS), [], label, parsed.failed_parts
            )
            _collect_field_refs(visual_filters, fields)
            page.visuals.append(
                ParsedVisual(
                    name=config.get(KEY_NAME) or "",
                    visual_type=single_visual.get(KEY_VISUAL_TYPE),
                    fields=fields,
                    title=_visual_title(single_visual.get(KEY_LEGACY_VC_OBJECTS)),
                    filters=parse_filters(
                        visual_filters,
                        FILTER_SCOPE_VISUAL,
                        label,
                        parsed.failed_filters,
                    ),
                    definition_failed=len(parsed.failed_parts) > failed_before,
                )
            )
        parsed.pages[page_name] = page
    return parsed


def _parse_pbir(parts: List[DefinitionPart]) -> ParsedReportDefinition:
    parsed = ParsedReportDefinition()
    for part in parts:
        if part.path == PBIR_REPORT_PATH:
            # Only filters are read from report.json, so a broken part costs
            # just the report-level filters.
            report_json = _loads_json(part.text, {}, part.path, parsed.failed_filters)
            parsed.filters = _pbir_filters(
                report_json, FILTER_SCOPE_REPORT, part.path, parsed
            )
            continue
        match = PBIR_PAGE_RE.match(part.path)
        if match:
            page_json = _loads_json(part.text, {}, part.path, parsed.failed_parts)
            parsed.pages[match.group(1)] = ParsedPage(
                name=page_json.get(KEY_NAME) or match.group(1),
                display_name=page_json.get(KEY_DISPLAY_NAME),
                filters=_pbir_filters(page_json, FILTER_SCOPE_PAGE, part.path, parsed),
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
        failed_before = len(parsed.failed_parts)
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
                title=_visual_title(visual.get(KEY_VISUAL_CONTAINER_OBJECTS)),
                filters=_pbir_filters(
                    visual_json, FILTER_SCOPE_VISUAL, part.path, parsed
                ),
                definition_failed=len(parsed.failed_parts) > failed_before,
            )
        )
    return parsed


def parse_report_definition(parts: List[DefinitionPart]) -> ParsedReportDefinition:
    """Visual field references, titles and report/page/visual filters from a
    report ``getDefinition``.

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
