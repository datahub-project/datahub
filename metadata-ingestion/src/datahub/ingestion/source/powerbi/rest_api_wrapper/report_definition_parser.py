import json
import logging
import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set

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

FILTER_TYPE_RELATIVE_DATE = "RelativeDate"
FILTER_TYPE_RELATIVE_TIME = "RelativeTime"
# Longest raw condition JSON kept when no readable rendering is possible.
RAW_CONDITION_MAX_CHARS = 200
# Query-language ComparisonKind codes.
COMPARISON_OPERATORS = {0: "=", 1: ">", 2: ">=", 3: "<", 4: "<="}
# Query-language TimeUnit codes.
TIME_UNITS = {
    0: "day",
    1: "week",
    2: "month",
    3: "year",
    4: "decade",
    5: "second",
    6: "minute",
    7: "hour",
}
ORDER_DIRECTION_DESCENDING = 2
# Literal values carry a type suffix (10L, 1.5D) or a type prefix (datetime'...').
LITERAL_NUMBER_RE = re.compile(r"^-?\d+(\.\d+)?[LDldM]?$")
LITERAL_TYPED_STRING_RE = re.compile(r"^[A-Za-z]+'(.*)'$")


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
    filters: List[ReportFilter] = field(default_factory=list)

    @property
    def field_refs(self) -> List[PageFieldReference]:
        """Fields the page's visuals read, then fields its page-level filters
        target, each once."""
        seen: Set[PageFieldReference] = set()
        ordered: List[PageFieldReference] = []
        visual_refs = [ref for visual in self.visuals for ref in visual.fields]
        for ref in [*visual_refs, *filter_targets(self.filters)]:
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


def filter_targets(filters: List[ReportFilter]) -> List[PageFieldReference]:
    return [f.target for f in filters if f.target is not None]


def _render_literal(node: Any) -> Optional[str]:
    """``{"Literal": {"Value": "'Open'"}}`` -> ``"Open"``, ``10L`` -> ``10``,
    ``datetime'2024-01-01'`` -> ``"2024-01-01"``."""
    if not isinstance(node, dict):
        return None
    literal = node.get(KEY_LITERAL)
    if not isinstance(literal, dict):
        return None
    value = literal.get(KEY_VALUE)
    if not isinstance(value, str):
        return None
    if len(value) >= 2 and value.startswith("'") and value.endswith("'"):
        return json.dumps(value[1:-1].replace("''", "'"))
    typed = LITERAL_TYPED_STRING_RE.match(value)
    if typed:
        return json.dumps(typed.group(1))
    if LITERAL_NUMBER_RE.match(value):
        return value.rstrip("LDldM")
    return value


def _render_field(node: Any, aliases: Dict[str, str]) -> Optional[str]:
    refs: List[PageFieldReference] = []
    _collect_field_refs(node, refs, aliases)
    return refs[0].label if refs else None


def _render_relative_date(where: List[Any]) -> Optional[str]:
    """``>= DateAdd(Now, -30, day)`` (usually paired with a ``< Now`` bound)
    as ``Last 30 days``."""
    for clause in where:
        condition = clause.get(KEY_CONDITION) if isinstance(clause, dict) else None
        comparison = (
            condition.get(KEY_COMPARISON) if isinstance(condition, dict) else None
        )
        if not isinstance(comparison, dict):
            continue
        for side in (comparison.get(KEY_RIGHT), comparison.get(KEY_LEFT)):
            date_add = side.get(KEY_DATE_ADD) if isinstance(side, dict) else None
            if not isinstance(date_add, dict):
                continue
            amount = date_add.get(KEY_AMOUNT)
            time_unit = date_add.get(KEY_TIME_UNIT)
            unit = TIME_UNITS.get(time_unit) if isinstance(time_unit, int) else None
            if not isinstance(amount, int) or unit is None:
                return None
            if amount == 0:
                return f"This {unit}"
            count = abs(amount)
            plural = "" if count == 1 else "s"
            return f"{'Last' if amount < 0 else 'Next'} {count} {unit}{plural}"
    return None


def _render_condition(condition: Any, aliases: Dict[str, str]) -> Optional[str]:
    """One ``Where[].Condition`` as text, or None when its shape is not one
    this renderer knows (the caller then falls back to the raw JSON)."""
    if not isinstance(condition, dict) or len(condition) != 1:
        return None
    operator, body = next(iter(condition.items()))
    if not isinstance(body, dict):
        return None
    if operator == KEY_IN:
        rows: List[str] = []
        for row in body.get(KEY_VALUES) or []:
            if not isinstance(row, list):
                return None
            cells: List[str] = []
            for cell in row:
                rendered = _render_literal(cell)
                if rendered is None:
                    return None
                cells.append(rendered)
            rows.append(cells[0] if len(cells) == 1 else f"({', '.join(cells)})")
        return f"In [{', '.join(rows)}]"
    if operator == KEY_COMPARISON:
        kind = body.get(KEY_COMPARISON_KIND)
        symbol = COMPARISON_OPERATORS.get(kind) if isinstance(kind, int) else None
        right = _render_literal(body.get(KEY_RIGHT))
        if symbol is None or right is None:
            return None
        return f"{symbol} {right}"
    if operator == KEY_NOT:
        inner = _render_condition(body.get(KEY_EXPRESSION), aliases)
        return f"Not {inner}" if inner else None
    if operator in (KEY_AND, KEY_OR):
        left = _render_condition(body.get(KEY_LEFT), aliases)
        right_side = _render_condition(body.get(KEY_RIGHT), aliases)
        if left is None or right_side is None:
            return None
        return f"({left}) {operator} ({right_side})"
    if operator in (KEY_CONTAINS, KEY_STARTS_WITH):
        right = _render_literal(body.get(KEY_RIGHT))
        return f"{operator} {right}" if right else None
    if operator == KEY_TOP_N:
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
        prefix = f"{'Top' if descending else 'Bottom'} {count}"
        by = _render_field(first.get(KEY_EXPRESSION), aliases)
        return f"{prefix} by {by}" if by else prefix
    return None


def _render_filter_condition(
    filter_body: Dict[str, Any], filter_type: Optional[str], aliases: Dict[str, str]
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
        text = (
            _render_condition(clause.get(KEY_CONDITION), aliases)
            if isinstance(clause, dict)
            else None
        )
        if text is None:
            raw = json.dumps(where, separators=(",", ":"))
            if len(raw) > RAW_CONDITION_MAX_CHARS:
                raw = raw[: RAW_CONDITION_MAX_CHARS - 1] + "…"
            return raw
        rendered.append(text)
    return " And ".join(rendered)


def _parse_filter(entry: Dict[str, Any], scope: FilterScope) -> ReportFilter:
    # PBIR names the target `field`; the legacy format `expression`.
    target_node = entry.get(KEY_FIELD)
    if target_node is None:
        target_node = entry.get(KEY_LEGACY_FILTER_EXPRESSION)
    filter_body = entry.get(KEY_FILTER)
    if not isinstance(filter_body, dict):
        filter_body = {}
    aliases: Dict[str, str] = {}
    for from_entry in filter_body.get(KEY_FROM) or []:
        if (
            isinstance(from_entry, dict)
            and from_entry.get(KEY_FROM_NAME)
            and from_entry.get(KEY_ENTITY)
        ):
            aliases[from_entry[KEY_FROM_NAME]] = from_entry[KEY_ENTITY]
    targets: List[PageFieldReference] = []
    _collect_field_refs(target_node, targets, aliases)
    if not targets:
        # No usable target declaration: fall back to the first field the
        # condition itself reads.
        _collect_field_refs(filter_body.get(KEY_WHERE), targets, aliases)
    filter_type = entry.get(KEY_TYPE)
    return ReportFilter(
        scope=scope,
        name=str(entry.get(KEY_DISPLAY_NAME) or entry.get(KEY_NAME) or ""),
        filter_type=filter_type if isinstance(filter_type, str) else None,
        target=targets[0] if targets else None,
        condition=_render_filter_condition(filter_body, filter_type, aliases),
    )


def parse_filters(
    filter_config: Any, scope: FilterScope, label: str, failed: List[str]
) -> List[ReportFilter]:
    """Filters from a PBIR ``filterConfig`` object (a report's, a page's or
    a visual's) or from an already-decoded legacy filter list.

    A block of the wrong shape, or a single filter that cannot be read, is
    recorded under `label` in `failed` and skipped; the remaining filters
    still parse.
    """
    if filter_config is None:
        return []
    filters = (
        filter_config.get(KEY_FILTERS)
        if isinstance(filter_config, dict)
        else filter_config
    )
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
        except Exception:
            logger.debug("Filter %s#%s not parsed", label, index, exc_info=True)
            failed.append(f"{label}#{index}")
    return parsed


def _loads_json(
    text: Optional[str], default: Any, label: str, failed: List[str]
) -> Any:
    """Parse report JSON (a part, or a JSON string embedded in the legacy
    format); a syntax error, or a document whose top-level shape differs from
    `default` (an object where a list is expected or vice versa), is recorded
    under `label` for the caller to report and the default is returned so the
    remaining visuals still parse. An absent value (`None`) returns the
    default silently; an empty string is a syntax error like any other."""
    if text is None:
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
    # Legacy filters are JSON strings embedded in the report JSON.
    parsed.filters = parse_filters(
        _loads_json(
            report.get(KEY_FILTERS), [], LEGACY_REPORT_PATH, parsed.failed_filters
        ),
        FILTER_SCOPE_REPORT,
        LEGACY_REPORT_PATH,
        parsed.failed_filters,
    )
    for section in report.get(KEY_SECTIONS) or []:
        page_name = section.get(KEY_NAME)
        if not page_name:
            continue
        page = ParsedPage(name=page_name, display_name=section.get(KEY_DISPLAY_NAME))
        page_label = f"{LEGACY_REPORT_PATH}#{page_name}"
        page.filters = parse_filters(
            _loads_json(
                section.get(KEY_FILTERS), [], page_label, parsed.failed_filters
            ),
            FILTER_SCOPE_PAGE,
            page_label,
            parsed.failed_filters,
        )
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
        if part.path == PBIR_REPORT_PATH:
            # Only filters are read from report.json, so a broken part costs
            # just the report-level filters.
            report_json = _loads_json(part.text, {}, part.path, parsed.failed_filters)
            parsed.filters = parse_filters(
                report_json.get(KEY_FILTER_CONFIG),
                FILTER_SCOPE_REPORT,
                part.path,
                parsed.failed_filters,
            )
            continue
        match = PBIR_PAGE_RE.match(part.path)
        if match:
            page_json = _loads_json(part.text, {}, part.path, parsed.failed_parts)
            parsed.pages[match.group(1)] = ParsedPage(
                name=page_json.get(KEY_NAME) or match.group(1),
                display_name=page_json.get(KEY_DISPLAY_NAME),
                filters=parse_filters(
                    page_json.get(KEY_FILTER_CONFIG),
                    FILTER_SCOPE_PAGE,
                    part.path,
                    parsed.failed_filters,
                ),
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
    """Visual field references and report/page filters from a report
    ``getDefinition``.

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
