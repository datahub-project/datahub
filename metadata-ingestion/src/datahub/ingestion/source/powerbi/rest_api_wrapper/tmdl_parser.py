import logging
import re
from dataclasses import dataclass, field
from typing import Dict, List, Literal, Optional, Set

from datahub.ingestion.source.powerbi.rest_api_wrapper.data_classes import (
    MeasureReference,
    Relationship,
)
from datahub.ingestion.source.powerbi.rest_api_wrapper.fabric_resolver import (
    DefinitionPart,
)

logger = logging.getLogger(__name__)

TMDL_TABLES_PREFIX = "definition/tables/"
TMDL_RELATIONSHIPS_PATH = "definition/relationships.tmdl"
TMDL_EXPRESSIONS_PATH = "definition/expressions.tmdl"
TMDL_SUFFIX = ".tmdl"

KEYWORD_TABLE = "table"
KEYWORD_COLUMN = "column"
KEYWORD_MEASURE = "measure"
KEYWORD_PARTITION = "partition"
KEYWORD_RELATIONSHIP = "relationship"
KEYWORD_EXPRESSION = "expression"

PROP_DATA_TYPE = "dataType"
PROP_SOURCE_COLUMN = "sourceColumn"
PROP_MODE = "mode"
PROP_SOURCE = "source"
PROP_QUERY = "query"
PROP_ENTITY_NAME = "entityName"
PROP_SCHEMA_NAME = "schemaName"
PROP_EXPRESSION_SOURCE = "expressionSource"
PROP_FROM_COLUMN = "fromColumn"
PROP_TO_COLUMN = "toColumn"
PROP_IS_ACTIVE = "isActive"
FLAG_IS_HIDDEN = "isHidden"

PartitionKind = Literal["m", "query", "calculated", "entity"]
PARTITION_KIND_M: PartitionKind = "m"
PARTITION_KIND_QUERY: PartitionKind = "query"
PARTITION_KIND_CALCULATED: PartitionKind = "calculated"
PARTITION_KIND_ENTITY: PartitionKind = "entity"
PARTITION_KINDS: Dict[str, PartitionKind] = {
    PARTITION_KIND_M: PARTITION_KIND_M,
    PARTITION_KIND_QUERY: PARTITION_KIND_QUERY,
    PARTITION_KIND_CALCULATED: PARTITION_KIND_CALCULATED,
    PARTITION_KIND_ENTITY: PARTITION_KIND_ENTITY,
}
PARTITION_MODE_DIRECT_LAKE = "directLake"

DOC_COMMENT_PREFIX = "///"
FALSE_LITERAL = "false"

# TMDL writes TOM data types in camelCase; the scan API (and FIELD_TYPE_MAPPING)
# use the TOM enum casing. Unknown types fall through unchanged.
TMDL_DATA_TYPE_TO_SCAN: Dict[str, str] = {
    "int64": "Int64",
    "double": "Double",
    "boolean": "Boolean",
    "dateTime": "DateTime",
    "string": "String",
    "decimal": "Decimal",
    "binary": "Binary",
    "variant": "Variant",
}

# `keyword` / `keyword name` / `keyword 'quoted name'` with an optional `= value`.
OBJECT_LINE_RE = re.compile(
    r"^(?P<keyword>[A-Za-z_]\w*)(?:\s+(?P<name>'(?:[^']|'')*'|[^=\s]\S*))?\s*(?:=(?P<value>.*))?$"
)
PROPERTY_LINE_RE = re.compile(r"^(?P<key>[A-Za-z_]\w*)\s*:\s*(?P<value>.*)$")
# `'Table'[Column]`, `Table[Column]` or a bare `[Measure]` reference in DAX.
DAX_REFERENCE_RE = re.compile(
    r"(?:'(?P<quoted>(?:[^']|'')+)'|(?P<bare>[A-Za-z_][\w]*))?\[(?P<field>[^\]]+)\]"
)
# `Sql.Database("<server>", "<sql endpoint id>")` inside a shared expression.
SQL_DATABASE_RE = re.compile(r'Sql\.Database\(\s*"([^"]+)"\s*,\s*"([^"]+)"')
# A quoted DAX table name, `'Table Name'`; `''` escapes a quote.
DAX_QUOTED_TABLE_RE = re.compile(r"'((?:[^']|'')+)'")


@dataclass
class TmdlNode:
    keyword: str
    name: Optional[str]
    indent: int
    value: Optional[str] = None
    description: Optional[str] = None
    properties: Dict[str, str] = field(default_factory=dict)
    flags: Set[str] = field(default_factory=set)
    children: List["TmdlNode"] = field(default_factory=list)
    expression_lines: List[str] = field(default_factory=list)

    @property
    def expression(self) -> Optional[str]:
        if self.value is not None and self.value.strip():
            return self.value.strip()
        if self.expression_lines:
            return _dedent("\n".join(self.expression_lines))
        return None

    def child_values(self, keyword: str) -> List["TmdlNode"]:
        return [child for child in self.children if child.keyword == keyword]

    def property_expression(self, key: str) -> Optional[str]:
        """A property whose value is an expression, e.g. ``source =``; either
        inline (``source = ROW("Value", 1)``) or on the following lines."""
        for child in self.children:
            if child.keyword == key and child.name is None:
                return child.expression
        return None


@dataclass
class ParsedColumn:
    name: str
    data_type: Optional[str]
    source_column: Optional[str]
    is_hidden: bool
    description: Optional[str]
    expression: Optional[str]


@dataclass
class ParsedMeasure:
    name: str
    expression: str
    is_hidden: bool
    description: Optional[str]
    references: List[MeasureReference]


@dataclass
class ParsedPartition:
    kind: PartitionKind
    mode: Optional[str]
    source: Optional[str]
    entity_name: Optional[str] = None
    schema_name: Optional[str] = None
    expression_source: Optional[str] = None


@dataclass
class ParsedTable:
    name: str
    description: Optional[str]
    columns: List[ParsedColumn]
    measures: List[ParsedMeasure]
    partition: Optional[ParsedPartition]


@dataclass
class ParsedSharedExpression:
    name: str
    expression: str
    sql_endpoint_server: Optional[str]
    sql_endpoint_id: Optional[str]


@dataclass
class ParsedModel:
    tables: List[ParsedTable] = field(default_factory=list)
    relationships: List[Relationship] = field(default_factory=list)
    shared_expressions: Dict[str, ParsedSharedExpression] = field(default_factory=dict)
    # Part paths whose content could not be parsed; the caller reports them.
    failed_parts: List[str] = field(default_factory=list)
    # Relationship names whose fromColumn/toColumn could not be split.
    skipped_relationships: List[str] = field(default_factory=list)


def _dedent(text: str) -> str:
    lines = text.split("\n")
    indents = [len(line) - len(line.lstrip("\t ")) for line in lines if line.strip()]
    cut = min(indents) if indents else 0
    return "\n".join(line[cut:] if line.strip() else "" for line in lines)


def _unquote(name: str) -> str:
    if len(name) >= 2 and name.startswith("'") and name.endswith("'"):
        return name[1:-1].replace("''", "'")
    return name


def _indent_of(line: str) -> int:
    return len(line) - len(line.lstrip("\t"))


def parse_tmdl(text: str) -> List[TmdlNode]:
    """Parse one TMDL document into a tree of nodes.

    TMDL is indentation based: an object's children are one tab deeper, its
    properties are ``key: value`` lines at that same depth, and a multi-line
    expression (``source =`` followed by M, or ``measure X =`` followed by
    DAX) is everything indented at least two tabs deeper than its header.
    """
    roots: List[TmdlNode] = []
    stack: List[TmdlNode] = []
    last_header: Optional[TmdlNode] = None
    pending_doc: List[str] = []

    for raw_line in text.splitlines():
        if not raw_line.strip():
            if last_header is not None and last_header.expression_lines:
                last_header.expression_lines.append("")
            continue

        indent = _indent_of(raw_line)
        content = raw_line.strip()

        # Only a header ending in a bare `=` continues onto deeper lines. An
        # object header (`measure X =`) keeps its properties one tab deeper
        # and its expression two tabs deeper; a property-expression header
        # (`source =`, `query =`) has no children, so anything deeper is text.
        if (
            last_header is not None
            and last_header.value is not None
            and not last_header.value.strip()
            and indent >= last_header.indent + (2 if last_header.name else 1)
        ):
            last_header.expression_lines.append(raw_line.lstrip("\t"))
            continue

        if content.startswith(DOC_COMMENT_PREFIX):
            pending_doc.append(content[len(DOC_COMMENT_PREFIX) :].strip())
            continue

        while stack and stack[-1].indent >= indent:
            stack.pop()
        parent = stack[-1] if stack else None

        property_match = PROPERTY_LINE_RE.match(content)
        if property_match and parent is not None:
            parent.properties[property_match.group("key")] = property_match.group(
                "value"
            ).strip()
            pending_doc = []
            last_header = None
            continue

        object_match = OBJECT_LINE_RE.match(content)
        if not object_match:
            logger.debug("Ignoring unrecognised TMDL line: %r", content)
            pending_doc = []
            continue

        keyword = object_match.group("keyword")
        name = object_match.group("name")
        value = object_match.group("value")
        if name is None and value is None and parent is not None:
            # A bare word is usually a boolean flag (`isHidden`), but a
            # DirectLake partition's `source` is a bare word whose properties
            # follow on deeper lines, so it is recorded as a node as well.
            parent.flags.add(keyword)

        node = TmdlNode(
            keyword=keyword,
            name=_unquote(name) if name is not None else None,
            indent=indent,
            value=value,
            description=" ".join(pending_doc) if pending_doc else None,
        )
        pending_doc = []
        if parent is None:
            roots.append(node)
        else:
            parent.children.append(node)
        stack.append(node)
        last_header = node

    return roots


def _dax_references(expression: str) -> List[MeasureReference]:
    references: List[MeasureReference] = []
    seen: Set[str] = set()
    for match in DAX_REFERENCE_RE.finditer(expression):
        table = match.group("quoted") or match.group("bare")
        field_name = match.group("field").strip()
        # `[@Alias]` is a row-context variable, not a model field.
        if field_name.startswith("@"):
            continue
        if table is not None:
            table = table.replace("''", "'")
        key = f"{table}|{field_name}"
        if key in seen:
            continue
        seen.add(key)
        references.append(MeasureReference(table=table, field=field_name))
    return references


def _parse_partition(node: TmdlNode) -> ParsedPartition:
    # An unknown partition kind is treated as M, the format of every
    # non-DirectLake import/DirectQuery partition the connector has seen.
    kind = PARTITION_KINDS.get((node.value or "").strip(), PARTITION_KIND_M)
    mode = node.properties.get(PROP_MODE)
    if kind == PARTITION_KIND_ENTITY:
        # DirectLake: `source` is an object with entityName / schemaName /
        # expressionSource properties rather than an expression.
        source_nodes = [
            child
            for child in node.children
            if child.keyword == PROP_SOURCE and child.name is None
        ]
        source_props = source_nodes[0].properties if source_nodes else {}
        return ParsedPartition(
            kind=kind,
            mode=mode or PARTITION_MODE_DIRECT_LAKE,
            source=None,
            entity_name=source_props.get(PROP_ENTITY_NAME),
            schema_name=source_props.get(PROP_SCHEMA_NAME),
            expression_source=source_props.get(PROP_EXPRESSION_SOURCE),
        )
    source = node.property_expression(PROP_SOURCE)
    if source is None and kind == PARTITION_KIND_QUERY:
        # DirectQuery over a native SQL statement: `source` is an object whose
        # `query =` child holds the SQL rather than an M expression.
        source_nodes = [
            child
            for child in node.children
            if child.keyword == PROP_SOURCE and child.name is None
        ]
        if source_nodes:
            source = source_nodes[0].property_expression(PROP_QUERY)
    return ParsedPartition(kind=kind, mode=mode, source=source)


def parse_table(text: str) -> Optional[ParsedTable]:
    tables = [node for node in parse_tmdl(text) if node.keyword == KEYWORD_TABLE]
    if not tables:
        return None
    node = tables[0]
    if node.name is None:
        return None

    columns: List[ParsedColumn] = []
    for column in node.child_values(KEYWORD_COLUMN):
        if column.name is None:
            continue
        raw_type = column.properties.get(PROP_DATA_TYPE)
        columns.append(
            ParsedColumn(
                name=column.name,
                data_type=TMDL_DATA_TYPE_TO_SCAN.get(raw_type, raw_type)
                if raw_type
                else None,
                source_column=column.properties.get(PROP_SOURCE_COLUMN),
                is_hidden=FLAG_IS_HIDDEN in column.flags,
                description=column.description,
                expression=column.expression,
            )
        )

    measures: List[ParsedMeasure] = []
    for measure in node.child_values(KEYWORD_MEASURE):
        if measure.name is None:
            continue
        expression = measure.expression or ""
        measures.append(
            ParsedMeasure(
                name=measure.name,
                expression=expression,
                is_hidden=FLAG_IS_HIDDEN in measure.flags,
                description=measure.description,
                references=_dax_references(expression),
            )
        )

    partitions = node.child_values(KEYWORD_PARTITION)
    partition = _parse_partition(partitions[0]) if partitions else None

    return ParsedTable(
        name=node.name,
        description=node.description,
        columns=columns,
        measures=measures,
        partition=partition,
    )


def _split_column_reference(reference: str) -> Optional[List[str]]:
    """``Claims.PolicyId`` or ``'Claims By State'.State`` -> [table, column]."""
    reference = reference.strip()
    if reference.startswith("'"):
        end = reference.find("'", 1)
        while end != -1 and reference[end + 1 : end + 2] == "'":
            end = reference.find("'", end + 2)
        if end == -1 or reference[end + 1 : end + 2] != ".":
            return None
        return [_unquote(reference[: end + 1]), _unquote(reference[end + 2 :])]
    if "." not in reference:
        return None
    table, column = reference.split(".", 1)
    return [table, _unquote(column)]


def parse_relationships(
    text: str, skipped: Optional[List[str]] = None
) -> List[Relationship]:
    relationships: List[Relationship] = []
    for node in parse_tmdl(text):
        if node.keyword != KEYWORD_RELATIONSHIP or node.name is None:
            continue
        from_ref = _split_column_reference(node.properties.get(PROP_FROM_COLUMN, ""))
        to_ref = _split_column_reference(node.properties.get(PROP_TO_COLUMN, ""))
        if from_ref is None or to_ref is None:
            if skipped is not None:
                skipped.append(node.name)
            continue
        is_active = (
            node.properties.get(PROP_IS_ACTIVE, "").strip().lower() != FALSE_LITERAL
        )
        relationships.append(
            Relationship(
                name=node.name,
                from_table=from_ref[0],
                from_column=from_ref[1],
                to_table=to_ref[0],
                to_column=to_ref[1],
                is_active=is_active,
            )
        )
    return relationships


def parse_shared_expressions(text: str) -> Dict[str, ParsedSharedExpression]:
    expressions: Dict[str, ParsedSharedExpression] = {}
    for node in parse_tmdl(text):
        if node.keyword != KEYWORD_EXPRESSION or node.name is None:
            continue
        expression = node.expression or ""
        sql_match = SQL_DATABASE_RE.search(expression)
        expressions[node.name] = ParsedSharedExpression(
            name=node.name,
            expression=expression,
            sql_endpoint_server=sql_match.group(1) if sql_match else None,
            sql_endpoint_id=sql_match.group(2) if sql_match else None,
        )
    return expressions


def parse_semantic_model(parts: List[DefinitionPart]) -> ParsedModel:
    model = ParsedModel()
    for part in parts:
        try:
            if part.path.startswith(TMDL_TABLES_PREFIX) and part.path.endswith(
                TMDL_SUFFIX
            ):
                table = parse_table(part.text)
                if table is None:
                    model.failed_parts.append(part.path)
                else:
                    model.tables.append(table)
            elif part.path == TMDL_RELATIONSHIPS_PATH:
                model.relationships.extend(
                    parse_relationships(part.text, model.skipped_relationships)
                )
            elif part.path == TMDL_EXPRESSIONS_PATH:
                model.shared_expressions.update(parse_shared_expressions(part.text))
        except Exception as e:
            # A malformed part must not take the whole model down; the caller
            # reports failed_parts and still gets the tables that parsed.
            logger.debug("Failed to parse TMDL part %s: %s", part.path, e)
            model.failed_parts.append(part.path)
    return model


def dax_table_references(expression: str) -> List[str]:
    """Distinct model tables a DAX expression reads from, in order of appearance."""
    tables: List[str] = []
    for reference in _dax_references(expression):
        if reference.table and reference.table not in tables:
            tables.append(reference.table)
    # Quoted names are consumed left to right so a closing quote is never
    # paired with the opening quote of the next reference.
    for match in DAX_QUOTED_TABLE_RE.finditer(expression):
        if expression[match.end() : match.end() + 1] == "[":
            continue
        table = match.group(1).replace("''", "'")
        if table not in tables:
            tables.append(table)
    return tables
