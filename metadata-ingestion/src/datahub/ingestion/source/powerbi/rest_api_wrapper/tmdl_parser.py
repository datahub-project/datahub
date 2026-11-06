import logging
import re
from dataclasses import dataclass, field
from typing import Dict, List, Literal, Optional, Set

from datahub.ingestion.source.powerbi.rest_api_wrapper.data_classes import (
    DaxFieldReference,
    FieldModelProperties,
    RefreshPolicy,
    Relationship,
)
from datahub.ingestion.source.powerbi.rest_api_wrapper.dax_parser import (
    extract_dax_references,
)
from datahub.ingestion.source.powerbi.rest_api_wrapper.fabric_resolver import (
    DefinitionPart,
)

logger = logging.getLogger(__name__)

TMDL_TABLES_PREFIX = "definition/tables/"
TMDL_ROLES_PREFIX = "definition/roles/"
TMDL_RELATIONSHIPS_PATH = "definition/relationships.tmdl"
TMDL_EXPRESSIONS_PATH = "definition/expressions.tmdl"
TMDL_SUFFIX = ".tmdl"

KEYWORD_TABLE = "table"
KEYWORD_COLUMN = "column"
KEYWORD_MEASURE = "measure"
KEYWORD_PARTITION = "partition"
KEYWORD_RELATIONSHIP = "relationship"
KEYWORD_EXPRESSION = "expression"
KEYWORD_ROLE = "role"
KEYWORD_TABLE_PERMISSION = "tablePermission"
KEYWORD_REFRESH_POLICY = "refreshPolicy"
KEYWORD_KPI = "kpi"

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
PROP_TYPE = "type"
PROP_FORMAT_STRING = "formatString"
PROP_DISPLAY_FOLDER = "displayFolder"
PROP_DATA_CATEGORY = "dataCategory"
PROP_SORT_BY_COLUMN = "sortByColumn"
PROP_SUMMARIZE_BY = "summarizeBy"
PROP_ROLLING_WINDOW_GRANULARITY = "rollingWindowGranularity"
PROP_ROLLING_WINDOW_PERIODS = "rollingWindowPeriods"
PROP_INCREMENTAL_GRANULARITY = "incrementalGranularity"
PROP_INCREMENTAL_PERIODS = "incrementalPeriods"
FLAG_IS_HIDDEN = "isHidden"
FLAG_IS_KEY = "isKey"
FLAG_IS_NULLABLE = "isNullable"
COLUMN_TYPE_CALCULATED = "calculated"

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
PARTITION_MODE_IMPORT = "import"
PARTITION_MODE_DIRECT_QUERY = "directQuery"
PARTITION_MODE_DUAL = "dual"
PARTITION_MODE_PUSH = "push"

# Keywords of the objects the connector reads. An unrecognised line opening
# one of them is an object that went missing; any other unrecognised line
# (annotations, extended properties, newer TMDL syntax) carries nothing the
# connector emits.
TRACKED_OBJECT_KEYWORDS = frozenset(
    {KEYWORD_TABLE, KEYWORD_COLUMN, KEYWORD_MEASURE, KEYWORD_PARTITION}
)

DOC_COMMENT_PREFIX = "///"
FALSE_LITERAL = "false"
TRUE_LITERAL = "true"

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
# `Sql.Database("<server>", "<sql endpoint id>")` inside a shared expression.
SQL_DATABASE_RE = re.compile(r'Sql\.Database\(\s*"([^"]+)"\s*,\s*"([^"]+)"')


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
    # `column X = <DAX>` or `type: calculated`: the column is computed in the
    # model and has no column in the partition's source.
    is_calculated: bool = False
    # Model fields the calculated column's DAX reads, and whether that DAX
    # iterates a table (a bare `[Column]` then belongs to the iterated table).
    references: List[DaxFieldReference] = field(default_factory=list)
    has_row_context_iterators: bool = False
    # Model properties beyond the data type (extract_model_properties).
    model_properties: Optional[FieldModelProperties] = None


@dataclass
class ParsedMeasure:
    name: str
    expression: str
    is_hidden: bool
    description: Optional[str]
    references: List[DaxFieldReference]
    # Whether the DAX iterates a table (a bare `[Column]` then belongs to the
    # iterated table rather than naming a measure).
    has_row_context_iterators: bool = False
    model_properties: Optional[FieldModelProperties] = None


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
    # The first partition; `partition_count` counts all of them (incremental
    # refresh splits a table into one partition per period).
    partition: Optional[ParsedPartition]
    partition_count: int = 0
    refresh_policy: Optional[RefreshPolicy] = None


@dataclass
class ParsedRole:
    """A row-level-security role. Its `member` children are identities and
    are deliberately never read."""

    name: str
    # table name -> DAX filter expression
    table_permissions: Dict[str, str] = field(default_factory=dict)


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
    roles: List[ParsedRole] = field(default_factory=list)
    # Part paths whose content could not be parsed; the caller reports them.
    failed_parts: List[str] = field(default_factory=list)
    # Table part paths that parsed but had a column, measure or partition
    # line the parser could not read; that object is missing from the table.
    incomplete_parts: List[str] = field(default_factory=list)
    # Role parts that could not be parsed; losing one only loses RLS
    # enrichment, so the caller reports them apart from failed_parts.
    failed_roles: List[str] = field(default_factory=list)
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


def parse_tmdl(text: str, unparsed: Optional[List[str]] = None) -> List[TmdlNode]:
    """Parse one TMDL document into a tree of nodes.

    TMDL is indentation based: an object's children are one tab deeper, its
    properties are ``key: value`` lines at that same depth, and a multi-line
    expression (``source =`` followed by M, or ``measure X =`` followed by
    DAX) is everything indented at least two tabs deeper than its header.

    Unrecognised lines that open a tracked object (``column``, ``measure``,
    ...) are appended to `unparsed`; other unrecognised lines are ignored.
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
            if unparsed is not None and content.split()[0] in TRACKED_OBJECT_KEYWORDS:
                unparsed.append(content)
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


def _dax_references(expression: str) -> List[DaxFieldReference]:
    return extract_dax_references(expression).fields


def _bool_property(node: TmdlNode, key: str) -> Optional[bool]:
    """TMDL writes a true boolean as a bare word and a non-default false as
    ``key: false``; None when the property is absent."""
    if key in node.flags:
        return True
    value = node.properties.get(key)
    if value is None:
        return None
    return value.strip().lower() == TRUE_LITERAL


def _parse_refresh_policy(node: TmdlNode) -> Optional[RefreshPolicy]:
    policies = [
        child
        for child in node.children
        if child.keyword == KEYWORD_REFRESH_POLICY and child.name is None
    ]
    if not policies:
        return None
    props = policies[0].properties
    return RefreshPolicy(
        rolling_window_granularity=props.get(PROP_ROLLING_WINDOW_GRANULARITY),
        rolling_window_periods=props.get(PROP_ROLLING_WINDOW_PERIODS),
        incremental_granularity=props.get(PROP_INCREMENTAL_GRANULARITY),
        incremental_periods=props.get(PROP_INCREMENTAL_PERIODS),
    )


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
        # TMDL quotes a name with spaces (`'DirectQuery to AS - Sales'`); the
        # shared-expression index and the lakehouse table name need the bare name.
        entity_name = source_props.get(PROP_ENTITY_NAME)
        expression_source = source_props.get(PROP_EXPRESSION_SOURCE)
        return ParsedPartition(
            kind=kind,
            mode=mode or PARTITION_MODE_DIRECT_LAKE,
            source=None,
            entity_name=_unquote(entity_name) if entity_name else None,
            schema_name=source_props.get(PROP_SCHEMA_NAME),
            expression_source=_unquote(expression_source)
            if expression_source
            else None,
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


def _parse_field_properties(node: TmdlNode) -> FieldModelProperties:
    """The column / measure properties beyond the data type. Properties a
    measure cannot have are simply absent on its node."""
    return FieldModelProperties(
        format_string=node.properties.get(PROP_FORMAT_STRING),
        display_folder=node.properties.get(PROP_DISPLAY_FOLDER),
        data_category=node.properties.get(PROP_DATA_CATEGORY),
        sort_by_column=node.properties.get(PROP_SORT_BY_COLUMN),
        summarize_by=node.properties.get(PROP_SUMMARIZE_BY),
        is_key=_bool_property(node, FLAG_IS_KEY) is True,
        is_nullable=_bool_property(node, FLAG_IS_NULLABLE),
        has_kpi=KEYWORD_KPI in node.flags,
    )


def _parse_column(node: TmdlNode) -> Optional[ParsedColumn]:
    if node.name is None:
        return None
    raw_type = node.properties.get(PROP_DATA_TYPE)
    expression = node.expression
    is_calculated = (
        expression is not None
        or node.properties.get(PROP_TYPE, "").strip() == COLUMN_TYPE_CALCULATED
    )
    dax = extract_dax_references(expression)
    return ParsedColumn(
        name=node.name,
        data_type=TMDL_DATA_TYPE_TO_SCAN.get(raw_type, raw_type) if raw_type else None,
        source_column=node.properties.get(PROP_SOURCE_COLUMN),
        is_hidden=_bool_property(node, FLAG_IS_HIDDEN) is True,
        description=node.description,
        expression=expression,
        is_calculated=is_calculated,
        references=dax.fields,
        has_row_context_iterators=dax.has_row_context_iterators,
        model_properties=_parse_field_properties(node),
    )


def _parse_measure(node: TmdlNode) -> Optional[ParsedMeasure]:
    if node.name is None:
        return None
    expression = node.expression or ""
    dax = extract_dax_references(expression)
    return ParsedMeasure(
        name=node.name,
        expression=expression,
        is_hidden=_bool_property(node, FLAG_IS_HIDDEN) is True,
        description=node.description,
        references=dax.fields,
        has_row_context_iterators=dax.has_row_context_iterators,
        model_properties=_parse_field_properties(node),
    )


def parse_table(
    text: str, unparsed: Optional[List[str]] = None
) -> Optional[ParsedTable]:
    tables = [
        node for node in parse_tmdl(text, unparsed) if node.keyword == KEYWORD_TABLE
    ]
    if not tables:
        return None
    node = tables[0]
    if node.name is None:
        return None

    columns = [_parse_column(child) for child in node.child_values(KEYWORD_COLUMN)]
    measures = [_parse_measure(child) for child in node.child_values(KEYWORD_MEASURE)]
    partitions = node.child_values(KEYWORD_PARTITION)
    partition = _parse_partition(partitions[0]) if partitions else None

    return ParsedTable(
        name=node.name,
        description=node.description,
        columns=[column for column in columns if column is not None],
        measures=[measure for measure in measures if measure is not None],
        partition=partition,
        partition_count=len(partitions),
        refresh_policy=_parse_refresh_policy(node),
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


def parse_roles(text: str) -> List[ParsedRole]:
    """Roles of one ``definition/roles/<Name>.tmdl`` part: the role name and
    its per-table DAX filters. ``member`` children are identities and are
    never read."""
    roles: List[ParsedRole] = []
    for node in parse_tmdl(text):
        if node.keyword != KEYWORD_ROLE or node.name is None:
            continue
        table_permissions: Dict[str, str] = {}
        for permission in node.child_values(KEYWORD_TABLE_PERMISSION):
            if permission.name is None or permission.expression is None:
                continue
            table_permissions[permission.name] = permission.expression.strip()
        roles.append(ParsedRole(name=node.name, table_permissions=table_permissions))
    return roles


def _is_role_part(path: str) -> bool:
    return path.startswith(TMDL_ROLES_PREFIX) and path.endswith(TMDL_SUFFIX)


def parse_semantic_model(parts: List[DefinitionPart]) -> ParsedModel:
    model = ParsedModel()
    for part in parts:
        try:
            if part.path.startswith(TMDL_TABLES_PREFIX) and part.path.endswith(
                TMDL_SUFFIX
            ):
                unparsed: List[str] = []
                table = parse_table(part.text, unparsed)
                if table is None:
                    model.failed_parts.append(part.path)
                else:
                    model.tables.append(table)
                    if unparsed:
                        model.incomplete_parts.append(part.path)
            elif _is_role_part(part.path):
                roles = parse_roles(part.text)
                if not roles:
                    model.failed_roles.append(part.path)
                model.roles.extend(roles)
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
            if _is_role_part(part.path):
                model.failed_roles.append(part.path)
            else:
                model.failed_parts.append(part.path)
    return model


def dax_table_references(expression: str) -> List[str]:
    """Distinct model tables a DAX expression reads from, in order of appearance."""
    return extract_dax_references(expression).tables
