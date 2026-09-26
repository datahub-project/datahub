from dataclasses import dataclass, field
from typing import AbstractSet, Any, Dict, List, Optional, Set, Tuple

from datahub.ingestion.source.sigma.formula_parser import extract_bracket_refs

_PAGES = "pages"
_SCHEMA_VERSION = "schemaVersion"
# The `schemaVersion` this parser was written against. A consumer should
# distrust a read whose `schema_version` differs.
SUPPORTED_SCHEMA_VERSION = 1
# A control's `source` binds its value to a column; it carries no lineage. Its
# `controlId` is the name a formula uses to read the control as a parameter.
_CONTROL_ELEMENT_KIND = "control"
_CONTROL_ID = "controlId"
# Sigma's schema requires `source` on a table element; other elements, such as
# a text box or a control's display, may have none.
_TABLE_ELEMENT_KIND = "table"
_ELEMENTS = "elements"
_COLUMNS = "columns"
_SOURCE = "source"
_JOINS = "joins"
_KIND = "kind"
_ID = "id"
_LEFT = "left"
_RIGHT = "right"
_ELEMENT_ID = "elementId"
_DATA_MODEL_ID = "dataModelId"
_CONNECTION_ID = "connectionId"
_PATH = "path"
_JOIN_KIND = "join"
_UNION_KIND = "union"
# Every source kind in Sigma's create-spec schema and published examples is one
# of these. Single-source kinds are ones /columns formulas already reach; a
# `data-model` source is an element of another Data Model. A transpose is valid
# Sigma this parser does not map: its upstream columns appear only in
# `columnsToMerge`. Any other kind -- a renamed "join", say -- is drift.
_SINGLE_SOURCE_KINDS = frozenset(
    {"warehouse-table", "table", "sql", "csv-table", "data-model"}
)
_UNMAPPED_KINDS = frozenset({"transpose"})
_MATCHES = "matches"
_SOURCES = "sources"
_OUTPUT_COLUMN_NAME = "outputColumnName"
_SOURCE_COLUMNS = "sourceColumns"
_JOIN_TYPE = "joinType"
_JOIN_OP = "op"
_INNER_JOIN_TYPE = "inner"
# The join types Sigma's write API accepts. `lookup` is normalised to
# `left-outer` on store, and is kept in case a spec carries it anyway. A stored
# spec always carries `joinType` -- Sigma's published join example has it even
# for an inner join -- so a missing or unknown one is reported, not scored.
_OUTER_JOIN_TYPES = frozenset({"left-outer", "right-outer", "full-outer", "lookup"})
_KNOWN_JOIN_TYPES = _OUTER_JOIN_TYPES | {_INNER_JOIN_TYPE}
# Only equality says two columns hold the same value; the others make a column a
# join participant without making it equal. These are the operators Sigma's
# data model write API accepts and stores verbatim. The UI's null-safe `<=>` and
# `<!=>` are stored as `is-not-distinct-from` and `is-distinct-from`; `<=>`
# itself is rejected. An operator outside the set is reported, so a wrong entry
# fails closed. A missing `op` is equality: Sigma's published join example
# omits it.
_EQUALITY_OPS = frozenset({"=", "is-not-distinct-from"})
_KNOWN_OPS = _EQUALITY_OPS | {
    "!=",
    "is-distinct-from",
    "<",
    "<=",
    ">",
    ">=",
    "within",
    "intersects",
}


@dataclass(frozen=True)
class SpecColumnRef:
    """A column named by a join side or a union branch.

    ``element_id`` is None for a warehouse table, whose columns appear nowhere
    in the spec; ``connection_id`` and ``path`` then identify the table for the
    caller to map. ``column`` is the display name the formula references;
    ``expression`` is the raw formula.
    """

    element_id: Optional[str]
    column: str
    expression: str = ""
    # Set for an element in another Data Model. Element ids are not unique
    # across models, so the column cannot be resolved without it.
    data_model_id: Optional[str] = None
    connection_id: Optional[str] = None
    path: Tuple[str, ...] = ()
    # A union branch's position in ``sources``; None for a join side.
    source_index: Optional[int] = None


@dataclass(frozen=True)
class UnionOutputColumn:
    """A ``union`` element's output column and the branch columns it merges.

    A union output's ``/columns`` formula names at most one branch; the spec
    names all of them, including warehouse-table branches.
    """

    union_element_id: str
    output_column: str
    # In ``sources`` order. A branch whose formula names several columns, such
    # as ``Concat([First], " ", [Last])``, contributes one ref per column, all
    # with the same ``source_index``.
    branches: Tuple[SpecColumnRef, ...]


@dataclass(frozen=True)
class JoinPredicate:
    """A key from a join's ON clause.

    ``left.column`` and ``right.column`` are the columns the join matches on.
    A side may transform its column (``[A] + 1``, ``Left([A], 3)``), so this is
    key participation, not value equality; ``expression`` keeps each formula.
    Under an outer join the key holds only on matched rows, so consumers score
    those edges lower. Either side may be a warehouse table (``element_id`` is
    None, ``connection_id`` and ``path`` set); whether to emit a key edge to
    one is the consumer's call. Identical predicates in different joins are not
    deduplicated.

    Each side's column belongs to that join entry's own ``left`` / ``right``
    descriptor, including in a chained join. Sigma's schema requires each
    entry's ``left`` to be the primary source or an earlier entry's ``right``,
    and its write API resolves each side's formula against that descriptor --
    naming a column the descriptor does not own is rejected as "Column
    reference not found".
    """

    join_element_id: str
    left: SpecColumnRef
    right: SpecColumnRef
    join_type: str

    @property
    def is_outer(self) -> bool:
        return self.join_type in _OUTER_JOIN_TYPES


@dataclass
class DataModelSpecIndex:
    """Join predicates and union columns read from a Data Model's /spec.

    An element whose shape does not match what this parser expects is reported,
    even when part of it was read: partial drift is the likeliest kind, and a
    consumer should not mistake it for a complete read. ``drift_detected`` is
    that judgement in one place.

    ``relationships[]`` is deliberately not read as lineage. A relationship is
    a declared join that moves no data until a formula uses it as
    ``[Element/Relationship/Column]``; resolving such a ref needs the
    relationship's target, which is for the resolver of those refs to index.
    """

    pairs: List[JoinPredicate] = field(default_factory=list)
    unions: List[UnionOutputColumn] = field(default_factory=list)
    unreadable_join_element_ids: List[str] = field(default_factory=list)
    unreadable_union_element_ids: List[str] = field(default_factory=list)
    # Elements whose own shape is not recognised, so a renamed key shows up
    # here instead of as a clean, empty index: by kind for an element with an
    # id and a kind this parser does not know, and as a bare count for one that
    # cannot be named (a non-object source, a missing or blank id or kind).
    unrecognised_kind_element_ids: Dict[str, List[str]] = field(default_factory=dict)
    unrecognised_element_count: int = 0
    # False when `pages`, or a page's `elements`, is not a list: a renamed key
    # there would otherwise read as an empty model, which is a real thing.
    structure_readable: bool = True
    # Every element seen, and those with a source.
    element_count: int = 0
    sourced_element_count: int = 0
    # Valid Sigma this parser reads but does not map, so the lineage left
    # behind is visible without being mistaken for drift: elements of a known
    # but unmapped kind, keyed by kind, and join or union elements with a
    # multi-segment ref (`[Element/Col]`, `[Element/Relationship/Col]`), which
    # needs the element or relationship it names.
    unmapped_element_ids: Dict[str, List[str]] = field(default_factory=dict)
    multi_segment_ref_element_ids: List[str] = field(default_factory=list)
    # The document's `schemaVersion`; see SUPPORTED_SCHEMA_VERSION.
    schema_version: Optional[int] = None

    @property
    def is_supported_schema(self) -> bool:
        return self.schema_version == SUPPORTED_SCHEMA_VERSION

    @property
    def drift_detected(self) -> bool:
        """Whether the read should not be trusted as complete.

        Unmapped kinds and multi-segment refs are valid Sigma, not drift, and an
        empty model is a real one; a renamed `pages`, `elements` or `source` key
        shows up through `structure_readable` and the unrecognised counts.
        """
        return bool(
            self.unreadable_join_element_ids
            or self.unreadable_union_element_ids
            or self.unrecognised_kind_element_ids
            or self.unrecognised_element_count
            or not self.structure_readable
            or not self.is_supported_schema
        )


@dataclass(frozen=True)
class _Owner:
    """Whose column a join side or union branch names."""

    element_id: Optional[str]
    data_model_id: Optional[str] = None
    connection_id: Optional[str] = None
    path: Tuple[str, ...] = ()


@dataclass
class _JoinRead:
    predicates: List[JoinPredicate]
    readable: bool
    multi_segment_refs: bool = False


@dataclass
class _UnionRead:
    columns: List[UnionOutputColumn]
    readable: bool
    multi_segment_refs: bool = False


@dataclass
class _Elements:
    elements: List[Dict[str, Any]]
    readable: bool


def _iter_spec_elements(spec: Dict[str, Any]) -> _Elements:
    pages = spec.get(_PAGES)
    if not isinstance(pages, list):
        return _Elements(elements=[], readable=False)
    elements: List[Dict[str, Any]] = []
    readable = True
    for page in pages:
        page_elements = page.get(_ELEMENTS) if isinstance(page, dict) else None
        if not isinstance(page_elements, list):
            readable = False
            continue
        for element in page_elements:
            if isinstance(element, dict):
                elements.append(element)
            else:
                readable = False
    return _Elements(elements=elements, readable=readable)


def _str_or_none(value: Any) -> Optional[str]:
    return value if isinstance(value, str) and value.strip() else None


def _element_kind(element: Dict[str, Any]) -> str:
    kind = element.get(_KIND)
    return kind.strip().lower() if isinstance(kind, str) else ""


def _is_control(element: Dict[str, Any]) -> bool:
    return _element_kind(element) == _CONTROL_ELEMENT_KIND


def _join_side_columns(
    expression: str, parameters: AbstractSet[str]
) -> Optional[Set[str]]:
    """The distinct columns a join side's formula references, or None for a
    multi-segment ref, which names a relationship or another element.

    Sides are formulas, not identifiers: ``[Col A]`` or ``Coalesce([Col A],
    -2)``. Names are compared exactly, so ``[Key]`` and ``[KEY]`` are two
    columns; Sigma's case rule is unverified, and two is the reading that
    claims less. ``parameters`` are the model's control ids: a ref to one is a
    constant, and any other ref is a column, whatever its name looks like.
    """
    refs = extract_bracket_refs(expression)
    if any(ref.column is not None for ref in refs):
        return None
    return {ref.source for ref in refs if ref.source not in parameters}


def _one_column(names: Optional[Set[str]]) -> Optional[str]:
    """A key is one distinct column, even inside a function or repeated."""
    return next(iter(names)) if names is not None and len(names) == 1 else None


def _owner(descriptor: Any, own_data_model_id: Optional[str]) -> Optional[_Owner]:
    """The owner a join side or union branch descriptor names, or None.

    Shapes: ``{elementId, ...}`` in this Data Model, ``{dataModelId, elementId,
    ...}`` in another, ``{connectionId, path, ...}`` for a warehouse table. A
    missing elementId is not evidence of a warehouse table: if Sigma renamed
    the key, that reading would hide the change instead of reporting it.
    """
    if not isinstance(descriptor, dict):
        return None
    element_id = _str_or_none(descriptor.get(_ELEMENT_ID))
    if element_id:
        data_model_id = _str_or_none(descriptor.get(_DATA_MODEL_ID))
        # Present but unusable is drift: falling back to this model would attach
        # the key to whatever local element shares the id.
        if _DATA_MODEL_ID in descriptor and data_model_id is None:
            return None
        # This model's own id names a local element.
        if data_model_id == own_data_model_id:
            data_model_id = None
        return _Owner(element_id=element_id, data_model_id=data_model_id)
    # A warehouse table needs both: the consumer builds its URN from them, so a
    # side with only one, or a malformed path, is drift.
    connection_id = _str_or_none(descriptor.get(_CONNECTION_ID))
    path = descriptor.get(_PATH)
    if (
        connection_id is None
        or not isinstance(path, list)
        or not path
        or not all(isinstance(p, str) and p for p in path)
    ):
        return None
    return _Owner(element_id=None, connection_id=connection_id, path=tuple(path))


def _column_ref(
    owner: _Owner,
    formula: str,
    column: str,
    *,
    source_index: Optional[int] = None,
) -> SpecColumnRef:
    return SpecColumnRef(
        element_id=owner.element_id,
        column=column,
        expression=formula,
        data_model_id=owner.data_model_id,
        connection_id=owner.connection_id,
        path=owner.path,
        source_index=source_index,
    )


def _read_join(
    join: Any,
    *,
    join_element_id: str,
    parameters: AbstractSet[str],
    own_data_model_id: Optional[str],
) -> _JoinRead:
    """One entry of a join element's ``joins``.

    Readability is decided on SHAPE, before any formula is resolved: a literal
    or composite side is a well-formed predicate that is simply not a key. A
    join with no predicates is unreadable, since nothing says what it matches
    on; an element with no joins at all has nothing to misread.
    """
    if not isinstance(join, dict):
        return _JoinRead(predicates=[], readable=False)
    raw_type = join.get(_JOIN_TYPE)
    join_type = raw_type.strip().lower() if isinstance(raw_type, str) else ""
    left_owner = _owner(join.get(_LEFT), own_data_model_id)
    right_owner = _owner(join.get(_RIGHT), own_data_model_id)
    entries = join.get(_COLUMNS)
    if (
        join_type not in _KNOWN_JOIN_TYPES
        or left_owner is None
        or right_owner is None
        or not isinstance(entries, list)
        or not entries
    ):
        return _JoinRead(predicates=[], readable=False)

    predicates: List[JoinPredicate] = []
    readable = True
    multi_segment_refs = False
    for entry in entries:
        # A side is a required formula, so an empty one is not a literal.
        if (
            not isinstance(entry, dict)
            or _str_or_none(entry.get(_LEFT)) is None
            or _str_or_none(entry.get(_RIGHT)) is None
        ):
            readable = False
            continue
        raw_op = entry.get(_JOIN_OP)
        if raw_op is None:
            op = "="
        elif isinstance(raw_op, str):
            op = raw_op.strip().lower()
        else:
            op = ""
        if op not in _KNOWN_OPS:
            readable = False
            continue
        if op not in _EQUALITY_OPS:
            continue
        # A literal or composite side is a well-formed predicate, not a key; a
        # multi-segment one is also recorded, since it may be a key we cannot map.
        left_names = _join_side_columns(entry[_LEFT], parameters)
        right_names = _join_side_columns(entry[_RIGHT], parameters)
        if left_names is None or right_names is None:
            multi_segment_refs = True
        left_column = _one_column(left_names)
        right_column = _one_column(right_names)
        if left_column is None or right_column is None:
            continue
        # A self-join on one column is not an edge.
        if (left_owner, left_column) == (right_owner, right_column):
            continue
        left = _column_ref(left_owner, entry[_LEFT], left_column)
        right = _column_ref(right_owner, entry[_RIGHT], right_column)
        predicates.append(
            JoinPredicate(
                join_element_id=join_element_id,
                left=left,
                right=right,
                join_type=join_type,
            )
        )
    return _JoinRead(
        predicates=predicates,
        readable=readable,
        multi_segment_refs=multi_segment_refs,
    )


def _read_union(
    source: Dict[str, Any],
    *,
    union_element_id: str,
    parameters: AbstractSet[str],
    own_data_model_id: Optional[str],
) -> _UnionRead:
    """A ``union`` element's source.

    Shape: ``{"kind": "union", "sources": [<descriptor>, ...],
    "matches": [{"outputColumnName": ..., "sourceColumns": [...]}]}``, where
    ``sourceColumns[i]`` is the formula branch ``sources[i]`` contributes and
    an empty slot is a branch contributing nothing. A branch may be a
    warehouse table.
    """
    sources = source.get(_SOURCES)
    matches = source.get(_MATCHES)
    if not isinstance(sources, list) or not isinstance(matches, list):
        return _UnionRead(columns=[], readable=False)
    owners = [_owner(branch, own_data_model_id) for branch in sources]
    # A union with sources but no output columns says nothing about what it
    # produces, like a join with no predicates.
    readable = all(owner is not None for owner in owners) and bool(
        matches or not sources
    )
    multi_segment_refs = False

    columns: List[UnionOutputColumn] = []
    for match in matches:
        output_column = (
            match.get(_OUTPUT_COLUMN_NAME) if isinstance(match, dict) else None
        )
        source_columns = match.get(_SOURCE_COLUMNS) if isinstance(match, dict) else None
        if not isinstance(output_column, str) or not output_column:
            readable = False
            continue
        if not isinstance(source_columns, list):
            readable = False
            continue
        branches: List[SpecColumnRef] = []
        for position, formula in enumerate(source_columns):
            # Positional alignment is inferred, not stated, so a slot with no
            # branch is reported rather than paired with another branch.
            if position >= len(owners):
                readable = False
                continue
            if formula is None or formula == "":
                continue
            if not isinstance(formula, str):
                readable = False
                continue
            # A branch is a data flow, not a key: every column it names feeds
            # the output, and a parameter is a constant it can contribute. No
            # column at all is a branch contributing a constant.
            refs = extract_bracket_refs(formula)
            if any(r.column is not None for r in refs):
                multi_segment_refs = True
            owner = owners[position]
            if owner is None:
                continue
            names = {
                r.source
                for r in refs
                if r.column is None and r.source not in parameters
            }
            # Sorted, not formula order: set order would depend on the hash seed.
            for name in sorted(names):
                branches.append(
                    _column_ref(owner, formula, name, source_index=position)
                )
        if branches:
            columns.append(
                UnionOutputColumn(
                    union_element_id=union_element_id,
                    output_column=output_column,
                    branches=tuple(branches),
                )
            )
    return _UnionRead(
        columns=columns, readable=readable, multi_segment_refs=multi_segment_refs
    )


def parse_data_model_spec(spec: Optional[Dict[str, Any]]) -> DataModelSpecIndex:
    """Join-key equivalences and union branches from a Data Model /spec.

    A join's output column names only one side in its formula, so the other
    side's key column is reachable only from the ON clause::

        source = {"kind": "join",
                  "primarySource": {...},
                  "joins": [{"joinType": ..., "left": {...}, "right": {...},
                             "columns": [{"left": ..., "right": ..., "op": ...}]}]}

    For the union shape see :func:`_read_union`.
    """
    index = DataModelSpecIndex()
    if not isinstance(spec, dict):
        return index
    version = spec.get(_SCHEMA_VERSION)
    if isinstance(version, int) and not isinstance(version, bool):
        index.schema_version = version
    own_data_model_id = _str_or_none(spec.get(_DATA_MODEL_ID))
    walked = _iter_spec_elements(spec)
    index.structure_readable = walked.readable
    elements = walked.elements
    parameters = frozenset(
        control_id
        for element in elements
        if _is_control(element)
        for control_id in [_str_or_none(element.get(_CONTROL_ID))]
        if control_id
    )

    for element in elements:
        index.element_count += 1
        # An element with no source (a text box) has no lineage, nor does a
        # control, whose source only binds its value to a column.
        if _is_control(element):
            continue
        if _SOURCE not in element:
            # A text box has no source; a table element always does.
            if _element_kind(element) == _TABLE_ELEMENT_KIND:
                index.unrecognised_element_count += 1
            continue
        index.sourced_element_count += 1
        source = element.get(_SOURCE)
        if not isinstance(source, dict):
            index.unrecognised_element_count += 1
            continue
        element_id = _str_or_none(element.get(_ID))
        raw_kind = source.get(_KIND)
        kind = raw_kind.strip().lower() if isinstance(raw_kind, str) else ""
        if element_id is None or not kind:
            index.unrecognised_element_count += 1
            continue
        if kind == _UNION_KIND:
            union = _read_union(
                source,
                union_element_id=element_id,
                parameters=parameters,
                own_data_model_id=own_data_model_id,
            )
            index.unions.extend(union.columns)
            if union.multi_segment_refs:
                index.multi_segment_ref_element_ids.append(element_id)
            if not union.readable:
                index.unreadable_union_element_ids.append(element_id)
        elif kind == _JOIN_KIND:
            joins = source.get(_JOINS)
            if not isinstance(joins, list):
                index.unreadable_join_element_ids.append(element_id)
                continue
            # Judged per join: one readable join must not hide a broken one.
            readable = True
            multi_segment_refs = False
            for join in joins:
                read = _read_join(
                    join,
                    join_element_id=element_id,
                    parameters=parameters,
                    own_data_model_id=own_data_model_id,
                )
                index.pairs.extend(read.predicates)
                readable = readable and read.readable
                multi_segment_refs = multi_segment_refs or read.multi_segment_refs
            if multi_segment_refs:
                index.multi_segment_ref_element_ids.append(element_id)
            if not readable:
                index.unreadable_join_element_ids.append(element_id)
        elif kind in _UNMAPPED_KINDS:
            index.unmapped_element_ids.setdefault(kind, []).append(element_id)
        elif kind not in _SINGLE_SOURCE_KINDS:
            index.unrecognised_kind_element_ids.setdefault(kind, []).append(element_id)
    return index
