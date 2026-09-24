from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set, Tuple

from datahub.ingestion.source.sigma.formula_parser import extract_bracket_refs

_PAGES = "pages"
_SCHEMA_VERSION = "schemaVersion"
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
# Kinds whose upstream /columns formulas already reach, as seen in Sigma's
# published examples. Any other kind -- a renamed "join", or one whose shape
# nobody has checked yet, such as a transpose -- is reported, not assumed safe.
_SINGLE_SOURCE_KINDS = frozenset({"warehouse-table", "table", "sql"})
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
# Only equality says two columns hold the same value. The others make a column a
# join participant without making it equal. The set is what Sigma's data model
# write API accepted when probed; an operator outside it is reported, so a wrong
# entry fails closed. A missing `op` is equality: Sigma's published join
# example omits it.
_EQUALITY_OPS = frozenset({"="})
_KNOWN_OPS = _EQUALITY_OPS | {"!=", "<", "<=", ">", ">=", "within", "intersects"}


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
    """An element-to-element key from a join's ON clause.

    ``left.column`` and ``right.column`` are the columns the join matches on.
    A side may transform its column (``[A] + 1``, ``Left([A], 3)``), so this is
    key participation, not value equality; ``expression`` keeps each formula.
    Under an outer join the key holds only on matched rows, so consumers score
    those edges lower. Pairs are element-to-element: a warehouse-table side is
    read (it is not drift) but not paired, and whether the consumer wants key
    edges to warehouse tables is its call. Identical predicates in different
    joins are not deduplicated.
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
    consumer should not mistake it for a complete read.
    """

    pairs: List[JoinPredicate] = field(default_factory=list)
    unions: List[UnionOutputColumn] = field(default_factory=list)
    unreadable_join_element_ids: List[str] = field(default_factory=list)
    unreadable_union_element_ids: List[str] = field(default_factory=list)
    # Elements whose own shape is not recognised: a source that is not an
    # object, a missing or blank id or kind, or a kind this parser does not
    # know. A renamed key shows up here instead of as a clean, empty index.
    unrecognised_element_count: int = 0
    # Every element seen, and those with a source. Zero of either is not a real
    # Data Model: a renamed `pages` or `source` key would otherwise read as one
    # with nothing to join.
    element_count: int = 0
    sourced_element_count: int = 0
    # Union branch refs that go through a relationship (`[Rel/Col]`). Valid
    # Sigma, but mapping one needs the relationship's target, which this module
    # does not index; counted so the lineage left behind is visible.
    union_relationship_ref_count: int = 0
    # The document's `schemaVersion`. Sigma bumps it when fields or types
    # change; this parser was written against 1, so anything else is a reason
    # to distrust the whole read.
    schema_version: Optional[int] = None


# `relationships[]` is deliberately not read as lineage. A relationship is a
# declared join that moves no data until a formula uses it as
# `[Element/Relationship/Column]`; resolving such a ref needs the relationship's
# target, which is for the resolver of those refs to index.


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


@dataclass
class _UnionRead:
    columns: List[UnionOutputColumn]
    readable: bool
    relationship_refs: int = 0


def _iter_spec_elements(spec: Dict[str, Any]) -> List[Dict[str, Any]]:
    pages = spec.get(_PAGES)
    if not isinstance(pages, list):
        return []
    elements: List[Dict[str, Any]] = []
    for page in pages:
        page_elements = page.get(_ELEMENTS) if isinstance(page, dict) else None
        if not isinstance(page_elements, list):
            continue
        elements.extend(e for e in page_elements if isinstance(e, dict))
    return elements


def _str_or_none(value: Any) -> Optional[str]:
    return value if isinstance(value, str) and value.strip() else None


def _join_side_columns(expression: str) -> Optional[Set[str]]:
    """The distinct columns a join side's formula references, or None for a
    multi-segment ref, which names a relationship or another element.

    Sides are formulas, not identifiers: ``[Col A]`` or ``Coalesce([Col A],
    -2)``. Names are compared exactly, so ``[Key]`` and ``[KEY]`` are two
    columns; Sigma's case rule is unverified, and two is the reading that
    claims less. A ``P_`` ref is a parameter, but a lone ``[P_KEY]`` would mean
    joining on a constant, so it is read as a column that starts with ``P_``.
    """
    refs = extract_bracket_refs(expression)
    if any(ref.column is not None for ref in refs):
        return None
    columns = [ref for ref in refs if not ref.is_parameter] or refs
    return {ref.source for ref in columns}


def _one_column(names: Optional[Set[str]]) -> Optional[str]:
    """A key is one distinct column, even inside a function or repeated."""
    return next(iter(names)) if names is not None and len(names) == 1 else None


def _owner(descriptor: Any) -> Optional[_Owner]:
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


def _read_join(join: Any, *, join_element_id: str) -> _JoinRead:
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
    left_owner = _owner(join.get(_LEFT))
    right_owner = _owner(join.get(_RIGHT))
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
    for entry in entries:
        if (
            not isinstance(entry, dict)
            or not isinstance(entry.get(_LEFT), str)
            or not isinstance(entry.get(_RIGHT), str)
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
        # A literal or composite side is a well-formed predicate, not a key.
        left_column = _one_column(_join_side_columns(entry[_LEFT]))
        right_column = _one_column(_join_side_columns(entry[_RIGHT]))
        if left_column is None or right_column is None:
            continue
        left = _column_ref(left_owner, entry[_LEFT], left_column)
        right = _column_ref(right_owner, entry[_RIGHT], right_column)
        if not left.element_id or not right.element_id:
            continue
        # A self-join on one column is not an edge.
        if (left.element_id, left.data_model_id, left.column) == (
            right.element_id,
            right.data_model_id,
            right.column,
        ):
            continue
        predicates.append(
            JoinPredicate(
                join_element_id=join_element_id,
                left=left,
                right=right,
                join_type=join_type,
            )
        )
    return _JoinRead(predicates=predicates, readable=readable)


def _read_union(source: Dict[str, Any], *, union_element_id: str) -> _UnionRead:
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
    owners = [_owner(branch) for branch in sources]
    readable = all(owner is not None for owner in owners)
    relationship_refs = 0

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
            refs = [r for r in extract_bracket_refs(formula) if not r.is_parameter]
            relationship_refs += sum(1 for r in refs if r.column is not None)
            owner = owners[position]
            if owner is None:
                continue
            for name in sorted({r.source for r in refs if r.column is None}):
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
        columns=columns, readable=readable, relationship_refs=relationship_refs
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

    for element in _iter_spec_elements(spec):
        index.element_count += 1
        # An element with no source (a control, a text box) has no lineage.
        if _SOURCE not in element:
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
            union = _read_union(source, union_element_id=element_id)
            index.unions.extend(union.columns)
            index.union_relationship_ref_count += union.relationship_refs
            if not union.readable:
                index.unreadable_union_element_ids.append(element_id)
        elif kind == _JOIN_KIND:
            joins = source.get(_JOINS)
            if not isinstance(joins, list):
                index.unreadable_join_element_ids.append(element_id)
                continue
            # Judged per join: one readable join must not hide a broken one.
            readable = True
            for join in joins:
                read = _read_join(join, join_element_id=element_id)
                index.pairs.extend(read.predicates)
                readable = readable and read.readable
            if not readable:
                index.unreadable_join_element_ids.append(element_id)
        elif kind not in _SINGLE_SOURCE_KINDS:
            index.unrecognised_element_count += 1
    return index
