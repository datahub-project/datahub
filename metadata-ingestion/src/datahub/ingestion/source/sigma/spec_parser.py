from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

from datahub.ingestion.source.sigma.formula_parser import extract_bracket_refs

_PAGES = "pages"
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
# A side with no elementId is a warehouse table only if it says so.
_WAREHOUSE_SIDE_KEYS = (_CONNECTION_ID, _PATH)
_JOIN_KIND = "join"
_UNION_KIND = "union"
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
    # One per contributing branch, in ``sources`` order; see ``source_index``.
    branches: Tuple[SpecColumnRef, ...]


@dataclass(frozen=True)
class JoinPredicate:
    """An element-to-element key from a join's ON clause.

    ``left.column`` and ``right.column`` are the columns the join matches on.
    A side may transform its column (``[A] + 1``, ``Left([A], 3)``), so this is
    key participation, not value equality; ``expression`` keeps each formula.
    Under an outer join the key holds only on matched rows, so consumers score
    those edges lower. Warehouse-table sides are not pairs.
    """

    join_element_id: str
    left: SpecColumnRef
    right: SpecColumnRef
    join_type: str = _INNER_JOIN_TYPE

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
    # Elements with a source but no string id, or a source with no string
    # kind. Every element Sigma stores has both, so a renamed key shows up here
    # instead of as an empty, clean-looking index.
    unrecognised_element_count: int = 0


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
    return value if isinstance(value, str) and value else None


def _column_from_expression(expression: str) -> Optional[str]:
    """The one column a formula references, or None.

    Join sides and union branch columns are formulas, not identifiers:
    ``[Col A]`` or ``Coalesce([Col A], -2)``. One distinct column is a key even
    inside a function, and repeating it (``If(IsNull([K]), -1, [K])``) is still
    one. Two columns, a multi-segment ref, or none is refused.

    A ``P_`` ref is a parameter only when a column ref sits beside it: a side
    whose only ref is ``[P_KEY]`` would be joining on a constant, so it is read
    as a column that happens to start with ``P_``.
    """
    refs = extract_bracket_refs(expression)
    if any(ref.column is not None for ref in refs):
        return None
    columns = [ref for ref in refs if not ref.is_parameter] or refs
    names = {ref.source for ref in columns}
    return names.pop() if len(names) == 1 else None


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
        return _Owner(
            element_id=element_id,
            data_model_id=_str_or_none(descriptor.get(_DATA_MODEL_ID)),
        )
    if any(key in descriptor for key in _WAREHOUSE_SIDE_KEYS):
        path = descriptor.get(_PATH)
        return _Owner(
            element_id=None,
            connection_id=_str_or_none(descriptor.get(_CONNECTION_ID)),
            path=(
                tuple(path)
                if isinstance(path, list) and all(isinstance(p, str) for p in path)
                else ()
            ),
        )
    return None


def _column_ref(
    owner: _Owner, formula: str, *, source_index: Optional[int] = None
) -> Optional[SpecColumnRef]:
    column = _column_from_expression(formula)
    if column is None:
        return None
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
        left = _column_ref(left_owner, entry[_LEFT])
        right = _column_ref(right_owner, entry[_RIGHT])
        if left is None or right is None or not left.element_id or not right.element_id:
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
            owner = owners[position]
            ref = (
                _column_ref(owner, formula, source_index=position)
                if owner is not None
                else None
            )
            if ref is not None:
                branches.append(ref)
        if branches:
            columns.append(
                UnionOutputColumn(
                    union_element_id=union_element_id,
                    output_column=output_column,
                    branches=tuple(branches),
                )
            )
    return _UnionRead(columns=columns, readable=readable)


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

    for element in _iter_spec_elements(spec):
        source = element.get(_SOURCE)
        # An element with no source (a control, a text box) has no lineage.
        if not isinstance(source, dict):
            continue
        element_id = _str_or_none(element.get(_ID))
        raw_kind = source.get(_KIND)
        if element_id is None or not isinstance(raw_kind, str) or not raw_kind:
            index.unrecognised_element_count += 1
            continue
        kind = raw_kind.strip().lower()
        if kind == _UNION_KIND:
            union = _read_union(source, union_element_id=element_id)
            index.unions.extend(union.columns)
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
        # Every other kind is single-source: /columns formulas already reach it.
    return index
