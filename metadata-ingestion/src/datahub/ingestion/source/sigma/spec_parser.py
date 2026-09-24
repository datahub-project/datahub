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
# A side with no elementId is a warehouse table only if it says so.
_WAREHOUSE_SIDE_KEYS = ("connectionId", "path")
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
# `left-outer` on store, and is kept in case a spec carries it anyway. Anything
# else is a shape this parser does not know, so the join is reported, not
# scored.
_OUTER_JOIN_TYPES = frozenset({"left-outer", "right-outer", "full-outer", "lookup"})
_KNOWN_JOIN_TYPES = _OUTER_JOIN_TYPES | {_INNER_JOIN_TYPE}
# Only equality says two columns hold the same value. The other operators Sigma
# accepts make a column a join participant without making it equal; an
# operator outside both sets is reported.
_EQUALITY_OPS = frozenset({"=", ""})
_KNOWN_OPS = _EQUALITY_OPS | {"!=", "<", "<=", ">", ">=", "within", "intersects"}


@dataclass(frozen=True)
class SpecColumnRef:
    """A column named by a join side or a union branch.

    ``element_id`` is None for a warehouse table: its columns appear nowhere in
    the spec, so the caller decides whether it can map them. ``column`` is the
    display name the formula references; ``expression`` is the raw formula.
    """

    element_id: Optional[str]
    column: str
    expression: str = ""
    # Set for an element in another Data Model. Element ids are not unique
    # across models, so the column cannot be resolved without it.
    data_model_id: Optional[str] = None


@dataclass(frozen=True)
class UnionOutputColumn:
    """A ``union`` element's output column and the branch columns it merges.

    A union output's ``/columns`` formula names at most one branch; the spec
    names all of them.
    """

    union_element_id: str
    output_column: str
    # One per contributing branch, in ``sources`` order.
    branches: Tuple[SpecColumnRef, ...]


@dataclass(frozen=True)
class JoinPredicate:
    """``left.column == right.column``, from a join's ON clause.

    Under an outer join the equality holds only on matched rows, so consumers
    score those edges lower rather than dropping them.
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

    An element whose shape does not match what this parser expects is listed as
    unreadable, even when part of it was read: partial drift is the likeliest
    kind, and a consumer should not mistake it for a complete read.
    """

    pairs: List[JoinPredicate] = field(default_factory=list)
    unions: List[UnionOutputColumn] = field(default_factory=list)
    unreadable_join_element_ids: List[str] = field(default_factory=list)
    unreadable_union_element_ids: List[str] = field(default_factory=list)


# `relationships[]` is deliberately not read as lineage. A relationship is a
# declared join that moves no data until a formula uses it as
# `[Element/Relationship/Column]`; resolving such a ref needs the relationship's
# target, which is for the resolver of those refs to index.


@dataclass(frozen=True)
class _Owner:
    """Whose column a join side or union branch names."""

    element_id: Optional[str]
    data_model_id: Optional[str] = None


@dataclass
class _JoinRead:
    predicates: List[JoinPredicate]
    readable: bool


@dataclass
class _UnionRead:
    columns: List[UnionOutputColumn]
    readable: bool


def _iter_spec_elements(spec: Dict[str, Any]) -> List[Dict[str, Any]]:
    elements: List[Dict[str, Any]] = []
    for page in spec.get(_PAGES) or []:
        if not isinstance(page, dict):
            continue
        for element in page.get(_ELEMENTS) or []:
            if isinstance(element, dict):
                elements.append(element)
    return elements


def _column_from_expression(expression: str) -> Optional[str]:
    """The one column a formula references, or None.

    Join sides and union branch columns are formulas, not identifiers:
    ``[Col A]`` or ``Coalesce([Col A], -2)``. One distinct column is a key even
    inside a function, and repeating it (``If(IsNull([K]), -1, [K])``) is still
    one. Two columns, a multi-segment ref, or none is refused. Parameters count
    as constants, like literals.
    """
    refs = [ref for ref in extract_bracket_refs(expression) if not ref.is_parameter]
    if any(ref.column is not None for ref in refs):
        return None
    names = {ref.source for ref in refs}
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
    element_id = descriptor.get(_ELEMENT_ID)
    if isinstance(element_id, str) and element_id:
        foreign = descriptor.get(_DATA_MODEL_ID)
        return _Owner(
            element_id=element_id,
            data_model_id=foreign if isinstance(foreign, str) and foreign else None,
        )
    if any(key in descriptor for key in _WAREHOUSE_SIDE_KEYS):
        return _Owner(element_id=None)
    return None


def _column_ref(owner: _Owner, formula: str) -> Optional[SpecColumnRef]:
    column = _column_from_expression(formula)
    if column is None:
        return None
    return SpecColumnRef(
        element_id=owner.element_id,
        column=column,
        expression=formula,
        data_model_id=owner.data_model_id,
    )


def _read_join(join: Any, *, join_element_id: str) -> _JoinRead:
    """One entry of a join element's ``joins``.

    Readability is decided on SHAPE, before any formula is resolved: a literal
    or composite side is a well-formed predicate that is simply not a key.
    """
    if not isinstance(join, dict):
        return _JoinRead(predicates=[], readable=False)
    join_type = str(join.get(_JOIN_TYPE) or _INNER_JOIN_TYPE).strip().lower()
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
        op = entry.get(_JOIN_OP) or ""
        if not isinstance(op, str) or op.strip() not in _KNOWN_OPS:
            readable = False
            continue
        if op.strip() not in _EQUALITY_OPS:
            continue
        left = _column_ref(left_owner, entry[_LEFT])
        right = _column_ref(right_owner, entry[_RIGHT])
        # A warehouse side is real, but cannot yield an element-to-element edge.
        if left is None or right is None or not left.element_id or not right.element_id:
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
            ref = _column_ref(owner, formula) if owner is not None else None
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
        element_id = str(element.get(_ID) or "")
        source = element.get(_SOURCE)
        if not isinstance(source, dict) or not element_id:
            continue
        kind = str(source.get(_KIND) or "").strip().lower()
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
