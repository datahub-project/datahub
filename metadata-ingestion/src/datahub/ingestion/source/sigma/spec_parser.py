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
# Joins whose ON equality holds only on matched rows. Sigma's write API accepts
# only these join types; `lookup` is normalised to `left-outer` on store, and is
# kept in case a spec carries it anyway.
_OUTER_JOIN_TYPES = frozenset({"left-outer", "right-outer", "full-outer", "lookup"})
# `joinType` is optional and defaults to inner.
_INNER_JOIN_TYPE = "inner"
_JOIN_OP = "op"
# Only equality says two columns hold the same value; `<`, `!=`, `within` and
# the rest make a column a join participant without making it equal.
_EQUALITY_OPS = frozenset({"=", ""})


@dataclass(frozen=True)
class SpecColumnRef:
    """One side of a join predicate.

    ``element_id`` is None for a warehouse-table side: its columns appear nowhere
    in the spec, so the caller decides whether it can map them. ``column`` is
    the display name the side's formula references; ``expression`` is the raw
    formula.
    """

    element_id: Optional[str]
    column: str
    expression: str = ""
    # Set when the side is an element in another Data Model. Element ids are not
    # unique across models, so the side cannot be resolved without it.
    data_model_id: Optional[str] = None


@dataclass(frozen=True)
class UnionOutputColumn:
    """A ``union`` element's output column and the branch columns it merges.

    A union output's ``/columns`` formula names at most one branch; the spec
    names all of them. Column names are kept verbatim for the caller to resolve.
    """

    union_element_id: str
    output_column: str
    # (branch element id, that branch's column), in ``sources`` order.
    branches: Tuple[Tuple[str, str], ...]


@dataclass(frozen=True)
class JoinPredicate:
    """``left.column == right.column``, from a join's ON clause.

    Under an outer join the equality holds only on matched rows, so consumers
    score those edges lower rather than dropping them.
    """

    join_element_id: str
    left: SpecColumnRef
    right: SpecColumnRef
    join_type: str = ""

    @property
    def is_outer(self) -> bool:
        return self.join_type in _OUTER_JOIN_TYPES


@dataclass
class DataModelSpecIndex:
    """Join predicates and union columns read from a Data Model's /spec."""

    pairs: List[JoinPredicate] = field(default_factory=list)
    unions: List[UnionOutputColumn] = field(default_factory=list)
    # Join elements whose predicates could not be read. Non-empty means the
    # parser's shape assumptions are wrong for this spec.
    unreadable_join_element_ids: List[str] = field(default_factory=list)
    # Union ``sourceColumns`` entries with no source at the same index. The
    # positional alignment is inferred, not stated, so a mismatch is counted
    # rather than paired with the wrong branch.
    union_branch_index_out_of_range: int = 0


# `relationships[]` is deliberately not read. A relationship is a declared,
# unused join: declaring one leaves both elements' /lineage unchanged, so an
# edge from it would assert a derivation that does not exist. Using one goes
# through a lookup join, which _predicates_for_join already reads.


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

    Predicate sides and union branch columns are formulas, not identifiers:
    ``[Col A]`` or ``Coalesce([Col A], -2)``. One reference is a key equality
    even inside a function; none (a literal) or several is refused.
    """
    refs = [
        ref
        for ref in extract_bracket_refs(expression)
        if not ref.is_parameter and ref.column is None
    ]
    if len(refs) != 1:
        return None
    return refs[0].source


def _side_ref(descriptor: Any, column: Any) -> Optional[SpecColumnRef]:
    """A predicate side from a join's ``left``/``right`` descriptor.

    Descriptor shapes: ``{elementId, ...}`` for an element in this Data Model,
    ``{dataModelId, elementId, ...}`` for one in another, and
    ``{connectionId, path, ...}`` for a warehouse table. A missing elementId is
    not evidence of a warehouse table: if Sigma renamed the descriptors, that
    reading would hide the change instead of reporting it unreadable.
    """
    if not isinstance(column, str) or not column:
        return None
    if not isinstance(descriptor, dict):
        return None
    resolved = _column_from_expression(column)
    if resolved is None:
        return None
    raw = descriptor.get(_ELEMENT_ID)
    if isinstance(raw, str) and raw:
        foreign = descriptor.get(_DATA_MODEL_ID)
        return SpecColumnRef(
            element_id=raw,
            column=resolved,
            expression=column,
            data_model_id=foreign if isinstance(foreign, str) and foreign else None,
        )
    if any(key in descriptor for key in _WAREHOUSE_SIDE_KEYS):
        return SpecColumnRef(element_id=None, column=resolved, expression=column)
    return None


@dataclass
class _JoinRead:
    predicates: List[JoinPredicate]
    # Well-formed predicate entries seen, including ones with a warehouse side.
    # Separates "shape unreadable" from "read fine, nothing element-to-element".
    understood: int


def _predicates_for_join(join: Dict[str, Any], *, join_element_id: str) -> _JoinRead:
    predicates: List[JoinPredicate] = []
    understood = 0
    join_type = str(join.get(_JOIN_TYPE) or _INNER_JOIN_TYPE).strip().lower()
    for entry in join.get(_COLUMNS) or []:
        if not isinstance(entry, dict):
            continue
        left = _side_ref(join.get(_LEFT), entry.get(_LEFT))
        right = _side_ref(join.get(_RIGHT), entry.get(_RIGHT))
        if left is None or right is None:
            continue
        understood += 1
        if str(entry.get(_JOIN_OP) or "").strip() not in _EQUALITY_OPS:
            continue
        if left.element_id is None or right.element_id is None:
            continue
        predicates.append(
            JoinPredicate(
                join_element_id=join_element_id,
                left=left,
                right=right,
                join_type=join_type,
            )
        )
    return _JoinRead(predicates=predicates, understood=understood)


def _union_output_columns(
    source: Dict[str, Any], *, union_element_id: str, index: DataModelSpecIndex
) -> List[UnionOutputColumn]:
    """Read a ``union`` source.

    Shape: ``{"kind": "union", "sources": [{"elementId": ...}],
    "matches": [{"outputColumnName": ..., "sourceColumns": [...]}]}``, where
    ``sourceColumns[i]`` is the column branch ``sources[i]`` contributes.
    """
    sources = source.get(_SOURCES)
    matches = source.get(_MATCHES)
    if not isinstance(sources, list) or not isinstance(matches, list):
        return []
    branch_element_ids = [
        str(branch.get(_ELEMENT_ID) or "") if isinstance(branch, dict) else ""
        for branch in sources
    ]

    outputs: List[UnionOutputColumn] = []
    for match in matches:
        if not isinstance(match, dict):
            continue
        output_column = str(match.get(_OUTPUT_COLUMN_NAME) or "")
        source_columns = match.get(_SOURCE_COLUMNS)
        if not output_column or not isinstance(source_columns, list):
            continue
        branches: List[Tuple[str, str]] = []
        for position, column in enumerate(source_columns):
            if position >= len(branch_element_ids):
                index.union_branch_index_out_of_range += 1
                continue
            element_id = branch_element_ids[position]
            column_name = _column_from_expression(str(column or ""))
            # An empty slot is a branch contributing nothing to this column.
            if element_id and column_name:
                branches.append((element_id, column_name))
        if branches:
            outputs.append(
                UnionOutputColumn(
                    union_element_id=union_element_id,
                    output_column=output_column,
                    branches=tuple(branches),
                )
            )
    return outputs


def parse_data_model_spec(spec: Optional[Dict[str, Any]]) -> DataModelSpecIndex:
    """Join-key equivalences and union branches from a Data Model /spec.

    A join's output column names only one side in its formula, so the other
    side's key column is reachable only from the ON clause. Shape::

        source = {"kind": "join",
                  "primarySource": {...},
                  "joins": [{"joinType": ..., "left": {...}, "right": {...},
                             "columns": [{"left": ..., "right": ..., "op": ...}]}]}
    """
    index = DataModelSpecIndex()
    if not isinstance(spec, dict):
        return index

    for element in _iter_spec_elements(spec):
        element_id = str(element.get(_ID) or "")
        source = element.get(_SOURCE)
        if not isinstance(source, dict) or not element_id:
            continue
        kind = str(source.get(_KIND) or "")
        if kind == _UNION_KIND:
            index.unions.extend(
                _union_output_columns(source, union_element_id=element_id, index=index)
            )
            continue
        if kind != _JOIN_KIND:
            # Single-source kinds: /columns formulas already reach the upstream.
            continue
        joins = source.get(_JOINS)
        understood = 0
        if isinstance(joins, list):
            for join in joins:
                if isinstance(join, dict):
                    read = _predicates_for_join(join, join_element_id=element_id)
                    index.pairs.extend(read.predicates)
                    understood += read.understood
        # An empty `joins` list has nothing to read, and a join whose every
        # predicate names a warehouse table was read correctly.
        if not (isinstance(joins, list) and (joins == [] or understood)):
            index.unreadable_join_element_ids.append(element_id)
    return index
