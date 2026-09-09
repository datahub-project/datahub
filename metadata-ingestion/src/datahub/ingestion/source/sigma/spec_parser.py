import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

from datahub.ingestion.source.sigma.formula_parser import extract_bracket_refs

logger = logging.getLogger(__name__)

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
# Samples per unread source kind, per process. More than one because a kind
# whose shape VARIES between elements would look settled from a single sample,
# and reading it wrongly is what cost two runs already. Small enough to stay
# off the hot path.
_MAX_SOURCE_KIND_SAMPLES = 5
_JOIN_TYPE = "joinType"
# Join types whose ON equality holds only on matched rows.
_OUTER_JOIN_TYPES = frozenset({"left", "right", "full", "outer", "full-outer"})


@dataclass(frozen=True)
class SpecColumnRef:
    """One side of a join predicate.

    ``element_id`` is None when the side is a warehouse table rather than an
    element in this Data Model: /spec identifies those by connection and path,
    and their columns appear nowhere in the document, so nothing in this file
    can map them to a Sigma column. The caller decides whether it can.

    ``column`` is the single column the side's expression references, by DISPLAY
    NAME. ``expression`` keeps the raw text, which is a Sigma formula rather
    than a bare identifier -- see :func:`_column_from_expression`.
    """

    element_id: Optional[str]
    column: str
    expression: str = ""
    # Set when the side names an element in ANOTHER Data Model. Sigma sends
    # ``{dataModelId, elementId, groupingId, kind}`` for those; the id is kept
    # because element ids are NOT unique across models -- one tenant has the
    # same element id in two -- so the consumer cannot resolve the side without
    # knowing which model to look in.
    data_model_id: Optional[str] = None


@dataclass(frozen=True)
class UnionOutputColumn:
    """One output column of a ``union`` element and the branch columns it merges.

    A union is the multi-source case a Sigma formula cannot express: the output
    column's ``/columns`` formula names at most one branch, so every other
    branch is invisible from ``/columns`` alone. ``/spec`` states all of them.

    ``output_column`` and each branch column are kept VERBATIM. The document
    does not say whether they are column ids or display names, exactly as with
    join predicate sides, so the caller resolves them against the element's real
    ``/columns`` and can try both.
    """

    union_element_id: str
    output_column: str
    # (branch element id, that branch's column) in ``sources`` order.
    branches: Tuple[Tuple[str, str], ...]


@dataclass
class DataModelSpecIndex:
    """Join predicates read from a Data Model's /spec document.

    ``pairs`` holds ``(left, right)`` column equivalences in document order.
    Both sides are kept verbatim; resolving a side's ``column`` to a real Sigma
    column needs the element's ``/columns`` response, which this module does not
    have.
    """

    pairs: List["JoinPredicate"] = field(default_factory=list)
    unions: List[UnionOutputColumn] = field(default_factory=list)
    # ``matches[].sourceColumns`` entries that had no source at the same index.
    # Positional alignment between ``sourceColumns`` and ``sources`` is the one
    # thing the skeleton log could not prove (it can only show that the two
    # lists are the same length), so a misalignment must show up as a number
    # rather than as silently mismatched column pairs.
    union_branch_index_out_of_range: int = 0
    # Elements whose source.kind is 'join' but whose predicates could not be
    # read. Non-empty means the shape below is wrong for this tenant, and the
    # debug log holds the key skeleton needed to correct it.
    unreadable_join_element_ids: List[str] = field(default_factory=list)
    source_kind_counts: Dict[str, int] = field(default_factory=dict)
    # Samples already logged per unread source kind. Per index rather than
    # module-level: a module global survives between ingestion runs in a
    # long-lived process, so the second run logs nothing, and it makes test
    # outcomes depend on execution order.
    logged_kind_samples: Dict[str, int] = field(default_factory=dict)
    # Predicate sides that name a warehouse table rather than an element. Kept
    # as a counter because they are a real, expected shape -- not a parse
    # failure -- but cannot become element-to-element column lineage.
    warehouse_side_predicates: int = 0


@dataclass(frozen=True)
class JoinPredicate:
    """``left.column == right.column``, as stated by a join's ON clause.

    ``join_type`` is Sigma's ``joinType`` verbatim, lowercased. It matters
    because the equality is only asserted for rows the join matched: under an
    outer join the unmatched side is NULL, so the two columns are equal on a
    subset of rows rather than on all of them. Consumers score those edges
    lower rather than dropping them -- the column is still a genuine upstream.
    """

    join_element_id: str
    left: SpecColumnRef
    right: SpecColumnRef
    join_type: str = ""

    @property
    def is_outer(self) -> bool:
        return self.join_type in _OUTER_JOIN_TYPES


def _iter_spec_elements(spec: Dict[str, Any]) -> List[Dict[str, Any]]:
    elements: List[Dict[str, Any]] = []
    for page in spec.get(_PAGES) or []:
        if not isinstance(page, dict):
            continue
        for element in page.get(_ELEMENTS) or []:
            if isinstance(element, dict):
                elements.append(element)
    return elements


def key_skeleton(node: Any, depth: int = 0) -> Any:
    """Key names and container shapes only -- never values.

    Public because ``sigma_api`` logs the same kind of skeleton for lineage
    nodes it cannot read, and importing a private name across modules hides
    that this is a shared contract about what may be logged.

    A Data Model spec is customer content, so an unreadable descriptor is
    logged as structure alone. Leaf strings are described by length, which is
    enough to tell an opaque id from a display name without printing either.
    """
    if depth > 6:
        return "..."
    if isinstance(node, dict):
        return {k: key_skeleton(v, depth + 1) for k, v in sorted(node.items())}
    if isinstance(node, list):
        return [key_skeleton(node[0], depth + 1), f"...x{len(node)}"] if node else []
    if isinstance(node, str):
        return f"<str len={len(node)}>"
    return type(node).__name__


def _column_from_expression(expression: str) -> Optional[str]:
    """The one column a predicate side references, or None.

    A predicate side is a Sigma FORMULA, not a column id and not a bare column
    name -- confirmed from a live tenant, where sides read ``[Col A]``,
    ``Coalesce([Col A], -2)`` and ``[Join Key]``. Treating the raw string
    as an identifier matched nothing: 61 predicates read, 0 resolved.

    A side that references exactly one column is a key equality, whether or not
    the value is wrapped in a function -- the join still ties the two columns
    together. Zero references (a literal) or several (a composite expression)
    are not a simple key equality and are refused rather than guessed at.
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
    """Build a predicate side from a join's ``left``/``right`` descriptor.

    Observed descriptor shapes:
      * ``{elementId, groupingId, kind}`` -- an element in this Data Model
      * ``{dataModelId, elementId, groupingId, kind}`` -- an element elsewhere
      * ``{connectionId, kind, path[...]}`` -- a warehouse table, which has no
        element id and whose columns are not described anywhere in the spec

    Returns None for anything else. A descriptor must POSITIVELY identify
    itself as one of those two, because "no elementId" is not evidence of a
    warehouse table: if Sigma renames or moves the side descriptors while
    keeping ``columns[].left``/``.right``, treating the absence as a warehouse
    side would file the mismatch under an expected outcome and leave
    ``unreadable_join_element_ids`` at zero -- silencing the one signal that
    exists to catch exactly that change.
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
    # No element id: accept as a warehouse table only on a positive signal.
    if any(key in descriptor for key in _WAREHOUSE_SIDE_KEYS):
        return SpecColumnRef(element_id=None, column=resolved, expression=column)
    return None


def _predicates_for_join(
    join: Dict[str, Any],
    *,
    join_element_id: str,
    index: DataModelSpecIndex,
    data_model_id: str,
) -> Tuple[List[JoinPredicate], int]:
    """Returns (element-to-element predicates, well-formed entries examined).

    The second value is what tells "this shape is unreadable" apart from "read
    fine, but every predicate had a warehouse table on one side" -- the latter
    is the commonest real case and must not be reported as a parse failure.
    """
    out: List[JoinPredicate] = []
    understood = 0
    join_type = str(join.get(_JOIN_TYPE) or "").strip().lower()
    # The descriptor's KEY NAMES, per join, before anything is interpreted.
    # This is the line that says whether a cross-model side carries
    # ``dataModelId`` -- the consumer cannot resolve such a side without it,
    # because element ids repeat across Data Models. Key names only: a spec is
    # customer content.
    logger.debug(
        "DM SPEC JOIN SIDES %s/%s: joinType=%r left_keys=%r right_keys=%r "
        "predicate_entries=%d",
        data_model_id,
        join_element_id,
        join_type,
        sorted(join[_LEFT].keys()) if isinstance(join.get(_LEFT), dict) else None,
        sorted(join[_RIGHT].keys()) if isinstance(join.get(_RIGHT), dict) else None,
        len(join.get(_COLUMNS) or []),
    )
    for entry in join.get(_COLUMNS) or []:
        if not isinstance(entry, dict):
            continue
        left = _side_ref(join.get(_LEFT), entry.get(_LEFT))
        right = _side_ref(join.get(_RIGHT), entry.get(_RIGHT))
        if left is None or right is None:
            continue
        understood += 1
        logger.debug(
            "DM SPEC PREDICATE %s/%s: left(element=%r dm=%r column=%r) "
            "right(element=%r dm=%r column=%r) joinType=%r",
            data_model_id,
            join_element_id,
            left.element_id,
            left.data_model_id,
            left.column,
            right.element_id,
            right.data_model_id,
            right.column,
            join_type,
        )
        if left.element_id is None or right.element_id is None:
            # One side is a warehouse table. Real and expected, but it cannot
            # produce an element-to-element column edge from this document.
            index.warehouse_side_predicates += 1
            continue
        out.append(
            JoinPredicate(
                join_element_id=join_element_id,
                left=left,
                right=right,
                join_type=join_type,
            )
        )
    return out, understood


def _union_output_columns(
    source: Dict[str, Any], *, union_element_id: str, index: DataModelSpecIndex
) -> List[UnionOutputColumn]:
    """Read a ``union`` source descriptor.

    Shape, confirmed from a live tenant's spec documents (2026-09)::

        source = {"kind": "union",
                  "sources": [{"elementId": ..., "groupingId": ..., "kind": ...}],
                  "matches": [{"outputColumnName": ...,
                               "sourceColumns": [...]}]}

    ``sourceColumns`` is POSITIONAL against ``sources``: entry *i* is the column
    contributed by branch *i*. Every sample had the two lists at equal length,
    including one with 37 branches. Because that alignment is an inference and
    not something the document states, an index with no matching source is
    counted rather than matched against some other branch -- pairing the wrong
    branch's column would assert lineage that does not exist.
    """
    sources = source.get(_SOURCES)
    matches = source.get(_MATCHES)
    if not isinstance(sources, list) or not isinstance(matches, list):
        return []
    branch_element_ids: List[str] = []
    for branch in sources:
        branch_element_ids.append(
            str(branch.get(_ELEMENT_ID) or "") if isinstance(branch, dict) else ""
        )

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
            # A branch column is a FORMULA, like a join predicate side --
            # confirmed on a live tenant (2026-09) where the entries read
            # ``[Id]`` and ``[ID]``. Passing them through verbatim matched
            # nothing: 1,077 output columns read, 0 edges emitted, because
            # every lookup carried the brackets. Same mistake, same fix.
            column_name = _column_from_expression(str(column or ""))
            # A branch that contributes nothing to this output column is a
            # normal union, not a defect: Sigma sends an empty slot for it.
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


def parse_data_model_spec(
    spec: Optional[Dict[str, Any]], *, data_model_id: str
) -> DataModelSpecIndex:
    """Extract join-key column equivalences from a Data Model /spec document.

    A join's output column carries a formula naming only one side, so the other
    side's key column is unreachable from /columns alone. The ON clause in the
    spec is the only statement that the two columns hold the same value.

    Shape, confirmed from a live tenant's spec documents::

        source = {"kind": "join",
                  "primarySource": {...},
                  "joins": [{"joinType": ..., "left": {...}, "right": {...},
                             "columns": [{"left": ..., "right": ..., "op": ...}]}]}

    ``columns[].left`` / ``.right`` name a column within the corresponding
    ``left`` / ``right`` source descriptor. They are returned verbatim because
    whether they are column ids or column names cannot be settled from the
    document alone -- the caller resolves them against the element's real
    columns and can try both.
    """
    index = DataModelSpecIndex()
    if not isinstance(spec, dict):
        return index

    elements = _iter_spec_elements(spec)
    for element in elements:
        element_id = str(element.get(_ID) or "")
        source = element.get(_SOURCE)
        if not isinstance(source, dict):
            continue
        kind = str(source.get(_KIND) or "")
        index.source_kind_counts[kind] = index.source_kind_counts.get(kind, 0) + 1
        if kind == _UNION_KIND and element_id:
            outputs = _union_output_columns(
                source, union_element_id=element_id, index=index
            )
            index.unions.extend(outputs)
            if not outputs and logger.isEnabledFor(logging.DEBUG):
                logger.debug(
                    "DM SPEC UNION %s/%s: source.kind='union' but no output "
                    "column could be read. Key skeleton (structure only, no "
                    "values): %r",
                    data_model_id,
                    element_id,
                    key_skeleton(source),
                )
            else:
                logger.debug(
                    "DM SPEC UNION %s/%s: %d output column(s), %d branch "
                    "reference(s) across %d branch element(s)",
                    data_model_id,
                    element_id,
                    len(outputs),
                    sum(len(o.branches) for o in outputs),
                    len({eid for o in outputs for eid, _ in o.branches}),
                )
            continue
        if kind != _JOIN_KIND or not element_id:
            # Every remaining kind is single-source, so /columns formulas
            # already reach its upstream. Logged anyway because a kind whose
            # shape this parser has never seen is exactly what cost two runs;
            # a few samples per kind keeps that off the hot path.
            seen = index.logged_kind_samples.get(kind, 0)
            if (
                kind
                and element_id
                and seen < _MAX_SOURCE_KIND_SAMPLES
                and logger.isEnabledFor(logging.DEBUG)
            ):
                index.logged_kind_samples[kind] = seen + 1
                logger.debug(
                    "DM SPEC SOURCE KIND %s/%s: kind=%r not read by this "
                    "parser (sample %d of %d). If lineage is missing for this "
                    "element, the shape below says what a reader would need. "
                    "Key skeleton (structure only, no values): %r",
                    data_model_id,
                    element_id,
                    kind,
                    seen + 1,
                    _MAX_SOURCE_KIND_SAMPLES,
                    key_skeleton(source),
                )
            continue
        joins = source.get(_JOINS)
        found: List[JoinPredicate] = []
        understood = 0
        if isinstance(joins, list):
            for join in joins:
                if isinstance(join, dict):
                    predicates, seen = _predicates_for_join(
                        join,
                        join_element_id=element_id,
                        index=index,
                        data_model_id=data_model_id,
                    )
                    found.extend(predicates)
                    understood += seen
        index.pairs.extend(found)
        # Readable when the descriptor parsed at all: an empty ``joins`` list is
        # a join element with nothing to read, and a join whose every predicate
        # names a warehouse table was understood perfectly well.
        if not (isinstance(joins, list) and (joins == [] or understood)):
            index.unreadable_join_element_ids.append(element_id)
            # ``_key_skeleton`` walks the whole descriptor, so it must not run
            # as an unguarded logger.debug argument.
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(
                    "DM SPEC JOIN %s/%s: source.kind=%r but no element-to-element "
                    "predicate could be read. Key skeleton (structure only, no "
                    "values): %r",
                    data_model_id,
                    element_id,
                    kind,
                    key_skeleton(source),
                )

    logger.debug(
        "DM SPEC %s: %d element(s), source kinds=%r, "
        "%d join predicate(s), %d warehouse-side predicate(s) skipped, "
        "%d unreadable join element(s), %d union output column(s), "
        "%d union branch index(es) out of range",
        data_model_id,
        len(elements),
        index.source_kind_counts,
        len(index.pairs),
        index.warehouse_side_predicates,
        len(index.unreadable_join_element_ids),
        len(index.unions),
        index.union_branch_index_out_of_range,
    )
    return index
