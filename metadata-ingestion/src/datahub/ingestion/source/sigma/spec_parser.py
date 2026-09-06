import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set, Tuple

logger = logging.getLogger(__name__)

_PAGES = "pages"
_ELEMENTS = "elements"
_COLUMNS = "columns"
_SOURCE = "source"
_KIND = "kind"
_ID = "id"
_JOIN_KIND = "join"

# A join predicate equates two columns, so a descriptor that yields any other
# count is not a key pair and is left alone rather than guessed at.
_COLUMNS_PER_JOIN_PREDICATE = 2


@dataclass(frozen=True)
class SpecColumnRef:
    """A column of a Data Model element, as the /spec document identifies it."""

    element_id: str
    column_id: str


@dataclass
class DataModelSpecIndex:
    """What a Data Model's /spec document says about its joins.

    ``element_id_by_column_id`` covers every column in the document; it is what
    makes the join parsing self-validating, since a string that is a known
    column id cannot be mistaken for a label or an opaque handle.

    ``partners`` is the symmetric closure of the join predicates: a column maps
    to every column a join equates it with. Symmetric because a predicate says
    the two values are the same, without direction.
    """

    element_id_by_column_id: Dict[str, str] = field(default_factory=dict)
    partners: Dict[SpecColumnRef, Set[SpecColumnRef]] = field(default_factory=dict)
    # Elements whose source.kind is 'join' but whose predicate could not be
    # read. Non-empty means the shape assumption below is wrong for this tenant
    # and the debug log holds the skeleton needed to correct it.
    unreadable_join_element_ids: List[str] = field(default_factory=list)
    # Every source.kind seen, with counts -- the cheapest way to learn the real
    # vocabulary of a tenant without dumping any of its content.
    source_kind_counts: Dict[str, int] = field(default_factory=dict)

    def partners_of(self, element_id: str, column_id: str) -> Set[SpecColumnRef]:
        return self.partners.get(SpecColumnRef(element_id, column_id), set())


def _iter_spec_elements(spec: Dict[str, Any]) -> List[Dict[str, Any]]:
    elements: List[Dict[str, Any]] = []
    for page in spec.get(_PAGES) or []:
        if not isinstance(page, dict):
            continue
        for element in page.get(_ELEMENTS) or []:
            if isinstance(element, dict):
                elements.append(element)
    return elements


def _key_skeleton(node: Any, depth: int = 0) -> Any:
    """Key names and container types only -- never values.

    Used to log the shape of a join descriptor this parser could not read. A
    Data Model spec is customer content, so nothing but structure is logged.
    """
    if depth > 4:
        return "..."
    if isinstance(node, dict):
        return {k: _key_skeleton(v, depth + 1) for k, v in sorted(node.items())}
    if isinstance(node, list):
        return [_key_skeleton(node[0], depth + 1), f"...x{len(node)}"] if node else []
    return type(node).__name__


def _known_column_ids_in(node: Any, known: Dict[str, str]) -> List[str]:
    """Every string anywhere under ``node`` that is a column id of this spec.

    Sigma documents the join predicate as ``source.columns[].left`` /
    ``.right``, but that spelling is unverified against a live tenant. Rather
    than trust key names, this collects the strings that are *provably* column
    ids of this same document -- an anchor no naming change can break.
    """
    found: List[str] = []
    if isinstance(node, str):
        if node in known:
            found.append(node)
    elif isinstance(node, dict):
        for value in node.values():
            found.extend(_known_column_ids_in(value, known))
    elif isinstance(node, list):
        for value in node:
            found.extend(_known_column_ids_in(value, known))
    return found


def _predicate_lists(source: Dict[str, Any]) -> List[Tuple[str, List[Any]]]:
    """Candidate lists of join predicates inside a join source descriptor.

    The documented key is ``columns``; it is tried first and any other list of
    objects is tried after, so a renamed field still resolves. Each candidate is
    validated by column-id content before use, so a wrong guess yields nothing
    rather than a wrong edge.
    """
    ordered: List[Tuple[str, List[Any]]] = []
    documented = source.get(_COLUMNS)
    if isinstance(documented, list):
        ordered.append((_COLUMNS, documented))
    for key, value in sorted(source.items()):
        if key != _COLUMNS and isinstance(value, list):
            ordered.append((key, value))
    return ordered


def _pairs_from_source(
    *, element_id: str, source: Dict[str, Any], known: Dict[str, str]
) -> List[Tuple[SpecColumnRef, SpecColumnRef]]:
    for key, candidates in _predicate_lists(source):
        pairs: List[Tuple[SpecColumnRef, SpecColumnRef]] = []
        for candidate in candidates:
            ids = _known_column_ids_in(candidate, known)
            # Deduplicate while preserving order: a descriptor may repeat the
            # same id in a label field alongside the reference itself.
            unique = list(dict.fromkeys(ids))
            if len(unique) != _COLUMNS_PER_JOIN_PREDICATE:
                pairs = []
                break
            left, right = unique
            pairs.append(
                (
                    SpecColumnRef(known[left], left),
                    SpecColumnRef(known[right], right),
                )
            )
        if pairs:
            logger.debug(
                "DM SPEC JOIN %s: read %d key pair(s) from source[%r]",
                element_id,
                len(pairs),
                key,
            )
            return pairs
    return []


def parse_data_model_spec(
    spec: Optional[Dict[str, Any]], *, data_model_id: str
) -> DataModelSpecIndex:
    """Extract join-key column equivalences from a Data Model /spec document.

    A join's output column carries a formula naming only one side, so the other
    side's key column is unreachable from /columns alone. The predicate in the
    spec is the only statement that the two columns hold the same value.
    """
    index = DataModelSpecIndex()
    if not isinstance(spec, dict):
        return index

    elements = _iter_spec_elements(spec)
    for element in elements:
        element_id = str(element.get(_ID) or "")
        if not element_id:
            continue
        for column in element.get(_COLUMNS) or []:
            if isinstance(column, dict):
                column_id = str(column.get(_ID) or "")
                if column_id:
                    index.element_id_by_column_id[column_id] = element_id

    for element in elements:
        element_id = str(element.get(_ID) or "")
        source = element.get(_SOURCE)
        if not isinstance(source, dict):
            continue
        kind = str(source.get(_KIND) or "")
        index.source_kind_counts[kind] = index.source_kind_counts.get(kind, 0) + 1
        if kind != _JOIN_KIND or not element_id:
            continue
        pairs = _pairs_from_source(
            element_id=element_id, source=source, known=index.element_id_by_column_id
        )
        if not pairs:
            index.unreadable_join_element_ids.append(element_id)
            logger.debug(
                "DM SPEC JOIN %s/%s: source.kind=%r but no predicate list "
                "yielded exactly %d known column ids per entry. Key skeleton "
                "(structure only, no values): %r",
                data_model_id,
                element_id,
                kind,
                _COLUMNS_PER_JOIN_PREDICATE,
                _key_skeleton(source),
            )
            continue
        for left, right in pairs:
            index.partners.setdefault(left, set()).add(right)
            index.partners.setdefault(right, set()).add(left)

    logger.debug(
        "DM SPEC %s: %d element(s), %d column id(s), source kinds=%r, "
        "%d join key pair(s) over %d column(s), %d unreadable join element(s)",
        data_model_id,
        len(elements),
        len(index.element_id_by_column_id),
        index.source_kind_counts,
        sum(len(v) for v in index.partners.values()) // 2,
        len(index.partners),
        len(index.unreadable_join_element_ids),
    )
    return index
