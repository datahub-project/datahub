"""CSN-based lineage extraction for SAP Datasphere.

Walks the CSN (Core Schema Notation) returned by the Datasphere Repository API and
extracts:
  - Upstream object names from `query.SELECT.from.ref` (single table) or
    `query.SELECT.from.join.args[*]` (joins).
  - The connection name (`@remote.source`) for federated remote tables.

Scope of v1: plain SELECT and INNER/LEFT/RIGHT joins of named refs.
Out of scope (handled by later iterations): unions, CTEs, deeply nested subqueries,
column-level lineage. Falling back to "no lineage extracted" for cases we can't
parse is acceptable — the connector still emits the asset without an upstream edge.
"""

from dataclasses import dataclass
from typing import (
    Dict,
    Final,
    Iterable,
    List,
    Literal,
    NamedTuple,
    Optional,
    Set,
    Tuple,
    TypeVar,
)

_T = TypeVar("_T")


def _dedup_preserving_order(items: Iterable[_T]) -> List[_T]:
    """Return items with duplicates removed, preserving first-seen order."""
    seen: set = set()
    out: List[_T] = []
    for item in items:
        if item not in seen:
            seen.add(item)
            out.append(item)
    return out


def _sanitize_for_report(value: object) -> str:
    """Render ``repr(value)`` with embedded newlines escaped, so report entries
    stay on a single line for operator log scanning."""
    return repr(value).replace("\n", "\\n").replace("\r", "\\r")


class UpstreamColRef(NamedTuple):
    """A reference to a single upstream column.

    Named tuple so existing call sites that unpack ``for qname, col in refs``
    continue to work, while giving call sites a self-documenting type.
    """

    qname: str
    col: str


# The set of transform-operation labels emitted on FineGrainedLineage. Mirrors
# the strings DataHub UI recognises for its transformation badge.
TransformOp = Literal["IDENTITY", "RENAME", "AGGREGATE", "TRANSFORMATION", "EXPRESSION"]


# Sentinel values stuffed into ColumnLineagePair.downstream_col when the CSN is
# structurally broken (`_MALFORMED_COL_SENTINEL`) or the expression has refs but
# no derivable name (`_UNNAMED_COL_SENTINEL`). The source layer recognizes these
# and routes them to `report.column_lineage_unresolved` instead of emitting a
# schemaField URN. ``Final[str]`` so mypy treats them as compile-time constants.
_MALFORMED_COL_SENTINEL: Final[str] = "<malformed>"
_UNNAMED_COL_SENTINEL: Final[str] = "<unnamed>"


@dataclass(frozen=True)
class ColumnLineagePair:
    """A single downstream column with the upstream column references that feed it.

    Resolution from CSN qualified-names to DataHub URNs is the source layer's job —
    this extractor only knows CSN-internal qualified names.

    Attributes:
        downstream_col: Output column name (the `as` alias if present, else the
            last segment of the column's `ref`). May be one of the sentinel
            values ``<malformed>`` / ``<unnamed>`` for structurally-broken or
            unnamed expressions.
        upstream_refs: Tuple of ``UpstreamColRef(qname, col)`` entries. Empty
            for pure-literal columns (those are not emitted as lineage pairs).
            Stored as a tuple so the frozen dataclass is truly immutable.
        transform_op: Transformation operation classification — one of
            "IDENTITY", "RENAME", "AGGREGATE", "TRANSFORMATION", "EXPRESSION".
            Populated to drive DataHub UI's transformation badge. None if unknown.
        unresolved_refs: Diagnostic strings for column references the walker
            could not attribute to an upstream (e.g. unknown alias, 3-segment
            ref, unqualified ref under a multi-source JOIN). Caller surfaces
            these via ``report.column_lineage_unresolved`` so operators can
            distinguish silent walker drops from legitimate base-table cases.
    """

    downstream_col: str  # may be _MALFORMED_COL_SENTINEL or _UNNAMED_COL_SENTINEL
    upstream_refs: Tuple[UpstreamColRef, ...] = ()
    transform_op: Optional[TransformOp] = None
    unresolved_refs: Tuple[str, ...] = ()

    def __post_init__(self) -> None:
        # Invariant: sentinel-valued pairs MUST carry no upstream refs.
        # Otherwise the source layer would build a schemaField URN containing
        # the sentinel string (e.g. urn:li:schemaField:(...,<malformed>)).
        if self.downstream_col in (_MALFORMED_COL_SENTINEL, _UNNAMED_COL_SENTINEL):
            if self.upstream_refs:
                raise ValueError(
                    f"Sentinel ColumnLineagePair must have empty upstream_refs; "
                    f"got downstream_col={self.downstream_col!r}, "
                    f"upstream_refs={self.upstream_refs!r}"
                )


@dataclass(frozen=True)
class ColumnLineageContext:
    """Bundles the column-lineage state passed to ``_build_upstream_lineage``.

    The two pieces of state — the list of column-lineage pairs and the
    downstream dataset URN — are logically coupled (you need the URN to build
    field URNs for the downstream side of every pair). Bundling them in a
    single value object keeps callers from passing inconsistent combinations
    (e.g. pairs without a downstream URN, or vice versa).
    """

    pairs: Tuple[ColumnLineagePair, ...]
    downstream_dataset_urn: str


class CsnLineageExtractor:
    _AGGREGATE_FUNCS = frozenset(
        {
            "SUM",
            "COUNT",
            "AVG",
            "MIN",
            "MAX",
            "STDDEV",
            "VARIANCE",
            "MEDIAN",
            "FIRST_VALUE",
            "LAST_VALUE",
        }
    )

    def _malformed_pair(self, reason: str) -> "ColumnLineagePair":
        """Construct the synthetic pair we return when CSN is structurally broken.

        The pair's empty ``upstream_refs`` ensures the source layer never builds
        a schemaField URN containing the ``<malformed>`` sentinel — but it DOES
        surface the diagnostic via ``report.column_lineage_unresolved``.
        """
        return ColumnLineagePair(
            downstream_col=_MALFORMED_COL_SENTINEL,
            upstream_refs=(),
            transform_op=None,
            unresolved_refs=(reason,),
        )

    def _safe_select(
        self, csn_def: dict
    ) -> Tuple[Optional[dict], Optional["ColumnLineagePair"]]:
        """Validate the CSN envelope and return either the inner SELECT dict
        (when structurally sound) or a synthetic malformed pair (when broken).
        Returns ``(None, None)`` for legitimate non-SELECT entities like base
        tables.

        Returns:
            ``(select_dict, None)``    — proceed to columns walk
            ``(None, malformed_pair)`` — caller should return [malformed_pair] immediately
            ``(None, None)``           — legitimate base table; caller returns []
        """
        if not isinstance(csn_def, dict):
            return None, None
        query = csn_def.get("query")
        if query is None:
            return None, None  # legitimate non-SELECT entity
        if not isinstance(query, dict):
            return None, self._malformed_pair(
                f"query is {type(query).__name__}, expected dict"
            )
        select = query.get("SELECT")
        if select is None:
            return None, None  # legitimate non-SELECT entity
        if not isinstance(select, dict):
            return None, self._malformed_pair(
                f"SELECT is {type(select).__name__}, expected dict"
            )
        return select, None

    def _resolve_output_name(self, col: dict) -> Optional[str]:
        """Return the downstream column name for a `columns[]` entry.

        Prefers `as` alias; falls back to the last segment of `ref`. Returns None
        when the expression has neither — e.g. an unnamed aggregate (`SUM(x)`)
        cannot produce lineage without an alias, so it's skipped.
        """
        as_alias = col.get("as")
        if isinstance(as_alias, str):
            return as_alias
        ref = col.get("ref")
        if isinstance(ref, list) and ref:
            last = ref[-1]
            if isinstance(last, str):
                return last
        return None

    def _infer_transform(self, col: dict) -> Optional[TransformOp]:
        """Classify the column expression for the FineGrainedLineage transformOperation
        field. Drives DataHub UI's transform badge.
        """
        if "func" in col:
            func = col["func"]
            if isinstance(func, str):
                if func.upper() in self._AGGREGATE_FUNCS:
                    return "AGGREGATE"
                return "TRANSFORMATION"
        if "xpr" in col:
            return "EXPRESSION"
        if "as" in col and "ref" in col:
            return "RENAME"
        if "ref" in col:
            return "IDENTITY"
        return None

    def extract_upstream_refs(self, csn_def: dict) -> List[str]:
        """Return the list of upstream object qualified-names referenced in the SELECT
        FROM clause of the entity's CSN definition.
        """
        query = csn_def.get("query")
        if not isinstance(query, dict):
            return []
        select = query.get("SELECT")
        if not isinstance(select, dict):
            return []
        from_clause = select.get("from")
        if from_clause is None:
            return []
        refs: List[str] = []
        self._walk_from(from_clause, refs)
        return refs

    def _walk_from(self, node: object, out: List[str]) -> None:
        if not isinstance(node, dict):
            return
        if "ref" in node and isinstance(node["ref"], list) and node["ref"]:
            # A direct table reference. Only string refs name an upstream object;
            # dict refs (parametrized / inline-defined entities) carry no
            # straightforward target name and are skipped to avoid emitting
            # malformed URNs downstream.
            first = node["ref"][0]
            if isinstance(first, str):
                out.append(first)
            return
        if "join" in node and isinstance(node.get("args"), list):
            for arg in node["args"]:
                self._walk_from(arg, out)
            return
        if "SELECT" in node:
            # Inline subquery — recurse into its FROM.
            inner_select = node["SELECT"]
            if isinstance(inner_select, dict) and "from" in inner_select:
                self._walk_from(inner_select["from"], out)

    def _build_alias_map(self, from_node: object) -> Dict[str, str]:
        """Build an alias→qualified-name map from a CSN FROM clause.

        For single-source FROMs, the empty-string key ``""`` ALSO maps to the
        source qualified-name so that unqualified column refs (``{"ref":
        ["X"]}``) can be resolved unambiguously — regardless of whether the
        FROM has an explicit ``as`` alias. (Real SAP CSN frequently uses
        ``FROM T AS T`` with unqualified column references in the SELECT
        columns — verified against the live tenant.)

        For multi-source JOINs, only the explicit aliases appear — unqualified
        column refs there cannot be safely attributed and are skipped at walk
        time.
        """
        result: Dict[str, str] = {}
        sources: List[str] = []
        self._walk_alias(from_node, result, sources)
        if len(sources) == 1:
            result[""] = sources[0]
        return result

    def _walk_alias(
        self,
        node: object,
        out: Dict[str, str],
        sources: List[str],
    ) -> None:
        if not isinstance(node, dict):
            return
        if "ref" in node and isinstance(node["ref"], list) and node["ref"]:
            first = node["ref"][0]
            if isinstance(first, str):
                alias = node.get("as", first)
                if isinstance(alias, str):
                    out[alias] = first
                sources.append(first)
            return
        if "join" in node and isinstance(node.get("args"), list):
            for arg in node["args"]:
                self._walk_alias(arg, out, sources)
            return
        # Subqueries (SELECT in FROM) intentionally skipped — column-lineage
        # through subqueries lands in a follow-up PR.

    def _walk_expr(
        self,
        node: object,
        alias_map: Dict[str, str],
        out: List[UpstreamColRef],
        unresolved: List[str],
    ) -> None:
        """Walk a column expression, appending (upstream_qualified_name, upstream_col)
        tuples for every resolvable column reference found.

        Handles direct `ref` lookups (1-segment and 2-segment), `func` calls with
        `args`, generic `xpr` expression arrays (CASE / arithmetic / comparison),
        `cast` / `case` containers, and inline scalar subqueries (`SELECT` with its
        own FROM + columns). Literals (`val`) contribute nothing.

        Scalar subqueries (e.g., ``SELECT (SELECT MAX(x) FROM T) AS m FROM BASE``)
        are walked transparently — the subquery's column refs are resolved against
        its OWN FROM clause's alias map (built afresh) and surface as refs on the
        outer column. Correlated subqueries that reference the outer query's
        aliases are NOT supported — refs against the outer alias map land in
        ``unresolved``.

        References that the walker can't attribute to a known alias (unqualified
        ref under multi-source FROM, unknown alias, 3+ segment refs, non-string
        segments) are appended to ``unresolved`` as human-readable diagnostic
        strings. The caller surfaces these via ``report.column_lineage_unresolved``.
        """
        if isinstance(node, list):
            for item in node:
                self._walk_expr(item, alias_map, out, unresolved)
            return
        if not isinstance(node, dict):
            return
        if "ref" in node and isinstance(node["ref"], list):
            segs = node["ref"]
            if len(segs) == 1 and isinstance(segs[0], str):
                source = alias_map.get("")
                if source is not None:
                    out.append(UpstreamColRef(qname=source, col=segs[0]))
                else:
                    unresolved.append(
                        f"<unqualified ref {_sanitize_for_report(segs[0])} in multi-source FROM>"
                    )
            elif len(segs) == 2 and all(isinstance(s, str) for s in segs):
                alias, col = segs
                if alias in alias_map:
                    out.append(UpstreamColRef(qname=alias_map[alias], col=col))
                else:
                    unresolved.append(
                        f"<unknown alias {_sanitize_for_report(alias)} for col {_sanitize_for_report(col)}>"
                    )
            else:
                # 3+ segments or non-string segs — unresolvable
                unresolved.append(
                    f"<unresolvable ref shape {_sanitize_for_report(segs)}>"
                )
            return
        if "SELECT" in node and isinstance(node["SELECT"], dict):
            # Inline scalar subquery — walk the subquery's columns under ITS OWN
            # alias map. Refs against the outer scope (correlated subqueries) are
            # NOT supported in v1; those land in `unresolved` because the inner
            # alias map doesn't carry the outer scope's aliases.
            inner = node["SELECT"]
            inner_from = inner.get("from")
            inner_alias_map: Dict[str, str] = (
                self._build_alias_map(inner_from) if inner_from is not None else {}
            )
            inner_cols = inner.get("columns")
            if isinstance(inner_cols, list):
                for inner_col in inner_cols:
                    self._walk_expr(inner_col, inner_alias_map, out, unresolved)
            else:
                # Subquery has no columns list (e.g. SELECT * — unresolvable here)
                unresolved.append(
                    "<scalar subquery without explicit columns — refs unresolvable>"
                )
            return
        for key in ("args", "xpr", "case", "cast"):
            if key in node:
                self._walk_expr(node[key], alias_map, out, unresolved)

    @staticmethod
    def _is_association_projection(col: dict, association_names: Set[str]) -> bool:
        """True if `col` is a bare ``{"ref": ["_assoc"]}`` projecting a CDS
        association element declared in the view's own ``elements`` map.

        Such columns are navigations to other entities, not scalar columns of
        the FROM table, so they must be excluded from column-level lineage
        (they would otherwise produce phantom upstream-column references that
        never resolve against the real upstream schema).
        """
        ref = col.get("ref")
        if (
            isinstance(ref, list)
            and len(ref) == 1
            and isinstance(ref[0], str)
            and ref[0] in association_names
        ):
            # Only treat as an association projection when there's no alias that
            # would turn it into a differently-named scalar output — a bare
            # association ref has no scalar source.
            return True
        return False

    def _pair_for_column(
        self,
        col: dict,
        alias_map: Dict[str, str],
    ) -> Optional[ColumnLineagePair]:
        """Build a single ColumnLineagePair for one entry of CSN ``SELECT.columns[]``.

        Returns:
            - A ColumnLineagePair for normal/unnamed/resolvable expressions
            - None for pure literals (no upstream, no unresolved — skip silently)
        """
        downstream_name = self._resolve_output_name(col)
        raw_refs: List[UpstreamColRef] = []
        unresolved: List[str] = []
        self._walk_expr(col, alias_map, raw_refs, unresolved)

        if downstream_name is None:
            # Unnamed expression (e.g. SUM(x) without AS). If it has refs the
            # user probably meant to alias, surface it as unresolved so
            # operators see the silent drop.
            if not raw_refs and not unresolved:
                return None  # pure literal, no diagnostic to surface
            deduped_refs = _dedup_preserving_order(raw_refs)
            deduped_unresolved = _dedup_preserving_order(unresolved)
            return ColumnLineagePair(
                downstream_col=_UNNAMED_COL_SENTINEL,
                upstream_refs=(),
                transform_op=None,
                unresolved_refs=tuple(
                    deduped_unresolved
                    + [
                        f"unnamed expression referencing {ref.qname}.{ref.col}"
                        for ref in deduped_refs
                    ]
                ),
            )

        if not raw_refs and not unresolved:
            return None  # pure literal — skip silently (legitimate)

        return ColumnLineagePair(
            downstream_col=downstream_name,
            upstream_refs=tuple(_dedup_preserving_order(raw_refs)),
            transform_op=self._infer_transform(col),
            unresolved_refs=tuple(_dedup_preserving_order(unresolved)),
        )

    def extract_column_lineage(self, csn_def: dict) -> List[ColumnLineagePair]:
        """Walk the CSN `query.SELECT.columns[]` array and return one
        `ColumnLineagePair` per downstream column.

        Behaviour:
          - Returns ``[]`` for legitimate non-SELECT entities (no ``query``, no
            ``SELECT``, no ``from``, no ``columns``).
          - Returns a single synthetic ``<malformed>`` pair (with diagnostic
            text in ``unresolved_refs``) when the CSN is structurally broken
            (e.g. ``SELECT`` is not a dict, ``columns`` is not a list). This
            lets the caller distinguish a corrupt CSN from a base table.
          - Pure-literal columns are skipped silently.
          - Walker-level unresolved refs (unknown alias, 3-segment ref, etc.)
            are recorded into ``ColumnLineagePair.unresolved_refs`` so the
            caller can surface them via ``report.column_lineage_unresolved``.
        """
        select, malformed = self._safe_select(csn_def)
        if malformed is not None:
            return [malformed]
        if select is None:
            return []  # legitimate non-SELECT entity (e.g. raw table)
        from_clause = select.get("from")
        if from_clause is None:
            return []  # legitimate base table
        columns = select.get("columns")
        if columns is None:
            return []  # legitimate: SELECT with no columns array
        if not isinstance(columns, list):
            return [
                self._malformed_pair(
                    f"columns is {type(columns).__name__}, expected list"
                )
            ]

        alias_map = self._build_alias_map(from_clause)

        # CDS association elements (leading-underscore navigations) declared in
        # the view's own elements map. A SELECT can project these directly
        # (e.g. {"ref": ["_DAY_OF_WEEK"]}); such projections are not scalar
        # columns of the FROM table and must be skipped — emitting them produces
        # phantom fine-grained edges that never resolve against the upstream.
        association_names = {
            name
            for name, el in (csn_def.get("elements") or {}).items()
            if isinstance(el, dict) and el.get("type") == "cds.Association"
        }

        pairs: List[ColumnLineagePair] = []
        for col in columns:
            if not isinstance(col, dict):
                continue
            if self._is_association_projection(col, association_names):
                continue  # navigation element, not a scalar column of the FROM table
            pair = self._pair_for_column(col, alias_map)
            if pair is not None:
                pairs.append(pair)
        return pairs

    def remote_source(self, csn_def: dict) -> Optional[str]:
        """Return the `@remote.source` annotation (connection name) if present."""
        v = csn_def.get("@remote.source")
        return v if isinstance(v, str) else None
