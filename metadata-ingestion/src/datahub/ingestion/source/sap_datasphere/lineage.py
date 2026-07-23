from typing import Dict, List, Optional, Set, Tuple

from datahub.ingestion.source.sap_datasphere.constants import (
    CSN_ARGS,
    CSN_AS,
    CSN_CASE,
    CSN_CAST,
    CSN_COLUMNS,
    CSN_FROM,
    CSN_FUNC,
    CSN_JOIN,
    CSN_KEY_ELEMENTS,
    CSN_KEY_QUERY,
    CSN_REF,
    CSN_REMOTE_SOURCE,
    CSN_SELECT,
    CSN_TYPE,
    CSN_TYPE_ASSOCIATION,
    CSN_XPR,
    MALFORMED_COL_SENTINEL,
    PROJECTION_ALIAS,
    REMOTE_CONNECTION_KEY,
    REMOTE_ENTITY_DELIMITER,
    REMOTE_ENTITY_KEY,
    UNNAMED_COL_SENTINEL,
)
from datahub.ingestion.source.sap_datasphere.models import (
    ColumnLineagePair,
    RemoteTableSource,
    TransformOp,
    UpstreamColRef,
    dedup_preserving_order,
)


def _sanitize_for_report(value: object) -> str:
    # Keep report entries single-line for log scanning.
    return repr(value).replace("\n", "\\n").replace("\r", "\\r")


class CsnLineageExtractor:
    """Extracts table- and column-level lineage from a CSN definition.

    Scope: plain SELECT, INNER/LEFT/RIGHT joins of named refs, the ``$projection``
    pseudo-alias, and inline scalar subqueries. Unions, CTEs, and correlated
    subqueries fall back to "no lineage" — the asset still emits without the edge.
    """

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
        return ColumnLineagePair(
            downstream_col=MALFORMED_COL_SENTINEL,
            upstream_refs=(),
            transform_op=None,
            unresolved_refs=(reason,),
        )

    def _safe_select(
        self, csn_def: dict
    ) -> Tuple[Optional[dict], Optional["ColumnLineagePair"]]:
        """Validate the CSN envelope, returning one of:
        ``(select_dict, None)`` proceed; ``(None, malformed_pair)`` broken CSN,
        return ``[malformed_pair]``; ``(None, None)`` legitimate non-SELECT entity.
        """
        if not isinstance(csn_def, dict):
            return None, None
        query = csn_def.get(CSN_KEY_QUERY)
        if query is None:
            return None, None
        if not isinstance(query, dict):
            return None, self._malformed_pair(
                f"query is {type(query).__name__}, expected dict"
            )
        select = query.get(CSN_SELECT)
        if select is None:
            return None, None
        if not isinstance(select, dict):
            return None, self._malformed_pair(
                f"SELECT is {type(select).__name__}, expected dict"
            )
        return select, None

    def _resolve_output_name(self, col: dict) -> Optional[str]:
        # Prefer the `as` alias; fall back to the last `ref` segment. An unnamed
        # aggregate (SUM(x) with no alias) can't produce lineage and is skipped.
        as_alias = col.get(CSN_AS)
        if isinstance(as_alias, str):
            return as_alias
        ref = col.get(CSN_REF)
        if isinstance(ref, list) and ref:
            last = ref[-1]
            if isinstance(last, str):
                return last
        return None

    def _infer_transform(self, col: dict) -> Optional[TransformOp]:
        if CSN_FUNC in col:
            func = col[CSN_FUNC]
            if isinstance(func, str):
                if func.upper() in self._AGGREGATE_FUNCS:
                    return "AGGREGATE"
                return "TRANSFORMATION"
        if CSN_XPR in col:
            return "EXPRESSION"
        if CSN_AS in col and CSN_REF in col:
            return "RENAME"
        if CSN_REF in col:
            return "IDENTITY"
        return None

    def extract_upstream_refs(self, csn_def: dict) -> List[str]:
        query = csn_def.get(CSN_KEY_QUERY)
        if not isinstance(query, dict):
            return []
        select = query.get(CSN_SELECT)
        if not isinstance(select, dict):
            return []
        from_clause = select.get(CSN_FROM)
        if from_clause is None:
            return []
        refs: List[str] = []
        self._walk_from(from_clause, refs)
        return refs

    def _walk_from(self, node: object, out: List[str]) -> None:
        if not isinstance(node, dict):
            return
        if CSN_REF in node and isinstance(node[CSN_REF], list) and node[CSN_REF]:
            # Only string refs name an upstream object; dict refs (parametrized /
            # inline-defined entities) carry no target name and are skipped to
            # avoid malformed URNs downstream.
            first = node[CSN_REF][0]
            if isinstance(first, str):
                out.append(first)
            return
        if CSN_JOIN in node and isinstance(node.get(CSN_ARGS), list):
            for arg in node[CSN_ARGS]:
                self._walk_from(arg, out)
            return
        if CSN_SELECT in node:
            inner_select = node[CSN_SELECT]
            if isinstance(inner_select, dict) and CSN_FROM in inner_select:
                self._walk_from(inner_select[CSN_FROM], out)

    def _build_alias_map(self, from_node: object) -> Dict[str, str]:
        """Build an alias→qualified-name map from a CSN FROM clause.

        For single-source FROMs the empty-string key ``""`` also maps to the
        source so unqualified column refs resolve unambiguously — real SAP CSN
        frequently uses ``FROM T AS T`` with unqualified column refs. For
        multi-source JOINs only explicit aliases appear; unqualified refs there
        can't be attributed and are skipped at walk time.
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
        if CSN_REF in node and isinstance(node[CSN_REF], list) and node[CSN_REF]:
            first = node[CSN_REF][0]
            if isinstance(first, str):
                alias = node.get(CSN_AS, first)
                if isinstance(alias, str):
                    out[alias] = first
                sources.append(first)
            return
        if CSN_JOIN in node and isinstance(node.get(CSN_ARGS), list):
            for arg in node[CSN_ARGS]:
                self._walk_alias(arg, out, sources)
            return
        # Column lineage through FROM-clause subqueries is not yet supported.

    def _walk_expr(
        self,
        node: object,
        alias_map: Dict[str, str],
        out: List[UpstreamColRef],
        unresolved: List[str],
        projection_map: Optional[Dict[str, dict]] = None,
        visited: Optional[Set[str]] = None,
    ) -> None:
        """Walk a column expression, appending an ``UpstreamColRef`` per resolvable
        column reference and a diagnostic string per unresolvable one.

        Handles direct 1-/2-segment refs, the ``$projection`` pseudo-alias, ``func``
        calls, ``xpr``/``case``/``cast`` containers, and inline scalar subqueries.
        A scalar subquery is walked against its OWN FROM's alias map (built afresh);
        correlated subqueries referencing the outer scope are not supported and land
        in ``unresolved``. The outer ``projection_map`` is intentionally not
        propagated into a subquery — a ``$projection`` ref there targets the
        subquery's own projection, not the outer query's.
        """
        if isinstance(node, list):
            for item in node:
                self._walk_expr(
                    item, alias_map, out, unresolved, projection_map, visited
                )
            return
        if not isinstance(node, dict):
            return
        if CSN_REF in node and isinstance(node[CSN_REF], list):
            segs = node[CSN_REF]
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
                if alias == PROJECTION_ALIAS:
                    self._resolve_projection_ref(
                        col, alias_map, out, unresolved, projection_map, visited
                    )
                elif alias in alias_map:
                    out.append(UpstreamColRef(qname=alias_map[alias], col=col))
                else:
                    unresolved.append(
                        f"<unknown alias {_sanitize_for_report(alias)} for col {_sanitize_for_report(col)}>"
                    )
            else:
                unresolved.append(
                    f"<unresolvable ref shape {_sanitize_for_report(segs)}>"
                )
            return
        if CSN_SELECT in node and isinstance(node[CSN_SELECT], dict):
            inner = node[CSN_SELECT]
            inner_from = inner.get(CSN_FROM)
            inner_alias_map: Dict[str, str] = (
                self._build_alias_map(inner_from) if inner_from is not None else {}
            )
            inner_cols = inner.get(CSN_COLUMNS)
            if isinstance(inner_cols, list):
                for inner_col in inner_cols:
                    self._walk_expr(inner_col, inner_alias_map, out, unresolved)
            else:
                unresolved.append(
                    "<scalar subquery without explicit columns — refs unresolvable>"
                )
            return
        for key in (CSN_ARGS, CSN_XPR, CSN_CASE, CSN_CAST):
            if key in node:
                self._walk_expr(
                    node[key], alias_map, out, unresolved, projection_map, visited
                )

    def _resolve_projection_ref(
        self,
        col: str,
        alias_map: Dict[str, str],
        out: List[UpstreamColRef],
        unresolved: List[str],
        projection_map: Optional[Dict[str, dict]],
        visited: Optional[Set[str]],
    ) -> None:
        # Follow the sibling output column's expression down to its real upstream
        # refs; ``visited`` guards against reference cycles (a -> b -> a).
        if not projection_map:
            unresolved.append(
                f"<$projection ref to {_sanitize_for_report(col)} (no projection context)>"
            )
            return
        sibling = projection_map.get(col)
        if sibling is None:
            unresolved.append(
                f"<$projection ref to unknown output col {_sanitize_for_report(col)}>"
            )
            return
        if visited is None:
            visited = set()
        if col in visited:
            unresolved.append(
                f"<$projection reference cycle at {_sanitize_for_report(col)}>"
            )
            return
        visited.add(col)
        self._walk_expr(sibling, alias_map, out, unresolved, projection_map, visited)

    @staticmethod
    def _is_association_projection(col: dict, association_names: Set[str]) -> bool:
        # A bare {"ref": ["_assoc"]} projecting a CDS association is a navigation
        # to another entity, not a scalar column of the FROM table — excluding it
        # avoids phantom fine-grained edges that never resolve upstream.
        ref = col.get(CSN_REF)
        if (
            isinstance(ref, list)
            and len(ref) == 1
            and isinstance(ref[0], str)
            and ref[0] in association_names
        ):
            return True
        return False

    def _pair_for_column(
        self,
        col: dict,
        alias_map: Dict[str, str],
        projection_map: Optional[Dict[str, dict]] = None,
    ) -> Optional[ColumnLineagePair]:
        downstream_name = self._resolve_output_name(col)
        raw_refs: List[UpstreamColRef] = []
        unresolved: List[str] = []
        # Fresh visited set per column bounds ``$projection`` expansion.
        self._walk_expr(col, alias_map, raw_refs, unresolved, projection_map, set())

        if downstream_name is None:
            # Unnamed expression. If it has refs the user probably meant to alias
            # it; surface as unresolved so the silent drop is visible.
            if not raw_refs and not unresolved:
                return None  # pure literal
            deduped_refs = dedup_preserving_order(raw_refs)
            deduped_unresolved = dedup_preserving_order(unresolved)
            return ColumnLineagePair(
                downstream_col=UNNAMED_COL_SENTINEL,
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
            return None  # pure literal

        return ColumnLineagePair(
            downstream_col=downstream_name,
            upstream_refs=tuple(dedup_preserving_order(raw_refs)),
            transform_op=self._infer_transform(col),
            unresolved_refs=tuple(dedup_preserving_order(unresolved)),
        )

    def extract_column_lineage(self, csn_def: dict) -> List[ColumnLineagePair]:
        """Return one ``ColumnLineagePair`` per downstream column.

        Returns ``[]`` for legitimate non-SELECT entities, and a single
        ``<malformed>`` pair (carrying a diagnostic) when the CSN is structurally
        broken, so the caller can tell a corrupt CSN apart from a base table.
        """
        select, malformed = self._safe_select(csn_def)
        if malformed is not None:
            return [malformed]
        if select is None:
            return []
        from_clause = select.get(CSN_FROM)
        if from_clause is None:
            return []
        columns = select.get(CSN_COLUMNS)
        if columns is None:
            return []
        if not isinstance(columns, list):
            return [
                self._malformed_pair(
                    f"columns is {type(columns).__name__}, expected list"
                )
            ]

        alias_map = self._build_alias_map(from_clause)

        # CDS association elements (leading-underscore navigations) can be
        # projected directly; those are not scalar columns of the FROM table.
        association_names = {
            name
            for name, el in (csn_def.get(CSN_KEY_ELEMENTS) or {}).items()
            if isinstance(el, dict) and el.get(CSN_TYPE) == CSN_TYPE_ASSOCIATION
        }

        # output-column name -> its CSN column dict, so ``$projection`` refs can be
        # followed to real upstream refs. First definition wins.
        projection_map: Dict[str, dict] = {}
        for col in columns:
            if not isinstance(col, dict):
                continue
            out_name = self._resolve_output_name(col)
            if out_name is not None:
                projection_map.setdefault(out_name, col)

        pairs: List[ColumnLineagePair] = []
        for col in columns:
            if not isinstance(col, dict):
                continue
            if self._is_association_projection(col, association_names):
                continue
            pair = self._pair_for_column(col, alias_map, projection_map)
            if pair is not None:
                pairs.append(pair)
        return pairs

    def remote_source(self, csn_def: dict) -> Optional[str]:
        v = csn_def.get(CSN_REMOTE_SOURCE)
        return v if isinstance(v, str) else None


def parse_remote_table_source(csn_entity: dict) -> Optional[RemoteTableSource]:
    """Read a Remote Table's external origin from its CSN entity's
    ``@DataWarehouse.remote.*`` annotations. Returns None when the entity is not
    a federated remote table (both annotations absent)."""
    connection = csn_entity.get(REMOTE_CONNECTION_KEY)
    entity = csn_entity.get(REMOTE_ENTITY_KEY)
    if not isinstance(connection, str) or not connection:
        return None
    if not isinstance(entity, str) or not entity:
        return None
    return RemoteTableSource(
        connection=connection,
        path_parts=tuple(entity.split(REMOTE_ENTITY_DELIMITER)),
    )
