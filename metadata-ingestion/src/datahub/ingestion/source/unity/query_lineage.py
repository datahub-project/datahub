import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable, Dict, List, Optional, Sequence, Set, Tuple, Union

import datahub.metadata.schema_classes as models
from datahub.ingestion.source.unity.proxy_types import Query
from datahub.metadata.urns import QueryUrn
from datahub.sql_parsing.sqlglot_utils import get_query_fingerprint

logger = logging.getLogger(__name__)

_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)

# Case-folded (upstream_urn, downstream_urn) — see _edge_key.
EdgeKey = Tuple[str, str]

_EMPTY_AUDIT_STAMP = models.AuditStampClass(time=0, actor="urn:li:corpuser:_ingestion")


def build_query_entity_aspects(
    query_urn: str,
    query_text: str,
    subject_urns: Sequence[str],
) -> List[Union[models.QueryPropertiesClass, models.QuerySubjectsClass]]:
    """Aspects for a Query entity representing one Databricks statement."""
    return [
        models.QueryPropertiesClass(
            statement=models.QueryStatementClass(
                value=query_text,
                language=models.QueryLanguageClass.SQL,
            ),
            source=models.QuerySourceClass.SYSTEM,
            created=_EMPTY_AUDIT_STAMP,
            lastModified=_EMPTY_AUDIT_STAMP,
        ),
        models.QuerySubjectsClass(
            subjects=[
                models.QuerySubjectClass(entity=urn)
                for urn in sorted(set(subject_urns))
            ]
        ),
    ]


def _edge_key(upstream_urn: str, downstream_urn: str) -> EdgeKey:
    """Case-insensitive match key for one lineage edge.

    The resolver is keyed on URNs derived from `system.access` full names but is
    looked up with URNs derived from the REST API's table refs, and Databricks does
    not guarantee the two agree on identifier case. usage.py:554 compares those same
    two populations with `.lower()` on both sides for the same reason. Only this key
    is folded: the URNs carried on the candidate — and therefore the ones written
    into `Upstream.query` and `querySubjects` — keep the exact casing
    `gen_dataset_urn` produced.
    """
    return (upstream_urn.casefold(), downstream_urn.casefold())


@dataclass
class _Candidate:
    query_urn: str
    query_text: str
    end_time: datetime
    # The unfolded URNs this candidate was matched on, so _offer can rebuild its
    # _by_edge key from a stored candidate (e.g. when comparing to a replacement).
    upstream_urn: str
    downstream_urn: str


@dataclass
class QueryLineageResolver:
    """Maps lineage edges to the Query entity that produced them.

    Only warehouse-executed Databricks statements reach here: system table rows for
    cluster-executed work (notebook/job/pipeline compute) carry no statement_id, so they
    have no recoverable statement text and are never linked.
    """

    resolve_urn: Callable[[str], Optional[str]]
    platform: str = "databricks"

    _by_edge: Dict[EdgeKey, _Candidate] = field(default_factory=dict)
    _seen: int = 0
    _skipped: int = 0
    _unresolved: int = 0
    # Populated by query_urn_for as lineage aspects are generated: the URNs a
    # caller actually asserted for that query_urn, i.e. the ones that end up in an
    # emitted Upstream/FineGrainedLineage edge. A statement can join system.access
    # to a table outside the ingested catalog/schema patterns, whose URN then
    # never appears in any lineage aspect; keying subjects off actual lookups
    # (rather than every _by_edge candidate for a query_urn) keeps that
    # never-ingested table out of querySubjects.
    _consumed_subjects: Dict[str, Set[str]] = field(default_factory=dict)

    def add_query(self, query: Query) -> None:
        self._seen += 1
        text = query.query_text
        if not text or not text.strip():
            self._skipped += 1
            return

        upstream_urns = self._resolve_unique(query.source_table_full_names)
        downstream_urns = self._resolve_unique(query.target_table_full_names)
        if not upstream_urns or not downstream_urns:
            # Read statements land here by design (a plain SELECT has no target
            # table) and are accounted for by num_statements_seen alone. Only a
            # statement that carried both a source and a target in system.access
            # counts as a drop: that means its identifiers could not be mapped onto
            # dataset URNs, which is the one actionable failure on this path.
            if query.source_table_full_names and query.target_table_full_names:
                self._unresolved += 1
            return

        # Mirrors usage.py's _to_preparsed_queries: a statement that fans out to more
        # than one resolved target gets one Query per downstream, disambiguated by
        # folding the downstream urn into the fingerprint. This keeps single-target
        # URNs (the common case, secondary_id=None) identical to the ones the
        # system-tables usage path emits, so we reuse its Query entities instead of
        # minting duplicates. Multi-target statements cannot reach that same parity:
        # usage.py folds in the schema-resolver-registered (REST-cased) URN, and that
        # resolver is near-empty while this runs (see resolve_urn's docstring), so a
        # multi-target statement may mint a Query entity distinct from usage.py's.
        # secondary_id is still casefolded like _edge_key, so at least this path is
        # self-consistent: a re-run where Databricks reports different casing for the
        # same downstream table reuses the same Query instead of minting a new one.
        multi_target = len(downstream_urns) > 1
        for downstream_urn in downstream_urns:
            secondary_id = downstream_urn.casefold() if multi_target else None
            fingerprint = self._fingerprint(text, query.query_id, secondary_id)
            query_urn = QueryUrn(fingerprint).urn()
            end_time = query.end_time or _EPOCH
            for upstream_urn in upstream_urns:
                if upstream_urn.casefold() == downstream_urn.casefold():
                    continue
                self._offer(
                    _Candidate(
                        query_urn=query_urn,
                        query_text=text,
                        end_time=end_time,
                        upstream_urn=upstream_urn,
                        downstream_urn=downstream_urn,
                    )
                )

    def _resolve_unique(self, full_names: List[str]) -> List[str]:
        urns: List[str] = []
        seen: Set[str] = set()
        for full_name in full_names:
            urn = self.resolve_urn(full_name)
            if urn and urn not in seen:
                seen.add(urn)
                urns.append(urn)
        return urns

    def _fingerprint(
        self, text: str, query_id: Optional[str], secondary_id: Optional[str]
    ) -> str:
        try:
            return get_query_fingerprint(
                text, self.platform, fast=True, secondary_id=secondary_id
            )
        except Exception as e:
            # get_query_fingerprint already falls back internally; a residual
            # failure here mirrors usage.py's _query_fingerprint fallback so both
            # paths agree on the id even when sqlglot can't parse the statement.
            logger.debug(
                "Could not fingerprint statement, using statement_id fallback: %s", e
            )
            base = f"unity-stmt-{query_id}"
            return f"{base}-{secondary_id}" if secondary_id else base

    def _offer(self, candidate: _Candidate) -> None:
        key = _edge_key(candidate.upstream_urn, candidate.downstream_urn)
        existing = self._by_edge.get(key)
        if existing is None or self._is_newer(candidate, existing):
            self._by_edge[key] = candidate

    @staticmethod
    def _is_newer(candidate: _Candidate, existing: _Candidate) -> bool:
        # Tie-break on the URN so repeated runs over identical input are stable.
        return (candidate.end_time, candidate.query_urn) > (
            existing.end_time,
            existing.query_urn,
        )

    def query_urn_for(self, upstream_urn: str, downstream_urn: str) -> Optional[str]:
        candidate = self._by_edge.get(_edge_key(upstream_urn, downstream_urn))
        if candidate is None:
            return None
        # Record the exact URNs this lookup asserted (not the candidate's own
        # stored URNs, which matched case-insensitively and may differ in case)
        # so subject_urns_for reports precisely what the caller put in its
        # emitted lineage edge.
        subjects = self._consumed_subjects.setdefault(candidate.query_urn, set())
        subjects.add(upstream_urn)
        subjects.add(downstream_urn)
        return candidate.query_urn

    def queries_to_emit(self) -> List[Tuple[str, str]]:
        """Every Query entity the resolver resolved, deduped by URN.

        Emitted regardless of subject_urns_for's outcome, so a query URN
        referenced by a lineage edge is never left without its own entity.
        """
        deduped: Dict[str, str] = {
            c.query_urn: c.query_text for c in self._by_edge.values()
        }
        return sorted(deduped.items())

    def subject_urns_for(self, query_urn: str) -> List[str]:
        """Every dataset actually referenced by an emitted lineage edge for this query.

        Only URNs that reached an Upstream/FineGrainedLineage edge via
        query_urn_for count as subjects, so a statement that also touched a
        table outside the ingested catalog/schema patterns cannot leak that
        table's (never-ingested) URN into querySubjects.
        """
        return sorted(self._consumed_subjects.get(query_urn, ()))

    @property
    def num_edges_linked(self) -> int:
        return len(self._by_edge)

    @property
    def num_statements_seen(self) -> int:
        """Every statement offered to the resolver, linkable or not."""
        return self._seen

    @property
    def num_statements_skipped(self) -> int:
        """Statements with no usable text. Resolved-to-text is seen minus this."""
        return self._skipped

    @property
    def num_statements_unresolved(self) -> int:
        """Statements whose source and target names did not map to dataset URNs."""
        return self._unresolved
