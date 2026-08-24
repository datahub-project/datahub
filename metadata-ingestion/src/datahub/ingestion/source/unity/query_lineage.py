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

# (upstream_urn, downstream_urn)
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


@dataclass
class _Candidate:
    query_urn: str
    query_text: str
    end_time: datetime


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
    _skipped: int = 0
    # Lazily built, invalidated whenever _by_edge changes (see _offer) so it can
    # never serve a stale subjects list for a query_urn added/replaced since the
    # last build. Rebuilding costs O(edges) once per generation instead of once
    # per subject_urns_for() call, which matters at production edge counts.
    _subjects_by_query: Optional[Dict[str, List[str]]] = None

    def add_query(self, query: Query) -> None:
        text = query.query_text
        if not text or not text.strip():
            self._skipped += 1
            return

        upstream_urns = self._resolve_unique(query.source_table_full_names)
        downstream_urns = self._resolve_unique(query.target_table_full_names)
        if not upstream_urns or not downstream_urns:
            return

        # Mirrors usage.py's _to_preparsed_queries: a statement that fans out to more
        # than one resolved target gets one Query per downstream, disambiguated by
        # folding the downstream urn into the fingerprint. This keeps our URNs
        # identical to the ones the system-tables usage path already emits, so we
        # reuse its Query entities instead of minting duplicates.
        multi_target = len(downstream_urns) > 1
        for downstream_urn in downstream_urns:
            secondary_id = downstream_urn if multi_target else None
            fingerprint = self._fingerprint(text, query.query_id, secondary_id)
            candidate = _Candidate(
                query_urn=QueryUrn(fingerprint).urn(),
                query_text=text,
                end_time=query.end_time or _EPOCH,
            )
            for upstream_urn in upstream_urns:
                if upstream_urn == downstream_urn:
                    continue
                self._offer((upstream_urn, downstream_urn), candidate)

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

    def _offer(self, key: EdgeKey, candidate: _Candidate) -> None:
        existing = self._by_edge.get(key)
        if existing is None or self._is_newer(candidate, existing):
            self._by_edge[key] = candidate
            self._subjects_by_query = None

    @staticmethod
    def _is_newer(candidate: _Candidate, existing: _Candidate) -> bool:
        # Tie-break on the URN so repeated runs over identical input are stable.
        return (candidate.end_time, candidate.query_urn) > (
            existing.end_time,
            existing.query_urn,
        )

    def query_urn_for(self, upstream_urn: str, downstream_urn: str) -> Optional[str]:
        candidate = self._by_edge.get((upstream_urn, downstream_urn))
        return candidate.query_urn if candidate else None

    def queries_to_emit(self) -> List[Tuple[str, str]]:
        """Every Query entity that must be emitted before its URN is referenced."""
        deduped: Dict[str, str] = {
            c.query_urn: c.query_text for c in self._by_edge.values()
        }
        return sorted(deduped.items())

    def subject_urns_for(self, query_urn: str) -> List[str]:
        """Every dataset touched by the statement behind this query URN."""
        if self._subjects_by_query is None:
            by_query: Dict[str, Set[str]] = {}
            for (upstream_urn, downstream_urn), candidate in self._by_edge.items():
                subjects = by_query.setdefault(candidate.query_urn, set())
                subjects.add(upstream_urn)
                subjects.add(downstream_urn)
            self._subjects_by_query = {
                urn: sorted(subjects) for urn, subjects in by_query.items()
            }
        return self._subjects_by_query.get(query_urn, [])

    @property
    def num_edges_linked(self) -> int:
        return len(self._by_edge)

    @property
    def num_statements_skipped(self) -> int:
        return self._skipped
