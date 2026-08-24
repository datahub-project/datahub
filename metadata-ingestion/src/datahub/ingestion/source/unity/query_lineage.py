import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable, Dict, List, Optional, Tuple

from datahub.ingestion.source.unity.proxy_types import Query
from datahub.metadata.urns import QueryUrn
from datahub.sql_parsing.sqlglot_utils import get_query_fingerprint

logger = logging.getLogger(__name__)

_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)

# (upstream_urn, downstream_urn)
EdgeKey = Tuple[str, str]


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

    def add_query(self, query: Query) -> None:
        text = query.query_text
        if not text or not text.strip():
            self._skipped += 1
            return

        try:
            fingerprint = get_query_fingerprint(text, platform=self.platform)
        except Exception as e:
            # get_query_fingerprint already falls back internally; treat any residual
            # failure as unlinkable rather than aborting the whole run.
            logger.debug("Could not fingerprint statement, skipping link: %s", e)
            self._skipped += 1
            return

        candidate = _Candidate(
            query_urn=QueryUrn(fingerprint).urn(),
            query_text=text,
            end_time=query.end_time or _EPOCH,
        )

        for source in query.source_table_full_names:
            upstream_urn = self.resolve_urn(source)
            if upstream_urn is None:
                continue
            for target in query.target_table_full_names:
                downstream_urn = self.resolve_urn(target)
                if downstream_urn is None or downstream_urn == upstream_urn:
                    continue
                self._offer((upstream_urn, downstream_urn), candidate)

    def _offer(self, key: EdgeKey, candidate: _Candidate) -> None:
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
        candidate = self._by_edge.get((upstream_urn, downstream_urn))
        return candidate.query_urn if candidate else None

    def queries_to_emit(self) -> List[Tuple[str, str]]:
        """Every Query entity that must be emitted before its URN is referenced."""
        deduped: Dict[str, str] = {
            c.query_urn: c.query_text for c in self._by_edge.values()
        }
        return sorted(deduped.items())

    @property
    def num_edges_linked(self) -> int:
        return len(self._by_edge)

    @property
    def num_statements_skipped(self) -> int:
        return self._skipped
