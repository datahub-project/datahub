import dataclasses
import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Callable, Dict, Optional, Tuple

from datahub.ingestion.source.powerbi.m_query._bridge import (
    MQueryBridgeError,
    MQueryParseError,
)
from datahub.ingestion.source.powerbi.m_query.ast_utils import (
    NodeIdMap,
    resolve_parameter_value,
)
from datahub.utilities.threading_timeout import TimeoutException

logger = logging.getLogger(__name__)

# A reference chain longer than this is not a real model shape; it is a sign the
# walk is going somewhere unproductive.
MAX_REFERENCE_DEPTH = 10

# Marker Power BI puts on an expression that holds a parameter value rather than
# a query. Following one leads to a literal, never to a data source.
PARAMETER_QUERY_MARKER = "IsParameterQuery=true"


class StopReason(Enum):
    """Why following a reference into another query produced no lineage.

    Only the first three are failures. The rest are queries we understood and
    declined to walk, which the operator has to be told apart from bad M --
    reporting them all as "could not be parsed" sends people looking for a
    malformed query that does not exist.
    """

    PARSE_ERROR = "parse_error"
    BRIDGE_ERROR = "bridge_error"
    TIMEOUT = "timeout"
    NO_LET = "no_let"
    CYCLE = "cycle"
    TOO_DEEP = "too_deep"

    @property
    def title(self) -> str:
        return _STOP_TITLES[self]

    @property
    def message(self) -> str:
        return _STOP_MESSAGES[self]


_REFERENCED = 'Queries with "Enable load" switched off are reached this way.'

# TIMEOUT deliberately reuses the title the m_query_parse_timeout description
# points operators at, so that instruction stays true on this path too.
_STOP_TITLES: Dict["StopReason", str] = {
    StopReason.PARSE_ERROR: "Unable to parse a referenced query",
    StopReason.BRIDGE_ERROR: "Unable to parse a referenced query",
    StopReason.TIMEOUT: "M-Query Parsing Timeout",
    StopReason.NO_LET: "Referenced query is a bare expression",
    StopReason.CYCLE: "Referenced queries form a loop",
    StopReason.TOO_DEEP: "Reference chain is too long to be a real model",
}

_STOP_MESSAGES: Dict["StopReason", str] = {
    StopReason.PARSE_ERROR: "A query this table is built on could not be parsed, so "
    f"the lineage it holds is missing. {_REFERENCED}",
    StopReason.BRIDGE_ERROR: "The M-Query parser failed on a query this table is "
    f"built on, so the lineage it holds is missing. {_REFERENCED}",
    StopReason.TIMEOUT: "Parsing a query this table is built on timed out, so the "
    "lineage it holds is missing. Increase m_query_parse_timeout if this recurs. "
    f"{_REFERENCED}",
    StopReason.NO_LET: "A query this table is built on parsed cleanly but is a bare "
    "expression with no `let` binding to walk, which is not followed -- the same as "
    f"a table's own expression of that shape. {_REFERENCED}",
    StopReason.CYCLE: "Queries this table is built on reference each other in a "
    f"loop, so the chain was stopped and its lineage is missing. {_REFERENCED}",
    StopReason.TOO_DEEP: "A chain of referenced queries ran past "
    f"{MAX_REFERENCE_DEPTH} hops and was stopped, so its lineage is missing. "
    f"{_REFERENCED}",
}


@dataclass
class ExpressionCache:
    """Parse results for one dataset's referenced queries.

    Shared by the dataset's tables so a query reaches the bridge once. The stop
    reason is kept beside the result: a table hitting a cached failure has to be
    told about it too, or only the first table to pay for the parse gets warned.
    """

    parsed: Dict[str, Optional[NodeIdMap]] = field(default_factory=dict)
    stops: Dict[str, Tuple["StopReason", Optional[str]]] = field(default_factory=dict)


@dataclass(frozen=True)
class SharedExpressions:
    """The dataset's shared expressions, and how far into them the walk has gone.

    A query with "Enable load" switched off is not a table in the model, so it
    never gets an entity of its own. Its M lives only here, which is why the
    chain has to be followed inline rather than emitted as an edge to it.

    Frozen because the reference chain identifies a path, not a running total:
    two siblings reaching the same expression are each legitimate, while the same
    expression appearing twice in one chain is a cycle.
    """

    texts: Dict[str, str]
    parse: Callable[[str], NodeIdMap]
    chain: Tuple[str, ...] = ()
    # Both shared across the whole walk rather than per path: parsing is
    # deterministic, so a query reached by several routes should be sent to the
    # bridge once, and a query that failed should not be retried per route --
    # with a timeout, each retry costs the full timeout again.
    # Supplied by the caller so one dataset's tables share it: `texts` is dataset
    # state, so parsing a referenced query once per table wastes a bridge call per
    # table -- and a full m_query_parse_timeout per table when it times out.
    cache: ExpressionCache = field(default_factory=ExpressionCache)
    # name -> (why we stopped, detail for the operator). Structured rather than
    # free text so the caller can report each reason under its own title and
    # counter instead of calling every one of them a parse failure.
    stops: Dict[str, Tuple["StopReason", Optional[str]]] = field(default_factory=dict)

    def lookup(self, name: str) -> Optional[str]:
        """The M text bound to a name, or None if the dataset has no such query."""
        text = resolve_parameter_value(self.texts, name)
        if text is None:
            return None
        if PARAMETER_QUERY_MARKER in text:
            logger.debug("'%s' is a parameter query, not following it", name)
            return None
        return text

    def parsed(self, name: str, text: str) -> Optional[NodeIdMap]:
        """Parse a query, once per dataset however many tables and routes reach it.

        Returns None when the query cannot be parsed. The reason is recorded on
        this walk every time, including on a cache hit, so each table that
        depends on a broken query is told rather than only the first one.
        Anything other than a parse, bridge or timeout failure is a defect rather
        than bad input, and is left to propagate.
        """
        key = name.casefold()
        if key in self.cache.parsed:
            result = self.cache.parsed[key]
            if result is None:
                reason, detail = self.cache.stops[key]
                self.stopped(name, reason, detail)
            return result

        try:
            self.cache.parsed[key] = self.parse(text)
        except MQueryParseError as e:
            self._failed(name, key, StopReason.PARSE_ERROR, str(e))
        except MQueryBridgeError as e:
            self._failed(name, key, StopReason.BRIDGE_ERROR, str(e))
        except TimeoutException as e:
            self._failed(name, key, StopReason.TIMEOUT, str(e))

        return self.cache.parsed[key]

    def _failed(self, name: str, key: str, reason: "StopReason", detail: str) -> None:
        self.cache.parsed[key] = None
        self.cache.stops[key] = (reason, detail)
        self.stopped(name, reason, detail)

    def stopped(
        self, name: str, reason: "StopReason", detail: Optional[str] = None
    ) -> None:
        """Record why a referenced query yielded nothing, for the caller to report."""
        self.stops.setdefault(name, (reason, detail))

    def would_repeat(self, name: str) -> bool:
        return name.casefold() in self.chain

    def exhausted(self) -> bool:
        return len(self.chain) >= MAX_REFERENCE_DEPTH

    def entered(self, name: str) -> "SharedExpressions":
        # replace() re-passes the caches by reference, so they stay shared while
        # the chain forks -- and a field added later cannot be silently dropped.
        return dataclasses.replace(self, chain=self.chain + (name.casefold(),))
