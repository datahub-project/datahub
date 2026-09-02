import dataclasses
import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Callable, Dict, Mapping, Optional, Set, Tuple

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


_REFERENCED = 'Queries with "Enable load" switched off are reached this way.'


class StopReason(Enum):
    """Why following a reference into another query produced no lineage.

    Each member carries its own operator-facing copy and whether it counts as a
    failure, so a member cannot be declared without them -- the lookup tables
    this replaced could not say that, and a missing entry raised from inside the
    reporter's `finally`, where it would have replaced the real exception.

    `route_dependent` separates "this query is broken" -- true on every route
    that reaches it -- from "this particular route stopped", which another route
    may still walk to a data source.
    """

    def __init__(
        self, title: str, message: str, is_failure: bool, route_dependent: bool = False
    ) -> None:
        self.title = title
        self.message = message
        self.is_failure = is_failure
        self.route_dependent = route_dependent

    PARSE_ERROR = (
        "Unable to parse a referenced query",
        "A query this table is built on could not be parsed, so the lineage it "
        f"holds is missing. {_REFERENCED}",
        True,
    )
    BRIDGE_ERROR = (
        "Unable to parse a referenced query",
        "The M-Query parser failed on a query this table is built on, so the "
        f"lineage it holds is missing. {_REFERENCED}",
        True,
    )
    # Reuses the title m_query_parse_timeout's description points operators at,
    # so that instruction stays true here. It deliberately does not touch
    # m_query_parse_timeouts -- see _report_referenced_query_stops.
    TIMEOUT = (
        "M-Query Parsing Timeout",
        "Parsing a query this table is built on timed out, so the lineage it "
        "holds is missing. Increase m_query_parse_timeout if this recurs. "
        f"{_REFERENCED}",
        True,
    )
    NO_LET = (
        "Referenced query is a bare expression",
        "A query this table is built on parsed cleanly but is a bare expression "
        "with no `let` binding to walk, which is not followed -- the same as a "
        f"table's own expression of that shape. {_REFERENCED}",
        False,
    )
    # Route-dependent: the message says the branch stopped, not that the table
    # has no lineage, because another route may have reached a data source.
    CYCLE = (
        "Referenced queries form a loop",
        "Queries this table is built on reference each other in a loop, so that "
        f"branch of the chain was stopped. {_REFERENCED}",
        False,
        True,
    )
    TOO_DEEP = (
        "Reference chain is too long to be a real model",
        f"A chain of referenced queries ran past {MAX_REFERENCE_DEPTH} hops, so "
        f"that branch was stopped. {_REFERENCED}",
        False,
        True,
    )


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
    # The dataset's loaded tables, name -> full name. A referenced name matching
    # one of these is a sibling table, which has an entity of its own, so it
    # becomes an edge instead of a chain to follow. Empty when the caller has
    # switched table-to-table lineage off, which makes the walk ignore them.
    tables: Mapping[str, str] = field(default_factory=dict)
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
    # Full names of sibling tables this walk reached. Collected on the walk
    # rather than returned, because the chain forks per path while the set
    # belongs to the table being resolved.
    table_refs: Set[str] = field(default_factory=set)
    # Names that resolved to nothing at all: not a step in scope, not a sibling
    # table, not one of the dataset's other queries. Almost always an M
    # parameter or an unsupported source's argument, so this is a denominator
    # for the operator rather than a failure -- without it, a model that should
    # have table-to-table edges and has none gives the report nothing to show.
    unresolved: Set[str] = field(default_factory=set)

    def sibling(self, name: str) -> Optional[str]:
        """The full name of the loaded table this name refers to, if any.

        Resolved the way every other name here is -- unquoted and
        case-insensitively -- so `#"Base Rows"` finds the table `Base Rows`.
        """
        return resolve_parameter_value(self.tables, name)

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
        """Record why a referenced query yielded nothing, for the caller to report.

        Keyed the way every other lookup here is, so one query referenced as
        `#"Base"` and `#"base"` is one entry rather than two.

        A route-dependent reason yields to a real failure: reaching a query at
        the depth cap down one branch says nothing about the query, so it must
        not mask a parse error found on a shorter route. Otherwise the first
        reason stands -- the rest are properties of the query text, identical
        however the walk arrives.
        """
        key = name.casefold()
        existing = self.stops.get(key)
        if existing is not None and not existing[0].route_dependent:
            return
        if existing is not None and reason.route_dependent:
            return
        self.stops[key] = (reason, detail)

    def would_repeat(self, name: str) -> bool:
        return name.casefold() in self.chain

    def exhausted(self) -> bool:
        return len(self.chain) >= MAX_REFERENCE_DEPTH

    def entered(self, name: str) -> "SharedExpressions":
        # replace() re-passes the caches by reference, so they stay shared while
        # the chain forks -- and a field added later cannot be silently dropped.
        return dataclasses.replace(self, chain=self.chain + (name.casefold(),))
