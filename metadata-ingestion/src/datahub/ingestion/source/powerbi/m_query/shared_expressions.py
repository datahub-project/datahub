import logging
from dataclasses import dataclass, field
from typing import Callable, Dict, Optional, Tuple

from datahub.ingestion.source.powerbi.m_query.ast_utils import (
    NodeIdMap,
    resolve_parameter_value,
)

logger = logging.getLogger(__name__)

# A reference chain longer than this is not a real model shape; it is a sign the
# walk is going somewhere unproductive.
MAX_REFERENCE_DEPTH = 10

# Marker Power BI puts on an expression that holds a parameter value rather than
# a query. Following one leads to a literal, never to a data source.
PARAMETER_QUERY_MARKER = "IsParameterQuery=true"


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
    # Parsing is deterministic and a query reached by several routes would
    # otherwise be sent to the bridge once per route, so the result is kept for
    # the whole walk rather than per path.
    _parsed: Dict[str, NodeIdMap] = field(default_factory=dict)

    def parse_once(self, name: str, text: str) -> NodeIdMap:
        key = name.casefold()
        if key not in self._parsed:
            self._parsed[key] = self.parse(text)
        return self._parsed[key]

    def lookup(self, name: str) -> Optional[str]:
        """The M text bound to a name, or None if the dataset has no such query."""
        text = resolve_parameter_value(self.texts, name)
        if text is None:
            return None
        if PARAMETER_QUERY_MARKER in text:
            logger.debug("'%s' is a parameter query, not following it", name)
            return None
        return text

    def would_repeat(self, name: str) -> bool:
        return name.casefold() in self.chain

    def exhausted(self) -> bool:
        return len(self.chain) >= MAX_REFERENCE_DEPTH

    def entered(self, name: str) -> "SharedExpressions":
        return SharedExpressions(
            texts=self.texts,
            parse=self.parse,
            chain=self.chain + (name.casefold(),),
            _parsed=self._parsed,
        )
