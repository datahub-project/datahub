"""Which Fivetran connectors ingestion keeps, as pure functions of the recipe.

Shared by the two log readers (ingestion) and
FivetranSourceConfig.probe_verdict_override (`probe filter`), so that neither
restates the rule.
"""

from dataclasses import dataclass
from enum import Enum
from typing import Literal, Optional, Protocol, Union

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.agent.verdicts import Verdict

CONNECTOR_PATTERNS = "connector_patterns"
DESTINATION_PATTERNS = "destination_patterns"
# Not a recipe field: a connector whose log row has a NULL destination_id.
# The DB reader matches destination_patterns against the NULL and stops the
# run with a TypeError, so the connector is never emitted.
NULL_DESTINATION_ID = "null_destination_id"


class ConnectorFilterConfig(Protocol):
    @property
    def connector_patterns(self) -> AllowDenyPattern: ...

    @property
    def destination_patterns(self) -> AllowDenyPattern: ...


class _Unknown(Enum):
    UNKNOWN = "unknown"


# A fact `probe filter` was not given (a connector named bare): the rule it
# decides is skipped. Not None, because a NULL destination_id is a value the
# log holds, and ingestion acts on it.
UNKNOWN: Literal[_Unknown.UNKNOWN] = _Unknown.UNKNOWN
MaybeConnectorId = Union[str, _Unknown]
# None is a NULL destination_id in the log (DB mode only).
MaybeDestinationId = Union[Optional[str], _Unknown]


@dataclass(frozen=True, kw_only=True)
class ConnectorFilters:
    """The two patterns, as the readers receive them (LogReader passes the
    patterns, not the config). Keyword-only: both are AllowDenyPatterns, so a
    swapped pair would still type-check."""

    connector_patterns: AllowDenyPattern
    destination_patterns: AllowDenyPattern


@dataclass(frozen=True, kw_only=True)
class ConnectorFacts:
    # The connector name: the string DB mode matches, and half of REST's rule.
    name: str
    # UNKNOWN for a connector named bare to `probe filter`; that half of the
    # rule is skipped. Ingestion always knows both.
    connector_id: MaybeConnectorId = UNKNOWN
    destination_id: MaybeDestinationId = UNKNOWN


def destination_verdict(config: ConnectorFilterConfig, destination_id: str) -> Verdict:
    if config.destination_patterns.allowed(destination_id):
        return Verdict.include()
    return Verdict.exclude(DESTINATION_PATTERNS)


def connector_pattern_verdict(
    config: ConnectorFilterConfig, facts: ConnectorFacts, *, rest: bool
) -> Verdict:
    """DB mode matches the name only, which keeps existing deny lists meaning
    what they did. REST keeps a connector when its id OR its name is
    allowed. When only the id is allowed, the id is what matched."""
    if config.connector_patterns.allowed(facts.name):
        return Verdict.include()
    if (
        rest
        and facts.connector_id is not UNKNOWN
        and config.connector_patterns.allowed(facts.connector_id)
    ):
        return Verdict(included=True, matched_target=facts.connector_id)
    return Verdict.exclude(CONNECTOR_PATTERNS)


def connector_verdict(
    config: ConnectorFilterConfig, facts: ConnectorFacts, *, rest: bool
) -> Verdict:
    """Both rules, in each reader's order, so the reported reason is the one
    that reader hits first. The DB reader checks the connector and then its
    destination. The REST reader skips a denied destination before it lists
    that destination's connectors."""
    by_connector = connector_pattern_verdict(config, facts, rest=rest)
    by_destination: Optional[Verdict]
    if facts.destination_id is UNKNOWN:
        by_destination = None
    elif facts.destination_id is None:
        # Reached only once connector_patterns keeps the connector: the DB
        # reader checks the name first, so a denied one never gets this far.
        by_destination = Verdict.exclude(NULL_DESTINATION_ID)
    else:
        by_destination = destination_verdict(config, facts.destination_id)
    ordered = (by_destination, by_connector) if rest else (by_connector, by_destination)
    for verdict in ordered:
        if verdict is not None and not verdict.included:
            return verdict
    return by_connector
