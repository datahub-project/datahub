"""Which Fivetran connectors ingestion keeps, as pure functions of the recipe.

Shared by the two log readers (ingestion) and
FivetranSourceConfig.probe_verdict_override (`probe filter`), so that neither
restates the rule.
"""

from dataclasses import dataclass
from typing import Optional, Protocol

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.agent.verdicts import Verdict

CONNECTOR_PATTERNS = "connector_patterns"
DESTINATION_PATTERNS = "destination_patterns"


class ConnectorFilterConfig(Protocol):
    @property
    def connector_patterns(self) -> AllowDenyPattern: ...

    @property
    def destination_patterns(self) -> AllowDenyPattern: ...


@dataclass(frozen=True)
class ConnectorFilters:
    """The two patterns, as the readers receive them (LogReader passes the
    patterns, not the config)."""

    connector_patterns: AllowDenyPattern
    destination_patterns: AllowDenyPattern


@dataclass(frozen=True)
class ConnectorFacts:
    # The connector name: the string DB mode matches, and half of REST's rule.
    name: str
    # None: not known (a connector named bare to `probe filter`); that half of
    # the rule is skipped. Ingestion always knows both.
    connector_id: Optional[str] = None
    destination_id: Optional[str] = None


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
        and facts.connector_id is not None
        and config.connector_patterns.allowed(facts.connector_id)
    ):
        return Verdict(True, None, matched_target=facts.connector_id)
    return Verdict.exclude(CONNECTOR_PATTERNS)


def connector_verdict(
    config: ConnectorFilterConfig, facts: ConnectorFacts, *, rest: bool
) -> Verdict:
    """Both rules, in each reader's order, so the reported reason is the one
    that reader hits first. The DB reader checks the connector and then its
    destination. The REST reader skips a denied destination before it lists
    that destination's connectors."""
    by_connector = connector_pattern_verdict(config, facts, rest=rest)
    by_destination = (
        None
        if facts.destination_id is None
        else destination_verdict(config, facts.destination_id)
    )
    ordered = (by_destination, by_connector) if rest else (by_connector, by_destination)
    for verdict in ordered:
        if verdict is not None and not verdict.included:
            return verdict
    return by_connector
