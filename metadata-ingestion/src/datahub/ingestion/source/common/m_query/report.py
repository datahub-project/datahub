from dataclasses import dataclass, field

from datahub.utilities.perf_timer import PerfTimer


@dataclass
class MQueryLineageReport:
    # Counters for the shared M-Query lineage engine. Connector reports mix this
    # in so the engine can record parse/resolve outcomes on the same report the
    # rest of the connector uses.
    m_query_parse_timer: PerfTimer = field(default_factory=PerfTimer)
    m_query_parse_attempts: int = 0
    m_query_parse_successes: int = 0
    m_query_parse_timeouts: int = 0
    m_query_native_query_skipped: int = 0
    # Expressions that reached the parser but are not M-Query at all
    # (e.g. DAX computed-table expressions, empty strings, label rows).
    # These fail with MQueryParseError but are expected and logged at INFO.
    m_query_non_mquery_expressions: int = 0
    m_query_parse_validation_errors: int = 0
    m_query_parse_unexpected_character_errors: int = 0
    # Genuine M-Query expressions that the parser could not handle.
    m_query_parse_unknown_errors: int = 0
    m_query_resolver_errors: int = 0
    m_query_resolver_no_lineage: int = 0
    m_query_resolver_successes: int = 0
