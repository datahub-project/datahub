from typing import Optional, Sequence

from datahub.ingestion.source.montecarlo.queries import TABLE_MONITOR_QUERY
from datahub.ingestion.source.montecarlo.query_builder import (
    MonteCarloGraphQLType,
    SchemaDrift,
    alerts_fallback_query,
    custom_rules_fallback_query,
    evaluate_asset_selection_fallback_query,
    get_job_executions_fallback_query,
    get_metrics_v4_fallback_query,
    get_schema_changes_fallback_query,
    get_table_fallback_query,
    monitors_fallback_query,
)


class StaticQueryBuilder:
    """Returns fixed query strings verbatim for unit tests that mock ``client._call``."""

    def __init__(
        self,
        monitors_query: str = monitors_fallback_query(),
        custom_rules_query: str = custom_rules_fallback_query(),
        alerts_query: str = alerts_fallback_query(),
    ) -> None:
        self._monitors = monitors_query
        self._custom_rules = custom_rules_query
        self._alerts = alerts_query

    def monitors_query(self) -> str:
        return self._monitors

    def custom_rules_query(self) -> str:
        return self._custom_rules

    def alerts_query(self) -> str:
        return self._alerts

    def get_table_query(self) -> str:
        return get_table_fallback_query()

    def table_monitor_query(self) -> str:
        return TABLE_MONITOR_QUERY

    def evaluate_asset_selection_query(self) -> str:
        return evaluate_asset_selection_fallback_query()

    def get_schema_changes_query(self) -> str:
        return get_schema_changes_fallback_query()

    def get_job_executions_query(self) -> str:
        return get_job_executions_fallback_query()

    def get_metrics_v4_query(self) -> str:
        return get_metrics_v4_fallback_query()

    def check_drift(
        self,
        strict: bool = False,
        types: Optional[Sequence[MonteCarloGraphQLType]] = None,
    ) -> SchemaDrift:
        return SchemaDrift()
