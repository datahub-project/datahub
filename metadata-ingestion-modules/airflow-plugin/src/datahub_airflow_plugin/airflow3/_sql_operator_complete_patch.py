"""Keeps DataHub's completion pass from triggering provider-side lineage emission.

``SQLExecuteQueryOperator.get_openlineage_facets_on_complete`` is
``get_openlineage_facets_on_start()`` merged with
``hook.get_openlineage_database_specific_lineage()``. The DataHub listener reads only
the inputs, outputs and its own SQL-parsing run facet, never the database-specific
part -- but on Snowflake producing that part calls
``emit_openlineage_events_for_snowflake_queries``, which emits OpenLineage START and
COMPLETE events directly to the provider's configured transport with a freshly
generated runId, and queries ``QUERY_HISTORY`` on another connection. Deployments
that keep the OpenLineage provider enabled (for example to feed an external
lineage consumer) therefore receive a duplicate set of query events attributable to
no real run.

``DbApiHook.get_openlineage_database_specific_lineage`` has an empty body, so this
guard changes behaviour only for hooks that override it -- today just Snowflake.

Patching the common-sql base class rather than individual hooks means Python's method
resolution handles the exemptions: wherever DataHub patches a specific operator
(BigQuery, Athena, Teradata), that subclass entry shadows this guard.

**Install this before the operator-specific patches.** They capture their original by
MRO lookup off the subclass, so a guard installed afterwards is shadowed by the
subclass entry they write and never runs.
"""

import functools
import logging
from types import TracebackType
from typing import TYPE_CHECKING, Any, Optional

from datahub_airflow_plugin.airflow3._extraction_scope import in_datahub_extraction

if TYPE_CHECKING:
    from airflow.providers.openlineage.extractors import OperatorLineage

logger = logging.getLogger(__name__)

_PATCHED_MARKER = "_datahub_complete_guard_installed"


def _build_guard(original: Any) -> Any:
    @functools.wraps(original)
    def get_openlineage_facets_on_complete(
        self: Any, task_instance: Any
    ) -> Optional["OperatorLineage"]:
        if in_datahub_extraction():
            logger.debug(
                "DataHub-driven completion pass - skipping database-specific lineage"
            )
            return self.get_openlineage_facets_on_start()
        return original(self, task_instance)

    return get_openlineage_facets_on_complete


class SqlOperatorCompletePatch:
    """Context manager / explicit patcher for the common-sql completion guard.

    Mirrors :class:`SQLParserPatch` so both patches are managed the same way.
    """

    def __init__(self) -> None:
        self._operator_class: Any = None
        self._original: Any = None

    def patch(self) -> "SqlOperatorCompletePatch":
        try:
            from airflow.providers.common.sql.operators.sql import (
                SQLExecuteQueryOperator,
            )
        except ImportError as e:
            logger.debug(f"common-sql provider not available - guard not applied: {e}")
            return self

        if getattr(SQLExecuteQueryOperator, _PATCHED_MARKER, False):
            logger.debug("SQLExecuteQueryOperator completion guard already installed")
            return self

        self._operator_class = SQLExecuteQueryOperator
        self._original = SQLExecuteQueryOperator.get_openlineage_facets_on_complete
        SQLExecuteQueryOperator.get_openlineage_facets_on_complete = _build_guard(  # type: ignore[method-assign]
            self._original
        )
        setattr(SQLExecuteQueryOperator, _PATCHED_MARKER, True)
        logger.debug(
            "Patched SQLExecuteQueryOperator.get_openlineage_facets_on_complete "
            "to skip database-specific lineage during DataHub extraction"
        )
        return self

    def unpatch(self) -> None:
        if self._operator_class is None or self._original is None:
            return
        self._operator_class.get_openlineage_facets_on_complete = self._original
        if hasattr(self._operator_class, _PATCHED_MARKER):
            delattr(self._operator_class, _PATCHED_MARKER)
        self._operator_class = None
        self._original = None

    def __enter__(self) -> "SqlOperatorCompletePatch":
        return self.patch()

    def __exit__(
        self,
        exc_type: Optional[type],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        self.unpatch()


def patch_sql_execute_query_operator() -> None:
    """Install the common-sql completion guard."""
    SqlOperatorCompletePatch().patch()
