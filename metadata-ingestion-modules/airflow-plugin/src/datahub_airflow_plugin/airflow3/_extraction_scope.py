"""Marks the window in which the DataHub listener is driving OpenLineage extraction.

Airflow's OpenLineage provider runs ``get_openlineage_facets_on_*`` inside
``os.fork()`` (``openlineage/plugins/listener.py``, ``_fork_execute``), which is why
the provider treats those methods as safe to re-run -- see the comment on
``should_use_external_connection`` in ``openlineage/utils/utils.py``. The DataHub
listener calls the same methods in the task-runner process, so it inherits their
side effects (warehouse ``information_schema`` lookups, and for Snowflake the direct
emission of OpenLineage query events) without that process isolation.

DataHub consumes only the inputs, outputs and its own SQL-parsing run facet, so
while this scope is active the patches short-circuit the work DataHub does not use.
The provider's forked pass never enters this scope, so its own events are unchanged.

This module deliberately imports nothing from the plugin: both the listener and the
patches import it, and the patches already work around circular imports.
"""

from contextlib import contextmanager
from contextvars import ContextVar
from typing import Iterator

_IN_DATAHUB_EXTRACTION: ContextVar[bool] = ContextVar(
    "datahub_in_extraction", default=False
)


def in_datahub_extraction() -> bool:
    """Whether the current call stack originates from the DataHub listener."""
    return _IN_DATAHUB_EXTRACTION.get()


@contextmanager
def datahub_extraction_scope() -> Iterator[None]:
    """Mark the enclosed block as DataHub-driven OpenLineage extraction."""
    token = _IN_DATAHUB_EXTRACTION.set(True)
    try:
        yield
    finally:
        _IN_DATAHUB_EXTRACTION.reset(token)
