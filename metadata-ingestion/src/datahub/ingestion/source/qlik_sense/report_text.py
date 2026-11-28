from typing import List, Optional

from datahub.ingestion.source.qlik_sense.data_classes import (
    App,
)


def bounded_list(items: List[str], limit: int = 10) -> str:
    more = f" (and {len(items) - limit} more)" if len(items) > limit else ""
    return ", ".join(items[:limit]) + more


def describe_error(error: Exception) -> str:
    # The type tells failures apart when the message is empty or generic.
    return f"{type(error).__name__}: {error}" if str(error) else type(error).__name__


def error_type(error: Optional[Exception]) -> str:
    # SQL parser errors quote the statement, which can hold credentials when a
    # script statement was split wrongly, so only the type goes in the report.
    return type(error).__name__ if error else "unknown"


def read_errors_context(app: App) -> str:
    first, *rest = app.read_errors
    more = f" (and {len(rest)} more)" if rest else ""
    return f"{app.id}: {first}{more}"
