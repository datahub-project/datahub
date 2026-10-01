"""Map a caller-supplied SQL identifier to the catalog's own string.

Several SQLAlchemy dialects build their reflection SQL by string formatting --
sqlalchemy-redshift, Vertica, Teradata, ClickHouse, Druid and Databricks
among them -- so a schema or table name handed to an Inspector method is SQL
the probe's gate never saw. Passing reflection only a string the server itself
listed closes that for every dialect, whatever its reflection does, and
without a per-dialect quoting rule to get wrong.
"""

from typing import Iterable

from datahub.ingestion.agent.provider_helpers import echoed, resolve_name

# The old private name, kept because mssql_probe imports it on the stacked
# SQL branch. New code imports agent.provider_helpers.echoed.
_echoed = echoed


def _listed_string(name: str) -> str:
    return name


def resolve_listed_name(
    name: str,
    listed: Iterable[str],
    *,
    what: str,
    where: str,
    list_command: str,
) -> str:
    """The element of `listed` equal to `name`, or ProbeArgumentError (exit 2).

    Returns the catalog's object, never the caller's, so whatever reaches a
    reflection call is something the server produced.

    Exact match only. A name that differs only in case is refused with the
    listed spelling as a hint rather than accepted: ingestion reflects and
    pattern-matches the listed spelling, and the probe reports the caller's
    argument as the parent path, so accepting `PUBLIC` for `public` would
    have `probe filter` judge a name ingestion never sees -- wrongly, under
    `ignoreCase: false`.

    `listed` is consumed lazily and only up to the first exact match, so a
    caller can chain several listings and pay for the later ones only on a
    miss. The SQL-catalog face of agent.provider_helpers.resolve_name.
    """
    return resolve_name(
        name,
        listed,
        key=_listed_string,
        kind=what,
        where=where,
        list_command=list_command,
        stop_at_first=True,
    ).record
