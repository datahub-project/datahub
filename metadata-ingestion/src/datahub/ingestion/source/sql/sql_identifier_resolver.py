"""Map a caller-supplied SQL identifier to the catalog's own string.

Several SQLAlchemy dialects build reflection SQL by string formatting, so a
name handed to an Inspector method is SQL the probe's gate never saw. Passing
reflection only a string the server itself listed closes that for every
dialect, with no per-dialect quoting rule to get wrong.
"""

from typing import Iterable

from datahub.ingestion.agent.provider_helpers import resolve_name


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

    Returns the catalog's string, never the caller's. Exact match: a case-only
    miss is refused with the listed spelling as a hint, since ingestion
    reflects and matches the listed spelling. `listed` is consumed only up to
    the first match, so chained listings cost only on a miss.
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
