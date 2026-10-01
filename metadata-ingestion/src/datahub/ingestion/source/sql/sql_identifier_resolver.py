"""Map a caller-supplied SQL identifier to the catalog's own string.

Several SQLAlchemy dialects build their reflection SQL by string formatting --
sqlalchemy-redshift, Vertica, Teradata, ClickHouse, Druid and Databricks
among them -- so a schema or table name handed to an Inspector method is SQL
the probe's gate never saw. Passing reflection only a string the server itself
listed closes that for every dialect, whatever its reflection does, and
without a per-dialect quoting rule to get wrong.
"""

from typing import Iterable, List

from datahub.ingestion.agent.verdicts import ProbeArgumentError

# Enough to recognise the argument; a hostile one can be arbitrarily long.
_MAX_ECHOED = 64
_MAX_HINTS = 5


def _echoed(name: str) -> str:
    clipped = name if len(name) <= _MAX_ECHOED else name[:_MAX_ECHOED] + "..."
    # repr escapes NUL and control characters, so a refusal cannot carry them
    # into a terminal or a log line.
    return repr(clipped)


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
    miss.
    """
    seen: List[str] = []
    for candidate in listed:
        if candidate == name:
            return candidate
        seen.append(candidate)
    folded = name.casefold()
    near = sorted({c for c in seen if c.casefold() == folded})
    message = f"no {what} named {_echoed(name)} {where}"
    if near:
        # The server listed these, but a listed name can still be long, so
        # it is clipped the same way.
        hints = ", ".join(_echoed(c) for c in near[:_MAX_HINTS])
        message += (
            f"; did you mean {hints}? Names are matched exactly, as the "
            f"catalog lists them"
        )
    raise ProbeArgumentError(f"{message}. Run `{list_command}` for the exact names")
