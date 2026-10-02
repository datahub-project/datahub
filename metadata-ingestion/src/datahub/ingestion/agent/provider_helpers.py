"""Plumbing a probe provider would otherwise write for itself.

Every connector that gained probe support hand-wrote some of these -- a name
lookup with an ambiguity error, a counter for withheld personal records, a
listing that stops paging at the limit, a lazily built client closed on exit,
a 403 that degrades to a warning -- and the copies drifted in wording, in the
errors they raised, and in whether they closed what they opened.

Everything here is framework-authored, but error_policy.is_authored does not
vouch for this module by file path: it calls provider callables that may be C
code, whose failures would surface with this module's frame innermost. So
what it raises on purpose is a framework type, trusted by type: a
caller-facing refusal is a ProbeArgumentError (exit 2) or a ProbeSoftError, a
helper misused by the provider is a ProbeInternalError (exit 1), and no
message is ever built from the text of an exception this module did not
raise.
"""

import itertools
from contextlib import ExitStack
from dataclasses import dataclass
from types import TracebackType
from typing import (
    Callable,
    Dict,
    Generic,
    Hashable,
    Iterable,
    Iterator,
    List,
    Optional,
    Type,
    TypeVar,
    cast,
)

from typing_extensions import Self

from datahub.ingestion.agent.error_policy import withhold_foreign_text
from datahub.ingestion.agent.verdicts import (
    ProbeArgumentError,
    ProbeInternalError,
    ProbeSoftError,
    soft_error_for,
)

T = TypeVar("T")

# Enough to recognise a value; a hostile argument can be arbitrarily long.
_MAX_ECHOED = 64
# Hints and distinguishing values shown in one refusal.
_MAX_LISTED = 5


def echoed(value: str) -> str:
    """`value` clipped and repr-quoted, safe to put in a refusal.

    repr escapes NUL and control characters, so neither a caller's argument
    nor a listed name can carry them into a terminal or a log line.
    """
    clipped = value if len(value) <= _MAX_ECHOED else value[:_MAX_ECHOED] + "..."
    return repr(clipped)


def _plain(value: str) -> str:
    # echoed without the quotes: ids and resource-group names in a list.
    return echoed(value)[1:-1]


@dataclass(frozen=True)
class Resolved(Generic[T]):
    record: T
    # The listed spelling, key(record): what a pattern is matched against.
    name: str
    # Matched by id_key, and the caller's argument is not the name. The
    # framework builds parent_path from the raw argument, so a caller that
    # accepted an id should say so (Fabric's _warn_if_resolved_by_id).
    by_id: bool


def resolve_name(
    arg: str,
    records: Iterable[T],
    *,
    key: Callable[[T], str],
    kind: str,
    where: str = "",
    list_command: Optional[str] = None,
    id_key: Optional[Callable[[T], Optional[str]]] = None,
    distinguish: Optional[Callable[[T], Optional[str]]] = None,
    on_ambiguous: str = "",
    on_miss: Optional[Callable[[], None]] = None,
    stop_at_first: bool = False,
) -> Resolved[T]:
    """The record in `records` the caller's `arg` names, or ProbeArgumentError.

    Matching is exact, on key(record) -- and on id_key(record) when given,
    where an id match wins over another record's equal name, since ids are
    unique and names are not. A name that differs only in case is refused,
    with the listed spelling as a hint: ingestion matches patterns against
    the listed spelling, so accepting another would have `probe filter`
    judge a name ingestion never sees.

    The hint and the ambiguity list are drawn from `records` only. **Pass
    records after withholding personal ones** (PersonalWithholding), or a
    hint could print an owner's name the listing itself withheld.

    `records` is iterated inside this call, so a listing that fails part-way
    fails here with its own exception (the framework reports a foreign one
    by class name). With stop_at_first, it is consumed only up to the first
    match -- for listings whose names are unique, and so that a caller can
    chain listings and pay for later ones only on a miss -- and ambiguity is
    not checked. It cannot be combined with id_key: the first name match
    would win over a later id match, the reverse of the precedence above.

    `on_miss` runs before the refusal: to note why a record may be missing
    (Power BI's withheld count), or to raise a read failure instead when an
    unread listing could hold it (Fivetran, Fabric). `distinguish` (default
    `id_key`) labels each candidate in an ambiguity refusal; `on_ambiguous`
    tells the caller how to pick one. `where` follows the kind ("in
    workspace 'x'"); it must not carry exception text.
    """
    if stop_at_first and id_key is not None:
        # ProbeInternalError, not TypeError: this module is not vouched for,
        # so a TypeError's text would be withheld from the provider's author.
        raise ProbeInternalError(
            "resolve_name cannot take stop_at_first with an id_key"
        )
    by_id: List[T] = []
    by_name: List[T] = []
    others: List[str] = []
    for record in records:
        name = key(record)
        id_matches = id_key is not None and id_key(record) == arg
        if not id_matches and name != arg:
            others.append(name)
            continue
        if stop_at_first:
            return Resolved(record=record, name=name, by_id=name != arg)
        (by_id if id_matches else by_name).append(record)
    matches = by_id or by_name
    if len(matches) == 1:
        name = key(matches[0])
        return Resolved(record=matches[0], name=name, by_id=name != arg)
    if not matches:
        if on_miss is not None:
            on_miss()
        raise ProbeArgumentError(
            _miss_message(arg, others, kind, where, list_command, id_key is not None)
        )
    raise ProbeArgumentError(
        _ambiguous_message(
            arg, matches, kind, where, distinguish or id_key, on_ambiguous
        )
    )


def _miss_message(
    arg: str,
    others: List[str],
    kind: str,
    where: str,
    list_command: Optional[str],
    accepts_id: bool,
) -> str:
    named = "named or with id" if accepts_id else "named"
    message = f"no {kind} {named} {echoed(arg)}"
    if where:
        message += f" {where}"
    folded = arg.casefold()
    near = sorted({n for n in others if n.casefold() == folded})
    if near:
        hints = ", ".join(echoed(n) for n in near[:_MAX_LISTED])
        message += (
            f"; did you mean {hints}? Names are matched exactly, as the source "
            f"lists them"
        )
    if list_command:
        message += f". Run `{list_command}` for the exact names"
    return message


def _ambiguous_message(
    arg: str,
    matches: List[T],
    kind: str,
    where: str,
    distinguish: Optional[Callable[[T], Optional[str]]],
    remedy: str,
) -> str:
    labels: List[str] = []
    if distinguish is not None:
        labels = sorted({_plain(v) for v in map(distinguish, matches) if v})
    message = f"{echoed(arg)} names more than one {kind}"
    if labels:
        shown = ", ".join(labels[:_MAX_LISTED])
        more = ", ..." if len(labels) > _MAX_LISTED else ""
        message += f" ({shown}{more})"
    if where:
        message += f" {where}"
    if remedy:
        message += f"; {remedy}"
    return message


def take(
    items: Iterable[T],
    limit: Optional[int],
    *,
    keep: Optional[Callable[[T], bool]] = None,
) -> List[T]:
    """The first `limit` items `keep` admits (every one when limit is None).

    Pulls nothing past the limit: on a paged API the discarded pages are real
    requests, and the framework asks for limit+1 to detect truncation, so
    returning exactly what was asked is correct. The source is closed
    explicitly, finished or not, rather than left to the garbage collector:
    some SDK generators hold patched state while suspended.
    """
    iterator = iter(items)
    try:
        kept: Iterator[T] = iterator if keep is None else filter(keep, iterator)
        if limit is None:
            return list(kept)
        return list(itertools.islice(kept, limit))
    finally:
        close = getattr(iterator, "close", None)
        if callable(close):
            close()


@dataclass
class PersonalWithholding(Generic[T]):
    """Leaves out records that name people and that ingestion would not emit,
    and counts them so the listing can say how many.

    `is_personal` must fail closed -- True when unsure -- because what it
    misses is printed. Databricks lists notebook paths from an allowlist
    (/Shared/) for exactly that reason. A record the recipe ingests is shown
    whatever it is: ingestion emits it anyway.

    A predicate rather than a filtered list, so it works on a raw body
    (`[r for r in body if w.keep(r)]`) and on a listing that must stop at the
    limit (`take(pages, limit, keep=w.keep)`). Only the count is kept, never
    a withheld record.
    """

    is_personal: Callable[[T], bool]
    would_ingest: Callable[[T], bool]
    withheld: int = 0

    def keep(self, record: T) -> bool:
        if self.is_personal(record) and not self.would_ingest(record):
            self.withheld += 1
            return False
        return True

    def count_text(self, *, stopped_early: bool) -> str:
        """The count for a warning. A walk that stopped at the limit saw only
        part of the source, so its count is a lower bound."""
        return f"at least {self.withheld}" if stopped_early else str(self.withheld)


class soft_listing:
    """One sub-listing that may degrade: a ProbeSoftError -- or an HTTP error
    whose status is in `codes` -- becomes a warning, and the caller's own
    fallback after the block is the answer.

        with soft_listing(self._warn, 403, 404, context="reports listing"):
            return fetch()
        return []

    "Could not look" must never read as "nothing here", which is why the
    reason is always recorded. Everything else propagates untouched, so auth
    and 5xx failures still fail the command. A class rather than a
    @contextmanager because mypy treats a `with` as possibly suppressing only
    when __exit__ returns bool: the fallback stays reachable, and a missing
    one is reported as a missing return.

    The recorded text has any foreign exception's text withheld (class name
    instead) -- a backstop for a connector translator that quoted one.
    Do not interpolate parts of a foreign exception (an attribute such as
    `e.doc`) into a ProbeSoftError's message: the backstop withholds only
    the foreign exception's whole text, so a quoted part reaches the warning.
    """

    def __init__(
        self, warn: Callable[[str], None], *codes: int, context: Optional[str] = None
    ) -> None:
        if codes and context is None:
            # ProbeInternalError, not TypeError: this module is not vouched
            # for, so a TypeError's text would be withheld from the author.
            raise ProbeInternalError(
                "soft_listing needs a context to map HTTP statuses"
            )
        self._warn = warn
        self._codes = codes
        self._context = context or ""

    def __enter__(self) -> None:
        return None

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc: Optional[BaseException],
        tb: Optional[TracebackType],
    ) -> bool:
        if not isinstance(exc, Exception):
            return False
        soft = exc if isinstance(exc, ProbeSoftError) else None
        if soft is None and self._codes:
            soft = soft_error_for(exc, self._codes, self._context)
            if soft is not None:
                soft.__cause__ = exc
        if soft is None:
            return False
        # No provider files: every non-framework exception in the chain counts
        # as foreign, which only ever withholds more.
        self._warn(withhold_foreign_text(soft, frozenset()))
        return True


class ProbeProviderBase:
    """An optional base for probe providers: de-duplicated warnings, lazily
    built clients, and an __exit__ that closes everything that was opened.

    Opt-in. The framework never looks it up -- it is not a config hook --
    and a provider without it is unaffected. It defines no __init__, because
    every provider has its own and tests build some with __new__; all state
    here is created on first use. A subclass still declares for_config
    itself.

    __exit__ closes last-opened first and runs every closer even when one
    fails; that failure then propagates, and the framework reports it as it
    reports any close failure (by class name when foreign, never replacing
    the command's own failure). When several closers fail, the
    earliest-registered one's failure is reported, because it runs last;
    the earlier-run failures are not shown. A subclass that overrides __exit__
    calls super().__exit__(*exc) last.
    """

    _probe_warnings: Optional[List[str]] = None
    _probe_opened: Optional[Dict[Hashable, object]] = None
    _probe_closers: Optional[ExitStack] = None

    def __init_subclass__(cls, **kwargs: object) -> None:
        super().__init_subclass__(**kwargs)
        # A class-level list replaces the warnings property with one list
        # shared by every instance, so one probe's warnings would carry into
        # the next. An annotation alone, or assignment in __init__, is fine.
        if isinstance(vars(cls).get("warnings"), list):
            raise TypeError(
                f"{cls.__name__} sets a class-level `warnings` list; assign "
                f"it in __init__ or leave it to ProbeProviderBase"
            )

    @property
    def warnings(self) -> List[str]:
        """Read back by run_probe_method after each command."""
        if self._probe_warnings is None:
            self._probe_warnings = []
        return self._probe_warnings

    @warnings.setter
    def warnings(self, value: List[str]) -> None:
        self._probe_warnings = value

    def _warn(self, message: str) -> None:
        if message not in self.warnings:
            self.warnings.append(message)

    def _on_exit(self, close: Callable[[], object]) -> None:
        if self._probe_closers is None:
            self._probe_closers = ExitStack()
        self._probe_closers.callback(close)

    def _open_once(
        self,
        key: Hashable,
        opener: Callable[[], T],
        *,
        close: Optional[Callable[[T], object]] = None,
    ) -> T:
        """The client cached under `key`, built by `opener` on first use.

        Built in a command rather than in for_config, so a bad credential
        surfaces as that command's failure. One key per kind of client: two
        openers under one key would share the first one's object.
        """
        if self._probe_opened is None:
            self._probe_opened = {}
        if key in self._probe_opened:
            return cast(T, self._probe_opened[key])
        client = opener()
        self._probe_opened[key] = client
        if close is not None:
            self._on_exit(lambda: close(client))
        return client

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc: object) -> None:
        closers, self._probe_closers = self._probe_closers, None
        self._probe_opened = None
        if closers is not None:
            closers.close()
