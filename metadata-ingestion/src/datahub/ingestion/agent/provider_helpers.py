"""Shared plumbing for probe providers: name lookup, limits, personal-record
withholding, lazily opened clients, and sub-listings that degrade.

What these helpers raise on purpose is a framework type, so its text is shown
(agent.error_policy): a refusal is a ProbeArgumentError (exit 2) or a
ProbeSoftError, and a misused helper a ProbeInternalError (exit 1). No message
is built from the text of an exception this module did not raise.
"""

import itertools
from contextlib import ExitStack, suppress
from dataclasses import dataclass
from types import TracebackType
from typing import (
    TYPE_CHECKING,
    Callable,
    ClassVar,
    Dict,
    Generic,
    Hashable,
    Iterable,
    Iterator,
    List,
    Optional,
    Sequence,
    Tuple,
    Type,
    TypeVar,
    cast,
)

from typing_extensions import Self

from datahub.ingestion.agent.error_policy import label_foreign_text
from datahub.ingestion.agent.verdicts import (
    ProbeArgumentError,
    ProbeInternalError,
    ProbeSoftError,
)

if TYPE_CHECKING:
    # Annotation only: sql_gate imports sqlglot.
    from datahub.ingestion.agent.sql_gate import CatalogScope

T = TypeVar("T")

# Enough to recognise a value; a hostile argument can be arbitrarily long.
_MAX_ECHOED = 64
# Hints and distinguishing values shown in one refusal.
_MAX_LISTED = 5


def echoed(value: str) -> str:
    """`value` clipped and repr-quoted for a refusal: repr escapes control
    characters, so none reaches a terminal or a log line."""
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
    # Matched by id_key. parent_path holds the raw argument, so a provider
    # that accepted an id should say so.
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

    Exact match on key(record), and on id_key(record) when given; an id match
    wins over another record's equal name. A case-only miss is refused with
    the listed spelling as a hint, since ingestion matches the listed spelling.
    The hint and the ambiguity list print listed names: pass records after
    withholding personal ones.

    `records` is iterated here, so a listing failing part-way fails here.
    stop_at_first consumes only up to the first match (unique names, chained
    listings) and skips the ambiguity check; it cannot take id_key. `on_miss`
    runs before the refusal, to note why a record may be missing or to raise
    a read failure instead. `distinguish` (default `id_key`) labels candidates
    in an ambiguity refusal, `on_ambiguous` says how to pick one, and `where`
    ("in workspace 'x'") must not carry exception text.
    """
    if stop_at_first and id_key is not None:
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
    """The first `limit` items `keep` admits (all when limit is None).

    Pulls nothing past the limit, since a paged API's discarded pages are real
    requests. Closes the source explicitly: some SDK generators hold patched
    state while suspended. A close that fails after a listing or `keep` has
    failed is dropped, so it never replaces the failure that ended the read.
    """
    iterator = iter(items)
    close = getattr(iterator, "close", None)
    try:
        kept: Iterator[T] = iterator if keep is None else filter(keep, iterator)
        bounded = kept if limit is None else itertools.islice(kept, limit)
        result = list(bounded)
    except BaseException:
        if callable(close):
            with suppress(Exception):
                close()
        raise
    if callable(close):
        close()
    return result


@dataclass
class PersonalWithholding(Generic[T]):
    """Leaves out records that name people and that ingestion would not emit,
    counting them for a warning.

    `is_personal` must fail closed (True when unsure): what it misses is
    printed. A predicate, so it serves a raw body and `take(..., keep=w.keep)`
    alike. Only the count is kept, never a withheld record.
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
    """One sub-listing that may degrade: a ProbeSoftError, or an HTTP error
    whose status is in `codes`, becomes a warning, and the fallback after the
    block is the answer. Anything else propagates.

        with soft_listing(self._warn, 403, 404, context="reports listing"):
            return fetch()
        return []

    A class, not a @contextmanager: mypy then sees the fallback as reachable.
    The warning withholds a quoted foreign exception's whole text (labelled
    with generic codes only), but not a part of one (`e.doc`) interpolated
    into a ProbeSoftError's message: never do that.
    """

    def __init__(
        self, warn: Callable[[str], None], *codes: int, context: Optional[str] = None
    ) -> None:
        if codes and context is None:
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
            # Duck-typed on `.response.status_code`, so the framework takes no
            # HTTP-library dependency. Named by context and status only, never
            # by the exception's text.
            status = getattr(getattr(exc, "response", None), "status_code", None)
            if status in self._codes:
                soft = ProbeSoftError(
                    f"{self._context} returned HTTP {status}; treating it as empty."
                )
                soft.__cause__ = exc
        if soft is None:
            return False
        self._warn(label_foreign_text(soft))
        return True


class ProbeProviderBase:
    """An optional base for providers: every attribute the framework reads
    (probe_methods.PROVIDER_ATTRIBUTES), with a default that reads as absent;
    de-duplicated warnings; lazily opened clients; an __exit__ closing them.

    No __init__ (providers have their own, tests build some with __new__);
    state is created on first use. __exit__ closes last-opened first and runs
    every closer; a failure then propagates like any close failure (the
    earliest-registered failing closer's, as it runs last). An overriding
    __exit__ calls super().__exit__(*exc) last. Declare for_config yourself.
    """

    # Required with scoped_sql_param: the dialect sqlglot parses the query as.
    sql_dialect: Optional[str] = None
    # What `probe sql` may read. None is information_schema only.
    catalog_scope: Optional["CatalogScope"] = None
    # Required with scoped_path_param. None, not (): unset is refused as the
    # provider's defect, where () would blame every path the caller tries.
    api_allowlist: Optional[Sequence[str]] = None
    # The URL a path is joined to, so the gate checks the path the client
    # will send. Empty: the gate resolves it against a placeholder.
    api_base_url: str = ""
    # Reads that could not complete (exit 3). A tuple, so no instance appends
    # to a shared default: assign a list per instance.
    failures: Sequence[str] = ()
    # Loggers whose records are dropped while the probe runs (see
    # agent.log_guard.quiet_reused_logs). Read off the class.
    silenced_loggers: ClassVar[Tuple[str, ...]] = ()

    _probe_warnings: Optional[List[str]] = None
    _probe_opened: Optional[Dict[Hashable, object]] = None
    _probe_closers: Optional[ExitStack] = None

    def __init_subclass__(cls, **kwargs: object) -> None:
        super().__init_subclass__(**kwargs)
        # A class-level list would be shared by every instance.
        for name in ("warnings", "failures"):
            if isinstance(vars(cls).get(name), list):
                raise TypeError(
                    f"{cls.__name__} sets a class-level `{name}` list; assign "
                    f"it in __init__ or leave it to ProbeProviderBase"
                )

    @property
    def probe_report(self) -> object:
        """The SourceReport reused ingestion code writes into, whose warnings
        and failures are read back after each command; None when there is
        none. Read-only here, so a subclass overrides it with a property."""
        return None

    @staticmethod
    def probe_error_code(exc: BaseException) -> Optional[str]:
        """The vendor's short code for a foreign exception, or None (see
        agent.error_policy.foreign_label). Asked on the class, so it also
        labels a failure to open the provider."""
        return None

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
        """The client cached under `key`, built by `opener` on first use: in a
        command, so a bad credential is that command's failure. One key per
        kind of client."""
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
