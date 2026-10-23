"""agent.provider_helpers: the plumbing every probe provider shares."""

import functools
from dataclasses import dataclass
from typing import Callable, Iterator, List, Optional

import pytest

from datahub.ingestion.agent import probe_methods
from datahub.ingestion.agent.probe_methods import (
    ProbeMethodResult,
    ProbeProvider,
    probe_method,
    run_probe_method,
)
from datahub.ingestion.agent.provider_helpers import (
    PersonalWithholding,
    ProbeProviderBase,
    Resolved,
    echoed,
    resolve_name,
    soft_listing,
    take,
)
from datahub.ingestion.agent.verdicts import (
    ProbeArgumentError,
    ProbeConnectionError,
    ProbeInternalError,
    ProbeReadFailed,
    ProbeSoftError,
)
from tests.unit.agent import _foreign_errors
from tests.unit.agent._foreign_errors import SENTINEL


@dataclass(frozen=True)
class _Ws:
    name: str
    id: str
    type: str = "Workspace"


_LISTED = [
    _Ws("Sales", "g-1"),
    _Ws("ops", "g-2"),
    _Ws("dup", "g-3"),
    _Ws("dup", "g-4", type="Warehouse"),
]


def _by_name(ws: _Ws) -> str:
    return ws.name


def _by_id(ws: _Ws) -> str:
    return ws.id


def _resolve(arg: str, records: List[_Ws] = _LISTED) -> Resolved[_Ws]:
    return resolve_name(
        arg,
        records,
        key=_by_name,
        kind="workspace",
        where="visible to this credential",
        list_command="probe run workspaces",
    )


def test_an_exact_name_returns_the_listed_record_itself() -> None:
    got = _resolve("ops")
    assert got.record is _LISTED[1]
    assert (got.name, got.by_id) == ("ops", False)


def test_a_case_only_mismatch_is_refused_with_the_listed_spelling() -> None:
    with pytest.raises(ProbeArgumentError) as info:
        _resolve("SALES")
    message = str(info.value)
    assert isinstance(info.value, ValueError)  # exit 2 family
    assert "no workspace named 'SALES' visible to this credential" in message
    assert "did you mean 'Sales'?" in message
    assert "probe run workspaces" in message


def test_an_unrelated_name_gets_no_hint() -> None:
    with pytest.raises(ProbeArgumentError) as info:
        _resolve("nope")
    assert "did you mean" not in str(info.value)


def test_an_id_resolves_and_says_it_was_matched_by_id() -> None:
    got = resolve_name("g-1", _LISTED, key=_by_name, id_key=_by_id, kind="workspace")
    assert got.record is _LISTED[0]
    assert (got.name, got.by_id) == ("Sales", True)


def test_an_id_wins_over_another_records_equal_name() -> None:
    # Ids are unique and names are not: Fivetran's rule.
    records = [_Ws("g-9", "g-1"), _Ws("x", "g-9")]
    got = resolve_name("g-9", records, key=_by_name, id_key=_by_id, kind="connector")
    assert got.record is records[1]


def test_a_miss_with_an_id_key_says_name_or_id() -> None:
    with pytest.raises(ProbeArgumentError, match="named or with id 'zzz'"):
        resolve_name("zzz", _LISTED, key=_by_name, id_key=_by_id, kind="workspace")


def test_a_shared_name_is_refused_with_what_tells_the_records_apart() -> None:
    with pytest.raises(ProbeArgumentError) as info:
        resolve_name(
            "dup",
            _LISTED,
            key=_by_name,
            distinguish=_by_id,
            kind="workspace",
            on_ambiguous="pass the workspace GUID instead",
        )
    message = str(info.value)
    assert "'dup' names more than one workspace (g-3, g-4)" in message
    assert message.endswith("; pass the workspace GUID instead")


def test_the_id_key_distinguishes_by_default() -> None:
    with pytest.raises(ProbeArgumentError, match=r"\(g-3, g-4\)"):
        resolve_name("dup", _LISTED, key=_by_name, id_key=_by_id, kind="workspace")


def test_on_miss_runs_before_the_refusal_and_may_raise_instead() -> None:
    def unread() -> None:
        raise ProbeReadFailed("listing lakehouses failed: HTTP 403")

    with pytest.raises(ProbeReadFailed):
        resolve_name("nope", _LISTED, key=_by_name, kind="item", on_miss=unread)


def test_on_miss_is_not_called_on_a_match_or_an_ambiguity() -> None:
    calls: List[str] = []
    resolve_name(
        "ops", _LISTED, key=_by_name, kind="ws", on_miss=lambda: calls.append("x")
    )
    with pytest.raises(ProbeArgumentError):
        resolve_name(
            "dup", _LISTED, key=_by_name, kind="ws", on_miss=lambda: calls.append("x")
        )
    assert calls == []


def test_stop_at_first_reads_the_listing_lazily() -> None:
    pulled: List[str] = []

    def listing() -> Iterator[_Ws]:
        for ws in _LISTED:
            pulled.append(ws.name)
            yield ws

    got = resolve_name("ops", listing(), key=_by_name, kind="ws", stop_at_first=True)
    assert got.name == "ops"
    assert pulled == ["Sales", "ops"]


def test_the_echoed_argument_is_clipped_and_escaped() -> None:
    hostile = "x\x00" + "y" * 500
    with pytest.raises(ProbeArgumentError) as info:
        _resolve(hostile)
    message = str(info.value)
    assert "\x00" not in message
    assert len(message) < 300


def test_hinted_and_distinguishing_values_are_clipped_and_escaped() -> None:
    listed = "Ab\x1b" + "c" * 500
    records = [
        _Ws(listed, "i\x1b1"),
        # Among the ambiguous matches, so its id is rendered as a label.
        _Ws(listed.lower(), "i\x1b2"),
        _Ws(listed.lower(), "i3"),
    ]
    with pytest.raises(ProbeArgumentError) as hint:
        resolve_name(listed.upper(), records, key=_by_name, kind="ws")
    with pytest.raises(ProbeArgumentError) as ambiguous:
        resolve_name(
            listed.lower(), records, key=_by_name, distinguish=_by_id, kind="ws"
        )
    assert "i\\x1b2" in str(ambiguous.value)
    for message in (str(hint.value), str(ambiguous.value)):
        assert "\x1b" not in message
        assert len(message) < 400


def test_echoed_matches_the_w2_resolver_rendering() -> None:
    assert echoed("public") == "'public'"
    assert echoed("y" * 70) == repr("y" * 64 + "...")


def test_take_stops_pulling_at_the_limit() -> None:
    pulled: List[int] = []

    def pages() -> Iterator[int]:
        for n in range(100):
            pulled.append(n)
            yield n

    assert take(pages(), 3) == [0, 1, 2]
    assert pulled == [0, 1, 2]


def test_take_closes_a_generator_it_stops_early() -> None:
    # proxy.tables patches an SDK class for as long as its loop is
    # suspended; only an explicit close runs its finally deterministically.
    state: List[str] = []

    def listing() -> Iterator[int]:
        state.append("patched")
        try:
            yield from range(10)
        finally:
            state.append("restored")

    assert take(listing(), 2) == [0, 1]
    assert state == ["patched", "restored"]


def test_take_closes_the_source_when_keep_raises() -> None:
    state: List[str] = []

    def listing() -> Iterator[int]:
        try:
            yield from range(10)
        finally:
            state.append("closed")

    def keep(n: int) -> bool:
        raise KeyError(n)

    with pytest.raises(KeyError):
        take(listing(), 5, keep=keep)
    assert state == ["closed"]


class _BrokenPager:
    """A paged listing whose next page fails, and whose close fails too."""

    def __iter__(self) -> "_BrokenPager":
        return self

    def __next__(self) -> int:
        raise ConnectionError("page 2 fetch failed")

    def close(self) -> None:
        raise RuntimeError("session already torn down")


def _listing_whose_close_fails() -> Iterator[int]:
    try:
        yield from range(10)
    finally:
        raise RuntimeError("cleanup failed")


def test_a_failing_close_does_not_replace_the_listings_own_failure() -> None:
    with pytest.raises(ConnectionError):
        take(_BrokenPager(), 10)


def test_a_failing_close_does_not_replace_a_keep_refusal() -> None:
    def keep(n: int) -> bool:
        raise ProbeArgumentError("bad record filter")

    with pytest.raises(ProbeArgumentError):
        take(_listing_whose_close_fails(), 5, keep=keep)


def test_a_failing_close_after_a_clean_listing_is_raised() -> None:
    with pytest.raises(RuntimeError, match="cleanup failed"):
        take(_listing_whose_close_fails(), 2)


def test_take_filters_before_counting_and_takes_all_without_a_limit() -> None:
    assert take(range(10), 2, keep=lambda n: n % 2 == 1) == [1, 3]
    assert take(iter([1, 2, 3]), None) == [1, 2, 3]


def _notebooks() -> PersonalWithholding[str]:
    return PersonalWithholding[str](
        is_personal=lambda path: not path.startswith("/Shared/"),
        would_ingest=lambda path: path.startswith("/Users/ingested"),
    )


def test_withholding_drops_personal_records_ingestion_would_not_read() -> None:
    withholding = _notebooks()
    paths = ["/Shared/a", "/Users/someone/b", "/Users/ingested/c", "/Repos/x/d"]
    assert [p for p in paths if withholding.keep(p)] == [
        "/Shared/a",
        "/Users/ingested/c",
    ]
    assert withholding.withheld == 2


def test_withholding_counts_only_what_a_cut_short_walk_saw() -> None:
    withholding = _notebooks()
    paths = ["/Users/a", "/Shared/b", "/Users/c", "/Shared/d", "/Users/e"]
    assert take(paths, 1, keep=withholding.keep) == ["/Shared/b"]
    assert withholding.withheld == 1
    assert withholding.count_text(stopped_early=True) == "at least 1"
    assert withholding.count_text(stopped_early=False) == "1"


class _Response:
    def __init__(self, status_code: int) -> None:
        self.status_code = status_code


class _HttpError(Exception):
    def __init__(self, status_code: int) -> None:
        super().__init__(f"{status_code} for https://host/api?token={SENTINEL}")
        self.response = _Response(status_code)


def _reports(warnings: List[str], error: Optional[BaseException]) -> List[str]:
    with soft_listing(warnings.append, 403, 404, context="reports listing"):
        if error is not None:
            raise error
        return ["Weekly"]
    return []


def test_a_clean_listing_passes_through_without_a_warning() -> None:
    warnings: List[str] = []
    assert _reports(warnings, None) == ["Weekly"]
    assert warnings == []


def test_a_listed_status_degrades_to_the_fallback_with_a_warning() -> None:
    warnings: List[str] = []
    assert _reports(warnings, _HttpError(403)) == []
    assert warnings == ["reports listing returned HTTP 403; treating it as empty."]
    assert SENTINEL not in warnings[0]


@pytest.mark.parametrize(
    "error",
    [
        _HttpError(401),
        _HttpError(500),
        # No .response at all: a dropped connection, exhausted retries.
        ConnectionError("connection reset"),
    ],
    ids=["unlisted-status", "server-error", "no-response"],
)
def test_a_failure_that_is_not_a_listed_status_propagates_untouched(
    error: Exception,
) -> None:
    """Degrading these would report "nothing here" for "could not look"."""
    warnings: List[str] = []
    with pytest.raises(type(error)) as info:
        _reports(warnings, error)
    assert info.value is error
    assert warnings == []


def test_a_soft_error_raised_by_a_connector_translator_degrades_without_codes() -> None:
    warnings: List[str] = []

    def site() -> Optional[str]:
        with soft_listing(warnings.append):
            raise ProbeSoftError("site details returned Tableau error 403069")
        return None

    assert site() is None
    assert warnings == ["site details returned Tableau error 403069"]


def test_a_soft_error_quoting_foreign_text_is_recorded_without_it() -> None:
    warnings: List[str] = []
    with soft_listing(warnings.append):
        try:
            _foreign_errors.fetch()
        except RuntimeError as exc:
            raise ProbeSoftError(f"listing said {exc}") from exc
    assert len(warnings) == 1
    assert SENTINEL not in warnings[0]
    assert "(RuntimeError)" in warnings[0]


def test_codes_without_a_context_is_a_programming_error() -> None:
    sink: List[str] = []
    with pytest.raises(ProbeInternalError):
        soft_listing(sink.append, 403)


def test_a_base_exception_is_never_swallowed() -> None:
    sink: List[str] = []
    with pytest.raises(KeyboardInterrupt):
        with soft_listing(sink.append, 403, context="x"):
            raise KeyboardInterrupt()
    assert sink == []


class _Handle:
    def __init__(self, log: List[str], name: str, fail: bool) -> None:
        self.log, self.name, self.fail = log, name, fail

    def close(self) -> None:
        self.log.append(self.name)
        if self.fail:
            raise RuntimeError(f"{self.name} close failed")


class _Clients(ProbeProviderBase):
    def __init__(self) -> None:
        self.log: List[str] = []
        self.opened = 0

    def handle(self, key: str, fail: bool = False) -> _Handle:
        def open_it() -> _Handle:
            self.opened += 1
            return _Handle(self.log, key, fail)

        return self._open_once(key, open_it, close=lambda h: h.close())


def test_open_once_builds_lazily_and_only_once() -> None:
    clients = _Clients()
    assert clients.opened == 0
    assert clients.handle("a") is clients.handle("a")
    assert clients.opened == 1


def test_a_failed_open_caches_nothing() -> None:
    clients = _Clients()
    attempts: List[int] = []

    def flaky() -> int:
        attempts.append(1)
        if len(attempts) == 1:
            raise ConnectionError("first try")
        return 7

    with pytest.raises(ConnectionError):
        clients._open_once("k", flaky)
    assert clients._open_once("k", flaky) == 7


def test_exit_closes_every_opened_client_in_reverse_order_even_when_one_fails() -> None:
    clients = _Clients()
    clients.handle("a")
    clients.handle("b", fail=True)
    clients.handle("c")
    with pytest.raises(RuntimeError, match="b close failed"):
        clients.__exit__(None, None, None)
    assert clients.log == ["c", "b", "a"]


def test_on_exit_registers_a_plain_closer_and_exit_with_nothing_open_is_fine() -> None:
    closed: List[str] = []
    with _Clients() as clients:
        clients._on_exit(lambda: closed.append("session"))
    assert closed == ["session"]
    with _Clients():
        pass


def test_an_instance_built_without_init_still_warns_and_exits() -> None:
    bare = _Clients.__new__(_Clients)
    assert bare.warnings == []
    bare._warn("degraded")
    bare._warn("degraded")
    assert bare.warnings == ["degraded"]
    bare.__exit__(None, None, None)


def test_warn_appends_to_the_list_a_subclass_assigned() -> None:
    class _OwnList(ProbeProviderBase):
        def __init__(self) -> None:
            self.warnings = []

    provider = _OwnList()
    provider._warn("x")
    assert provider.warnings == ["x"]


def test_the_base_declares_no_command_and_only_provider_attributes() -> None:
    assert probe_methods._iter_specs(ProbeProviderBase) == []
    assert {n for n in dir(ProbeProviderBase) if n.startswith("probe_")} <= set(
        probe_methods.PROVIDER_ATTRIBUTES
    )
    assert "for_config" not in vars(ProbeProviderBase)


def test_the_base_defaults_read_as_absent() -> None:
    # Each default means what an undeclared attribute means to the
    # framework, so inheriting the base changes nothing a provider omits.
    bare = _Clients.__new__(_Clients)
    assert bare.sql_dialect is None
    assert bare.catalog_scope is None
    assert bare.api_allowlist is None
    assert bare.api_base_url == ""
    assert list(bare.failures) == []
    assert bare.probe_report is None
    assert _Clients.silenced_loggers == ()
    assert _Clients.probe_error_code(RuntimeError("x")) is None


def test_failures_a_subclass_assigns_reach_the_result(
    run: Callable[..., ProbeMethodResult],
) -> None:
    assert run("record-failure").failures == ["GET /things returned 403"]


class _RunProvider(ProbeProviderBase):
    def __init__(self, mode: str) -> None:
        self.mode = mode

    @classmethod
    def for_config(cls, config: object) -> "_RunProvider":
        provider = cls(getattr(config, "mode", ""))
        if provider.mode.startswith("close-foreign"):
            provider._on_exit(_foreign_errors.close)
        if provider.mode == "close-c-callable":
            # int is C code: the innermost Python frame is the helper's closer.
            provider._open_once("c", lambda: f"x-{SENTINEL}", close=int)
        return provider

    @probe_method(name="things")
    def things(self, name: str = "") -> List[str]:
        """List things."""
        if self.mode == "close-foreign-after-refusal":
            resolve_name(name, ["a"], key=str, kind="thing")
        if self.mode == "resolve-foreign":
            resolve_name(name, _foreign_listing(), key=str, kind="thing")
        if self.mode == "warn":
            self._warn("one listing degraded")
        if self.mode == "record-failure":
            self.failures = ["GET /things returned 403"]
        if self.mode == "open-c-callable":
            self._open_once("p", functools.partial(open, f"/nonexistent-{SENTINEL}/x"))
        if self.mode == "take-c-iterator":
            take(map(int, [f"x-{SENTINEL}"]), 5)
        if self.mode == "soft-listing-no-context":
            with soft_listing(self._warn, 404):
                return []
        if self.mode == "resolve-stop-at-first-by-id":
            resolve_name(
                name, ["a"], key=str, kind="thing", id_key=str, stop_at_first=True
            )
        return []


def _foreign_listing() -> Iterator[str]:
    yield "a"
    _foreign_errors.fetch()


def test_the_base_satisfies_the_provider_protocol_once_for_config_exists() -> None:
    assert issubclass(_RunProvider, ProbeProvider)


@pytest.fixture
def run(monkeypatch: pytest.MonkeyPatch) -> Callable[..., ProbeMethodResult]:
    from datahub.configuration.common import ConfigModel

    class _Config(ConfigModel):
        mode: str = ""

        @classmethod
        def probe_provider_class(cls) -> type:
            return _RunProvider

    monkeypatch.setattr(probe_methods, "config_class_for", lambda _st: _Config)

    def _run(mode: str, **kwargs: object) -> ProbeMethodResult:
        return run_probe_method("fake", {"mode": mode}, "things", dict(kwargs))

    return _run


def test_a_foreign_close_failure_reports_only_its_class(
    run: Callable[..., ProbeMethodResult],
) -> None:
    with pytest.raises(ProbeConnectionError) as info:
        run("close-foreign")
    assert SENTINEL not in str(info.value)
    assert "TypeError" in str(info.value)


def test_a_close_failure_never_replaces_the_commands_own_refusal(
    run: Callable[..., ProbeMethodResult],
) -> None:
    with pytest.raises(ProbeArgumentError) as info:
        run("close-foreign-after-refusal", name="widget")
    assert "no thing named 'widget'" in str(info.value)
    assert SENTINEL not in str(info.value)


def test_a_foreign_failure_while_resolving_reports_only_its_class(
    run: Callable[..., ProbeMethodResult],
) -> None:
    # A listing that fails part-way is the source's failure (exit 3), not a
    # "no such name" (exit 2).
    with pytest.raises(ProbeConnectionError) as info:
        run("resolve-foreign", name="b")
    assert SENTINEL not in str(info.value)
    assert "RuntimeError" in str(info.value)


def test_base_warnings_reach_the_result(run: Callable[..., ProbeMethodResult]) -> None:
    assert run("warn").warnings == ["one listing degraded"]


# Helpers call back into provider-supplied callables, some of them C code (a
# DB-API close, functools.partial over a driver's connect, a cursor iterated
# by take). What those raise is untrusted whichever frame raised it, so it is
# named by class only.


def test_a_c_closer_registered_by_open_once_is_foreign(
    run: Callable[..., ProbeMethodResult],
) -> None:
    with pytest.raises(ProbeConnectionError) as info:
        run("close-c-callable")
    assert SENTINEL not in str(info.value)
    assert "ValueError" in str(info.value)


def test_a_c_opener_is_foreign(run: Callable[..., ProbeMethodResult]) -> None:
    with pytest.raises(ProbeConnectionError) as info:
        run("open-c-callable")
    assert SENTINEL not in str(info.value)
    assert "FileNotFoundError" in str(info.value)


def test_a_c_iterator_failing_inside_take_is_reported_by_class_only(
    run: Callable[..., ProbeMethodResult],
) -> None:
    # A foreign ValueError keeps its exit family (2) but not its text.
    with pytest.raises(ProbeArgumentError) as info:
        run("take-c-iterator")
    assert SENTINEL not in str(info.value)
    assert "ValueError" in str(info.value)


# A helper misused by its caller is a defect in the provider: exit 1, with the
# helper's own text kept so the author can see which rule was broken.


def test_soft_listing_without_a_context_reports_why(
    run: Callable[..., ProbeMethodResult],
) -> None:
    with pytest.raises(ProbeInternalError, match="context"):
        run("soft-listing-no-context")


def test_stop_at_first_with_an_id_key_is_refused(
    run: Callable[..., ProbeMethodResult],
) -> None:
    # The first record whose name matches would win over a later record whose
    # id matches, which is the precedence resolve_name promises the other way.
    with pytest.raises(ProbeInternalError, match="stop_at_first"):
        run("resolve-stop-at-first-by-id", name="a")


def test_a_class_level_warnings_list_is_refused() -> None:
    # It would replace the base's property with one list shared by every
    # instance, so one probe's warnings would leak into the next.
    with pytest.raises(TypeError):

        class _Shared(ProbeProviderBase):
            warnings: List[str] = []


def test_a_class_level_failures_list_is_refused() -> None:
    with pytest.raises(TypeError):

        class _Shared(ProbeProviderBase):
            failures: List[str] = []
