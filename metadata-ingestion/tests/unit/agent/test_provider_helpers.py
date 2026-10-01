"""agent.provider_helpers: the plumbing every probe provider used to hand-write."""

from dataclasses import dataclass
from typing import Iterator, List, Optional

import pytest

from datahub.ingestion.agent.provider_helpers import (
    PersonalWithholding,
    Resolved,
    echoed,
    resolve_name,
    soft_listing,
    take,
)
from datahub.ingestion.agent.verdicts import (
    ProbeArgumentError,
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
        _Ws(listed.lower(), "i2"),
        _Ws(listed.lower(), "i3"),
    ]
    with pytest.raises(ProbeArgumentError) as hint:
        resolve_name(listed.upper(), records, key=_by_name, kind="ws")
    with pytest.raises(ProbeArgumentError) as ambiguous:
        resolve_name(
            listed.lower(), records, key=_by_name, distinguish=_by_id, kind="ws"
        )
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


def test_an_unlisted_status_propagates_untouched() -> None:
    error = _HttpError(401)
    with pytest.raises(_HttpError) as info:
        _reports([], error)
    assert info.value is error


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
    with pytest.raises(TypeError):
        soft_listing(sink.append, 403)


def test_a_base_exception_is_never_swallowed() -> None:
    sink: List[str] = []
    with pytest.raises(KeyboardInterrupt):
        with soft_listing(sink.append, 403, context="x"):
            raise KeyboardInterrupt()
    assert sink == []
