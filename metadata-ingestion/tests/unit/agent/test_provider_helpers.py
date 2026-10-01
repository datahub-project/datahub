"""agent.provider_helpers: the plumbing every probe provider used to hand-write."""

from dataclasses import dataclass
from typing import Iterator, List

import pytest

from datahub.ingestion.agent.provider_helpers import Resolved, echoed, resolve_name
from datahub.ingestion.agent.verdicts import ProbeArgumentError, ProbeReadFailed


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
