"""resolve_listed_name: a caller's identifier, mapped to the catalog's own string."""

from typing import Iterable, Iterator, List

import pytest

from datahub.ingestion.agent.verdicts import ProbeArgumentError
from datahub.ingestion.source.sql.sql_identifier_resolver import resolve_listed_name


def _resolve(name: str, listed: Iterable[str]) -> str:
    return resolve_listed_name(
        name,
        listed,
        what="schema",
        where="on this connection",
        list_command="containers",
    )


def test_an_exact_match_returns_the_catalogs_own_string() -> None:
    listed = ["analytics", "public"]
    # A distinct object equal to the listed one: what reaches reflection must be
    # the string the server produced, not the caller's.
    caller = "".join(["pub", "lic"])
    got = _resolve(caller, listed)
    assert got == "public"
    assert got is listed[1]


def test_an_exact_match_wins_over_case_variants() -> None:
    listed = ["Sales", "sales"]
    assert _resolve("sales", listed) is listed[1]


def test_a_case_only_mismatch_is_refused_with_the_catalog_spelling() -> None:
    with pytest.raises(ProbeArgumentError) as info:
        _resolve("PUBLIC", ["analytics", "public"])
    message = str(info.value)
    assert "'public'" in message
    assert "containers" in message


def test_every_case_variant_is_offered_when_several_exist() -> None:
    with pytest.raises(ProbeArgumentError) as info:
        _resolve("SALES", ["Sales", "sales", "ops"])
    message = str(info.value)
    assert "'Sales'" in message and "'sales'" in message
    assert "'ops'" not in message


def test_an_unrelated_name_gets_no_hint() -> None:
    with pytest.raises(ProbeArgumentError) as info:
        _resolve("nope", ["public"])
    assert "did you mean" not in str(info.value)


def test_the_listing_is_read_lazily_and_stops_at_the_first_exact_match() -> None:
    pulled: List[str] = []

    def listing() -> Iterator[str]:
        for name in ["a", "b", "c"]:
            pulled.append(name)
            yield name

    assert _resolve("b", listing()) == "b"
    assert pulled == ["a", "b"]


def test_the_echoed_argument_is_clipped_and_escaped() -> None:
    hostile = "x\x00" + "y" * 500
    with pytest.raises(ProbeArgumentError) as info:
        _resolve(hostile, ["public"])
    message = str(info.value)
    assert "\x00" not in message
    assert len(message) < 300


def test_a_hinted_listed_name_is_clipped_and_escaped_too() -> None:
    # The server listed it, but a listed name can still be long or carry
    # control characters, and the refusal is printed to a terminal.
    listed = "Ab\x1b" + "c" * 500
    with pytest.raises(ProbeArgumentError) as info:
        _resolve(listed.lower(), [listed])
    message = str(info.value)
    assert "did you mean" in message
    assert "\x1b" not in message
    assert len(message) < 400
