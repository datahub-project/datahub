from dataclasses import replace
from typing import Optional, Sequence

import pytest

from datahub.ingestion.agent.filter_input import (
    FilterRequest,
    RunListing,
    filter_request,
    listing_from_run,
)


def test_record_entries_carry_their_scalar_fields_as_attributes() -> None:
    listing = listing_from_run(
        {
            "kind": "Workspace",
            "parent_path": [],
            "result": [
                {"name": "Sales", "id": "ws-1", "type": "Workspace", "state": None},
                {"name": "Ops", "id": "ws-2", "is_personal": False, "tags": ["x"]},
            ],
        }
    )
    assert listing.kind == "Workspace"
    assert listing.names == ["Sales", "Ops"]
    # None and non-scalars are dropped; bools are spelled the way JSON does.
    assert listing.attributes == [
        {"id": "ws-1", "type": "Workspace"},
        {"id": "ws-2", "is_personal": "false"},
    ]


def test_bare_string_entries_have_no_attributes() -> None:
    listing = listing_from_run(
        {"kind": "Table", "parent_path": ["db", "public"], "result": ["a", "b"]}
    )
    assert listing.names == ["a", "b"]
    assert listing.attributes == [{}, {}]
    assert listing.parent_path == ["db", "public"]


def test_a_run_whose_result_is_not_a_listing_is_refused() -> None:
    # `probe run sql` returns an envelope dict, not a list of names.
    with pytest.raises(ValueError, match="not a listing"):
        listing_from_run({"kind": None, "result": {"rows": []}})


def test_a_record_without_a_name_is_refused() -> None:
    with pytest.raises(ValueError, match="entry 1"):
        listing_from_run({"kind": "Thing", "result": ["a", {"id": "x"}]})


@pytest.mark.parametrize("envelope", [["a", "b"], "a", 3])
def test_a_file_that_is_not_an_envelope_is_refused(envelope: object) -> None:
    with pytest.raises(ValueError, match="not a `probe run` output"):
        listing_from_run(envelope)


def test_the_source_type_travels_with_the_listing() -> None:
    listing = listing_from_run({"source_type": "mysql", "result": ["a"]})
    assert listing.source_type == "mysql"
    assert listing_from_run({"result": ["a"]}).source_type is None


def test_a_redacted_name_is_skipped_and_reported() -> None:
    listing = listing_from_run(
        {
            "kind": "Table",
            "result": ["a", "***", {"name": "x***REDACTED:PW***y"}, {"name": "b"}],
        }
    )
    # A masked name is not a name the source has, so judging it would answer
    # about an object that does not exist.
    assert listing.names == ["a", "b"]
    assert listing.attributes == [{}, {}]
    assert listing.skipped == [1, 2]


def test_a_redacted_attribute_is_dropped_and_reported() -> None:
    listing = listing_from_run(
        {"kind": "Workspace", "result": [{"name": "Sales", "id": "***", "t": "x"}]}
    )
    assert listing.attributes == [{"t": "x"}]
    assert listing.masked_attributes == ["id"]
    assert listing.skipped == []


def test_a_truncated_or_failed_run_is_flagged() -> None:
    listing = listing_from_run(
        {"result": ["a"], "truncated": True, "failures": ["HTTP 403 on x"]}
    )
    assert listing.truncated is True
    assert listing.incomplete is True
    clean = listing_from_run({"result": ["a"], "truncated": False, "failures": []})
    assert clean.truncated is False
    assert clean.incomplete is False


@pytest.mark.parametrize("parent", ["db", ["db", 3], {"a": "b"}])
def test_a_malformed_parent_path_is_refused(parent: object) -> None:
    with pytest.raises(ValueError, match="parent_path is malformed"):
        listing_from_run({"result": ["a"], "parent_path": parent})


def test_an_absent_parent_path_is_empty() -> None:
    assert listing_from_run({"result": ["a"], "parent_path": None}).parent_path == []
    assert listing_from_run({"result": ["a"]}).parent_path == []


def test_a_redacted_parent_path_is_flagged_not_used() -> None:
    listing = listing_from_run(
        {"kind": "Table", "parent_path": ["db", "***"], "result": ["a"]}
    )
    assert listing.parent_redacted is True
    assert (
        listing_from_run({"result": ["a"], "parent_path": ["db"]}).parent_redacted
        is False
    )


def test_a_redacted_attribute_key_is_dropped_and_reported() -> None:
    # The redactor masks dict keys too: an attribute stored under "***" is one
    # no verdict reads, so the real field would be missing without a word.
    listing = listing_from_run(
        {"kind": "Workspace", "result": [{"name": "Sales", "***": "ws-1", "t": "x"}]}
    )
    assert listing.attributes == [{"t": "x"}]
    assert listing.masked_attributes == ["***"]


def test_a_redacted_kind_or_source_type_is_treated_as_absent() -> None:
    listing = listing_from_run({"kind": "***", "source_type": "x***y", "result": ["a"]})
    # "***" is not a kind the source has; reading it as one would judge every
    # name against a kind with no filters.
    assert listing.kind is None
    assert listing.source_type is None


def test_the_runs_warnings_travel_with_the_listing() -> None:
    listing = listing_from_run(
        {"result": ["a"], "warnings": ["could not list owners of x", 3]}
    )
    # A degraded sub-fetch means the listing may be partial even though the
    # run recorded no failures.
    assert listing.run_warnings == ["could not list owners of x"]
    assert listing_from_run({"result": ["a"]}).run_warnings == []


_LISTING = RunListing(
    kind="Table",
    source_type="postgres",
    parent_path=["public"],
    names=["orders", "users"],
    attributes=[{"id": "1"}, {}],
)


def _request(
    *,
    kind: Optional[str] = None,
    parents: Sequence[str] = (),
    names: Sequence[str] = (),
    listing: Optional[RunListing] = None,
) -> FilterRequest:
    return filter_request(
        source_type="postgres",
        kind=kind,
        parents=parents,
        names=names,
        listing=listing,
    )


def test_bare_names_are_judged_under_the_given_kind_and_parent() -> None:
    assert _request(kind="Table", parents=("public",), names=("orders",)) == (
        FilterRequest(
            kind="Table",
            parent_path=["public"],
            names=["orders"],
            attributes=None,
            warnings=[],
        )
    )


def test_no_names_and_no_listing_is_refused() -> None:
    with pytest.raises(ValueError, match="nothing to judge"):
        _request(kind="Table")


def test_bare_names_need_a_kind() -> None:
    with pytest.raises(ValueError, match="pass --kind"):
        _request(names=("orders",))


def test_a_listing_brings_its_kind_parent_names_and_facts() -> None:
    request = _request(listing=_LISTING)
    assert request.kind == "Table"
    assert request.parent_path == ["public"]
    assert request.names == ["orders", "users"]
    assert request.attributes == [{"id": "1"}, {}]
    assert request.warnings == []


def test_a_listing_caveat_travels_as_a_warning() -> None:
    assert len(_request(listing=replace(_LISTING, truncated=True)).warnings) == 1


def test_a_restated_kind_is_kept_and_a_contradicting_one_refused() -> None:
    assert _request(kind="table", listing=_LISTING).kind == "table"
    with pytest.raises(ValueError, match="contradicts the listing"):
        _request(kind="View", listing=_LISTING)


def test_a_listing_without_a_kind_needs_one() -> None:
    unkinded = replace(_LISTING, kind=None)
    with pytest.raises(ValueError, match="does not say what kind it holds"):
        _request(listing=unkinded)
    assert _request(kind="Table", listing=unkinded).kind == "Table"


def test_a_parent_replaces_the_listings() -> None:
    request = _request(parents=("mydb", "sales"), listing=_LISTING)
    assert request.parent_path == ["mydb", "sales"]


def test_a_redacted_parent_is_refused_unless_replaced_by_a_real_one() -> None:
    redacted = replace(_LISTING, parent_path=["***"], parent_redacted=True)
    with pytest.raises(ValueError, match="parent_path was redacted"):
        _request(listing=redacted)
    # The replacement is the caller's input, so it is checked like any --parent.
    with pytest.raises(ValueError, match=r"--parent value holds '\*\*\*'"):
        _request(parents=("***",), listing=redacted)
    assert _request(parents=("public",), listing=redacted).parent_path == ["public"]


# A secret equal to an identifier masks it in every output the caller could
# have copied it from, alone or inside a longer name.
@pytest.mark.parametrize("masked", ["***", "prod_***"])
def test_a_masked_parent_is_refused_not_judged(masked: str) -> None:
    with pytest.raises(ValueError, match=r"--parent value holds '\*\*\*'") as info:
        _request(kind="Table", parents=("mydb", masked), names=("orders",))
    # Only the mask is echoed: the rest of the value, and the other segments,
    # are the caller's identifiers.
    assert "prod_" not in str(info.value)
    assert "mydb" not in str(info.value)


@pytest.mark.parametrize("masked", ["***", "prod_***"])
def test_a_masked_name_is_refused_not_judged(masked: str) -> None:
    with pytest.raises(ValueError, match=r"--name value holds '\*\*\*'"):
        _request(kind="Table", parents=("public",), names=("orders", masked))


def test_a_name_with_stars_that_are_not_the_mask_is_judged() -> None:
    request = _request(kind="Table", parents=("pub*",), names=("orders_*", "**"))
    assert request.parent_path == ["pub*"]
    assert request.names == ["orders_*", "**"]


def test_a_listing_from_another_source_is_refused() -> None:
    with pytest.raises(ValueError, match="this listing came from mysql"):
        _request(listing=replace(_LISTING, source_type="mysql"))


def test_a_listing_without_a_source_type_is_judged_as_the_recipes() -> None:
    # A masked source_type reads as absent: nothing to compare, so the
    # listing is judged as this recipe's source.
    request = _request(listing=replace(_LISTING, source_type=None))
    assert request.kind == "Table"
    assert request.names == ["orders", "users"]
