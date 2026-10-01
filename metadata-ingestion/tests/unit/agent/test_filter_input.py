import pytest

from datahub.ingestion.agent.filter_input import listing_from_run


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
