import json
import re
from pathlib import Path
from typing import List, Pattern, Set, Union

import pytest
import yaml
from click.testing import CliRunner

from datahub.cli.recipe_cli import recipe as recipe_group
from datahub.emitter.mce_builder import make_container_urn, make_dataset_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.sink.file import write_metadata_file
from datahub.metadata.schema_classes import (
    ContainerPropertiesClass,
    StatusClass,
    SubTypesClass,
)
from datahub.metadata.urns import DatasetUrn
from tests.test_helpers.probe_parity import (
    EmittedIndex,
    FanOut,
    JudgedRecord,
    ParityListing,
    assert_probe_parity,
    by_name,
    pipeline_ingestion,
    report_envelope,
)
from tests.unit.agent._parity_fake_source import (
    ARCHIVED_DUPLICATE,
    BENIGN_NOTE,
    DROP_ITEM_A,
    GROUP_KIND,
    GROUP_NOTE,
    IGNORE_ITEM_PATTERN,
    LIST_NOTHING,
    SOFT_DEGRADE,
    SOURCE_TYPE,
    item_note,
)


def _item_names(index: EmittedIndex) -> Set[str]:
    return {DatasetUrn.from_string(urn).name for urn in index.urns("dataset")}


def _qualified(record: JudgedRecord) -> str:
    return f"{record.parent_path[-1]}.{record.name}"


_GROUPS = ParityListing(
    "groups", "groups", emitted=lambda index: index.container_names(GROUP_KIND)
)
_ITEMS = ParityListing(
    "items",
    "items",
    emitted=_item_names,
    fan_out=FanOut("groups", "group"),
    identity=_qualified,
)


def test_agreeing_source_passes_and_reports_each_exclusion(tmp_path: Path) -> None:
    report = assert_probe_parity(
        SOURCE_TYPE,
        {"item_pattern": {"deny": ["^b$"]}},
        pipeline_ingestion(SOURCE_TYPE, tmp_path),
        [_GROUPS, _ITEMS],
    )
    assert report.kinds["items"].included == {"g1.a", "g2.a", "g2.c"}
    assert report.excluded_by("items") == {"g1.b": "item_pattern"}
    assert report.excluded_by("groups") == {}


def test_children_of_a_dropped_parent_are_judged(tmp_path: Path) -> None:
    report = assert_probe_parity(
        SOURCE_TYPE,
        {"group_pattern": {"deny": ["^g2$"]}},
        pipeline_ingestion(SOURCE_TYPE, tmp_path),
        [_GROUPS, _ITEMS],
    )
    # Listed under the excluded g2 and excluded through it, not skipped.
    assert report.excluded_by("items") == {
        "g2.a": "group_pattern",
        "g2.c": "group_pattern",
    }


def test_an_object_ingestion_emits_but_the_probe_excludes_fails(
    tmp_path: Path,
) -> None:
    with pytest.raises(AssertionError, match="g1.b"):
        assert_probe_parity(
            SOURCE_TYPE,
            {"item_pattern": {"deny": ["^b$"]}, "drift": IGNORE_ITEM_PATTERN},
            pipeline_ingestion(SOURCE_TYPE, tmp_path),
            [_ITEMS],
        )


def test_an_object_the_probe_includes_but_ingestion_skips_fails(
    tmp_path: Path,
) -> None:
    with pytest.raises(AssertionError, match="g1.a"):
        assert_probe_parity(
            SOURCE_TYPE,
            {"drift": DROP_ITEM_A},
            pipeline_ingestion(SOURCE_TYPE, tmp_path),
            [_ITEMS],
        )


def test_no_listing_at_all_compares_nothing_and_fails(tmp_path: Path) -> None:
    with pytest.raises(AssertionError, match="no listing"):
        assert_probe_parity(
            SOURCE_TYPE, {}, pipeline_ingestion(SOURCE_TYPE, tmp_path), []
        )


def test_an_empty_kind_fails_unless_expected(tmp_path: Path) -> None:
    with pytest.raises(AssertionError, match="groups: ingestion emitted none"):
        assert_probe_parity(
            SOURCE_TYPE,
            {"group_pattern": {"deny": [".*"]}},
            pipeline_ingestion(SOURCE_TYPE, tmp_path),
            [_GROUPS],
        )


def test_expect_empty_accepts_a_kind_the_recipe_switches_off(tmp_path: Path) -> None:
    switched_off = ParityListing(
        "groups",
        "groups",
        emitted=lambda index: index.container_names(GROUP_KIND),
        expect_empty=True,
    )
    report = assert_probe_parity(
        SOURCE_TYPE,
        {"group_pattern": {"deny": [".*"]}},
        pipeline_ingestion(SOURCE_TYPE, tmp_path),
        [switched_off],
    )
    assert report.excluded_by("groups") == {
        "g1": "group_pattern",
        "g2": "group_pattern",
    }


def test_a_truncated_listing_is_refused(tmp_path: Path) -> None:
    capped = ParityListing(
        "groups",
        "groups",
        emitted=lambda index: index.container_names(GROUP_KIND),
        kwargs={"limit": 1},
    )
    with pytest.raises(AssertionError, match="stopped at its limit"):
        assert_probe_parity(
            SOURCE_TYPE, {}, pipeline_ingestion(SOURCE_TYPE, tmp_path), [capped]
        )


def test_a_redacted_name_is_refused(tmp_path: Path) -> None:
    # A recipe secret equal to item "a" masks that name in the round trip.
    with pytest.raises(AssertionError, match="redact"):
        assert_probe_parity(
            SOURCE_TYPE,
            {"password": "a"},
            pipeline_ingestion(SOURCE_TYPE, tmp_path),
            [_ITEMS],
        )


_ONE_IDENTITY = ParityListing(
    "items",
    "items",
    emitted=lambda index: {"same"} if index.urns("dataset") else set(),
    fan_out=FanOut("groups", "group"),
    identity=lambda record: "same",
)


def test_the_harness_reads_the_file_probe_run_writes(tmp_path: Path) -> None:
    # A secret equal to item "a" makes redaction part of the envelope: the
    # masked name and the notice the CLI appends must both reach the harness.
    config = {"password": "a"}
    recipe_path = tmp_path / "recipe.yml"
    recipe_path.write_text(
        yaml.safe_dump({"source": {"type": SOURCE_TYPE, "config": config}})
    )
    report = tmp_path / "run.json"
    result = CliRunner().invoke(
        recipe_group,
        [
            "probe",
            "run",
            "items",
            "--recipe",
            str(recipe_path),
            "--group",
            "g1",
            "--report-to",
            str(report),
        ],
    )
    assert result.exit_code == 0, result.output
    assert json.loads(report.read_text()) == report_envelope(
        SOURCE_TYPE, config, "items", {"group": "g1"}
    )


def test_distinct_records_with_conflicting_verdicts_in_one_identity_fail(
    tmp_path: Path,
) -> None:
    with pytest.raises(AssertionError, match="distinct listing records"):
        assert_probe_parity(
            SOURCE_TYPE,
            {"item_pattern": {"deny": ["^b$"]}},
            pipeline_ingestion(SOURCE_TYPE, tmp_path),
            [_ONE_IDENTITY],
        )


def test_one_record_listed_twice_with_different_verdicts_fails(
    tmp_path: Path,
) -> None:
    # g1/a is listed once plain and once archived, which the probe drops:
    # the same listing record, so no identity function can tell them apart.
    with pytest.raises(AssertionError, match="different verdicts"):
        assert_probe_parity(
            SOURCE_TYPE,
            {"probe_drift": ARCHIVED_DUPLICATE},
            pipeline_ingestion(SOURCE_TYPE, tmp_path),
            [_ITEMS],
        )


def test_a_collapsing_identity_fails_even_when_the_verdicts_agree(
    tmp_path: Path,
) -> None:
    # Every record includes and ingestion drops g1.a: merged into one
    # identity, both sides say {"same"} and the drift would pass unseen.
    with pytest.raises(AssertionError, match="distinct listing records"):
        assert_probe_parity(
            SOURCE_TYPE,
            {"drift": DROP_ITEM_A},
            pipeline_ingestion(SOURCE_TYPE, tmp_path),
            [_ONE_IDENTITY],
        )


def test_a_fanned_out_listing_qualifies_its_identity_by_parent(
    tmp_path: Path,
) -> None:
    # No identity given: item "a" under g1 and under g2 stay two identities.
    default_identity = ParityListing(
        "items", "items", emitted=_item_names, fan_out=FanOut("groups", "group")
    )
    report = assert_probe_parity(
        SOURCE_TYPE,
        {"item_pattern": {"deny": ["^b$"]}},
        pipeline_ingestion(SOURCE_TYPE, tmp_path),
        [default_identity],
    )
    assert report.kinds["items"].included == {"g1.a", "g2.a", "g2.c"}


def test_bare_names_under_a_fan_out_fail_on_the_shared_name(tmp_path: Path) -> None:
    bare = ParityListing(
        "items",
        "items",
        emitted=lambda index: {
            DatasetUrn.from_string(urn).name.rsplit(".", 1)[-1]
            for urn in index.urns("dataset")
        },
        fan_out=FanOut("groups", "group"),
        identity=by_name,
    )
    with pytest.raises(AssertionError, match="'a' names 2 distinct listing records"):
        assert_probe_parity(
            SOURCE_TYPE, {}, pipeline_ingestion(SOURCE_TYPE, tmp_path), [bare]
        )


def test_a_listing_with_no_records_fails_even_when_expected_empty(
    tmp_path: Path,
) -> None:
    switched_off = ParityListing(
        "items",
        "items",
        emitted=_item_names,
        fan_out=FanOut("groups", "group"),
        expect_empty=True,
    )
    with pytest.raises(AssertionError, match="the probe listed nothing"):
        assert_probe_parity(
            SOURCE_TYPE,
            {"item_pattern": {"deny": [".*"]}, "probe_drift": LIST_NOTHING},
            pipeline_ingestion(SOURCE_TYPE, tmp_path),
            [switched_off],
        )


def test_a_listing_the_run_warned_may_be_partial_is_refused(tmp_path: Path) -> None:
    with pytest.raises(AssertionError, match="may be partial"):
        assert_probe_parity(
            SOURCE_TYPE,
            {"probe_drift": SOFT_DEGRADE},
            pipeline_ingestion(SOURCE_TYPE, tmp_path),
            [_ITEMS],
        )


def _accepting(*accept: Union[str, Pattern[str]]) -> ParityListing:
    return ParityListing(
        "items",
        "items",
        emitted=_item_names,
        fan_out=FanOut("groups", "group"),
        accept_warnings=accept,
    )


def test_an_accepted_exact_warning_passes_and_is_reported(tmp_path: Path) -> None:
    listing = _accepting(GROUP_NOTE, item_note("g1"), item_note("g2"))
    report = assert_probe_parity(
        SOURCE_TYPE,
        {"probe_drift": BENIGN_NOTE},
        pipeline_ingestion(SOURCE_TYPE, tmp_path),
        [listing],
    )
    # The fan-out parent's warning is accepted too, before any listing of it.
    assert report.kinds["items"].accepted_warnings == (
        GROUP_NOTE,
        item_note("g1"),
        item_note("g2"),
    )


def test_an_accepted_pattern_must_match_the_whole_warning(tmp_path: Path) -> None:
    listing = _accepting(
        GROUP_NOTE, re.compile(r"archived items of \w+ are never listed")
    )
    report = assert_probe_parity(
        SOURCE_TYPE,
        {"probe_drift": BENIGN_NOTE},
        pipeline_ingestion(SOURCE_TYPE, tmp_path),
        [listing],
    )
    assert item_note("g2") in report.kinds["items"].accepted_warnings
    # A pattern matching only a prefix accepts nothing.
    with pytest.raises(AssertionError, match="may be partial"):
        assert_probe_parity(
            SOURCE_TYPE,
            {"probe_drift": BENIGN_NOTE},
            pipeline_ingestion(SOURCE_TYPE, tmp_path),
            [_accepting(GROUP_NOTE, re.compile("archived items"))],
        )


def test_a_warning_no_entry_accepts_is_still_refused(tmp_path: Path) -> None:
    with pytest.raises(AssertionError, match="may be partial: archived items"):
        assert_probe_parity(
            SOURCE_TYPE,
            {"probe_drift": BENIGN_NOTE},
            pipeline_ingestion(SOURCE_TYPE, tmp_path),
            [_accepting(GROUP_NOTE)],
        )


def test_an_accepted_entry_that_matched_nothing_fails(tmp_path: Path) -> None:
    listing = _accepting(GROUP_NOTE, re.compile("archived .*"), "a note never given")
    with pytest.raises(AssertionError, match="'a note never given' matched nothing"):
        assert_probe_parity(
            SOURCE_TYPE,
            {"probe_drift": BENIGN_NOTE},
            pipeline_ingestion(SOURCE_TYPE, tmp_path),
            [listing],
        )


def test_accept_warnings_cannot_waive_a_truncated_listing(tmp_path: Path) -> None:
    capped = ParityListing(
        "groups",
        "groups",
        emitted=lambda index: index.container_names(GROUP_KIND),
        kwargs={"limit": 1},
        accept_warnings=(re.compile(".*"),),
    )
    with pytest.raises(AssertionError, match="stopped at its limit"):
        assert_probe_parity(
            SOURCE_TYPE,
            {"probe_drift": BENIGN_NOTE},
            pipeline_ingestion(SOURCE_TYPE, tmp_path),
            [capped],
        )


def _workunits() -> List[MetadataWorkUnit]:
    container = make_container_urn("g1")
    return [
        MetadataChangeProposalWrapper(
            entityUrn=container, aspect=ContainerPropertiesClass(name="g1")
        ).as_workunit(),
        MetadataChangeProposalWrapper(
            entityUrn=container, aspect=SubTypesClass(typeNames=["Group"])
        ).as_workunit(),
        MetadataChangeProposalWrapper(
            entityUrn=make_dataset_urn("fake", "g1.a"),
            aspect=SubTypesClass(typeNames=["Table"]),
        ).as_workunit(),
        MetadataChangeProposalWrapper(
            entityUrn=make_dataset_urn("fake", "g1.lineage_only"),
            aspect=StatusClass(removed=False),
        ).as_workunit(),
    ]


def test_emitted_index_reads_workunits_and_a_file_sink_alike(tmp_path: Path) -> None:
    out = tmp_path / "out.json"
    write_metadata_file(out, [wu.metadata for wu in _workunits()])
    for index in (
        EmittedIndex.from_workunits(_workunits()),
        EmittedIndex.from_file(out),
    ):
        assert index.container_names("Group") == {"g1"}
        assert index.container_names("Schema") == set()
        assert len(index.urns("dataset")) == 2
        assert index.urns("dataset", with_aspect=SubTypesClass) == {
            make_dataset_urn("fake", "g1.a")
        }
        assert index.urns("container") == {make_container_urn("g1")}
