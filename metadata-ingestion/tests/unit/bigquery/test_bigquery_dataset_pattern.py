"""`dataset_pattern` qualification under `match_fully_qualified_names`.

With the flag on (the default) the connector matches `dataset_pattern` against
`<project_id>.<dataset_name>`. An entry with no `.` gets the `.*\\.` prefix,
grouped as `.*\\.(?:<entry>)` when it has a `|`; an entry with a `.` is
matched as written. These tests go through `BigQueryFilter`, the filter the
source actually uses.
"""

from typing import Any, Dict, Iterable, List
from unittest.mock import patch

import pytest

from datahub.configuration.common import AllowDenyPattern
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.agent.pattern_path import validate_pattern_at
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.bigquery_v2.bigquery_config import BigQueryV2Config
from datahub.ingestion.source.bigquery_v2.bigquery_report import BigQueryV2Report
from datahub.ingestion.source.bigquery_v2.bigquery_schema import (
    BigqueryColumn,
    BigqueryDataset,
    BigqueryProject,
)
from datahub.ingestion.source.bigquery_v2.common import BigQueryFilter
from datahub.metadata.schema_classes import StatusClass
from datahub.utilities.file_backed_collections import FileBackedDict
from tests.unit.bigquery.conftest import PROJECT, as_mock, make_schema_gen, make_table

_DOTTED_ENTRIES_AS_WRITTEN = (
    "An entry containing a `.` is matched as written against "
    "`<project_id>.<dataset_name>`; a dataset-name regex with a `.` must "
    "include the project part, e.g. `.*\\.staging_.*`."
)


def _filter(dataset_pattern: Dict[str, List[str]]) -> BigQueryFilter:
    config = BigQueryV2Config.model_validate({"dataset_pattern": dataset_pattern})
    assert config.match_fully_qualified_names
    return BigQueryFilter(config, BigQueryV2Report())


def test_dataset_name_is_qualified_and_still_matches() -> None:
    filters = _filter({"allow": ["staging_events"]})
    assert filters.filter_config.dataset_pattern.allow == [r".*\.staging_events"]
    assert filters.is_dataset_allowed("staging_events", PROJECT)
    assert not filters.is_dataset_allowed("prod_events", PROJECT)


@pytest.mark.parametrize("dataset", ["a", "b"])
def test_every_alternative_is_qualified(dataset: str) -> None:
    filters = _filter({"allow": ["a|b"]})
    assert filters.filter_config.dataset_pattern.allow == [r".*\.(?:a|b)"]
    assert filters.is_dataset_allowed(dataset, PROJECT)
    assert not filters.is_dataset_allowed("c", PROJECT)


@pytest.mark.parametrize("deny", [["restricted_pii"], ["^tmp|restricted_pii$"]])
def test_deny_entry_denies_its_dataset(deny: List[str]) -> None:
    filters = _filter({"deny": deny})
    assert not filters.is_dataset_allowed("restricted_pii", PROJECT)
    assert filters.is_dataset_allowed("public_events", PROJECT)


@pytest.mark.parametrize(
    "entry", [f"{PROJECT}.*", f"{PROJECT}\\.prod_.*", ".*\\.prod_.*", ".*prod_.*"]
)
def test_entry_with_a_dot_is_matched_as_written(entry: str) -> None:
    filters = _filter({"allow": [entry]})
    assert filters.filter_config.dataset_pattern.allow == [entry]
    assert filters.is_dataset_allowed("prod_events", PROJECT)
    assert not _filter({"deny": [entry]}).is_dataset_allowed("prod_events", PROJECT)


@pytest.mark.xfail(reason=_DOTTED_ENTRIES_AS_WRITTEN, strict=True)
@pytest.mark.parametrize("allow", [["^staging_.*"], ["staging_.*"]])
def test_dataset_name_regex_with_a_dot(allow: List[str]) -> None:
    assert _filter({"allow": allow}).is_dataset_allowed("staging_events", PROJECT)


@pytest.mark.xfail(
    reason="Only a leading `^` is moved in front of the project prefix; a `^` "
    "inside the entry is kept, so `^a|^b` qualifies to `^.*\\.(?:a|^b)` and "
    "its second alternative cannot match a qualified name.",
    strict=True,
)
def test_anchor_in_every_alternative() -> None:
    assert _filter({"allow": ["^a|^b"]}).is_dataset_allowed("b", PROJECT)


def test_qualification_is_idempotent() -> None:
    config = BigQueryV2Config.model_validate(
        {"dataset_pattern": {"allow": ["a|b"], "deny": ["^tmp$"]}}
    )
    qualified = config.dataset_pattern
    validate_pattern_at(config, "dataset_pattern", qualified)
    assert config.dataset_pattern.allow == [r".*\.(?:a|b)"]
    assert config.dataset_pattern.deny == [r"^.*\.tmp$"]


def test_qualification_ignores_a_warm_regex_cache() -> None:
    """A pattern that was already used caches its compiled regexes; the
    qualified pattern must match its own entries, not the cached ones."""
    pattern = AllowDenyPattern(allow=["mydataset"])
    assert pattern.allowed("mydataset")  # warms _compiled_allow
    config = BigQueryV2Config(dataset_pattern=pattern)
    assert BigQueryFilter(config, BigQueryV2Report()).is_dataset_allowed(
        "mydataset", PROJECT
    )
    assert pattern.allow == ["mydataset"]


def test_revalidating_a_warm_pattern_qualifies_it() -> None:
    config = BigQueryV2Config.model_validate({})
    pattern = AllowDenyPattern(allow=["mydataset"])
    assert pattern.allowed("mydataset")  # warms _compiled_allow
    validate_pattern_at(config, "dataset_pattern", pattern)
    assert BigQueryFilter(config, BigQueryV2Report()).is_dataset_allowed(
        "mydataset", PROJECT
    )
    assert pattern.allow == ["mydataset"]


def test_schema_pattern_fallback_is_an_independent_pattern() -> None:
    config = BigQueryV2Config.model_validate(
        {"schema_pattern": {"allow": ["mydataset"]}}
    )
    assert config.dataset_pattern is not config.schema_pattern
    assert config.schema_pattern.allow == ["mydataset"]
    assert BigQueryFilter(config, BigQueryV2Report()).is_dataset_allowed(
        "mydataset", PROJECT
    )


def _columns(
    by_table: Dict[str, List[BigqueryColumn]],
) -> FileBackedDict[List[BigqueryColumn]]:
    columns: FileBackedDict[List[BigqueryColumn]] = FileBackedDict()
    columns.update(by_table)
    return columns


def _status_urns(wus: Iterable[MetadataWorkUnit]) -> List[str]:
    out: List[str] = []
    for wu in wus:
        if isinstance(wu.metadata, MetadataChangeProposalWrapper) and isinstance(
            wu.metadata.aspect, StatusClass
        ):
            assert wu.metadata.entityUrn
            out.append(wu.metadata.entityUrn)
    return out


def _run_over_datasets(schema_gen: Any, names: List[str]) -> List[MetadataWorkUnit]:
    """Run the project's dataset loop over `names`, one table each."""
    as_mock(schema_gen.schema_api.get_columns_for_dataset).side_effect = (
        lambda *_, **__: _columns({})
    )
    with patch.object(
        schema_gen,
        "get_tables_for_dataset",
        side_effect=lambda project_id, dataset: [make_table("t1")],
    ):
        return list(
            schema_gen._process_project_datasets(
                BigqueryProject(
                    PROJECT,
                    PROJECT,
                    datasets=[BigqueryDataset(name=n) for n in names],
                ),
                {},
            )
        )


def test_alternation_deny_keeps_every_alternative_out() -> None:
    """End to end: with deny `tmp|restricted_pii`, restricted_pii is dropped."""
    schema_gen = make_schema_gen(
        dataset_pattern={"deny": ["tmp|restricted_pii"]},
        include_views=False,
        include_table_snapshots=False,
        include_table_constraints=False,
    )

    urns = _status_urns(
        _run_over_datasets(schema_gen, ["restricted_pii", "public_events"])
    )

    assert not any("restricted_pii" in urn for urn in urns), urns
    assert any("public_events" in urn for urn in urns), urns
    assert "restricted_pii.*" in schema_gen.report.filtered
