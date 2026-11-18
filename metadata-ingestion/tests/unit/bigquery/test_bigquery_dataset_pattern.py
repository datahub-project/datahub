"""`dataset_pattern` qualification under `match_fully_qualified_names`.

With the flag on (the default) the connector matches `dataset_pattern` against
`<project_id>.<dataset_name>`, so `backward_compatibility_configs_set` prefixes
entries written for the dataset name alone with "any project". These tests go
through `BigQueryFilter`, the filter the source actually uses.
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


def _filter(dataset_pattern: Dict[str, List[str]]) -> BigQueryFilter:
    config = BigQueryV2Config.model_validate({"dataset_pattern": dataset_pattern})
    assert config.match_fully_qualified_names
    return BigQueryFilter(config, BigQueryV2Report())


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


@pytest.mark.parametrize(
    "allow,dataset",
    [
        (["staging_events"], "staging_events"),
        (["^staging_.*"], "staging_events"),
        (["staging_.*"], "staging_events"),
        (["^staging_.*$"], "staging_events"),
        (["^(a|b)$"], "b"),
        (["a|b"], "a"),
        (["a|b"], "b"),
        (["^a$|^b$"], "b"),
    ],
)
def test_dataset_name_regex_allows_its_dataset(allow: List[str], dataset: str) -> None:
    assert _filter({"allow": allow}).is_dataset_allowed(dataset, PROJECT)


@pytest.mark.parametrize(
    "allow,dataset",
    [
        (["^staging_.*"], "prod_events"),
        (["a|b"], "c"),
        (["^a$|^b$"], "ab"),
    ],
)
def test_dataset_name_regex_still_filters(allow: List[str], dataset: str) -> None:
    assert not _filter({"allow": allow}).is_dataset_allowed(dataset, PROJECT)


@pytest.mark.parametrize("deny", [["^restricted_.*"], ["x|restricted_pii"]])
def test_dataset_name_regex_denies_its_dataset(deny: List[str]) -> None:
    filters = _filter({"deny": deny})
    assert not filters.is_dataset_allowed("restricted_pii", PROJECT)
    assert filters.is_dataset_allowed("public_events", PROJECT)


@pytest.mark.parametrize(
    "entry",
    [
        f"{PROJECT}.prod_.*",
        f"{PROJECT}\\.prod_.*",
        "^my-.*\\.prod_.*",
        ".*\\.prod_.*",
        ".*prod_.*",
    ],
)
def test_project_qualified_entry_is_kept_as_written(entry: str) -> None:
    filters = _filter({"allow": [entry]})
    assert filters.filter_config.dataset_pattern.allow == [entry]
    assert filters.is_dataset_allowed("prod_events", PROJECT)


@pytest.mark.parametrize(
    "entry",
    [
        f"{PROJECT}.*",
        f"^{PROJECT}.*",
        f"{PROJECT}.+",
        f"{PROJECT}.(my_dataset|other)$",
        f"{PROJECT}.[a-z_]+$",
        f"{PROJECT}.\\w+",
        f"{PROJECT}[.]my_dataset",
    ],
)
def test_project_qualified_entry_with_a_wildcard_dot_still_matches(entry: str) -> None:
    """A `.` that may be the separator keeps matching as it did, in both
    lists: the dataset-name reading is added, never substituted."""
    assert _filter({"allow": [entry]}).is_dataset_allowed("my_dataset", PROJECT)
    assert not _filter({"deny": [entry]}).is_dataset_allowed("my_dataset", PROJECT)


def test_escaped_dot_names_the_project() -> None:
    """Dataset names cannot contain a dot, so `a\\.b` can only mean project
    `a`, dataset `b`; it is not read as a dataset-name regex."""
    filters = _filter({"allow": ["my_dataset\\.v2"]})
    assert filters.filter_config.dataset_pattern.allow == ["my_dataset\\.v2"]
    assert filters.is_dataset_allowed("v2", "my_dataset")
    assert not filters.is_dataset_allowed("v2", PROJECT)


@pytest.mark.parametrize("entry", ["(?i)STAGING_events", "(?i)^STAGING_.*"])
def test_leading_inline_flags_stay_in_front(entry: str) -> None:
    filters = _filter({"allow": [entry]})
    assert filters.filter_config.dataset_pattern.allow[0].startswith("(?i)")
    assert filters.is_dataset_allowed("staging_events", PROJECT)


def test_qualification_is_idempotent() -> None:
    config = BigQueryV2Config.model_validate(
        {"dataset_pattern": {"allow": ["^staging_.*", "a|b"], "deny": ["^tmp$"]}}
    )
    qualified = config.dataset_pattern
    assert qualified.allow == [r"^(?:.*\.)?staging_.*", r".*\.a|.*\.b"]
    assert qualified.deny == [r"^.*\.tmp$"]
    validate_pattern_at(config, "dataset_pattern", qualified)
    assert config.dataset_pattern.allow == qualified.allow
    assert config.dataset_pattern.deny == qualified.deny


def test_qualification_ignores_a_warm_regex_cache() -> None:
    """A pattern that was already used caches its compiled regexes; the
    qualified pattern must match its own entries, not the cached ones."""
    pattern = AllowDenyPattern(allow=["mydataset"])
    assert pattern.allowed("mydataset")  # warms _compiled_allow
    config = BigQueryV2Config(dataset_pattern=pattern)
    filters = BigQueryFilter(config, BigQueryV2Report())
    assert filters.is_dataset_allowed("mydataset", PROJECT)
    assert pattern.allow == ["mydataset"]


def test_revalidating_a_warm_pattern_qualifies_it() -> None:
    config = BigQueryV2Config.model_validate({})
    pattern = AllowDenyPattern(allow=["mydataset"])
    assert pattern.allowed("mydataset")  # warms _compiled_allow
    validate_pattern_at(config, "dataset_pattern", pattern)
    filters = BigQueryFilter(config, BigQueryV2Report())
    assert filters.is_dataset_allowed("mydataset", PROJECT)
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


def test_not_qualified_when_matching_bare_names() -> None:
    config = BigQueryV2Config.model_validate(
        {
            "dataset_pattern": {"allow": ["^staging_.*"]},
            "match_fully_qualified_names": False,
        }
    )
    assert config.dataset_pattern.allow == ["^staging_.*"]
    assert BigQueryFilter(config, BigQueryV2Report()).is_dataset_allowed(
        "staging_events", PROJECT
    )


def test_regex_allow_entry_ingests_the_matching_dataset() -> None:
    """End to end: with allow `^staging_.*`, staging_events must be ingested."""
    schema_gen = make_schema_gen(
        dataset_pattern={"allow": ["^staging_.*"]},
        include_views=False,
        include_table_snapshots=False,
        include_table_constraints=False,
    )

    urns = _status_urns(
        _run_over_datasets(schema_gen, ["staging_events", "prod_events"])
    )

    assert any(urn.endswith("staging_events.t1,PROD)") for urn in urns), urns
    assert not any("prod_events" in urn for urn in urns), urns


def test_regex_deny_entry_keeps_the_matching_dataset_out() -> None:
    """End to end: with deny `^restricted_.*`, restricted_pii must be dropped."""
    schema_gen = make_schema_gen(
        dataset_pattern={"deny": ["^restricted_.*"]},
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
