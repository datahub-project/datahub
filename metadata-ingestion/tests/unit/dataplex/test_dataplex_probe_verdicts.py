"""Connection-free Dataplex verdicts, each compared against the predicate
ingestion itself runs for the same input."""

from typing import Dict, Sequence

from datahub.ingestion.agent.filter_check import FilterCheckResult, check_filters
from datahub.ingestion.agent.introspect import describe_source
from datahub.ingestion.source.common.gcp_project_filter import is_project_allowed
from datahub.ingestion.source.dataplex.dataplex_config import (
    DATAPLEX_ASPECT_TYPE_KIND,
    DATAPLEX_ENTRY_FQN_KIND,
    DATAPLEX_ENTRY_GROUP_KIND,
    DATAPLEX_ENTRY_KIND,
    DATAPLEX_PROJECT_KIND,
    DataplexConfig,
)
from datahub.ingestion.source.dataplex.dataplex_context import DataplexContext
from datahub.ingestion.source.dataplex.dataplex_entries import (
    DataplexEntriesProcessor,
    DataplexEntriesReport,
)
from datahub.ingestion.source.dataplex.dataplex_report import DataplexReport

GROUP_SALES = "projects/proj-a/locations/us/entryGroups/sales"
GROUP_BQ = "projects/proj-a/locations/us/entryGroups/@bigquery"
ENTRY_ORDERS = f"{GROUP_SALES}/entries/orders"
# AllowDenyPattern matches from the start of the string, so a pattern on the
# tail of a resource name needs a leading ".*".

EXPORT_MODE: Dict[str, object] = {
    "extraction_method": "export",
    "export_config": {
        "export_job_runner_project": "proj-runner",
        "bucket_base_name": "exports",
    },
}


def _judge(
    config_dict: Dict[str, object],
    kind: str,
    names: Sequence[str],
    parents: Sequence[str] = (),
) -> FilterCheckResult:
    return check_filters(
        source_type="dataplex",
        config_dict=config_dict,
        kind=kind,
        parent_path=list(parents),
        names=list(names),
    )


def _processor(config_dict: Dict[str, object]) -> DataplexEntriesProcessor:
    config = DataplexConfig.model_validate(config_dict)
    return DataplexEntriesProcessor(
        config=config,
        catalog_client=None,
        report=DataplexEntriesReport(),
        source_report=DataplexReport(),
        ctx=DataplexContext(config=config, credentials=None),
    )


def test_entry_group_verdicts_match_should_process_entry_group() -> None:
    config: Dict[str, object] = {
        "project_ids": ["proj-a"],
        "filter_config": {"entry_groups": {"pattern": {"deny": [".*/@bigquery$"]}}},
    }
    names = [GROUP_BQ, GROUP_SALES]
    result = _judge(config, DATAPLEX_ENTRY_GROUP_KIND, names, parents=["proj-a"])
    ingestion = [_processor(config).should_process_entry_group(n) for n in names]
    assert [r.included for r in result.results] == ingestion == [False, True]
    assert result.pattern_field == "filter_config.entry_groups.pattern"
    assert [r.target for r in result.results] == names


def test_entry_name_and_fqn_are_judged_by_their_own_patterns() -> None:
    config: Dict[str, object] = {
        "project_ids": ["proj-a"],
        "filter_config": {
            "entries": {
                "pattern": {"deny": [".*/entries/tmp_"]},
                "fqn_pattern": {"allow": ["^bigquery:proj-a\\."]},
            }
        },
    }
    processor = _processor(config)
    names = [ENTRY_ORDERS, f"{GROUP_SALES}/entries/tmp_scratch"]
    fqns = ["bigquery:proj-a.sales.orders", "bigquery:proj-b.sales.orders"]

    by_name = _judge(config, DATAPLEX_ENTRY_KIND, names)
    by_fqn = _judge(config, DATAPLEX_ENTRY_FQN_KIND, fqns)

    assert (
        [r.included for r in by_name.results]
        == [processor._entry_name_allowed(n) for n in names]
        == [True, False]
    )
    assert (
        [r.included for r in by_fqn.results]
        == [processor._entry_fqn_allowed(f) for f in fqns]
        == [True, False]
    )
    assert by_name.pattern_field == "filter_config.entries.pattern"
    assert by_fqn.pattern_field == "filter_config.entries.fqn_pattern"


def test_an_entry_inside_a_denied_entry_group_is_reported_excluded() -> None:
    config: Dict[str, object] = {
        "project_ids": ["proj-a"],
        "filter_config": {"entry_groups": {"pattern": {"deny": [".*/sales$"]}}},
    }
    result = _judge(
        config, DATAPLEX_ENTRY_KIND, [ENTRY_ORDERS], parents=["proj-a", GROUP_SALES]
    )
    assert result.results[0].included is False
    assert result.results[0].excluded_by == "filter_config.entry_groups.pattern"


def test_an_entry_group_inside_an_unlisted_project_is_reported_excluded() -> None:
    config: Dict[str, object] = {"project_ids": ["proj-a"]}
    group_b = "projects/proj-b/locations/us/entryGroups/sales"
    result = _judge(config, DATAPLEX_ENTRY_GROUP_KIND, [group_b], parents=["proj-b"])
    assert result.results[0].included is False
    assert result.results[0].excluded_by == "project_ids"


def test_project_verdicts_match_is_project_allowed_when_discovering() -> None:
    config: Dict[str, object] = {"project_id_pattern": {"allow": ["^prod-"]}}
    names = ["prod-a", "dev-a"]
    result = _judge(config, DATAPLEX_PROJECT_KIND, names)
    parsed = DataplexConfig.model_validate(config)
    assert (
        [r.included for r in result.results]
        == [is_project_allowed(parsed, n) for n in names]
        == [True, False]
    )
    assert result.pattern_field == "project_id_pattern"
    assert result.warnings == []


def test_project_ids_decide_the_project_verdict() -> None:
    config: Dict[str, object] = {
        "project_ids": ["proj-a"],
        "project_id_pattern": {"deny": ["^proj-a$"]},
    }
    result = _judge(config, DATAPLEX_PROJECT_KIND, ["proj-a", "proj-b"])
    # The pattern alone says proj-a is excluded and proj-b included; ingestion
    # reads proj-a (listed) and never proj-b (unlisted).
    parsed = DataplexConfig.model_validate(config)
    assert [(r.included, r.excluded_by) for r in result.results] == [
        (True, None),
        (False, "project_ids"),
    ]
    assert [r.included for r in result.results] == [
        is_project_allowed(parsed, n) for n in ["proj-a", "proj-b"]
    ]
    assert any("project_id_pattern" in w for w in result.warnings)


def test_aspect_type_verdict_uses_the_default_sync_back_deny() -> None:
    result = _judge(
        {"project_ids": ["proj-a"]},
        DATAPLEX_ASPECT_TYPE_KIND,
        ["datahub-tags", "schema"],
    )
    assert [r.included for r in result.results] == [False, True]
    assert result.pattern_field == "aspect_type_pattern"


def test_export_mode_does_not_judge_the_entry_group_above_an_entry() -> None:
    config: Dict[str, object] = {
        **EXPORT_MODE,
        "project_ids": ["proj-a"],
        "filter_config": {"entry_groups": {"pattern": {"deny": [".*/sales$"]}}},
    }
    entry = _judge(
        config, DATAPLEX_ENTRY_KIND, [ENTRY_ORDERS], parents=["proj-a", GROUP_SALES]
    )
    group = _judge(config, DATAPLEX_ENTRY_GROUP_KIND, [GROUP_SALES], parents=["proj-a"])
    assert entry.results[0].included is True
    assert any("extraction_method" in w for w in group.warnings)


def test_describe_lists_the_nested_filters_under_their_dotted_names() -> None:
    fields = {f.name: f.filters for f in describe_source("dataplex").fields}
    assert fields["filter_config.entry_groups.pattern"] == DATAPLEX_ENTRY_GROUP_KIND
    assert fields["filter_config.entries.pattern"] == DATAPLEX_ENTRY_KIND
    assert fields["filter_config.entries.fqn_pattern"] == DATAPLEX_ENTRY_FQN_KIND
    assert fields["aspect_type_pattern"] == DATAPLEX_ASPECT_TYPE_KIND
    assert fields["project_id_pattern"] == DATAPLEX_PROJECT_KIND


READ_EXPORT_MODE: Dict[str, object] = {
    "extraction_method": "read_export",
    "read_export_config": {"export_paths": {"us": "gs://exports-us/run"}},
}
GROUP_B = "projects/proj-b/locations/us/entryGroups/sales"
ENTRY_B = f"{GROUP_B}/entries/orders"


def test_export_mode_judges_the_project_above_an_entry() -> None:
    # `export` submits jobs scoped to the resolved projects (run_exports), so an
    # entry from an unlisted project is never in the export.
    config: Dict[str, object] = {**EXPORT_MODE, "project_ids": ["proj-a"]}
    for kind, name in (
        (DATAPLEX_ENTRY_KIND, ENTRY_B),
        (DATAPLEX_ENTRY_FQN_KIND, "bigquery:proj-b.sales.orders"),
    ):
        excluded = _judge(config, kind, [name], parents=["proj-b", GROUP_B])
        assert [(r.included, r.excluded_by) for r in excluded.results] == [
            (False, "project_ids")
        ]
    included = _judge(
        config, DATAPLEX_ENTRY_KIND, [ENTRY_ORDERS], parents=["proj-a", GROUP_SALES]
    )
    assert included.results[0].included is True


def test_export_mode_judges_a_discovered_project_by_its_pattern() -> None:
    config: Dict[str, object] = {
        **EXPORT_MODE,
        "project_id_pattern": {"allow": ["^proj-a$"]},
    }
    result = _judge(config, DATAPLEX_ENTRY_KIND, [ENTRY_B], parents=["proj-b", GROUP_B])
    assert [(r.included, r.excluded_by) for r in result.results] == [
        (False, "project_id_pattern")
    ]


def test_read_export_mode_says_the_export_scope_decides_the_project() -> None:
    config: Dict[str, object] = {**READ_EXPORT_MODE, "project_ids": ["proj-a"]}
    result = _judge(config, DATAPLEX_ENTRY_KIND, [ENTRY_B], parents=["proj-b", GROUP_B])
    assert result.results[0].included is True
    assert any("read_export" in w for w in result.warnings)


def test_a_spanner_entry_warns_that_the_entry_group_pattern_is_bypassed() -> None:
    config: Dict[str, object] = {"project_ids": ["proj-a"]}
    spanner = _judge(
        config,
        DATAPLEX_ENTRY_FQN_KIND,
        ["spanner:proj-a.regional-us.inst.db.orders"],
    )
    plain = _judge(config, DATAPLEX_ENTRY_FQN_KIND, ["bigquery:proj-a.sales.orders"])
    assert any("search_entries" in w for w in spanner.warnings)
    assert not any("search_entries" in w for w in plain.warnings)


def test_project_labels_with_a_bare_name_warn_that_labels_were_not_checked() -> None:
    config: Dict[str, object] = {"project_labels": ["env:prod"]}
    bare = _judge(config, DATAPLEX_PROJECT_KIND, ["prod-a"])
    assert bare.results[0].included is True
    assert any("project_labels" in w for w in bare.warnings)
    listed = check_filters(
        source_type="dataplex",
        config_dict=config,
        kind=DATAPLEX_PROJECT_KIND,
        parent_path=[],
        names=["prod-a"],
        attributes=[{"display_name": "prod-a"}],
    )
    assert not any("project_labels" in w for w in listed.warnings)
