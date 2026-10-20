"""Connection-free Dataplex verdicts. Agreement with what ingestion really
emits is pinned in test_dataplex_probe_parity.py."""

from typing import Dict, Sequence, Tuple

from datahub.ingestion.agent.filter_check import FilterCheckResult, check_filters
from datahub.ingestion.agent.introspect import describe_source
from datahub.ingestion.agent.verdicts import VerdictContext
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


def test_discovered_projects_are_judged_by_project_id_pattern() -> None:
    config: Dict[str, object] = {"project_id_pattern": {"allow": ["^prod-"]}}
    result = _judge(config, DATAPLEX_PROJECT_KIND, ["prod-a", "dev-a"])
    assert [(r.included, r.excluded_by) for r in result.results] == [
        (True, None),
        (False, "project_id_pattern"),
    ]
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
    assert [(r.included, r.excluded_by) for r in result.results] == [
        (True, None),
        (False, "project_ids"),
    ]
    assert any("project_id_pattern" in w for w in result.warnings)


def test_default_project_pattern_does_not_warn_after_it_has_been_used() -> None:
    config = DataplexConfig.model_validate({"project_ids": ["proj-a"]})
    # allowed() fills the pattern's compiled-regex caches, which a plain
    # equality check against a fresh allow_all() would then see as a difference.
    config.project_id_pattern.allowed("proj-a")
    warnings: list = []
    ctx = VerdictContext(
        kind=DATAPLEX_PROJECT_KIND,
        name="proj-a",
        target="proj-a",
        parent_path=(),
        pattern_field="project_id_pattern",
        structural=None,
        attributes={},
        warn=warnings.append,
    )
    verdict = config.probe_verdict_override(ctx)
    assert verdict is not None and verdict.included
    assert warnings == []


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


GROUP_SPANNER = "projects/proj-a/locations/us/entryGroups/@spanner"


def test_a_spanner_entry_under_a_denied_spanner_group_is_reported_included() -> None:
    # search_entries reads Spanner entries without consulting
    # entry_groups.pattern, so denying @spanner does not drop them.
    config: Dict[str, object] = {
        "project_ids": ["proj-a"],
        "filter_config": {"entry_groups": {"pattern": {"deny": [".*/@spanner$"]}}},
    }
    entry = _judge(
        config,
        DATAPLEX_ENTRY_KIND,
        [f"{GROUP_SPANNER}/entries/orders"],
        parents=["proj-a", GROUP_SPANNER],
    )
    assert entry.results[0].included is True
    assert any("search_entries" in w for w in entry.warnings)
    # The project above it still decides.
    other_project = "projects/proj-b/locations/us/entryGroups/@spanner"
    unlisted = _judge(
        config,
        DATAPLEX_ENTRY_KIND,
        [f"{other_project}/entries/orders"],
        parents=["proj-b", other_project],
    )
    assert [(r.included, r.excluded_by) for r in unlisted.results] == [
        (False, "project_ids")
    ]


def test_export_mode_reports_a_denied_entry_group_included() -> None:
    # process_exported_entries never consults entry_groups.pattern, so the
    # group's entries are exported whatever it says.
    config: Dict[str, object] = {
        **EXPORT_MODE,
        "project_ids": ["proj-a"],
        "filter_config": {"entry_groups": {"pattern": {"deny": [".*/sales$"]}}},
    }
    group = _judge(config, DATAPLEX_ENTRY_GROUP_KIND, [GROUP_SALES], parents=["proj-a"])
    assert group.results[0].included is True
    assert any("extraction_method" in w for w in group.warnings)


def test_export_mode_entry_from_run_warns_that_project_labels_were_not_checked() -> (
    None
):
    # An entry record carries entry attributes, not the project's labels, so
    # the labels resolve_gcp_projects narrows the export by were not judged.
    config: Dict[str, object] = {**EXPORT_MODE, "project_labels": ["env:prod"]}
    result = check_filters(
        source_type="dataplex",
        config_dict=config,
        kind=DATAPLEX_ENTRY_KIND,
        parent_path=["proj-a", GROUP_SALES],
        names=[ENTRY_ORDERS],
        attributes=[
            {"entry_type": "bigquery-table", "fully_qualified_name": "bigquery:x.y.z"}
        ],
    )
    assert any("project_labels" in w for w in result.warnings)


def _judge_attrs(
    config_dict: Dict[str, object],
    kind: str,
    names: Sequence[str],
    attributes: Sequence[Dict[str, str]],
    parents: Sequence[str] = (),
) -> FilterCheckResult:
    return check_filters(
        source_type="dataplex",
        config_dict=config_dict,
        kind=kind,
        parent_path=list(parents),
        names=list(names),
        attributes=list(attributes),
    )


def test_read_export_mode_includes_a_group_in_an_unlisted_project() -> None:
    # read_export reads whatever export it is pointed at, and never scopes a
    # group by project, so project_ids must not exclude it via its parent.
    config: Dict[str, object] = {**READ_EXPORT_MODE, "project_ids": ["catalog-proj"]}
    group = "projects/tenant-proj/locations/us/entryGroups/sales"
    result = _judge(config, DATAPLEX_ENTRY_GROUP_KIND, [group], parents=["tenant-proj"])
    assert [(r.included, r.excluded_by) for r in result.results] == [(True, None)]


def test_export_mode_finds_the_project_in_a_lone_group_parent() -> None:
    # The innermost container alone is a natural --parent; its resource name
    # holds the project.
    config: Dict[str, object] = {**EXPORT_MODE, "project_ids": ["proj-a"]}
    kept = _judge(config, DATAPLEX_ENTRY_KIND, [ENTRY_ORDERS], parents=[GROUP_SALES])
    assert kept.results[0].included
    dropped = _judge(config, DATAPLEX_ENTRY_KIND, [ENTRY_B], parents=[GROUP_B])
    assert [(r.included, r.excluded_by) for r in dropped.results] == [
        (False, "project_ids")
    ]


def test_export_mode_reads_the_project_from_the_entry_name_without_a_parent() -> None:
    config: Dict[str, object] = {**EXPORT_MODE, "project_ids": ["proj-a"]}
    result = _judge(config, DATAPLEX_ENTRY_KIND, [ENTRY_B])
    assert [(r.included, r.excluded_by) for r in result.results] == [
        (False, "project_ids")
    ]


def test_export_mode_warns_when_no_project_can_be_found() -> None:
    config: Dict[str, object] = {**EXPORT_MODE, "project_ids": ["proj-a"]}
    result = _judge(config, DATAPLEX_ENTRY_FQN_KIND, ["bigquery:proj-b.sales.orders"])
    assert result.results[0].included
    assert result.warnings


def test_export_mode_does_not_judge_a_project_number_against_project_ids() -> None:
    config: Dict[str, object] = {**EXPORT_MODE, "project_ids": ["proj-a"]}
    numbered = "projects/123456/locations/us/entryGroups/sales/entries/orders"
    result = _judge(config, DATAPLEX_ENTRY_KIND, [numbered])
    assert result.results[0].included
    assert result.warnings


def test_entry_kind_also_applies_fqn_pattern_from_a_listed_record() -> None:
    # Ingestion keeps an entry only if both patterns allow it.
    config: Dict[str, object] = {
        "project_ids": ["proj-a"],
        "filter_config": {"entries": {"fqn_pattern": {"deny": ["bigquery:.*"]}}},
    }
    names = [ENTRY_ORDERS, f"{GROUP_SALES}/entries/raw", f"{GROUP_SALES}/entries/x"]
    result = _judge_attrs(
        config,
        DATAPLEX_ENTRY_KIND,
        names,
        attributes=[
            {"fully_qualified_name": "bigquery:proj-a.sales.orders"},
            {"fully_qualified_name": "custom:raw"},
            {"fully_qualified_name": ""},
        ],
        parents=["proj-a", GROUP_SALES],
    )
    assert [(r.included, r.excluded_by) for r in result.results] == [
        (False, "filter_config.entries.fqn_pattern"),
        (True, None),
        (False, "missing_fully_qualified_name"),
    ]


def test_entry_kind_without_the_fqn_warns_that_fqn_pattern_was_not_judged() -> None:
    config: Dict[str, object] = {
        "project_ids": ["proj-a"],
        "filter_config": {"entries": {"fqn_pattern": {"deny": ["bigquery:.*"]}}},
    }
    result = _judge(
        config, DATAPLEX_ENTRY_KIND, [ENTRY_ORDERS], parents=["proj-a", GROUP_SALES]
    )
    assert result.results[0].included
    assert any("fqn_pattern" in w for w in result.warnings)


def test_export_mode_finds_the_project_in_a_bare_project_resource_parent() -> None:
    # `probe filter --parent projects/proj-a --name orders`
    cases: Sequence[Tuple[Dict[str, object], str]] = (
        ({**EXPORT_MODE, "project_ids": ["proj-b"]}, "project_ids"),
        (
            {**EXPORT_MODE, "project_id_pattern": {"allow": ["^proj-b$"]}},
            "project_id_pattern",
        ),
    )
    for config, field in cases:
        result = _judge(
            config, DATAPLEX_ENTRY_KIND, ["orders"], parents=["projects/proj-a"]
        )
        assert [(r.included, r.excluded_by) for r in result.results] == [(False, field)]


def test_a_missing_fqn_is_reported_before_a_denied_entry_name() -> None:
    # Ingestion's _report_and_should_process_entry counts a missing FQN first,
    # whatever the patterns say.
    config: Dict[str, object] = {
        "project_ids": ["proj-a"],
        "filter_config": {"entries": {"pattern": {"deny": [".*/entries/orders$"]}}},
    }
    result = _judge_attrs(
        config,
        DATAPLEX_ENTRY_KIND,
        [ENTRY_ORDERS],
        attributes=[{"fully_qualified_name": ""}],
        parents=["proj-a", GROUP_SALES],
    )
    assert [(r.included, r.excluded_by) for r in result.results] == [
        (False, "missing_fully_qualified_name")
    ]
