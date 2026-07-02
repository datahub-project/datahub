"""Unit tests for Dataplex entry-type mappers (payload -> DataHub entities)."""

import string
from typing import Optional, cast
from unittest.mock import Mock, patch

import pytest
from google.cloud import dataplex_v1

from datahub.ingestion.source.dataplex.dataplex_config import DataplexConfig
from datahub.ingestion.source.dataplex.dataplex_mappers import (
    ENTRY_MAPPERS,
    ContainerIdentity,
    DatasetIdentity,
    EntryMappingContext,
    dataset_urn_from_fqn_only,
    get_entry_mapper,
    is_lineage_supported,
)
from datahub.sdk.container import Container
from datahub.sdk.dataset import Dataset

ENTRY_TYPE_PREFIX = "projects/123/locations/global/entryTypes/"


def _make_entry(
    *,
    short_name: str,
    fqn: str,
    parent_entry: str = "",
    name: Optional[str] = None,
) -> dataplex_v1.Entry:
    entry = Mock(spec=dataplex_v1.Entry)
    entry.name = name or f"projects/p/locations/us/entryGroups/g/entries/{short_name}"
    entry.entry_type = f"{ENTRY_TYPE_PREFIX}{short_name}"
    entry.fully_qualified_name = fqn
    entry.parent_entry = parent_entry
    entry.entry_source = None
    entry.aspects = {}
    return entry


def _ctx(include_schema: bool = False) -> EntryMappingContext:
    config = DataplexConfig(project_ids=["my-project"], env="PROD")
    config.include_schema = include_schema
    return EntryMappingContext(config=config, location="us-west2", report=Mock())


# (short_name, fqn, parent_entry, expected main type, expected dataset name or None)
CASES = [
    ("bigquery-dataset", "bigquery:my-project.my_dataset", "", "Container", None),
    (
        "bigquery-table",
        "bigquery:my-project.my_dataset.my_table",
        "projects/my-project/locations/us/entryGroups/@bigquery/entries/"
        "bigquery.googleapis.com/projects/my-project/datasets/my_dataset",
        "Dataset",
        "my-project.my_dataset.my_table",
    ),
    (
        "bigquery-view",
        "bigquery:my-project.my_dataset.my_view",
        "projects/my-project/locations/us/entryGroups/@bigquery/entries/"
        "bigquery.googleapis.com/projects/my-project/datasets/my_dataset",
        "Dataset",
        "my-project.my_dataset.my_view",
    ),
    (
        "cloudsql-mysql-instance",
        "cloudsql_mysql:my-project.us-west2.my-instance",
        "",
        "Container",
        None,
    ),
    (
        "cloudsql-mysql-database",
        "cloudsql_mysql:my-project.us-west2.my-instance.my-db",
        "projects/my-project/locations/us-west2/entryGroups/@cloudsql/entries/"
        "cloudsql.googleapis.com/projects/my-project/locations/us-west2/instances/my-instance",
        "Container",
        None,
    ),
    (
        "cloudsql-mysql-table",
        "cloudsql_mysql:my-project.us-west2.my-instance.my-db.my_table",
        "projects/my-project/locations/us-west2/entryGroups/@cloudsql/entries/"
        "cloudsql.googleapis.com/projects/my-project/locations/us-west2/instances/my-instance/databases/my-db",
        "Dataset",
        "my-project.us-west2.my-instance.my-db.my_table",
    ),
    (
        "cloud-spanner-instance",
        "spanner:my-project.regional-us-west2.my-instance",
        "",
        "Container",
        None,
    ),
    (
        "cloud-spanner-database",
        "spanner:my-project.regional-us-west2.my-instance.my-db",
        "projects/my-project/locations/us-west2/entryGroups/@spanner/entries/"
        "spanner.googleapis.com/projects/my-project/instances/my-instance",
        "Container",
        None,
    ),
    (
        "cloud-spanner-table",
        "spanner:my-project.regional-us-west2.my-instance.my-db.my_table",
        "projects/my-project/locations/us-west2/entryGroups/@spanner/entries/"
        "spanner.googleapis.com/projects/my-project/instances/my-instance/databases/my-db",
        "Dataset",
        "my-project.regional-us-west2.my-instance.my-db.my_table",
    ),
    (
        "cloud-spanner-graph",
        "spanner:graph:my-project.regional-us-west2.my-instance.my-db.MyGraph",
        "projects/my-project/locations/us-west2/entryGroups/@spanner/entries/"
        "spanner.googleapis.com/projects/my-project/instances/my-instance/databases/my-db",
        "Dataset",
        "my-project.regional-us-west2.my-instance.my-db.MyGraph",
    ),
    (
        "cloud-bigtable-instance",
        "bigtable:my-project.my-instance",
        "",
        "Container",
        None,
    ),
    (
        "cloud-bigtable-table",
        "bigtable:my-project.my-instance.my_table",
        "projects/my-project/locations/global/entryGroups/@bigtable/entries/"
        "bigtable.googleapis.com/projects/my-project/instances/my-instance",
        "Dataset",
        "my-project.my-instance.my_table",
    ),
    (
        "pubsub-topic",
        "pubsub:topic:my-project.my-topic",
        "",
        "Dataset",
        "my-project.my-topic",
    ),
    (
        "vertexai-dataset",
        "vertex_ai:dataset:my-project.us-west2.123456",
        "",
        "Dataset",
        "my-project.us-west2.123456",
    ),
]


def test_registry_covers_all_supported_entry_types() -> None:
    assert set(ENTRY_MAPPERS.keys()) == {case[0] for case in CASES}


@pytest.mark.parametrize("short_name,fqn,parent_entry,main_type,dataset_name", CASES)
def test_mapper_builds_expected_entity(
    short_name: str,
    fqn: str,
    parent_entry: str,
    main_type: str,
    dataset_name: Optional[str],
) -> None:
    mapper = ENTRY_MAPPERS[short_name]
    result = mapper.map(
        _make_entry(short_name=short_name, fqn=fqn, parent_entry=parent_entry), _ctx()
    )

    assert result is not None
    assert result.main_entity is not None

    # Every entry also emits its owning project container as an additional entity.
    assert len(result.additional_entities) == 1
    project_container = result.additional_entities[0]
    assert isinstance(project_container, Container)
    assert project_container.display_name == "my-project"
    assert project_container.parent_container is None

    if main_type == "Dataset":
        assert isinstance(result.main_entity, Dataset)
        assert dataset_name is not None
        expected_platform = mapper.datahub_platform
        assert result.main_entity.urn.urn() == (
            f"urn:li:dataset:(urn:li:dataPlatform:{expected_platform},{dataset_name},PROD)"
        )
        # Dataset entries produce a lineage record with the same identity.
        assert result.lineage_entry is not None
        assert result.lineage_entry.datahub_dataset_name == dataset_name
        assert result.lineage_entry.dataplex_location == "us-west2"
    else:
        assert isinstance(result.main_entity, Container)
        assert result.lineage_entry is None


@pytest.mark.parametrize("short_name,fqn,parent_entry,main_type,dataset_name", CASES)
def test_parent_entry_link_present_iff_entry_has_parent(
    short_name: str,
    fqn: str,
    parent_entry: str,
    main_type: str,
    dataset_name: Optional[str],
) -> None:
    """A mapper declares a ParentEntryLink exactly when the entry has a
    parent_entry finer than its project; otherwise the property is None.

    When present, build the entry and assert the parent-entry regex actually
    resolves a parent container URN — this pins every platform's parent-entry
    pattern (a regression would silently emit an entity with no parent)."""
    mapper = ENTRY_MAPPERS[short_name]
    if not parent_entry:
        assert mapper.dataplex_parent_entry is None
        return

    assert mapper.dataplex_parent_entry is not None
    result = mapper.map(
        _make_entry(short_name=short_name, fqn=fqn, parent_entry=parent_entry), _ctx()
    )
    assert result is not None and result.main_entity is not None
    assert isinstance(result.main_entity, (Dataset, Container))
    assert str(result.main_entity.parent_container).startswith("urn:li:container:")


def test_dataset_missing_expected_parent_warns_and_omits_parent() -> None:
    ctx = _ctx()
    # bigquery-table expects a parent_entry; empty one triggers a warning + fallback.
    result = ENTRY_MAPPERS["bigquery-table"].map(
        _make_entry(
            short_name="bigquery-table",
            fqn="bigquery:my-project.my_dataset.my_table",
            parent_entry="",
        ),
        ctx,
    )
    assert result is not None and result.main_entity is not None
    assert isinstance(result.main_entity, Dataset)
    assert result.main_entity.parent_container is None
    mock_warning = cast(Mock, ctx.report.warning)
    mock_warning.assert_called_once()
    assert mock_warning.call_args.kwargs["title"] == "Missing Dataplex parent_entry"


def test_top_level_dataset_uses_project_parent_without_warning() -> None:
    ctx = _ctx()
    # pubsub-topic has no parent_entry regex: parent linkage is the project key.
    result = ENTRY_MAPPERS["pubsub-topic"].map(
        _make_entry(short_name="pubsub-topic", fqn="pubsub:topic:my-project.my-topic"),
        ctx,
    )
    assert result is not None and result.main_entity is not None
    assert isinstance(result.main_entity, Dataset)
    assert result.main_entity.parent_container is not None
    assert str(result.main_entity.parent_container).startswith("urn:li:container:")
    mock_warning = cast(Mock, ctx.report.warning)
    mock_warning.assert_not_called()


def test_dataset_unparseable_parent_entry_warns_and_omits_parent() -> None:
    ctx = _ctx()
    # parent_entry is present but does not match the parent-entry regex.
    result = ENTRY_MAPPERS["bigquery-table"].map(
        _make_entry(
            short_name="bigquery-table",
            fqn="bigquery:my-project.my_dataset.my_table",
            parent_entry="not-a-valid-parent-entry-path",
        ),
        ctx,
    )
    assert result is not None and result.main_entity is not None
    assert isinstance(result.main_entity, Dataset)
    assert result.main_entity.parent_container is None
    assert (
        cast(Mock, ctx.report.warning).call_args.kwargs["title"]
        == "Unparseable Dataplex parent_entry"
    )


def test_map_returns_none_for_missing_fqn() -> None:
    result = ENTRY_MAPPERS["bigquery-table"].map(
        _make_entry(short_name="bigquery-table", fqn=""), _ctx()
    )
    assert result is None


def test_map_returns_none_for_unparseable_fqn() -> None:
    result = ENTRY_MAPPERS["bigquery-table"].map(
        _make_entry(short_name="bigquery-table", fqn="bigquery"), _ctx()
    )
    assert result is None


def test_include_schema_toggle_does_not_break_build() -> None:
    # With no aspects present, schema extraction returns None regardless.
    result = ENTRY_MAPPERS["bigquery-table"].map(
        _make_entry(
            short_name="bigquery-table",
            fqn="bigquery:my-project.my_dataset.my_table",
        ),
        _ctx(include_schema=True),
    )
    assert result is not None and isinstance(result.main_entity, Dataset)


def test_get_entry_mapper_resolves_and_warns() -> None:
    report = Mock()
    assert get_entry_mapper(f"{ENTRY_TYPE_PREFIX}bigquery-table", report) is not None
    report.warning.assert_not_called()

    # Invalid entry_type format (not a full path).
    assert get_entry_mapper("bigquery-table", report) is None
    assert (
        report.warning.call_args.kwargs["title"] == "Invalid Dataplex entry type format"
    )

    # Well-formed path but unsupported short name.
    assert get_entry_mapper(f"{ENTRY_TYPE_PREFIX}unknown-type", report) is None
    assert report.warning.call_args.kwargs["title"] == "Unsupported Dataplex entry type"


DATASET_TYPES = {c[0] for c in CASES if c[3] == "Dataset"}


@pytest.mark.parametrize("short_name", [c[0] for c in CASES])
def test_is_lineage_supported_matches_dataset_types(short_name: str) -> None:
    assert is_lineage_supported(short_name) == (short_name in DATASET_TYPES)


def test_is_lineage_supported_unknown_type() -> None:
    assert not is_lineage_supported("unknown-entry-type")


@pytest.mark.parametrize(
    "fqn,expected_urn",
    [
        (
            "bigquery:my-project.my_dataset.my_table",
            "urn:li:dataset:(urn:li:dataPlatform:bigquery,my-project.my_dataset.my_table,PROD)",
        ),
        (
            "pubsub:topic:my-project.my-topic",
            "urn:li:dataset:(urn:li:dataPlatform:pubsub,my-project.my-topic,PROD)",
        ),
        (
            "bigtable:my-project.my-instance.my_table",
            "urn:li:dataset:(urn:li:dataPlatform:bigtable,my-project.my-instance.my_table,PROD)",
        ),
        ("unknown:project.dataset.table", None),
        ("invalid", None),
    ],
)
def test_dataset_urn_from_fqn_only(fqn: str, expected_urn: Optional[str]) -> None:
    assert dataset_urn_from_fqn_only(fqn, env="PROD") == expected_urn


def _format_fields(fmt: str) -> set[str]:
    return {
        field_name
        for _, field_name, _, _ in string.Formatter().parse(fmt)
        if field_name
    }


@pytest.mark.parametrize("short_name", sorted(DATASET_TYPES))
def test_dataset_name_format_fields_are_captured_by_fqn_regex(short_name: str) -> None:
    """Guardrail: every field referenced by a dataset name format must be a named
    group in that mapper's FQN regex, else name extraction silently fails."""
    mapper = ENTRY_MAPPERS[short_name]
    identity = mapper.datahub_identity
    assert isinstance(identity, DatasetIdentity)
    fmt_fields = _format_fields(identity.name_format)
    regex_groups = set(mapper.dataplex_fqn_regex.groupindex.keys())
    assert fmt_fields.issubset(regex_groups), (
        f"{short_name}: format fields {fmt_fields} not all in regex groups {regex_groups}"
    )


CONTAINER_TYPES = {c[0] for c in CASES if c[3] == "Container"}


@pytest.mark.parametrize("short_name", sorted(CONTAINER_TYPES))
def test_container_fqn_regex_groups_are_key_fields(short_name: str) -> None:
    """Guardrail: every FQN named group of a container mapper must be a field on
    its ContainerKey class, else key identity silently drops fields. Also pins the
    Spanner ``regional-{location}`` -> ``location`` normalization."""
    mapper = ENTRY_MAPPERS[short_name]
    identity = mapper.datahub_identity
    assert isinstance(identity, ContainerIdentity)
    regex_groups = set(mapper.dataplex_fqn_regex.groupindex.keys())
    key_fields = set(identity.key_class.model_fields.keys())
    assert regex_groups <= key_fields, (
        f"{short_name}: regex groups {regex_groups} not all key fields {key_fields}"
    )


def test_container_builds_expected_key_urn_for_non_bigquery_types() -> None:
    """Concrete container-identity checks for non-BigQuery platforms (the golden
    files are BigQuery-only), incl. the Spanner regional- location normalization."""
    for short_name, fqn, parent_entry, _, _ in CASES:
        if short_name not in {"cloud-spanner-database", "cloudsql-mysql-database"}:
            continue
        result = ENTRY_MAPPERS[short_name].map(
            _make_entry(short_name=short_name, fqn=fqn, parent_entry=parent_entry),
            _ctx(),
        )
        assert result is not None
        assert isinstance(result.main_entity, Container)
        # container's own key and its parent both resolve to container URNs
        assert result.main_entity.urn.urn().startswith("urn:li:container:")
        assert str(result.main_entity.parent_container).startswith("urn:li:container:")


def test_graph_schema_fallback_used_only_for_spanner_graph() -> None:
    """The graph-schema aspect fallback is scoped to cloud-spanner-graph; other
    dataset types use only the standard schema aspect."""
    mappers_module = "datahub.ingestion.source.dataplex.dataplex_mappers"
    with (
        patch(f"{mappers_module}.extract_schema_from_entry_aspects", return_value=None),
        patch(
            f"{mappers_module}.extract_graph_schema_from_entry_aspects",
            return_value=None,
        ) as mock_graph,
    ):
        ENTRY_MAPPERS["cloud-spanner-graph"].map(
            _make_entry(
                short_name="cloud-spanner-graph",
                fqn="spanner:graph:my-project.regional-us-west2.my-instance.my-db.MyGraph",
            ),
            _ctx(include_schema=True),
        )
        assert mock_graph.called

        mock_graph.reset_mock()
        ENTRY_MAPPERS["cloud-spanner-table"].map(
            _make_entry(
                short_name="cloud-spanner-table",
                fqn="spanner:my-project.regional-us-west2.my-instance.my-db.my_table",
            ),
            _ctx(include_schema=True),
        )
        assert not mock_graph.called
