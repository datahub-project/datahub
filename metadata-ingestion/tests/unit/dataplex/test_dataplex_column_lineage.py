"""Unit tests for Dataplex column-level lineage extraction."""

from dataclasses import dataclass, field
from typing import Any, Callable, List, cast
from unittest.mock import MagicMock, Mock

import pytest

from datahub.ingestion.source.dataplex.dataplex_config import DataplexConfig
from datahub.ingestion.source.dataplex.dataplex_helpers import EntryDataTuple
from datahub.ingestion.source.dataplex.dataplex_lineage import (
    COLUMN_LINK_BATCH_SIZE,
    DataplexLineageExtractor,
)
from datahub.ingestion.source.dataplex.dataplex_report import DataplexReport
from tests.unit.dataplex.workunit_assertions import (
    aspect_of,
)

SCAN_PAIRS = [("my-project", "us-central1")]


@dataclass
class FakeEntityReference:
    """Stands in for the Data Lineage API's EntityReference on a link."""

    fully_qualified_name: str = ""
    field: List[str] = field(default_factory=list)


@dataclass
class FakeLink:
    source: FakeEntityReference
    target: FakeEntityReference
    name: str = ""


def make_entry(
    *,
    fqn: str = "bigquery:my-project.my_dataset.my_table",
    dataset_name: str = "my-project.my_dataset.my_table",
    schema_field_paths: tuple = (),
    entry_type: str = "bigquery-table",
    platform: str = "bigquery",
) -> EntryDataTuple:
    return EntryDataTuple(
        dataplex_entry_short_name=dataset_name.rsplit(".", 1)[-1],
        dataplex_entry_name=f"projects/p/locations/us/entryGroups/g/entries/{dataset_name}",
        dataplex_location="us",
        dataplex_entry_fqn=fqn,
        dataplex_entry_type_short_name=entry_type,
        datahub_platform=platform,
        datahub_dataset_name=dataset_name,
        datahub_dataset_urn=(
            f"urn:li:dataset:(urn:li:dataPlatform:{platform},{dataset_name},PROD)"
        ),
        schema_field_paths=schema_field_paths,
    )


def make_extractor(
    lineage_client: Mock,
    *,
    include_column_lineage: bool = True,
) -> DataplexLineageExtractor:
    config = DataplexConfig(
        project_ids=["my-project"],
        entries_locations=["us"],
        lineage_locations=["us-central1"],
        include_lineage=True,
        include_column_lineage=include_column_lineage,
    )
    return DataplexLineageExtractor(
        config=config,
        report=DataplexReport().lineage_report,
        source_report=Mock(),
        lineage_client=lineage_client,
    )


@pytest.fixture
def lineage_client() -> MagicMock:
    return MagicMock()


def route_search_links(
    table_links: List[FakeLink], column_links: List[FakeLink]
) -> Callable[[Any], List[FakeLink]]:
    """search_links side effect: table-scoped vs column-scoped requests."""

    def _side_effect(request):
        if getattr(request, "targets", None) and request.targets.entities:
            return column_links
        return table_links

    return _side_effect


class TestColumnLineageExtraction:
    def test_disabled_by_default_issues_no_column_calls(
        self, lineage_client: MagicMock
    ) -> None:
        extractor = make_extractor(lineage_client, include_column_lineage=False)
        lineage_client.search_links.side_effect = route_search_links(
            [
                FakeLink(
                    source=FakeEntityReference("bigquery:my-project.my_dataset.src"),
                    target=FakeEntityReference(
                        "bigquery:my-project.my_dataset.my_table"
                    ),
                )
            ],
            [],
        )

        result = extractor.get_lineage_for_entry(
            make_entry(schema_field_paths=("col_a",)), SCAN_PAIRS
        )

        assert result is not None
        assert result["column_mappings"] == {}
        assert extractor.report.num_column_lineage_api_calls == 0

    def test_emits_fine_grained_lineage_on_the_same_aspect(
        self, lineage_client: MagicMock
    ) -> None:
        extractor = make_extractor(lineage_client)
        upstream_fqn = "bigquery:my-project.my_dataset.upstream_table"
        lineage_client.search_links.side_effect = route_search_links(
            [
                FakeLink(
                    source=FakeEntityReference(upstream_fqn),
                    target=FakeEntityReference(
                        "bigquery:my-project.my_dataset.my_table"
                    ),
                )
            ],
            [
                FakeLink(
                    source=FakeEntityReference(upstream_fqn, ["col_a"]),
                    target=FakeEntityReference(
                        "bigquery:my-project.my_dataset.my_table", ["col_a"]
                    ),
                )
            ],
        )
        entry = make_entry(schema_field_paths=("col_a", "col_b"))

        workunits = list(extractor.get_lineage_workunits([entry], SCAN_PAIRS))

        assert len(workunits) == 1
        aspect = aspect_of(workunits[0])
        assert len(aspect.upstreams) == 1
        assert len(aspect.fineGrainedLineages) == 1
        fine_grained = aspect.fineGrainedLineages[0]
        assert fine_grained.upstreams == [
            "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:bigquery,"
            "my-project.my_dataset.upstream_table,PROD),col_a)"
        ]
        assert fine_grained.downstreams == [
            "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:bigquery,"
            "my-project.my_dataset.my_table,PROD),col_a)"
        ]
        # col_b had no column link.
        assert extractor.report.num_columns_without_lineage == 1

    def test_column_names_match_case_insensitively(
        self, lineage_client: MagicMock
    ) -> None:
        extractor = make_extractor(lineage_client)
        upstream_fqn = "bigquery:my-project.my_dataset.upstream_table"
        lineage_client.search_links.side_effect = route_search_links(
            [
                FakeLink(
                    source=FakeEntityReference(upstream_fqn),
                    target=FakeEntityReference(
                        "bigquery:my-project.my_dataset.my_table"
                    ),
                )
            ],
            [
                FakeLink(
                    source=FakeEntityReference(upstream_fqn, ["col_a"]),
                    # The API reports a different casing than the schema.
                    target=FakeEntityReference(
                        "bigquery:my-project.my_dataset.my_table", ["COL_A"]
                    ),
                )
            ],
        )
        entry = make_entry(schema_field_paths=("col_a",))

        result = extractor.get_lineage_for_entry(entry, SCAN_PAIRS)

        assert result is not None
        mappings = result["column_mappings"]
        assert mappings == {"col_a": [(upstream_fqn, "col_a")]}
        assert extractor.report.num_column_names_unmatched == 0

    def test_unknown_downstream_column_is_counted_and_skipped(
        self, lineage_client: MagicMock
    ) -> None:
        extractor = make_extractor(lineage_client)
        upstream_fqn = "bigquery:my-project.my_dataset.upstream_table"
        lineage_client.search_links.side_effect = route_search_links(
            [
                FakeLink(
                    source=FakeEntityReference(upstream_fqn),
                    target=FakeEntityReference(
                        "bigquery:my-project.my_dataset.my_table"
                    ),
                )
            ],
            [
                FakeLink(
                    source=FakeEntityReference(upstream_fqn, ["col_a"]),
                    target=FakeEntityReference(
                        "bigquery:my-project.my_dataset.my_table", ["not_in_schema"]
                    ),
                )
            ],
        )
        entry = make_entry(schema_field_paths=("col_a",))

        result = extractor.get_lineage_for_entry(entry, SCAN_PAIRS)

        assert result is not None
        mappings = result["column_mappings"]
        assert mappings == {}
        assert extractor.report.num_column_names_unmatched == 1

    def test_nested_columns_query_simple_paths_and_emit_v2_paths(
        self, lineage_client: MagicMock
    ) -> None:
        """The API addresses columns by plain dotted names, but the emitted
        downstream must be the entry's own [version=2.0] fieldPath."""
        extractor = make_extractor(lineage_client)
        upstream_fqn = "bigquery:my-project.my_dataset.upstream_table"
        v2_path = (
            "[version=2.0].[type=struct].[type=struct].payload.[type=string].item_id"
        )
        lineage_client.search_links.side_effect = route_search_links(
            [
                FakeLink(
                    source=FakeEntityReference(upstream_fqn),
                    target=FakeEntityReference(
                        "bigquery:my-project.my_dataset.my_table"
                    ),
                )
            ],
            [
                FakeLink(
                    source=FakeEntityReference(upstream_fqn, ["item_id"]),
                    target=FakeEntityReference(
                        "bigquery:my-project.my_dataset.my_table",
                        ["payload", "item_id"],
                    ),
                )
            ],
        )
        entry = make_entry(schema_field_paths=(v2_path,))

        result = extractor.get_lineage_for_entry(entry, SCAN_PAIRS)

        assert result is not None
        mappings = result["column_mappings"]
        # Queried with the simple path...
        column_request = lineage_client.search_links.call_args_list[-1].kwargs[
            "request"
        ]
        assert [entity.field for entity in column_request.targets.entities] == [
            ["payload", "item_id"]
        ]
        # ...and keyed by the entry's own v2 fieldPath.
        assert mappings == {v2_path: [(upstream_fqn, "item_id")]}

    def test_upstream_column_remapped_to_its_own_field_path(
        self, lineage_client: MagicMock
    ) -> None:
        """An upstream ingested in this run gets its v2 fieldPath back, so both
        ends of the fine-grained edge exist in schemaMetadata."""
        extractor = make_extractor(lineage_client)
        upstream_fqn = "bigquery:my-project.my_dataset.upstream_table"
        upstream_v2 = (
            "[version=2.0].[type=struct].[type=struct].payload.[type=string].item_id"
        )
        lineage_client.search_links.side_effect = route_search_links(
            [
                FakeLink(
                    source=FakeEntityReference(upstream_fqn),
                    target=FakeEntityReference(
                        "bigquery:my-project.my_dataset.my_table"
                    ),
                )
            ],
            [
                FakeLink(
                    source=FakeEntityReference(upstream_fqn, ["payload", "item_id"]),
                    target=FakeEntityReference(
                        "bigquery:my-project.my_dataset.my_table", ["col_a"]
                    ),
                )
            ],
        )
        downstream = make_entry(schema_field_paths=("col_a",))
        upstream = make_entry(
            fqn=upstream_fqn,
            dataset_name="my-project.my_dataset.upstream_table",
            schema_field_paths=(upstream_v2,),
        )
        extractor.register_schema_field_paths([downstream, upstream])

        result = extractor.get_lineage_for_entry(downstream, SCAN_PAIRS)
        assert result is not None
        _edges, mappings = extractor._extract_lineage_edges_for_entry(
            downstream, result
        )

        assert mappings == {"col_a": [(upstream.datahub_dataset_urn, upstream_v2)]}

    def test_aliased_upstream_column_is_remapped_to_the_resolved_table(
        self, lineage_client: MagicMock
    ) -> None:
        """A ``hive_metastore:`` upstream resolves to a table ingested under
        another FQN; its column must still land on that table's fieldPath."""
        extractor = make_extractor(lineage_client)
        hive_fqn = "hive_metastore:`localhost:9083`.my_database.my_table"
        upstream_v2 = "[version=2.0].[type=struct].[type=string].item_id"
        lineage_client.search_links.side_effect = route_search_links(
            [
                FakeLink(
                    source=FakeEntityReference(hive_fqn),
                    target=FakeEntityReference(
                        "bigquery:my-project.my_dataset.my_table"
                    ),
                )
            ],
            [
                FakeLink(
                    source=FakeEntityReference(hive_fqn, ["item_id"]),
                    target=FakeEntityReference(
                        "bigquery:my-project.my_dataset.my_table", ["col_a"]
                    ),
                )
            ],
        )
        downstream = make_entry(schema_field_paths=("col_a",))
        metastore_table = make_entry(
            fqn=(
                "dataproc_metastore:my-project.us-west1.my-service.my_database.my_table"
            ),
            dataset_name="my-project.us-west1.my-service.my_database.my_table",
            entry_type="dataproc-metastore-table",
            platform="dataproc-metastore",
            schema_field_paths=(upstream_v2,),
        )
        extractor.register_schema_field_paths([downstream, metastore_table])
        extractor.register_dpms_tables([metastore_table])

        result = extractor.get_lineage_for_entry(downstream, SCAN_PAIRS)
        assert result is not None
        _edges, mappings = extractor._extract_lineage_edges_for_entry(
            downstream, result
        )

        assert mappings == {
            "col_a": [(metastore_table.datahub_dataset_urn, upstream_v2)]
        }

    def test_column_only_upstream_is_promoted_to_a_table_edge(
        self, lineage_client: MagicMock
    ) -> None:
        """fineGrainedLineages must never reference a dataset missing from
        ``upstreams``."""
        extractor = make_extractor(lineage_client)
        table_upstream = "bigquery:my-project.my_dataset.upstream_table"
        column_only_upstream = "bigquery:my-project.my_dataset.other_table"
        lineage_client.search_links.side_effect = route_search_links(
            [
                FakeLink(
                    source=FakeEntityReference(table_upstream),
                    target=FakeEntityReference(
                        "bigquery:my-project.my_dataset.my_table"
                    ),
                )
            ],
            [
                FakeLink(
                    source=FakeEntityReference(column_only_upstream, ["col_a"]),
                    target=FakeEntityReference(
                        "bigquery:my-project.my_dataset.my_table", ["col_a"]
                    ),
                )
            ],
        )
        entry = make_entry(schema_field_paths=("col_a",))

        workunits = list(extractor.get_lineage_workunits([entry], SCAN_PAIRS))

        aspect = aspect_of(workunits[0])
        upstream_urns = {upstream.dataset for upstream in aspect.upstreams}
        assert upstream_urns == {
            "urn:li:dataset:(urn:li:dataPlatform:bigquery,"
            "my-project.my_dataset.upstream_table,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:bigquery,"
            "my-project.my_dataset.other_table,PROD)",
        }
        assert (
            aspect.fineGrainedLineages[0]
            .upstreams[0]
            .startswith(
                "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:bigquery,"
                "my-project.my_dataset.other_table,PROD)"
            )
        )

    def test_columns_are_batched_at_the_api_limit(
        self, lineage_client: MagicMock
    ) -> None:
        extractor = make_extractor(lineage_client)
        lineage_client.search_links.side_effect = route_search_links(
            [
                FakeLink(
                    source=FakeEntityReference(
                        "bigquery:my-project.my_dataset.upstream_table"
                    ),
                    target=FakeEntityReference(
                        "bigquery:my-project.my_dataset.my_table"
                    ),
                )
            ],
            [],
        )
        columns = tuple(f"col_{index}" for index in range(45))
        entry = make_entry(schema_field_paths=columns)

        extractor.get_lineage_for_entry(entry, SCAN_PAIRS)

        # 45 columns -> ceil(45 / 20) == 3 column-scoped calls.
        assert extractor.report.num_column_lineage_api_calls == 3
        batch_sizes = [
            len(call.kwargs["request"].targets.entities)
            for call in lineage_client.search_links.call_args_list
            if getattr(call.kwargs["request"], "targets", None)
            and call.kwargs["request"].targets.entities
        ]
        assert batch_sizes == [COLUMN_LINK_BATCH_SIZE, COLUMN_LINK_BATCH_SIZE, 5]

    def test_asset_level_link_echoed_back_is_ignored(
        self, lineage_client: MagicMock
    ) -> None:
        extractor = make_extractor(lineage_client)
        upstream_fqn = "bigquery:my-project.my_dataset.upstream_table"
        lineage_client.search_links.side_effect = route_search_links(
            [
                FakeLink(
                    source=FakeEntityReference(upstream_fqn),
                    target=FakeEntityReference(
                        "bigquery:my-project.my_dataset.my_table"
                    ),
                )
            ],
            [
                # No field on either side: the table-level link echoed back.
                FakeLink(
                    source=FakeEntityReference(upstream_fqn),
                    target=FakeEntityReference(
                        "bigquery:my-project.my_dataset.my_table"
                    ),
                )
            ],
        )
        entry = make_entry(schema_field_paths=("col_a",))

        result = extractor.get_lineage_for_entry(entry, SCAN_PAIRS)

        assert result is not None
        mappings = result["column_mappings"]
        assert mappings == {}

    def test_column_lookup_failure_warns_and_keeps_table_lineage(
        self, lineage_client: MagicMock
    ) -> None:
        extractor = make_extractor(lineage_client)
        upstream_fqn = "bigquery:my-project.my_dataset.upstream_table"

        def _side_effect(request):
            if getattr(request, "targets", None) and request.targets.entities:
                raise RuntimeError("column lookup unavailable")
            return [
                FakeLink(
                    source=FakeEntityReference(upstream_fqn),
                    target=FakeEntityReference(
                        "bigquery:my-project.my_dataset.my_table"
                    ),
                )
            ]

        lineage_client.search_links.side_effect = _side_effect
        entry = make_entry(schema_field_paths=("col_a",))

        result = extractor.get_lineage_for_entry(entry, SCAN_PAIRS)

        assert result is not None
        assert result["upstream"] == [upstream_fqn]
        assert result["column_mappings"] == {}
        assert cast(Mock, extractor.source_report.warning).called


class TestColumnLineageConfig:
    @pytest.mark.parametrize("disabled_flag", ["include_lineage", "include_schema"])
    def test_column_lineage_degrades_instead_of_failing(
        self, disabled_flag: str
    ) -> None:
        config = DataplexConfig(
            project_ids=["my-project"],
            include_column_lineage=True,
            **{disabled_flag: False},
        )
        assert config.include_column_lineage is False

    def test_column_lineage_stays_on_when_dependencies_are_met(self) -> None:
        config = DataplexConfig(
            project_ids=["my-project"],
            include_column_lineage=True,
            include_lineage=True,
            include_schema=True,
        )
        assert config.include_column_lineage is True


def test_register_schema_field_paths_keeps_first_path_on_collision() -> None:
    extractor = make_extractor(MagicMock())
    entry = make_entry(
        schema_field_paths=(
            "[version=2.0].[type=struct].[type=string].id",
            "[version=2.0].[type=struct].[type=string].ID",
        )
    )

    extractor.register_schema_field_paths([entry])

    # Exact match wins for each spelling; the casefold index keeps the first.
    assert (
        extractor._remap_upstream_column(entry.datahub_dataset_urn, "id")
        == "[version=2.0].[type=struct].[type=string].id"
    )
    assert (
        extractor._remap_upstream_column(entry.datahub_dataset_urn, "ID")
        == "[version=2.0].[type=struct].[type=string].ID"
    )
    assert (
        extractor._remap_upstream_column(entry.datahub_dataset_urn, "Id")
        == "[version=2.0].[type=struct].[type=string].id"
    )


def test_unknown_upstream_keeps_the_api_column_name() -> None:
    extractor = make_extractor(MagicMock())
    assert (
        extractor._remap_upstream_column(
            "urn:li:dataset:(urn:li:dataPlatform:bigquery,other.ds.table,PROD)",
            "col_a",
        )
        == "col_a"
    )


def test_link_field_path_joins_segments() -> None:
    assert (
        DataplexLineageExtractor._link_field_path(
            FakeEntityReference("bigquery:my-project.my_dataset.my_table", ["a", "b"])
        )
        == "a.b"
    )
    assert (
        DataplexLineageExtractor._link_field_path(
            FakeEntityReference("bigquery:my-project.my_dataset.my_table")
        )
        is None
    )
    assert DataplexLineageExtractor._link_field_path(None) is None
