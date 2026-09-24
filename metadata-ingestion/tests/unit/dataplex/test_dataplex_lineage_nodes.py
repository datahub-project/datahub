"""Unit tests for lineage-only upstream nodes and sticky lineage merging."""

import datetime
from typing import Any, Dict, Iterable, List, Optional, Set, cast
from unittest.mock import MagicMock, Mock

import pytest

from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.dataplex.dataplex_config import DataplexConfig
from datahub.ingestion.source.dataplex.dataplex_helpers import EntryDataTuple
from datahub.ingestion.source.dataplex.dataplex_lineage import (
    LINEAGE_ONLY_CONTAINER_PROPERTY,
    DataplexLineageExtractor,
    LineageEdge,
    _UpstreamNodeAction,
)
from datahub.ingestion.source.dataplex.dataplex_report import DataplexReport
from datahub.metadata.schema_classes import (
    ContainerPropertiesClass,
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
    StatusClass,
    UpstreamClass,
    UpstreamLineageClass,
)
from tests.unit.dataplex.workunit_assertions import (
    aspect_of,
    entity_urn,
)

GCS_BUCKET_URN = "urn:li:dataset:(urn:li:dataPlatform:gcs,my-bucket,PROD)"
IN_SCOPE_TABLE_URN = (
    "urn:li:dataset:(urn:li:dataPlatform:bigquery,"
    "my-project.my_dataset.upstream_table,PROD)"
)
OUT_OF_SCOPE_TABLE_URN = (
    "urn:li:dataset:(urn:li:dataPlatform:bigquery,"
    "other-project.my_dataset.upstream_table,PROD)"
)
HIVE_TABLE_URN = "urn:li:dataset:(urn:li:dataPlatform:hive,my_database.my_table,PROD)"


class FakeGraph:
    """Minimal stand-in for DataHubGraph's existence / aspect reads."""

    def __init__(
        self,
        existing: Optional[Set[str]] = None,
        aspects: Optional[Dict[str, object]] = None,
    ) -> None:
        self.existing = existing or set()
        self.aspects = aspects or {}
        self.exists_calls: List[str] = []

    def exists(self, urn: str) -> bool:
        self.exists_calls.append(urn)
        return urn in self.existing

    def get_aspect(self, urn: str, aspect_type: type) -> Any:
        return self.aspects.get(f"{urn}|{aspect_type.__name__}")


def make_extractor(
    graph: Optional[Any] = None, **config_overrides: object
) -> DataplexLineageExtractor:
    config = DataplexConfig(
        project_ids=["my-project"],
        entries_locations=["us"],
        lineage_locations=["us-central1"],
        include_lineage=True,
        **config_overrides,
    )
    return DataplexLineageExtractor(
        config=config,
        report=DataplexReport().lineage_report,
        source_report=Mock(),
        lineage_client=MagicMock(),
        graph=graph,
    )


def make_edge(urn: str, *, fqn: Optional[str] = None) -> LineageEdge:
    return LineageEdge(
        upstream_datahub_urn=urn,
        audit_stamp=datetime.datetime.now(datetime.timezone.utc),
        upstream_fqn=fqn,
    )


def make_entry(dataset_name: str) -> EntryDataTuple:
    return EntryDataTuple(
        dataplex_entry_short_name=dataset_name.rsplit(".", 1)[-1],
        dataplex_entry_name=f"projects/p/locations/us/entryGroups/g/entries/{dataset_name}",
        dataplex_location="us",
        dataplex_entry_fqn=f"bigquery:{dataset_name}",
        dataplex_entry_type_short_name="bigquery-table",
        datahub_platform="bigquery",
        datahub_dataset_name=dataset_name,
        datahub_dataset_urn=(
            f"urn:li:dataset:(urn:li:dataPlatform:bigquery,{dataset_name},PROD)"
        ),
    )


def status_workunits(workunits: Iterable[MetadataWorkUnit]) -> List[str]:
    return [
        entity_urn(workunit)
        for workunit in workunits
        if isinstance(aspect_of(workunit), StatusClass)
    ]


class TestNodeAction:
    """Only URNs this run emits skip the graph check; gcs and hive included."""

    def test_urn_this_run_emits_gets_status_only(self) -> None:
        extractor = make_extractor()
        extractor._exported_dataset_urns = {IN_SCOPE_TABLE_URN}
        assert (
            extractor._upstream_node_action(IN_SCOPE_TABLE_URN)
            is _UpstreamNodeAction.STATUS_ONLY
        )

    @pytest.mark.parametrize("urn", [GCS_BUCKET_URN, HIVE_TABLE_URN])
    def test_object_store_and_hive_are_not_exempt_from_the_check(
        self, urn: str
    ) -> None:
        """These namespaces collide with DataHub's own connectors, so they
        cannot be assumed to belong to this one."""
        extractor = make_extractor()
        assert extractor._upstream_node_action(urn) is _UpstreamNodeAction.SKIP

    def test_in_scope_project_alone_is_not_enough(self) -> None:
        """A configured project's table that this run does not ingest is not ours."""
        extractor = make_extractor()
        assert (
            extractor._upstream_node_action(IN_SCOPE_TABLE_URN)
            is _UpstreamNodeAction.SKIP
        )

    def test_without_a_graph_nothing_outside_the_run_is_written(self) -> None:
        extractor = make_extractor()
        assert (
            extractor._upstream_node_action(OUT_OF_SCOPE_TABLE_URN)
            is _UpstreamNodeAction.SKIP
        )

    def test_absent_entity_is_rendered_in_full(self) -> None:
        extractor = make_extractor(graph=FakeGraph())
        assert (
            extractor._upstream_node_action(OUT_OF_SCOPE_TABLE_URN)
            is _UpstreamNodeAction.FULL
        )

    def test_live_entity_keeps_its_own_properties(self) -> None:
        """Status is re-emitted so stale-removal does not tombstone the node on
        the next run, but the properties belong to whoever wrote them."""
        graph = FakeGraph(
            existing={OUT_OF_SCOPE_TABLE_URN},
            aspects={
                f"{OUT_OF_SCOPE_TABLE_URN}|StatusClass": StatusClass(removed=False)
            },
        )
        extractor = make_extractor(graph=graph)
        assert (
            extractor._upstream_node_action(OUT_OF_SCOPE_TABLE_URN)
            is _UpstreamNodeAction.STATUS_ONLY
        )

    def test_soft_deleted_entity_is_never_revived(self) -> None:
        graph = FakeGraph(
            existing={OUT_OF_SCOPE_TABLE_URN},
            aspects={
                f"{OUT_OF_SCOPE_TABLE_URN}|StatusClass": StatusClass(removed=True)
            },
        )
        extractor = make_extractor(graph=graph)
        assert (
            extractor._upstream_node_action(OUT_OF_SCOPE_TABLE_URN)
            is _UpstreamNodeAction.SKIP
        )

    def test_graph_errors_fail_closed(self) -> None:
        graph = MagicMock()
        graph.exists.side_effect = RuntimeError("graph unavailable")
        extractor = make_extractor(graph=graph)
        assert (
            extractor._upstream_node_action(OUT_OF_SCOPE_TABLE_URN)
            is _UpstreamNodeAction.SKIP
        )


class TestNodeEmission:
    def test_disabled_by_default(self) -> None:
        extractor = make_extractor()
        entry = make_entry("my-project.my_dataset.my_table")
        cast(MagicMock, extractor.lineage_client).search_links.return_value = []

        workunits = extractor._process_entry_lineage(
            entry, [("my-project", "us-central1")]
        )

        assert status_workunits(workunits) == []

    def test_absent_gcs_upstream_is_rendered_like_an_entry(self) -> None:
        extractor = make_extractor(
            graph=FakeGraph(), include_lineage_only_upstreams=True
        )
        edge = make_edge(GCS_BUCKET_URN, fqn="gcs:my-bucket")

        workunits = list(extractor._gen_upstream_node_workunits({edge}))

        assert status_workunits(workunits) == [GCS_BUCKET_URN]
        assert all(entity_urn(workunit) == GCS_BUCKET_URN for workunit in workunits)
        assert extractor.report.num_upstream_nodes_emitted == 1
        aspect_names = {type(aspect_of(workunit)).__name__ for workunit in workunits}
        assert "DatasetPropertiesClass" in aspect_names
        assert "SubTypesClass" in aspect_names

    def test_exported_upstream_gets_status_only(self) -> None:
        extractor = make_extractor(include_lineage_only_upstreams=True)
        extractor._exported_dataset_urns = {IN_SCOPE_TABLE_URN}
        edge = make_edge(
            IN_SCOPE_TABLE_URN, fqn="bigquery:my-project.my_dataset.upstream_table"
        )

        workunits = list(extractor._gen_upstream_node_workunits({edge}))

        assert len(workunits) == 1
        assert isinstance(aspect_of(workunits[0]), StatusClass)

    def test_urn_is_evaluated_once_per_run(self) -> None:
        extractor = make_extractor(
            graph=FakeGraph(), include_lineage_only_upstreams=True
        )
        edge = make_edge(GCS_BUCKET_URN, fqn="gcs:my-bucket")

        first = list(extractor._gen_upstream_node_workunits({edge}))
        second = list(extractor._gen_upstream_node_workunits({edge}))

        assert first
        assert second == []

    def test_unsafe_node_is_counted(self) -> None:
        graph = FakeGraph(
            existing={OUT_OF_SCOPE_TABLE_URN},
            aspects={
                f"{OUT_OF_SCOPE_TABLE_URN}|StatusClass": StatusClass(removed=True)
            },
        )
        extractor = make_extractor(graph=graph, include_lineage_only_upstreams=True)
        edge = make_edge(OUT_OF_SCOPE_TABLE_URN)

        assert list(extractor._gen_upstream_node_workunits({edge})) == []
        assert extractor.report.num_upstream_nodes_skipped_unsafe == 1

    def test_sticky_lineage_exempts_nodes_from_stale_removal(self) -> None:
        extractor = make_extractor(
            graph=FakeGraph(),
            include_lineage_only_upstreams=True,
            remove_stale_lineage=False,
        )
        edge = make_edge(GCS_BUCKET_URN, fqn="gcs:my-bucket")

        workunits = list(extractor._gen_upstream_node_workunits({edge}))

        assert all(not workunit.is_primary_source for workunit in workunits)


class TestContainerEmission:
    def test_absent_containers_are_created_with_the_marker(self) -> None:
        graph = FakeGraph()
        extractor = make_extractor(graph=graph, include_lineage_only_upstreams=True)
        edge = make_edge(
            OUT_OF_SCOPE_TABLE_URN,
            fqn="bigquery:other-project.my_dataset.upstream_table",
        )

        workunits = list(extractor._gen_upstream_node_workunits({edge}))

        container_properties = [
            aspect_of(workunit)
            for workunit in workunits
            if isinstance(aspect_of(workunit), ContainerPropertiesClass)
        ]
        # The dataset container and the project container.
        assert len(container_properties) == 2
        assert all(
            properties.customProperties[LINEAGE_ONLY_CONTAINER_PROPERTY] == "true"
            for properties in container_properties
        )
        assert extractor.report.num_upstream_containers_emitted == 2

    def test_foreign_container_is_left_alone(self) -> None:
        extractor = make_extractor(
            graph=FakeGraph(), include_lineage_only_upstreams=True
        )
        container_urn = "urn:li:container:abc"
        graph = cast(FakeGraph, extractor.graph)
        graph.existing.add(container_urn)
        graph.aspects[f"{container_urn}|ContainerPropertiesClass"] = (
            ContainerPropertiesClass(name="my_dataset", customProperties={})
        )

        assert not extractor._container_write_allowed(container_urn)

    def test_our_own_container_is_refreshed(self) -> None:
        extractor = make_extractor(
            graph=FakeGraph(), include_lineage_only_upstreams=True
        )
        container_urn = "urn:li:container:abc"
        graph = cast(FakeGraph, extractor.graph)
        graph.existing.add(container_urn)
        graph.aspects[f"{container_urn}|ContainerPropertiesClass"] = (
            ContainerPropertiesClass(
                name="my_dataset",
                customProperties={LINEAGE_ONLY_CONTAINER_PROPERTY: "true"},
            )
        )

        assert extractor._container_write_allowed(container_urn)

    def test_soft_deleted_container_is_never_touched(self) -> None:
        extractor = make_extractor(
            graph=FakeGraph(), include_lineage_only_upstreams=True
        )
        container_urn = "urn:li:container:abc"
        graph = cast(FakeGraph, extractor.graph)
        graph.existing.add(container_urn)
        graph.aspects[f"{container_urn}|StatusClass"] = StatusClass(removed=True)

        assert not extractor._container_write_allowed(container_urn)

    def test_nothing_is_written_without_a_graph(self) -> None:
        extractor = make_extractor(include_lineage_only_upstreams=True)
        edge = make_edge(GCS_BUCKET_URN, fqn="gcs:my-bucket")

        assert list(extractor._gen_upstream_node_workunits({edge})) == []


class TestStickyLineage:
    @staticmethod
    def _fresh() -> UpstreamLineageClass:
        return UpstreamLineageClass(
            upstreams=[UpstreamClass(dataset=IN_SCOPE_TABLE_URN, type="TRANSFORMED")]
        )

    def test_persisted_upstreams_are_carried_over(self) -> None:
        downstream_urn = (
            "urn:li:dataset:(urn:li:dataPlatform:bigquery,"
            "my-project.my_dataset.my_table,PROD)"
        )
        graph = FakeGraph(
            aspects={
                f"{downstream_urn}|UpstreamLineageClass": UpstreamLineageClass(
                    upstreams=[
                        UpstreamClass(
                            dataset=OUT_OF_SCOPE_TABLE_URN, type="TRANSFORMED"
                        )
                    ]
                )
            }
        )
        extractor = make_extractor(graph=graph, remove_stale_lineage=False)

        merged = extractor._merge_with_persisted_lineage(downstream_urn, self._fresh())

        assert {upstream.dataset for upstream in merged.upstreams} == {
            IN_SCOPE_TABLE_URN,
            OUT_OF_SCOPE_TABLE_URN,
        }
        assert extractor.report.num_lineage_upstreams_preserved == 1

    def test_fresh_wins_over_its_persisted_copy(self) -> None:
        downstream_urn = "urn:li:dataset:(urn:li:dataPlatform:bigquery,d,PROD)"
        graph = FakeGraph(
            aspects={
                f"{downstream_urn}|UpstreamLineageClass": UpstreamLineageClass(
                    upstreams=[UpstreamClass(dataset=IN_SCOPE_TABLE_URN, type="COPY")]
                )
            }
        )
        extractor = make_extractor(graph=graph, remove_stale_lineage=False)

        merged = extractor._merge_with_persisted_lineage(downstream_urn, self._fresh())

        assert len(merged.upstreams) == 1
        assert merged.upstreams[0].type == "TRANSFORMED"

    def test_fine_grained_entries_merge_per_downstream_field(self) -> None:
        downstream_urn = "urn:li:dataset:(urn:li:dataPlatform:bigquery,d,PROD)"
        persisted_field = f"urn:li:schemaField:({downstream_urn},col_b)"
        fresh_field = f"urn:li:schemaField:({downstream_urn},col_a)"
        graph = FakeGraph(
            aspects={
                f"{downstream_urn}|UpstreamLineageClass": UpstreamLineageClass(
                    upstreams=[
                        UpstreamClass(dataset=IN_SCOPE_TABLE_URN, type="TRANSFORMED")
                    ],
                    fineGrainedLineages=[
                        FineGrainedLineageClass(
                            upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                            downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                            downstreams=[persisted_field],
                        )
                    ],
                )
            }
        )
        extractor = make_extractor(graph=graph, remove_stale_lineage=False)
        fresh = UpstreamLineageClass(
            upstreams=[UpstreamClass(dataset=IN_SCOPE_TABLE_URN, type="TRANSFORMED")],
            fineGrainedLineages=[
                FineGrainedLineageClass(
                    upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                    downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                    downstreams=[fresh_field],
                )
            ],
        )

        merged = extractor._merge_with_persisted_lineage(downstream_urn, fresh)

        assert merged.fineGrainedLineages is not None
        assert len(merged.fineGrainedLineages) == 2
        assert extractor.report.num_fine_grained_lineages_preserved == 1

    def test_without_a_graph_the_fresh_aspect_is_emitted_unchanged(self) -> None:
        extractor = make_extractor(remove_stale_lineage=False)
        fresh = self._fresh()
        assert (
            extractor._merge_with_persisted_lineage("urn:li:dataset:x", fresh) is fresh
        )

    def test_read_failure_falls_back_to_the_fresh_aspect(self) -> None:
        graph = MagicMock()
        graph.get_aspect.side_effect = RuntimeError("read failed")
        extractor = make_extractor(graph=graph, remove_stale_lineage=False)
        fresh = self._fresh()
        assert (
            extractor._merge_with_persisted_lineage("urn:li:dataset:x", fresh) is fresh
        )

    def test_merging_is_skipped_when_stale_removal_is_on(self) -> None:
        graph = MagicMock()
        extractor = make_extractor(graph=graph, remove_stale_lineage=True)

        list(extractor._gen_lineage("d", "urn:li:dataset:x", self._fresh()))

        graph.get_aspect.assert_not_called()


@pytest.mark.parametrize(
    "urn,fqn,expected_platform,expected_name",
    [
        (GCS_BUCKET_URN, "gcs:my-bucket/raw/events.csv", "gcs", "my-bucket"),
        (
            OUT_OF_SCOPE_TABLE_URN,
            "bigquery:other-project.my_dataset.upstream_table",
            "bigquery",
            "other-project.my_dataset.upstream_table",
        ),
    ],
)
def test_node_spec_describes_the_upstream(
    urn: str, fqn: str, expected_platform: str, expected_name: str
) -> None:
    extractor = make_extractor()
    edge = make_edge(urn, fqn=fqn)

    spec = extractor._upstream_node_spec(edge)

    assert spec is not None
    assert spec.platform == expected_platform
    assert spec.dataset_name == expected_name


def test_hive_node_spec_matches_the_hive_connector_naming() -> None:
    extractor = make_extractor()
    edge = make_edge(
        HIVE_TABLE_URN, fqn="hive_metastore:`localhost:9083`.my_database.my_table"
    )

    spec = extractor._upstream_node_spec(edge)

    assert spec is not None
    assert spec.platform == "hive"
    assert spec.dataset_name == "my_database.my_table"
    assert spec.display_name == "my_table"
