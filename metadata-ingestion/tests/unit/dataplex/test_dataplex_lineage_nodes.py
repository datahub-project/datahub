"""Unit tests for lineage-only upstream nodes."""

import dataclasses
import json
from typing import Any, Dict, List, Optional, Set
from unittest.mock import MagicMock, Mock, call

import pytest

import datahub.emitter.mce_builder as mce_builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.run.pipeline_config import AutoResolveLineageUrnsConfig
from datahub.ingestion.source.dataplex.dataplex_config import DataplexConfig
from datahub.ingestion.source.dataplex.dataplex_lineage import (
    FAILED_PARENTS_KEY,
    DataplexLineageExtractor,
)
from datahub.ingestion.source.dataplex.dataplex_pubsub import PubSubSubscriptionResolver
from datahub.ingestion.source.dataplex.dataplex_report import DataplexReport
from datahub.metadata.schema_classes import (
    ChangeTypeClass,
    MetadataChangeProposalClass,
    UpstreamLineageClass,
)
from datahub.metadata.urns import DatasetUrn
from tests.unit.dataplex.test_dataplex_upstream_resolution import (
    FakeEntityReference,
    FakeLink,
    make_dpms_table_entry,
)
from tests.unit.dataplex.workunit_assertions import aspect_of

SCAN_PAIRS = [("my-project", "us-central1")]
GCS_BUCKET_URN = "urn:li:dataset:(urn:li:dataPlatform:gcs,my-bucket,PROD)"
OUT_OF_SCOPE_TABLE_URN = (
    "urn:li:dataset:(urn:li:dataPlatform:bigquery,"
    "other-project.my_dataset.upstream_table,PROD)"
)
HIVE_TABLE_URN = "urn:li:dataset:(urn:li:dataPlatform:hive,my_db.my_table,PROD)"
DOWNSTREAM_URN = make_dpms_table_entry().datahub_dataset_urn


class FakeGraph:
    """Stand-in for DataHubGraph.exists."""

    def __init__(self, existing: Optional[Set[str]] = None) -> None:
        self.existing = existing or set()

    def exists(self, urn: str) -> bool:
        return urn in self.existing


def make_extractor(
    graph: Optional[Any] = None,
    lineage_client: Optional[MagicMock] = None,
    lineage_urn_casing: Optional[AutoResolveLineageUrnsConfig] = None,
    **config_overrides: object,
) -> DataplexLineageExtractor:
    config_overrides.setdefault("include_lineage_only_upstreams", True)
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
        lineage_client=lineage_client or MagicMock(),
        graph=graph,
        lineage_urn_casing=lineage_urn_casing,
    )


def lineage_data(*upstream_fqns: str) -> Dict[str, Any]:
    return {
        "upstream": list(upstream_fqns),
        "downstream": [],
        FAILED_PARENTS_KEY: [],
        "column_mappings": {},
    }


def stub_workunits(
    extractor: DataplexLineageExtractor, data: Dict[str, Any]
) -> List[MetadataWorkUnit]:
    """The entry's workunits other than its own upstreamLineage."""
    extractor.get_lineage_for_entry = Mock(return_value=data)  # type: ignore[method-assign]
    workunits = extractor._process_entry_lineage(make_dpms_table_entry(), SCAN_PAIRS)
    return [
        workunit
        for workunit in workunits
        if workunit.get_aspect_of_type(UpstreamLineageClass) is None
    ]


def by_aspect(workunits: List[MetadataWorkUnit]) -> Dict[str, MetadataWorkUnit]:
    named: Dict[str, MetadataWorkUnit] = {}
    for workunit in workunits:
        metadata = workunit.metadata
        assert isinstance(
            metadata, (MetadataChangeProposalWrapper, MetadataChangeProposalClass)
        )
        assert metadata.aspectName is not None
        named[metadata.aspectName] = workunit
    return named


def patched_display_name(workunit: MetadataWorkUnit) -> str:
    metadata = workunit.metadata
    assert isinstance(metadata, MetadataChangeProposalClass)
    assert metadata.changeType == ChangeTypeClass.PATCH
    assert metadata.aspect is not None
    (operation,) = json.loads(metadata.aspect.value)
    assert operation["path"] == "/name"
    return operation["value"]


class TestNodeEmission:
    def test_absent_upstream_gets_status_display_name_and_subtype(self) -> None:
        extractor = make_extractor(graph=FakeGraph())

        workunits = list(extractor._gen_upstream_node_workunits([GCS_BUCKET_URN]))

        assert {workunit.get_urn() for workunit in workunits} == {GCS_BUCKET_URN}
        aspects = by_aspect(workunits)
        assert set(aspects) == {"status", "datasetProperties", "subTypes"}
        assert not aspect_of(aspects["status"]).removed
        assert patched_display_name(aspects["datasetProperties"]) == "my-bucket"
        assert aspect_of(aspects["subTypes"]).typeNames == ["GCS bucket"]
        assert extractor.report.num_upstream_nodes_emitted == 1

    def test_nodes_are_never_primary_source(self) -> None:
        """Stale-entity removal only tracks primary-source workunits, so these
        can never be soft-deleted by this connector."""
        extractor = make_extractor(graph=FakeGraph())

        workunits = list(
            extractor._gen_upstream_node_workunits(
                [GCS_BUCKET_URN, OUT_OF_SCOPE_TABLE_URN]
            )
        )

        assert workunits
        assert not any(workunit.is_primary_source for workunit in workunits)

    def test_existing_upstream_is_left_alone(self) -> None:
        """An entity another connector owns gets no write at all."""
        extractor = make_extractor(graph=FakeGraph(existing={OUT_OF_SCOPE_TABLE_URN}))

        assert (
            list(extractor._gen_upstream_node_workunits([OUT_OF_SCOPE_TABLE_URN])) == []
        )
        assert extractor.report.num_upstream_nodes_skipped == 1

    def test_an_entry_upstream_of_another_entry_gets_no_node(self) -> None:
        upstream_entry = make_dpms_table_entry(table_id="upstream_table")
        downstream_entry = make_dpms_table_entry()
        upstream_fqns = {
            downstream_entry.dataplex_entry_fqn: [
                upstream_entry.dataplex_entry_fqn,
                "gcs:my-bucket",
            ]
        }
        graph = MagicMock()
        graph.exists.return_value = False
        extractor = make_extractor(graph=graph)
        extractor.get_lineage_for_entry = Mock(  # type: ignore[method-assign]
            side_effect=lambda entry, **_: lineage_data(
                *upstream_fqns.get(entry.dataplex_entry_fqn, [])
            )
        )

        workunits = list(
            extractor.get_lineage_workunits(
                [upstream_entry, downstream_entry], SCAN_PAIRS
            )
        )

        assert {workunit.get_urn() for workunit in workunits} == {
            DOWNSTREAM_URN,
            GCS_BUCKET_URN,
        }
        assert graph.exists.call_args_list == [call(GCS_BUCKET_URN)]
        assert extractor.report.num_upstream_nodes_emitted == 1

    def test_without_a_graph_nothing_is_written_and_a_warning_explains_why(
        self,
    ) -> None:
        extractor = make_extractor()

        assert list(extractor._gen_upstream_node_workunits([GCS_BUCKET_URN])) == []
        warning = extractor.source_report.warning
        assert isinstance(warning, Mock)
        assert warning.call_args.kwargs["title"] == (
            "Dataplex lineage-only upstreams disabled"
        )

    def test_graph_errors_fail_closed_and_warn_once(self) -> None:
        graph = MagicMock()
        graph.exists.side_effect = RuntimeError("graph unavailable")
        extractor = make_extractor(graph=graph)

        workunits = list(
            extractor._gen_upstream_node_workunits(
                [GCS_BUCKET_URN, OUT_OF_SCOPE_TABLE_URN]
            )
        )

        assert workunits == []
        assert extractor.report.num_upstream_node_checks_failed == 2
        assert extractor.report.num_upstream_nodes_skipped == 0
        warning = extractor.source_report.warning
        assert isinstance(warning, Mock)
        assert [c.kwargs["title"] for c in warning.call_args_list] == [
            "Dataplex lineage-only upstream check failed"
        ]

    def test_each_urn_is_checked_once_per_run(self) -> None:
        graph = MagicMock()
        graph.exists.return_value = False
        extractor = make_extractor(graph=graph)

        first = list(extractor._gen_upstream_node_workunits([GCS_BUCKET_URN]))
        second = list(extractor._gen_upstream_node_workunits([GCS_BUCKET_URN]))

        assert first
        assert second == []
        assert graph.exists.call_args_list == [call(GCS_BUCKET_URN)]

    def test_off_by_default(self) -> None:
        assert not DataplexConfig(project_ids=["p"]).include_lineage_only_upstreams

    def test_disabled_writes_nothing(self) -> None:
        graph = MagicMock()
        extractor = make_extractor(graph=graph, include_lineage_only_upstreams=False)

        assert stub_workunits(extractor, lineage_data("gcs:my-bucket")) == []
        graph.exists.assert_not_called()


class TestCasingResolution:
    """No stub where auto_resolve_lineage_urns may later move the edge's URN."""

    def test_reconciled_platform_gets_no_node(self) -> None:
        extractor = make_extractor(
            graph=FakeGraph(),
            lineage_urn_casing=AutoResolveLineageUrnsConfig.model_validate(
                {"enabled": True, "upstream_platforms": [{"platform": "hive"}]}
            ),
        )

        workunits = list(
            extractor._gen_upstream_node_workunits([HIVE_TABLE_URN, GCS_BUCKET_URN])
        )

        assert {workunit.get_urn() for workunit in workunits} == {GCS_BUCKET_URN}
        assert extractor.report.num_upstream_nodes_casing_skipped == 1

    def test_all_platforms_get_no_node_and_one_warning(self) -> None:
        extractor = make_extractor(
            graph=FakeGraph(),
            lineage_urn_casing=AutoResolveLineageUrnsConfig(
                enabled=True, resolve_all_platforms=True
            ),
        )

        workunits = list(
            extractor._gen_upstream_node_workunits([HIVE_TABLE_URN, GCS_BUCKET_URN])
        )

        assert workunits == []
        assert extractor.report.num_upstream_nodes_casing_skipped == 2
        warning = extractor.source_report.warning
        assert isinstance(warning, Mock)
        assert [c.kwargs["title"] for c in warning.call_args_list] == [
            "Dataplex lineage-only upstream left to casing resolution"
        ]

    def test_disabled_flag_changes_nothing(self) -> None:
        extractor = make_extractor(
            graph=FakeGraph(),
            lineage_urn_casing=AutoResolveLineageUrnsConfig.model_validate(
                {"upstream_platforms": [{"platform": "hive"}]}
            ),
        )

        workunits = list(extractor._gen_upstream_node_workunits([HIVE_TABLE_URN]))

        assert {workunit.get_urn() for workunit in workunits} == {HIVE_TABLE_URN}


class TestNodeIdentity:
    """Every stub aspect lands on the URN the edge points at."""

    def test_dotted_storage_bucket_keeps_its_name(self) -> None:
        extractor = make_extractor(graph=FakeGraph(), include_storage_lineage=True)
        extractor.get_lineage_for_entry = Mock(return_value=lineage_data())  # type: ignore[method-assign]
        bucket_urn = "urn:li:dataset:(urn:li:dataPlatform:gcs,my.example.com,PROD)"

        workunits = extractor._process_entry_lineage(
            make_dpms_table_entry(storage_gcs_bucket="my.example.com"), SCAN_PAIRS
        )

        stubs = [w for w in workunits if w.get_urn() != DOWNSTREAM_URN]
        assert {workunit.get_urn() for workunit in stubs} == {bucket_urn}
        assert (
            patched_display_name(by_aspect(stubs)["datasetProperties"])
            == "my.example.com"
        )

    def test_lowercased_urns_get_no_second_entity(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(mce_builder, "DATASET_URN_TO_LOWER", True)
        extractor = make_extractor(graph=FakeGraph())

        workunits = stub_workunits(
            extractor, lineage_data("hive_metastore:thrift-host.My_Db.My_Table")
        )

        assert {workunit.get_urn() for workunit in workunits} == {
            "urn:li:dataset:(urn:li:dataPlatform:hive,my_db.my_table,PROD)"
        }

    def test_metastore_fallback_upstream_gets_a_node(self) -> None:
        extractor = make_extractor(
            graph=FakeGraph(),
            dpms_hive_metastore_service="other-project.us-west1.other-service",
        )

        workunits = stub_workunits(
            extractor, lineage_data("hive_metastore:thrift-host.other_db.clicks")
        )

        assert {workunit.get_urn() for workunit in workunits} == {
            "urn:li:dataset:(urn:li:dataPlatform:dataproc-metastore,"
            "other-project.us-west1.other-service.other_db.clicks,PROD)"
        }
        aspects = by_aspect(workunits)
        assert patched_display_name(aspects["datasetProperties"]) == "clicks"
        assert aspect_of(aspects["subTypes"]).typeNames == ["Table"]
        assert extractor.report.num_upstream_nodes_emitted == 1

    def test_subscription_upstream_is_described_as_its_topic(self) -> None:
        extractor = make_extractor(graph=FakeGraph(), resolve_pubsub_subscriptions=True)
        subscriber = MagicMock()
        subscriber.get_subscription.return_value = MagicMock(
            topic="projects/other-project/topics/my-topic"
        )
        extractor._pubsub_resolver = PubSubSubscriptionResolver(
            report=extractor.report,
            source_report=extractor.source_report,
            subscriber_client=subscriber,
        )

        workunits = stub_workunits(
            extractor, lineage_data("pubsub:subscription:other-project.my-sub")
        )

        assert {workunit.get_urn() for workunit in workunits} == {
            "urn:li:dataset:(urn:li:dataPlatform:pubsub,other-project.my-topic,PROD)"
        }
        aspects = by_aspect(workunits)
        assert patched_display_name(aspects["datasetProperties"]) == "my-topic"
        assert aspect_of(aspects["subTypes"]).typeNames == ["Topic"]


@pytest.mark.parametrize(
    "urn,expected_display_name,expected_subtype",
    [
        (
            "urn:li:dataset:(urn:li:dataPlatform:gcs,my.example.com,PROD)",
            "my.example.com",
            "GCS bucket",
        ),
        (
            "urn:li:dataset:(urn:li:dataPlatform:hive,my_database.my_table,PROD)",
            "my_table",
            "Table",
        ),
        (
            "urn:li:dataset:(urn:li:dataPlatform:pubsub,my-project.my.topic,PROD)",
            "my.topic",
            "Topic",
        ),
        (
            "urn:li:dataset:(urn:li:dataPlatform:pubsub,"
            "example.com:my-project.my-topic,PROD)",
            "my-topic",
            "Topic",
        ),
        # A BigQuery table and view, or a Spanner table and graph, share one shape.
        (OUT_OF_SCOPE_TABLE_URN, "upstream_table", None),
        (
            "urn:li:dataset:(urn:li:dataPlatform:spanner,"
            "my-project.regional-us-central1.my-instance.my-db.orders,PROD)",
            "orders",
            None,
        ),
        ("urn:li:dataset:(urn:li:dataPlatform:other,a.b,PROD)", "a.b", None),
    ],
)
def test_node_spec_describes_the_upstream(
    urn: str, expected_display_name: str, expected_subtype: Optional[str]
) -> None:
    spec = DataplexLineageExtractor._upstream_node_spec(DatasetUrn.from_string(urn))

    assert spec.display_name == expected_display_name
    assert spec.subtype == expected_subtype


def test_bigquery_stub_writes_no_subtype() -> None:
    extractor = make_extractor(graph=FakeGraph())

    workunits = list(extractor._gen_upstream_node_workunits([OUT_OF_SCOPE_TABLE_URN]))

    assert set(by_aspect(workunits)) == {"status", "datasetProperties"}


class TestWithheldLineage:
    """No stubs for an upstreamLineage aspect that is not written."""

    def test_failed_column_batch_writes_no_stubs(self) -> None:
        upstream_fqn = "bigquery:other-project.my_dataset.upstream_table"
        downstream_fqn = make_dpms_table_entry().dataplex_entry_fqn
        lineage_client = MagicMock()
        lineage_client.search_links.side_effect = [
            [
                FakeLink(
                    FakeEntityReference(upstream_fqn),
                    FakeEntityReference(downstream_fqn),
                )
            ],
            RuntimeError("column lookup unavailable"),
        ]
        graph = MagicMock()
        extractor = make_extractor(
            graph=graph, lineage_client=lineage_client, include_column_lineage=True
        )
        entry = dataclasses.replace(
            make_dpms_table_entry(), schema_field_paths=("col_a",)
        )

        assert extractor._process_entry_lineage(entry, SCAN_PAIRS) == []
        graph.exists.assert_not_called()

    def test_withheld_storage_edge_writes_no_stub(self) -> None:
        graph = MagicMock()
        extractor = make_extractor(graph=graph, include_storage_lineage=True)
        data = lineage_data()
        data[FAILED_PARENTS_KEY] = ["projects/my-project/locations/us-east1"]
        extractor.get_lineage_for_entry = Mock(return_value=data)  # type: ignore[method-assign]

        workunits = extractor._process_entry_lineage(
            make_dpms_table_entry(storage_gcs_bucket="my-bucket"), SCAN_PAIRS
        )

        assert workunits == []
        graph.exists.assert_not_called()
