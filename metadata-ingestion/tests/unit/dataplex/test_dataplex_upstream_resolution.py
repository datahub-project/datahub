"""Unit tests for lineage upstreams that are not Dataplex entries."""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, cast
from unittest.mock import MagicMock, Mock, patch

import pytest
from google.cloud import dataplex_v1
from pydantic import ValidationError

from datahub.ingestion.source.dataplex.dataplex_config import DataplexConfig
from datahub.ingestion.source.dataplex.dataplex_helpers import EntryDataTuple
from datahub.ingestion.source.dataplex.dataplex_ids import (
    HiveTableRef,
    PubSubSubscriptionRef,
    parse_gcs_bucket_fqn,
    parse_hive_metastore_fqn,
    parse_pubsub_subscription_fqn,
)
from datahub.ingestion.source.dataplex.dataplex_lineage import DataplexLineageExtractor
from datahub.ingestion.source.dataplex.dataplex_pubsub import PubSubSubscriptionResolver
from datahub.ingestion.source.dataplex.dataplex_report import DataplexReport
from datahub.ingestion.source.dataplex.dataplex_schema import (
    extract_gcs_storage_bucket_from_entry_aspects,
)
from datahub.metadata.schema_classes import UpstreamLineageClass

SCAN_PAIRS = [("my-project", "us-central1")]

DPMS_TABLE_URN = (
    "urn:li:dataset:(urn:li:dataPlatform:dataproc-metastore,"
    "my-project.us-west1.my-service.my_database.my_table,PROD)"
)
HIVE_TABLE_URN = "urn:li:dataset:(urn:li:dataPlatform:hive,my_database.my_table,PROD)"


@dataclass
class FakeEntityReference:
    fully_qualified_name: str = ""
    field: List[str] = field(default_factory=list)


@dataclass
class FakeLink:
    source: FakeEntityReference
    target: FakeEntityReference
    name: str = ""


def make_extractor(
    lineage_client: Optional[MagicMock] = None, **config_overrides: object
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
        lineage_client=lineage_client or MagicMock(),
    )


def make_dpms_table_entry(
    *,
    database_id: str = "my_database",
    table_id: str = "my_table",
    service_id: str = "my-service",
    storage_gcs_bucket: Optional[str] = None,
) -> EntryDataTuple:
    dataset_name = f"my-project.us-west1.{service_id}.{database_id}.{table_id}"
    return EntryDataTuple(
        dataplex_entry_short_name=table_id,
        dataplex_entry_name=f"projects/p/locations/us/entryGroups/g/entries/{table_id}",
        dataplex_location="us",
        dataplex_entry_fqn=f"dataproc_metastore:{dataset_name}",
        dataplex_entry_type_short_name="dataproc-metastore-table",
        datahub_platform="dataproc-metastore",
        datahub_dataset_name=dataset_name,
        datahub_dataset_urn=(
            "urn:li:dataset:(urn:li:dataPlatform:dataproc-metastore,"
            f"{dataset_name},PROD)"
        ),
        storage_gcs_bucket=storage_gcs_bucket,
    )


def make_same_table_in_two_services() -> List[EntryDataTuple]:
    return [
        make_dpms_table_entry(service_id="service-a"),
        make_dpms_table_entry(service_id="service-b"),
    ]


class TestFqnParsers:
    @pytest.mark.parametrize(
        "fqn,expected",
        [
            ("gcs:my-bucket", "my-bucket"),
            # Paths are dropped: a bucket is the granularity every producer
            # agrees on.
            ("gcs:my-bucket.raw/events", "my-bucket"),
            ("gcs:my-bucket.`raw/events.csv`", "my-bucket"),
            ("gcs:`my.bucket`.`raw/events.csv`", "my.bucket"),
            ("gcs:my-bucket/raw/events.csv", "my-bucket"),
            ("bigquery:my-project.my_dataset.my_table", None),
        ],
    )
    def test_parse_gcs_bucket_fqn(self, fqn: str, expected: Optional[str]) -> None:
        assert parse_gcs_bucket_fqn(fqn) == expected

    @pytest.mark.parametrize(
        "fqn,expected",
        [
            (
                "hive_metastore:`localhost:9083`.my_database.my_table",
                HiveTableRef("my_database", "my_table"),
            ),
            (
                "hive_metastore:thrifthost.my_database.my_table",
                HiveTableRef("my_database", "my_table"),
            ),
            (
                "hive_metastore:`localhost:9083`.`my database`.`my ``odd`` table`",
                HiveTableRef("my database", "my `odd` table"),
            ),
            ("hive_metastore:only.two", None),
            ("bigquery:my-project.my_dataset.my_table", None),
        ],
    )
    def test_parse_hive_metastore_fqn(
        self, fqn: str, expected: Optional[HiveTableRef]
    ) -> None:
        assert parse_hive_metastore_fqn(fqn) == expected

    @pytest.mark.parametrize(
        "fqn,expected",
        [
            (
                "pubsub:subscription:my-project.my-subscription",
                PubSubSubscriptionRef("my-project", "my-subscription"),
            ),
            (
                "pubsub:subscription:my-project.`orders.v2-sub`",
                PubSubSubscriptionRef("my-project", "orders.v2-sub"),
            ),
            (
                "pubsub:subscription:my-project.orders.v2-sub",
                PubSubSubscriptionRef("my-project", "orders.v2-sub"),
            ),
            (
                "pubsub:subscription:`example.com:my-project`.my-subscription",
                PubSubSubscriptionRef("example.com:my-project", "my-subscription"),
            ),
            ("pubsub:subscription:example.com:my-project.my-subscription", None),
            ("pubsub:topic:my-project.my-topic", None),
        ],
    )
    def test_parse_pubsub_subscription_fqn(
        self, fqn: str, expected: Optional[PubSubSubscriptionRef]
    ) -> None:
        assert parse_pubsub_subscription_fqn(fqn) == expected


class TestGcsUpstreams:
    def test_gcs_upstream_resolves_to_a_bucket_urn(self) -> None:
        extractor = make_extractor()
        resolved = extractor._resolve_upstream_fqn("gcs:my-bucket/raw/events.csv", {})
        assert resolved == "urn:li:dataset:(urn:li:dataPlatform:gcs,my-bucket,PROD)"


class TestHiveMetastoreResolution:
    def test_exact_match_against_this_runs_tables(self) -> None:
        extractor = make_extractor()
        extractor.register_dpms_tables([make_dpms_table_entry()])

        resolved = extractor._resolve_hive_metastore_fqn(
            "hive_metastore:`localhost:9083`.my_database.my_table"
        )

        assert resolved == DPMS_TABLE_URN
        assert extractor.report.num_hive_metastore_fqns_resolved == 1

    def test_casefolded_match_against_this_runs_tables(self) -> None:
        extractor = make_extractor()
        extractor.register_dpms_tables([make_dpms_table_entry()])

        resolved = extractor._resolve_hive_metastore_fqn(
            "hive_metastore:`localhost:9083`.MY_DATABASE.MY_TABLE"
        )

        assert resolved == DPMS_TABLE_URN

    def test_ambiguous_pair_falls_back_to_a_hive_node(self) -> None:
        """The same db.table under two services must not be guessed."""
        extractor = make_extractor()
        extractor.register_dpms_tables(make_same_table_in_two_services())

        resolved = extractor._resolve_hive_metastore_fqn(
            "hive_metastore:`localhost:9083`.my_database.my_table"
        )

        assert resolved == HIVE_TABLE_URN
        assert extractor.report.num_hive_metastore_fqns_ambiguous == 1
        assert extractor.report.num_hive_metastore_fallback_nodes == 1

    @pytest.mark.parametrize(
        "db_table", ["my_database.my_table", "MY_DATABASE.MY_TABLE"]
    )
    def test_ambiguous_pair_uses_the_config_fallback(self, db_table: str) -> None:
        """The fallback picks that service's ingested table, not an FQN-cased URN."""
        extractor = make_extractor(
            dpms_hive_metastore_service="my-project.us-west1.service-b"
        )
        tables = make_same_table_in_two_services()
        extractor.register_dpms_tables(tables)

        resolved = extractor._resolve_hive_metastore_fqn(
            f"hive_metastore:`localhost:9083`.{db_table}"
        )

        assert resolved == tables[1].datahub_dataset_urn
        assert extractor.report.num_hive_metastore_fqns_ambiguous == 1

    def test_ambiguous_pair_mints_a_config_fallback_outside_the_run(self) -> None:
        extractor = make_extractor(
            dpms_hive_metastore_service="my-project.us-west1.my-service"
        )
        extractor.register_dpms_tables(make_same_table_in_two_services())

        resolved = extractor._resolve_hive_metastore_fqn(
            "hive_metastore:`localhost:9083`.my_database.my_table"
        )

        assert resolved == DPMS_TABLE_URN
        assert extractor.report.num_hive_metastore_fqns_ambiguous == 1

    def test_config_fallback_mints_a_metastore_urn(self) -> None:
        extractor = make_extractor(
            dpms_hive_metastore_service="my-project.us-west1.my-service"
        )

        resolved = extractor._resolve_hive_metastore_fqn(
            "hive_metastore:`localhost:9083`.my_database.my_table"
        )

        assert resolved == DPMS_TABLE_URN

    @pytest.mark.parametrize("value", ["not-three-parts", "my-project..my-service"])
    def test_malformed_config_fallback_is_rejected(self, value: str) -> None:
        with pytest.raises(ValidationError):
            make_extractor(dpms_hive_metastore_service=value)

    def test_hive_nodes_disabled_skips_the_edge(self) -> None:
        extractor = make_extractor(include_hive_metastore_nodes=False)

        resolved = extractor._resolve_hive_metastore_fqn(
            "hive_metastore:`localhost:9083`.my_database.my_table"
        )

        assert resolved is None
        assert extractor.report.num_hive_metastore_fqns_unresolved == 1

    @pytest.mark.parametrize(
        "upstream_fqn,title",
        [
            (
                "hive_metastore:`localhost:9083`.other_db.other_table",
                "Dataplex hive_metastore upstream unmatched",
            ),
            (
                "hive_metastore:`localhost:9083`.my_database.my_table",
                "Dataplex hive_metastore upstream ambiguous",
            ),
        ],
    )
    def test_unresolved_upstream_warns_with_a_dedicated_title(
        self, upstream_fqn: str, title: str
    ) -> None:
        extractor = make_extractor(include_hive_metastore_nodes=False)
        extractor.register_dpms_tables(make_same_table_in_two_services())

        extractor._report_unresolved_upstream(make_dpms_table_entry(), upstream_fqn)

        warning = cast(Mock, extractor.source_report.warning).call_args
        assert warning.kwargs["title"] == title


class TestStorageLineage:
    def test_storage_aspect_produces_a_bucket_edge(self) -> None:
        extractor = make_extractor(include_storage_lineage=True)
        entry = make_dpms_table_entry(storage_gcs_bucket="my-bucket")

        edge = extractor._storage_lineage_edge(entry)

        assert edge is not None
        assert (
            edge.upstream_datahub_urn
            == "urn:li:dataset:(urn:li:dataPlatform:gcs,my-bucket,PROD)"
        )
        assert extractor.report.num_storage_lineage_edges_added == 1

    def test_missing_storage_aspect_is_counted(self) -> None:
        extractor = make_extractor(include_storage_lineage=True)

        assert extractor._storage_lineage_edge(make_dpms_table_entry()) is None
        assert extractor.report.num_storage_lineage_missing == 1

    def test_off_by_default(self) -> None:
        """Opt-in: the edge comes from catalog metadata, not the Data Lineage API."""
        extractor = make_extractor()
        entry = make_dpms_table_entry(storage_gcs_bucket="my-bucket")

        assert extractor._storage_lineage_edge(entry) is None

    def test_non_metastore_entries_have_no_storage_edge(self) -> None:
        extractor = make_extractor(include_storage_lineage=True)
        entry = EntryDataTuple(
            dataplex_entry_short_name="my_table",
            dataplex_entry_name="projects/p/locations/us/entryGroups/g/entries/my_table",
            dataplex_location="us",
            dataplex_entry_fqn="bigquery:my-project.my_dataset.my_table",
            dataplex_entry_type_short_name="bigquery-table",
            datahub_platform="bigquery",
            datahub_dataset_name="my-project.my_dataset.my_table",
            datahub_dataset_urn=(
                "urn:li:dataset:(urn:li:dataPlatform:bigquery,"
                "my-project.my_dataset.my_table,PROD)"
            ),
            storage_gcs_bucket="my-bucket",
        )

        assert extractor._storage_lineage_edge(entry) is None
        assert extractor.report.num_storage_lineage_missing == 0

    def test_storage_edge_survives_an_empty_api_answer(self) -> None:
        extractor = make_extractor(include_storage_lineage=True)
        entry = make_dpms_table_entry(storage_gcs_bucket="my-bucket")

        edges, _mappings = extractor._extract_lineage_edges_for_entry(
            entry, {"upstream": [], "downstream": []}
        )

        assert len(edges) == 1
        assert extractor.report.num_lineage_entries_without_lineage == 0

    def test_empty_api_answer_without_a_storage_edge_is_counted(self) -> None:
        extractor = make_extractor()
        entry = make_dpms_table_entry(storage_gcs_bucket="my-bucket")

        edges, _mappings = extractor._extract_lineage_edges_for_entry(
            entry, {"upstream": [], "downstream": []}
        )

        assert edges == set()
        assert extractor.report.num_lineage_entries_without_lineage == 1
        assert list(extractor.report.lineage_entries_without_lineage_samples) == [
            f"entry={entry.dataplex_entry_name}, reason=empty_upstream"
        ]

    def test_storage_edge_is_withheld_when_the_lookup_failed(self) -> None:
        """upstreamLineage is whole-value: a storage-only edge set would
        replace persisted real upstreams on a transient API failure."""
        extractor = make_extractor(include_storage_lineage=True)
        entry = make_dpms_table_entry(storage_gcs_bucket="my-bucket")

        edges, _mappings = extractor._extract_lineage_edges_for_entry(entry, None)

        assert edges == set()

    def test_api_link_to_the_same_bucket_merges_with_the_storage_edge(self) -> None:
        extractor = make_extractor(include_storage_lineage=True)
        entry = make_dpms_table_entry(storage_gcs_bucket="my-bucket")

        edges, _mappings = extractor._extract_lineage_edges_for_entry(
            entry,
            {
                "upstream": ["gcs:my-bucket/warehouse/my_database.db/my_table"],
                "downstream": [],
            },
        )

        assert len(edges) == 1
        assert extractor.report.num_storage_lineage_edges_added == 1
        assert extractor.report.num_lineage_edges_added == 0


def _resolver(extractor: DataplexLineageExtractor, client: Mock) -> None:
    extractor._pubsub_resolver = PubSubSubscriptionResolver(
        report=extractor.report,
        source_report=extractor.source_report,
        subscriber_client=client,
    )


def _subscriber(topic: str) -> MagicMock:
    client = MagicMock()
    client.get_subscription.return_value = MagicMock(topic=topic)
    return client


class TestPubSubSubscriptionResolution:
    def test_subscription_resolves_to_the_backing_topic_urn(self) -> None:
        extractor = make_extractor(resolve_pubsub_subscriptions=True)
        _resolver(extractor, _subscriber("projects/my-project/topics/my-topic"))

        resolved = extractor._resolve_upstream_fqn(
            "pubsub:subscription:my-project.my-subscription", {}
        )

        assert (
            resolved
            == "urn:li:dataset:(urn:li:dataPlatform:pubsub,my-project.my-topic,PROD)"
        )
        assert extractor.report.num_pubsub_subscriptions_resolved == 1

    def test_resolution_is_cached_per_subscription(self) -> None:
        extractor = make_extractor(resolve_pubsub_subscriptions=True)
        client = _subscriber("projects/my-project/topics/my-topic")
        _resolver(extractor, client)
        assert extractor._pubsub_resolver is not None

        for _ in range(3):
            extractor._pubsub_resolver.resolve_topic("my-project", "my-subscription")

        assert client.get_subscription.call_count == 1
        assert extractor.report.num_pubsub_subscription_cache_hits == 2

    def test_dotted_topic_id_resolves_to_the_topic_urn(self) -> None:
        extractor = make_extractor(resolve_pubsub_subscriptions=True)
        _resolver(extractor, _subscriber("projects/my-project/topics/orders.v2"))

        resolved = extractor._resolve_upstream_fqn(
            "pubsub:subscription:my-project.orders-sub", {}
        )

        assert (
            resolved
            == "urn:li:dataset:(urn:li:dataPlatform:pubsub,my-project.orders.v2,PROD)"
        )
        assert extractor.report.num_pubsub_subscriptions_resolved == 1

    def test_deleted_topic_sentinel_is_unresolved(self) -> None:
        extractor = make_extractor(resolve_pubsub_subscriptions=True)
        _resolver(extractor, _subscriber("_deleted-topic_"))

        assert (
            extractor._resolve_upstream_fqn(
                "pubsub:subscription:my-project.my-subscription", {}
            )
            is None
        )
        assert extractor.report.num_pubsub_subscriptions_unresolved == 1

    def test_api_failure_degrades_to_skipping_the_edge(self) -> None:
        extractor = make_extractor(resolve_pubsub_subscriptions=True)
        client = MagicMock()
        client.get_subscription.side_effect = RuntimeError("permission denied")
        _resolver(extractor, client)

        assert (
            extractor._resolve_upstream_fqn(
                "pubsub:subscription:my-project.my-subscription", {}
            )
            is None
        )
        assert extractor.report.num_pubsub_subscriptions_unresolved == 1

    def test_missing_client_is_reported_once(self) -> None:
        extractor = make_extractor(resolve_pubsub_subscriptions=True)
        assert extractor._pubsub_resolver is not None

        with patch(
            "google.cloud.pubsub_v1.SubscriberClient",
            side_effect=RuntimeError("no credentials"),
        ):
            for subscription in ("sub-a", "sub-b"):
                extractor._pubsub_resolver.resolve_topic("my-project", subscription)

        warning = cast(Mock, extractor.source_report.warning)
        assert warning.call_count == 1
        assert (
            warning.call_args.kwargs["title"] == "Dataplex Pub/Sub client unavailable"
        )
        assert extractor.report.num_pubsub_subscriptions_unresolved == 2

    def test_feature_off_leaves_subscriptions_unresolved(self) -> None:
        extractor = make_extractor()

        assert extractor._pubsub_resolver is None
        assert (
            extractor._resolve_upstream_fqn(
                "pubsub:subscription:my-project.my-subscription", {}
            )
            is None
        )

    def test_unresolved_subscription_warns_with_a_dedicated_title(self) -> None:
        extractor = make_extractor()
        entry = make_dpms_table_entry()

        extractor._report_unresolved_upstream(
            entry, "pubsub:subscription:my-project.my-subscription"
        )

        warning = cast(Mock, extractor.source_report.warning).call_args
        assert (
            warning.kwargs["title"]
            == "Dataplex Pub/Sub subscription upstream unresolved"
        )


def _storage_entry(aspects: Dict[str, Any]) -> dataplex_v1.Entry:
    entry = Mock(spec=dataplex_v1.Entry)
    entry.aspects = aspects
    return entry


@pytest.mark.parametrize(
    "aspects,expected",
    [
        (
            {
                "655216118709.global.storage": dataplex_v1.Aspect(
                    data={
                        "service": "CLOUD_STORAGE",
                        "resourceName": "gs://warehouse-bucket/my_db.db/events",
                    }
                )
            },
            "warehouse-bucket",
        ),
        (
            {
                "projects/p/locations/global/aspectTypes/storage": dataplex_v1.Aspect(
                    data={"service": "CLOUD_STORAGE", "resourceName": "gs://b/x"}
                )
            },
            "b",
        ),
        (
            {
                "655216118709.global.storage": dataplex_v1.Aspect(
                    data={"service": "BIGQUERY", "resourceName": "gs://b/x"}
                )
            },
            None,
        ),
        (
            {
                "655216118709.global.storage": dataplex_v1.Aspect(
                    data={"service": "CLOUD_STORAGE", "resourceName": "s3://b/x"}
                )
            },
            None,
        ),
        (
            {
                "655216118709.global.storage": dataplex_v1.Aspect(
                    data={"service": "CLOUD_STORAGE", "resourceName": "gs:///x"}
                )
            },
            None,
        ),
        ({"655216118709.global.schema": dataplex_v1.Aspect(data={})}, None),
        ({}, None),
    ],
)
def test_storage_bucket_from_entry_aspects(
    aspects: Dict[str, Any], expected: Optional[str]
) -> None:
    assert (
        extract_gcs_storage_bucket_from_entry_aspects(_storage_entry(aspects), "e")
        == expected
    )


def test_storage_edge_is_withheld_when_every_parent_fails() -> None:
    lineage_client = MagicMock()
    lineage_client.search_links.side_effect = RuntimeError("unavailable")
    extractor = make_extractor(lineage_client, include_storage_lineage=True)

    workunits = extractor._process_entry_lineage(
        make_dpms_table_entry(storage_gcs_bucket="my-bucket"), SCAN_PAIRS
    )

    assert workunits == []


@pytest.mark.parametrize(
    "healthy_parent_fqns",
    [
        pytest.param([], id="empty"),
        pytest.param(["pubsub:subscription:my-project.some-sub"], id="unresolvable"),
    ],
)
def test_storage_edge_is_withheld_when_a_parent_fails_and_no_link_resolves(
    healthy_parent_fqns: List[str],
) -> None:
    def search_links(request: Any) -> List[FakeLink]:
        if request.parent.endswith("/us-central1"):
            raise RuntimeError("unavailable")
        return [
            FakeLink(source=FakeEntityReference(fqn), target=FakeEntityReference())
            for fqn in healthy_parent_fqns
        ]

    lineage_client = MagicMock()
    lineage_client.search_links.side_effect = search_links
    extractor = make_extractor(lineage_client, include_storage_lineage=True)

    workunits = extractor._process_entry_lineage(
        make_dpms_table_entry(storage_gcs_bucket="my-bucket"),
        [("my-project", "us-central1"), ("my-project", "us-east1")],
    )

    assert workunits == []
    assert extractor.report.num_storage_lineage_withheld == 1


def test_storage_edge_is_kept_when_a_parent_fails_but_a_link_resolves() -> None:
    def search_links(request: Any) -> List[FakeLink]:
        if request.parent.endswith("/us-central1"):
            raise RuntimeError("unavailable")
        return [
            FakeLink(
                source=FakeEntityReference("gcs:other-bucket"),
                target=FakeEntityReference(),
            )
        ]

    lineage_client = MagicMock()
    lineage_client.search_links.side_effect = search_links
    extractor = make_extractor(lineage_client, include_storage_lineage=True)

    workunits = extractor._process_entry_lineage(
        make_dpms_table_entry(storage_gcs_bucket="my-bucket"),
        [("my-project", "us-central1"), ("my-project", "us-east1")],
    )

    assert len(workunits) == 1
    lineage = workunits[0].get_aspect_of_type(UpstreamLineageClass)
    assert lineage is not None
    assert {upstream.dataset for upstream in lineage.upstreams} == {
        "urn:li:dataset:(urn:li:dataPlatform:gcs,other-bucket,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:gcs,my-bucket,PROD)",
    }
    assert extractor.report.num_storage_lineage_withheld == 0
