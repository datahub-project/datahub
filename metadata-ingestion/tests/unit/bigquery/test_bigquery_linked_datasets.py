from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any, Dict, Iterable, List, Optional
from unittest.mock import MagicMock, patch

import pytest
from google.api_core.exceptions import (
    GoogleAPIError,
    InternalServerError,
    NotFound,
    PermissionDenied,
    ResourceExhausted,
    ServiceUnavailable,
)
from google.cloud import bigquery_analyticshub_v1, resourcemanager_v3
from google.rpc.error_details_pb2 import ErrorInfo

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.bigquery_v2.bigquery_config import BigQueryV2Config
from datahub.ingestion.source.bigquery_v2.bigquery_linked_datasets import (
    _LIST_SUBSCRIPTIONS_RETRY,
    _LIST_SUBSCRIPTIONS_TIMEOUT,
    BigQueryLinkedDatasetsHandler,
    LinkedDatasetInfo,
    PublisherRef,
    create_analyticshub_client,
)
from datahub.ingestion.source.bigquery_v2.bigquery_report import BigQueryV2Report
from datahub.ingestion.source.bigquery_v2.bigquery_schema import (
    BigqueryColumn,
    BigqueryDataset,
)
from datahub.ingestion.source.bigquery_v2.bigquery_schema_gen import (
    BigQuerySchemaGenerator,
)
from datahub.ingestion.source.bigquery_v2.common import (
    BigQueryFilter,
    BigQueryIdentifierBuilder,
)
from datahub.metadata.com.linkedin.pegasus2avro.common import Siblings
from datahub.metadata.com.linkedin.pegasus2avro.dataset import (
    DatasetLineageType,
    UpstreamLineage,
)
from datahub.metadata.schema_classes import (
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
)
from tests.integration.bigquery_v2.common import (
    DEFAULT_CREATION_TIME,
    DEFAULT_LAST_MODIFY_TIME,
    STATE_ACTIVE,
    make_dataset_with_linked_source,
    make_dataset_without_linked_source,
    make_subscription,
)

# --- Fixtures and helpers --------------------------------------------------


def _make_config(**overrides: Any) -> BigQueryV2Config:
    base: Dict[str, Any] = {
        "project_ids": ["consumer-project"],
        "include_linked_datasets": True,
        "include_linked_dataset_lineage": True,
    }
    base.update(overrides)
    return BigQueryV2Config.model_validate(base)


def test_linked_dataset_lineage_requires_queries_v2() -> None:
    """The COPY edge is only single-writer under queries-v2, so the legacy path
    keeps detection but drops lineage."""
    assert _make_config().include_linked_dataset_lineage
    assert not _make_config(use_queries_v2=False).include_linked_dataset_lineage
    # Detection itself is unaffected by the extraction path.
    assert _make_config(use_queries_v2=False).include_linked_datasets


def _make_handler(
    config: Optional[BigQueryV2Config] = None,
) -> BigQueryLinkedDatasetsHandler:
    config = config or _make_config()
    report = BigQueryV2Report()
    identifiers = BigQueryIdentifierBuilder(config, report)
    filters = BigQueryFilter(config, report)
    handler = BigQueryLinkedDatasetsHandler(
        config=config, report=report, identifiers=identifiers, filters=filters
    )
    return handler


def _ah_client_returning(
    subscriptions_per_location: Dict[str, List[Any]],
) -> MagicMock:
    """Build an AH client mock whose `list_subscriptions` returns by location."""
    mock_client = MagicMock()

    def _list_subscriptions(parent: str, **kwargs: Any) -> List[Any]:
        # parent format: projects/<p>/locations/<location>
        location = parent.rsplit("/", 1)[-1]
        return subscriptions_per_location.get(location, [])

    mock_client.list_subscriptions.side_effect = _list_subscriptions
    return mock_client


def _bq_client_returning(
    datasets: Dict[str, Any],
) -> MagicMock:
    """Build a BigQuery client mock whose `get_dataset` returns a fake dataset.

    `datasets` maps `"<project>.<dataset>"` to either a fake dataset object or
    an exception instance to raise.
    """
    mock_client = MagicMock()

    def _get_dataset(fqn: str) -> Any:
        result = datasets[fqn]
        if isinstance(result, BaseException):
            raise result
        return result

    mock_client.get_dataset.side_effect = _get_dataset
    return mock_client


def _rm_client_returning(
    project_ids: Dict[str, Any],
) -> MagicMock:
    """Build a Resource Manager client mock for `get_project`.

    `project_ids` maps `"<number>"` to either a project_id string or an
    exception instance to raise.
    """
    mock_client = MagicMock()

    def _get_project(name: str) -> Any:
        number = name.rsplit("/", 1)[-1]
        result = project_ids[number]
        if isinstance(result, BaseException):
            raise result
        return resourcemanager_v3.Project(project_id=result)

    mock_client.get_project.side_effect = _get_project
    return mock_client


@dataclass
class _SeededHandler:
    handler: BigQueryLinkedDatasetsHandler
    ah: MagicMock
    bq: MagicMock
    rm: MagicMock


def _seeded_handler(
    *,
    subscriptions: Optional[Dict[str, List[Any]]] = None,
    ah: Optional[MagicMock] = None,
    datasets: Optional[Dict[str, Any]] = None,
    publisher_projects: Optional[Dict[str, Any]] = None,
    config: Optional[BigQueryV2Config] = None,
) -> _SeededHandler:
    """Build a handler with all three Google clients replaced by mocks.

    `publisher_projects` defaults to resolving the number
    `make_dataset_with_linked_source` publishes from.
    """
    handler = _make_handler(config=config)
    ah_client = ah if ah is not None else _ah_client_returning(subscriptions or {})
    bq_client = _bq_client_returning(datasets or {})
    rm_client = _rm_client_returning(
        publisher_projects
        if publisher_projects is not None
        else {"111222333": "publisher-project"}
    )
    handler._ah_client = ah_client
    handler._bq_client = bq_client
    handler._rm_client = rm_client
    return _SeededHandler(handler=handler, ah=ah_client, bq=bq_client, rm=rm_client)


def _column(name: str) -> BigqueryColumn:
    return BigqueryColumn(
        name=name,
        ordinal_position=1,
        is_nullable=False,
        field_path=name,
        is_partition_column=False,
        cluster_column_position=None,
        data_type="INT64",
        comment="",
    )


# --- Client wiring (credentials) -------------------------------------------

_AH_CLIENT_CLASS = (
    "datahub.ingestion.source.bigquery_v2.bigquery_linked_datasets"
    ".bigquery_analyticshub_v1.AnalyticsHubServiceClient"
)


def test_create_analyticshub_client_uses_configured_credentials():
    # Regression guard: the Analytics Hub client must authenticate as the
    # configured identity, not silently fall back to ADC.
    config = _make_config()
    sentinel = object()
    with (
        patch.object(BigQueryV2Config, "get_credentials", return_value=sentinel),
        patch(_AH_CLIENT_CLASS) as ah_cls,
    ):
        create_analyticshub_client(config)
    kwargs = ah_cls.call_args.kwargs
    assert kwargs["credentials"] is sentinel
    assert "DataHub" in kwargs["client_info"].user_agent


def test_get_rm_client_delegates_to_config_projects_client():
    # Regression guard: reuse the credential-aware factory rather than building
    # a bare ProjectsClient that ignores configured credentials.
    config = _make_config()
    handler = _make_handler(config=config)
    sentinel = object()
    with patch.object(
        BigQueryV2Config, "get_projects_client", return_value=sentinel
    ) as get_projects:
        result = handler._get_rm_client()
    assert result is sentinel
    get_projects.assert_called_once_with()


def test_list_subscriptions_uses_transient_retry_and_deadline():
    # The Analytics Hub client neither auto-retries nor applies an RPC deadline,
    # so the call must pass both explicitly.
    seeded = _seeded_handler(subscriptions={"us": []})
    seeded.handler.populate_for_project(
        "consumer-project", [BigqueryDataset(name="d", location="US")]
    )
    kwargs = seeded.ah.list_subscriptions.call_args.kwargs
    assert kwargs["retry"] is _LIST_SUBSCRIPTIONS_RETRY
    assert kwargs["timeout"] == _LIST_SUBSCRIPTIONS_TIMEOUT


def test_list_subscriptions_retry_covers_transient_codes_only():
    predicate = _LIST_SUBSCRIPTIONS_RETRY._predicate
    assert predicate(ServiceUnavailable("503"))
    assert predicate(InternalServerError("500"))
    assert predicate(ResourceExhausted("429"))
    assert not predicate(PermissionDenied("403"))
    assert not predicate(NotFound("404"))


# --- LinkedDatasetInfo tests -----------------------------------------------


def test_extra_properties_includes_source_when_publisher_resolved():
    info = LinkedDatasetInfo(
        publisher=PublisherRef(
            dataset="publisher_dataset",
            project_id="publisher-project",
        ),
        subscription_state=STATE_ACTIVE,
        link_state="LINKED",
        listing="listing_a",
        publisher_organization="Publisher Inc",
        creation_time="2024-01-02T03:04:05+00:00",
        last_modify_time="2024-03-04T05:06:07+00:00",
    )
    props = info.to_extra_properties()
    assert props["linked_dataset.source"] == "publisher-project.publisher_dataset"
    assert props["linked_dataset.link_state"] == "LINKED"
    assert props["analytics_hub.listing"] == "listing_a"
    assert props["analytics_hub.subscription_state"] == "STATE_ACTIVE"
    assert props["analytics_hub.publisher_organization"] == "Publisher Inc"
    assert props["analytics_hub.link_creation_time"] == "2024-01-02T03:04:05+00:00"
    assert props["analytics_hub.last_modify_time"] == "2024-03-04T05:06:07+00:00"


def test_extra_properties_omits_unpopulated_keys():
    info = LinkedDatasetInfo(
        publisher=PublisherRef(
            dataset="publisher_dataset",
            project_id="publisher-project",
        ),
    )
    props = info.to_extra_properties()
    assert "linked_dataset.source" in props
    # Optional keys must be omitted when None.
    assert "linked_dataset.link_state" not in props
    assert "analytics_hub.listing" not in props
    assert "analytics_hub.publisher_organization" not in props
    assert "analytics_hub.link_creation_time" not in props


def test_extra_properties_no_source_when_publisher_unresolved():
    info = LinkedDatasetInfo(
        publisher=None,
        link_state="LINKED",
    )
    props = info.to_extra_properties()
    # Without a resolved publisher project, the source key must not appear.
    assert "linked_dataset.source" not in props
    # Other governance keys still emit.
    assert props["linked_dataset.link_state"] == "LINKED"


# --- populate_for_project tests --------------------------------------------


def test_subscription_fields_mapped_onto_linked_dataset_info():
    seeded = _seeded_handler(
        subscriptions={"us": [make_subscription(dataset_id="shared_a")]},
        datasets={
            "consumer-project.shared_a": make_dataset_with_linked_source(
                dataset_id="shared_a"
            )
        },
    )

    seeded.handler.populate_for_project(
        "consumer-project", [BigqueryDataset(name="shared_a", location="US")]
    )

    info = seeded.handler.get_info("consumer-project", "shared_a")
    assert info is not None
    assert info.publisher == PublisherRef(
        dataset="publisher_dataset", project_id="publisher-project"
    )
    assert info.link_state == "LINKED"
    assert info.listing == "listing_a"
    assert info.subscription_state == STATE_ACTIVE
    assert info.publisher_organization == "Publisher Inc"
    assert info.creation_time == DEFAULT_CREATION_TIME.isoformat()
    assert info.last_modify_time == DEFAULT_LAST_MODIFY_TIME.isoformat()


def test_only_bigquery_dataset_subscriptions_advance():
    """Non-BigQuery shared resources (e.g. Pub/Sub) are skipped."""
    pubsub_sub = make_subscription(
        dataset_id="shared_b",
        resource_type=bigquery_analyticshub_v1.SharedResourceType.PUBSUB_TOPIC,
    )
    seeded = _seeded_handler(
        subscriptions={"us": [make_subscription(dataset_id="shared_a"), pubsub_sub]},
        datasets={"consumer-project.shared_a": make_dataset_with_linked_source()},
    )

    datasets = [BigqueryDataset(name="shared_a", location="US")]
    seeded.handler.populate_for_project("consumer-project", datasets)

    assert seeded.handler.get_info("consumer-project", "shared_a") is not None
    assert seeded.handler.get_info("consumer-project", "shared_b") is None
    assert seeded.handler.report.num_linked_datasets_scanned == 1
    # Publisher resolution should have happened only once for the BQ_DATASET sub.
    seeded.bq.get_dataset.assert_called_once_with("consumer-project.shared_a")


def test_dataset_pattern_filter_short_circuits():
    """A dataset excluded by dataset_pattern is skipped before any get_dataset call."""
    config = _make_config(
        dataset_pattern=AllowDenyPattern(allow=[".*shared_a$"], deny=[])
    )
    seeded = _seeded_handler(
        config=config,
        subscriptions={
            "us": [
                make_subscription(dataset_id="shared_a"),
                make_subscription(dataset_id="shared_b"),
            ]
        },
        datasets={"consumer-project.shared_a": make_dataset_with_linked_source()},
    )

    datasets = [
        BigqueryDataset(name="shared_a", location="US"),
        BigqueryDataset(name="shared_b", location="US"),
    ]
    seeded.handler.populate_for_project("consumer-project", datasets)

    assert seeded.handler.get_info("consumer-project", "shared_a") is not None
    assert seeded.handler.get_info("consumer-project", "shared_b") is None
    seeded.bq.get_dataset.assert_called_once_with("consumer-project.shared_a")


def test_locations_lowercased_for_ah_call():
    seeded = _seeded_handler(
        subscriptions={"eu": [make_subscription(dataset_id="shared_a")]},
        datasets={"consumer-project.shared_a": make_dataset_with_linked_source()},
    )

    # Mixed-case location coming back from BigQuery's API is normalised.
    datasets = [BigqueryDataset(name="shared_a", location="EU")]
    seeded.handler.populate_for_project("consumer-project", datasets)

    assert (
        seeded.ah.list_subscriptions.call_args.kwargs["parent"]
        == "projects/consumer-project/locations/eu"
    )


@pytest.mark.parametrize(
    "error",
    [
        # Missing bigquery.datasets.get, or a subscription outliving its
        # linked dataset (404) — both drop it to plain dataset ingestion.
        PermissionDenied("bigquery.datasets.get denied"),
        NotFound("dataset deleted"),
    ],
)
def test_get_dataset_error_skips_dataset(error):
    seeded = _seeded_handler(
        subscriptions={"us": [make_subscription(dataset_id="shared_a")]},
        datasets={"consumer-project.shared_a": error},
    )

    datasets = [BigqueryDataset(name="shared_a", location="US")]
    seeded.handler.populate_for_project("consumer-project", datasets)

    assert seeded.handler.get_info("consumer-project", "shared_a") is None
    assert seeded.handler.report.num_linked_dataset_get_dataset_errors == 1


def test_linked_dataset_without_source_is_warned_and_kept():
    seeded = _seeded_handler(
        subscriptions={"us": [make_subscription(dataset_id="shared_a")]},
        datasets={
            "consumer-project.shared_a": make_dataset_without_linked_source(
                dataset_id="shared_a"
            )
        },
    )

    datasets = [BigqueryDataset(name="shared_a", location="US")]
    seeded.handler.populate_for_project("consumer-project", datasets)

    info = seeded.handler.get_info("consumer-project", "shared_a")
    assert info is not None
    assert info.has_resolved_publisher is False
    assert seeded.handler.report.num_linked_dataset_source_unresolved == 1
    assert len(seeded.handler.report.warnings) == 1


def test_publisher_resolve_failure_keeps_dataset_but_skips_lineage():
    seeded = _seeded_handler(
        subscriptions={"us": [make_subscription(dataset_id="shared_a")]},
        datasets={"consumer-project.shared_a": make_dataset_with_linked_source()},
        publisher_projects={
            "111222333": PermissionDenied("resourcemanager.projects.get denied")
        },
    )

    datasets = [BigqueryDataset(name="shared_a", location="US")]
    seeded.handler.populate_for_project("consumer-project", datasets)

    info = seeded.handler.get_info("consumer-project", "shared_a")
    # Dataset is still recognised as linked (governance properties emit).
    assert info is not None
    # But publisher project ID was not resolved, so no lineage can be emitted.
    assert info.has_resolved_publisher is False
    assert seeded.handler.report.num_linked_dataset_project_resolve_errors == 1

    # And lineage emission is a no-op on this dataset.
    wus = list(
        seeded.handler.gen_lineage_workunits(
            consumer_project_id="consumer-project",
            consumer_dataset="shared_a",
            entity_name="t1",
            columns=[_column("id")],
        )
    )
    assert wus == []


@pytest.mark.parametrize(
    "rm_result",
    [
        "publisher-project",
        PermissionDenied(
            "resourcemanager.projects.get denied",
            error_info=ErrorInfo(
                reason="IAM_PERMISSION_DENIED",
                domain="cloudresourcemanager.googleapis.com",
            ),
        ),
    ],
    ids=["success", "denied"],
)
def test_resource_manager_result_is_cached_per_project_number(rm_result):
    """Publisher project resolution is cached per number, for success and denial."""
    # Both subscriptions point at the same publisher project number.
    seeded = _seeded_handler(
        subscriptions={
            "us": [
                make_subscription(dataset_id="shared_a"),
                make_subscription(dataset_id="shared_b"),
            ]
        },
        datasets={
            "consumer-project.shared_a": make_dataset_with_linked_source(
                publisher_dataset="dataset_a"
            ),
            "consumer-project.shared_b": make_dataset_with_linked_source(
                publisher_dataset="dataset_b"
            ),
        },
        publisher_projects={"111222333": rm_result},
    )

    datasets = [
        BigqueryDataset(name="shared_a", location="US"),
        BigqueryDataset(name="shared_b", location="US"),
    ]
    seeded.handler.populate_for_project("consumer-project", datasets)

    # Both outcomes are cached, so the second subscription reuses the result.
    seeded.rm.get_project.assert_called_once_with(name="projects/111222333")


@pytest.mark.parametrize(
    "rm_error, expected_title",
    [
        (
            PermissionDenied(
                "resourcemanager.projects.get denied",
                error_info=ErrorInfo(
                    reason="IAM_PERMISSION_DENIED",
                    domain="cloudresourcemanager.googleapis.com",
                ),
            ),
            "Missing permission to read publisher project",
        ),
        (InternalServerError("500"), "Cannot resolve publisher project ID"),
    ],
    ids=["iam_denied", "server_error"],
)
def test_publisher_resolve_failure_title_matches_the_cause(rm_error, expected_title):
    seeded = _seeded_handler(
        subscriptions={"us": [make_subscription(dataset_id="shared_a")]},
        datasets={"consumer-project.shared_a": make_dataset_with_linked_source()},
        publisher_projects={"111222333": rm_error},
    )

    seeded.handler.populate_for_project(
        "consumer-project", [BigqueryDataset(name="shared_a", location="US")]
    )

    assert [w.title for w in seeded.handler.report.warnings] == [expected_title]


def test_unresolved_source_is_counted_per_dataset_not_per_publisher():
    """Every dataset behind an unreadable publisher is counted, not just the first."""
    names = ["shared_a", "shared_b", "shared_c"]
    seeded = _seeded_handler(
        subscriptions={"us": [make_subscription(dataset_id=name) for name in names]},
        datasets={
            f"consumer-project.{name}": make_dataset_with_linked_source(
                publisher_dataset=name
            )
            for name in names
        },
        publisher_projects={"111222333": PermissionDenied("403")},
    )

    seeded.handler.populate_for_project(
        "consumer-project",
        [BigqueryDataset(name=name, location="US") for name in names],
    )

    assert seeded.handler.report.num_linked_dataset_source_unresolved == 3
    # The RM call and its warning still happen once, thanks to the negative cache.
    assert seeded.handler.report.num_linked_dataset_project_resolve_errors == 1
    seeded.rm.get_project.assert_called_once_with(name="projects/111222333")


def test_list_subscriptions_api_disabled_is_warned_not_fatal():
    ah = MagicMock()
    ah.list_subscriptions.side_effect = PermissionDenied(
        "Analytics Hub API has not been used in project ... or it is disabled.",
        error_info=ErrorInfo(reason="SERVICE_DISABLED", domain="googleapis.com"),
    )
    handler = _seeded_handler(ah=ah).handler

    datasets = [BigqueryDataset(name="shared_a", location="US")]
    handler.populate_for_project("consumer-project", datasets)

    assert handler.get_info("consumer-project", "shared_a") is None
    assert not handler.report.failures
    assert any(
        w.title == "BigQuery Sharing (Analytics Hub) API not enabled"
        for w in handler.report.warnings
    )


def test_list_subscriptions_iam_denied_is_reported_as_failure():
    ah = MagicMock()
    ah.list_subscriptions.side_effect = PermissionDenied(
        "Permission analyticshub.subscriptions.list denied.",
        error_info=ErrorInfo(
            reason="IAM_PERMISSION_DENIED", domain="analyticshub.googleapis.com"
        ),
    )
    handler = _seeded_handler(ah=ah).handler

    datasets = [BigqueryDataset(name="shared_a", location="US")]
    # A missing grant on an explicitly-enabled feature is a failure, but must
    # not raise — core dataset ingestion continues.
    handler.populate_for_project("consumer-project", datasets)

    assert handler.get_info("consumer-project", "shared_a") is None
    assert not handler.report.warnings
    assert any(
        f.title == "Missing permission to list BigQuery Sharing subscriptions"
        for f in handler.report.failures
    )


def test_list_subscriptions_unclassified_error_is_reported_as_failure():
    ah = MagicMock()
    # A PermissionDenied we cannot classify (no ErrorInfo reason).
    ah.list_subscriptions.side_effect = PermissionDenied("denied")
    handler = _seeded_handler(ah=ah).handler

    datasets = [BigqueryDataset(name="shared_a", location="US")]
    handler.populate_for_project("consumer-project", datasets)

    assert handler.get_info("consumer-project", "shared_a") is None
    assert handler.report.num_linked_dataset_location_errors == 1
    assert any(
        f.title == "Unable to list BigQuery Sharing subscriptions"
        for f in handler.report.failures
    )


def test_one_failing_location_does_not_stop_the_others():
    """Each location is queried independently, so one failing does not stop the rest."""
    ah = MagicMock()

    def _list_subscriptions(parent: str, **kwargs: Any) -> List[Any]:
        if parent.endswith("/aws-us-east-1"):
            # Analytics Hub cannot serve a BigQuery Omni location.
            raise NotFound("unsupported location")
        return [make_subscription(dataset_id="shared_a")]

    ah.list_subscriptions.side_effect = _list_subscriptions
    seeded = _seeded_handler(
        ah=ah,
        datasets={"consumer-project.shared_a": make_dataset_with_linked_source()},
    )

    datasets = [
        BigqueryDataset(name="shared_a", location="EU"),
        BigqueryDataset(name="omni_ds", location="aws-us-east-1"),
    ]
    seeded.handler.populate_for_project("consumer-project", datasets)

    assert seeded.handler.get_info("consumer-project", "shared_a") is not None
    assert seeded.handler.report.num_linked_dataset_location_errors == 1
    assert seeded.ah.list_subscriptions.call_count == 2


def test_data_exchange_only_subscription_is_skipped():
    """Test only listing-level subscriptions are processed."""
    sub = make_subscription(
        dataset_id="shared_a",
        data_exchange="projects/123456789/locations/us/dataExchanges/exch_a",
    )
    seeded = _seeded_handler(subscriptions={"us": [sub]})

    datasets = [BigqueryDataset(name="shared_a", location="US")]
    seeded.handler.populate_for_project("consumer-project", datasets)

    assert seeded.handler.get_info("consumer-project", "shared_a") is None
    seeded.bq.get_dataset.assert_not_called()


# --- Lineage emission tests ------------------------------------------------


def _seed_with_linked_dataset(
    *,
    config: Optional[BigQueryV2Config] = None,
    publisher_project_id: Optional[str] = "publisher-project",
    publisher_dataset: str = "publisher_dataset",
) -> BigQueryLinkedDatasetsHandler:
    handler = _make_handler(config=config)
    publisher = (
        PublisherRef(dataset=publisher_dataset, project_id=publisher_project_id)
        if publisher_project_id is not None
        else None
    )
    # Populate the lookup directly to exercise emission without detection.
    handler._lookup[("consumer-project", "shared_dataset")] = LinkedDatasetInfo(
        publisher=publisher,
    )
    return handler


def _aspect(wu: MetadataWorkUnit) -> Any:
    return getattr(wu.metadata, "aspect", None)


def _urn(wu: MetadataWorkUnit) -> Any:
    return getattr(wu.metadata, "entityUrn", None)


def _aspects(wus: Iterable[MetadataWorkUnit]) -> List[Any]:
    return [aspect for aspect in map(_aspect, wus) if aspect is not None]


def test_emits_siblings_and_upstream_lineage_with_per_column_finegrained():
    """Reciprocal siblings plus a per-column COPY UpstreamLineage are emitted."""
    handler = _seed_with_linked_dataset()
    columns = [_column(c) for c in ("id", "email", "name")]

    wus = list(
        handler.gen_lineage_workunits(
            consumer_project_id="consumer-project",
            consumer_dataset="shared_dataset",
            entity_name="users",
            columns=columns,
        )
    )

    assert handler.report.num_linked_dataset_lineage_emitted == 1

    aspects = _aspects(wus)
    sibling_wus = [wu for wu in wus if isinstance(_aspect(wu), Siblings)]
    upstream_lineages = [a for a in aspects if isinstance(a, UpstreamLineage)]
    assert len(sibling_wus) == 1
    assert len(upstream_lineages) == 1

    consumer_wu = sibling_wus[0]
    consumer_urn = _urn(consumer_wu)
    consumer_sibling = _aspect(consumer_wu)
    assert "shared_dataset" in consumer_urn
    assert consumer_sibling.primary is False
    assert consumer_wu.is_primary_source is True

    (publisher_urn,) = consumer_sibling.siblings
    assert "publisher_dataset" in publisher_urn
    assert publisher_urn.endswith(",PROD)")

    # The reciprocal aspect is deferred to the end of the run.
    publisher_wus = list(handler.gen_publisher_sibling_workunits())
    assert len(publisher_wus) == 1
    publisher_wu = publisher_wus[0]
    publisher_sibling = _aspect(publisher_wu)
    assert _urn(publisher_wu) == publisher_urn
    assert publisher_sibling.primary is True
    assert publisher_sibling.siblings == [consumer_urn]
    # Out of this recipe's scope, so it must stay out of the stale-entity state.
    assert publisher_wu.is_primary_source is False

    upstream = upstream_lineages[0]
    assert len(upstream.upstreams) == 1
    assert upstream.upstreams[0].type == DatasetLineageType.COPY

    fine_grained = upstream.fineGrainedLineages
    assert fine_grained is not None
    assert len(fine_grained) == 3
    for fgl, col in zip(fine_grained, columns, strict=False):
        assert fgl.upstreamType == FineGrainedLineageUpstreamTypeClass.FIELD_SET
        assert fgl.downstreamType == FineGrainedLineageDownstreamTypeClass.FIELD
        assert fgl.upstreams is not None
        assert fgl.downstreams is not None
        assert len(fgl.upstreams) == 1
        assert len(fgl.downstreams) == 1
        assert "publisher_dataset" in fgl.upstreams[0]
        assert "shared_dataset" in fgl.downstreams[0]
        assert col.name in fgl.upstreams[0]
        assert col.name in fgl.downstreams[0]


def test_no_emission_when_dataset_not_in_lookup():
    handler = _make_handler()
    wus = list(
        handler.gen_lineage_workunits(
            consumer_project_id="consumer-project",
            consumer_dataset="nope",
            entity_name="users",
            columns=[_column("id")],
        )
    )
    assert wus == []


def test_convert_column_urns_to_lowercase_lowercases_both_sides():
    config = _make_config(convert_column_urns_to_lowercase=True)
    handler = _seed_with_linked_dataset(config=config)
    columns = [_column("Email"), _column("USER_ID")]

    wus = list(
        handler.gen_lineage_workunits(
            consumer_project_id="consumer-project",
            consumer_dataset="shared_dataset",
            entity_name="users",
            columns=columns,
        )
    )

    upstream = next(a for a in _aspects(wus) if isinstance(a, UpstreamLineage))
    fine_grained = upstream.fineGrainedLineages
    assert fine_grained is not None
    for fgl in fine_grained:
        # Both sides lowercased — "Email" and "USER_ID" become "email"/"user_id".
        assert fgl.upstreams is not None
        assert fgl.downstreams is not None
        assert fgl.upstreams[0].endswith(",email)") or fgl.upstreams[0].endswith(
            ",user_id)"
        )
        assert fgl.downstreams[0].endswith(",email)") or fgl.downstreams[0].endswith(
            ",user_id)"
        )


def test_duplicate_column_names_deduped():
    handler = _seed_with_linked_dataset()
    columns = [_column("id"), _column("id")]
    wus = list(
        handler.gen_lineage_workunits(
            consumer_project_id="consumer-project",
            consumer_dataset="shared_dataset",
            entity_name="users",
            columns=columns,
        )
    )
    upstream = next(a for a in _aspects(wus) if isinstance(a, UpstreamLineage))
    assert upstream.fineGrainedLineages is not None
    assert len(upstream.fineGrainedLineages) == 1


def test_empty_columns_emit_copy_lineage_without_finegrained():
    """A linked entity with no columns still emits reciprocal siblings and a
    COPY upstream, but no column-level lineage."""
    handler = _seed_with_linked_dataset()
    wus = list(
        handler.gen_lineage_workunits(
            consumer_project_id="consumer-project",
            consumer_dataset="shared_dataset",
            entity_name="users",
            columns=[],
        )
    )
    sibling_wus = [wu for wu in wus if isinstance(_aspect(wu), Siblings)]
    upstreams = [a for a in _aspects(wus) if isinstance(a, UpstreamLineage)]
    assert len(sibling_wus) == 1
    assert len(list(handler.gen_publisher_sibling_workunits())) == 1
    assert len(upstreams) == 1
    assert upstreams[0].upstreams[0].type == DatasetLineageType.COPY
    assert upstreams[0].fineGrainedLineages is None


def test_finegrained_failure_emits_nothing():
    """A failure building column lineage must not leave the pair merged as
    siblings with no lineage tying them together."""
    handler = _seed_with_linked_dataset()
    with patch.object(
        handler, "_build_fine_grained_lineages", side_effect=ValueError("bad urn")
    ):
        with pytest.raises(ValueError):
            list(
                handler.gen_lineage_workunits(
                    consumer_project_id="consumer-project",
                    consumer_dataset="shared_dataset",
                    entity_name="users",
                    columns=[_column("id")],
                )
            )

    assert list(handler.gen_publisher_sibling_workunits()) == []
    assert handler.report.num_linked_dataset_lineage_emitted == 0


def test_publisher_siblings_grouped_across_consumers():
    """One publisher shared into several linked datasets gets a single aspect
    listing every consumer, rather than one aspect per consumer."""
    handler = _seed_with_linked_dataset()
    handler._lookup[("other-project", "other_shared")] = LinkedDatasetInfo(
        publisher=PublisherRef(
            dataset="publisher_dataset", project_id="publisher-project"
        ),
    )

    for project_id, dataset in (
        ("consumer-project", "shared_dataset"),
        ("other-project", "other_shared"),
    ):
        list(
            handler.gen_lineage_workunits(
                consumer_project_id=project_id,
                consumer_dataset=dataset,
                entity_name="users",
                columns=[_column("id")],
            )
        )

    publisher_wus = list(handler.gen_publisher_sibling_workunits())
    assert len(publisher_wus) == 1

    sibling = _aspect(publisher_wus[0])
    assert sibling.primary is True
    assert [u.split(",")[1] for u in sibling.siblings] == [
        "consumer-project.shared_dataset.users",
        "other-project.other_shared.users",
    ]


def test_publisher_siblings_separate_per_publisher_entity():
    """Two tables in one linked dataset map to two publisher entities."""
    handler = _seed_with_linked_dataset()
    for entity_name in ("users", "orders"):
        list(
            handler.gen_lineage_workunits(
                consumer_project_id="consumer-project",
                consumer_dataset="shared_dataset",
                entity_name=entity_name,
                columns=[_column("id")],
            )
        )

    publisher_wus = list(handler.gen_publisher_sibling_workunits())
    assert len(publisher_wus) == 2
    for wu in publisher_wus:
        assert len(_aspect(wu).siblings) == 1


# --- API error path -------------------------------------------------------


def test_list_subscriptions_generic_api_error_is_reported_as_failure():
    ah = MagicMock()
    ah.list_subscriptions.side_effect = GoogleAPIError("boom")
    handler = _seeded_handler(ah=ah).handler

    datasets = [BigqueryDataset(name="shared_a", location="US")]
    handler.populate_for_project("consumer-project", datasets)

    assert handler.get_info("consumer-project", "shared_a") is None
    assert handler.report.num_linked_dataset_location_errors == 1
    assert any(
        f.title == "Unable to list BigQuery Sharing subscriptions"
        for f in handler.report.failures
    )


# --- Lineage emission guard (schema-gen wiring) ---------------------------


def test_emission_error_is_recorded_and_not_fatal():
    """A failure inside lineage emission is downgraded to a warning, not raised."""
    handler = MagicMock()
    handler.emits_copy_lineage.return_value = True
    handler.gen_lineage_workunits.side_effect = ValueError("bad urn")
    report = BigQueryV2Report()
    # Lightweight stand-in for the generator; the guard reads only these attrs.
    gen: Any = SimpleNamespace(
        linked_datasets_handler=handler,
        report=report,
    )

    wus = list(
        BigQuerySchemaGenerator._emit_linked_dataset_lineage(
            gen,
            project_id="consumer-project",
            dataset_name="shared_dataset",
            entity_name="active_users",
            columns=[],
        )
    )

    assert wus == []
    assert list(report.linked_dataset_lineage_emission_errors) == [
        "consumer-project.shared_dataset.active_users"
    ]
    assert len(report.warnings) == 1


@pytest.mark.parametrize(
    "with_handler, publisher_resolved, include_lineage, expected",
    [
        (True, True, True, True),
        (True, True, False, False),
        (True, False, True, False),
        (False, True, True, False),
    ],
)
def test_emits_copy_lineage_predicate(
    with_handler: bool,
    publisher_resolved: bool,
    include_lineage: bool,
    expected: bool,
) -> None:
    """COPY lineage is claimed only for a resolved linked dataset with the flag on."""
    handler = (
        _seed_with_linked_dataset(
            config=_make_config(include_linked_dataset_lineage=include_lineage),
            publisher_project_id="publisher-project" if publisher_resolved else None,
        )
        if with_handler
        else None
    )
    gen: Any = SimpleNamespace(linked_datasets_handler=handler)
    assert (
        BigQuerySchemaGenerator._emits_linked_dataset_copy_lineage(
            gen, "consumer-project", "shared_dataset"
        )
        is expected
    )


def test_no_lineage_emitted_when_predicate_false():
    """When emits_copy_lineage is False (flag off or unresolved), emission is skipped."""
    handler = MagicMock()
    handler.emits_copy_lineage.return_value = False
    gen: Any = SimpleNamespace(
        linked_datasets_handler=handler,
        report=BigQueryV2Report(),
    )
    wus = list(
        BigQuerySchemaGenerator._emit_linked_dataset_lineage(
            gen,
            project_id="consumer-project",
            dataset_name="shared_dataset",
            entity_name="users",
            columns=[],
        )
    )
    assert wus == []
    handler.gen_lineage_workunits.assert_not_called()
