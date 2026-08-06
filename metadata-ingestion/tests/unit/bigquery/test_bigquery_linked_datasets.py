from types import SimpleNamespace
from typing import Any, Dict, Iterable, List, Optional
from unittest.mock import MagicMock

import pytest
from google.api_core.exceptions import GoogleAPIError, NotFound, PermissionDenied
from google.cloud import bigquery_analyticshub_v1
from google.rpc.error_details_pb2 import ErrorInfo

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.bigquery_v2.bigquery_config import BigQueryV2Config
from datahub.ingestion.source.bigquery_v2.bigquery_linked_datasets import (
    BigQueryLinkedDatasetsHandler,
    LinkedDatasetInfo,
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

# --- Fixtures and helpers --------------------------------------------------


def _make_config(**overrides: Any) -> BigQueryV2Config:
    base: Dict[str, Any] = {
        "project_ids": ["consumer-project"],
        "include_linked_datasets": True,
        "include_linked_dataset_lineage": True,
    }
    base.update(overrides)
    return BigQueryV2Config.model_validate(base)


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


_DEFAULT_STATE_ACTIVE = int(bigquery_analyticshub_v1.Subscription.State.STATE_ACTIVE)
_DEFAULT_RESOURCE_TYPE_BQ = int(
    bigquery_analyticshub_v1.SharedResourceType.BIGQUERY_DATASET
)


def _make_subscription(
    *,
    state: int = _DEFAULT_STATE_ACTIVE,
    dataset_id: str = "shared_dataset",
    project_id: str = "consumer-project",
    listing: str = (
        "projects/123456789/locations/us/dataExchanges/exch_a/listings/listing_a"
    ),
    data_exchange: str = "",
    org_display: str = "Publisher Inc",
    resource_type: int = _DEFAULT_RESOURCE_TYPE_BQ,
) -> SimpleNamespace:
    """Minimal stand-in for `Subscription`.

    SimpleNamespace avoids the real proto's field typing; the handler only
    reads attributes via getattr.
    """
    destination = SimpleNamespace(
        dataset_reference=SimpleNamespace(project_id=project_id, dataset_id=dataset_id)
    )
    return SimpleNamespace(
        name=f"projects/{project_id}/locations/us/subscriptions/sub_1",
        listing=listing,
        data_exchange=data_exchange,
        state=state,
        organization_id="987654321",
        organization_display_name=org_display,
        subscriber_contact="ops@example.com",
        creation_time=None,
        last_modify_time=None,
        log_linked_dataset_query_user_email=False,
        resource_type=resource_type,
        destination_dataset=destination,
    )


def _make_dataset_with_linked_source(
    *,
    publisher_project_number: str = "111222333",
    publisher_dataset: str = "publisher_dataset",
    link_state: str = "LINKED",
) -> SimpleNamespace:
    """Stand-in for the Dataset returned by `get_dataset`.

    The handler reads `_properties` for linked-dataset fields not exposed as
    typed attributes.
    """
    properties: Dict[str, Any] = {
        "linkedDatasetSource": {
            "sourceDataset": {
                "projectId": publisher_project_number,
                "datasetId": publisher_dataset,
            }
        },
        "linkedDatasetMetadata": {"linkState": link_state},
    }
    return SimpleNamespace(_properties=properties)


def _ah_client_returning(
    subscriptions_per_location: Dict[str, List[Any]],
) -> MagicMock:
    """Build an AH client mock whose `list_subscriptions` returns by location."""
    mock_client = MagicMock()

    def _list_subscriptions(parent: str) -> List[Any]:
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
        return SimpleNamespace(project_id=result)

    mock_client.get_project.side_effect = _get_project
    return mock_client


def _install_clients(
    handler: BigQueryLinkedDatasetsHandler,
    *,
    ah: Optional[MagicMock] = None,
    bq: Optional[MagicMock] = None,
    rm: Optional[MagicMock] = None,
) -> None:
    if ah is not None:
        handler._ah_client = ah
    if bq is not None:
        handler._bq_client = bq
    if rm is not None:
        handler._rm_client = rm


# --- LinkedDatasetInfo tests -----------------------------------------------


def test_extra_properties_includes_source_when_publisher_resolved():
    info = LinkedDatasetInfo(
        consumer_project_id="c-proj",
        consumer_dataset="shared_dataset",
        publisher_project_number="111",
        publisher_project_id="publisher-project",
        publisher_dataset="publisher_dataset",
        subscription_state=bigquery_analyticshub_v1.Subscription.State(
            _DEFAULT_STATE_ACTIVE
        ),
        link_state="LINKED",
        listing="listing_a",
        publisher_organization="Publisher Inc",
    )
    props = info.to_extra_properties()
    assert props["linked_dataset.source"] == "publisher-project.publisher_dataset"
    assert props["linked_dataset.link_type"] == "LINKED"
    assert props["linked_dataset.link_state"] == "LINKED"
    assert props["analytics_hub.listing"] == "listing_a"
    assert props["analytics_hub.subscription_state"] == "STATE_ACTIVE"
    assert props["analytics_hub.publisher_organization"] == "Publisher Inc"


def test_extra_properties_falls_back_to_data_exchange():
    info = LinkedDatasetInfo(
        consumer_project_id="c-proj",
        consumer_dataset="shared_dataset",
        publisher_project_number="111",
        publisher_project_id="publisher-project",
        publisher_dataset="publisher_dataset",
        listing=None,
        data_exchange="exchange_a",
    )
    props = info.to_extra_properties()
    assert props["analytics_hub.listing"] == "exchange_a"


def test_extra_properties_omits_unpopulated_keys():
    info = LinkedDatasetInfo(
        consumer_project_id="c-proj",
        consumer_dataset="shared_dataset",
        publisher_project_number="111",
        publisher_project_id="publisher-project",
        publisher_dataset="publisher_dataset",
    )
    props = info.to_extra_properties()
    # Always-emitted keys are present.
    assert "linked_dataset.source" in props
    assert props["linked_dataset.link_type"] == "LINKED"
    # Optional keys must be omitted when None.
    assert "linked_dataset.link_state" not in props
    assert "analytics_hub.listing" not in props
    assert "analytics_hub.publisher_organization" not in props
    assert "analytics_hub.link_creation_time" not in props


def test_extra_properties_no_source_when_publisher_unresolved():
    info = LinkedDatasetInfo(
        consumer_project_id="c-proj",
        consumer_dataset="shared_dataset",
        publisher_project_number="111",
        publisher_project_id=None,
        publisher_dataset="publisher_dataset",
        link_state="LINKED",
    )
    props = info.to_extra_properties()
    # Without a resolved publisher project, the source key must not appear.
    assert "linked_dataset.source" not in props
    # Other governance keys still emit.
    assert props["linked_dataset.link_type"] == "LINKED"
    assert props["linked_dataset.link_state"] == "LINKED"


# --- populate_for_project tests --------------------------------------------


def test_only_bigquery_dataset_subscriptions_advance():
    """Non-BigQuery shared resources (e.g. Pub/Sub) are skipped."""
    handler = _make_handler()
    bq_dataset_sub = _make_subscription(dataset_id="shared_a")
    pubsub_sub = _make_subscription(
        dataset_id="shared_b",
        resource_type=99,  # any non-BIGQUERY_DATASET value
    )

    ah = _ah_client_returning({"us": [bq_dataset_sub, pubsub_sub]})
    bq = _bq_client_returning(
        {
            "consumer-project.shared_a": _make_dataset_with_linked_source(),
        }
    )
    rm = _rm_client_returning({"111222333": "publisher-project"})
    _install_clients(handler, ah=ah, bq=bq, rm=rm)

    datasets = [BigqueryDataset(name="shared_a", location="US")]
    handler.populate_for_project("consumer-project", datasets)

    assert handler.get_info("consumer-project", "shared_a") is not None
    assert handler.get_info("consumer-project", "shared_b") is None
    assert handler.report.num_linked_datasets_scanned == 1
    # Publisher resolution should have happened only once for the BQ_DATASET sub.
    bq.get_dataset.assert_called_once_with("consumer-project.shared_a")


def test_dataset_pattern_filter_short_circuits():
    """A dataset excluded by dataset_pattern is skipped before any get_dataset call."""
    config = _make_config(
        dataset_pattern=AllowDenyPattern(allow=[".*shared_a$"], deny=[])
    )
    handler = _make_handler(config=config)
    sub_a = _make_subscription(dataset_id="shared_a")
    sub_b = _make_subscription(dataset_id="shared_b")

    ah = _ah_client_returning({"us": [sub_a, sub_b]})
    bq = _bq_client_returning(
        {
            "consumer-project.shared_a": _make_dataset_with_linked_source(),
        }
    )
    rm = _rm_client_returning({"111222333": "publisher-project"})
    _install_clients(handler, ah=ah, bq=bq, rm=rm)

    datasets = [
        BigqueryDataset(name="shared_a", location="US"),
        BigqueryDataset(name="shared_b", location="US"),
    ]
    handler.populate_for_project("consumer-project", datasets)

    assert handler.get_info("consumer-project", "shared_a") is not None
    assert handler.get_info("consumer-project", "shared_b") is None
    bq.get_dataset.assert_called_once_with("consumer-project.shared_a")


def test_locations_lowercased_for_ah_call():
    handler = _make_handler()
    sub = _make_subscription(dataset_id="shared_a")
    ah = _ah_client_returning({"eu": [sub]})
    bq = _bq_client_returning(
        {"consumer-project.shared_a": _make_dataset_with_linked_source()}
    )
    rm = _rm_client_returning({"111222333": "publisher-project"})
    _install_clients(handler, ah=ah, bq=bq, rm=rm)

    # Mixed-case location coming back from BigQuery's API is normalised.
    datasets = [BigqueryDataset(name="shared_a", location="EU")]
    handler.populate_for_project("consumer-project", datasets)

    ah.list_subscriptions.assert_called_with(
        parent="projects/consumer-project/locations/eu"
    )


def test_state_counters_incremented():
    """STALE and INACTIVE subscriptions are still ingested; only counters differ."""
    handler = _make_handler()
    stale_sub = _make_subscription(
        dataset_id="shared_stale",
        state=int(bigquery_analyticshub_v1.Subscription.State.STATE_STALE),
    )
    inactive_sub = _make_subscription(
        dataset_id="shared_inactive",
        state=int(bigquery_analyticshub_v1.Subscription.State.STATE_INACTIVE),
    )
    active_sub = _make_subscription(dataset_id="shared_active")

    ah = _ah_client_returning({"us": [stale_sub, inactive_sub, active_sub]})
    bq = _bq_client_returning(
        {
            "consumer-project.shared_stale": _make_dataset_with_linked_source(),
            "consumer-project.shared_inactive": _make_dataset_with_linked_source(),
            "consumer-project.shared_active": _make_dataset_with_linked_source(),
        }
    )
    rm = _rm_client_returning({"111222333": "publisher-project"})
    _install_clients(handler, ah=ah, bq=bq, rm=rm)

    datasets = [
        BigqueryDataset(name=name, location="US")
        for name in ("shared_stale", "shared_inactive", "shared_active")
    ]
    handler.populate_for_project("consumer-project", datasets)

    assert handler.report.num_linked_datasets_scanned == 3
    assert handler.report.num_linked_dataset_state_stale == 1
    assert handler.report.num_linked_dataset_state_inactive == 1


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
    handler = _make_handler()
    sub = _make_subscription(dataset_id="shared_a")
    ah = _ah_client_returning({"us": [sub]})
    bq = _bq_client_returning({"consumer-project.shared_a": error})
    _install_clients(handler, ah=ah, bq=bq)

    datasets = [BigqueryDataset(name="shared_a", location="US")]
    handler.populate_for_project("consumer-project", datasets)

    assert handler.get_info("consumer-project", "shared_a") is None
    assert handler.report.num_linked_dataset_get_dataset_errors == 1


def test_linked_dataset_without_source_is_warned_and_kept():
    handler = _make_handler()
    sub = _make_subscription(dataset_id="shared_a")
    ah = _ah_client_returning({"us": [sub]})
    # get_dataset succeeds but exposes no linkedDatasetSource (e.g. the
    # subscriber cannot see the publisher project).
    dataset_no_source = SimpleNamespace(
        _properties={"linkedDatasetMetadata": {"linkState": "LINKED"}}
    )
    bq = _bq_client_returning({"consumer-project.shared_a": dataset_no_source})
    _install_clients(handler, ah=ah, bq=bq)

    datasets = [BigqueryDataset(name="shared_a", location="US")]
    handler.populate_for_project("consumer-project", datasets)

    info = handler.get_info("consumer-project", "shared_a")
    assert info is not None
    assert info.has_publisher is False
    assert handler.report.num_linked_dataset_source_unresolved == 1
    assert any("source not resolved" in str(w).lower() for w in handler.report.warnings)


def test_publisher_resolve_failure_keeps_dataset_but_skips_lineage():
    handler = _make_handler()
    sub = _make_subscription(dataset_id="shared_a")
    ah = _ah_client_returning({"us": [sub]})
    bq = _bq_client_returning(
        {
            "consumer-project.shared_a": _make_dataset_with_linked_source(
                publisher_project_number="111222333",
            )
        }
    )
    rm = _rm_client_returning(
        {"111222333": PermissionDenied("resourcemanager.projects.get denied")}
    )
    _install_clients(handler, ah=ah, bq=bq, rm=rm)

    datasets = [BigqueryDataset(name="shared_a", location="US")]
    handler.populate_for_project("consumer-project", datasets)

    info = handler.get_info("consumer-project", "shared_a")
    # Dataset is still recognised as linked (governance properties emit).
    assert info is not None
    # But publisher project ID was not resolved — has_publisher is False.
    assert info.has_publisher is False
    assert handler.report.num_linked_dataset_project_resolve_errors == 1

    # And lineage emission is a no-op on this dataset.
    wus = list(
        handler.gen_lineage_workunits(
            consumer_project_id="consumer-project",
            consumer_dataset="shared_a",
            entity_name="t1",
            columns=[
                BigqueryColumn(
                    name="id",
                    ordinal_position=1,
                    is_nullable=False,
                    field_path="id",
                    is_partition_column=False,
                    cluster_column_position=None,
                    data_type="INT64",
                    comment="",
                )
            ],
        )
    )
    assert wus == []


@pytest.mark.parametrize(
    "rm_result",
    ["publisher-project", PermissionDenied("denied")],
    ids=["success", "failure"],
)
def test_resource_manager_result_is_cached_per_project_number(rm_result):
    """Publisher project resolution is cached per number, for success and failure."""
    handler = _make_handler()
    ah = _ah_client_returning(
        {
            "us": [
                _make_subscription(dataset_id="shared_a"),
                _make_subscription(dataset_id="shared_b"),
            ]
        }
    )
    # Both subscriptions point at the same publisher project number.
    bq = _bq_client_returning(
        {
            "consumer-project.shared_a": _make_dataset_with_linked_source(
                publisher_dataset="dataset_a"
            ),
            "consumer-project.shared_b": _make_dataset_with_linked_source(
                publisher_dataset="dataset_b"
            ),
        }
    )
    rm = _rm_client_returning({"111222333": rm_result})
    _install_clients(handler, ah=ah, bq=bq, rm=rm)

    datasets = [
        BigqueryDataset(name="shared_a", location="US"),
        BigqueryDataset(name="shared_b", location="US"),
    ]
    handler.populate_for_project("consumer-project", datasets)

    # Both outcomes are cached, so the second subscription reuses the result.
    rm.get_project.assert_called_once_with(name="projects/111222333")


def test_list_subscriptions_api_disabled_is_warned_not_fatal():
    handler = _make_handler()
    ah = MagicMock()
    ah.list_subscriptions.side_effect = PermissionDenied(
        "Analytics Hub API has not been used in project ... or it is disabled.",
        error_info=ErrorInfo(reason="SERVICE_DISABLED", domain="googleapis.com"),
    )
    _install_clients(handler, ah=ah)

    datasets = [BigqueryDataset(name="shared_a", location="US")]
    handler.populate_for_project("consumer-project", datasets)

    assert handler.get_info("consumer-project", "shared_a") is None
    assert any("not enabled" in str(w) for w in handler.report.warnings)
    assert list(handler.report.failures) == []


def test_list_subscriptions_iam_denied_is_reported_as_failure():
    handler = _make_handler()
    ah = MagicMock()
    ah.list_subscriptions.side_effect = PermissionDenied(
        "Permission analyticshub.subscriptions.list denied.",
        error_info=ErrorInfo(
            reason="IAM_PERMISSION_DENIED", domain="analyticshub.googleapis.com"
        ),
    )
    _install_clients(handler, ah=ah)

    datasets = [BigqueryDataset(name="shared_a", location="US")]
    # A missing grant on an explicitly-enabled feature is a failure, but must
    # not raise — core dataset ingestion continues.
    handler.populate_for_project("consumer-project", datasets)

    assert handler.get_info("consumer-project", "shared_a") is None
    assert any(
        "analyticshub.subscriptions.list" in str(f) for f in handler.report.failures
    )


def test_list_subscriptions_unclassified_permission_denied_propagates():
    handler = _make_handler()
    ah = MagicMock()
    # A PermissionDenied we cannot classify (no ErrorInfo reason) is not
    # swallowed; it propagates to the schema-gen call-site guard.
    ah.list_subscriptions.side_effect = PermissionDenied("denied")
    _install_clients(handler, ah=ah)

    datasets = [BigqueryDataset(name="shared_a", location="US")]
    with pytest.raises(PermissionDenied):
        handler.populate_for_project("consumer-project", datasets)


def test_data_exchange_only_subscription_uses_data_exchange_segment():
    """With no listing, the data_exchange resource path is reduced to its last segment."""
    handler = _make_handler()
    sub = _make_subscription(
        dataset_id="shared_a",
        listing="",  # no listing
        data_exchange=("projects/123/locations/us/dataExchanges/my_exchange"),
    )
    ah = _ah_client_returning({"us": [sub]})
    bq = _bq_client_returning(
        {"consumer-project.shared_a": _make_dataset_with_linked_source()}
    )
    rm = _rm_client_returning({"111222333": "publisher-project"})
    _install_clients(handler, ah=ah, bq=bq, rm=rm)

    datasets = [BigqueryDataset(name="shared_a", location="US")]
    handler.populate_for_project("consumer-project", datasets)

    info = handler.get_info("consumer-project", "shared_a")
    assert info is not None
    assert info.listing is None
    assert info.data_exchange == "my_exchange"


# --- Lineage emission tests ------------------------------------------------


def _seed_with_linked_dataset(
    *,
    config: Optional[BigQueryV2Config] = None,
    publisher_project_id: Optional[str] = "publisher-project",
    publisher_dataset: str = "publisher_dataset",
) -> BigQueryLinkedDatasetsHandler:
    handler = _make_handler(config=config)
    # Populate the lookup directly to exercise emission without detection.
    handler._lookup[("consumer-project", "shared_dataset")] = LinkedDatasetInfo(
        consumer_project_id="consumer-project",
        consumer_dataset="shared_dataset",
        publisher_project_number="111222333",
        publisher_project_id=publisher_project_id,
        publisher_dataset=publisher_dataset,
    )
    return handler


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


def _aspects(wus: Iterable[MetadataWorkUnit]) -> List[Any]:
    out: List[Any] = []
    for wu in wus:
        mcp = wu.metadata
        aspect = getattr(mcp, "aspect", None)
        if aspect is not None:
            out.append(aspect)
    return out


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
    sibling_wus = [
        wu for wu in wus if isinstance(getattr(wu.metadata, "aspect", None), Siblings)
    ]
    upstream_lineages = [a for a in aspects if isinstance(a, UpstreamLineage)]
    assert len(sibling_wus) == 2
    assert len(upstream_lineages) == 1

    # Reciprocal siblings: consumer non-primary pointing at publisher,
    # publisher primary pointing back at consumer.
    by_urn: Dict[Any, Any] = {
        getattr(wu.metadata, "entityUrn", None): getattr(wu.metadata, "aspect", None)
        for wu in sibling_wus
    }
    consumer_urn = next(u for u in by_urn if "shared_dataset" in u)
    publisher_urn = next(u for u in by_urn if "publisher_dataset" in u)

    consumer_sibling = by_urn[consumer_urn]
    assert consumer_sibling.primary is False
    assert consumer_sibling.siblings == [publisher_urn]
    assert publisher_urn.endswith(",PROD)")

    publisher_sibling = by_urn[publisher_urn]
    assert publisher_sibling.primary is True
    assert publisher_sibling.siblings == [consumer_urn]

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


# --- API error path -------------------------------------------------------


def test_list_subscriptions_generic_api_error_propagates():
    handler = _make_handler()
    ah = MagicMock()
    ah.list_subscriptions.side_effect = GoogleAPIError("boom")
    _install_clients(handler, ah=ah)

    datasets = [BigqueryDataset(name="shared_a", location="US")]
    # Unexpected API errors are not swallowed here; they propagate to the
    # schema-gen call site, which scopes the failure to the project.
    with pytest.raises(GoogleAPIError):
        handler.populate_for_project("consumer-project", datasets)


# --- Lineage emission guard (schema-gen wiring) ---------------------------


def test_emission_error_is_recorded_and_not_fatal():
    """A failure inside lineage emission is downgraded to a warning, not raised."""
    handler = MagicMock()
    handler.gen_lineage_workunits.side_effect = ValueError("bad urn")
    report = BigQueryV2Report()
    # Lightweight stand-in for the generator; the guard reads only these attrs.
    gen: Any = SimpleNamespace(
        linked_datasets_handler=handler,
        config=SimpleNamespace(include_linked_dataset_lineage=True),
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
    assert any(
        "Failed to emit linked dataset lineage" in str(w) for w in report.warnings
    )


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
            publisher_project_id="publisher-project" if publisher_resolved else None
        )
        if with_handler
        else None
    )
    gen: Any = SimpleNamespace(
        linked_datasets_handler=handler,
        config=SimpleNamespace(include_linked_dataset_lineage=include_lineage),
    )
    assert (
        BigQuerySchemaGenerator._emits_linked_dataset_copy_lineage(
            gen, "consumer-project", "shared_dataset"
        )
        is expected
    )


def test_no_lineage_emitted_when_flag_disabled():
    """With include_linked_dataset_lineage off, emission is skipped entirely."""
    handler = MagicMock()
    gen: Any = SimpleNamespace(
        linked_datasets_handler=handler,
        config=SimpleNamespace(include_linked_dataset_lineage=False),
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
