from datetime import datetime, timezone
from typing import Any, Dict, Optional

from google.cloud import bigquery, bigquery_analyticshub_v1

DEFAULT_LISTING = (
    "projects/123456789/locations/us/dataExchanges/exch_a/listings/listing_a"
)
# Constructed rather than referenced, because mypy types a proto-plus enum
# member as plain `int`.
STATE_ACTIVE = bigquery_analyticshub_v1.Subscription.State(
    bigquery_analyticshub_v1.Subscription.State.STATE_ACTIVE
)
DEFAULT_CREATION_TIME = datetime(2024, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
DEFAULT_LAST_MODIFY_TIME = datetime(2024, 3, 4, 5, 6, 7, tzinfo=timezone.utc)


def make_subscription(
    *,
    state: int = STATE_ACTIVE,
    dataset_id: str = "shared_dataset",
    project_id: str = "consumer-project",
    listing: str = DEFAULT_LISTING,
    data_exchange: str = "",
    org_display: str = "Publisher Inc",
    resource_type: int = bigquery_analyticshub_v1.SharedResourceType.BIGQUERY_DATASET,
    creation_time: Optional[datetime] = DEFAULT_CREATION_TIME,
    last_modify_time: Optional[datetime] = DEFAULT_LAST_MODIFY_TIME,
) -> bigquery_analyticshub_v1.Subscription:
    """Build a Subscription as the Analytics Hub API returns it.

    `listing` and `data_exchange` are a proto `oneof`, so passing
    `data_exchange` leaves `listing` unset.
    """
    subscription = bigquery_analyticshub_v1.Subscription(
        name=f"projects/{project_id}/locations/us/subscriptions/sub_1",
        state=state,
        organization_id="987654321",
        organization_display_name=org_display,
        subscriber_contact="ops@example.com",
        creation_time=creation_time,
        last_modify_time=last_modify_time,
        log_linked_dataset_query_user_email=False,
        resource_type=resource_type,
        destination_dataset=bigquery_analyticshub_v1.DestinationDataset(
            dataset_reference=bigquery_analyticshub_v1.DestinationDatasetReference(
                project_id=project_id, dataset_id=dataset_id
            ),
        ),
    )
    if data_exchange:
        subscription.data_exchange = data_exchange
    else:
        subscription.listing = listing
    return subscription


def make_dataset_with_linked_source(
    *,
    project_id: str = "consumer-project",
    dataset_id: str = "shared_dataset",
    publisher_project_number: str = "111222333",
    publisher_dataset: str = "publisher_dataset",
    link_state: str = "LINKED",
) -> bigquery.Dataset:
    """Build the Dataset `get_dataset` returns for a linked dataset."""
    return _make_dataset(
        project_id=project_id,
        dataset_id=dataset_id,
        link_state=link_state,
        linked_dataset_source={
            "sourceDataset": {
                "projectId": publisher_project_number,
                "datasetId": publisher_dataset,
            }
        },
    )


def make_dataset_without_linked_source(
    *,
    project_id: str = "consumer-project",
    dataset_id: str = "shared_dataset",
    link_state: str = "LINKED",
) -> bigquery.Dataset:
    """Build the Dataset `get_dataset` returns with no linked source exposed."""
    return _make_dataset(
        project_id=project_id,
        dataset_id=dataset_id,
        link_state=link_state,
        linked_dataset_source=None,
    )


def _make_dataset(
    *,
    project_id: str,
    dataset_id: str,
    link_state: str,
    linked_dataset_source: Optional[Dict[str, Any]],
) -> bigquery.Dataset:
    resource: Dict[str, Any] = {
        "datasetReference": {"projectId": project_id, "datasetId": dataset_id},
        "linkedDatasetMetadata": {"linkState": link_state},
    }
    if linked_dataset_source is not None:
        resource["linkedDatasetSource"] = linked_dataset_source
    return bigquery.Dataset.from_api_repr(resource)
