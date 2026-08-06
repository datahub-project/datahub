from types import SimpleNamespace
from typing import Any, Dict

from google.cloud import bigquery_analyticshub_v1

DEFAULT_STATE_ACTIVE = int(bigquery_analyticshub_v1.Subscription.State.STATE_ACTIVE)
DEFAULT_RESOURCE_TYPE_BQ = int(
    bigquery_analyticshub_v1.SharedResourceType.BIGQUERY_DATASET
)


def make_subscription(
    *,
    state: int = DEFAULT_STATE_ACTIVE,
    dataset_id: str = "shared_dataset",
    project_id: str = "consumer-project",
    listing: str = (
        "projects/123456789/locations/us/dataExchanges/exch_a/listings/listing_a"
    ),
    data_exchange: str = "",
    org_display: str = "Publisher Inc",
    resource_type: int = DEFAULT_RESOURCE_TYPE_BQ,
) -> SimpleNamespace:
    """Minimal stand-in for `Subscription`.

    SimpleNamespace avoids the real proto's field typing. `state` is stored as
    the enum member the real proto exposes, so the handler reads it directly.
    """
    destination = SimpleNamespace(
        dataset_reference=SimpleNamespace(project_id=project_id, dataset_id=dataset_id)
    )
    return SimpleNamespace(
        name=f"projects/{project_id}/locations/us/subscriptions/sub_1",
        listing=listing,
        data_exchange=data_exchange,
        state=bigquery_analyticshub_v1.Subscription.State(state),
        organization_id="987654321",
        organization_display_name=org_display,
        subscriber_contact="ops@example.com",
        creation_time=None,
        last_modify_time=None,
        log_linked_dataset_query_user_email=False,
        resource_type=resource_type,
        destination_dataset=destination,
    )


def make_dataset_with_linked_source(
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
