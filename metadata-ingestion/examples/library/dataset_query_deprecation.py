from typing import Optional, Tuple

from datahub.metadata.schema_classes import DeprecationClass
from datahub.sdk import DataHubClient, DatasetUrn


def query_dataset_deprecation(
    client: DataHubClient, dataset_urn: DatasetUrn
) -> Tuple[bool, Optional[str], Optional[int]]:
    """
    Query the deprecation status of a dataset.

    Args:
        client: DataHub client to use for the query
        dataset_urn: URN of the dataset to check

    Returns:
        Tuple of (is_deprecated, deprecation_note, decommission_time_millis)
    """
    dataset = client.entities.get(dataset_urn)

    deprecation = dataset._get_aspect(DeprecationClass)
    if deprecation and deprecation.deprecated:
        return (True, deprecation.note, deprecation.decommissionTime)
    return (False, None, None)


def main(client: Optional[DataHubClient] = None) -> None:
    """
    Main function to query dataset deprecation example.

    Args:
        client: Optional DataHub client (for testing). If not provided, creates one from env.
    """
    client = client or DataHubClient.from_env()

    dataset_urn = DatasetUrn(platform="hive", name="fct_users_created", env="PROD")

    is_deprecated, note, decommission_time = query_dataset_deprecation(
        client, dataset_urn
    )

    if is_deprecated:
        print(f"Dataset is deprecated: {note}")
        if decommission_time:
            print(f"Decommission time: {decommission_time}")
    else:
        print("Dataset is not deprecated")


if __name__ == "__main__":
    main()
