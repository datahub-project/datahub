import argparse

from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.graph.config import DatahubClientConfig

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Fetch entities from DataHub using get_entities_v3"
    )
    parser.add_argument("--token", required=False, help="DataHub access token")
    parser.add_argument(
        "--server_url",
        required=False,
        default="http://localhost:8080",
        help="DataHub server URL (defaults to http://localhost:8080)",
    )
    parser.add_argument(
        "--entity_name",
        required=True,
        help="Entity type name (e.g., dataset, dashboard, chart)",
    )
    parser.add_argument(
        "--urn",
        required=True,
        action="append",
        dest="urns",
        help="Entity URN(s) to fetch. Can specify multiple times.",
    )
    parser.add_argument(
        "--aspect",
        action="append",
        dest="aspects",
        help="Aspect name(s) to fetch. Can specify multiple times. If none provided, all aspects will be fetched.",
    )
    args = parser.parse_args()

    # Validate that at least one URN is provided
    if not args.urns:
        parser.error("At least one --urn argument is required")

    # Initialize the DataHub client
    client = DataHubGraph(
        config=DatahubClientConfig(
            server=args.server_url,
            token=args.token,
        )
    )

    response = client.get_entities_v3(
        entity_name=args.entity_name,
        urns=args.urns,
        aspects=args.aspects,
    )

    print(f"Received {len(response)} entities")

    for urn, entity in response.items():
        print(f"Entity: {urn}")

        if not entity:
            print("\tNo aspects found for this entity")
            continue

        for aspect_name, aspect in entity.items():
            print(f"\tAspect: {aspect_name} Type: {type(aspect).__name__}")
            print(f"\t\t{aspect}")

        print()
