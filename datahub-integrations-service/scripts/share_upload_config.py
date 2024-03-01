import os

from datahub_integrations.app import graph
from datahub_integrations.graphql.connection import save_connection_json

if __name__ == "__main__":
    server = os.environ["DATAHUB_SERVER"]
    token = os.environ.get("DATAHUB_TOKEN")

    save_connection_json(
        graph=graph,
        urn="urn:li:dataHubConnection:__sync_0",
        platform_urn="urn:li:dataPlatform:datahub",
        config={
            "connection": {
                "server": server,
                "token": token,
            }
        },
        name="limited instance",
    )
