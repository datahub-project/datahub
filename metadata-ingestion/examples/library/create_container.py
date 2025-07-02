from datahub.emitter.mcp_builder import ContainerKey
from datahub.sdk import Container, DataHubClient

client = DataHubClient.from_env()

# datajob will inherit the platform and platform instance from the flow

container = Container(
    container_key=ContainerKey(platform="mlflow", name="airline_forecast_experiment"),
    display_name="Airline Forecast Experiment",
)

client.entities.upsert(container)
