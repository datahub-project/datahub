from datahub.emitter.mcp_builder import DatabaseKey
from datahub.sdk import Container, DataHubClient

client = DataHubClient.from_env()

container = Container(
    container_key=DatabaseKey(platform="snowflake", database="my_database"),
    display_name="MY_DATABASE",
)

client.entities.upsert(container)
