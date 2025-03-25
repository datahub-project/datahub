import pathlib

GRAPHQL_LIST_MONITORS_QUERY = (
    pathlib.Path(__file__).parent / "list_monitors.gql"
).read_text()
GRAPHQL_LIST_MONITORS_OPERATION = "listMonitors"
GRAPHQL_GET_MONITOR_OPERATION = "getMonitor"


GRAPHQL_LIST_INGESTION_SOURCES_QUERY = (
    pathlib.Path(__file__).parent / "list_ingestion_sources.gql"
).read_text()


GRAPHQL_INGESTION_SOURCE_FOR_ENTITY_QUERY = (
    pathlib.Path(__file__).parent / "ingestion_source_for_entity.gql"
).read_text()


GRAPHQL_GET_ASSERTION_QUERY = (
    pathlib.Path(__file__).parent / "get_assertion.gql"
).read_text()


GRAPHQL_GET_DATASET_QUERY = (
    pathlib.Path(__file__).parent / "get_dataset.gql"
).read_text()
