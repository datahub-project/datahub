import pathlib

GRAPHQL_LIST_MONITORS_QUERY = (
    pathlib.Path(__file__).parent / "list_monitors.gql"
).read_text()


GRAPHQL_LIST_INGESTION_SOURCES_QUERY = (
    pathlib.Path(__file__).parent / "list_ingestion_sources.gql"
).read_text()


GRAPHQL_INGESTION_SOURCE_FOR_ENTITY_QUERY = (
    pathlib.Path(__file__).parent / "ingestion_source_for_entity.gql"
).read_text()
