import pathlib

GRAPHQL_LIST_INGESTION_SOURCES_QUERY = (
    pathlib.Path(__file__).parent / "list_ingestion_sources.gql"
).read_text()
