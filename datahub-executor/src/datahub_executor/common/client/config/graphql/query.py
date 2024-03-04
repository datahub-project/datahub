import pathlib

GRAPHQL_FETCH_EXECUTOR_CONFIGS = (
    pathlib.Path(__file__).parent / "list_executor_configs.gql"
).read_text()
