import pathlib

GRAPHQL_SCROLL_SHARED_ENTITIES = (
    pathlib.Path(__file__).parent / "scroll_shared_entities.gql"
).read_text()

GRAPHQL_SHARE_ENTITY = (pathlib.Path(__file__).parent / "share_entity.gql").read_text()
