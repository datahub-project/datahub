import pathlib

ENTITY_FRAGMENT = (pathlib.Path(__file__).parent / "entity.graphql").read_text()
SEARCH_FRAGMENT = (pathlib.Path(__file__).parent / "search.graphql").read_text()

SLACK_SEARCH_QUERY = f"""
    {ENTITY_FRAGMENT}
    {SEARCH_FRAGMENT}
    query getSearchResultsForMultiple($input: SearchAcrossEntitiesInput!) {{
      searchAcrossEntities(input: $input) {{
        ...searchResults
      }}
    }}
"""

SLACK_GET_ENTITY_QUERY = f"""
    {ENTITY_FRAGMENT}
    query getEntity($urn: String!) {{
      entity(urn: $urn) {{
        ...entityFields
      }}
    }}
"""
